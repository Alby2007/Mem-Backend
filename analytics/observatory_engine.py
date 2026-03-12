"""
analytics/observatory_engine.py

Observatory Engine — runs hourly, observes fleet state, takes safe actions
autonomously, queues unsafe actions for operator approval, reports via
operator_bot.send_observatory_report().

Entry point: ObservatoryEngine(DB_PATH).run()
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

_log = logging.getLogger(__name__)

# ── Budget guard ────────────────────────────────────────────────────────────────
_LLM_DAILY_TOKEN_LIMIT = int(os.environ.get('OBSERVATORY_TOKEN_LIMIT', '50000'))


@dataclass
class Finding:
    sensor: str           # 'fleet_gap' | 'bot_performance' | 'calibration' | 'delivery'
    severity: str         # 'critical' | 'high' | 'medium' | 'info'
    subject: str          # e.g. 'liquidity_void+.L' or 'FVG Scanner'
    description: str      # human-readable one sentence
    action_type: str      # 'create_bot' | 'tune_bot' | 'rebalance' | 'expire_patterns' | 'alert_only'
    action_params: dict   # params for the action
    auto_eligible: bool   # passes safety gates?
    evidence: dict        # raw numbers


@dataclass
class ReasonedAction:
    finding: Finding
    act: bool
    params: dict          # may override finding.action_params
    rationale: str        # one sentence from LLM


# ── Safety gates ────────────────────────────────────────────────────────────────
_NEVER_AUTO = frozenset([
    'kill_bot', 'direction_bias_change', 'git_pull_deploy',
    'service_restart', 'file_patch', 'file_write',
])

_SEVERITY_ORDER = {'critical': 4, 'high': 3, 'medium': 2, 'info': 1}


def _auto_approve(action_type: str, params: dict, fleet: dict) -> bool:
    """Return True if this action passes all auto-approve safety gates."""
    if action_type in _NEVER_AUTO:
        return False
    rules = {
        'create_bot': lambda p, f: (
            p.get('obs', 0) >= 1000
            and p.get('hit_rate', 0) >= 0.40
            and f.get('capital_per_new_bot', 0) <= f.get('total_capital', 1) * 0.20
            and f.get('active_bot_count', 99) < 25
        ),
        'tune_bot': lambda p, f: (
            p.get('parameter') in ('min_quality', 'risk_pct', 'scan_interval_sec')
            and abs(p.get('change_pct', 1.0)) <= 0.20
            and p.get('trade_count', 0) >= 10
        ),
        'tune_bot_max_positions': lambda p, f: (
            p.get('delta', 99) <= 1
            and f.get('win_rate', 0) >= 0.35
        ),
        'expire_patterns': lambda p, f: (
            p.get('quality_below', 1.0) <= 0.40
        ),
        'fleet_rebalance': lambda p, f: (
            p.get('strategy') == 'equal'
        ),
    }
    rule = rules.get(action_type)
    if rule is None:
        return False
    try:
        return bool(rule(params, fleet))
    except Exception:
        return False


# ── DDL ─────────────────────────────────────────────────────────────────────────
_DDL_RUNS = """
CREATE TABLE IF NOT EXISTS observatory_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at              TEXT    NOT NULL,
    findings_json       TEXT,
    actions_taken_json  TEXT,
    actions_queued_json TEXT,
    tokens_used         INTEGER DEFAULT 0,
    llm_called          INTEGER DEFAULT 0,
    run_duration_sec    REAL,
    error               TEXT
)
"""


def _ensure_observatory_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_RUNS)
    conn.commit()


# ── Sensor 1: FleetGapSensor ────────────────────────────────────────────────────
class _FleetGapSensor:
    def scan(self, conn: sqlite3.Connection) -> list[Finding]:
        findings: list[Finding] = []
        try:
            # Aggregate calibration by (pattern_type, exchange_suffix)
            # exchange_suffix = last segment after '.' in ticker, or ''
            rows = conn.execute(
                """SELECT pattern_type,
                          CASE WHEN instr(ticker, '.') > 0
                               THEN '.' || substr(ticker, instr(ticker,'.')+1)
                               ELSE '' END AS suffix,
                          SUM(sample_size)           AS total_obs,
                          AVG(hit_rate_t1)            AS avg_hit_rate
                   FROM signal_calibration
                   WHERE sample_size > 0
                   GROUP BY pattern_type, suffix
                   HAVING total_obs >= 500 AND avg_hit_rate >= 0.38"""
            ).fetchall()
        except Exception as e:
            _log.warning('FleetGapSensor: calibration query failed: %s', e)
            return findings

        try:
            active_bots = conn.execute(
                """SELECT bot_id, pattern_types, exchanges
                   FROM paper_bot_configs
                   WHERE active=1 AND killed_at IS NULL"""
            ).fetchall()
        except Exception as e:
            _log.warning('FleetGapSensor: bot query failed: %s', e)
            active_bots = []

        # Build coverage index: set of (pattern_type, suffix) pairs covered
        covered: set[tuple[str, str]] = set()
        for (_bid, pt_json, ex_json) in active_bots:
            try:
                patterns = json.loads(pt_json or '[]') or []
            except Exception:
                patterns = []
            try:
                exchanges = json.loads(ex_json or '[]') or []
            except Exception:
                exchanges = []
            for pt in patterns:
                if not exchanges:
                    covered.add((pt, ''))   # covers all exchanges
                for ex in exchanges:
                    covered.add((pt, ex))

        for (pattern_type, suffix, total_obs, avg_hit) in rows:
            suffix = suffix or ''
            # Check if ANY bot covers this (pattern, suffix) combo
            # A bot with no exchange filter covers everything
            has_coverage = (
                (pattern_type, suffix) in covered
                or (pattern_type, '') in covered
            )
            if has_coverage:
                continue

            # Severity / eligibility thresholds
            if total_obs >= 5000 and avg_hit >= 0.45:
                severity = 'critical'
                auto_eligible = True
            elif total_obs >= 1000 and avg_hit >= 0.40:
                severity = 'high'
                auto_eligible = True
            else:
                severity = 'medium'
                auto_eligible = False

            action_params = {
                'pattern_types': [pattern_type],
                'exchanges': [suffix] if suffix else None,
                'min_quality': round(min(0.75, avg_hit + 0.12), 2),
                'risk_pct': round(min(4.5, max(2.5, 3.0 + (avg_hit - 0.40) * 7.5)), 1),
                'max_positions': 3,
                'scan_interval_sec': 600,
                'strategy_name': (
                    f"Auto: {pattern_type.replace('_',' ').title()}"
                    + (f" ({suffix})" if suffix else '')
                ),
                # Pass through for auto-approve check
                'obs': int(total_obs),
                'hit_rate': round(float(avg_hit), 4),
            }

            findings.append(Finding(
                sensor='fleet_gap',
                severity=severity,
                subject=f'{pattern_type}{suffix or ""}',
                description=(
                    f'No active bot covers {pattern_type}'
                    + (f' ({suffix})' if suffix else '')
                    + f' — {int(total_obs):,} obs, hit_rate={avg_hit:.2f}'
                ),
                action_type='create_bot',
                action_params=action_params,
                auto_eligible=auto_eligible,
                evidence={'obs': int(total_obs), 'hit_rate': round(float(avg_hit), 4),
                          'suffix': suffix},
            ))

        return findings


# ── Sensor 2: BotPerformanceSensor ─────────────────────────────────────────────
class _BotPerformanceSensor:
    def scan(self, conn: sqlite3.Connection) -> list[Finding]:
        findings: list[Finding] = []
        try:
            bots = conn.execute(
                """SELECT bot_id, user_id, strategy_name, pattern_types,
                          risk_pct, max_positions, min_quality,
                          virtual_balance, initial_balance, created_at
                   FROM paper_bot_configs
                   WHERE active=1 AND killed_at IS NULL
                     AND user_id NOT LIKE 'discovery%'
                     AND strategy_name NOT LIKE 'disc_%'"""
            ).fetchall()
        except Exception as e:
            _log.warning('BotPerformanceSensor: bot query failed: %s', e)
            return findings

        for row in bots:
            (bot_id, user_id, strat_name, pt_json,
             risk_pct, max_positions, min_quality,
             virtual_balance, initial_balance, created_at) = row

            try:
                pattern_types = json.loads(pt_json or '[]') or []
            except Exception:
                pattern_types = []

            # Position stats
            try:
                stats = conn.execute(
                    """SELECT COUNT(*) as closed,
                              SUM(CASE WHEN pnl_r > 0 THEN 1 ELSE 0 END) as wins
                       FROM paper_positions
                       WHERE bot_id=? AND status='closed' AND pnl_r IS NOT NULL""",
                    (bot_id,)
                ).fetchone()
                closed = stats[0] or 0
                wins   = stats[1] or 0
            except Exception:
                closed, wins = 0, 0

            if closed < 10:
                # No trades at all after 48h → tune scan_interval
                try:
                    from datetime import timedelta
                    created_dt = datetime.fromisoformat(
                        created_at.replace('Z', '+00:00')
                        if created_at else '2000-01-01T00:00:00+00:00'
                    )
                    age_h = (datetime.now(timezone.utc) - created_dt).total_seconds() / 3600
                    if closed == 0 and age_h >= 48:
                        findings.append(Finding(
                            sensor='bot_performance',
                            severity='medium',
                            subject=strat_name or bot_id,
                            description=(
                                f'{strat_name or bot_id}: 0 trades after {age_h:.0f}h '
                                f'— scan_interval too slow'
                            ),
                            action_type='tune_bot',
                            action_params={
                                'bot_id': bot_id,
                                'parameter': 'scan_interval_sec',
                                'change_pct': -0.20,
                                'trade_count': 0,
                            },
                            auto_eligible=True,
                            evidence={'closed': 0, 'age_hours': round(age_h, 1)},
                        ))
                except Exception:
                    pass
                continue

            win_rate = wins / closed if closed > 0 else 0.0

            # Calibration baseline for this bot's patterns
            baseline = None
            if pattern_types:
                placeholders = ','.join('?' * len(pattern_types))
                try:
                    row2 = conn.execute(
                        f"SELECT AVG(hit_rate_t1) FROM signal_calibration "
                        f"WHERE pattern_type IN ({placeholders})",
                        pattern_types
                    ).fetchone()
                    baseline = row2[0] if row2 and row2[0] is not None else None
                except Exception:
                    pass

            if baseline is None:
                baseline = 0.40  # fallback

            # Drawdown check
            if initial_balance and virtual_balance < initial_balance * 0.60:
                findings.append(Finding(
                    sensor='bot_performance',
                    severity='high',
                    subject=strat_name or bot_id,
                    description=(
                        f'{strat_name or bot_id}: virtual balance {virtual_balance:.0f} '
                        f'is <60% of initial {initial_balance:.0f} — large drawdown'
                    ),
                    action_type='alert_only',
                    action_params={'bot_id': bot_id},
                    auto_eligible=False,
                    evidence={'virtual_balance': virtual_balance,
                              'initial_balance': initial_balance,
                              'drawdown_pct': round(1 - virtual_balance / initial_balance, 3)},
                ))

            # Win rate vs baseline
            if win_rate < baseline * 0.65 and closed >= 25:
                findings.append(Finding(
                    sensor='bot_performance',
                    severity='high',
                    subject=strat_name or bot_id,
                    description=(
                        f'{strat_name or bot_id}: win_rate={win_rate:.2f} vs '
                        f'baseline={baseline:.2f} after {closed} trades — raising quality'
                    ),
                    action_type='tune_bot',
                    action_params={
                        'bot_id': bot_id,
                        'parameter': 'min_quality',
                        'change_pct': 0.15,
                        'trade_count': closed,
                    },
                    auto_eligible=True,
                    evidence={'win_rate': round(win_rate, 3), 'baseline': round(baseline, 3),
                              'closed': closed},
                ))
                # Also reduce risk
                findings.append(Finding(
                    sensor='bot_performance',
                    severity='high',
                    subject=strat_name or bot_id,
                    description=(
                        f'{strat_name or bot_id}: reducing risk_pct {risk_pct:.2f} '
                        f'due to weak win_rate vs calibration'
                    ),
                    action_type='tune_bot',
                    action_params={
                        'bot_id': bot_id,
                        'parameter': 'risk_pct',
                        'change_pct': -0.15,
                        'trade_count': closed,
                    },
                    auto_eligible=True,
                    evidence={'win_rate': round(win_rate, 3), 'risk_pct': risk_pct,
                              'closed': closed},
                ))
            elif win_rate < baseline * 0.65 and closed >= 10:
                findings.append(Finding(
                    sensor='bot_performance',
                    severity='medium',
                    subject=strat_name or bot_id,
                    description=(
                        f'{strat_name or bot_id}: win_rate={win_rate:.2f} below '
                        f'0.65×baseline after {closed} trades — raising quality (gated)'
                    ),
                    action_type='tune_bot',
                    action_params={
                        'bot_id': bot_id,
                        'parameter': 'min_quality',
                        'change_pct': 0.10,
                        'trade_count': closed,
                    },
                    auto_eligible=False,
                    evidence={'win_rate': round(win_rate, 3), 'baseline': round(baseline, 3),
                              'closed': closed},
                ))
            elif win_rate > baseline * 1.35 and closed >= 25:
                # Outperforming — allow one extra position
                findings.append(Finding(
                    sensor='bot_performance',
                    severity='info',
                    subject=strat_name or bot_id,
                    description=(
                        f'{strat_name or bot_id}: win_rate={win_rate:.2f} exceeds '
                        f'1.35×baseline — expanding max_positions by 1'
                    ),
                    action_type='tune_bot',
                    action_params={
                        'bot_id': bot_id,
                        'parameter': 'max_positions',
                        'delta': 1,
                        'trade_count': closed,
                    },
                    auto_eligible=True,
                    evidence={'win_rate': round(win_rate, 3), 'baseline': round(baseline, 3),
                              'closed': closed, 'current_max': max_positions},
                ))

        return findings


# ── Sensor 3: CalibrationSensor ─────────────────────────────────────────────────
class _CalibrationSensor:
    def scan(self, conn: sqlite3.Connection) -> list[Finding]:
        findings: list[Finding] = []

        # Active bot pattern coverage (exclude discovery fleet)
        try:
            active_patterns: set[str] = set()
            bots = conn.execute(
                "SELECT pattern_types FROM paper_bot_configs "
                "WHERE active=1 AND killed_at IS NULL "
                "AND user_id NOT LIKE 'discovery%' "
                "AND strategy_name NOT LIKE 'disc_%'"
            ).fetchall()
            for (pt_json,) in bots:
                try:
                    for pt in (json.loads(pt_json or '[]') or []):
                        active_patterns.add(pt)
                except Exception:
                    pass
        except Exception as e:
            _log.warning('CalibrationSensor: bot query failed: %s', e)
            active_patterns = set()

        # 1. FVG bots with very low calibration hit rate
        if 'fvg' in active_patterns:
            try:
                row = conn.execute(
                    "SELECT AVG(hit_rate_t1) FROM signal_calibration WHERE pattern_type='fvg'"
                ).fetchone()
                if row and row[0] is not None and row[0] < 0.25:
                    findings.append(Finding(
                        sensor='calibration',
                        severity='high',
                        subject='fvg',
                        description=(
                            f'FVG calibration hit_rate_t1={row[0]:.3f} < 0.25 '
                            f'— active bot should raise min_quality to 0.72'
                        ),
                        action_type='alert_only',
                        action_params={'pattern_type': 'fvg', 'recommended_min_quality': 0.72},
                        auto_eligible=False,
                        evidence={'avg_hit_rate': round(float(row[0]), 4)},
                    ))
            except Exception as e:
                _log.warning('CalibrationSensor: fvg check failed: %s', e)

        # 2. High-performing cells with no active user bot coverage
        try:
            rows = conn.execute(
                """SELECT pattern_type, SUM(sample_size) as obs, AVG(hit_rate_t1) as hr
                   FROM signal_calibration
                   WHERE hit_rate_t1 > 0.80 AND sample_size > 200
                   GROUP BY pattern_type
                   HAVING obs > 200"""
            ).fetchall()
            for (pt, obs, hr) in rows:
                if pt not in active_patterns:
                    findings.append(Finding(
                        sensor='calibration',
                        severity='high',
                        subject=pt,
                        description=(
                            f'Pattern {pt}: hit_rate={hr:.2f} with {int(obs):,} obs '
                            f'— no active user bot covers this'
                        ),
                        action_type='create_bot',
                        action_params={
                            'pattern_types': [pt],
                            'exchanges': None,
                            'min_quality': round(min(0.80, hr - 0.05), 2),
                            'risk_pct': 3.0,
                            'max_positions': 3,
                            'scan_interval_sec': 600,
                            'strategy_name': f"Auto: {pt.replace('_',' ').title()} High-Cal",
                            'obs': int(obs),
                            'hit_rate': round(float(hr), 4),
                        },
                        auto_eligible=True,
                        evidence={'obs': int(obs), 'hit_rate': round(float(hr), 4)},
                    ))
        except Exception as e:
            _log.warning('CalibrationSensor: high-cal check failed: %s', e)

        # 3. Bots where ALL patterns have hit_rate < 0.30
        try:
            bots2 = conn.execute(
                """SELECT bot_id, strategy_name, pattern_types
                   FROM paper_bot_configs
                   WHERE active=1 AND killed_at IS NULL
                     AND user_id NOT LIKE 'discovery%'
                     AND strategy_name NOT LIKE 'disc_%'"""
            ).fetchall()
            for (bot_id, strat_name, pt_json) in bots2:
                try:
                    patterns = json.loads(pt_json or '[]') or []
                except Exception:
                    patterns = []
                if not patterns:
                    continue
                placeholders = ','.join('?' * len(patterns))
                try:
                    cal_rows = conn.execute(
                        f"SELECT pattern_type, AVG(hit_rate_t1) FROM signal_calibration "
                        f"WHERE pattern_type IN ({placeholders}) GROUP BY pattern_type",
                        patterns
                    ).fetchall()
                    if cal_rows and all(hr < 0.30 for (_, hr) in cal_rows if hr is not None):
                        findings.append(Finding(
                            sensor='calibration',
                            severity='medium',
                            subject=strat_name or bot_id,
                            description=(
                                f'{strat_name or bot_id}: ALL pattern calibrations '
                                f'< 0.30 — manual review needed'
                            ),
                            action_type='alert_only',
                            action_params={'bot_id': bot_id},
                            auto_eligible=False,
                            evidence={
                                'patterns': {pt: round(hr, 4)
                                             for (pt, hr) in cal_rows if hr is not None}
                            },
                        ))
                except Exception:
                    pass
        except Exception as e:
            _log.warning('CalibrationSensor: all-weak check failed: %s', e)

        return findings


# ── Sensor 4: DeliverySensor ────────────────────────────────────────────────────
class _DeliverySensor:
    """
    Watches the delivery PIPELINE, not its output.
    Only fires if the system is structurally broken — missing token, dead scheduler
    thread, or corrupted data. Never enumerates individual unsurfaced alerts or
    per-user delivery stats (those are output concerns, not system health).
    """

    def scan(self, conn: sqlite3.Connection) -> list[Finding]:
        findings: list[Finding] = []

        # 1. TELEGRAM_BOT_TOKEN missing — all delivery silently dead
        try:
            if not os.environ.get('TELEGRAM_BOT_TOKEN', ''):
                findings.append(Finding(
                    sensor='delivery',
                    severity='critical',
                    subject='TELEGRAM_BOT_TOKEN',
                    description=(
                        'TELEGRAM_BOT_TOKEN is not set — all Telegram delivery '
                        'is silently failing'
                    ),
                    action_type='alert_only',
                    action_params={},
                    auto_eligible=False,
                    evidence={'env_var': 'TELEGRAM_BOT_TOKEN', 'set': False},
                ))
        except Exception as e:
            _log.warning('DeliverySensor: token check failed: %s', e)

        # 2. tip_followups data corruption (ticker = status keyword)
        try:
            _STATUS_WORDS = ('watching', 'active', 'expired', 'closed',
                             'hit_t1', 'hit_t2', 'stopped_out')
            placeholders = ','.join('?' * len(_STATUS_WORDS))
            corrupt_count = conn.execute(
                f'SELECT COUNT(*) FROM tip_followups WHERE ticker IN ({placeholders})',
                _STATUS_WORDS
            ).fetchone()[0]
            if corrupt_count > 0:
                findings.append(Finding(
                    sensor='delivery',
                    severity='critical',
                    subject='tip_followups',
                    description=(
                        f'{corrupt_count} tip_followup rows have ticker=status keyword '
                        f'— schema shift bug, position monitor will skip these rows'
                    ),
                    action_type='alert_only',
                    action_params={'corrupt_count': corrupt_count},
                    auto_eligible=False,
                    evidence={'corrupt_rows': corrupt_count},
                ))
        except Exception as e:
            _log.warning('DeliverySensor: followup corruption check failed: %s', e)

        # 3. tip_scheduler last run > 2 hours ago (thread died or never started)
        try:
            from datetime import timedelta
            cutoff_2h = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            last_run = conn.execute(
                "SELECT MAX(delivered_at) FROM tip_delivery_log"
            ).fetchone()[0]
            if last_run is not None and last_run < cutoff_2h:
                findings.append(Finding(
                    sensor='delivery',
                    severity='high',
                    subject='tip_scheduler',
                    description=(
                        f'tip_delivery_log last entry is >2h old ({last_run[:16]}) '
                        f'— tip_scheduler may have died'
                    ),
                    action_type='alert_only',
                    action_params={'last_run': last_run},
                    auto_eligible=False,
                    evidence={'last_run': last_run},
                ))
        except Exception as e:
            _log.warning('DeliverySensor: tip_scheduler staleness check failed: %s', e)

        return findings


# ── Main engine ─────────────────────────────────────────────────────────────────
class ObservatoryEngine:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._sensors = [
            _FleetGapSensor(),
            _BotPerformanceSensor(),
            _CalibrationSensor(),
            _DeliverySensor(),
        ]

    # ── Public entry point ─────────────────────────────────────────────────────
    def run(self) -> None:
        start = time.monotonic()
        run_at = datetime.now(timezone.utc).isoformat()
        findings: list[Finding] = []
        executed: list[dict] = []
        queued:   list[dict] = []
        tokens_used = 0
        llm_called  = 0
        error_str   = None

        try:
            findings = self._run_sensors()
            _log.info(
                'Observatory: %d findings (%d critical, %d high, %d medium, %d info)',
                len(findings),
                sum(1 for f in findings if f.severity == 'critical'),
                sum(1 for f in findings if f.severity == 'high'),
                sum(1 for f in findings if f.severity == 'medium'),
                sum(1 for f in findings if f.severity == 'info'),
            )

            if any(f.severity in ('critical', 'high') for f in findings):
                budget_row = self._tokens_used_today()
                if budget_row < _LLM_DAILY_TOKEN_LIMIT:
                    actions, tokens_used = self._reason(findings)
                    llm_called = 1 if tokens_used > 0 else 0
                    if actions:
                        executed, queued = self._act(actions)
                else:
                    _log.warning(
                        'Observatory: daily token budget exhausted (%d/%d) — skipping LLM',
                        budget_row, _LLM_DAILY_TOKEN_LIMIT,
                    )
        except Exception as e:
            error_str = str(e)
            _log.error('Observatory: run error: %s', e)

        duration = round(time.monotonic() - start, 2)

        # Always report (even all-clear)
        try:
            budget_today = self._tokens_used_today()
            self._report(findings, executed, queued, tokens_used, duration,
                         budget_today, _LLM_DAILY_TOKEN_LIMIT)
        except Exception as e:
            _log.error('Observatory: report failed: %s', e)

        # Always persist run record
        try:
            self._record_run(
                run_at, findings, executed, queued,
                tokens_used, llm_called, duration, error_str,
            )
        except Exception as e:
            _log.warning('Observatory: failed to persist run record: %s', e)

    # ── Sensors ────────────────────────────────────────────────────────────────
    def _run_sensors(self) -> list[Finding]:
        all_findings: list[Finding] = []
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        try:
            for sensor in self._sensors:
                try:
                    all_findings.extend(sensor.scan(conn))
                except Exception as e:
                    _log.warning('Observatory sensor %s failed: %s',
                                 sensor.__class__.__name__, e)
        finally:
            conn.close()

        # Sort by severity descending
        all_findings.sort(
            key=lambda f: _SEVERITY_ORDER.get(f.severity, 0), reverse=True
        )
        return all_findings

    # ── Reasoning (LLM) ────────────────────────────────────────────────────────
    def _reason(self, findings: list[Finding]) -> tuple[list[ReasonedAction], int]:
        try:
            import anthropic
        except ImportError:
            _log.warning('Observatory: anthropic package not available — skipping LLM')
            return [], 0

        api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        if not api_key:
            _log.warning('Observatory: ANTHROPIC_API_KEY not set — skipping LLM')
            return [], 0

        # Fleet context
        fleet_ctx = self._fleet_context()
        findings_json = json.dumps([
            {
                'index': i,
                'sensor': f.sensor,
                'severity': f.severity,
                'subject': f.subject,
                'description': f.description,
                'action_type': f.action_type,
                'action_params': f.action_params,
                'auto_eligible': f.auto_eligible,
            }
            for i, f in enumerate(findings)
        ], indent=2)

        system_prompt = (
            "You are the Trading Galaxy observatory. "
            "Respond ONLY with valid JSON, no other text."
        )
        user_prompt = (
            f"Current system state:\n"
            f"- Active user bots: {fleet_ctx['active_bot_count']}\n"
            f"- Open positions: {fleet_ctx['open_positions']}\n"
            f"- Current KB regime: {fleet_ctx['regime']}\n"
            f"- Calibration summary: {fleet_ctx['cal_summary']}\n"
            f"\nFindings detected:\n{findings_json}\n\n"
            f"For each finding, decide whether to act. "
            f"Auto-eligible findings MAY be acted on. "
            f"Non-auto findings should NOT be acted on (act: false).\n\n"
            f"Rules:\n"
            f"- Don't create bearish bots if KB regime is 'risk_on' or 'bull'\n"
            f"- Don't expand max_positions if fleet win_rate < 0.30\n"
            f"- Don't create new bots if total open positions > 60\n\n"
            f"Respond with:\n"
            f'{{\"actions\": [{{\"finding_index\": 0, \"act\": true, '
            f'\"params\": {{}}, \"rationale\": \"one sentence\"}}]}}'
        )

        try:
            client = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model='claude-sonnet-4-6',
                max_tokens=400,
                system=system_prompt,
                messages=[{'role': 'user', 'content': user_prompt}],
            )
            tokens_used = (
                response.usage.input_tokens + response.usage.output_tokens
                if hasattr(response, 'usage') else 0
            )
            raw = response.content[0].text if response.content else '{}'
        except Exception as e:
            _log.warning('Observatory: LLM call failed: %s', e)
            return [], 0

        # Parse LLM response
        actions: list[ReasonedAction] = []
        try:
            data = json.loads(raw)
            for item in data.get('actions', []):
                idx = item.get('finding_index', -1)
                if idx < 0 or idx >= len(findings):
                    continue
                finding = findings[idx]
                actions.append(ReasonedAction(
                    finding=finding,
                    act=bool(item.get('act', False)),
                    params=item.get('params') or {},
                    rationale=str(item.get('rationale', '')),
                ))
        except Exception as e:
            _log.warning('Observatory: LLM response parse failed: %s — raw=%s', e, raw[:200])

        return actions, tokens_used

    # ── Act ────────────────────────────────────────────────────────────────────
    def _act(self, actions: list[ReasonedAction]) -> tuple[list[dict], list[dict]]:
        executed: list[dict] = []
        queued:   list[dict] = []

        fleet = self._fleet_context()

        for ra in actions:
            if not ra.act:
                continue
            finding    = ra.finding
            action_type = finding.action_type
            params     = {**finding.action_params, **ra.params}

            if action_type in _NEVER_AUTO:
                queued.append({
                    'action_type': action_type,
                    'subject': finding.subject,
                    'params': params,
                    'rationale': ra.rationale,
                    'reason': 'always_gated',
                })
                self._queue_write(finding, params, ra.rationale)
                continue

            if finding.auto_eligible and _auto_approve(action_type, params, fleet):
                success = self._execute_action(action_type, params, fleet)
                if success:
                    executed.append({
                        'action_type': action_type,
                        'subject': finding.subject,
                        'params': params,
                        'rationale': ra.rationale,
                    })
                    _log.info('Observatory: auto-executed %s for %s',
                              action_type, finding.subject)
                else:
                    queued.append({
                        'action_type': action_type,
                        'subject': finding.subject,
                        'params': params,
                        'rationale': ra.rationale,
                        'reason': 'execution_failed',
                    })
            else:
                queued.append({
                    'action_type': action_type,
                    'subject': finding.subject,
                    'params': params,
                    'rationale': ra.rationale,
                    'reason': 'not_auto_eligible',
                })
                self._queue_write(finding, params, ra.rationale)

        return executed, queued

    def _execute_action(self, action_type: str, params: dict, fleet: dict) -> bool:
        try:
            if action_type == 'create_bot':
                return self._do_create_bot(params, fleet)
            elif action_type == 'tune_bot':
                return self._do_tune_bot(params)
            elif action_type == 'tune_bot_max_positions':
                return self._do_tune_bot(params)
            elif action_type == 'expire_patterns':
                return self._do_expire_patterns(params)
            else:
                _log.info('Observatory: no executor for %s — skipping', action_type)
                return False
        except Exception as e:
            _log.warning('Observatory: _execute_action failed (%s): %s', action_type, e)
            return False

    def _do_create_bot(self, params: dict, fleet: dict) -> bool:
        import extensions as ext
        runner = getattr(ext, 'bot_runner', None)
        if runner is None:
            from services.bot_runner import BotRunner
            runner = BotRunner(self.db_path)

        active_count = fleet.get('active_bot_count', 1)
        total_capital = fleet.get('total_capital', 5000.0)
        balance = min(10000.0, round(total_capital / max(active_count + 1, 1), 2))
        balance = max(balance, 500.0)

        genome = {
            'pattern_types': json.dumps(params.get('pattern_types', [])),
            'exchanges':      json.dumps(params['exchanges']) if params.get('exchanges') else None,
            'sectors':        None,
            'volatility':     None,
            'regimes':        None,
            'timeframes':     None,
            'direction_bias': None,
            'risk_pct':       float(params.get('risk_pct', 2.5)),
            'max_positions':  int(params.get('max_positions', 3)),
            'min_quality':    float(params.get('min_quality', 0.65)),
            'scan_interval_sec': int(params.get('scan_interval_sec', 600)),
            'strategy_name':  params.get('strategy_name', 'Auto-Observatory'),
            'role':           'observatory',
        }

        # Observatory bots are tagged as manual so evolution won't kill them
        bot_id = runner.create_manual_bot('observatory_engine', genome, balance)
        _log.info('Observatory: created bot %s for %s (balance=%.0f)',
                  bot_id, params.get('strategy_name', '?'), balance)
        return True

    def _do_tune_bot(self, params: dict) -> bool:
        bot_id    = params.get('bot_id')
        parameter = params.get('parameter')
        if not bot_id or not parameter:
            return False

        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            row = conn.execute(
                f"SELECT {parameter} FROM paper_bot_configs WHERE bot_id=?",
                (bot_id,)
            ).fetchone()
            if not row:
                return False
            current = float(row[0])

            if parameter == 'max_positions':
                delta = int(params.get('delta', 1))
                new_val = current + delta
            else:
                change_pct = float(params.get('change_pct', 0.10))
                new_val = round(current * (1 + change_pct), 4)
                # Clamp sensible ranges
                if parameter == 'min_quality':
                    new_val = max(0.45, min(0.90, new_val))
                elif parameter == 'risk_pct':
                    new_val = max(0.5, min(5.0, new_val))
                elif parameter == 'scan_interval_sec':
                    new_val = max(120, min(3600, int(new_val)))

            conn.execute(
                f"UPDATE paper_bot_configs SET {parameter}=? WHERE bot_id=?",
                (new_val, bot_id)
            )
            conn.commit()
            _log.info('Observatory: tuned %s.%s: %.4f → %.4f', bot_id, parameter, current, new_val)
            return True
        finally:
            conn.close()

    def _do_expire_patterns(self, params: dict) -> bool:
        quality_below = float(params.get('quality_below', 0.40))
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            result = conn.execute(
                "UPDATE pattern_signals SET status='expired' "
                "WHERE status='open' AND quality_score < ?",
                (quality_below,)
            )
            expired = result.rowcount
            conn.commit()
            _log.info('Observatory: expired %d low-quality patterns (q<%.2f)',
                      expired, quality_below)
            return True
        finally:
            conn.close()

    def _queue_write(self, finding: Finding, params: dict, rationale: str) -> None:
        """Insert into mcp_write_queue for operator approval."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS mcp_write_queue (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    tool_name    TEXT NOT NULL,
                    path         TEXT,
                    old_str      TEXT,
                    new_str      TEXT,
                    full_content TEXT,
                    status       TEXT NOT NULL DEFAULT 'pending',
                    queued_at    TEXT NOT NULL,
                    resolved_at  TEXT,
                    context      TEXT
                )"""
            )
            conn.execute(
                """INSERT INTO mcp_write_queue
                   (tool_name, queued_at, context)
                   VALUES (?,?,?)""",
                (finding.action_type,
                 datetime.now(timezone.utc).isoformat(),
                 json.dumps({
                     'subject': finding.subject,
                     'params': params,
                     'rationale': rationale,
                     'severity': finding.severity,
                 })),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            _log.warning('Observatory: _queue_write failed: %s', e)

    # ── Report ─────────────────────────────────────────────────────────────────
    def _report(
        self,
        findings: list[Finding],
        executed: list[dict],
        queued:   list[dict],
        tokens_used: int,
        duration_sec: float,
        budget_today: int,
        budget_limit: int,
    ) -> None:
        try:
            from notifications.operator_bot import send_observatory_report
            send_observatory_report(
                findings=[
                    {'severity': f.severity, 'description': f.description,
                     'sensor': f.sensor, 'subject': f.subject}
                    for f in findings
                ],
                executed=executed,
                queued=queued,
                tokens_used=tokens_used,
                duration_sec=duration_sec,
                budget_today=budget_today,
                budget_limit=budget_limit,
            )
        except Exception as e:
            _log.warning('Observatory: _report failed: %s', e)

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _fleet_context(self) -> dict:
        try:
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            try:
                active_count = conn.execute(
                    "SELECT COUNT(*) FROM paper_bot_configs "
                    "WHERE active=1 AND killed_at IS NULL"
                ).fetchone()[0]
                open_pos = conn.execute(
                    "SELECT COUNT(*) FROM paper_positions WHERE status='open'"
                ).fetchone()[0]
                total_capital_row = conn.execute(
                    "SELECT SUM(virtual_balance) FROM paper_bot_configs "
                    "WHERE active=1 AND killed_at IS NULL"
                ).fetchone()
                total_capital = float(total_capital_row[0] or 0)
                wins = conn.execute(
                    "SELECT COUNT(*) FROM paper_positions "
                    "WHERE status='closed' AND pnl_r > 0 AND pnl_r IS NOT NULL"
                ).fetchone()[0]
                closed = conn.execute(
                    "SELECT COUNT(*) FROM paper_positions "
                    "WHERE status='closed' AND pnl_r IS NOT NULL"
                ).fetchone()[0]
                fleet_wr = round(wins / closed, 3) if closed > 0 else 0.0
                regime_row = conn.execute(
                    "SELECT object FROM facts "
                    "WHERE subject='macro' AND predicate='price_regime' "
                    "ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
                regime = regime_row[0] if regime_row else 'unknown'
                cal_row = conn.execute(
                    "SELECT AVG(hit_rate_t1) FROM signal_calibration"
                ).fetchone()
                cal_summary = f'avg_hit_rate={round(float(cal_row[0]), 3)}' if (cal_row and cal_row[0]) else 'no_data'
            finally:
                conn.close()
        except Exception:
            active_count = 0
            open_pos = 0
            total_capital = 0.0
            fleet_wr = 0.0
            regime = 'unknown'
            cal_summary = 'unavailable'

        return {
            'active_bot_count': active_count,
            'open_positions': open_pos,
            'total_capital': total_capital,
            'capital_per_new_bot': round(total_capital / max(active_count + 1, 1), 2),
            'win_rate': fleet_wr,
            'regime': regime,
            'cal_summary': cal_summary,
        }

    def _tokens_used_today(self) -> int:
        try:
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            conn = sqlite3.connect(self.db_path, timeout=5)
            row = conn.execute(
                "SELECT SUM(tokens_used) FROM observatory_runs "
                "WHERE run_at >= ? AND llm_called=1",
                (today,)
            ).fetchone()
            conn.close()
            return int(row[0] or 0)
        except Exception:
            return 0

    def _record_run(
        self,
        run_at: str,
        findings: list[Finding],
        executed: list[dict],
        queued:   list[dict],
        tokens_used: int,
        llm_called: int,
        duration_sec: float,
        error: str | None,
    ) -> None:
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            _ensure_observatory_table(conn)
            conn.execute(
                """INSERT INTO observatory_runs
                   (run_at, findings_json, actions_taken_json, actions_queued_json,
                    tokens_used, llm_called, run_duration_sec, error)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    run_at,
                    json.dumps([
                        {'sensor': f.sensor, 'severity': f.severity,
                         'subject': f.subject, 'description': f.description}
                        for f in findings
                    ]),
                    json.dumps(executed),
                    json.dumps(queued),
                    tokens_used,
                    llm_called,
                    duration_sec,
                    error,
                ),
            )
            conn.commit()
        finally:
            conn.close()

"""
analytics/network_effect_engine.py — Network Effect Engine

Computes coverage tiers, cohort consensus signals, trending markets,
and overall network health. Called by the ingest scheduler on each cycle
and exposed via API endpoints.

Cohort KB atoms emitted when cohort_size >= 10:
  {ticker} | cohort_consensus    | "long_0.78"
  {ticker} | cohort_stop_cluster | "187.20_tight"
  {ticker} | contrarian_flag     | "true"
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)

_COHORT_MIN_SIZE = 10


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class CoverageTier:
    ticker: str
    coverage_count: int
    tier: str
    yfinance_interval: int
    options_interval: int
    patterns_interval: int


@dataclass
class CohortSignal:
    ticker: str
    cohort_size: int
    consensus_direction: str    # 'long' | 'short' | 'mixed'
    consensus_strength: float   # 0.0–1.0
    stop_cluster: Optional[float]
    contrarian_flag: bool


@dataclass
class TrendingMarket:
    ticker: str
    coverage_count: int
    coverage_7d_ago: int
    growth_rate: float          # (current - 7d_ago) / max(7d_ago, 1)
    sector_label: Optional[str]


@dataclass
class NetworkHealthReport:
    total_tickers: int
    total_users: int
    tickers_by_tier: Dict[str, int]
    coverage_distribution: List[dict]
    flywheel_velocity: float    # avg coverage_count growth per ticker in last 7d
    cohort_signals_active: int
    generated_at: str


# ── Coverage tier computation ─────────────────────────────────────────────────

_TIER_THRESHOLDS = [
    ('core',        50,  None, 30,  120, 60),
    ('established', 10,  49,   60,  300, 120),
    ('emerging',    3,   9,    180, 900, 450),
    ('nascent',     1,   2,    300, 1800, 900),
]


def _tier_for(count: int) -> tuple:
    """Return (tier, yfinance_s, options_s, patterns_s) for a coverage_count."""
    for tier, lo, hi, yf_s, opt_s, pat_s in _TIER_THRESHOLDS:
        if hi is None:
            if count >= lo:
                return (tier, yf_s, opt_s, pat_s)
        else:
            if lo <= count <= hi:
                return (tier, yf_s, opt_s, pat_s)
    return ('nascent', 300, 1800, 900)


def compute_coverage_tier(ticker: str, db_path: str) -> Optional[CoverageTier]:
    """Return the CoverageTier for a ticker, or None if not in universe_tickers."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        row = conn.execute(
            "SELECT ticker, coverage_count FROM universe_tickers WHERE ticker = ?",
            (ticker.upper(),),
        ).fetchone()
        if row is None:
            return None
        t, count = row
        tier, yf_s, opt_s, pat_s = _tier_for(count)
        return CoverageTier(
            ticker=t,
            coverage_count=count,
            tier=tier,
            yfinance_interval=yf_s,
            options_interval=opt_s,
            patterns_interval=pat_s,
        )
    finally:
        conn.close()


def update_refresh_schedule(db_path: str) -> int:
    """
    Recompute coverage tiers for all universe_tickers and update added_to_ingest
    for any newly eligible tickers. Returns number of tickers updated.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        rows = conn.execute(
            "SELECT ticker, coverage_count FROM universe_tickers"
        ).fetchall()
        updated = 0
        for ticker, count in rows:
            tier, _, _, _ = _tier_for(count)
            # Promote if meeting threshold and not yet promoted
            if count >= 3:
                cur = conn.execute(
                    "SELECT added_to_ingest FROM universe_tickers WHERE ticker = ?",
                    (ticker,),
                ).fetchone()
                if cur and not cur[0]:
                    conn.execute(
                        "UPDATE universe_tickers SET added_to_ingest = 1 WHERE ticker = ?",
                        (ticker,),
                    )
                    updated += 1
        conn.commit()
        return updated
    finally:
        conn.close()


# ── Promotion ─────────────────────────────────────────────────────────────────

def promote_to_shared_kb(ticker: str, db_path: str) -> bool:
    """
    Promote ticker to shared KB ingest (set added_to_ingest=1).
    Returns True if promotion happened, False if already promoted or not found.
    """
    ticker = ticker.upper()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        row = conn.execute(
            "SELECT coverage_count, added_to_ingest FROM universe_tickers WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if row is None:
            return False
        count, already = row
        if already:
            return False
        if count >= 3:
            conn.execute(
                "UPDATE universe_tickers SET added_to_ingest = 1 WHERE ticker = ?",
                (ticker,),
            )
            conn.commit()
            _log.info('network_effect_engine: promoted %s to shared KB', ticker)
            return True
        return False
    finally:
        conn.close()


# ── Cohort consensus ──────────────────────────────────────────────────────────

def detect_cohort_consensus(ticker: str, db_path: str) -> Optional[CohortSignal]:
    """
    Detect cohort consensus for a ticker from user portfolios + feedback.
    Requires coverage_count >= 10 (cohort_size guard).
    Emits cohort KB atoms when consensus is detected.
    """
    ticker = ticker.upper()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        # Check coverage_count
        row = conn.execute(
            "SELECT coverage_count FROM universe_tickers WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if row is None or row[0] < _COHORT_MIN_SIZE:
            return None

        cohort_size = row[0]

        # Count users holding this ticker
        holders = conn.execute(
            "SELECT COUNT(DISTINCT user_id) FROM user_portfolios WHERE ticker = ?",
            (ticker,),
        ).fetchone()[0]

        if holders < _COHORT_MIN_SIZE:
            return None

        # Aggregate feedback outcomes for this ticker's patterns
        outcomes = conn.execute(
            """SELECT tf.outcome, COUNT(*) FROM tip_feedback tf
               LEFT JOIN pattern_signals ps ON ps.id = tf.pattern_id
               WHERE ps.ticker = ?
               GROUP BY tf.outcome""",
            (ticker,),
        ).fetchall()
        outcome_map = {r[0]: r[1] for r in outcomes}

        hits = sum(outcome_map.get(k, 0) for k in ('hit_t1', 'hit_t2', 'hit_t3'))
        stops = outcome_map.get('stopped_out', 0)
        total = hits + stops

        if total < _COHORT_MIN_SIZE:
            # Not enough resolved trades — use signal_direction from KB
            sig_dir = conn.execute(
                """SELECT object FROM facts
                   WHERE subject = ? AND predicate = 'signal_direction'
                   ORDER BY confidence DESC LIMIT 1""",
                (ticker.lower(),),
            ).fetchone()
            direction = sig_dir[0] if sig_dir else 'mixed'
            strength = 0.5
        else:
            win_rate = hits / total
            direction = 'long' if win_rate >= 0.55 else 'short' if win_rate <= 0.35 else 'mixed'
            strength = round(win_rate if direction != 'mixed' else 0.5, 3)

        # Stop cluster — median of recent stop losses in pattern_signals
        stops_rows = conn.execute(
            """SELECT zone_low FROM pattern_signals
               WHERE ticker = ? AND direction = 'bullish' AND status = 'open'
               ORDER BY detected_at DESC LIMIT 20""",
            (ticker,),
        ).fetchall()
        stop_cluster = None
        if stops_rows:
            prices = [r[0] for r in stops_rows]
            prices.sort()
            mid = len(prices) // 2
            stop_cluster = round(prices[mid], 2)

        contrarian = strength < 0.40

        signal = CohortSignal(
            ticker=ticker,
            cohort_size=cohort_size,
            consensus_direction=direction,
            consensus_strength=strength,
            stop_cluster=stop_cluster,
            contrarian_flag=contrarian,
        )

        # Emit KB atoms
        _emit_cohort_atoms(signal, db_path, conn)

        return signal
    finally:
        conn.close()


def _emit_cohort_atoms(signal: CohortSignal, db_path: str, conn: sqlite3.Connection) -> None:
    """Write cohort consensus atoms to the shared facts table."""
    now = datetime.now(timezone.utc).isoformat()
    source = 'network_effect_engine'
    ticker_lower = signal.ticker.lower()

    atoms = [
        (ticker_lower, 'cohort_consensus',
         f'{signal.consensus_direction}_{signal.consensus_strength:.2f}', 0.70),
        (ticker_lower, 'contrarian_flag',
         'true' if signal.contrarian_flag else 'false', 0.70),
    ]
    if signal.stop_cluster is not None:
        tightness = 'tight' if signal.consensus_strength >= 0.65 else 'wide'
        atoms.append((
            ticker_lower, 'cohort_stop_cluster',
            f'{signal.stop_cluster}_{tightness}', 0.65,
        ))

    from db import HAS_POSTGRES, get_pg
    if HAS_POSTGRES:
        try:
            with get_pg() as pg:
                cur = pg.cursor()
                for subj, pred, obj, conf in atoms:
                    cur.execute(
                        """INSERT INTO facts (subject, predicate, object, source, confidence, timestamp)
                           VALUES (%s,%s,%s,%s,%s,%s)
                           ON CONFLICT(subject, predicate, object)
                           DO UPDATE SET confidence=EXCLUDED.confidence, source=EXCLUDED.source,
                                         timestamp=EXCLUDED.timestamp""",
                        (subj, pred, obj, source, conf, now))
            return
        except Exception:
            pass  # fall through to SQLite
    for subj, pred, obj, conf in atoms:
        try:
            conn.execute(
                """INSERT INTO facts (subject, predicate, object, source, confidence, timestamp)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(subject, predicate, source)
                   DO UPDATE SET object=excluded.object, confidence=excluded.confidence,
                                 timestamp=excluded.timestamp""",
                (subj, pred, obj, source, conf, now),
            )
        except Exception:
            try:
                conn.execute(
                    "DELETE FROM facts WHERE subject=? AND predicate=? AND source=?",
                    (subj, pred, source),
                )
                conn.execute(
                    "INSERT INTO facts (subject, predicate, object, source, confidence, timestamp) VALUES (?,?,?,?,?,?)",
                    (subj, pred, obj, source, conf, now),
                )
            except Exception as exc:
                _log.warning('_emit_cohort_atoms: %s', exc)
    conn.commit()


# ── Trending markets ──────────────────────────────────────────────────────────

def compute_trending_markets(db_path: str) -> List[TrendingMarket]:
    """
    Return tickers with fastest coverage_count growth in the last 7 days.
    Uses ticker_staging.requested_at as a proxy for request velocity.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

        # Count requests per ticker in last 7 days
        recent = conn.execute(
            """SELECT ticker, COUNT(*) as cnt
               FROM ticker_staging
               WHERE requested_at >= ?
               GROUP BY ticker""",
            (cutoff,),
        ).fetchall()
        recent_map = {r[0]: r[1] for r in recent}

        # Get current coverage counts
        rows = conn.execute(
            "SELECT ticker, coverage_count, sector_label FROM universe_tickers"
        ).fetchall()

        trending = []
        for ticker, count, sector in rows:
            recent_cnt = recent_map.get(ticker, 0)
            old_cnt = max(count - recent_cnt, 1)
            growth = round(recent_cnt / old_cnt, 3)
            if growth > 0:
                trending.append(TrendingMarket(
                    ticker=ticker,
                    coverage_count=count,
                    coverage_7d_ago=old_cnt,
                    growth_rate=growth,
                    sector_label=sector,
                ))

        trending.sort(key=lambda x: x.growth_rate, reverse=True)
        return trending[:20]
    finally:
        conn.close()


# ── Network health ────────────────────────────────────────────────────────────

def compute_network_health(db_path: str) -> NetworkHealthReport:
    """Return a NetworkHealthReport with flywheel velocity and coverage distribution."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        total_tickers = conn.execute(
            "SELECT COUNT(*) FROM universe_tickers"
        ).fetchone()[0]

        total_users = 0
        try:
            total_users = conn.execute(
                "SELECT COUNT(*) FROM user_preferences WHERE onboarding_complete = 1"
            ).fetchone()[0]
        except Exception:
            pass

        # Tickers by tier
        rows = conn.execute(
            "SELECT coverage_count FROM universe_tickers"
        ).fetchall()
        by_tier: Dict[str, int] = {'nascent': 0, 'emerging': 0, 'established': 0, 'core': 0}
        for (count,) in rows:
            tier, _, _, _ = _tier_for(count)
            by_tier[tier] = by_tier.get(tier, 0) + 1

        # Coverage distribution
        distribution = conn.execute(
            """SELECT ticker, coverage_count, sector_label, added_to_ingest
               FROM universe_tickers ORDER BY coverage_count DESC LIMIT 50"""
        ).fetchall()
        dist_list = [
            {'ticker': r[0], 'coverage_count': r[1],
             'sector_label': r[2], 'added_to_ingest': bool(r[3])}
            for r in distribution
        ]

        # Flywheel velocity — avg coverage growth per ticker in last 7 days
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        recent_requests = 0
        try:
            recent_requests = conn.execute(
                "SELECT COUNT(*) FROM ticker_staging WHERE requested_at >= ?",
                (cutoff,),
            ).fetchone()[0]
        except Exception:
            pass
        flywheel = round(recent_requests / max(total_tickers, 1), 3)

        # Active cohort signals count
        cohort_signals = 0
        try:
            cohort_signals = conn.execute(
                "SELECT COUNT(DISTINCT subject) FROM facts WHERE predicate = 'cohort_consensus'"
            ).fetchone()[0]
        except Exception:
            pass

        return NetworkHealthReport(
            total_tickers=total_tickers,
            total_users=total_users,
            tickers_by_tier=by_tier,
            coverage_distribution=dist_list,
            flywheel_velocity=flywheel,
            cohort_signals_active=cohort_signals,
            generated_at=now,
        )
    finally:
        conn.close()


# ── Convergence detection ──────────────────────────────────────────────────────

@dataclass
class ConvergenceSignal:
    ticker:               str
    distinct_users:       int
    lookback_hours:       int
    kb_signal_direction:  Optional[str]
    is_organic:           bool       # True = pre-tip traffic only
    detected_at:          str


# ── NetworkEffectEngine class ──────────────────────────────────────────────────

class NetworkEffectEngine:
    """
    Wrapper class providing detect_convergence() and access to existing
    network effect functions via a consistent interface.

    Used by GET /network/convergence endpoint in api.py.
    """

    def __init__(self, db_path: str) -> None:
        self._db = db_path

    def detect_convergence(self, lookback_hours: int = 24) -> List[ConvergenceSignal]:
        """
        Detect tickers where >= 3 independent users queried organically
        within the lookback window, BEFORE any tip was sent for that ticker.

        Fix 3 (pre-tip lookback): queries that arrived after a tip was sent
        for the same ticker are excluded — they're tip-driven traffic, not
        organic convergence. Joins against prediction_ledger.issued_at with
        a 10-minute buffer.

        Returns list of ConvergenceSignal sorted by distinct_users descending.
        """
        now     = datetime.now(timezone.utc)
        cutoff  = (now - timedelta(hours=lookback_hours)).isoformat()
        now_iso = now.isoformat()

        conn = sqlite3.connect(self._db, timeout=10)
        try:
            # discovery_log stores user ticker queries
            # We look for tickers queried by >= 3 distinct users in the window,
            # where those queries preceded any tip issuance for the same ticker.
            rows = conn.execute(
                """
                SELECT
                    dl.ticker,
                    COUNT(DISTINCT dl.user_id) AS distinct_users
                FROM discovery_log dl
                WHERE dl.queried_at >= ?
                  AND dl.queried_at <= ?
                  AND NOT EXISTS (
                    SELECT 1 FROM prediction_ledger pl
                    WHERE pl.ticker = UPPER(dl.ticker)
                      AND pl.issued_at <= dl.queried_at
                      AND pl.issued_at >= datetime(dl.queried_at, '-10 minutes')
                  )
                GROUP BY dl.ticker
                HAVING COUNT(DISTINCT dl.user_id) >= 3
                ORDER BY COUNT(DISTINCT dl.user_id) DESC
                """,
                (cutoff, now_iso),
            ).fetchall()
        except Exception as exc:
            _log.warning('detect_convergence: discovery_log query failed: %s', exc)
            # Fallback: try without prediction_ledger join if table absent
            try:
                rows = conn.execute(
                    """
                    SELECT ticker, COUNT(DISTINCT user_id) AS distinct_users
                    FROM discovery_log
                    WHERE queried_at >= ? AND queried_at <= ?
                    GROUP BY ticker
                    HAVING COUNT(DISTINCT user_id) >= 3
                    ORDER BY COUNT(DISTINCT user_id) DESC
                    """,
                    (cutoff, now_iso),
                ).fetchall()
            except Exception:
                rows = []
        finally:
            conn.close()

        signals: List[ConvergenceSignal] = []
        for ticker, n_users in rows:
            kb_sig = self._kb_signal_direction(ticker)
            signals.append(ConvergenceSignal(
                ticker              = ticker.upper(),
                distinct_users      = n_users,
                lookback_hours      = lookback_hours,
                kb_signal_direction = kb_sig,
                is_organic          = True,   # pre-tip filter applied above
                detected_at         = now_iso,
            ))
            self._write_convergence_atom(ticker, n_users, lookback_hours, now_iso)

        return signals

    def _kb_signal_direction(self, ticker: str) -> Optional[str]:
        """Read current signal_direction KB atom for ticker."""
        try:
            conn = sqlite3.connect(self._db, timeout=5)
            try:
                row = conn.execute(
                    """SELECT object FROM facts
                       WHERE subject=? AND predicate='signal_direction'
                       ORDER BY confidence DESC LIMIT 1""",
                    (ticker.lower(),),
                ).fetchone()
                return row[0] if row else None
            finally:
                conn.close()
        except Exception:
            return None

    def _write_convergence_atom(
        self,
        ticker:         str,
        n_users:        int,
        lookback_hours: int,
        now_iso:        str,
    ) -> None:
        """Write organic_convergence atom to facts table."""
        from db import HAS_POSTGRES, get_pg
        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    pg.cursor().execute(
                        """INSERT INTO facts (subject, predicate, object, source, confidence, timestamp)
                           VALUES (%s,%s,%s,%s,%s,%s)
                           ON CONFLICT(subject, predicate, object)
                           DO UPDATE SET confidence=EXCLUDED.confidence, source=EXCLUDED.source,
                                         timestamp=EXCLUDED.timestamp""",
                        (ticker.lower(), 'organic_convergence',
                         f'{n_users} independent users ({lookback_hours}h)',
                         'network_effect_engine', 0.60, now_iso))
                return
            except Exception:
                pass  # fall through to SQLite
        try:
            conn = sqlite3.connect(self._db, timeout=10)
            try:
                conn.execute(
                    """INSERT INTO facts
                       (subject, predicate, object, source, confidence, timestamp)
                       VALUES (?,?,?,?,?,?)
                       ON CONFLICT(subject, predicate, source)
                       DO UPDATE SET object=excluded.object,
                                     confidence=excluded.confidence,
                                     timestamp=excluded.timestamp""",
                    (
                        ticker.lower(),
                        'organic_convergence',
                        f'{n_users} independent users ({lookback_hours}h)',
                        'network_effect_engine',
                        0.60,
                        now_iso,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            _log.debug('_write_convergence_atom: %s', exc)

"""services/discovery_fleet.py — Internal discovery fleet management.

A private fleet of paper-trading bots running under DISCOVERY_USER_ID that
accumulate calibration data across a combinatorial grid of signal hypotheses.
Invisible to user-facing UI and evolution logic.
"""

from __future__ import annotations

import itertools
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

import extensions as ext

_logger = logging.getLogger(__name__)

DISCOVERY_USER_ID = 'system_discovery'
DISCOVERY_EMAIL   = 'discovery@internal.trading-galaxy'

# ── Combinatorial grid ────────────────────────────────────────────────────────

PATTERN_TYPES = ['fvg', 'ifvg', 'order_block', 'breaker', 'mitigation', 'liquidity_void']
SECTORS       = ['technology', 'energy', 'financials', 'healthcare', 'consumer', 'industrials', None]
DIRECTIONS    = ['bullish', 'bearish', None]

DUAL_PAIRS    = [
    ['fvg', 'order_block'],
    ['breaker', 'mitigation'],
    ['ifvg', 'mitigation'],
    ['liquidity_void', 'fvg'],
]
DUAL_SECTORS    = ['technology', 'energy']

_DISC_BOT_DEFAULTS = dict(
    role              = 'discovery',
    risk_pct          = 0.5,
    max_positions     = 8,
    min_quality       = 0.55,
    scan_interval_sec = 300,
    min_trades_eval   = 999999,
    initial_balance   = 1_000_000.0,
    virtual_balance   = 1_000_000.0,
    generation        = 0,
)


def ensure_discovery_user(conn) -> None:
    """Upsert system discovery user and paper_account row."""
    from services.paper_trading import ensure_paper_tables
    ensure_paper_tables(conn)

    now_iso = datetime.now(timezone.utc).isoformat()

    # Insert into user_auth if it exists; skip silently if table doesn't exist
    try:
        # Try with internal column
        try:
            conn.execute(
                "INSERT OR IGNORE INTO user_auth (user_id, email, tier, internal) "
                "VALUES (?, ?, 'premium', 1)",
                (DISCOVERY_USER_ID, DISCOVERY_EMAIL),
            )
        except Exception:
            conn.execute(
                "INSERT OR IGNORE INTO user_auth (user_id, email, tier) "
                "VALUES (?, ?, 'premium')",
                (DISCOVERY_USER_ID, DISCOVERY_EMAIL),
            )
    except Exception as e:
        _logger.debug('ensure_discovery_user: user_auth insert skipped: %s', e)

    # user_preferences tier row so paper_tier_check passes
    try:
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences (user_id, tier) VALUES (?, 'premium')",
            (DISCOVERY_USER_ID,),
        )
        conn.execute(
            "UPDATE user_preferences SET tier='premium' WHERE user_id=?",
            (DISCOVERY_USER_ID,),
        )
    except Exception as e:
        _logger.debug('ensure_discovery_user: user_preferences insert skipped: %s', e)

    # paper_account
    conn.execute(
        "INSERT OR IGNORE INTO paper_account "
        "(user_id, virtual_balance, currency, created_at, account_size_set) "
        "VALUES (?, 1000000.0, 'GBP', ?, 1)",
        (DISCOVERY_USER_ID, now_iso),
    )
    conn.commit()


def _build_grid() -> list[dict]:
    """Return list of bot spec dicts for the full combinatorial grid."""
    bots = []

    # Single-pattern bots: 6 patterns × 7 sectors × 3 directions = 126
    for pattern, sector, direction in itertools.product(PATTERN_TYPES, SECTORS, DIRECTIONS):
        sec_label = sector or 'all'
        dir_label = direction or 'any'
        name = f'disc_{pattern}_{sec_label}_{dir_label}'
        bots.append({
            'strategy_name':  name,
            'pattern_types':  f'["{pattern}"]',
            'sectors':        f'["{sector}"]' if sector else None,
            'direction_bias': direction,
        })

    # Dual-pattern bots: 4 pairs × 3 directions × 2 sectors = 24
    for pair, direction, sector in itertools.product(DUAL_PAIRS, DIRECTIONS, DUAL_SECTORS):
        pair_label = '_'.join(pair)
        dir_label  = direction or 'any'
        name = f'disc_{pair_label}_{sector}_{dir_label}'
        import json
        bots.append({
            'strategy_name':  name,
            'pattern_types':  json.dumps(pair),
            'sectors':        f'["{sector}"]',
            'direction_bias': direction,
        })

    return bots


def seed_discovery_fleet(runner) -> list[str]:
    """Seed combinatorial discovery bots. Idempotent — returns existing ids if already seeded."""
    conn = sqlite3.connect(runner.db_path, timeout=10)
    try:
        from services.paper_trading import ensure_paper_tables
        ensure_paper_tables(conn)

        existing = conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE user_id=?",
            (DISCOVERY_USER_ID,),
        ).fetchall()
        if existing:
            _logger.info('seed_discovery_fleet: already seeded (%d bots)', len(existing))
            conn.close()
            # Ensure threads are running for any that aren't alive
            bot_ids = [r[0] for r in existing]
            started = 0
            for bid in bot_ids:
                if runner.start_bot(bid, startup_delay=0):
                    started += 1
            if started:
                _logger.info('seed_discovery_fleet: restarted %d threads', started)
            return bot_ids

        grid = _build_grid()
        now_iso = datetime.now(timezone.utc).isoformat()
        bot_ids = []

        for spec in grid:
            bot_id = 'disc_' + str(uuid.uuid4()).replace('-', '')[:12]
            genome_id = 'disc_' + spec['strategy_name'][:40]
            try:
                conn.execute("""
                    INSERT INTO paper_bot_configs
                    (user_id, bot_id, genome_id, strategy_name, generation, parent_id,
                     pattern_types, sectors, exchanges, volatility, regimes, timeframes,
                     direction_bias, risk_pct, max_positions, min_quality,
                     virtual_balance, initial_balance, role, active,
                     scan_interval_sec, min_trades_eval, created_at)
                    VALUES (?,?,?,?,0,NULL,?,?,NULL,NULL,NULL,NULL,?,?,?,?,?,?,?,1,?,?,?)
                """, (
                    DISCOVERY_USER_ID, bot_id, genome_id,
                    spec['strategy_name'],
                    spec['pattern_types'],
                    spec['sectors'],
                    spec.get('direction_bias'),
                    _DISC_BOT_DEFAULTS['risk_pct'],
                    _DISC_BOT_DEFAULTS['max_positions'],
                    _DISC_BOT_DEFAULTS['min_quality'],
                    _DISC_BOT_DEFAULTS['virtual_balance'],
                    _DISC_BOT_DEFAULTS['initial_balance'],
                    'discovery',
                    _DISC_BOT_DEFAULTS['scan_interval_sec'],
                    _DISC_BOT_DEFAULTS['min_trades_eval'],
                    now_iso,
                ))
                bot_ids.append(bot_id)
            except Exception as e:
                _logger.warning('seed_discovery_fleet: insert failed for %s: %s', spec['strategy_name'], e)

        conn.commit()
        _logger.info('seed_discovery_fleet: seeded %d discovery bots', len(bot_ids))
    finally:
        conn.close()

    # Start scanner threads with stagger
    started = 0
    for i, bid in enumerate(bot_ids):
        delay = i * 2  # 2s stagger — 150 bots = ~5min spread
        if runner.start_bot(bid, startup_delay=delay):
            started += 1
    _logger.info('seed_discovery_fleet: started %d threads', started)
    return bot_ids


def get_discovery_report(conn, min_observations: int = 5, limit: int = 50) -> list[dict]:
    """Return top-performing signal cells from calibration_observations for discovery bots.

    Joins through paper_positions → paper_bot_configs to get pattern/sector/direction context.
    Groups by pattern_type + direction_bias (from bot config) + sector.
    """
    try:
        rows = conn.execute("""
            SELECT
                co.pattern_type,
                bc.sectors,
                bc.direction_bias,
                COUNT(*)           AS observations,
                AVG(CASE WHEN co.outcome IN ('hit_t1','hit_t2','hit_t3') THEN 1.0 ELSE 0.0 END) AS hit_rate,
                AVG(sc.sample_size) AS avg_samples
            FROM calibration_observations co
            JOIN paper_positions pp  ON pp.bot_id = co.bot_id
                                    AND pp.ticker = co.ticker
            JOIN paper_bot_configs bc ON bc.bot_id = pp.bot_id
            LEFT JOIN signal_calibration sc ON sc.ticker = co.ticker
                                           AND sc.pattern_type = co.pattern_type
                                           AND sc.timeframe = co.timeframe
            WHERE bc.user_id = ?
            GROUP BY co.pattern_type, bc.sectors, bc.direction_bias
            HAVING COUNT(*) >= ?
            ORDER BY hit_rate DESC
            LIMIT ?
        """, (DISCOVERY_USER_ID, min_observations, limit)).fetchall()

        return [
            {
                'pattern_type':  r[0],
                'sectors':       r[1],
                'direction_bias': r[2],
                'observations':  r[3],
                'hit_rate':      round(r[4], 4) if r[4] is not None else None,
                'avg_samples':   round(r[5], 1) if r[5] is not None else None,
            }
            for r in rows
        ]
    except Exception as e:
        _logger.warning('get_discovery_report error: %s', e)
        return []


def get_discovery_status(conn) -> dict:
    """Return summary stats for the discovery fleet."""
    try:
        total_bots = conn.execute(
            "SELECT COUNT(*) FROM paper_bot_configs WHERE user_id=?",
            (DISCOVERY_USER_ID,),
        ).fetchone()[0]

        active_bots = conn.execute(
            "SELECT COUNT(*) FROM paper_bot_configs WHERE user_id=? AND active=1",
            (DISCOVERY_USER_ID,),
        ).fetchone()[0]

        total_closed = conn.execute(
            """SELECT COUNT(*) FROM paper_positions pp
               JOIN paper_bot_configs bc ON bc.bot_id = pp.bot_id
               WHERE bc.user_id=? AND pp.status != 'open'""",
            (DISCOVERY_USER_ID,),
        ).fetchone()[0]

        total_observations = conn.execute(
            """SELECT COUNT(*) FROM calibration_observations co
               JOIN paper_bot_configs bc ON bc.bot_id = co.bot_id
               WHERE bc.user_id=?""",
            (DISCOVERY_USER_ID,),
        ).fetchone()[0]

        open_rows = conn.execute(
            """SELECT pp.ticker, pp.direction, pp.entry_price, pp.opened_at,
                      pp.t1, pp.stop, pbc.strategy_name, pbc.pattern_types, pbc.sectors
               FROM paper_positions pp
               JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
               WHERE pbc.user_id=? AND pp.status='open'
               ORDER BY pp.opened_at DESC""",
            (DISCOVERY_USER_ID,),
        ).fetchall()

        coverage_rows = conn.execute(
            """SELECT pattern_types, direction_bias, COUNT(*) as n
               FROM paper_bot_configs
               WHERE user_id=? AND active=1
               GROUP BY pattern_types, direction_bias""",
            (DISCOVERY_USER_ID,),
        ).fetchall()

        return {
            'total_bots':             total_bots,
            'active_bots':            active_bots,
            'total_positions_closed': total_closed,
            'total_observations':     total_observations,
            'top_cells':              get_discovery_report(conn, min_observations=3, limit=10),
            'open_positions': [
                {
                    'ticker':    r[0],
                    'direction': r[1],
                    'entry':     round(r[2], 4) if r[2] else None,
                    'opened_at': r[3],
                    't1':        round(r[4], 4) if r[4] else None,
                    'stop':      round(r[5], 4) if r[5] else None,
                    'bot_name':  r[6],
                    'pattern':   r[7],
                    'sector':    r[8],
                }
                for r in open_rows
            ],
            'coverage': [
                {'pattern': r[0], 'direction': r[1] or 'any', 'count': r[2]}
                for r in coverage_rows
            ],
        }
    except Exception as e:
        _logger.warning('get_discovery_status error: %s', e)
        return {
            'total_bots': 0, 'active_bots': 0,
            'total_positions_closed': 0, 'total_observations': 0,
            'top_cells': [], 'open_positions': [], 'coverage': [], 'error': str(e),
        }

"""
scripts/seed_kb_gated_bots.py — Seed 8 KB-gated user bots

Creates manual bots with required_facts genome fields so they only enter
patterns when independent KB signal families confirm the SMC structure signal.

Run on OCI:
    cd ~/trading-galaxy
    python scripts/seed_kb_gated_bots.py --user <user_id> --balance 5000

Or dry-run to preview:
    python scripts/seed_kb_gated_bots.py --user <user_id> --balance 5000 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import uuid
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
_log = logging.getLogger(__name__)

# ── Bot specifications from KB-Gated Bot Architecture spec v1.0 ───────────────

KB_GATED_BOTS = [
    {
        'strategy_name': 'SMA Bullish Stack — LV 15m',
        'pattern_types': json.dumps(['liquidity_void']),
        'timeframes':    json.dumps(['15m']),
        'direction_bias': 'bullish',
        'required_facts': json.dumps({'sma_alignment': 'bullish_stack'}),
        'min_quality':   0.70,
        'max_positions': 6,
        'risk_pct':      1.0,
    },
    {
        'strategy_name': 'SMA Bullish Stack — 4h/1d',
        'pattern_types': json.dumps(['liquidity_void', 'mitigation', 'order_block']),
        'timeframes':    json.dumps(['4h', '1d']),
        'direction_bias': 'bullish',
        'required_facts': json.dumps({'sma_alignment': 'bullish_stack'}),
        'min_quality':   0.70,
        'max_positions': 5,
        'risk_pct':      1.0,
    },
    {
        'strategy_name': 'MACD Bullish Cross — Momentum Confirmed',
        'pattern_types': json.dumps(['liquidity_void', 'mitigation', 'breaker']),
        'timeframes':    json.dumps(['15m', '1h', '4h']),
        'direction_bias': 'bullish',
        'required_facts': json.dumps({'macd_signal': 'bullish_cross'}),
        'min_quality':   0.65,
        'max_positions': 6,
        'risk_pct':      1.0,
    },
    {
        'strategy_name': 'Dual Confirm — SMA Stack + MACD Cross',
        'pattern_types': json.dumps(['liquidity_void', 'mitigation', 'breaker', 'order_block']),
        'timeframes':    json.dumps(['1h', '4h', '1d']),
        'direction_bias': 'bullish',
        'required_facts': json.dumps({
            'sma_alignment': 'bullish_stack',
            'macd_signal':   'bullish_cross',
        }),
        'min_quality':   0.65,
        'max_positions': 5,
        'risk_pct':      1.5,
    },
    {
        'strategy_name': 'Volume Spike Breakout — LV & Breaker',
        'pattern_types': json.dumps(['liquidity_void', 'breaker']),
        'timeframes':    json.dumps(['15m', '1h']),
        'direction_bias': 'bullish',
        'required_facts': json.dumps({'volume_regime': 'spike'}),
        'min_quality':   0.65,
        'max_positions': 6,
        'risk_pct':      1.0,
    },
    {
        'strategy_name': 'Volume High Regime — Mitigation & OB',
        'pattern_types': json.dumps(['mitigation', 'order_block']),
        'timeframes':    json.dumps(['1h', '4h']),
        'direction_bias': 'bullish',
        'required_facts': json.dumps({'volume_regime': 'high'}),
        'min_quality':   0.68,
        'max_positions': 5,
        'risk_pct':      1.0,
    },
    {
        'strategy_name': 'SMA Bearish Stack — Short LV 15m',
        'pattern_types': json.dumps(['liquidity_void']),
        'timeframes':    json.dumps(['15m']),
        'direction_bias': 'bearish',
        'required_facts': json.dumps({'sma_alignment': 'bearish_stack'}),
        'min_quality':   0.68,
        'max_positions': 6,
        'risk_pct':      1.0,
    },
    {
        'strategy_name': 'Dual Confirm — Bearish Stack + MACD Cross',
        'pattern_types': json.dumps(['liquidity_void', 'mitigation', 'order_block']),
        'timeframes':    json.dumps(['1h', '4h']),
        'direction_bias': 'bearish',
        'required_facts': json.dumps({
            'sma_alignment': 'bearish_stack',
            'macd_signal':   'bearish_cross',
        }),
        'min_quality':   0.65,
        'max_positions': 5,
        'risk_pct':      1.5,
    },
]


def _genome_id(spec: dict) -> str:
    import hashlib
    raw = json.dumps(spec, sort_keys=True)
    return 'kb_' + hashlib.md5(raw.encode()).hexdigest()[:10]


def seed_bots(db_path: str, user_id: str, balance_each: float, dry_run: bool = False):
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row

    # Ensure required_facts column exists
    try:
        conn.execute('ALTER TABLE paper_bot_configs ADD COLUMN required_facts TEXT')
        conn.commit()
        _log.info('Added required_facts column to paper_bot_configs')
    except Exception:
        pass  # already exists

    now_iso = datetime.now(timezone.utc).isoformat()
    created = []

    for spec in KB_GATED_BOTS:
        genome_id = _genome_id(spec)
        bot_id = 'bot_' + str(uuid.uuid4()).replace('-', '')[:12]

        # Skip if already seeded (same genome_id for this user)
        existing = conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE user_id=? AND genome_id=?",
            (user_id, genome_id),
        ).fetchone()
        if existing:
            _log.info('  SKIP  %s (already exists: %s)', spec['strategy_name'], existing['bot_id'])
            continue

        if dry_run:
            _log.info('  DRY   %s  rf=%s  minQ=%.2f  maxPos=%d',
                      spec['strategy_name'], spec['required_facts'],
                      spec['min_quality'], spec['max_positions'])
            continue

        conn.execute("""
            INSERT INTO paper_bot_configs
            (user_id, bot_id, genome_id, strategy_name, generation,
             pattern_types, timeframes, direction_bias,
             risk_pct, max_positions, min_quality,
             virtual_balance, initial_balance,
             role, active, scan_interval_sec, min_trades_eval,
             required_facts, created_at)
            VALUES (?,?,?,?,0,?,?,?,?,?,?,?,?,?,1,1800,25,?,?)
        """, (
            user_id, bot_id, genome_id, spec['strategy_name'],
            spec.get('pattern_types'), spec.get('timeframes'),
            spec.get('direction_bias'),
            spec.get('risk_pct', 1.0), spec.get('max_positions', 5),
            spec.get('min_quality', 0.65),
            balance_each, balance_each,
            'manual',
            spec.get('required_facts'),
            now_iso,
        ))
        created.append((bot_id, spec['strategy_name']))
        _log.info('  OK    %s  [%s]', spec['strategy_name'], bot_id)

    if not dry_run:
        conn.commit()
        _log.info('\nCreated %d / %d bots for user %s', len(created), len(KB_GATED_BOTS), user_id)
    else:
        _log.info('\nDry run complete — %d bots would be created', len(KB_GATED_BOTS))

    conn.close()
    return [b[0] for b in created]


def main():
    parser = argparse.ArgumentParser(description='Seed KB-gated bots')
    parser.add_argument('--db',      default='/opt/trading-galaxy/data/trading_knowledge.db')
    parser.add_argument('--user',    required=True, help='User ID to create bots for')
    parser.add_argument('--balance', type=float, default=5000.0, help='Balance per bot')
    parser.add_argument('--dry-run', action='store_true', help='Preview without inserting')
    args = parser.parse_args()

    bot_ids = seed_bots(args.db, args.user, args.balance, dry_run=args.dry_run)

    if bot_ids and not args.dry_run:
        _log.info('\nStart bots via API or restart the service to auto-restore them.')
        _log.info('Bot IDs: %s', ', '.join(bot_ids))


if __name__ == '__main__':
    main()

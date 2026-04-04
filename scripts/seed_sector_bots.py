"""
scripts/seed_sector_bots.py — One-shot sector bot fleet seeder.

Creates Priority 1 sector-targeted bots based on calibration heatmap edges.
Run once after deploy: python scripts/seed_sector_bots.py

Priority 1 bots (Tier 1 calibration edges):
    FX Structural Edge          — fx        mit+liq_void  1d+15m  HR=75%/65%
    Real Estate Daily Zones     — real_estate liq_void    1d      HR=69.5%
    Utilities Daily Mitigation  — utilities  mitigation   1d      HR=68.8%
    ETF Liquidity Sweep         — etf        liq_void     1d      HR=67.2%
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone

DB_PATH = os.environ.get('DB_PATH', '/opt/trading-galaxy/data/trading_knowledge.db')
SEED_USER = os.environ.get('SEED_USER_ID', 'discovery_user')

_PRIORITY_1_BOTS = [
    {
        'strategy_name':     'FX Structural Edge',
        'pattern_types':     json.dumps(['mitigation', 'liquidity_void']),
        'sectors':           json.dumps(['fx']),
        'timeframes':        json.dumps(['1d', '15m']),
        'direction_bias':    None,
        'min_quality':       0.70,
        'risk_pct':          1.5,
        'max_positions':     4,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 300,
    },
    {
        'strategy_name':     'Real Estate Daily Zones',
        'pattern_types':     json.dumps(['liquidity_void']),
        'sectors':           json.dumps(['real_estate']),
        'timeframes':        json.dumps(['1d']),
        'direction_bias':    None,
        'min_quality':       0.78,
        'risk_pct':          2.0,
        'max_positions':     3,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 3600,
    },
    {
        'strategy_name':     'Utilities Daily Mitigation',
        'pattern_types':     json.dumps(['mitigation']),
        'sectors':           json.dumps(['utilities']),
        'timeframes':        json.dumps(['1d']),
        'direction_bias':    None,
        'min_quality':       0.77,
        'risk_pct':          2.0,
        'max_positions':     3,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 3600,
    },
    {
        'strategy_name':     'ETF Liquidity Sweep',
        'pattern_types':     json.dumps(['liquidity_void']),
        'sectors':           json.dumps(['etf']),
        'timeframes':        json.dumps(['1d']),
        'direction_bias':    None,
        'min_quality':       0.76,
        'risk_pct':          2.0,
        'max_positions':     3,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 3600,
    },
    # ── Priority 2: Exchange-targeted liq_void 15m bots (v2 spec) ────────────
    {
        'strategy_name':     'XETRA Liquidity Void 15m',
        'pattern_types':     json.dumps(['liquidity_void']),
        'exchanges':         json.dumps(['.DE']),
        'timeframes':        json.dumps(['15m']),
        'direction_bias':    None,
        'min_quality':       0.70,
        'risk_pct':          1.5,
        'max_positions':     3,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 300,
    },
    {
        'strategy_name':     'Paris Liquidity Void 15m',
        'pattern_types':     json.dumps(['liquidity_void']),
        'exchanges':         json.dumps(['.PA']),
        'timeframes':        json.dumps(['15m']),
        'direction_bias':    None,
        'min_quality':       0.70,
        'risk_pct':          1.5,
        'max_positions':     3,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 300,
    },
    {
        'strategy_name':     'US Platforms Liquidity Void 15m',
        'pattern_types':     json.dumps(['liquidity_void']),
        'exchanges':         json.dumps(['US']),
        'sectors':           json.dumps([
            'services_business_services,_nec',
            'security_brokers,_dealers_&_flotation_companies',
            'finance_services',
        ]),
        'timeframes':        json.dumps(['15m']),
        'direction_bias':    None,
        'min_quality':       0.70,
        'risk_pct':          2.0,
        'max_positions':     4,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 300,
    },
    {
        'strategy_name':     'LSE Daily Zones',
        'pattern_types':     json.dumps(['mitigation', 'liquidity_void']),
        'exchanges':         json.dumps(['.L']),
        'timeframes':        json.dumps(['1d']),
        'direction_bias':    None,
        'min_quality':       0.72,
        'risk_pct':          2.0,
        'max_positions':     3,
        'role':              'seed',
        'initial_balance':   6250.0,
        'scan_interval_sec': 3600,
    },
]


def _genome_hash(g: dict) -> str:
    import hashlib
    key = json.dumps({k: g.get(k) for k in sorted(g.keys())}, sort_keys=True)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def seed_bots(db_path: str, user_id: str) -> None:
    conn = sqlite3.connect(db_path, timeout=10)

    # Ensure paper_bot_configs table exists (BotRunner DDL)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_bot_configs (
            bot_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            genome_id TEXT,
            strategy_name TEXT,
            generation INTEGER DEFAULT 0,
            parent_id TEXT,
            pattern_types TEXT,
            sectors TEXT,
            exchanges TEXT,
            volatility TEXT,
            regimes TEXT,
            timeframes TEXT,
            direction_bias TEXT,
            risk_pct REAL DEFAULT 1.0,
            max_positions INTEGER DEFAULT 3,
            min_quality REAL DEFAULT 0.65,
            virtual_balance REAL DEFAULT 6250.0,
            initial_balance REAL DEFAULT 6250.0,
            role TEXT DEFAULT 'seed',
            active INTEGER DEFAULT 1,
            scan_interval_sec INTEGER DEFAULT 1800,
            min_trades_eval INTEGER DEFAULT 25,
            killed_at TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()

    created = 0
    skipped = 0
    now = datetime.now(timezone.utc).isoformat()

    for genome in _PRIORITY_1_BOTS:
        # Skip if a bot with the same strategy_name already exists for this user
        existing = conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE user_id=? AND strategy_name=? AND active=1",
            (user_id, genome['strategy_name']),
        ).fetchone()
        if existing:
            print(f"  SKIP (exists): {genome['strategy_name']}")
            skipped += 1
            continue

        bot_id = 'bot_' + str(uuid.uuid4()).replace('-', '')[:12]
        genome_id = _genome_hash(genome)

        conn.execute("""
            INSERT INTO paper_bot_configs
            (user_id, bot_id, genome_id, strategy_name, generation, parent_id,
             pattern_types, sectors, exchanges, volatility, regimes, timeframes,
             direction_bias, risk_pct, max_positions, min_quality,
             virtual_balance, initial_balance, role, active,
             scan_interval_sec, min_trades_eval, created_at)
            VALUES (?,?,?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,25,?)
        """, (
            user_id, bot_id, genome_id, genome['strategy_name'],
            None,
            genome.get('pattern_types'), genome.get('sectors'), genome.get('exchanges'),
            None, None, genome.get('timeframes'),
            genome.get('direction_bias'),
            float(genome.get('risk_pct', 1.0)),
            int(genome.get('max_positions', 3)),
            float(genome.get('min_quality', 0.65)),
            float(genome.get('initial_balance', 6250.0)),
            float(genome.get('initial_balance', 6250.0)),
            genome.get('role', 'seed'),
            int(genome.get('scan_interval_sec', 3600)),
            now,
        ))
        conn.commit()
        print(f"  CREATED: {genome['strategy_name']} → {bot_id}")
        created += 1

    conn.close()
    print(f"\nDone: {created} created, {skipped} skipped.")


if __name__ == '__main__':
    db = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    uid = sys.argv[2] if len(sys.argv) > 2 else SEED_USER
    print(f"Seeding sector bots → DB={db}  user={uid}")
    seed_bots(db, uid)

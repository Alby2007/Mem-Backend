#!/usr/bin/env python3
"""
Migrate the 5 hot tables from SQLite to Postgres.
Idempotent — uses INSERT ... ON CONFLICT DO NOTHING.
Run once: python3 scripts/migrate_to_postgres.py

Progress is printed to stdout. Large tables are batched.
"""
import os
import sqlite3
import sys
import time

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed — run: pip install psycopg2-binary")
    sys.exit(1)

SQLITE_PATH = os.environ.get('TRADING_KB_DB',
    '/opt/trading-galaxy/data/trading_knowledge.db')
PG_DSN = os.environ.get('PG_DSN',
    'postgresql://tg_app:password@localhost:5432/trading_galaxy')
BATCH = 10_000

TABLES = {
    'ohlcv_cache': {
        'cols': 'ticker, interval, ts, open, high, low, close, volume, cached_at',
        'pk':   '(ticker, interval, ts)',
    },
    'pattern_signals': {
        'cols': ('id, ticker, pattern_type, direction, zone_high, zone_low, '
                 'zone_size_pct, timeframe, formed_at, status, filled_at, '
                 'quality_score, kb_conviction, kb_regime, kb_signal_dir, '
                 'alerted_users, detected_at, expires_at, volume_at_formation, volume_vs_avg'),
        'pk': '(id)',
    },
    'facts': {
        'cols': ('id, subject, predicate, object, confidence, source, timestamp, '
                 'metadata, confidence_effective, hit_count, conf_n, conf_var'),
        'pk': '(subject, predicate, object)',
    },
    'paper_agent_log': {
        'cols': 'id, user_id, event_type, ticker, detail, created_at, bot_id',
        'pk':   '(id)',
    },
    'paper_bot_equity': {
        'cols': 'id, bot_id, equity_value, cash_balance, open_positions, logged_at',
        'pk':   '(id)',
    },
}

print(f"Source SQLite: {SQLITE_PATH}")
print(f"Target PG:    {PG_DSN.split('@')[-1] if '@' in PG_DSN else PG_DSN}\n")

src = sqlite3.connect(SQLITE_PATH, timeout=30)
src.row_factory = sqlite3.Row
pg  = psycopg2.connect(PG_DSN)
cur = pg.cursor()

for table, cfg in TABLES.items():
    cols  = cfg['cols']
    pk    = cfg['pk']
    ncols = len([c.strip() for c in cols.split(',')])
    phs   = ', '.join(['%s'] * ncols)

    try:
        total = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception as e:
        print(f"\n  SKIP {table}: {e}")
        continue
    print(f"\n{table}: {total:,} rows")

    inserted = 0
    offset   = 0
    t0 = time.time()

    while True:
        rows = src.execute(
            f"SELECT {cols} FROM {table} LIMIT {BATCH} OFFSET {offset}"
        ).fetchall()
        if not rows:
            break

        data = [tuple(r) for r in rows]
        cur.executemany(
            f"INSERT INTO {table} ({cols}) VALUES ({phs}) "
            f"ON CONFLICT {pk} DO NOTHING",
            data
        )
        pg.commit()
        inserted += len(rows)
        offset   += BATCH
        elapsed   = time.time() - t0
        rate      = inserted / elapsed if elapsed > 0 else 0
        print(f"  {inserted:>10,}/{total:,}  ({rate:,.0f} rows/s)", end='\r')

    print(f"  ✓ {inserted:,} rows migrated in {time.time()-t0:.1f}s")

# Reset sequences for BIGSERIAL columns
for table in ['pattern_signals', 'facts', 'paper_agent_log', 'paper_bot_equity']:
    try:
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
            f"COALESCE(MAX(id), 1)) FROM {table}"
        )
    except Exception as e:
        print(f"  Sequence reset for {table} failed: {e}")
        pg.rollback()
        continue
pg.commit()
print("\n✓ Sequences reset")

src.close()
pg.close()
print("\nMigration complete.")

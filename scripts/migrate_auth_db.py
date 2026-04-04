#!/usr/bin/env python3
"""
One-time migration: copies user_auth, refresh_tokens, audit_log, conv_sessions
from trading_knowledge.db to auth.db.

Run: python3 scripts/migrate_auth_db.py
Safe to run multiple times (INSERT OR IGNORE).
"""
import sqlite3
import os
import sys

MAIN_DB = os.environ.get('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
AUTH_DB = os.environ.get(
    'TRADING_AUTH_DB',
    os.path.join(os.path.dirname(MAIN_DB), 'auth.db')
)

TABLES = ['user_auth', 'refresh_tokens', 'audit_log', 'conv_sessions']

print(f"Migrating from: {MAIN_DB}")
print(f"Migrating to:   {AUTH_DB}\n")

src = sqlite3.connect(MAIN_DB, timeout=30)
dst = sqlite3.connect(AUTH_DB, timeout=30)
dst.execute("PRAGMA journal_mode=WAL")
dst.execute("PRAGMA busy_timeout=30000")

for table in TABLES:
    schema = src.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not schema or not schema[0]:
        print(f"  SKIP {table} (not found in source)")
        continue

    try:
        dst.execute(schema[0])
    except sqlite3.OperationalError as e:
        if 'already exists' not in str(e):
            raise

    for (idx_sql,) in src.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,)
    ).fetchall():
        try:
            dst.execute(idx_sql)
        except sqlite3.OperationalError:
            pass

    rows = src.execute(f"SELECT * FROM {table}").fetchall()
    if rows:
        cols = len(rows[0])
        placeholders = ','.join('?' * cols)
        dst.executemany(
            f"INSERT OR IGNORE INTO {table} VALUES ({placeholders})", rows
        )

    dst.commit()
    print(f"  ✓ {table}: {len(rows)} rows migrated")

src.close()
dst.close()
print("\nDone. Run with TRADING_AUTH_DB env var to use a custom path.")

#!/usr/bin/env python3
"""
Load the shared KB seed into a local or Docker-volume database.

Works anywhere Python is available — no sqlite3 CLI needed.

Usage:
    python scripts/load_seed.py                          # default DB path
    python scripts/load_seed.py /data/trading_knowledge.db
"""

from __future__ import annotations

import os
import re
import sys
import sqlite3
import pathlib

SEED    = pathlib.Path("tests/fixtures/kb_seed.sql")
DB_PATH = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "TRADING_KB_DB", "trading_knowledge.db"
)

def die(msg: str) -> None:
    print(f"\nERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    if not SEED.exists():
        die(f"seed file not found at '{SEED}'\n"
            "       Make sure you are running this from the repo root.")

    seed_sql = SEED.read_text(encoding="utf-8")

    print(f"Loading KB seed into {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")

    # Split on statement boundaries and execute each one
    # executescript() auto-commits so we use it directly
    try:
        conn.executescript(seed_sql)
    except sqlite3.Error as e:
        conn.close()
        die(f"seed load failed: {e}\n"
            "       The seed may be corrupt — try: git checkout tests/fixtures/kb_seed.sql")
    conn.close()

    # Verify
    conn = sqlite3.connect(DB_PATH, timeout=5)
    total_facts = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    open_patterns = conn.execute(
        "SELECT COUNT(*) FROM pattern_signals WHERE status='open'"
    ).fetchone()[0]
    unique_subjects = conn.execute(
        "SELECT COUNT(DISTINCT subject) FROM facts"
    ).fetchone()[0]
    conn.close()

    print(f"\nSeed loaded successfully.")
    print(f"  total_facts     = {total_facts}")
    print(f"  open_patterns   = {open_patterns}")
    print(f"  unique_subjects = {unique_subjects}")

    if total_facts < 100:
        print("\nWARNING: very few facts loaded — seed may be stale or partially exported.")
        print("         Ask the repo owner to run: python scripts/export_seed.py")
    else:
        print("\nKB is ready. Next steps:")
        print("  1. Start the API       : python api.py")
        print("  2. Register test user  : curl -s -X POST http://localhost:5050/auth/register \\")
        print("                            -H 'Content-Type: application/json' \\")
        print("                            -d '{\"user_id\": \"alice\", \"password\": \"test\"}'")
        print("  3. Explore signal data : curl -s http://localhost:5050/stats")


if __name__ == "__main__":
    main()

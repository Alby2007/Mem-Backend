#!/usr/bin/env python3
"""
Export shared KB data only — no user data.
Run on your live machine to update tests/fixtures/kb_seed.sql

Usage:
    python scripts/export_seed.py
    python scripts/export_seed.py path/to/trading_knowledge.db
"""

from __future__ import annotations

import os
import sys
import sqlite3
import pathlib

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH  = sys.argv[1] if len(sys.argv) > 1 else "trading_knowledge.db"
OUTPUT   = pathlib.Path("tests/fixtures/kb_seed.sql")

SHARED_TABLES = [
    "facts",
    "fact_conflicts",
    "causal_edges",
    "pattern_signals",
    "signal_calibration",
    "extraction_queue",
    "edgar_realtime_seen",
    # KB structure / governance
    "taxonomy",
    "fact_categories",
    "predicate_vocabulary",
    "working_state",
    "governance_metrics",
    "domain_refresh_queue",
    "synthesis_queue",
    "consolidation_log",
    "kb_insufficient_log",
    "repair_execution_log",
    "repair_rollback_log",
]

USER_TABLES = [
    "user_auth",
    "refresh_tokens",
    "user_portfolios",
    "user_models",
    "user_preferences",
    "user_kb_context",
    "user_engagement_events",
    "user_universe_expansions",
    "universe_tickers",
    "ticker_staging",
    "snapshot_delivery_log",
    "tip_delivery_log",
    "tip_feedback",
    "audit_log",
    "alerts",
]

MIN_FACTS        = 2000
MIN_OPEN_PATTERNS = 1
MAX_STRESS_SCORE  = 0.4

# ── Helpers ───────────────────────────────────────────────────────────────────

def die(msg: str) -> None:
    print(f"\nERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def dump_table(conn: sqlite3.Connection, table: str, schema_only: bool = False) -> str:
    """Return SQL to recreate a table (always) and optionally its rows."""
    lines: list[str] = []

    # Schema
    schema_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if schema_row is None:
        return ""
    lines.append(f"{schema_row[0]};")

    if schema_only:
        return "\n".join(lines) + "\n"

    # Indexes
    for idx_row in conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,),
    ):
        lines.append(f"{idx_row[0]};")

    # Rows
    rows = conn.execute(f"SELECT * FROM [{table}]").fetchall()  # noqa: S608
    if rows:
        col_names = [d[0] for d in conn.execute(f"SELECT * FROM [{table}] LIMIT 0").description]  # noqa: S608
        for row in rows:
            values = ", ".join(_escape(v) for v in row)
            lines.append(f"INSERT OR IGNORE INTO [{table}] ({', '.join(col_names)}) VALUES ({values});")

    return "\n".join(lines) + "\n"


def _escape(v: object) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, bytes):
        return f"X'{v.hex()}'"
    return "'" + str(v).replace("'", "''") + "'"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not os.path.exists(DB_PATH):
        die(f"database not found at '{DB_PATH}'\n"
            "       Pass the path as the first argument: python scripts/export_seed.py /path/to/db")

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row

    # ── 1. Pre-flight: all tables must exist ──────────────────────────────────
    print(f"Running pre-flight table check on {DB_PATH}...")
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    missing = [t for t in SHARED_TABLES + USER_TABLES if t not in existing]
    if missing:
        for t in missing:
            print(f"  MISSING TABLE: {t}")
        die(f"{len(missing)} table(s) missing.\n"
            "       Run the app once (python api.py) so all ensure_*_tables() calls fire,\n"
            "       then re-run this script.")
    print("All tables present.")

    # ── 2. Quality gate ───────────────────────────────────────────────────────
    print("\nRunning quality gate...")
    failures = 0

    total_facts = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    if total_facts < MIN_FACTS:
        print(f"  FAIL  total_facts = {total_facts}  (need >= {MIN_FACTS})")
        print( "        Run: curl -X POST http://localhost:5050/ingest/run-all")
        print( "             curl -X POST http://localhost:5050/ingest/historical")
        print( "        Then wait ~5 minutes for signal enrichment to complete.")
        failures += 1
    else:
        print(f"  OK    total_facts = {total_facts}")

    open_patterns = conn.execute(
        "SELECT COUNT(*) FROM pattern_signals WHERE status='open'"
    ).fetchone()[0]
    total_patterns = conn.execute(
        "SELECT COUNT(*) FROM pattern_signals"
    ).fetchone()[0]
    if open_patterns < MIN_OPEN_PATTERNS:
        print(f"  WARN  open_patterns = {open_patterns}, total_patterns = {total_patterns}")
        print( "        Pattern detector may not have run yet — seed will still ship shared KB facts.")
        print( "        To populate patterns: POST /ingest/run-all then wait for yfinance OHLCV data.")
        # Warn only — do not block export, facts are the primary value
    else:
        print(f"  OK    open_patterns = {open_patterns}")

    conflict_count = conn.execute("SELECT COUNT(*) FROM fact_conflicts").fetchone()[0]
    conflict_ratio = conflict_count / max(total_facts, 1)
    if conflict_ratio > 0.5:
        print(f"  FAIL  fact_conflicts = {conflict_count} ({conflict_ratio:.0%} of facts — KB under stress)")
        print( "        More than 50% of facts have conflicts — resolve before exporting.")
        failures += 1
    else:
        print(f"  OK    fact_conflicts = {conflict_count} ({conflict_ratio:.0%} of facts)")

    if failures:
        die(f"{failures} quality check(s) failed — export aborted.\n"
            "       The seed is only as good as the KB state when you export it.\n"
            "       Fix the issues above and re-run this script.")
    print("Quality gate passed.")

    # ── 3. Export ─────────────────────────────────────────────────────────────
    print(f"\nExporting shared KB from {DB_PATH} to {OUTPUT}...")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    chunks: list[str] = [
        "-- Trading Galaxy — shared KB seed\n"
        "-- Contains market intelligence only. No user data.\n"
        "-- Generated by scripts/export_seed.py\n\n"
        "PRAGMA journal_mode=WAL;\n"
        "BEGIN TRANSACTION;\n"
    ]

    for table in SHARED_TABLES:
        chunks.append(f"\n-- {table}\n")
        chunks.append(dump_table(conn, table, schema_only=False))

    chunks.append("\n-- User/personal table schemas only (no data)\n")
    for table in USER_TABLES:
        chunks.append(f"\n-- {table}\n")
        chunks.append(dump_table(conn, table, schema_only=True))

    chunks.append("\nCOMMIT;\n")

    OUTPUT.write_text("".join(chunks), encoding="utf-8")
    conn.close()

    # ── 4. Summary ────────────────────────────────────────────────────────────
    insert_count = OUTPUT.read_text(encoding="utf-8").count("\nINSERT ")
    print(f"\nDone.")
    print(f"  Output              : {OUTPUT}")
    print(f"  INSERT rows exported: {insert_count}")
    print(f"\nCommit the updated seed:")
    print(f"  git add {OUTPUT} && git commit -m 'chore: refresh KB seed'")


if __name__ == "__main__":
    main()

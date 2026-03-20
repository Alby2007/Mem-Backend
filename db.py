"""
db.py — Unified database connection module.

Hot tables (ohlcv_cache, pattern_signals, paper_agent_log, facts,
paper_bot_equity) → PostgreSQL via PG_DSN env var.

Everything else → SQLite at DB_PATH / TRADING_KB_DB env var.

Usage:
    from db import get_pg, get_sqlite, PG_TABLES, HAS_POSTGRES

    # Postgres (hot tables)
    with get_pg() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM ohlcv_cache WHERE ticker=%s", ("AAPL",))

    # SQLite (everything else)
    conn = get_sqlite()
    conn.execute("SELECT * FROM signal_calibration WHERE ticker=?", ("AAPL",))
"""
from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Generator

_log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PG_DSN   = os.environ.get("PG_DSN", "")
DB_PATH  = os.environ.get("TRADING_KB_DB", "/opt/trading-galaxy/data/trading_knowledge.db")

# Tables that live in Postgres when PG_DSN is set
PG_TABLES = frozenset({
    "ohlcv_cache",
    "paper_agent_log",
    "pattern_signals",
    "facts",
    "paper_bot_equity",
})

HAS_POSTGRES = bool(PG_DSN)

# ── Postgres connection pool ─────────────────────────────────────────────────
_pg_pool = None  # type: ignore

def _init_pool():
    """Initialise the threaded connection pool. Called once at startup."""
    global _pg_pool, HAS_POSTGRES
    if _pg_pool is not None:
        return _pg_pool
    if not PG_DSN:
        HAS_POSTGRES = False
        return None
    try:
        import psycopg2
        import psycopg2.extras
        from psycopg2.pool import ThreadedConnectionPool
        _pg_pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=PG_DSN,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        HAS_POSTGRES = True
        _log.info("PostgreSQL pool initialised (%s)", PG_DSN.split("@")[-1] if "@" in PG_DSN else "local")
        return _pg_pool
    except Exception as exc:
        _log.warning("PostgreSQL pool failed: %s — falling back to SQLite", exc)
        HAS_POSTGRES = False
        return None


@contextmanager
def get_pg() -> Generator:
    """Context manager: yields a Postgres connection from the pool.
    Auto-commits on success, rolls back on exception."""
    pool = _pg_pool or _init_pool()
    if pool is None:
        raise RuntimeError("PostgreSQL is not configured (PG_DSN not set)")
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ── SQLite ────────────────────────────────────────────────────────────────────
def get_sqlite(path: str | None = None, timeout: int = 30) -> sqlite3.Connection:
    """Return a WAL-mode SQLite connection for non-hot tables."""
    conn = sqlite3.connect(path or DB_PATH, timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

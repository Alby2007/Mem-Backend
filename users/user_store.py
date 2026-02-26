"""
users/user_store.py — User Management Store

Manages four tables in trading_knowledge.db:
  user_portfolios      — per-user holdings (ticker, quantity, avg_cost)
  user_models          — derived model from portfolio analysis
  user_preferences     — onboarding settings, Telegram chat ID, delivery time/tz
  snapshot_delivery_log — history of briefing deliveries

All functions use direct sqlite3 connections (same db as KB).
No external dependencies.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    from middleware.encryption import encrypt_field, decrypt_field
except ImportError:
    def encrypt_field(v): return v   # type: ignore
    def decrypt_field(v): return v   # type: ignore


# ── Schema ─────────────────────────────────────────────────────────────────────

_DDL_PORTFOLIO = """
CREATE TABLE IF NOT EXISTS user_portfolios (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    quantity     REAL,
    avg_cost     REAL,
    sector       TEXT,
    submitted_at TEXT NOT NULL
)
"""

_DDL_MODELS = """
CREATE TABLE IF NOT EXISTS user_models (
    user_id                  TEXT PRIMARY KEY,
    risk_tolerance           TEXT,
    sector_affinity          TEXT,
    avg_conviction_threshold REAL,
    holding_style            TEXT,
    portfolio_beta           REAL,
    concentration_risk       TEXT,
    last_updated             TEXT NOT NULL
)
"""

_DDL_PREFERENCES = """
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id                TEXT PRIMARY KEY,
    onboarding_complete    INTEGER DEFAULT 0,
    selected_sectors       TEXT DEFAULT '[]',
    selected_risk          TEXT DEFAULT 'moderate',
    telegram_chat_id       TEXT,
    delivery_time          TEXT DEFAULT '08:00',
    timezone               TEXT DEFAULT 'UTC',
    tier                   TEXT DEFAULT 'basic',
    tip_delivery_time      TEXT DEFAULT '08:00',
    tip_delivery_timezone  TEXT DEFAULT 'UTC',
    tip_markets            TEXT DEFAULT '["equities"]',
    tip_timeframes         TEXT DEFAULT '["1h"]',
    tip_pattern_types      TEXT,
    account_size           REAL,
    max_risk_per_trade_pct REAL DEFAULT 1.0,
    account_currency       TEXT DEFAULT 'GBP'
)
"""

_DDL_PATTERN_SIGNALS = """
CREATE TABLE IF NOT EXISTS pattern_signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker        TEXT NOT NULL,
    pattern_type  TEXT NOT NULL,
    direction     TEXT NOT NULL,
    zone_high     REAL NOT NULL,
    zone_low      REAL NOT NULL,
    zone_size_pct REAL NOT NULL,
    timeframe     TEXT NOT NULL,
    formed_at     TEXT NOT NULL,
    status        TEXT DEFAULT 'open',
    filled_at     TEXT,
    quality_score REAL,
    kb_conviction TEXT,
    kb_regime     TEXT,
    kb_signal_dir TEXT,
    alerted_users TEXT DEFAULT '[]',
    detected_at   TEXT NOT NULL
)
"""

_DDL_TIP_DELIVERY_LOG = """
CREATE TABLE IF NOT EXISTS tip_delivery_log (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id                 TEXT NOT NULL,
    pattern_signal_id       INTEGER,
    delivered_at            TEXT NOT NULL,
    delivered_at_local_date TEXT,
    success                 INTEGER NOT NULL DEFAULT 0,
    message_length          INTEGER
)
"""

# New columns added to user_preferences after initial schema creation
_PREFERENCES_MIGRATIONS = [
    "ALTER TABLE user_preferences ADD COLUMN tier TEXT DEFAULT 'basic'",
    "ALTER TABLE user_preferences ADD COLUMN tip_delivery_time TEXT DEFAULT '08:00'",
    "ALTER TABLE user_preferences ADD COLUMN tip_delivery_timezone TEXT DEFAULT 'UTC'",
    "ALTER TABLE user_preferences ADD COLUMN tip_markets TEXT DEFAULT '[\"equities\"]'",
    "ALTER TABLE user_preferences ADD COLUMN tip_timeframes TEXT DEFAULT '[\"1h\"]'",
    "ALTER TABLE user_preferences ADD COLUMN tip_pattern_types TEXT",
    "ALTER TABLE user_preferences ADD COLUMN account_size REAL",
    "ALTER TABLE user_preferences ADD COLUMN max_risk_per_trade_pct REAL DEFAULT 1.0",
    "ALTER TABLE user_preferences ADD COLUMN account_currency TEXT DEFAULT 'GBP'",
]

_DDL_DELIVERY_LOG = """
CREATE TABLE IF NOT EXISTS snapshot_delivery_log (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id                 TEXT NOT NULL,
    delivered_at            TEXT NOT NULL,
    delivered_at_local_date TEXT,
    success                 INTEGER NOT NULL DEFAULT 0,
    message_length          INTEGER,
    regime_at_delivery      TEXT,
    opportunities_count     INTEGER
)
"""


def ensure_user_tables(conn: sqlite3.Connection) -> None:
    """Create all user-related tables if they do not exist. Idempotent."""
    conn.execute(_DDL_PORTFOLIO)
    conn.execute(_DDL_MODELS)
    conn.execute(_DDL_PREFERENCES)
    conn.execute(_DDL_DELIVERY_LOG)
    conn.execute(_DDL_PATTERN_SIGNALS)
    conn.execute(_DDL_TIP_DELIVERY_LOG)
    # Migrate existing user_preferences rows to include new columns
    for sql in _PREFERENCES_MIGRATIONS:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()


# ── User preferences ───────────────────────────────────────────────────────────

def create_user(
    db_path: str,
    user_id: str,
    telegram_chat_id: Optional[str] = None,
    delivery_time: str = '08:00',
    timezone_str: str = 'UTC',
) -> dict:
    """
    Create a user preferences row. Returns the created row.
    If user already exists, returns existing row unchanged.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            """INSERT OR IGNORE INTO user_preferences
               (user_id, telegram_chat_id, delivery_time, timezone)
               VALUES (?, ?, ?, ?)""",
            (user_id, encrypt_field(telegram_chat_id), delivery_time, timezone_str),
        )
        conn.commit()
        return get_user(db_path, user_id) or {}
    finally:
        conn.close()


def get_user(db_path: str, user_id: str) -> Optional[dict]:
    """Return the user_preferences row for user_id, or None if not found."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            """SELECT user_id, onboarding_complete, selected_sectors,
                      selected_risk, telegram_chat_id, delivery_time, timezone
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        if row is None:
            return None
        cols = ['user_id', 'onboarding_complete', 'selected_sectors',
                'selected_risk', 'telegram_chat_id', 'delivery_time', 'timezone']
        d = dict(zip(cols, row))
        try:
            d['selected_sectors'] = json.loads(d['selected_sectors'] or '[]')
        except (json.JSONDecodeError, TypeError):
            d['selected_sectors'] = []
        d['telegram_chat_id'] = decrypt_field(d.get('telegram_chat_id'))
        return d
    finally:
        conn.close()


def update_preferences(
    db_path: str,
    user_id: str,
    *,
    selected_sectors: Optional[List[str]] = None,
    selected_risk: Optional[str] = None,
    telegram_chat_id: Optional[str] = None,
    delivery_time: Optional[str] = None,
    timezone_str: Optional[str] = None,
    onboarding_complete: Optional[int] = None,
) -> dict:
    """
    Upsert user_preferences fields. Only updates provided (non-None) fields.
    Creates the row if it does not exist.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences (user_id) VALUES (?)",
            (user_id,),
        )
        if selected_sectors is not None:
            conn.execute(
                "UPDATE user_preferences SET selected_sectors = ? WHERE user_id = ?",
                (json.dumps(selected_sectors), user_id),
            )
        if selected_risk is not None:
            conn.execute(
                "UPDATE user_preferences SET selected_risk = ? WHERE user_id = ?",
                (selected_risk, user_id),
            )
        if telegram_chat_id is not None:
            conn.execute(
                "UPDATE user_preferences SET telegram_chat_id = ? WHERE user_id = ?",
                (encrypt_field(telegram_chat_id), user_id),
            )
        if delivery_time is not None:
            conn.execute(
                "UPDATE user_preferences SET delivery_time = ? WHERE user_id = ?",
                (delivery_time, user_id),
            )
        if timezone_str is not None:
            conn.execute(
                "UPDATE user_preferences SET timezone = ? WHERE user_id = ?",
                (timezone_str, user_id),
            )
        if onboarding_complete is not None:
            conn.execute(
                "UPDATE user_preferences SET onboarding_complete = ? WHERE user_id = ?",
                (int(onboarding_complete), user_id),
            )
        conn.commit()
        return get_user(db_path, user_id) or {}
    finally:
        conn.close()


# ── Portfolio ──────────────────────────────────────────────────────────────────

def upsert_portfolio(
    db_path: str,
    user_id: str,
    holdings: List[dict],
) -> dict:
    """
    Replace all holdings for a user with the provided list.
    Each holding: { ticker, quantity?, avg_cost?, sector? }
    Returns { user_id, count, submitted_at }.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            "DELETE FROM user_portfolios WHERE user_id = ?", (user_id,)
        )
        for h in holdings:
            ticker = str(h.get('ticker', '')).upper().strip()
            if not ticker:
                continue
            conn.execute(
                """INSERT INTO user_portfolios
                   (user_id, ticker, quantity, avg_cost, sector, submitted_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    user_id,
                    ticker,
                    h.get('quantity'),
                    h.get('avg_cost'),
                    h.get('sector'),
                    now_iso,
                ),
            )
        conn.commit()
        return {'user_id': user_id, 'count': len(holdings), 'submitted_at': now_iso}
    finally:
        conn.close()


def get_portfolio(db_path: str, user_id: str) -> List[dict]:
    """Return all holdings for a user as a list of dicts."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        rows = conn.execute(
            """SELECT ticker, quantity, avg_cost, sector, submitted_at
               FROM user_portfolios WHERE user_id = ?
               ORDER BY ticker""",
            (user_id,),
        ).fetchall()
        cols = ['ticker', 'quantity', 'avg_cost', 'sector', 'submitted_at']
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


# ── User model ─────────────────────────────────────────────────────────────────

def upsert_user_model(
    db_path: str,
    user_id: str,
    risk_tolerance: str,
    sector_affinity: List[str],
    avg_conviction_threshold: Optional[float],
    holding_style: str,
    portfolio_beta: Optional[float],
    concentration_risk: str,
) -> dict:
    """Insert or replace the derived user model row."""
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            """INSERT OR REPLACE INTO user_models
               (user_id, risk_tolerance, sector_affinity,
                avg_conviction_threshold, holding_style, portfolio_beta,
                concentration_risk, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                risk_tolerance,
                json.dumps(sector_affinity),
                avg_conviction_threshold,
                holding_style,
                portfolio_beta,
                concentration_risk,
                now_iso,
            ),
        )
        conn.commit()
        return get_user_model(db_path, user_id) or {}
    finally:
        conn.close()


def get_user_model(db_path: str, user_id: str) -> Optional[dict]:
    """Return the user_models row for user_id, or None."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            """SELECT user_id, risk_tolerance, sector_affinity,
                      avg_conviction_threshold, holding_style, portfolio_beta,
                      concentration_risk, last_updated
               FROM user_models WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        if row is None:
            return None
        cols = ['user_id', 'risk_tolerance', 'sector_affinity',
                'avg_conviction_threshold', 'holding_style', 'portfolio_beta',
                'concentration_risk', 'last_updated']
        d = dict(zip(cols, row))
        try:
            d['sector_affinity'] = json.loads(d['sector_affinity'] or '[]')
        except (json.JSONDecodeError, TypeError):
            d['sector_affinity'] = []
        return d
    finally:
        conn.close()


# ── Delivery log ───────────────────────────────────────────────────────────────

def log_delivery(
    db_path: str,
    user_id: str,
    success: bool,
    message_length: Optional[int] = None,
    regime_at_delivery: Optional[str] = None,
    opportunities_count: Optional[int] = None,
    local_date: Optional[str] = None,
) -> dict:
    """
    Insert a delivery log row. Returns the inserted row.

    Parameters
    ----------
    local_date  YYYY-MM-DD in the user's local timezone. When provided by the
                delivery scheduler this enables DST-safe dedup via
                already_delivered_today(). Defaults to UTC date if not supplied.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    if local_date is None:
        local_date = now_iso[:10]  # UTC date as fallback
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        # Add column if it was created before the schema update (idempotent)
        try:
            conn.execute(
                "ALTER TABLE snapshot_delivery_log ADD COLUMN delivered_at_local_date TEXT"
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
        cur = conn.execute(
            """INSERT INTO snapshot_delivery_log
               (user_id, delivered_at, delivered_at_local_date, success,
                message_length, regime_at_delivery, opportunities_count)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id, now_iso, local_date, int(success),
                message_length, regime_at_delivery, opportunities_count,
            ),
        )
        conn.commit()
        return {
            'id':                     cur.lastrowid,
            'user_id':                user_id,
            'delivered_at':           now_iso,
            'delivered_at_local_date': local_date,
            'success':                success,
            'message_length':         message_length,
            'regime_at_delivery':     regime_at_delivery,
            'opportunities_count':    opportunities_count,
        }
    finally:
        conn.close()


def get_delivery_history(
    db_path: str,
    user_id: str,
    limit: int = 30,
) -> List[dict]:
    """Return recent delivery log rows for a user, newest first."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        try:
            conn.execute(
                "ALTER TABLE snapshot_delivery_log ADD COLUMN delivered_at_local_date TEXT"
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
        rows = conn.execute(
            """SELECT id, user_id, delivered_at, delivered_at_local_date,
                      success, message_length,
                      regime_at_delivery, opportunities_count
               FROM snapshot_delivery_log
               WHERE user_id = ?
               ORDER BY delivered_at DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        cols = ['id', 'user_id', 'delivered_at', 'delivered_at_local_date',
                'success', 'message_length',
                'regime_at_delivery', 'opportunities_count']
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def already_delivered_today(
    db_path: str,
    user_id: str,
    local_date_str: str,
) -> bool:
    """
    Return True if a successful delivery already exists for user_id on
    local_date_str (format YYYY-MM-DD in the user's local timezone).

    Checks delivered_at_local_date column (written by the scheduler with
    the user's local date, DST-aware). Falls back to UTC date prefix match
    on delivered_at if the column is absent (pre-migration rows).
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        # Primary check: local date column (set by scheduler)
        try:
            row = conn.execute(
                """SELECT 1 FROM snapshot_delivery_log
                   WHERE user_id = ? AND success = 1
                     AND delivered_at_local_date = ?
                   LIMIT 1""",
                (user_id, local_date_str),
            ).fetchone()
            if row is not None:
                return True
        except sqlite3.OperationalError:
            pass  # column doesn't exist on old schema

        # Fallback: UTC date prefix on delivered_at (for pre-migration rows)
        rows = conn.execute(
            """SELECT delivered_at FROM snapshot_delivery_log
               WHERE user_id = ? AND success = 1
               ORDER BY delivered_at DESC LIMIT 10""",
            (user_id,),
        ).fetchall()
        for (delivered_at,) in rows:
            if delivered_at and delivered_at[:10] == local_date_str:
                return True
        return False
    finally:
        conn.close()


# ── Pattern signals ────────────────────────────────────────────────────────────

def upsert_pattern_signal(db_path: str, signal: dict) -> int:
    """
    Insert a pattern_signals row. Returns the row id.

    Parameters (all from PatternSignal dataclass, serialised to dict):
      ticker, pattern_type, direction, zone_high, zone_low, zone_size_pct,
      timeframe, formed_at, status, quality_score, kb_conviction,
      kb_regime, kb_signal_dir.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        cur = conn.execute(
            """INSERT INTO pattern_signals
               (ticker, pattern_type, direction, zone_high, zone_low,
                zone_size_pct, timeframe, formed_at, status,
                quality_score, kb_conviction, kb_regime, kb_signal_dir,
                alerted_users, detected_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'[]',?)""",
            (
                signal['ticker'], signal['pattern_type'], signal['direction'],
                signal['zone_high'], signal['zone_low'], signal['zone_size_pct'],
                signal['timeframe'], signal['formed_at'],
                signal.get('status', 'open'), signal.get('quality_score'),
                signal.get('kb_conviction', ''), signal.get('kb_regime', ''),
                signal.get('kb_signal_dir', ''), now_iso,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_open_patterns(
    db_path: str,
    ticker: Optional[str] = None,
    pattern_type: Optional[str] = None,
    direction: Optional[str] = None,
    timeframe: Optional[str] = None,
    min_quality: float = 0.0,
    limit: int = 100,
) -> List[dict]:
    """Return open (non-filled, non-broken) pattern_signals rows, newest first."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        clauses = ["status NOT IN ('filled','broken')", "quality_score >= ?"]
        params: list = [min_quality]
        if ticker:
            clauses.append("ticker = ?")
            params.append(ticker)
        if pattern_type:
            clauses.append("pattern_type = ?")
            params.append(pattern_type)
        if direction:
            clauses.append("direction = ?")
            params.append(direction)
        if timeframe:
            clauses.append("timeframe = ?")
            params.append(timeframe)
        where = ' AND '.join(clauses)
        params.append(limit)
        rows = conn.execute(
            f"""SELECT id, ticker, pattern_type, direction, zone_high, zone_low,
                       zone_size_pct, timeframe, formed_at, status, filled_at,
                       quality_score, kb_conviction, kb_regime, kb_signal_dir,
                       alerted_users, detected_at
                FROM pattern_signals
                WHERE {where}
                ORDER BY detected_at DESC LIMIT ?""",
            params,
        ).fetchall()
        cols = ['id', 'ticker', 'pattern_type', 'direction', 'zone_high', 'zone_low',
                'zone_size_pct', 'timeframe', 'formed_at', 'status', 'filled_at',
                'quality_score', 'kb_conviction', 'kb_regime', 'kb_signal_dir',
                'alerted_users', 'detected_at']
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            try:
                d['alerted_users'] = json.loads(d['alerted_users'] or '[]')
            except (json.JSONDecodeError, TypeError):
                d['alerted_users'] = []
            result.append(d)
        return result
    finally:
        conn.close()


def mark_pattern_alerted(db_path: str, pattern_id: int, user_id: str) -> None:
    """Add user_id to alerted_users JSON array for a pattern_signals row."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            "SELECT alerted_users FROM pattern_signals WHERE id = ?", (pattern_id,)
        ).fetchone()
        if row is None:
            return
        try:
            alerted = json.loads(row[0] or '[]')
        except (json.JSONDecodeError, TypeError):
            alerted = []
        if user_id not in alerted:
            alerted.append(user_id)
            conn.execute(
                "UPDATE pattern_signals SET alerted_users = ? WHERE id = ?",
                (json.dumps(alerted), pattern_id),
            )
            conn.commit()
    finally:
        conn.close()


def update_pattern_status(
    db_path: str,
    pattern_id: int,
    status: str,
    filled_at: Optional[str] = None,
) -> None:
    """Update the status (and optionally filled_at) of a pattern_signals row."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        if filled_at:
            conn.execute(
                "UPDATE pattern_signals SET status = ?, filled_at = ? WHERE id = ?",
                (status, filled_at, pattern_id),
            )
        else:
            conn.execute(
                "UPDATE pattern_signals SET status = ? WHERE id = ?",
                (status, pattern_id),
            )
        conn.commit()
    finally:
        conn.close()


# ── Tip delivery log ───────────────────────────────────────────────────────────

def log_tip_delivery(
    db_path: str,
    user_id: str,
    success: bool,
    pattern_signal_id: Optional[int] = None,
    message_length: Optional[int] = None,
    local_date: Optional[str] = None,
) -> dict:
    """Insert a tip_delivery_log row. Returns the inserted row."""
    now_iso = datetime.now(timezone.utc).isoformat()
    if local_date is None:
        local_date = now_iso[:10]
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        cur = conn.execute(
            """INSERT INTO tip_delivery_log
               (user_id, pattern_signal_id, delivered_at, delivered_at_local_date,
                success, message_length)
               VALUES (?,?,?,?,?,?)""",
            (user_id, pattern_signal_id, now_iso, local_date,
             int(success), message_length),
        )
        conn.commit()
        return {
            'id':                     cur.lastrowid,
            'user_id':                user_id,
            'pattern_signal_id':      pattern_signal_id,
            'delivered_at':           now_iso,
            'delivered_at_local_date': local_date,
            'success':                success,
            'message_length':         message_length,
        }
    finally:
        conn.close()


def already_tipped_today(db_path: str, user_id: str, local_date_str: str) -> bool:
    """Return True if a successful tip was already delivered on local_date_str."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            """SELECT 1 FROM tip_delivery_log
               WHERE user_id = ? AND success = 1
                 AND delivered_at_local_date = ?
               LIMIT 1""",
            (user_id, local_date_str),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def get_tip_history(db_path: str, user_id: str, limit: int = 30) -> List[dict]:
    """Return recent tip_delivery_log rows for a user, newest first."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        rows = conn.execute(
            """SELECT id, user_id, pattern_signal_id, delivered_at,
                      delivered_at_local_date, success, message_length
               FROM tip_delivery_log
               WHERE user_id = ?
               ORDER BY delivered_at DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        cols = ['id', 'user_id', 'pattern_signal_id', 'delivered_at',
                'delivered_at_local_date', 'success', 'message_length']
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


# ── Tier + tip config ──────────────────────────────────────────────────────────

def get_user_tier(db_path: str, user_id: str) -> str:
    """Return the user's tier ('basic' or 'pro'). Defaults to 'basic'."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            "SELECT tier FROM user_preferences WHERE user_id = ?", (user_id,)
        ).fetchone()
        if row and row[0]:
            return row[0]
        return 'basic'
    finally:
        conn.close()


# ── Hybrid build tables ────────────────────────────────────────────────────────

_DDL_UNIVERSE_TICKERS = """
CREATE TABLE IF NOT EXISTS universe_tickers (
    ticker          TEXT PRIMARY KEY,
    requested_by    TEXT,
    sector_label    TEXT,
    coverage_count  INTEGER DEFAULT 1,
    added_to_ingest INTEGER DEFAULT 0,
    added_at        TEXT NOT NULL
)
"""

_DDL_TICKER_STAGING = """
CREATE TABLE IF NOT EXISTS ticker_staging (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker           TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    requested_at     TEXT NOT NULL,
    coverage_count   INTEGER DEFAULT 1,
    promoted         INTEGER DEFAULT 0,
    promoted_at      TEXT,
    rejection_reason TEXT
)
"""

_DDL_USER_UNIVERSE_EXPANSIONS = """
CREATE TABLE IF NOT EXISTS user_universe_expansions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id       TEXT NOT NULL,
    description   TEXT NOT NULL,
    sector_label  TEXT,
    tickers       TEXT DEFAULT '[]',
    etfs          TEXT DEFAULT '[]',
    keywords      TEXT DEFAULT '[]',
    causal_edges  TEXT DEFAULT '[]',
    status        TEXT DEFAULT 'active',
    requested_at  TEXT NOT NULL,
    activated_at  TEXT
)
"""

_DDL_USER_KB_CONTEXT = """
CREATE TABLE IF NOT EXISTS user_kb_context (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     TEXT NOT NULL,
    subject     TEXT NOT NULL,
    predicate   TEXT NOT NULL,
    object      TEXT NOT NULL,
    confidence  REAL DEFAULT 1.0,
    source      TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE(user_id, subject, predicate)
)
"""

_DDL_USER_ENGAGEMENT_EVENTS = """
CREATE TABLE IF NOT EXISTS user_engagement_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    ticker       TEXT,
    pattern_type TEXT,
    sector       TEXT,
    timestamp    TEXT NOT NULL
)
"""

_DDL_SIGNAL_CALIBRATION = """
CREATE TABLE IF NOT EXISTS signal_calibration (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                  TEXT NOT NULL,
    pattern_type            TEXT NOT NULL,
    timeframe               TEXT NOT NULL,
    market_regime           TEXT,
    sample_size             INTEGER DEFAULT 0,
    hit_rate_t1             REAL,
    hit_rate_t2             REAL,
    hit_rate_t3             REAL,
    stopped_out_rate        REAL,
    avg_time_to_target_hours REAL,
    calibration_confidence  REAL DEFAULT 0.0,
    last_updated            TEXT NOT NULL,
    UNIQUE(ticker, pattern_type, timeframe, market_regime)
)
"""

_DDL_KB_META = """
CREATE TABLE IF NOT EXISTS kb_meta (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
)
"""


def ensure_hybrid_tables(conn: sqlite3.Connection) -> None:
    """Create all hybrid build tables if they do not exist. Idempotent."""
    conn.execute(_DDL_UNIVERSE_TICKERS)
    conn.execute(_DDL_TICKER_STAGING)
    conn.execute(_DDL_USER_UNIVERSE_EXPANSIONS)
    conn.execute(_DDL_USER_KB_CONTEXT)
    conn.execute(_DDL_USER_ENGAGEMENT_EVENTS)
    conn.execute(_DDL_SIGNAL_CALIBRATION)
    conn.execute(_DDL_KB_META)
    conn.commit()


def ensure_kb_meta_table(conn: sqlite3.Connection) -> None:
    """Create kb_meta if it does not exist. Idempotent."""
    conn.execute(_DDL_KB_META)
    conn.commit()


def get_kb_meta(db_path: str, key: str) -> str | None:
    """Read a value from kb_meta. Returns None if key absent."""
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        row = conn.execute(
            "SELECT value FROM kb_meta WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def set_kb_meta(db_path: str, key: str, value: str) -> None:
    """Upsert a key-value pair in kb_meta."""
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO kb_meta (key, value, updated_at) "
            "VALUES (?, ?, datetime('now'))",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


# ── Universe tickers store helpers ─────────────────────────────────────────────

def get_universe_tickers(db_path: str) -> List[dict]:
    """Return all promoted universe_tickers rows."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_hybrid_tables(conn)
        rows = conn.execute(
            """SELECT ticker, requested_by, sector_label, coverage_count,
                      added_to_ingest, added_at
               FROM universe_tickers ORDER BY coverage_count DESC"""
        ).fetchall()
        cols = ['ticker', 'requested_by', 'sector_label', 'coverage_count',
                'added_to_ingest', 'added_at']
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


def get_staged_tickers(db_path: str, user_id: Optional[str] = None) -> List[dict]:
    """Return ticker_staging rows, optionally filtered by user."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_hybrid_tables(conn)
        if user_id:
            rows = conn.execute(
                """SELECT id, ticker, user_id, requested_at, coverage_count,
                          promoted, promoted_at, rejection_reason
                   FROM ticker_staging WHERE user_id = ? ORDER BY requested_at DESC""",
                (user_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, ticker, user_id, requested_at, coverage_count,
                          promoted, promoted_at, rejection_reason
                   FROM ticker_staging ORDER BY requested_at DESC"""
            ).fetchall()
        cols = ['id', 'ticker', 'user_id', 'requested_at', 'coverage_count',
                'promoted', 'promoted_at', 'rejection_reason']
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


def log_engagement_event(
    db_path: str,
    user_id: str,
    event_type: str,
    ticker: Optional[str] = None,
    pattern_type: Optional[str] = None,
    sector: Optional[str] = None,
) -> None:
    """Insert a user engagement event row."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_hybrid_tables(conn)
        conn.execute(
            """INSERT INTO user_engagement_events
               (user_id, event_type, ticker, pattern_type, sector, timestamp)
               VALUES (?,?,?,?,?,?)""",
            (user_id, event_type, ticker, pattern_type, sector, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_engagement_events(
    db_path: str, user_id: str, limit: int = 100
) -> List[dict]:
    """Return recent engagement events for a user."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_hybrid_tables(conn)
        rows = conn.execute(
            """SELECT id, user_id, event_type, ticker, pattern_type, sector, timestamp
               FROM user_engagement_events WHERE user_id = ?
               ORDER BY timestamp DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        cols = ['id', 'user_id', 'event_type', 'ticker', 'pattern_type', 'sector', 'timestamp']
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


# ── tip_feedback ────────────────────────────────────────────────────────────────

_DDL_TIP_FEEDBACK = """
CREATE TABLE IF NOT EXISTS tip_feedback (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT NOT NULL,
    tip_id       INTEGER,
    pattern_id   INTEGER,
    outcome      TEXT NOT NULL,
    submitted_at TEXT NOT NULL
)
"""


def ensure_tip_feedback_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_TIP_FEEDBACK)
    conn.commit()


def log_tip_feedback(
    db_path: str,
    user_id: str,
    outcome: str,
    tip_id: Optional[int] = None,
    pattern_id: Optional[int] = None,
) -> dict:
    """Record a user-reported tip outcome. Returns the inserted row."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_tip_feedback_table(conn)
        cur = conn.execute(
            """INSERT INTO tip_feedback (user_id, tip_id, pattern_id, outcome, submitted_at)
               VALUES (?, ?, ?, ?, ?)""",
            (user_id, tip_id, pattern_id, outcome, now),
        )
        conn.commit()
        return {
            'id':           cur.lastrowid,
            'user_id':      user_id,
            'tip_id':       tip_id,
            'pattern_id':   pattern_id,
            'outcome':      outcome,
            'submitted_at': now,
        }
    finally:
        conn.close()


def get_tip_performance(db_path: str, user_id: str) -> dict:
    """
    Return a simple performance summary for a user by joining tip_delivery_log
    with tip_feedback.

    Returns counts by outcome and a win_rate based on resolved tips.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        ensure_tip_feedback_table(conn)

        tips_sent = conn.execute(
            "SELECT COUNT(*) FROM tip_delivery_log WHERE user_id = ? AND success = 1",
            (user_id,),
        ).fetchone()[0]

        rows = conn.execute(
            """SELECT outcome, COUNT(*) FROM tip_feedback
               WHERE user_id = ? GROUP BY outcome""",
            (user_id,),
        ).fetchall()

        by_outcome: Dict[str, int] = {r[0]: r[1] for r in rows}

        hit_t1 = by_outcome.get('hit_t1', 0)
        hit_t2 = by_outcome.get('hit_t2', 0)
        hit_t3 = by_outcome.get('hit_t3', 0)
        stopped_out = by_outcome.get('stopped_out', 0)
        pending = by_outcome.get('pending', 0)
        skipped = by_outcome.get('skipped', 0)

        wins = hit_t1 + hit_t2 + hit_t3
        resolved = wins + stopped_out
        win_rate = round(wins / resolved * 100, 1) if resolved > 0 else None

        history = conn.execute(
            """SELECT f.id, f.tip_id, f.pattern_id, f.outcome, f.submitted_at,
                      p.ticker, p.pattern_type
               FROM tip_feedback f
               LEFT JOIN pattern_signals p ON p.id = f.pattern_id
               WHERE f.user_id = ?
               ORDER BY f.submitted_at DESC
               LIMIT 50""",
            (user_id,),
        ).fetchall()
        history_cols = ['id', 'tip_id', 'pattern_id', 'outcome', 'submitted_at', 'ticker', 'pattern_type']

        return {
            'tips_sent':    tips_sent,
            'hit_t1':       hit_t1,
            'hit_t2':       hit_t2,
            'hit_t3':       hit_t3,
            'stopped_out':  stopped_out,
            'pending':      pending,
            'skipped':      skipped,
            'win_rate_pct': win_rate,
            'history':      [dict(zip(history_cols, r)) for r in history],
        }
    finally:
        conn.close()


def get_user_watchlist_tickers(db_path: str, user_id: str) -> List[str]:
    """Return the list of ticker strings in the user's portfolio (upper-case)."""
    holdings = get_portfolio(db_path, user_id)
    return list({h['ticker'].upper() for h in holdings if h.get('ticker')})


def update_tip_config(
    db_path: str,
    user_id: str,
    tip_delivery_time: Optional[str] = None,
    tip_delivery_timezone: Optional[str] = None,
    tip_markets: Optional[list] = None,
    tip_timeframes: Optional[list] = None,
    tip_pattern_types: Optional[list] = None,
    account_size: Optional[float] = None,
    max_risk_per_trade_pct: Optional[float] = None,
    account_currency: Optional[str] = None,
    tier: Optional[str] = None,
) -> dict:
    """
    Upsert tip configuration columns in user_preferences.
    Only provided (non-None) fields are updated.
    Returns the full updated preferences row.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        # Ensure row exists
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences (user_id) VALUES (?)", (user_id,)
        )
        updates = []
        params = []
        if tip_delivery_time is not None:
            updates.append("tip_delivery_time = ?"); params.append(tip_delivery_time)
        if tip_delivery_timezone is not None:
            updates.append("tip_delivery_timezone = ?"); params.append(tip_delivery_timezone)
        if tip_markets is not None:
            updates.append("tip_markets = ?"); params.append(json.dumps(tip_markets))
        if tip_timeframes is not None:
            updates.append("tip_timeframes = ?"); params.append(json.dumps(tip_timeframes))
        if tip_pattern_types is not None:
            updates.append("tip_pattern_types = ?"); params.append(json.dumps(tip_pattern_types))
        if account_size is not None:
            updates.append("account_size = ?"); params.append(account_size)
        if max_risk_per_trade_pct is not None:
            updates.append("max_risk_per_trade_pct = ?"); params.append(max_risk_per_trade_pct)
        if account_currency is not None:
            updates.append("account_currency = ?"); params.append(account_currency)
        if tier is not None:
            updates.append("tier = ?"); params.append(tier)
        if updates:
            params.append(user_id)
            conn.execute(
                f"UPDATE user_preferences SET {', '.join(updates)} WHERE user_id = ?",
                params,
            )
            conn.commit()
        row = conn.execute(
            """SELECT user_id, tier, tip_delivery_time, tip_delivery_timezone,
                      tip_markets, tip_timeframes, tip_pattern_types,
                      account_size, max_risk_per_trade_pct, account_currency
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        if row is None:
            return {}
        cols = ['user_id', 'tier', 'tip_delivery_time', 'tip_delivery_timezone',
                'tip_markets', 'tip_timeframes', 'tip_pattern_types',
                'account_size', 'max_risk_per_trade_pct', 'account_currency']
        d = dict(zip(cols, row))
        for json_col in ('tip_markets', 'tip_timeframes', 'tip_pattern_types'):
            try:
                d[json_col] = json.loads(d[json_col]) if d[json_col] else None
            except (json.JSONDecodeError, TypeError):
                d[json_col] = None
        return d
    finally:
        conn.close()

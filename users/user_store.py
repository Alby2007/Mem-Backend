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
    delivery_time          TEXT DEFAULT '07:30',
    timezone               TEXT DEFAULT 'Europe/London',
    tier                   TEXT DEFAULT 'free',
    tip_delivery_time      TEXT DEFAULT '07:30',
    tip_delivery_timezone  TEXT DEFAULT 'Europe/London',
    tip_markets            TEXT DEFAULT '["equities"]',
    tip_timeframes         TEXT DEFAULT '["1h"]',
    tip_pattern_types      TEXT,
    account_size           REAL,
    max_risk_per_trade_pct REAL DEFAULT 1.0,
    account_currency       TEXT DEFAULT 'GBP',
    is_dev                 INTEGER NOT NULL DEFAULT 0
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
    detected_at   TEXT NOT NULL,
    expires_at    TEXT
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
    message_length          INTEGER,
    pattern_meta            TEXT
)
"""

# New columns added to user_preferences after initial schema creation
_PREFERENCES_MIGRATIONS = [
    "ALTER TABLE user_preferences ADD COLUMN tier TEXT DEFAULT 'free'",
    "ALTER TABLE user_preferences ADD COLUMN tip_delivery_time TEXT DEFAULT '07:30'",
    "ALTER TABLE user_preferences ADD COLUMN tip_delivery_timezone TEXT DEFAULT 'Europe/London'",
    "ALTER TABLE user_preferences ADD COLUMN tip_markets TEXT DEFAULT '[\"equities\"]'",
    "ALTER TABLE user_preferences ADD COLUMN tip_timeframes TEXT DEFAULT '[\"1h\"]'",
    "ALTER TABLE user_preferences ADD COLUMN tip_pattern_types TEXT",
    "ALTER TABLE user_preferences ADD COLUMN account_size REAL",
    "ALTER TABLE user_preferences ADD COLUMN max_risk_per_trade_pct REAL DEFAULT 1.0",
    "ALTER TABLE user_preferences ADD COLUMN account_currency TEXT DEFAULT 'GBP'",
    "ALTER TABLE user_preferences ADD COLUMN available_cash REAL DEFAULT NULL",
    "ALTER TABLE user_preferences ADD COLUMN cash_currency TEXT DEFAULT 'GBP'",
    "ALTER TABLE user_preferences ADD COLUMN is_dev INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE user_preferences ADD COLUMN trader_level TEXT DEFAULT 'developing'",
    "ALTER TABLE user_preferences ADD COLUMN style_risk_tolerance TEXT DEFAULT 'moderate'",
    "ALTER TABLE user_preferences ADD COLUMN style_timeframe TEXT DEFAULT 'swing'",
    "ALTER TABLE user_preferences ADD COLUMN style_sector_focus TEXT DEFAULT '[]'",
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
    delivery_time: str = '07:30',
    timezone_str: str = 'Europe/London',
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
               (user_id, telegram_chat_id, delivery_time, timezone, tier)
               VALUES (?, ?, ?, ?, 'free')""",
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
                      selected_risk, telegram_chat_id, delivery_time, timezone,
                      tier, max_risk_per_trade_pct, is_dev,
                      tip_delivery_time, tip_delivery_timezone, trader_level
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        if row is None:
            return None
        cols = ['user_id', 'onboarding_complete', 'selected_sectors',
                'selected_risk', 'telegram_chat_id', 'delivery_time', 'timezone',
                'tier', 'max_risk_per_trade_pct', 'is_dev',
                'tip_delivery_time', 'tip_delivery_timezone', 'trader_level']
        d = dict(zip(cols, row))
        try:
            d['selected_sectors'] = json.loads(d['selected_sectors'] or '[]')
        except (json.JSONDecodeError, TypeError):
            d['selected_sectors'] = []
        d['telegram_chat_id'] = decrypt_field(d.get('telegram_chat_id'))
        d['tier'] = d.get('tier') or 'free'
        d['trader_level'] = d.get('trader_level') or 'developing'
        return d
    finally:
        conn.close()


def set_user_dev(db_path: str, user_id: str, is_dev: bool) -> None:
    """Set or clear the is_dev flag for a user."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        conn.execute(
            "UPDATE user_preferences SET is_dev=? WHERE user_id=?",
            (1 if is_dev else 0, user_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_user_by_chat_id(db_path: str, chat_id: str) -> Optional[str]:
    """
    Return the user_id whose decrypted telegram_chat_id matches chat_id.
    Returns None if no match found (unlinked / unknown sender).
    """
    if not chat_id:
        return None
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        rows = conn.execute(
            "SELECT user_id, telegram_chat_id FROM user_preferences "
            "WHERE telegram_chat_id IS NOT NULL"
        ).fetchall()
        chat_id_str = str(chat_id)
        for row in rows:
            try:
                decrypted = decrypt_field(row[1])
                if decrypted and str(decrypted) == chat_id_str:
                    return row[0]
            except Exception:
                continue
        return None
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
            "INSERT OR IGNORE INTO user_preferences (user_id, tier) VALUES (?, 'free')",
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


# Timeframe style → PatternAdapter timeframe strings
_STYLE_TF_MAP: dict = {
    'scalp':    ['15m'],
    'intraday': ['1h'],
    'swing':    ['4h', '1d'],
    'position': ['1d', '1w'],
}


def get_style_prefs(db_path: str, user_id: str) -> dict:
    """Return the three style preference fields for a user with safe defaults."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            """SELECT style_risk_tolerance, style_timeframe, style_sector_focus
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {'style_risk_tolerance': 'moderate', 'style_timeframe': 'swing', 'style_sector_focus': []}
    risk, tf, sectors_raw = row
    try:
        sectors = json.loads(sectors_raw or '[]')
    except (json.JSONDecodeError, TypeError):
        sectors = []
    return {
        'style_risk_tolerance': risk or 'moderate',
        'style_timeframe':      tf   or 'swing',
        'style_sector_focus':   sectors,
        'style_tf_values':      _STYLE_TF_MAP.get(tf or 'swing', ['4h', '1d']),
    }


def update_style_prefs(
    db_path: str,
    user_id: str,
    *,
    style_risk_tolerance: Optional[str] = None,
    style_timeframe: Optional[str] = None,
    style_sector_focus: Optional[List[str]] = None,
) -> dict:
    """
    Update style preference fields. Only provided (non-None) fields are changed.
    Returns the updated style prefs dict.
    """
    _VALID_RISK = frozenset({'conservative', 'moderate', 'aggressive'})
    _VALID_TF   = frozenset(_STYLE_TF_MAP.keys())
    if style_risk_tolerance and style_risk_tolerance not in _VALID_RISK:
        raise ValueError(f"style_risk_tolerance must be one of {sorted(_VALID_RISK)}")
    if style_timeframe and style_timeframe not in _VALID_TF:
        raise ValueError(f"style_timeframe must be one of {sorted(_VALID_TF)}")

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences (user_id, tier) VALUES (?, 'free')",
            (user_id,),
        )
        if style_risk_tolerance is not None:
            conn.execute(
                "UPDATE user_preferences SET style_risk_tolerance = ? WHERE user_id = ?",
                (style_risk_tolerance, user_id),
            )
        if style_timeframe is not None:
            conn.execute(
                "UPDATE user_preferences SET style_timeframe = ? WHERE user_id = ?",
                (style_timeframe, user_id),
            )
        if style_sector_focus is not None:
            conn.execute(
                "UPDATE user_preferences SET style_sector_focus = ? WHERE user_id = ?",
                (json.dumps(style_sector_focus), user_id),
            )
        conn.commit()
    finally:
        conn.close()
    return get_style_prefs(db_path, user_id)


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


def upsert_single_holding(
    db_path: str,
    user_id: str,
    ticker: str,
    quantity: Optional[float] = None,
    avg_cost: Optional[float] = None,
    sector: Optional[str] = None,
) -> dict:
    """
    Add or update a single holding in the user's portfolio without touching other rows.
    If the ticker already exists, updates quantity and avg_cost (if provided).
    Returns the updated holding dict.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    ticker = ticker.upper().strip()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        existing = conn.execute(
            "SELECT quantity, avg_cost, sector FROM user_portfolios WHERE user_id=? AND ticker=?",
            (user_id, ticker),
        ).fetchone()
        if existing:
            new_qty  = quantity  if quantity  is not None else existing[0]
            new_cost = avg_cost  if avg_cost  is not None else existing[1]
            new_sec  = sector    if sector    is not None else existing[2]
            conn.execute(
                """UPDATE user_portfolios
                   SET quantity=?, avg_cost=?, sector=?, submitted_at=?
                   WHERE user_id=? AND ticker=?""",
                (new_qty, new_cost, new_sec, now_iso, user_id, ticker),
            )
        else:
            conn.execute(
                """INSERT INTO user_portfolios (user_id, ticker, quantity, avg_cost, sector, submitted_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, ticker, quantity, avg_cost, sector, now_iso),
            )
        conn.commit()
        return {'user_id': user_id, 'ticker': ticker, 'quantity': quantity,
                'avg_cost': avg_cost, 'sector': sector, 'submitted_at': now_iso}
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


_PORTFOLIO_SIGNAL_PREDS = ('signal_direction', 'conviction_tier', 'last_price', 'price_target', 'upside_pct')


def get_portfolio_with_signals(db_path: str, user_id: str) -> List[dict]:
    """Return holdings enriched with up to 5 KB signal atoms per ticker (lowercase lookup)."""
    holdings = get_portfolio(db_path, user_id)
    if not holdings:
        return []
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        placeholders = ','.join('?' for _ in _PORTFOLIO_SIGNAL_PREDS)
        for h in holdings:
            tk_lower = h['ticker'].lower()
            rows = conn.execute(
                f'SELECT predicate, object FROM facts WHERE subject=? AND predicate IN ({placeholders})',
                (tk_lower, *_PORTFOLIO_SIGNAL_PREDS)
            ).fetchall()
            atoms = {p: v for p, v in rows}
            h['signal_direction'] = atoms.get('signal_direction')
            h['conviction_tier'] = atoms.get('conviction_tier')
            h['last_price'] = atoms.get('last_price')
            h['price_target'] = atoms.get('price_target')
            h['upside_pct'] = atoms.get('upside_pct')
            try:
                lp  = float(h['last_price']) if h.get('last_price') else None
                ac  = float(h['avg_cost'])   if h.get('avg_cost')   else None
                qty = float(h.get('quantity') or 0)
                if lp and ac and ac > 0:
                    pnl_pct = round((lp - ac) / ac * 100, 2)
                    pnl_val = round((lp - ac) * qty, 2) if qty else None
                    h['unrealized_pnl_pct'] = pnl_pct
                    h['unrealized_pnl']     = pnl_val
                else:
                    h['unrealized_pnl_pct'] = None
                    h['unrealized_pnl']     = None
            except (TypeError, ValueError):
                h['unrealized_pnl_pct'] = None
                h['unrealized_pnl']     = None
    finally:
        conn.close()
    return holdings


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
      kb_regime, kb_signal_dir, expires_at (optional).
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        # Migrate: add expires_at column if the table pre-dates this change
        existing_cols = {r[1] for r in conn.execute('PRAGMA table_info(pattern_signals)')}
        if 'expires_at' not in existing_cols:
            conn.execute('ALTER TABLE pattern_signals ADD COLUMN expires_at TEXT')
            conn.commit()
        cur = conn.execute(
            """INSERT INTO pattern_signals
               (ticker, pattern_type, direction, zone_high, zone_low,
                zone_size_pct, timeframe, formed_at, status,
                quality_score, kb_conviction, kb_regime, kb_signal_dir,
                alerted_users, detected_at, expires_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'[]',?,?)""",
            (
                signal['ticker'], signal['pattern_type'], signal['direction'],
                signal['zone_high'], signal['zone_low'], signal['zone_size_pct'],
                signal['timeframe'], signal['formed_at'],
                signal.get('status', 'open'), signal.get('quality_score'),
                signal.get('kb_conviction', ''), signal.get('kb_regime', ''),
                signal.get('kb_signal_dir', ''), now_iso,
                signal.get('expires_at'),
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
    """Return open (non-filled, non-broken) pattern_signals rows, best quality first."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        clauses = ["status NOT IN ('filled','broken')", "quality_score >= ?"]
        params: list = [min_quality]
        if ticker:
            clauses.append("ticker LIKE ?")
            params.append(f'%{ticker}%')
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
        if ticker:
            # Specific ticker search — no diversity enforcement needed
            sql = f"""SELECT id, ticker, pattern_type, direction, zone_high, zone_low,
                             zone_size_pct, timeframe, formed_at, status, filled_at,
                             quality_score, kb_conviction, kb_regime, kb_signal_dir,
                             alerted_users, detected_at
                      FROM pattern_signals
                      WHERE {where}
                      ORDER BY quality_score DESC, detected_at DESC LIMIT ?"""
        else:
            # No ticker filter — pick best 3 per ticker by quality so all
            # tickers get representation with their highest-conviction setups.
            sql = f"""SELECT id, ticker, pattern_type, direction, zone_high, zone_low,
                             zone_size_pct, timeframe, formed_at, status, filled_at,
                             quality_score, kb_conviction, kb_regime, kb_signal_dir,
                             alerted_users, detected_at
                      FROM (
                          SELECT *,
                                 ROW_NUMBER() OVER (
                                     PARTITION BY ticker
                                     ORDER BY quality_score DESC, detected_at DESC
                                 ) AS rn
                          FROM pattern_signals
                          WHERE {where}
                      )
                      WHERE rn <= 3
                      ORDER BY quality_score DESC, detected_at DESC LIMIT ?"""
        rows = conn.execute(sql, params).fetchall()
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
    pattern_meta: Optional[list] = None,
) -> dict:
    """Insert a tip_delivery_log row. Returns the inserted row."""
    import json as _json
    now_iso = datetime.now(timezone.utc).isoformat()
    if local_date is None:
        local_date = now_iso[:10]
    meta_json = _json.dumps(pattern_meta) if pattern_meta else None
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        # Ensure pattern_meta column exists (idempotent)
        try:
            conn.execute('ALTER TABLE tip_delivery_log ADD COLUMN pattern_meta TEXT')
            conn.commit()
        except Exception:
            pass
        cur = conn.execute(
            """INSERT INTO tip_delivery_log
               (user_id, pattern_signal_id, delivered_at, delivered_at_local_date,
                success, message_length, pattern_meta)
               VALUES (?,?,?,?,?,?,?)""",
            (user_id, pattern_signal_id, now_iso, local_date,
             int(success), message_length, meta_json),
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
            'pattern_meta':           pattern_meta,
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


def get_available_cash(db_path: str, user_id: str) -> dict:
    """Return the user's stored available_cash and cash_currency. Values may be None."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            "SELECT available_cash, cash_currency FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if row is None:
            return {'available_cash': None, 'cash_currency': 'GBP'}
        return {'available_cash': row[0], 'cash_currency': row[1] or 'GBP'}
    finally:
        conn.close()


def update_available_cash(
    db_path: str,
    user_id: str,
    amount: float,
    cash_currency: str = 'GBP',
) -> dict:
    """
    Set the user's available_cash and cash_currency.
    Negative values are stored as-is (overcommitted state).
    Returns { 'user_id', 'available_cash', 'cash_currency' }.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences (user_id) VALUES (?)", (user_id,)
        )
        conn.execute(
            """UPDATE user_preferences
               SET available_cash = ?, cash_currency = ?
               WHERE user_id = ?""",
            (amount, cash_currency.upper(), user_id),
        )
        conn.commit()
        return {'user_id': user_id, 'available_cash': amount, 'cash_currency': cash_currency.upper()}
    finally:
        conn.close()


def deduct_from_cash(
    db_path: str,
    user_id: str,
    amount: float,
    tip_id: Optional[int] = None,
) -> dict:
    """
    Deduct `amount` from the user's available_cash.

    Idempotency: if `tip_id` is provided, checks tip_feedback for an existing
    'taking_it' row with the same tip_id — if found, skips deduction and
    returns {'skipped': True}.

    Negative balances are stored as-is (overcommitted state, not masked).

    Returns dict with keys:
      skipped      bool — True if already deducted for this tip_id
      new_balance  float | None
      deducted     float
      is_negative  bool
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        ensure_tip_feedback_table(conn)

        # Idempotency check
        if tip_id is not None:
            existing = conn.execute(
                """SELECT 1 FROM tip_feedback
                   WHERE user_id = ? AND tip_id = ? AND outcome = 'taking_it'
                   LIMIT 1""",
                (user_id, tip_id),
            ).fetchone()
            if existing:
                cash_row = conn.execute(
                    "SELECT available_cash FROM user_preferences WHERE user_id = ?",
                    (user_id,),
                ).fetchone()
                current = cash_row[0] if cash_row else None
                return {
                    'skipped':     True,
                    'new_balance': current,
                    'deducted':    0.0,
                    'is_negative': bool(current is not None and current < 0),
                }

        # Fetch current balance
        row = conn.execute(
            "SELECT available_cash FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if row is None or row[0] is None:
            return {
                'skipped':     False,
                'new_balance': None,
                'deducted':    0.0,
                'is_negative': False,
            }

        new_balance = row[0] - amount
        conn.execute(
            "UPDATE user_preferences SET available_cash = ? WHERE user_id = ?",
            (new_balance, user_id),
        )
        conn.commit()
        return {
            'skipped':     False,
            'new_balance': new_balance,
            'deducted':    amount,
            'is_negative': new_balance < 0,
        }
    finally:
        conn.close()


def already_sent_this_week_slot(
    db_path: str,
    user_id: str,
    weekday_name: str,
    week_monday_date_str: str,
) -> bool:
    """
    Return True if a successful tip batch was already delivered this ISO week
    on the given weekday slot (e.g. 'monday' or 'wednesday').

    week_monday_date_str: ISO date string of Monday of the current week (YYYY-MM-DD).
    Queries tip_delivery_log for all success=1 rows in [monday, monday+6] and
    checks whether any fall on the requested weekday.
    """
    from datetime import date as _date, timedelta as _td
    monday = _date.fromisoformat(week_monday_date_str)
    sunday = monday + _td(days=6)
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        rows = conn.execute(
            """SELECT delivered_at_local_date FROM tip_delivery_log
               WHERE user_id = ? AND success = 1
                 AND delivered_at_local_date >= ? AND delivered_at_local_date <= ?""",
            (user_id, week_monday_date_str, sunday.isoformat()),
        ).fetchall()
        target_weekday = weekday_name.lower()
        _WEEKDAY_NAMES = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
        for (d_str,) in rows:
            try:
                d = _date.fromisoformat(d_str)
                if _WEEKDAY_NAMES[d.weekday()] == target_weekday:
                    return True
            except Exception:
                pass
        return False
    finally:
        conn.close()


def get_monday_pattern_meta(
    db_path: str,
    user_id: str,
    week_monday_date_str: str,
) -> list:
    """
    Return the pattern_meta list stored in Monday's tip delivery log for this
    ISO week, or [] if none found.
    """
    import json as _json
    from datetime import date as _date
    monday = _date.fromisoformat(week_monday_date_str)
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            """SELECT pattern_meta FROM tip_delivery_log
               WHERE user_id = ? AND success = 1
                 AND delivered_at_local_date = ?
               ORDER BY id DESC LIMIT 1""",
            (user_id, monday.isoformat()),
        ).fetchone()
        if row and row[0]:
            try:
                return _json.loads(row[0]) or []
            except Exception:
                pass
        return []
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
    """Return the user's tier. Defaults to 'free'."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        row = conn.execute(
            "SELECT tier FROM user_preferences WHERE user_id = ?", (user_id,)
        ).fetchone()
        if row and row[0]:
            return row[0]
        return 'free'
    finally:
        conn.close()


def set_user_tier(db_path: str, user_id: str, tier: str) -> None:
    """Set the user's tier in user_preferences, upserting the row if needed."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            """INSERT INTO user_preferences (user_id, tier)
               VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET tier = excluded.tier""",
            (user_id, tier),
        )
        conn.commit()
    finally:
        conn.close()


_VALID_TRADER_LEVELS = frozenset({'beginner', 'developing', 'experienced', 'quant'})


def set_trader_level(db_path: str, user_id: str, level: str) -> None:
    """Set the trader_level for a user. level must be one of the four valid values."""
    if level not in _VALID_TRADER_LEVELS:
        raise ValueError(f"Invalid trader_level '{level}'. Must be one of: {sorted(_VALID_TRADER_LEVELS)}")
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        conn.execute(
            """INSERT INTO user_preferences (user_id, trader_level)
               VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET trader_level = excluded.trader_level""",
            (user_id, level),
        )
        conn.commit()
    finally:
        conn.close()


def get_today_chat_count(db_path: str, user_id: str) -> int:
    """
    Return the number of user chat messages sent today (in UTC).
    Queries conv_messages for the session belonging to this user
    where role='user' and timestamp falls on today's UTC date.
    Returns 0 if conversation_store tables don't exist yet.
    """
    session_id = f"TRADING_{user_id}"
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            row = conn.execute(
                """SELECT COUNT(*) FROM conv_messages
                   WHERE session_id = ?
                     AND role = 'user'
                     AND DATE(timestamp) = DATE('now')""",
                (session_id,),
            ).fetchone()
            return row[0] if row else 0
        except Exception:
            return 0
        finally:
            conn.close()
    except Exception:
        return 0


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


# ── tip_followups table ────────────────────────────────────────────────────────

_DDL_TIP_FOLLOWUPS = """
CREATE TABLE IF NOT EXISTS tip_followups (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id        TEXT    NOT NULL,
    ticker         TEXT    NOT NULL,
    direction      TEXT    NOT NULL DEFAULT 'bullish',
    entry_price    REAL,
    stop_loss      REAL,
    target_1       REAL,
    target_2       REAL,
    target_3       REAL,
    tracking_target TEXT   DEFAULT 'T1',
    status         TEXT    NOT NULL DEFAULT 'watching',
    alert_level    TEXT,
    last_alert_at  TEXT,
    tip_id         TEXT,
    opened_at      TEXT    NOT NULL,
    closed_at      TEXT
)
"""

_DDL_TIP_FOLLOWUPS_IDX = """
CREATE INDEX IF NOT EXISTS idx_tip_followups_watching
ON tip_followups(status, user_id)
"""


_FOLLOWUP_EXPIRY_DAYS = {'15m': 2, '1h': 5, '4h': 14, '1d': 28}
_FOLLOWUP_EXPIRY_DEFAULT = 14


def _ensure_tip_followups_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_TIP_FOLLOWUPS)
    conn.execute(_DDL_TIP_FOLLOWUPS_IDX)
    # Idempotent column migrations
    cols = {row[1] for row in conn.execute("PRAGMA table_info(tip_followups)")}
    if 'opened_at' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN opened_at TEXT NOT NULL DEFAULT ''")
        conn.execute("UPDATE tip_followups SET opened_at = created_at WHERE opened_at = ''")
    if 'closed_at' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN closed_at TEXT")
    # Living portfolio briefing columns
    if 'pattern_type' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN pattern_type TEXT")
    if 'timeframe' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN timeframe TEXT")
    if 'zone_low' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN zone_low REAL")
    if 'zone_high' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN zone_high REAL")
    if 'expires_at' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN expires_at TEXT")
    if 'regime_at_entry' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN regime_at_entry TEXT")
    if 'conviction_at_entry' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN conviction_at_entry TEXT")
    # Profit target tracking columns
    if 'peak_price' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN peak_price REAL")
    if 'peak_price_updated_at' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN peak_price_updated_at TEXT")
    if 'alerted_peak_price' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN alerted_peak_price REAL")
    if 'thesis_id' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN thesis_id TEXT")
        try:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tip_followups_thesis "
                "ON tip_followups(thesis_id)"
            )
        except Exception:
            pass
    # Journal enrichment columns (P3)
    if 'holding_hours' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN holding_hours INTEGER")
    if 'user_note' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN user_note TEXT")
    if 'partial_pct' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN partial_pct INTEGER DEFAULT 100")
    if 'r_multiple' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN r_multiple REAL")
    if 'exit_price' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN exit_price REAL")
    if 'pattern_id' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN pattern_id INTEGER")
    if 'position_size' not in cols:
        conn.execute("ALTER TABLE tip_followups ADD COLUMN position_size REAL")
    # 'active' = user consciously accepted; 'watching' = auto-created at tip send
    # status column already exists in DDL with DEFAULT 'watching'
    conn.commit()


_FOLLOWUP_COLS = [
    'id', 'user_id', 'ticker', 'direction', 'entry_price',
    'stop_loss', 'target_1', 'target_2', 'target_3',
    'tracking_target', 'status', 'alert_level', 'last_alert_at',
    'tip_id', 'opened_at', 'closed_at',
    'pattern_type', 'timeframe', 'zone_low', 'zone_high',
    'expires_at', 'regime_at_entry', 'conviction_at_entry',
    'peak_price', 'peak_price_updated_at', 'alerted_peak_price',
    'thesis_id',
]
_FOLLOWUP_SELECT = """
    SELECT id, user_id, ticker, direction, entry_price,
           stop_loss, target_1, target_2, target_3,
           tracking_target, status, alert_level, last_alert_at,
           tip_id, opened_at, closed_at,
           pattern_type, timeframe, zone_low, zone_high,
           expires_at, regime_at_entry, conviction_at_entry,
           peak_price, peak_price_updated_at, alerted_peak_price,
           thesis_id
    FROM tip_followups
"""


def get_watching_followups(db_path: str) -> List[dict]:
    """Return all tip followups with status in ('watching','active') across all users."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            _FOLLOWUP_SELECT +
            "WHERE status IN ('watching','active') ORDER BY opened_at DESC"
        ).fetchall()
        return [dict(zip(_FOLLOWUP_COLS, r)) for r in rows]
    finally:
        conn.close()


def get_user_open_positions(db_path: str, user_id: str) -> List[dict]:
    """Return open (watching + active) followups for a single user, newest first."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            _FOLLOWUP_SELECT +
            "WHERE user_id = ? AND status IN ('watching','active') ORDER BY opened_at DESC",
            (user_id,),
        ).fetchall()
        return [dict(zip(_FOLLOWUP_COLS, r)) for r in rows]
    finally:
        conn.close()


def get_recently_closed_positions(db_path: str, user_id: str, since_date: str) -> List[dict]:
    """
    Return followups closed since `since_date` (ISO date string, e.g. '2026-03-01').
    Status values included: closed, expired, stopped_out, hit_t1, hit_t2.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            _FOLLOWUP_SELECT +
            """WHERE user_id = ?
               AND status NOT IN ('watching','active')
               AND closed_at >= ?
               ORDER BY closed_at DESC""",
            (user_id, since_date),
        ).fetchall()
        return [dict(zip(_FOLLOWUP_COLS, r)) for r in rows]
    finally:
        conn.close()


def expire_stale_followups(db_path: str) -> List[dict]:
    """
    Close any followups whose expires_at has passed and status is still watching/active.
    Returns list of expired rows (for inclusion in next scheduled delivery message).
    Does NOT send Telegram — caller is responsible for surfacing in briefing.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            _FOLLOWUP_SELECT +
            """WHERE expires_at IS NOT NULL
               AND expires_at <= ?
               AND status IN ('watching','active')""",
            (now_iso,),
        ).fetchall()
        expired = [dict(zip(_FOLLOWUP_COLS, r)) for r in rows]
        if expired:
            ids = [e['id'] for e in expired]
            conn.execute(
                f"UPDATE tip_followups SET status='expired', closed_at=? WHERE id IN ({','.join('?'*len(ids))})",
                [now_iso] + ids,
            )
            conn.commit()
        return expired
    finally:
        conn.close()


def get_kb_changes_since(
    db_path: str,
    since_iso: str,
    tickers: Optional[List[str]] = None,
) -> List[dict]:
    """
    Return significant KB atom changes since `since_iso` (ISO datetime string).
    Filters to _KB_CHANGE_PREDICATES only. Deduplicates by (subject, predicate),
    keeping the latest value. Optionally scoped to a list of tickers.

    Returns list of dicts: {ticker, predicate, value, changed_at}.
    """
    _KB_CHANGE_PREDICATES = (
        'regime_label', 'market_regime', 'conviction_tier',
        'macro_signal', 'geopolitical_risk', 'sector_tailwind',
        'pre_earnings_flag', 'signal_direction',
        # Additional predicates confirmed written in KB
        'price_regime', 'macro_event_risk', 'smart_money_signal',
        'flow_conviction', 'uk_market_regime', 'volatility_regime',
    )
    placeholders = ','.join('?' * len(_KB_CHANGE_PREDICATES))
    params: list = list(_KB_CHANGE_PREDICATES) + [since_iso]

    base_sql = f"""
        SELECT subject, predicate, object, MAX(timestamp) as changed_at
        FROM facts
        WHERE predicate IN ({placeholders})
          AND timestamp >= ?
    """
    if tickers:
        lower_tickers = [t.lower() for t in tickers]
        base_sql += f" AND LOWER(subject) IN ({','.join('?'*len(lower_tickers))})"
        params += lower_tickers
    base_sql += " GROUP BY subject, predicate ORDER BY changed_at DESC"

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        rows = conn.execute(base_sql, params).fetchall()
        return [
            {'ticker': r[0].upper(), 'predicate': r[1], 'value': r[2], 'changed_at': r[3]}
            for r in rows
        ]
    except Exception:
        return []
    finally:
        conn.close()


def update_followup_status(
    db_path: str,
    followup_id: int,
    status: Optional[str] = None,
    alert_level: Optional[str] = None,
    last_alert_at: Optional[str] = None,
    closed_at: Optional[str] = None,
    tracking_target: Optional[str] = None,
    stop_loss: Optional[float] = None,
    holding_hours: Optional[int] = None,
    user_note: Optional[str] = None,
    partial_pct: Optional[int] = None,
    r_multiple: Optional[float] = None,
    exit_price: Optional[float] = None,
) -> None:
    """Update status, alert_level, last_alert_at, closed_at, and journal fields for a followup."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        updates = []
        params: list = []
        if status is not None:
            updates.append('status = ?'); params.append(status)
        if alert_level is not None:
            updates.append('alert_level = ?'); params.append(alert_level)
        if last_alert_at is not None:
            updates.append('last_alert_at = ?'); params.append(last_alert_at)
        if closed_at is not None:
            updates.append('closed_at = ?'); params.append(closed_at)
        if tracking_target is not None:
            updates.append('tracking_target = ?'); params.append(tracking_target)
        if stop_loss is not None:
            updates.append('stop_loss = ?'); params.append(stop_loss)
        if holding_hours is not None:
            updates.append('holding_hours = ?'); params.append(holding_hours)
        if user_note is not None:
            updates.append('user_note = ?'); params.append(user_note[:500])
        if partial_pct is not None:
            updates.append('partial_pct = ?'); params.append(partial_pct)
        if r_multiple is not None:
            updates.append('r_multiple = ?'); params.append(r_multiple)
        if exit_price is not None:
            updates.append('exit_price = ?'); params.append(exit_price)
        if not updates:
            return
        params.append(followup_id)
        conn.execute(
            f"UPDATE tip_followups SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        conn.commit()
    finally:
        conn.close()


def _auto_link_thesis(
    conn: sqlite3.Connection,
    user_id: str,
    ticker: str,
) -> tuple:
    """
    Find active thesis_index rows matching (user_id, ticker).

    Returns (linked_thesis_id, candidates):
      - 0 matches: (None, [])
      - 1 match, created within 30 days: (thesis_id, [])   ← auto-link
      - 1 match, older than 30 days:     (None, [match])   ← stale, ask user
      - 2+ matches:                       (None, [all])     ← ambiguous, ask user
    """
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=30)).isoformat()
    ticker_up = ticker.upper()
    try:
        rows = conn.execute(
            """SELECT thesis_id, ticker, direction, thesis_status, created_at,
                      invalidation_condition
               FROM thesis_index
               WHERE user_id=? AND UPPER(ticker)=?
                 AND thesis_status != 'INVALIDATED'
               ORDER BY created_at DESC""",
            (user_id, ticker_up),
        ).fetchall()
    except Exception:
        return (None, [])
    if not rows:
        return (None, [])
    cols = ['thesis_id', 'ticker', 'direction', 'thesis_status', 'created_at',
            'invalidation_condition']
    matches = [dict(zip(cols, r)) for r in rows]
    if len(matches) == 1:
        if matches[0]['created_at'] > cutoff:
            return (matches[0]['thesis_id'], [])   # fresh single match → auto-link
        return (None, matches)   # stale single match → prompt user
    return (None, matches)   # multiple matches → prompt user


def link_followup_thesis(db_path: str, followup_id: int, thesis_id: str) -> None:
    """Set thesis_id on an existing tip_followup row."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        conn.execute(
            "UPDATE tip_followups SET thesis_id=? WHERE id=?",
            (thesis_id, followup_id),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_tip_followup(
    db_path: str,
    user_id: str,
    ticker: str,
    direction: str = 'bullish',
    entry_price: Optional[float] = None,
    stop_loss: Optional[float] = None,
    target_1: Optional[float] = None,
    target_2: Optional[float] = None,
    target_3: Optional[float] = None,
    tip_id: Optional[str] = None,
    pattern_type: Optional[str] = None,
    timeframe: Optional[str] = None,
    zone_low: Optional[float] = None,
    zone_high: Optional[float] = None,
    regime_at_entry: Optional[str] = None,
    conviction_at_entry: Optional[str] = None,
    initial_status: str = 'watching',
) -> tuple:
    """
    Insert a new tip followup record.
    Returns (new_row_id, thesis_candidates) where thesis_candidates is a list
    of thesis_index rows that need user confirmation before linking.
    Empty list means either auto-linked (thesis_id set on the row) or no match.
    initial_status='watching' for auto-created (tip delivery);
    initial_status='active' for user-accepted (taking_it).
    """
    from datetime import timedelta
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        expiry_days = _FOLLOWUP_EXPIRY_DAYS.get(timeframe or '', _FOLLOWUP_EXPIRY_DEFAULT)
        expires_at = (now + timedelta(days=expiry_days)).isoformat()

        # Attempt semi-automatic thesis linkage before insert
        auto_thesis_id, thesis_candidates = _auto_link_thesis(conn, user_id, ticker)

        # Detect whether the server schema has created_at/updated_at columns
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(tip_followups)")}
        has_timestamps = 'created_at' in existing_cols
        if has_timestamps:
            cur = conn.execute(
                """INSERT INTO tip_followups
                   (user_id, ticker, direction, entry_price, stop_loss,
                    target_1, target_2, target_3, tip_id, opened_at, status,
                    pattern_type, timeframe, zone_low, zone_high, expires_at,
                    regime_at_entry, conviction_at_entry, thesis_id,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (user_id, ticker.upper(), direction, entry_price, stop_loss,
                 target_1, target_2, target_3, tip_id, now_iso, initial_status,
                 pattern_type, timeframe, zone_low, zone_high, expires_at,
                 regime_at_entry, conviction_at_entry, auto_thesis_id,
                 now_iso, now_iso),
            )
        else:
            cur = conn.execute(
                """INSERT INTO tip_followups
                   (user_id, ticker, direction, entry_price, stop_loss,
                    target_1, target_2, target_3, tip_id, opened_at, status,
                    pattern_type, timeframe, zone_low, zone_high, expires_at,
                    regime_at_entry, conviction_at_entry, thesis_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (user_id, ticker.upper(), direction, entry_price, stop_loss,
                 target_1, target_2, target_3, tip_id, now_iso, initial_status,
                 pattern_type, timeframe, zone_low, zone_high, expires_at,
                 regime_at_entry, conviction_at_entry, auto_thesis_id),
            )
        conn.commit()
        return cur.lastrowid, thesis_candidates
    finally:
        conn.close()


def update_peak_price(db_path: str, followup_id: int, price: float) -> None:
    """Update peak_price and peak_price_updated_at for a followup (high watermark)."""
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        conn.execute(
            "UPDATE tip_followups SET peak_price = ?, peak_price_updated_at = ? WHERE id = ?",
            (price, now_iso, followup_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_alerted_peak_price(db_path: str, followup_id: int, peak_price: float) -> None:
    """Record that a trailing pullback alert was fired for this peak price."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        conn.execute(
            "UPDATE tip_followups SET alerted_peak_price = ? WHERE id = ?",
            (peak_price, followup_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_user_open_positions_with_thesis(db_path: str, user_id: str) -> List[dict]:
    """
    Same as get_user_open_positions() but ensures thesis_id is included.
    Returns open (watching + active) followups with thesis_id field populated.
    """
    return get_user_open_positions(db_path, user_id)


def create_tip_followup(
    db_path: str,
    user_id: str,
    ticker: str,
    tip_id: Optional[str] = None,
    pattern_id: Optional[int] = None,
    direction: str = 'bullish',
    entry_price: Optional[float] = None,
    stop_loss: Optional[float] = None,
    target_1: Optional[float] = None,
    target_2: Optional[float] = None,
    target_3: Optional[float] = None,
    position_size: Optional[float] = None,
    regime_at_entry: Optional[str] = None,
    conviction_at_entry: Optional[str] = None,
    pattern_type: Optional[str] = None,
    timeframe: Optional[str] = None,
    zone_low: Optional[float] = None,
    zone_high: Optional[float] = None,
) -> int:
    """
    Create a tip followup from a user 'taking_it' action.
    Sets status='active' (user-accepted position, distinct from auto-created 'watching').
    Returns (new_followup_id, thesis_candidates).
    """
    return upsert_tip_followup(
        db_path,
        user_id             = user_id,
        ticker              = ticker,
        direction           = direction,
        entry_price         = entry_price,
        stop_loss           = stop_loss,
        target_1            = target_1,
        target_2            = target_2,
        target_3            = target_3,
        tip_id              = tip_id,
        pattern_type        = pattern_type,
        timeframe           = timeframe,
        zone_low            = zone_low,
        zone_high           = zone_high,
        regime_at_entry     = regime_at_entry,
        conviction_at_entry = conviction_at_entry,
        initial_status      = 'active',
    )


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


def get_pro_premium_users(db_path: str) -> List[str]:
    """Return list of user_ids whose tier is 'pro' or 'premium'."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_tables(conn)
        rows = conn.execute(
            "SELECT user_id FROM user_preferences WHERE tier IN ('pro', 'premium')"
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


# ── Journal aggregation functions (P4) ────────────────────────────────────────

def get_journal_open(db_path: str, user_id: str) -> List[dict]:
    """
    Return all open positions (status watching/active/partial) for the journal screen.
    Joins KB last_price atom for live P&L computation.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            """SELECT id, ticker, direction, pattern_type, timeframe,
                      entry_price, stop_loss, target_1, target_2, target_3,
                      position_size, tracking_target, status, regime_at_entry,
                      conviction_at_entry, zone_low, zone_high, opened_at,
                      expires_at, user_note, partial_pct, created_at
               FROM tip_followups
               WHERE user_id = ? AND status IN ('watching', 'active', 'partial')
               ORDER BY created_at DESC""",
            (user_id,),
        ).fetchall()
        cols = ['id', 'ticker', 'direction', 'pattern_type', 'timeframe',
                'entry_price', 'stop_loss', 'target_1', 'target_2', 'target_3',
                'position_size', 'tracking_target', 'status', 'regime_at_entry',
                'conviction_at_entry', 'zone_low', 'zone_high', 'opened_at',
                'expires_at', 'user_note', 'partial_pct', 'created_at']
        positions = [dict(zip(cols, r)) for r in rows]

        # Enrich with live KB last_price
        if positions:
            tickers = list({p['ticker'].lower() for p in positions if p['ticker']})
            ph = ','.join('?' * len(tickers))
            price_rows = conn.execute(
                f"SELECT subject, object FROM facts WHERE predicate='last_price' AND subject IN ({ph})",
                tickers,
            ).fetchall()
            prices = {r[0].upper(): float(r[1]) for r in price_rows if r[1]}

            from datetime import timezone as _tz, datetime as _dt
            now = _dt.now(_tz.utc)
            for p in positions:
                ticker = p['ticker'].upper()
                current_price = prices.get(ticker)
                p['current_price'] = current_price
                entry = p['entry_price'] or 0
                stop  = p['stop_loss']  or 0
                if current_price and entry:
                    bullish = p['direction'] != 'bearish'
                    raw = (current_price - entry) / entry * 100
                    p['live_pnl_pct'] = round(raw if bullish else -raw, 2)
                    risk = abs(entry - stop) if stop else entry * 0.02
                    p['r_multiple'] = round((current_price - entry) / risk, 2) if risk else None
                else:
                    p['live_pnl_pct'] = None
                    p['r_multiple'] = None
                # Holding duration
                try:
                    started = _dt.fromisoformat(p['created_at'].replace('Z', '+00:00'))
                    hours = (now - started).total_seconds() / 3600
                    p['holding_hours'] = int(hours)
                except Exception:
                    p['holding_hours'] = None

        return positions
    finally:
        conn.close()


def get_journal_closed(db_path: str, user_id: str, since_days: int = 90) -> List[dict]:
    """
    Return closed/stopped/expired positions with computed P&L, R-multiple, holding time.
    """
    from datetime import timedelta, timezone as _tz, datetime as _dt
    since = (_dt.now(_tz.utc) - timedelta(days=since_days)).isoformat()
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            """SELECT id, ticker, direction, pattern_type, timeframe,
                      entry_price, exit_price, stop_loss, target_1, target_2,
                      position_size, status, regime_at_entry,
                      holding_hours, r_multiple, user_note, partial_pct,
                      opened_at, closed_at, created_at
               FROM tip_followups
               WHERE user_id = ?
                 AND status IN ('closed', 'stopped', 'expired')
                 AND (closed_at >= ? OR created_at >= ?)
               ORDER BY closed_at DESC""",
            (user_id, since, since),
        ).fetchall()
        cols = ['id', 'ticker', 'direction', 'pattern_type', 'timeframe',
                'entry_price', 'exit_price', 'stop_loss', 'target_1', 'target_2',
                'position_size', 'status', 'regime_at_entry',
                'holding_hours', 'r_multiple', 'user_note', 'partial_pct',
                'opened_at', 'closed_at', 'created_at']
        trades = [dict(zip(cols, r)) for r in rows]
        # Compute any missing P&L
        for t in trades:
            if t['exit_price'] and t['entry_price']:
                bullish = t['direction'] != 'bearish'
                raw_pct = (t['exit_price'] - t['entry_price']) / t['entry_price'] * 100
                t['pnl_pct'] = round(raw_pct if bullish else -raw_pct, 2)
                stop  = t['stop_loss'] or 0
                risk  = abs(t['entry_price'] - stop) if stop else t['entry_price'] * 0.02
                if t['r_multiple'] is None and risk:
                    t['r_multiple'] = round((t['exit_price'] - t['entry_price']) / risk, 2)
            else:
                t['pnl_pct'] = None
        return trades
    finally:
        conn.close()


def get_journal_stats(db_path: str, user_id: str) -> dict:
    """
    Aggregate personal trading statistics for the journal stats panel.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            """SELECT status, r_multiple, pattern_type, regime_at_entry
               FROM tip_followups
               WHERE user_id = ? AND status IN ('closed', 'stopped', 'expired')""",
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {'total_trades': 0, 'win_rate': None, 'avg_r': None,
                'best_pattern': None, 'worst_pattern': None,
                'best_regime': None, 'worst_regime': None}

    wins       = sum(1 for r in rows if r[0] == 'closed' and r[1] is not None and r[1] > 0)
    total      = len(rows)
    r_values   = [r[1] for r in rows if r[1] is not None]
    avg_r      = round(sum(r_values) / len(r_values), 2) if r_values else None
    win_rate   = round(wins / total * 100, 1) if total else None

    # Pattern breakdown
    from collections import defaultdict as _dd
    pat_wins  = _dd(int); pat_total = _dd(int)
    reg_wins  = _dd(int); reg_total = _dd(int)
    for status, r_mult, ptype, regime in rows:
        if ptype:
            pat_total[ptype] += 1
            if status == 'closed' and r_mult and r_mult > 0:
                pat_wins[ptype] += 1
        if regime:
            reg_total[regime] += 1
            if status == 'closed' and r_mult and r_mult > 0:
                reg_wins[regime] += 1

    best_pat  = max(((p, pat_wins[p]/pat_total[p]) for p in pat_total if pat_total[p] >= 3),
                    key=lambda x: x[1], default=(None, None))
    worst_pat = min(((p, pat_wins[p]/pat_total[p]) for p in pat_total if pat_total[p] >= 3),
                    key=lambda x: x[1], default=(None, None))
    best_reg  = max(((r, reg_wins[r]/reg_total[r]) for r in reg_total if reg_total[r] >= 3),
                    key=lambda x: x[1], default=(None, None))
    worst_reg = min(((r, reg_wins[r]/reg_total[r]) for r in reg_total if reg_total[r] >= 3),
                    key=lambda x: x[1], default=(None, None))

    return {
        'total_trades':  total,
        'win_rate':      win_rate,
        'avg_r':         avg_r,
        'best_pattern':  best_pat[0],
        'worst_pattern': worst_pat[0],
        'best_regime':   best_reg[0],
        'worst_regime':  worst_reg[0],
    }


def get_pattern_breakdown(db_path: str, user_id: str) -> List[dict]:
    """Per-pattern-type: win_rate, sample_count, avg_R."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            """SELECT pattern_type, status, r_multiple
               FROM tip_followups
               WHERE user_id = ? AND status IN ('closed', 'stopped', 'expired')
                 AND pattern_type IS NOT NULL""",
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    from collections import defaultdict as _dd
    buckets: dict = _dd(lambda: {'wins': 0, 'total': 0, 'r_sum': 0.0, 'r_count': 0})
    for ptype, status, r_mult in rows:
        b = buckets[ptype]
        b['total'] += 1
        if status == 'closed' and r_mult is not None and r_mult > 0:
            b['wins'] += 1
        if r_mult is not None:
            b['r_sum']   += r_mult
            b['r_count'] += 1

    result = []
    for ptype, b in sorted(buckets.items()):
        result.append({
            'pattern_type': ptype,
            'sample_count': b['total'],
            'win_rate':     round(b['wins'] / b['total'] * 100, 1) if b['total'] else None,
            'avg_r':        round(b['r_sum'] / b['r_count'], 2) if b['r_count'] else None,
        })
    return result


def get_regime_breakdown(db_path: str, user_id: str) -> List[dict]:
    """Per-market-regime: win_rate, sample_count."""
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_tip_followups_table(conn)
        rows = conn.execute(
            """SELECT regime_at_entry, status, r_multiple
               FROM tip_followups
               WHERE user_id = ? AND status IN ('closed', 'stopped', 'expired')
                 AND regime_at_entry IS NOT NULL""",
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    from collections import defaultdict as _dd
    buckets: dict = _dd(lambda: {'wins': 0, 'total': 0})
    for regime, status, r_mult in rows:
        b = buckets[regime]
        b['total'] += 1
        if status == 'closed' and r_mult is not None and r_mult > 0:
            b['wins'] += 1

    result = []
    for regime, b in sorted(buckets.items()):
        result.append({
            'regime':       regime,
            'sample_count': b['total'],
            'win_rate':     round(b['wins'] / b['total'] * 100, 1) if b['total'] else None,
        })
    return result

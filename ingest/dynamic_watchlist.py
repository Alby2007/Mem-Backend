"""
ingest/dynamic_watchlist.py — Dynamic Watchlist Manager

Manages the active ingest universe: a merge of the static default tickers
plus any user-promoted tickers from universe_tickers table.

Coverage tiers determine how frequently each ticker is refreshed:

  Tier        coverage_count  yfinance  options  patterns
  ──────────────────────────────────────────────────────
  nascent     1–2             300s      1800s    900s
  emerging    3–9             180s      900s     450s
  established 10–49           60s       300s     120s
  core        50+             30s       120s     60s

Promotion rule (checked on every add_tickers call):
  coverage_count >= 3  AND  avg_daily_volume >= 500_000  AND  price >= 1.0
  → universe_tickers.added_to_ingest = 1
  Below threshold → ticker_staging
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

_log = logging.getLogger(__name__)

# ── Default watchlist ──────────────────────────────────────────────────────────

_DEFAULT_TICKERS: List[str] = [
    # Mega-cap tech
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO',
    # Financials
    'JPM', 'V', 'MA', 'BAC', 'GS', 'MS', 'BRK-B',
    # Healthcare
    'UNH', 'JNJ', 'LLY', 'ABBV', 'PFE',
    # Energy
    'XOM', 'CVX', 'COP',
    # Consumer
    'WMT', 'PG', 'KO', 'MCD', 'COST',
    # Industrials
    'CAT', 'HON', 'RTX',
    # Comms
    'DIS', 'NFLX', 'CMCSA',
    # Broad market ETFs
    'SPY', 'QQQ', 'IWM', 'DIA', 'VTI',
    # Sector ETFs
    'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLC', 'XLY', 'XLP',
    # Macro proxies
    'GLD', 'SLV', 'TLT', 'HYG', 'LQD', 'UUP',
    # Additional high-coverage equities
    'AMD', 'INTC', 'QCOM', 'MU', 'CRM', 'ADBE', 'NOW', 'SNOW',
    'PYPL', 'COIN',
    'AXP', 'BLK', 'SCHW',
    'CVS', 'MRK', 'BMY', 'GILD',
    'NEE', 'DUK', 'SO',
    'AMT', 'PLD', 'EQIX',
]

# ── Promotion criteria ─────────────────────────────────────────────────────────

SHARED_KB_PROMOTION = {
    'min_coverage_count': 3,
    'min_avg_daily_volume': 500_000,
    'min_price': 1.0,
}

# ── Coverage tier refresh intervals (seconds) ──────────────────────────────────

COVERAGE_TIERS = {
    'nascent':     {'min': 1,  'max': 2,  'yfinance': 300,  'options': 1800, 'patterns': 900},
    'emerging':    {'min': 3,  'max': 9,  'yfinance': 180,  'options': 900,  'patterns': 450},
    'established': {'min': 10, 'max': 49, 'yfinance': 60,   'options': 300,  'patterns': 120},
    'core':        {'min': 50, 'max': None, 'yfinance': 30,  'options': 120,  'patterns': 60},
}


def coverage_tier_for(coverage_count: int) -> str:
    """Return the tier name for a given coverage_count."""
    if coverage_count >= 50:
        return 'core'
    if coverage_count >= 10:
        return 'established'
    if coverage_count >= 3:
        return 'emerging'
    return 'nascent'


# ── DynamicWatchlistManager ────────────────────────────────────────────────────

class DynamicWatchlistManager:
    """
    Static-method manager for the active ingest universe.
    All methods accept db_path and open their own short-lived connection.
    """

    @staticmethod
    def get_active_tickers(db_path: str) -> List[str]:
        """
        Return merged list of _DEFAULT_TICKERS + promoted universe tickers.
        Deduplicated by string key — each ticker appears exactly once.
        """
        tickers = list(_DEFAULT_TICKERS)
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            try:
                rows = conn.execute(
                    "SELECT ticker FROM universe_tickers WHERE added_to_ingest = 1"
                ).fetchall()
                for (t,) in rows:
                    if t.upper() not in (x.upper() for x in tickers):
                        tickers.append(t.upper())
            finally:
                conn.close()
        except Exception as exc:
            _log.warning('DynamicWatchlistManager.get_active_tickers: DB error %s', exc)
        # Final dedup preserving order
        seen = set()
        result = []
        for t in tickers:
            key = t.upper()
            if key not in seen:
                seen.add(key)
                result.append(t)
        return result

    @staticmethod
    def get_priority_tickers(db_path: str) -> List[str]:
        """Return tickers with coverage_count >= 3 (eligible for priority ingest)."""
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            try:
                rows = conn.execute(
                    "SELECT ticker FROM universe_tickers WHERE coverage_count >= 3"
                ).fetchall()
                return [r[0] for r in rows]
            finally:
                conn.close()
        except Exception as exc:
            _log.warning('DynamicWatchlistManager.get_priority_tickers: DB error %s', exc)
            return []

    @staticmethod
    def get_user_tickers(user_id: str, db_path: str) -> List[str]:
        """Return tickers a specific user added (from universe_tickers requested_by)."""
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            try:
                rows = conn.execute(
                    "SELECT ticker FROM universe_tickers WHERE requested_by = ?",
                    (user_id,),
                ).fetchall()
                return [r[0] for r in rows]
            finally:
                conn.close()
        except Exception as exc:
            _log.warning('DynamicWatchlistManager.get_user_tickers: DB error %s', exc)
            return []

    @staticmethod
    def add_tickers(
        tickers: List[str],
        user_id: str,
        db_path: str,
        sector_label: Optional[str] = None,
        price_check: Optional[dict] = None,
    ) -> dict:
        """
        Insert tickers into universe_tickers or ticker_staging.

        price_check: optional dict { ticker: {'price': float, 'avg_volume': float} }
                     used to evaluate promotion criteria.

        Returns: { promoted: [str], staged: [str] }
        """
        now = datetime.now(timezone.utc).isoformat()
        promoted = []
        staged = []

        conn = sqlite3.connect(db_path, timeout=10)
        try:
            _ensure_hybrid_tables(conn)

            for raw in tickers:
                ticker = raw.upper().strip()
                if not ticker:
                    continue

                # Check if already in universe_tickers
                existing = conn.execute(
                    "SELECT coverage_count FROM universe_tickers WHERE ticker = ?",
                    (ticker,),
                ).fetchone()

                if existing:
                    new_count = existing[0] + 1
                    conn.execute(
                        "UPDATE universe_tickers SET coverage_count = ? WHERE ticker = ?",
                        (new_count, ticker),
                    )
                    # Re-evaluate promotion
                    if _meets_promotion_criteria(ticker, new_count, price_check):
                        conn.execute(
                            "UPDATE universe_tickers SET added_to_ingest = 1 WHERE ticker = ?",
                            (ticker,),
                        )
                        promoted.append(ticker)
                    conn.commit()
                    continue

                # Determine if meets promotion criteria
                meets = _meets_promotion_criteria(ticker, 1, price_check)
                if meets:
                    conn.execute(
                        """INSERT OR IGNORE INTO universe_tickers
                           (ticker, requested_by, sector_label, coverage_count, added_to_ingest, added_at)
                           VALUES (?,?,?,1,1,?)""",
                        (ticker, user_id, sector_label, now),
                    )
                    promoted.append(ticker)
                else:
                    # Insert into universe_tickers as nascent (not promoted)
                    conn.execute(
                        """INSERT OR IGNORE INTO universe_tickers
                           (ticker, requested_by, sector_label, coverage_count, added_to_ingest, added_at)
                           VALUES (?,?,?,1,0,?)""",
                        (ticker, user_id, sector_label, now),
                    )
                    # Also track in staging
                    conn.execute(
                        """INSERT INTO ticker_staging
                           (ticker, user_id, requested_at, coverage_count, promoted)
                           VALUES (?,?,?,1,0)""",
                        (ticker, user_id, now),
                    )
                    staged.append(ticker)

            conn.commit()
        finally:
            conn.close()

        _log.info(
            'DynamicWatchlistManager.add_tickers: promoted=%s staged=%s',
            promoted, staged,
        )
        return {'promoted': promoted, 'staged': staged}

    @staticmethod
    def remove_ticker(ticker: str, user_id: str, db_path: str) -> bool:
        """Remove a ticker from user's universe (only if user owns it). Returns True if removed."""
        ticker = ticker.upper().strip()
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            row = conn.execute(
                "SELECT requested_by FROM universe_tickers WHERE ticker = ?",
                (ticker,),
            ).fetchone()
            if row is None or row[0] != user_id:
                return False
            conn.execute("DELETE FROM universe_tickers WHERE ticker = ?", (ticker,))
            conn.commit()
            return True
        finally:
            conn.close()

    @staticmethod
    def get_bootstrap_status(user_id: str, db_path: str) -> dict:
        """
        Return per-ticker bootstrap completion status for a user's expanded universe.
        bootstrap_complete = added_to_ingest=1 AND has last_price, conviction_tier, pattern_signals row.
        """
        user_tickers = DynamicWatchlistManager.get_user_tickers(user_id, db_path)
        if not user_tickers:
            return {'tickers': [], 'all_ready': True}

        conn = sqlite3.connect(db_path, timeout=10)
        try:
            result = []
            for ticker in user_tickers:
                row = conn.execute(
                    "SELECT added_to_ingest FROM universe_tickers WHERE ticker = ?",
                    (ticker,),
                ).fetchone()
                added_to_ingest = bool(row and row[0]) if row else False

                has_price = bool(conn.execute(
                    "SELECT 1 FROM facts WHERE subject = ? AND predicate = 'last_price' LIMIT 1",
                    (ticker.lower(),),
                ).fetchone())

                has_signals = bool(conn.execute(
                    "SELECT 1 FROM facts WHERE subject = ? AND predicate = 'conviction_tier' LIMIT 1",
                    (ticker.lower(),),
                ).fetchone())

                has_patterns = bool(conn.execute(
                    "SELECT 1 FROM pattern_signals WHERE ticker = ? LIMIT 1",
                    (ticker,),
                ).fetchone())

                bootstrap_complete = added_to_ingest and has_price and has_signals and has_patterns
                result.append({
                    'ticker':            ticker,
                    'added_to_ingest':   added_to_ingest,
                    'has_price':         has_price,
                    'has_signals':       has_signals,
                    'has_patterns':      has_patterns,
                    'bootstrap_complete': bootstrap_complete,
                })
            all_ready = all(r['bootstrap_complete'] for r in result)
            return {'tickers': result, 'all_ready': all_ready}
        finally:
            conn.close()


# ── Internal helpers ───────────────────────────────────────────────────────────

def _meets_promotion_criteria(
    ticker: str,
    coverage_count: int,
    price_check: Optional[dict],
) -> bool:
    """Return True if ticker passes the shared KB promotion threshold."""
    if coverage_count < SHARED_KB_PROMOTION['min_coverage_count']:
        return False
    if price_check and ticker in price_check:
        info = price_check[ticker]
        if info.get('price', 0) < SHARED_KB_PROMOTION['min_price']:
            return False
        if info.get('avg_volume', 0) < SHARED_KB_PROMOTION['min_avg_daily_volume']:
            return False
    return True


def _ensure_hybrid_tables(conn: sqlite3.Connection) -> None:
    """Idempotent table creation — mirrors users/user_store.ensure_hybrid_tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS universe_tickers (
            ticker          TEXT PRIMARY KEY,
            requested_by    TEXT,
            sector_label    TEXT,
            coverage_count  INTEGER DEFAULT 1,
            added_to_ingest INTEGER DEFAULT 0,
            added_at        TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ticker_staging (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker           TEXT NOT NULL,
            user_id          TEXT NOT NULL,
            requested_at     TEXT NOT NULL,
            coverage_count   INTEGER DEFAULT 1,
            promoted         INTEGER DEFAULT 0,
            promoted_at      TEXT,
            rejection_reason TEXT
        );
    """)
    conn.commit()

"""
ingest/pattern_adapter.py — OHLCV Pattern Detection Adapter

Fetches OHLCV candles via yfinance for each ticker in the watchlist,
runs detect_all_patterns() for each configured timeframe, persists new
patterns to pattern_signals, and updates fill/break status for existing
open patterns on every run.

Cycle: every 15 minutes (configurable via interval_sec).

Fill tracking
=============
On each run, existing open/partially_filled rows are re-evaluated:
  - Re-fetch latest candle to check if zone was touched
  - Call update_pattern_status() if status changed

Dedup guard
===========
A pattern is only inserted if no identical (ticker, pattern_type, direction,
formed_at, timeframe) row already exists — prevents duplicates across runs.

KB context enrichment
=====================
For each ticker, reads kb_conviction / kb_regime / kb_signal_dir atoms from
the facts table (same SQLite DB) to attach to PatternSignal objects before
persisting.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

_logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    yf = None  # type: ignore

from analytics.pattern_detector import OHLCV, PatternSignal, detect_all_patterns
from users.user_store import (
    ensure_user_tables,
    get_open_patterns,
    upsert_pattern_signal,
    update_pattern_status,
)


# ── Timeframe → yfinance interval + period mapping ────────────────────────────

_TF_MAP = {
    '15m': {'interval': '15m', 'period': '5d'},
    '1h':  {'interval': '1h',  'period': '30d'},
    '4h':  {'interval': '1h',  'period': '60d'},   # yf has no 4h; use 1h and resample
    '1d':  {'interval': '1d',  'period': '180d'},
}

# Default timeframes run by the adapter
_DEFAULT_TIMEFRAMES = ['1h', '1d']


# ── KB atom reader ─────────────────────────────────────────────────────────────

def _read_kb_context(conn: sqlite3.Connection, ticker: str) -> Dict[str, str]:
    """
    Read conviction, regime, and signal_direction atoms from the facts table
    for this ticker. Returns a dict with keys: conviction, regime, signal_dir.
    """
    ctx: Dict[str, str] = {'conviction': '', 'regime': '', 'signal_dir': ''}
    try:
        rows = conn.execute(
            """SELECT predicate, object FROM facts
               WHERE LOWER(subject) = LOWER(?)
                 AND predicate IN ('signal_confidence','signal_direction',
                                   'market_regime','kb_conviction')
               ORDER BY created_at DESC""",
            (ticker,),
        ).fetchall()
        for predicate, value in rows:
            if predicate in ('signal_confidence', 'kb_conviction') and not ctx['conviction']:
                ctx['conviction'] = (value or '').lower()
            elif predicate == 'market_regime' and not ctx['regime']:
                ctx['regime'] = (value or '').lower()
            elif predicate == 'signal_direction' and not ctx['signal_dir']:
                ctx['signal_dir'] = (value or '').lower()
    except Exception:
        pass
    return ctx


# ── OHLCV helpers ──────────────────────────────────────────────────────────────

def _fetch_ohlcv(ticker: str, timeframe: str) -> List[OHLCV]:
    """
    Fetch OHLCV candles from yfinance for the given ticker and timeframe.
    Returns an empty list on any error.
    """
    if not HAS_YFINANCE:
        return []
    tf = _TF_MAP.get(timeframe, _TF_MAP['1h'])
    try:
        df = yf.download(
            ticker,
            interval=tf['interval'],
            period=tf['period'],
            auto_adjust=True,
            progress=False,
        )
        if df is None or df.empty:
            return []
        candles: List[OHLCV] = []
        for ts, row in df.iterrows():
            try:
                candles.append(OHLCV(
                    timestamp = ts.isoformat(),
                    open      = float(row['Open']),
                    high      = float(row['High']),
                    low       = float(row['Low']),
                    close     = float(row['Close']),
                    volume    = float(row.get('Volume', 0) or 0),
                ))
            except Exception:
                continue
        return candles
    except Exception as exc:
        _logger.debug('OHLCV fetch failed for %s/%s: %s', ticker, timeframe, exc)
        return []


def _resample_4h(candles_1h: List[OHLCV]) -> List[OHLCV]:
    """Aggregate 1h candles into 4h candles (groups of 4)."""
    result: List[OHLCV] = []
    for i in range(0, len(candles_1h) - 3, 4):
        group = candles_1h[i:i + 4]
        result.append(OHLCV(
            timestamp = group[0].timestamp,
            open      = group[0].open,
            high      = max(c.high for c in group),
            low       = min(c.low for c in group),
            close     = group[-1].close,
            volume    = sum(c.volume for c in group),
        ))
    return result


# ── Dedup check ────────────────────────────────────────────────────────────────

def _pattern_exists(conn: sqlite3.Connection, sig: PatternSignal) -> bool:
    """Return True if an identical pattern row already exists in pattern_signals."""
    row = conn.execute(
        """SELECT 1 FROM pattern_signals
           WHERE ticker = ? AND pattern_type = ? AND direction = ?
             AND formed_at = ? AND timeframe = ?
           LIMIT 1""",
        (sig.ticker, sig.pattern_type, sig.direction, sig.formed_at, sig.timeframe),
    ).fetchone()
    return row is not None


# ── Fill tracker ───────────────────────────────────────────────────────────────

def _update_existing_patterns(db_path: str, db_conn: sqlite3.Connection) -> None:
    """
    Re-evaluate open/partially_filled pattern rows by checking the latest
    candle against their zones. Updates status in DB if changed.
    """
    open_rows = get_open_patterns(db_path)
    if not open_rows:
        return

    # Group by (ticker, timeframe) to batch OHLCV fetches
    groups: Dict[tuple, List[dict]] = {}
    for row in open_rows:
        key = (row['ticker'], row['timeframe'])
        groups.setdefault(key, []).append(row)

    for (ticker, timeframe), rows in groups.items():
        candles = _fetch_ohlcv(ticker, timeframe)
        if not candles:
            continue
        latest = candles[-1]
        now_iso = datetime.now(timezone.utc).isoformat()

        for row in rows:
            old_status = row['status']
            new_status = old_status
            zh, zl = row['zone_high'], row['zone_low']

            if row['direction'] == 'bullish':
                if latest.low <= zl:
                    new_status = 'filled'
                elif latest.low < zh:
                    new_status = 'partially_filled'
            else:  # bearish
                if latest.high >= zh:
                    new_status = 'filled'
                elif latest.high > zl:
                    new_status = 'partially_filled'

            if new_status != old_status:
                filled_at = now_iso if new_status == 'filled' else None
                update_pattern_status(db_path, row['id'], new_status, filled_at)
                _logger.debug('Pattern %d (%s %s %s) → %s',
                              row['id'], ticker, row['pattern_type'],
                              row['direction'], new_status)


# ── Main adapter ───────────────────────────────────────────────────────────────

class PatternAdapter:
    """
    Ingest adapter that detects and persists price-action patterns.

    Parameters
    ----------
    db_path     Path to the SQLite knowledge base file.
    tickers     List of tickers to scan. Defaults to all KB subjects.
    timeframes  Timeframes to scan. Defaults to ['1h', '1d'].
    interval_sec  Run interval in seconds. Default 900 (15 min).
    """

    name         = 'pattern_adapter'
    interval_sec = 900  # 15 minutes

    def __init__(
        self,
        db_path:    str,
        tickers:    Optional[List[str]] = None,
        timeframes: Optional[List[str]] = None,
    ):
        self.db_path    = db_path
        self.tickers    = tickers
        self.timeframes = timeframes or _DEFAULT_TIMEFRAMES

    def _get_tickers(self, conn: sqlite3.Connection) -> List[str]:
        """Return configured tickers or all unique subjects from the facts table."""
        if self.tickers:
            return self.tickers
        try:
            rows = conn.execute(
                "SELECT DISTINCT subject FROM facts ORDER BY subject"
            ).fetchall()
            return [r[0] for r in rows if r[0] and len(r[0]) <= 6]
        except Exception:
            return []

    def run(self) -> int:
        """
        Execute one full detection cycle.

        Returns the number of new pattern_signals rows inserted.
        """
        if not HAS_YFINANCE:
            _logger.warning('PatternAdapter: yfinance not installed — skipping')
            return 0

        conn = sqlite3.connect(self.db_path, timeout=15)
        try:
            ensure_user_tables(conn)
            tickers = self._get_tickers(conn)
            if not tickers:
                _logger.info('PatternAdapter: no tickers found')
                return 0

            # Update fill status for existing open patterns first
            _update_existing_patterns(self.db_path, conn)

            inserted = 0
            for ticker in tickers:
                for timeframe in self.timeframes:
                    try:
                        inserted += self._process_ticker(conn, ticker, timeframe)
                    except Exception as exc:
                        _logger.error('PatternAdapter error %s/%s: %s',
                                      ticker, timeframe, exc)

            _logger.info('PatternAdapter: inserted %d new patterns across %d tickers',
                         inserted, len(tickers))
            return inserted
        finally:
            conn.close()

    def _process_ticker(
        self,
        conn:      sqlite3.Connection,
        ticker:    str,
        timeframe: str,
    ) -> int:
        """Fetch OHLCV, detect patterns, persist new ones. Returns inserted count."""
        if timeframe == '4h':
            raw = _fetch_ohlcv(ticker, '4h')  # fetches 1h and resamples
            candles = _resample_4h(raw) if raw else []
        else:
            candles = _fetch_ohlcv(ticker, timeframe)

        if len(candles) < 3:
            return 0

        kb_ctx = _read_kb_context(conn, ticker)
        signals = detect_all_patterns(
            candles,
            ticker        = ticker,
            timeframe     = timeframe,
            kb_conviction = kb_ctx['conviction'],
            kb_regime     = kb_ctx['regime'],
            kb_signal_dir = kb_ctx['signal_dir'],
        )

        inserted = 0
        for sig in signals:
            if _pattern_exists(conn, sig):
                continue
            try:
                upsert_pattern_signal(self.db_path, {
                    'ticker':        sig.ticker,
                    'pattern_type':  sig.pattern_type,
                    'direction':     sig.direction,
                    'zone_high':     sig.zone_high,
                    'zone_low':      sig.zone_low,
                    'zone_size_pct': sig.zone_size_pct,
                    'timeframe':     sig.timeframe,
                    'formed_at':     sig.formed_at,
                    'status':        sig.status,
                    'quality_score': sig.quality_score,
                    'kb_conviction': sig.kb_conviction,
                    'kb_regime':     sig.kb_regime,
                    'kb_signal_dir': sig.kb_signal_dir,
                })
                inserted += 1
            except Exception as exc:
                _logger.error('PatternAdapter: insert failed for %s %s: %s',
                              ticker, sig.pattern_type, exc)
        return inserted

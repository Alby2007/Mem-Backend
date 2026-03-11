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
from datetime import datetime, timedelta, timezone
from ingest.base import BaseIngestAdapter, RawAtom, db_connect
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
_DEFAULT_TIMEFRAMES = ['15m', '1h', '4h', '1d']

_TTL_BY_TIMEFRAME = {'5m': 2, '15m': 5, '1h': 14, '4h': 21, '1d': 60}  # days


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
                 AND predicate IN ('conviction_tier','signal_confidence','signal_direction',
                                   'market_regime','kb_conviction','price_regime',
                                   'volatility_regime')
               ORDER BY created_at DESC""",
            (ticker,),
        ).fetchall()
        for predicate, value in rows:
            if predicate in ('conviction_tier', 'signal_confidence', 'kb_conviction') and not ctx['conviction']:
                ctx['conviction'] = (value or '').lower()
            elif predicate in ('market_regime', 'price_regime', 'volatility_regime') and not ctx['regime']:
                ctx['regime'] = (value or '').lower()
            elif predicate == 'signal_direction' and not ctx['signal_dir']:
                ctx['signal_dir'] = (value or '').lower()
    except Exception:
        pass
    return ctx


# ── OHLCV helpers ──────────────────────────────────────────────────────────────

def _read_ohlcv_cache(db_path: str, ticker: str) -> List[OHLCV]:
    """
    Read daily candles from the ohlcv_cache table (populated by YFinanceAdapter).
    Returns candles sorted oldest-first.
    """
    try:
        conn = db_connect(db_path)
        rows = conn.execute(
            """SELECT ts, open, high, low, close, volume
               FROM ohlcv_cache
               WHERE ticker=? AND interval='1d'
               ORDER BY ts ASC""",
            (ticker,),
        ).fetchall()
        conn.close()
        candles: List[OHLCV] = []
        for ts, o, h, l, c, v in rows:
            try:
                candles.append(OHLCV(
                    timestamp=ts,
                    open=float(o or 0), high=float(h or 0),
                    low=float(l or 0), close=float(c or 0),
                    volume=float(v or 0),
                ))
            except Exception:
                continue
        return candles
    except Exception:
        return []


def _fetch_ohlcv(ticker: str, timeframe: str, db_path: Optional[str] = None) -> List[OHLCV]:
    """
    Fetch OHLCV candles. Reads from ohlcv_cache first (populated by YFinanceAdapter
    via Ticker.history() which works on OCI rate-limited IPs). Falls back to live
    yfinance only when the cache is empty.
    """
    # Try cache first — avoids hitting yfinance bulk download endpoint
    if db_path:
        cached = _read_ohlcv_cache(db_path, ticker)
        if len(cached) >= 3:
            return cached

    if not HAS_YFINANCE:
        return []
    tf = _TF_MAP.get(timeframe, _TF_MAP['1h'])
    try:
        import yfinance as _yf
        t = _yf.Ticker(ticker)
        df = t.history(period=tf['period'], interval=tf['interval'], auto_adjust=True)
        if df is None or df.empty:
            return []
        def _scalar(v) -> float:
            try:
                return float(v.iloc[0]) if hasattr(v, 'iloc') else float(v)
            except Exception:
                return 0.0

        candles: List[OHLCV] = []
        for ts, row in df.iterrows():
            try:
                candles.append(OHLCV(
                    timestamp = ts.isoformat(),
                    open      = _scalar(row['Open']),
                    high      = _scalar(row['High']),
                    low       = _scalar(row['Low']),
                    close     = _scalar(row['Close']),
                    volume    = _scalar(row.get('Volume', 0) or 0),
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
    """Return True if an identical pattern row already exists in pattern_signals.

    Intentionally omits timeframe from the key — the same zone detected on
    both 1h and 4h is a duplicate and the first/highest-quality row wins.
    Uses zone_high/zone_low (rounded to 2dp) as the identity key.
    """
    zh = round(sig.zone_high, 2)
    zl = round(sig.zone_low, 2)
    row = conn.execute(
        """SELECT 1 FROM pattern_signals
           WHERE ticker = ? AND pattern_type = ? AND direction = ?
             AND ROUND(zone_high, 2) = ? AND ROUND(zone_low, 2) = ?
             AND status NOT IN ('filled', 'broken')
           LIMIT 1""",
        (sig.ticker, sig.pattern_type, sig.direction, zh, zl),
    ).fetchone()
    return row is not None


def _dedup_existing_patterns(conn: sqlite3.Connection) -> int:
    """One-time cleanup: for each duplicate group (same ticker/type/direction/zone)
    keep the row with the highest quality_score (oldest detected_at if tied),
    expire the rest by setting status='broken'.
    Returns number of rows expired.
    """
    rows = conn.execute(
        """SELECT ticker, pattern_type, direction,
                  ROUND(zone_high,2) as zh, ROUND(zone_low,2) as zl,
                  COUNT(*) as cnt
           FROM pattern_signals
           WHERE status NOT IN ('filled','broken')
           GROUP BY ticker, pattern_type, direction, zh, zl
           HAVING cnt > 1"""
    ).fetchall()
    expired = 0
    for (ticker, ptype, direction, zh, zl, _cnt) in rows:
        candidates = conn.execute(
            """SELECT id, quality_score, detected_at FROM pattern_signals
               WHERE ticker=? AND pattern_type=? AND direction=?
                 AND ROUND(zone_high,2)=? AND ROUND(zone_low,2)=?
                 AND status NOT IN ('filled','broken')
               ORDER BY quality_score DESC, detected_at ASC""",
            (ticker, ptype, direction, zh, zl),
        ).fetchall()
        # Keep the first row (highest quality / oldest); expire the rest
        for row_id, _qs, _dt in candidates[1:]:
            conn.execute(
                "UPDATE pattern_signals SET status='broken' WHERE id=?", (row_id,)
            )
            expired += 1
    if expired:
        conn.commit()
        _logger.info('PatternAdapter dedup: expired %d duplicate pattern rows', expired)
    return expired


# ── Fill tracker ───────────────────────────────────────────────────────────────

def _kb_last_price(db_conn: sqlite3.Connection, ticker: str) -> Optional[float]:
    """Read last_price atom from the KB facts table for a ticker."""
    try:
        row = db_conn.execute(
            "SELECT object FROM facts WHERE LOWER(subject)=LOWER(?) AND predicate='last_price' LIMIT 1",
            (ticker,),
        ).fetchone()
        if row:
            return float(row[0])
    except Exception:
        pass
    return None


def _update_existing_patterns(db_path: str, db_conn: sqlite3.Connection) -> None:
    """
    Re-evaluate open/partially_filled pattern rows by checking the latest
    candle against their zones. Updates status in DB if changed.
    Falls back to KB last_price when yfinance is unavailable.
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
        candles = _fetch_ohlcv(ticker, timeframe, db_path=db_path)

        # Fallback: use KB last_price when yfinance is blocked
        if not candles:
            kb_price = _kb_last_price(db_conn, ticker)
            if kb_price is None:
                continue
            latest = OHLCV(
                timestamp=datetime.now(timezone.utc).isoformat(),
                open=kb_price, high=kb_price, low=kb_price, close=kb_price,
            )
        else:
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
        """Return configured tickers or subjects that have a last_price atom in the KB.
        Using last_price as the filter ensures only yfinance-resolvable tickers are scanned
        and skips macro/country/concept subjects that pollute the facts table."""
        if self.tickers:
            return self.tickers
        try:
            rows = conn.execute(
                "SELECT DISTINCT subject FROM facts WHERE predicate='last_price' ORDER BY subject"
            ).fetchall()
            return [r[0] for r in rows if r[0] and len(r[0]) <= 10]
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

        conn = db_connect(self.db_path)
        try:
            ensure_user_tables(conn)
            tickers = self._get_tickers(conn)
            if not tickers:
                _logger.info('PatternAdapter: no tickers found')
                return 0

            # One-time cleanup of pre-existing cross-timeframe duplicates
            _dedup_existing_patterns(conn)

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
            raw = _fetch_ohlcv(ticker, '4h', db_path=self.db_path)
            candles = _resample_4h(raw) if raw else []
        else:
            candles = _fetch_ohlcv(ticker, timeframe, db_path=self.db_path)

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
                ttl_days = _TTL_BY_TIMEFRAME.get(sig.timeframe, 30)
                expires_at = (
                    datetime.utcnow() + timedelta(days=ttl_days)
                ).isoformat()
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
                    'expires_at':    expires_at,
                })
                inserted += 1
            except Exception as exc:
                _logger.error('PatternAdapter: insert failed for %s %s: %s',
                              ticker, sig.pattern_type, exc)
        return inserted

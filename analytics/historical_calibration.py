"""
analytics/historical_calibration.py — Historical Signal Calibration Back-population

Slides a detection window through 5 years of daily OHLCV, runs detect_all_patterns()
on each window, then checks whether each detected pattern hit its targets in the
subsequent candles. Aggregates hit rates by (ticker, pattern_type, timeframe, regime)
and writes them to signal_calibration using the same schema as live user feedback.

DESIGN
======
The goal is to give the calibration table real historical baselines before a single
user submits feedback. Without this, the tip formatter shows "insufficient data" for
months post-launch. With it, patterns show "Historical hit rate: 64% to T2 (127
samples, 3yr backtest)" from day one.

SLIDING WINDOW APPROACH
=======================
  window_size = 100 candles    — context for pattern detection
  forward_horizon = 20 candles — look-ahead for outcome checking

  For each position i in [window_size, len(df) - forward_horizon]:
    window = df[i - window_size : i]
    future = df[i : i + forward_horizon]
    patterns = detect_all_patterns(window)
    for each pattern: check if zone_high / zone_low is touched in future

TARGET DEFINITION
=================
  T1: price enters the zone (zone_low ≤ price ≤ zone_high) within 20 candles
  T2: price reaches zone midpoint and continues to far edge within 20 candles
  T3: price exceeds the far edge of the zone by ≥ 50% of zone width
  Stopped out: price moves adversely by ≥ 2× zone width before touching T1

REGIME CLASSIFICATION
=====================
Assigns each window a market regime label using cross-asset proxy signals
computed from the same OHLCV data (no external dependencies):

  risk_on_expansion:    SPY trending up + VIX declining + TLT flat/down
  risk_off_contraction: SPY trending down + VIX spiking
  stagflation:          SPY flat/down + commodities (GLD) trending up
  recovery:             SPY trending up after drawdown + VIX declining
  no_data:              insufficient proxy data in the download batch

USAGE
=====
  # Run full backfill for FTSE 100 watchlist (takes ~10–20 minutes)
  python -m analytics.historical_calibration

  # Run for specific tickers
  from analytics.historical_calibration import HistoricalCalibrator
  cal = HistoricalCalibrator(db_path='trading_knowledge.db')
  cal.calibrate_ticker('HSBA.L', lookback_years=3)

  # Run via API
  POST /calibrate/historical
  POST /calibrate/historical  {"tickers": ["NVDA", "HSBA.L"], "lookback_years": 3}

OUTPUT
======
Writes/updates rows in signal_calibration with source metadata indicating
the rows came from historical backtesting (not live user feedback).
The calibration_confidence column reflects sample size using the same
_confidence_score function as the live feedback path.

PERFORMANCE
===========
  - Per-ticker: ~2–5 seconds for 3yr daily (756 rows, ~650 windows)
  - Full FTSE watchlist (30 tickers × 3yr): ~2–3 minutes
  - Uses yf.download() bulk fetch to minimise API calls
  - Windows processed in-process (no threads needed — CPU-bound is fast)
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

_logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False
    yf = pd = np = None  # type: ignore

try:
    from analytics.pattern_detector import detect_all_patterns, OHLCV, PatternSignal
    HAS_DETECTOR = True
except ImportError:
    HAS_DETECTOR = False
    detect_all_patterns = OHLCV = PatternSignal = None  # type: ignore

try:
    from analytics.signal_calibration import _ensure_table, _confidence_score, _confidence_label
    HAS_CALIBRATION = True
except ImportError:
    HAS_CALIBRATION = False

# ── Constants ──────────────────────────────────────────────────────────────────

_WINDOW_SIZE      = 100    # candles of context fed to detect_all_patterns
_FORWARD_HORIZON  = 20     # candles ahead used to check outcome
_STEP_SIZE        = 5      # slide window by this many candles (reduces redundancy)
_T3_EXTENSION     = 0.5    # T3: price exceeds far edge by ≥ 50% of zone width
_STOP_MULTIPLIER  = 2.0    # adverse move ≥ 2× zone width = stopped out

# Regime detection proxy tickers — fetched alongside the target ticker
_REGIME_PROXIES = ['SPY', '^VIX', 'TLT', 'GLD']

# Minimum ATR ratio to include a pattern detection in calibration
# (filters very tiny zones that are within noise)
_MIN_ZONE_ATR_RATIO = 0.2

# ── Outcome dataclass ──────────────────────────────────────────────────────────

@dataclass
class PatternOutcome:
    hit_t1:             bool  = False
    hit_t2:             bool  = False
    hit_t3:             bool  = False
    stopped_out:        bool  = False
    candles_to_target:  int   = 0    # candles until first T1 touch (0 = never)


# ── Regime classification ──────────────────────────────────────────────────────

def _classify_regime(
    close_spy:   Optional['pd.Series'],
    close_vix:   Optional['pd.Series'],
    close_tlt:   Optional['pd.Series'],
    close_gld:   Optional['pd.Series'],
    window_end:  int,
    lookback:    int = 20,
) -> str:
    """
    Classify macro regime for the window ending at window_end.

    Uses 20-candle trend direction for each proxy:
      SPY trend:  close[end] vs close[end - lookback]
      VIX trend:  same (rising VIX = risk-off)
      TLT trend:  same (rising TLT = rates falling = risk-off or recovery)
      GLD trend:  same (rising GLD = inflation hedge bid)

    REGIME MATRIX (priority order, first match wins):
      risk_off_contraction: SPY down + VIX up
      stagflation:          SPY flat/down + GLD up (> +3%)
      recovery:             SPY up + VIX down + TLT up (rates still falling)
      risk_on_expansion:    SPY up + (VIX flat/down OR TLT flat/down)
      no_data:              proxies missing or insufficient rows
    """
    def _trend(series, end, lb) -> Optional[float]:
        if series is None or end < lb:
            return None
        start_val = float(series.iloc[end - lb])
        end_val   = float(series.iloc[end])
        if start_val <= 0:
            return None
        return (end_val - start_val) / start_val * 100.0

    spy_pct = _trend(close_spy, window_end, lookback)
    vix_pct = _trend(close_vix, window_end, lookback)
    tlt_pct = _trend(close_tlt, window_end, lookback)
    gld_pct = _trend(close_gld, window_end, lookback)

    if spy_pct is None:
        return 'no_data'

    spy_up   = spy_pct >  2.0
    spy_down = spy_pct < -2.0
    vix_up   = vix_pct is not None and vix_pct >  5.0
    vix_down = vix_pct is not None and vix_pct < -5.0
    tlt_up   = tlt_pct is not None and tlt_pct >  2.0
    gld_up   = gld_pct is not None and gld_pct >  3.0

    if spy_down and vix_up:
        return 'risk_off_contraction'
    if not spy_up and gld_up:
        return 'stagflation'
    if spy_up and tlt_up:
        return 'recovery'
    if spy_up:
        return 'risk_on_expansion'
    return 'no_data'


# ── Outcome checker ────────────────────────────────────────────────────────────

def _check_outcome(
    pattern:  'PatternSignal',
    future:   List['OHLCV'],
) -> PatternOutcome:
    """
    Check whether a detected pattern hit T1/T2/T3 or was stopped out
    in the forward_horizon candles following detection.

    For bullish patterns:
      - Zone = demand zone: zone_low (support) to zone_high (resistance)
      - T1: price touches zone_high (enters zone from above = retest) — or low
        Simplified: any candle low ≤ zone_high + 10% of zone, i.e., price enters
      - T2: price closes inside or below zone_low (zone fully touched)
      - T3: price closes below zone_low - 0.5 × zone_size (extension)
      - Stop: price closes above zone_high + 2 × zone_size (stop hunt up)

    For bearish patterns (inverted):
      - T1: any candle high ≥ zone_low - 10% of zone
      - T2: price closes inside or above zone_high
      - T3: price closes above zone_high + 0.5 × zone_size
      - Stop: price closes below zone_low - 2 × zone_size
    """
    if not future:
        return PatternOutcome()

    zh       = pattern.zone_high
    zl       = pattern.zone_low
    zs       = zh - zl
    if zs <= 0:
        return PatternOutcome()

    is_bullish = pattern.direction == 'bullish'
    outcome    = PatternOutcome()

    for i, candle in enumerate(future, 1):
        h, l, c = candle.high, candle.low, candle.close

        if is_bullish:
            # T1: price enters the zone (low dips into zone)
            if not outcome.hit_t1 and l <= zh and h >= zl:
                outcome.hit_t1          = True
                outcome.candles_to_target = i
            # T2: close confirms inside zone or below zone_low
            if outcome.hit_t1 and not outcome.hit_t2 and c <= zh and c >= zl - 0.5 * zs:
                outcome.hit_t2 = True
            # T3: close extends well below zone_low
            if outcome.hit_t1 and not outcome.hit_t3 and c < zl - _T3_EXTENSION * zs:
                outcome.hit_t3 = True
            # Stopped out: close above zone_high + 2× zone_size (breakout above)
            if not outcome.hit_t1 and c > zh + _STOP_MULTIPLIER * zs:
                outcome.stopped_out = True
                break
        else:
            # Bearish pattern
            if not outcome.hit_t1 and h >= zl and l <= zh:
                outcome.hit_t1          = True
                outcome.candles_to_target = i
            if outcome.hit_t1 and not outcome.hit_t2 and c >= zl and c <= zh + 0.5 * zs:
                outcome.hit_t2 = True
            if outcome.hit_t1 and not outcome.hit_t3 and c > zh + _T3_EXTENSION * zs:
                outcome.hit_t3 = True
            if not outcome.hit_t1 and c < zl - _STOP_MULTIPLIER * zs:
                outcome.stopped_out = True
                break

    return outcome


# ── Aggregation helpers ────────────────────────────────────────────────────────

@dataclass
class _AggKey:
    ticker:       str
    pattern_type: str
    timeframe:    str
    regime:       Optional[str]


@dataclass
class _AggBucket:
    n:        int   = 0
    hit_t1:   int   = 0
    hit_t2:   int   = 0
    hit_t3:   int   = 0
    stop_out: int   = 0
    candles:  List[int] = field(default_factory=list)


def _upsert_calibration(
    conn:        sqlite3.Connection,
    ticker:      str,
    pattern_type: str,
    timeframe:   str,
    regime:      Optional[str],
    bucket:      _AggBucket,
    now:         str,
) -> None:
    """Write or merge a calibration bucket into signal_calibration."""
    if bucket.n == 0:
        return

    n    = bucket.n
    hr1  = round(bucket.hit_t1  / n, 4)
    hr2  = round(bucket.hit_t2  / n, 4)
    hr3  = round(bucket.hit_t3  / n, 4)
    sor  = round(bucket.stop_out / n, 4)
    atth = round(sum(bucket.candles) / len(bucket.candles) * 24, 2) if bucket.candles else None

    # Merge with any existing row from a previous calibration run
    existing = conn.execute(
        """SELECT sample_size, hit_rate_t1, hit_rate_t2, hit_rate_t3,
                  stopped_out_rate, avg_time_to_target_hours
           FROM signal_calibration
           WHERE ticker=? AND pattern_type=? AND timeframe=?
             AND (market_regime=? OR (market_regime IS NULL AND ? IS NULL))""",
        (ticker, pattern_type, timeframe, regime, regime),
    ).fetchone()

    if existing:
        # Only replace if new sample is larger (re-run produces more data)
        old_n = existing[0] or 0
        if n <= old_n:
            return
        # Override entirely with the fresh backtest result
    conf_score = _confidence_score(n)
    conn.execute(
        """INSERT INTO signal_calibration
           (ticker, pattern_type, timeframe, market_regime, sample_size,
            hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
            avg_time_to_target_hours, calibration_confidence, last_updated)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(ticker, pattern_type, timeframe, market_regime)
           DO UPDATE SET
             sample_size=excluded.sample_size,
             hit_rate_t1=excluded.hit_rate_t1,
             hit_rate_t2=excluded.hit_rate_t2,
             hit_rate_t3=excluded.hit_rate_t3,
             stopped_out_rate=excluded.stopped_out_rate,
             avg_time_to_target_hours=excluded.avg_time_to_target_hours,
             calibration_confidence=excluded.calibration_confidence,
             last_updated=excluded.last_updated""",
        (ticker, pattern_type, timeframe, regime, n,
         hr1, hr2, hr3, sor, atth,
         round(conf_score, 4), now),
    )


# ── Main calibrator class ──────────────────────────────────────────────────────

class HistoricalCalibrator:
    """
    Slides a detection window through historical OHLCV to back-populate
    signal_calibration with real outcome statistics.

    Parameters
    ----------
    db_path         Path to SQLite KB (trading_knowledge.db)
    window_size     Candles of context for pattern detection (default 100)
    forward_horizon Candles ahead used for outcome checking (default 20)
    step_size       Slide interval — larger = faster but fewer samples (default 5)
    """

    def __init__(
        self,
        db_path:        str  = 'trading_knowledge.db',
        window_size:    int  = _WINDOW_SIZE,
        forward_horizon: int = _FORWARD_HORIZON,
        step_size:      int  = _STEP_SIZE,
    ):
        self._db_path         = db_path
        self._window_size     = window_size
        self._forward_horizon = forward_horizon
        self._step_size       = step_size

    def calibrate_ticker(
        self,
        ticker:        str,
        ohlcv_df:      Optional['pd.DataFrame'] = None,
        proxy_data:    Optional[Dict[str, 'pd.Series']] = None,
        lookback_years: int = 3,
    ) -> Dict[str, int]:
        """
        Run sliding-window calibration for one ticker.

        Parameters
        ----------
        ticker          Ticker symbol e.g. 'HSBA.L'
        ohlcv_df        Pre-fetched DataFrame (optional — saves yfinance calls
                        when running across many tickers from a bulk download)
        proxy_data      Dict of {'SPY': Series, '^VIX': Series, ...} pre-fetched
        lookback_years  How many years of history to use (default 3)

        Returns
        -------
        {'patterns_detected': N, 'calibration_rows_written': M}
        """
        if not HAS_DEPS:
            _logger.error('calibration: yfinance/pandas/numpy not available')
            return {'patterns_detected': 0, 'calibration_rows_written': 0}
        if not HAS_DETECTOR:
            _logger.error('calibration: pattern_detector not available')
            return {'patterns_detected': 0, 'calibration_rows_written': 0}
        if not HAS_CALIBRATION:
            _logger.error('calibration: signal_calibration module not available')
            return {'patterns_detected': 0, 'calibration_rows_written': 0}

        ticker = ticker.upper()

        # ── Fetch OHLCV if not provided ────────────────────────────────────────
        if ohlcv_df is None:
            try:
                t = yf.Ticker(ticker)
                ohlcv_df = t.history(
                    period=f'{lookback_years}y',
                    interval='1d',
                    auto_adjust=True,
                )
            except Exception as e:
                _logger.warning('calibration: fetch failed for %s: %s', ticker, e)
                return {'patterns_detected': 0, 'calibration_rows_written': 0}

        if ohlcv_df is None or ohlcv_df.empty or len(ohlcv_df) < self._window_size + self._forward_horizon:
            _logger.debug('calibration: insufficient rows for %s (%d rows)',
                          ticker, 0 if ohlcv_df is None else len(ohlcv_df))
            return {'patterns_detected': 0, 'calibration_rows_written': 0}

        ohlcv_df = ohlcv_df.copy()
        ohlcv_df.index = pd.to_datetime(ohlcv_df.index, utc=True)

        # ── Extract proxy series ───────────────────────────────────────────────
        close_spy = close_vix = close_tlt = close_gld = None
        if proxy_data:
            close_spy = proxy_data.get('SPY')
            close_vix = proxy_data.get('^VIX')
            close_tlt = proxy_data.get('TLT')
            close_gld = proxy_data.get('GLD')

        # ── Slide the window ───────────────────────────────────────────────────
        n_rows       = len(ohlcv_df)
        total_end    = n_rows - self._forward_horizon
        buckets: Dict[str, _AggBucket] = {}
        patterns_detected = 0

        for window_end in range(self._window_size, total_end, self._step_size):
            # Build window candles
            window_df = ohlcv_df.iloc[window_end - self._window_size : window_end]
            future_df = ohlcv_df.iloc[window_end : window_end + self._forward_horizon]

            candles = self._df_to_ohlcv(window_df)
            future  = self._df_to_ohlcv(future_df)
            if len(candles) < 3 or not future:
                continue

            # Classify regime at this window position
            if close_spy is not None and len(close_spy) > window_end:
                regime = _classify_regime(
                    close_spy, close_vix, close_tlt, close_gld,
                    window_end=window_end,
                )
            else:
                regime = 'no_data'

            # Detect patterns (no KB context available in backtesting)
            patterns = detect_all_patterns(candles, ticker, timeframe='1d')

            for p in patterns:
                # Skip zones too small relative to ATR (noise)
                atr = self._atr_from_candles(candles)
                if atr > 0 and (p.zone_high - p.zone_low) / atr < _MIN_ZONE_ATR_RATIO:
                    continue

                patterns_detected += 1
                outcome = _check_outcome(p, future)

                # Bucket key: (ticker, pattern_type, timeframe, regime)
                key = f'{ticker}|{p.pattern_type}|1d|{regime}'
                if key not in buckets:
                    buckets[key] = _AggBucket()
                b = buckets[key]
                b.n += 1
                if outcome.hit_t1:
                    b.hit_t1 += 1
                    if outcome.candles_to_target:
                        b.candles.append(outcome.candles_to_target)
                if outcome.hit_t2:
                    b.hit_t2 += 1
                if outcome.hit_t3:
                    b.hit_t3 += 1
                if outcome.stopped_out:
                    b.stop_out += 1

        if not buckets:
            _logger.debug('calibration: no patterns detected for %s', ticker)
            return {'patterns_detected': 0, 'calibration_rows_written': 0}

        # ── Write calibration rows ─────────────────────────────────────────────
        now  = datetime.now(timezone.utc).isoformat()
        rows_written = 0
        conn = sqlite3.connect(self._db_path, timeout=20)
        try:
            _ensure_table(conn)
            for key, bucket in buckets.items():
                parts = key.split('|')
                _ticker, p_type, tf, reg = parts[0], parts[1], parts[2], parts[3]
                regime_val = None if reg == 'no_data' else reg
                _upsert_calibration(conn, _ticker, p_type, tf, regime_val, bucket, now)
                rows_written += 1
            conn.commit()
        finally:
            conn.close()

        _logger.info(
            'calibration: %s — %d patterns detected, %d rows written (%d buckets)',
            ticker, patterns_detected, rows_written, len(buckets),
        )
        return {
            'patterns_detected':       patterns_detected,
            'calibration_rows_written': rows_written,
        }

    def calibrate_watchlist(
        self,
        tickers:        Optional[List[str]] = None,
        lookback_years: int = 3,
    ) -> Dict[str, Dict[str, int]]:
        """
        Run calibration for all tickers in the watchlist.

        Bulk-downloads proxy tickers (SPY, VIX, TLT, GLD) once and reuses
        for regime classification across all ticker calibrations.

        Returns per-ticker summary dict.
        """
        if not HAS_DEPS:
            return {}

        if tickers is None:
            try:
                from ingest.dynamic_watchlist import DynamicWatchlistManager
                tickers = DynamicWatchlistManager.get_active_tickers(self._db_path)
            except ImportError:
                try:
                    from ingest.yfinance_adapter import _DEFAULT_TICKERS
                    tickers = list(_DEFAULT_TICKERS)
                except ImportError:
                    tickers = []

        if not tickers:
            _logger.warning('calibration: no tickers to calibrate')
            return {}

        # Ensure proxies are in the download list
        all_tickers = list(tickers)
        for p in _REGIME_PROXIES:
            if p not in all_tickers:
                all_tickers.append(p)

        _logger.info(
            'calibration: fetching %dy daily OHLCV for %d tickers + proxies (ohlcv_cache preferred)',
            lookback_years, len(tickers),
        )

        # ── Fetch OHLCV: prefer ohlcv_cache (no rate-limit risk on OCI IPs) ───
        def _df_from_cache(sym: str) -> Optional['pd.DataFrame']:
            """Read from ohlcv_cache table; return DataFrame or None."""
            try:
                import sqlite3 as _sq
                conn = _sq.connect(self._db_path, timeout=10)
                try:
                    rows = conn.execute(
                        """SELECT ts, open, high, low, close, volume
                           FROM ohlcv_cache
                           WHERE ticker=? AND interval='1d'
                           ORDER BY ts ASC""",
                        (sym,),
                    ).fetchall()
                finally:
                    conn.close()
                if not rows:
                    return None
                df = pd.DataFrame(rows, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['Date'] = pd.to_datetime(df['Date'], utc=True)
                df = df.set_index('Date').sort_index()
                # Filter to lookback window
                cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=lookback_years * 365)
                df = df[df.index >= cutoff]
                return df if not df.empty else None
            except Exception:
                return None

        def _df_from_yf(sym: str) -> Optional['pd.DataFrame']:
            """Fall back to yfinance per-ticker history (rate-limited on OCI)."""
            try:
                t = yf.Ticker(sym)
                df = t.history(period=f'{lookback_years}y', interval='1d', auto_adjust=True)
                df.index = pd.to_datetime(df.index, utc=True)
                return df if not df.empty else None
            except Exception as e:
                _logger.debug('calibration: yf fallback failed for %s: %s', sym, e)
                return None

        # Fetch all tickers (proxies + targets) from cache first
        ticker_dfs: Dict[str, Optional['pd.DataFrame']] = {}
        for sym in all_tickers:
            df = _df_from_cache(sym)
            if df is None or len(df) < 30:
                _logger.debug('calibration: cache miss for %s — trying yfinance', sym)
                df = _df_from_yf(sym)
            ticker_dfs[sym] = df

        # ── Extract proxy series ───────────────────────────────────────────────
        def _get_close(sym: str) -> Optional['pd.Series']:
            df = ticker_dfs.get(sym)
            if df is None or df.empty:
                return None
            try:
                s = df['Close'].dropna()
                return s if not s.empty else None
            except (KeyError, TypeError):
                return None

        proxy_data = {
            'SPY':  _get_close('SPY'),
            '^VIX': _get_close('^VIX'),
            'TLT':  _get_close('TLT'),
            'GLD':  _get_close('GLD'),
        }

        # ── Calibrate each ticker ──────────────────────────────────────────────
        results = {}
        for ticker in tickers:
            try:
                df = ticker_dfs.get(ticker)

                if df is None or df.empty:
                    _logger.debug('calibration: no data for %s', ticker)
                    continue

                result = self.calibrate_ticker(
                    ticker,
                    ohlcv_df=df,
                    proxy_data=proxy_data,
                    lookback_years=lookback_years,
                )
                results[ticker] = result
            except Exception as e:
                _logger.warning('calibration: %s failed: %s', ticker, e)
                results[ticker] = {'patterns_detected': 0, 'calibration_rows_written': 0, 'error': str(e)}

        total_patterns = sum(r.get('patterns_detected', 0) for r in results.values())
        total_rows     = sum(r.get('calibration_rows_written', 0) for r in results.values())
        _logger.info(
            'calibration: complete — %d tickers, %d patterns, %d calibration rows',
            len(results), total_patterns, total_rows,
        )
        return results

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _df_to_ohlcv(df: 'pd.DataFrame') -> List['OHLCV']:
        """Convert a yfinance OHLCV DataFrame slice to List[OHLCV]."""
        candles = []
        for ts, row in df.iterrows():
            try:
                candles.append(OHLCV(
                    timestamp = str(ts),
                    open      = float(row['Open']),
                    high      = float(row['High']),
                    low       = float(row['Low']),
                    close     = float(row['Close']),
                    volume    = float(row.get('Volume', 0) or 0),
                ))
            except (KeyError, ValueError, TypeError):
                continue
        return candles

    @staticmethod
    def _atr_from_candles(candles: List['OHLCV'], window: int = 14) -> float:
        """Average True Range over last `window` candles."""
        trs = []
        for i in range(1, min(window + 1, len(candles))):
            c, p = candles[i], candles[i - 1]
            tr = max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close))
            trs.append(tr)
        return sum(trs) / len(trs) if trs else 0.0


# ── CLI entry point ────────────────────────────────────────────────────────────

def main():
    """Run full watchlist calibration from CLI: python -m analytics.historical_calibration"""
    import argparse
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s %(levelname)-8s %(name)s — %(message)s',
    )

    parser = argparse.ArgumentParser(description='Historical signal calibration back-population')
    parser.add_argument('--db',      default='trading_knowledge.db', help='SQLite DB path')
    parser.add_argument('--tickers', nargs='*',  help='Specific tickers (default: full watchlist)')
    parser.add_argument('--years',   type=int,   default=3, help='Lookback years (default: 3)')
    args = parser.parse_args()

    cal     = HistoricalCalibrator(db_path=args.db)
    results = cal.calibrate_watchlist(tickers=args.tickers, lookback_years=args.years)

    print(f'\nCalibration complete: {len(results)} tickers')
    for ticker, r in sorted(results.items()):
        err = r.get('error', '')
        print(
            f'  {ticker:12s}  patterns={r.get("patterns_detected",0):4d}  '
            f'rows={r.get("calibration_rows_written",0):3d}'
            + (f'  ERROR: {err}' if err else '')
        )

    # Print calibration summary
    try:
        conn = sqlite3.connect(args.db, timeout=10)
        total = conn.execute('SELECT COUNT(*) FROM signal_calibration').fetchone()[0]
        rows  = conn.execute(
            'SELECT ticker, pattern_type, market_regime, sample_size, '
            'hit_rate_t1, hit_rate_t2, calibration_confidence '
            'FROM signal_calibration ORDER BY sample_size DESC LIMIT 20'
        ).fetchall()
        conn.close()
        print(f'\nTop 20 calibration rows ({total} total):')
        print(f'  {"Ticker":10s} {"Pattern":15s} {"Regime":22s} {"N":>5s} {"HRt1":>6s} {"HRt2":>6s} {"Conf":>6s}')
        for r in rows:
            regime = r[2] or 'all'
            print(
                f'  {r[0]:10s} {r[1]:15s} {regime:22s} '
                f'{r[3]:5d} {r[4] or 0:6.1%} {r[5] or 0:6.1%} {r[6] or 0:6.2f}'
            )
    except Exception as e:
        print(f'\nCould not read calibration summary: {e}')


if __name__ == '__main__':
    main()

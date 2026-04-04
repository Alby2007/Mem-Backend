"""
ingest/historical_adapter.py — Historical Summary Backfill Adapter

Fetches five years of daily OHLCV for the watchlist via a single yf.download()
call and computes interpretable summary atoms. Raw OHLCV is never stored in
the KB — only the meaningful derived facts that the LLM can reason about.

DESIGN RATIONALE
================
The KB is a triple store of interpretable facts, not a time-series database.
Storing raw daily OHLCV rows would produce ~100k atoms for 80 tickers × 252
days × 5 fields, flooding the KB with low-value price history that crowds
out signal atoms in retrieval.

Instead, this adapter extracts the facts that matter:
  - Return over standard windows (1w, 1m, 3m, 6m, 1y)
  - Realised volatility over standard windows (30d, 90d)
  - Drawdown from 52-week high
  - Anchoring reference prices (6m ago, 1y ago) for trend narrative
  - Average volume (30d) for liquidity classification
  - 52-week high/low levels (used by SignalEnrichmentAdapter for price_regime)
  - Relative performance vs SPY benchmark (1m, 3m)

For raw time-series storage (backtesting, regime detection), use a columnar
store (DuckDB, Parquet) — not this KB.

USAGE
=====
One-shot backfill (run once at startup or via POST /ingest/historical):

    from ingest.historical_adapter import HistoricalBackfillAdapter
    from knowledge.graph import TradingKnowledgeGraph

    kg = TradingKnowledgeGraph('trading_knowledge.db')
    adapter = HistoricalBackfillAdapter()
    result = adapter.run_and_push(kg)
    print(result)  # {'ingested': N, 'skipped': M}

Or via API:
    POST /ingest/historical          # all tickers
    POST /ingest/historical {"tickers": ["NVDA", "META"]}  # subset

Subsequent calls are idempotent — all atoms are upsert=True so they
update rather than append.

ATOMS PRODUCED
==============
All atoms use source prefix 'derived_signal_historical_{ticker}' (authority 0.65).

  Predicate              Example value         Notes
  ─────────────────────────────────────────────────────────────────────
  return_1w              "3.21"                % return over 5 trading days
  return_1m              "8.45"                % return over 21 trading days
  return_3m              "-4.12"               % return over 63 trading days
  return_6m              "22.80"               % return over 126 trading days
  return_1y              "67.40"               % return over 252 trading days
  return_3y              "+42.3"               % return over 756 trading days (~3yr)
  return_5y              "+67.8"               % return over 1260 trading days (~5yr)
  volatility_30d         "42.5"                annualised realised vol, 30-day window
  volatility_90d         "38.2"                annualised realised vol, 90-day window
  volatility_5y          "28.4"                annualised realised vol, full 5-year window
  max_drawdown_5y        "-42.1"               max peak-to-trough drawdown over 5yr history
  drawdown_from_52w_high "-12.4"               % drawdown from 52-week high close
  price_6m_ago           "143.00"              reference close price 126 trading days ago
  price_1y_ago           "98.50"               reference close price 252 trading days ago
  price_3y_ago           "82.40"               reference close price 756 trading days ago
  avg_volume_30d         "42500000"            mean daily volume over 30 trading days
  high_52w               "974.00"              52-week highest close
  low_52w                "392.30"              52-week lowest close
  return_vs_spy_1m       "3.8"                 excess return vs SPY over 1 month
  return_vs_spy_3m       "12.1"                excess return vs SPY over 3 months

  Predicate naming note: return values are stored as plain percent strings
  (e.g. "8.45" not "8.45%") for consistent numeric parsing.

CONFIDENCE LEVELS
=================
  Return / vol atoms:    0.80  (computed from direct price history, high accuracy)
  Reference prices:      0.85  (directly observed historical close)
  52w high/low:          0.85  (directly observed)
  Volume metrics:        0.75  (exchange data, minor staleness acceptable)
  Relative performance:  0.75  (depends on SPY also being present in download)

INTERVAL
========
NOT registered in the scheduler. Run once at startup or on demand via
POST /ingest/historical. Data is stable over hours; re-running daily is
sufficient if needed.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

try:
    from ingest.dynamic_watchlist import DynamicWatchlistManager
    _HAS_DYNAMIC_WATCHLIST = True
except ImportError:
    _HAS_DYNAMIC_WATCHLIST = False
    DynamicWatchlistManager = None  # type: ignore

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False
    yf = pd = np = None  # type: ignore

_logger = logging.getLogger(__name__)

# Trading day windows
_W_1W  =    5
_W_1M  =   21
_W_3M  =   63
_W_6M  =  126
_W_1Y  =  252
_W_3Y  =  756
_W_5Y  = 1260
_W_30D =   30
_W_90D =   90

# Annualisation factor for daily vol → annual
_ANNUAL_FACTOR = math.sqrt(252)

# Minimum rows required to compute a return for a given window
_MIN_ROWS_BUFFER = 5


def _pct_return(series, window: int) -> Optional[float]:
    """
    Compute the percent return over `window` bars ending at the last row.
    Returns None if insufficient data.

    For the 1y window (252 bars): uses series.iloc[0] as start when the
    series has fewer than window + _MIN_ROWS_BUFFER rows (common when the
    download period exactly matches the window length).
    """
    if len(series) < window:
        return None
    end = float(series.iloc[-1])
    # Use row 0 when the series is not long enough for window+1 look-back
    start_idx = -(window + 1) if len(series) >= window + _MIN_ROWS_BUFFER else 0
    start = float(series.iloc[start_idx])
    if start <= 0:
        return None
    return round((end - start) / start * 100, 2)


def _realised_vol(series, window: int) -> Optional[float]:
    """
    Compute annualised realised volatility (std of daily log returns) over
    the last `window` bars. Returns None if insufficient data.
    """
    if len(series) < window + _MIN_ROWS_BUFFER:
        return None
    returns = series.iloc[-window:].pct_change().dropna()
    if len(returns) < window // 2:
        return None
    vol = float(returns.std()) * _ANNUAL_FACTOR * 100
    return round(vol, 2)


def _drawdown_from_high(series) -> Optional[float]:
    """
    Percent drawdown of last price from 52-week high.
    Returns 0.0 if at/above 52-week high (shouldn't happen with close data but
    possible with intraday vs close discrepancy).
    """
    if series.empty:
        return None
    high = float(series.max())
    last = float(series.iloc[-1])
    if high <= 0:
        return None
    return round((last - high) / high * 100, 2)


def _max_drawdown(series) -> Optional[float]:
    """
    Maximum peak-to-trough drawdown over the full series.
    Uses the rolling cumulative maximum as the peak reference.
    Returns the worst percentage drawdown (negative number, e.g. -42.1).
    """
    if series is None or len(series) < 2:
        return None
    try:
        rolling_max = series.cummax()
        drawdowns   = (series - rolling_max) / rolling_max * 100
        mdd = float(drawdowns.min())
        return round(mdd, 2)
    except Exception:
        return None


class HistoricalBackfillAdapter(BaseIngestAdapter):
    """
    One-shot historical summary backfill. Downloads 5 years of daily OHLCV
    for all tickers in a single bulk call and emits interpretable summary atoms.

    Idempotent — all atoms are upsert=True.
    Not registered in the scheduler; call run_and_push() directly or via API.
    """

    def __init__(self, tickers: Optional[List[str]] = None, db_path: Optional[str] = None):
        super().__init__(name='historical_backfill')
        if tickers:
            self.tickers = [t.upper() for t in tickers]
        elif _HAS_DYNAMIC_WATCHLIST and db_path:
            self.tickers = DynamicWatchlistManager.get_active_tickers(db_path)
        else:
            # Import default list from yfinance adapter to stay in sync
            try:
                from ingest.yfinance_adapter import _DEFAULT_TICKERS
                self.tickers = list(_DEFAULT_TICKERS)
            except ImportError:
                self.tickers = []

    def fetch(self) -> List[RawAtom]:
        if not HAS_DEPS:
            _logger.error('yfinance, pandas, numpy required — pip install yfinance pandas numpy')
            return []
        if not self.tickers:
            _logger.warning('historical_backfill: no tickers configured')
            return []

        now_iso = datetime.now(timezone.utc).isoformat()

        # Filter out known-dead tickers before hitting Yahoo
        try:
            from ingest.yfinance_adapter import _STATIC_DEAD_TICKERS as _dead
            _dead_upper = {t.upper() for t in _dead}
        except ImportError:
            _dead_upper = set()
        active = [t for t in self.tickers if t.upper() not in _dead_upper]
        if len(active) < len(self.tickers):
            _logger.info(
                'historical_backfill: skipping %d dead tickers, %d active',
                len(self.tickers) - len(active), len(active),
            )

        _logger.info('historical_backfill: downloading 5y daily OHLCV for %d tickers', len(active))

        # ── Single bulk download: 5 years of daily closes + volume ───────────
        try:
            raw = yf.download(
                tickers   = active,
                period    = '5y',
                interval  = '1d',
                group_by  = 'ticker',
                auto_adjust = True,
                progress  = False,
                threads   = True,
            )
        except Exception as e:
            _logger.error('historical_backfill: yf.download failed: %s', e)
            return []

        if raw is None or raw.empty:
            _logger.warning('historical_backfill: empty download result')
            return []

        # ── Extract SPY returns as benchmark for relative performance ─────────
        spy_close = self._extract_close(raw, 'SPY')

        atoms: List[RawAtom] = []
        ok = 0
        skipped = 0

        for ticker in self.tickers:
            close  = self._extract_close(raw, ticker)
            volume = self._extract_volume(raw, ticker)

            if close is None or len(close) < _W_1M + _MIN_ROWS_BUFFER:
                skipped += 1
                continue

            src  = f'derived_signal_historical_{ticker.lower().replace("-", "_")}'
            meta = {'as_of': now_iso, 'rows': len(close)}

            ticker_atoms = self._compute_atoms(
                ticker, close, volume, spy_close, src, meta
            )
            atoms.extend(ticker_atoms)
            ok += 1

        _logger.info(
            'historical_backfill: computed atoms for %d tickers (%d skipped), %d total atoms',
            ok, skipped, len(atoms),
        )
        return atoms

    # ── Private helpers ───────────────────────────────────────────────────────

    def _extract_close(self, raw, ticker: str):
        """Extract the Close price series for a ticker from the bulk download."""
        try:
            if len(self.tickers) == 1:
                series = raw['Close']
            else:
                series = raw[ticker]['Close']
            series = series.dropna()
            return series if not series.empty else None
        except (KeyError, TypeError):
            return None

    def _extract_volume(self, raw, ticker: str):
        """Extract the Volume series for a ticker from the bulk download."""
        try:
            if len(self.tickers) == 1:
                series = raw['Volume']
            else:
                series = raw[ticker]['Volume']
            series = series.dropna()
            return series if not series.empty else None
        except (KeyError, TypeError):
            return None

    def _compute_atoms(
        self,
        ticker: str,
        close,
        volume,
        spy_close,
        src: str,
        meta: dict,
    ) -> List[RawAtom]:
        """Compute all summary atoms for one ticker."""
        atoms: List[RawAtom] = []

        def _atom(predicate, value, confidence=0.80):
            if value is None:
                return
            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = predicate,
                object     = str(value),
                confidence = confidence,
                source     = src,
                metadata   = meta,
                upsert     = True,
            ))

        # ── Return windows ────────────────────────────────────────────────────
        _atom('return_1w',  _pct_return(close, _W_1W))
        _atom('return_1m',  _pct_return(close, _W_1M))
        _atom('return_3m',  _pct_return(close, _W_3M))
        _atom('return_6m',  _pct_return(close, _W_6M))
        _atom('return_1y',  _pct_return(close, _W_1Y))
        _atom('return_3y',  _pct_return(close, _W_3Y))
        _atom('return_5y',  _pct_return(close, _W_5Y))

        # ── Realised volatility ───────────────────────────────────────────────
        _atom('volatility_30d', _realised_vol(close, _W_30D))
        _atom('volatility_90d', _realised_vol(close, _W_90D))
        _atom('volatility_5y',  _realised_vol(close, min(_W_5Y, len(close) - _MIN_ROWS_BUFFER)))

        # ── Drawdown from 52-week high ────────────────────────────────────────
        _atom('drawdown_from_52w_high', _drawdown_from_high(close))

        # ── 52-week high / low (absolute levels) ─────────────────────────────
        _atom('high_52w', round(float(close.max()), 2), confidence=0.85)
        _atom('low_52w',  round(float(close.min()), 2), confidence=0.85)

        # ── Max drawdown over 5y ──────────────────────────────────────────────
        _atom('max_drawdown_5y', _max_drawdown(close), confidence=0.85)

        # ── Reference prices (anchoring) ──────────────────────────────────────
        # 6 months ago
        if len(close) >= _W_6M + _MIN_ROWS_BUFFER:
            _atom('price_6m_ago', round(float(close.iloc[-(_W_6M + 1)]), 2), confidence=0.85)
        # 1 year ago
        if len(close) >= _W_1Y + _MIN_ROWS_BUFFER:
            _atom('price_1y_ago', round(float(close.iloc[-(_W_1Y + 1)]), 2), confidence=0.85)
        # 3 years ago
        if len(close) >= _W_3Y + _MIN_ROWS_BUFFER:
            _atom('price_3y_ago', round(float(close.iloc[-(_W_3Y + 1)]), 2), confidence=0.85)

        # ── Average volume (30d) ──────────────────────────────────────────────
        if volume is not None and len(volume) >= _W_30D:
            avg_vol = int(round(float(volume.iloc[-_W_30D:].mean())))
            _atom('avg_volume_30d', avg_vol, confidence=0.75)

        # ── Relative performance vs SPY ───────────────────────────────────────
        # Only compute when SPY is in the download and has enough data
        if spy_close is not None:
            spy_1m = _pct_return(spy_close, _W_1M)
            spy_3m = _pct_return(spy_close, _W_3M)
            tkr_1m = _pct_return(close, _W_1M)
            tkr_3m = _pct_return(close, _W_3M)

            if tkr_1m is not None and spy_1m is not None:
                _atom('return_vs_spy_1m', round(tkr_1m - spy_1m, 2), confidence=0.75)
            if tkr_3m is not None and spy_3m is not None:
                _atom('return_vs_spy_3m', round(tkr_3m - spy_3m, 2), confidence=0.75)

        return atoms

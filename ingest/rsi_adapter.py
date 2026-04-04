"""
ingest/rsi_adapter.py — RSI-14 Adapter

Computes Wilder RSI-14 from ohlcv_cache daily closes for every tracked ticker
and writes two atoms per ticker:

  rsi_14       — numeric RSI value e.g. "68.42"      (confidence 0.95)
  rsi_regime   — categorical label                    (confidence 0.95)

rsi_regime labels:
  overbought    rsi >= 70
  oversold      rsi <= 30
  neutral_high  rsi >= 55
  neutral_low   rsi <= 45
  neutral       otherwise (45 < rsi < 55)

The RSI gate in analytics/pattern_detector._quality() uses rsi_14 to penalise:
  -0.12  bullish entry when RSI >= 75  (overbought — high failure rate)
  -0.12  bearish entry when RSI <= 25  (oversold — high failure rate)

Requires at least 15 closes (14 deltas + 1 seed) from ohlcv_cache.
Registered in api_v2.py at interval_sec=3600 (hourly).
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_MIN_CANDLES = 15       # minimum closes needed for RSI-14
_OHLCV_AGE_DAYS = 60   # how many days back to load from ohlcv_cache


def _rsi14(closes: list) -> Optional[float]:
    """Standard Wilder RSI-14. Returns None if fewer than 15 closes."""
    if len(closes) < _MIN_CANDLES:
        return None
    deltas = [closes[i + 1] - closes[i] for i in range(len(closes) - 1)]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    avg_gain = sum(gains[:14]) / 14
    avg_loss = sum(losses[:14]) / 14
    for i in range(14, len(gains)):
        avg_gain = (avg_gain * 13 + gains[i]) / 14
        avg_loss = (avg_loss * 13 + losses[i]) / 14
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def _rsi_label(rsi: float) -> str:
    if rsi >= 70:
        return 'overbought'
    if rsi <= 30:
        return 'oversold'
    if rsi >= 55:
        return 'neutral_high'
    if rsi <= 45:
        return 'neutral_low'
    return 'neutral'


class RSIAdapter(BaseIngestAdapter):
    """
    Computes RSI-14 from daily OHLCV cache for all tracked tickers
    and writes rsi_14 + rsi_regime atoms to the KB.
    """

    def __init__(self, db_path: str = ''):
        super().__init__(name='rsi_adapter')
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        if not self._db_path:
            _logger.warning('rsi_adapter: no db_path configured')
            return []

        now_utc = datetime.now(timezone.utc)
        now_iso = now_utc.isoformat()
        cutoff  = (now_utc - timedelta(days=_OHLCV_AGE_DAYS)).isoformat()
        atoms: List[RawAtom] = []

        from db import HAS_POSTGRES, get_pg
        rows = []
        if HAS_POSTGRES:
            try:
                with get_pg() as pg:
                    cur = pg.cursor()
                    cur.execute(
                        "SELECT ticker, close, ts FROM ohlcv_cache "
                        "WHERE interval='1d' AND ts >= %s AND close IS NOT NULL "
                        "ORDER BY ticker, ts ASC", (cutoff,))
                    rows = [(r['ticker'], r['close'], r['ts']) for r in cur.fetchall()]
            except Exception as e:
                _logger.error('rsi_adapter: PG query failed: %s', e)
                rows = []
        if not rows:
            try:
                conn = sqlite3.connect(self._db_path, timeout=15)
                conn.execute('PRAGMA journal_mode=WAL')
            except Exception as e:
                _logger.error('rsi_adapter: DB connect failed: %s', e)
                return []
            try:
                rows = conn.execute(
                    """SELECT ticker, close, ts
                       FROM ohlcv_cache
                       WHERE interval = '1d'
                         AND ts >= ?
                         AND close IS NOT NULL
                       ORDER BY ticker, ts ASC""",
                    (cutoff,),
                ).fetchall()
            except Exception as e:
                _logger.error('rsi_adapter: DB query failed: %s', e)
                conn.close()
                return []
            conn.close()

        # Group closes by ticker
        ticker_closes: dict[str, list[float]] = {}
        for ticker, close, ts in rows:
            t = ticker.upper()
            if t not in ticker_closes:
                ticker_closes[t] = []
            ticker_closes[t].append(float(close))

        processed = 0
        skipped   = 0
        for ticker, closes in ticker_closes.items():
            rsi = _rsi14(closes)
            if rsi is None:
                skipped += 1
                continue

            label = _rsi_label(rsi)
            src   = f'rsi_{ticker.lower().replace("=", "_")}'

            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = 'rsi_14',
                object     = str(rsi),
                confidence = 0.95,
                source     = src,
                metadata   = {'candles': len(closes), 'as_of': now_iso},
                upsert     = True,
            ))
            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = 'rsi_regime',
                object     = label,
                confidence = 0.95,
                source     = src,
                metadata   = {'rsi': rsi, 'as_of': now_iso},
                upsert     = True,
            ))
            processed += 1

        _logger.info(
            'rsi_adapter: %d tickers processed, %d skipped (insufficient data), %d atoms',
            processed, skipped, len(atoms),
        )
        return atoms

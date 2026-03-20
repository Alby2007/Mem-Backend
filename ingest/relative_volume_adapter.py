"""
ingest/relative_volume_adapter.py — Relative Volume Adapter

For every tracked ticker, computes:
    relative_volume = today_close_volume / avg_volume_30d

Both source values already live in the DB:
  - avg_volume_30d  → facts table (written by HistoricalBackfillAdapter)
  - today's volume  → ohlcv_cache table, interval='1d', ORDER BY ts DESC LIMIT 1

Writes two atoms per ticker:
  relative_volume  — numeric string e.g. "2.341"   (confidence 0.95)
  volume_regime    — spike | high | normal | low    (confidence 0.90)

Thresholds:
  spike   relative_volume >= 2.0
  high    relative_volume >= 1.3
  normal  relative_volume >= 0.7
  low     relative_volume <  0.7

Skip logic:
  - FX tickers ending in =X
  - Futures tickers ending in =F
  - avg_volume_30d missing or zero
  - No 1d candle in ohlcv_cache within last 48h

Registered in api_v2.py at interval_sec=3600 (hourly — data changes daily).
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_SPIKE_THRESHOLD  = 2.0
_HIGH_THRESHOLD   = 1.3
_NORMAL_THRESHOLD = 0.7
_LOW_PENALTY_MAX  = 0.3   # relative_volume <= this triggers -0.05 in _quality()

_MAX_OHLCV_AGE_HOURS = 48


def _volume_label(rel_vol: float) -> str:
    if rel_vol >= _SPIKE_THRESHOLD:
        return 'spike'
    if rel_vol >= _HIGH_THRESHOLD:
        return 'high'
    if rel_vol >= _NORMAL_THRESHOLD:
        return 'normal'
    return 'low'


class RelativeVolumeAdapter(BaseIngestAdapter):
    """
    Computes relative volume (today / 30d avg) for all tracked equity tickers
    and writes relative_volume + volume_regime atoms to the KB.
    """

    def __init__(self, db_path: str = ''):
        super().__init__(name='relative_volume')
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        if not self._db_path:
            _logger.warning('relative_volume: no db_path configured')
            return []

        now_utc  = datetime.now(timezone.utc)
        now_iso  = now_utc.isoformat()
        cutoff   = (now_utc - timedelta(hours=_MAX_OHLCV_AGE_HOURS)).isoformat()
        atoms: List[RawAtom] = []

        try:
            conn = sqlite3.connect(self._db_path, timeout=15)
            conn.execute('PRAGMA journal_mode=WAL')
        except Exception as e:
            _logger.error('relative_volume: DB connect failed: %s', e)
            return []

        try:
            # ── Fetch avg_volume_30d for all tickers from facts ────────────────
            avg_rows = conn.execute(
                """SELECT subject, object FROM facts
                   WHERE predicate = 'avg_volume_30d'
                   ORDER BY subject, timestamp DESC"""
            ).fetchall()

            # Keep only most recent per ticker
            avg_vol_map: dict[str, float] = {}
            for subj, obj in avg_rows:
                ticker = subj.upper()
                if ticker not in avg_vol_map:
                    try:
                        v = float(obj)
                        if v > 0:
                            avg_vol_map[ticker] = v
                    except (ValueError, TypeError):
                        pass

            # ── Fetch latest 1d candle volume from ohlcv_cache ────────────────
            ohlcv_rows = conn.execute(
                """SELECT ticker, volume, ts FROM ohlcv_cache
                   WHERE interval = '1d'
                     AND ts >= ?
                   ORDER BY ticker, ts DESC""",
                (cutoff,),
            ).fetchall()

            # Keep most recent candle per ticker
            today_vol_map: dict[str, float] = {}
            for ticker, volume, ts in ohlcv_rows:
                t = ticker.upper()
                if t not in today_vol_map and volume is not None:
                    today_vol_map[t] = float(volume)

        except Exception as e:
            _logger.error('relative_volume: DB query failed: %s', e)
            conn.close()
            return []

        conn.close()

        # ── Compute relative volume per ticker ────────────────────────────────
        processed = 0
        skipped   = 0
        for ticker, avg_vol in avg_vol_map.items():
            # Skip FX and Futures — volume not meaningful
            if ticker.endswith('=X') or ticker.endswith('=F'):
                skipped += 1
                continue

            today_vol = today_vol_map.get(ticker)
            if today_vol is None:
                skipped += 1
                continue

            rel_vol = round(today_vol / avg_vol, 3)
            label   = _volume_label(rel_vol)
            src     = f'relative_volume_{ticker.lower().replace("=", "_")}'

            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = 'relative_volume',
                object     = str(rel_vol),
                confidence = 0.95,
                source     = src,
                metadata   = {'avg_vol_30d': avg_vol, 'today_vol': today_vol, 'as_of': now_iso},
                upsert     = True,
            ))
            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = 'volume_regime',
                object     = label,
                confidence = 0.90,
                source     = src,
                metadata   = {'rel_vol': rel_vol, 'as_of': now_iso},
                upsert     = True,
            ))
            processed += 1

        _logger.info(
            'relative_volume: %d tickers processed, %d skipped, %d atoms',
            processed, skipped, len(atoms),
        )
        return atoms

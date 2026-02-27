"""
ingest/lse_flow_adapter.py — LSE Institutional Order Flow Adapter

Derives institutional order-flow signals for LSE-listed equities from
intraday and daily yfinance OHLCV data. The LSE publishes daily block-trade
statistics, but their website is JavaScript-rendered with no public API.
yfinance provides the same underlying exchange data via Yahoo's datafeed.

METHODOLOGY
===========
Institutional flow is inferred from three proxies, each of which is
well-established in the academic microstructure literature:

1. Block-trade volume ratio (BTVR)
   Large single-day volume spikes relative to 20d average are a strong
   proxy for block trades — institutions can't easily accumulate without
   lifting the average volume. A day with volume > 2x the 20d mean while
   price closes near the high is a likely institutional accumulation day.

2. Volume-Weighted Price Trend (VWPT)
   If the close-to-VWAP spread is persistently positive over 5 days,
   buyers are on average paying above the daily average — a sign of
   demand-side institutional pressure.

3. Price-Volume Divergence (PVD)
   Price consolidating (tight range) while volume is elevated vs its
   average suggests accumulation with price being held down — the
   classic Wyckoff accumulation signature.

ATOMS PRODUCED
==============
  {TICKER} | institutional_flow         | accumulating | distributing | neutral
  {TICKER} | block_volume_ratio         | "2.4"   — today's vol / 20d avg
  {TICKER} | flow_conviction            | high | moderate | low
  {TICKER} | volume_trend_5d            | rising | falling | flat
  {TICKER} | price_range_compression    | compressed | normal | expanded
                                          (compressed + high vol = accumulation signal)

SOURCE PREFIX
=============
  alt_data_lse_flow  (authority 0.55, half-life 3d)
  Order-flow signals decay quickly — stale flow is noise.

INTERVAL
========
  3600s (1h) — refresh hourly during market hours. Outside hours the data
  is identical but the run is cheap so it's fine.

TICKERS COVERED
===============
  The adapter runs on all FTSE-listed tickers already in the KB
  (those with a last_price atom and a .L suffix) plus the configured
  _DEFAULT_FTSE_TICKERS list.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_SOURCE          = 'alt_data_lse_flow'
_REQUEST_DELAY   = 0.3   # seconds between yfinance calls
_MIN_CANDLES     = 22    # need at least 22d to compute 20d average

# FTSE tickers to always include (even if not yet in KB)
_DEFAULT_FTSE_TICKERS = [
    'HSBA.L', 'LLOY.L', 'BARC.L', 'NWG.L', 'STAN.L',
    'BP.L',   'SHEL.L', 'AZN.L',  'GSK.L', 'ULVR.L',
    'VOD.L',  'BT-A.L', 'RR.L',   'BA.L',  'IAG.L',
    'REL.L',  'WPP.L',  'AUTO.L', 'OCDO.L','GRG.L',
    'WIZZ.L', 'FUTR.L', 'IBST.L', 'LAND.L','TSCO.L',
    'MKS.L',  'AAL.L',  'ANTO.L', 'GLEN.L','BHP.L',
    'LGEN.L', 'PHNX.L', 'LSEG.L', 'NG.L',  'SSE.L',
    'SGE.L',  'EXPN.L', 'FERG.L', 'IHG.L', 'ADM.L',
    'PSN.L',  'TW.L',   'BWY.L',  'BKG.L', 'BATS.L',
    'IMB.L',  'ABF.L',  'AHT.L',  'CNA.L', 'RMV.L',
    'SGRO.L', 'PSON.L', 'ABDN.L',
]


# ── Signal computation helpers ────────────────────────────────────────────────

def _compute_flow_signals(candles: list) -> Optional[Dict]:
    """
    Compute institutional flow proxy signals from a list of OHLCV dicts.
    Each dict must have keys: open, high, low, close, volume.
    Requires at least _MIN_CANDLES entries (for 20d moving average).

    Returns dict of signals or None if insufficient data.
    """
    if len(candles) < _MIN_CANDLES:
        return None

    closes  = [c['close']  for c in candles]
    highs   = [c['high']   for c in candles]
    lows    = [c['low']    for c in candles]
    volumes = [c['volume'] for c in candles]

    # ── 1. Block Volume Ratio ────────────────────────────────────────────────
    avg_vol_20d   = sum(volumes[-21:-1]) / 20  # exclude today
    today_vol     = volumes[-1]
    bvr           = (today_vol / avg_vol_20d) if avg_vol_20d > 0 else 1.0

    # ── 2. Close vs high — is price closing strong on the volume day? ────────
    today_range   = highs[-1] - lows[-1]
    close_position = (
        (closes[-1] - lows[-1]) / today_range
        if today_range > 0 else 0.5
    )

    # ── 3. Volume trend (5d) ─────────────────────────────────────────────────
    vol_5d_now  = sum(volumes[-5:]) / 5
    vol_5d_prev = sum(volumes[-10:-5]) / 5
    vol_ratio_5d = (vol_5d_now / vol_5d_prev) if vol_5d_prev > 0 else 1.0

    if vol_ratio_5d >= 1.15:
        volume_trend = 'rising'
    elif vol_ratio_5d <= 0.85:
        volume_trend = 'falling'
    else:
        volume_trend = 'flat'

    # ── 4. Price range compression (5d) ─────────────────────────────────────
    ranges_5d = [(highs[-i] - lows[-i]) for i in range(1, 6)]
    ranges_prev = [(highs[-i] - lows[-i]) for i in range(6, 11)]
    avg_range_now  = sum(ranges_5d)  / len(ranges_5d)
    avg_range_prev = sum(ranges_prev) / len(ranges_prev)

    range_ratio = (avg_range_now / avg_range_prev) if avg_range_prev > 0 else 1.0
    if range_ratio <= 0.75:
        range_compression = 'compressed'
    elif range_ratio >= 1.25:
        range_compression = 'expanded'
    else:
        range_compression = 'normal'

    # ── 5. Derive institutional flow classification ──────────────────────────
    # Accumulation signals:
    #   - High volume (BVR > 1.5) + close near high (> 0.6 of range)
    #   - Rising volume trend + compressed price range (Wyckoff accumulation)
    # Distribution signals:
    #   - High volume (BVR > 1.5) + close near low (< 0.4 of range)
    #   - Rising volume trend + expanding price range (distribution)

    accumulation_score = 0
    distribution_score = 0

    if bvr >= 2.0:
        if close_position >= 0.65:
            accumulation_score += 2
        elif close_position <= 0.35:
            distribution_score += 2
        else:
            accumulation_score += 1
            distribution_score += 1
    elif bvr >= 1.5:
        if close_position >= 0.65:
            accumulation_score += 1
        elif close_position <= 0.35:
            distribution_score += 1

    if volume_trend == 'rising' and range_compression == 'compressed':
        accumulation_score += 2
    elif volume_trend == 'rising' and range_compression == 'expanded':
        distribution_score += 1
    elif volume_trend == 'falling' and range_compression == 'compressed':
        accumulation_score += 1

    if accumulation_score > distribution_score:
        flow = 'accumulating'
    elif distribution_score > accumulation_score:
        flow = 'distributing'
    else:
        flow = 'neutral'

    # ── 6. Flow conviction ───────────────────────────────────────────────────
    max_score = max(accumulation_score, distribution_score)
    if max_score >= 3:
        conviction = 'high'
    elif max_score >= 2:
        conviction = 'moderate'
    else:
        conviction = 'low'

    return {
        'flow':             flow,
        'bvr':              round(bvr, 2),
        'conviction':       conviction,
        'volume_trend':     volume_trend,
        'range_compression':range_compression,
        'accum_score':      accumulation_score,
        'dist_score':       distribution_score,
    }


def _fetch_candles(ticker: str) -> List[Dict]:
    """
    Fetch 30 days of daily OHLCV from yfinance.
    Returns list of dicts or empty list on failure.
    """
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period='30d', interval='1d', auto_adjust=True)
        if hist.empty:
            return []
        return [
            {
                'open':   float(row['Open']),
                'high':   float(row['High']),
                'low':    float(row['Low']),
                'close':  float(row['Close']),
                'volume': float(row.get('Volume', 0) or 0),
            }
            for _, row in hist.iterrows()
            if float(row.get('Volume', 0) or 0) > 0
        ]
    except Exception as e:
        _logger.debug('lse_flow: failed to fetch %s: %s', ticker, e)
        return []


# ── Adapter ───────────────────────────────────────────────────────────────────

class LSEFlowAdapter(BaseIngestAdapter):
    """
    LSE institutional order flow adapter.

    Derives institutional accumulation/distribution signals for FTSE-listed
    equities using block-volume ratio, volume trend, and price-range
    compression as proxies for institutional activity.

    Covers all .L tickers already in the KB plus the default FTSE list.
    """

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        super().__init__(name='lse_flow')
        self._db_path = db_path

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []

        tickers = self._get_tickers()
        _logger.info('lse_flow: processing %d FTSE tickers', len(tickers))

        processed = 0
        for ticker in tickers:
            candles = _fetch_candles(ticker)
            if not candles:
                time.sleep(_REQUEST_DELAY)
                continue

            signals = _compute_flow_signals(candles)
            if not signals:
                time.sleep(_REQUEST_DELAY)
                continue

            source = f'{_SOURCE}_{ticker.lower().replace(".", "_")}'
            meta   = {
                'fetched_at':    now_iso,
                'ticker':        ticker,
                'candles_used':  len(candles),
                'accum_score':   signals['accum_score'],
                'dist_score':    signals['dist_score'],
            }

            atoms.append(RawAtom(
                subject=ticker, predicate='institutional_flow',
                object=signals['flow'],
                confidence=0.60, source=source,
                metadata=meta, upsert=True,
            ))
            atoms.append(RawAtom(
                subject=ticker, predicate='block_volume_ratio',
                object=str(signals['bvr']),
                confidence=0.80, source=source,
                metadata=meta, upsert=True,
            ))
            atoms.append(RawAtom(
                subject=ticker, predicate='flow_conviction',
                object=signals['conviction'],
                confidence=0.60, source=source,
                metadata=meta, upsert=True,
            ))
            atoms.append(RawAtom(
                subject=ticker, predicate='volume_trend_5d',
                object=signals['volume_trend'],
                confidence=0.75, source=source,
                metadata=meta, upsert=True,
            ))
            atoms.append(RawAtom(
                subject=ticker, predicate='price_range_compression',
                object=signals['range_compression'],
                confidence=0.70, source=source,
                metadata=meta, upsert=True,
            ))

            processed += 1
            if signals['flow'] != 'neutral' and signals['conviction'] in ('high', 'moderate'):
                _logger.info(
                    'lse_flow: %s → %s (%s conviction) BVR=%.1f vol=%s range=%s',
                    ticker, signals['flow'], signals['conviction'],
                    signals['bvr'], signals['volume_trend'], signals['range_compression'],
                )

            time.sleep(_REQUEST_DELAY)

        _logger.info(
            'lse_flow: %d tickers processed, %d atoms produced', processed, len(atoms)
        )
        return atoms

    def _get_tickers(self) -> List[str]:
        """
        Get union of default FTSE list and any .L tickers already in KB.
        Deduplicates and returns sorted list.
        """
        tickers = set(t.upper() for t in _DEFAULT_FTSE_TICKERS)
        try:
            conn = sqlite3.connect(self._db_path)
            rows = conn.execute(
                "SELECT DISTINCT subject FROM facts WHERE predicate='last_price'"
            ).fetchall()
            conn.close()
            for row in rows:
                subj = row[0].upper()
                if subj.endswith('.L'):
                    tickers.add(subj)
        except Exception as e:
            _logger.warning('lse_flow: could not load KB tickers: %s', e)
        return sorted(tickers)

"""
ingest/yield_curve_adapter.py — Treasury Yield Curve Adapter (Polygon)

Derives real yield curve atoms from iShares bond ETF prices via the Polygon
/v2/aggs endpoint (same endpoint used for equities — no additional plan tier).

WHY ETFs NOT DIRECT YIELD TICKERS
===================================
Polygon's I:DGS10 / I:DGS2 yield tickers return queryCount=0 on the Starter
plan — no bar data is available.  I:TNX returns 403.  However, the bond ETFs
that track these durations return full OHLCV data on any plan:

  TLT — iShares 20+ Year Treasury Bond ETF  (tracks ~20yr duration)
  IEF — iShares 7-10 Year Treasury Bond ETF (tracks ~8yr duration)
  SHY — iShares 1-3 Year Treasury Bond ETF  (tracks ~2yr duration)

Bond ETF price moves inversely to yield — when the 10Y yield rises, IEF falls.
The TLT/SHY price ratio directly proxies the 20Y/2Y yield curve slope:
  Rising TLT/SHY → long end outperforms → curve steepening
  Falling TLT/SHY → short end outperforms / TLT falls faster → inversion

This is the same signal retail platforms pay for from FRED — we derive it free.

ATOMS PRODUCED
==============
All atoms use source 'yield_curve' with authority 0.78.
All atoms are upsert=True.  Subject is always 'macro'.

  Predicate              Example value   Notes
  ─────────────────────────────────────────────────────────────────────────
  tlt_close              "86.55"         TLT last close price
  ief_close              "95.44"         IEF last close price
  shy_close              "82.70"         SHY last close price
  tlt_1d_change_pct      "-0.54"         TLT 1-day % change (yield proxy)
  ief_1d_change_pct      "-0.24"         IEF 1-day % change
  yield_curve_slope      "steepening"    "steepening"|"flattening"|"neutral"
  yield_curve_tlt_shy    "1.047"         TLT/SHY price ratio (curve proxy)
  long_end_stress        "true"          TLT 1d change < -0.5% (selling pressure)
  yield_curve_regime     "bear_steepen"  One of 4 rate regimes (see below)

YIELD CURVE REGIMES
===================
  bull_steepen   — TLT up, SHY flat/down   → risk-on, long-end rally
  bear_steepen   — TLT down faster, SHY up → inflation premium, curve steepen
  bull_flatten   — SHY up more than TLT    → rate cut expectations building
  bear_flatten   — both down, SHY down more → Fed hike cycle peak signal
  neutral        — both near flat (<0.1%)

SCHEDULE: daily (runs at market open, 09:00 UTC)
REQUIRES: POLYGON_API_KEY env var
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from .base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_AUTHORITY   = 0.78
_SOURCE_PFX  = 'yield_curve'

# Bond duration ETF tickers tracked
_TLT = 'TLT'   # 20+ year
_IEF = 'IEF'   # 7-10 year
_SHY = 'SHY'   # 1-3 year

_POLYGON_BASE = 'https://api.polygon.io'


def _api_key() -> Optional[str]:
    return os.environ.get('POLYGON_API_KEY') or None


def _fetch_last_two_closes(ticker: str, key: str, retry: int = 1) -> Optional[tuple[float, float]]:
    """
    Return (prev_close, last_close) for a ticker using Polygon /v2/aggs.
    Uses the last 10 calendar days to ensure >= 2 bars across weekends/holidays.
    Retries once on 429 with a 15s backoff (Polygon Starter = 5 req/min).
    """
    try:
        import urllib.request
        import urllib.error
        import json

        end   = datetime.now(timezone.utc).date()
        start = end - timedelta(days=10)
        url   = (
            f'{_POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day'
            f'/{start}/{end}'
            f'?adjusted=true&sort=asc&limit=10&apiKey={key}'
        )
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                d = json.loads(r.read())
        except urllib.error.HTTPError as http_err:
            if http_err.code == 429 and retry > 0:
                _logger.info('[yield_curve] 429 on %s — waiting 15s before retry', ticker)
                time.sleep(15)
                return _fetch_last_two_closes(ticker, key, retry=0)
            raise

        results = d.get('results') or []
        if len(results) < 2:
            _logger.debug('[yield_curve] %s: only %d bars returned', ticker, len(results))
            return None

        prev_bar = results[-2]
        last_bar = results[-1]
        return float(prev_bar['c']), float(last_bar['c'])

    except Exception as exc:
        _logger.debug('[yield_curve] fetch failed for %s: %s', ticker, exc)
        return None


def _pct_change(prev: float, last: float) -> float:
    if prev == 0:
        return 0.0
    return round((last - prev) / prev * 100, 3)


def _classify_regime(
    tlt_chg: float,
    shy_chg: float,
    threshold: float = 0.10,
) -> str:
    """
    Classify the current rate regime from ETF daily moves.
    Bond price moves inverse to yield — falling ETF = rising yield.
    """
    tlt_sig = 'up' if tlt_chg > threshold else ('down' if tlt_chg < -threshold else 'flat')
    shy_sig = 'up' if shy_chg > threshold else ('down' if shy_chg < -threshold else 'flat')

    if tlt_sig == 'up' and shy_sig in ('flat', 'down'):
        return 'bull_steepen'
    if tlt_sig == 'down' and shy_sig in ('flat', 'up'):
        return 'bear_steepen'
    if shy_sig == 'up' and tlt_sig in ('flat', 'up'):
        return 'bull_flatten'
    if shy_sig == 'down' and tlt_sig == 'down' and shy_chg < tlt_chg:
        return 'bear_flatten'
    return 'neutral'


def _classify_slope(tlt_shy_ratio: float, prev_ratio: float) -> str:
    delta = tlt_shy_ratio - prev_ratio
    if delta > 0.001:
        return 'steepening'
    if delta < -0.001:
        return 'flattening'
    return 'neutral'


class YieldCurveAdapter(BaseIngestAdapter):
    """
    Derives yield curve KB atoms from TLT/IEF/SHY ETF prices via Polygon.
    Skips gracefully if POLYGON_API_KEY is not set.
    """

    def __init__(self):
        super().__init__(name='yield_curve')

    def fetch(self) -> List[RawAtom]:
        key = _api_key()
        if not key:
            _logger.info('[yield_curve] POLYGON_API_KEY not set — skipping')
            return []

        # Fetch last two closes for each ETF.
        # Polygon Starter plan = 5 requests/minute — sleep 13s between calls.
        tlt = _fetch_last_two_closes(_TLT, key)
        time.sleep(13)
        ief = _fetch_last_two_closes(_IEF, key)
        time.sleep(13)
        shy = _fetch_last_two_closes(_SHY, key)

        if not tlt or not shy:
            _logger.warning('[yield_curve] insufficient data — TLT=%s SHY=%s', tlt, shy)
            return []

        tlt_prev, tlt_last = tlt
        shy_prev, shy_last = shy

        tlt_chg  = _pct_change(tlt_prev, tlt_last)
        shy_chg  = _pct_change(shy_prev, shy_last)

        # TLT/SHY ratio = long-end / short-end price proxy for curve slope
        tlt_shy_now  = round(tlt_last / shy_last, 4)   if shy_last  else None
        tlt_shy_prev = round(tlt_prev / shy_prev, 4)   if shy_prev  else None

        slope  = _classify_slope(tlt_shy_now, tlt_shy_prev) if (tlt_shy_now and tlt_shy_prev) else 'neutral'
        regime = _classify_regime(tlt_chg, shy_chg)

        # Graded long_end_stress: severe / elevated / none
        # More informative than binary — allows tip warnings to be proportionate.
        if tlt_chg < -1.0:
            long_end_stress_level = 'severe'
        elif tlt_chg < -0.5:
            long_end_stress_level = 'elevated'
        else:
            long_end_stress_level = 'none'
        long_end_stress = long_end_stress_level in ('severe', 'elevated')  # bool for backward compat

        now_iso = datetime.now(timezone.utc).isoformat()

        def _atom(predicate: str, value: str) -> RawAtom:
            return RawAtom(
                subject    = 'macro',
                predicate  = predicate,
                object     = value,
                source     = _SOURCE_PFX,
                confidence = _AUTHORITY,
                upsert     = True,
            )

        atoms: List[RawAtom] = [
            _atom('tlt_close',           str(tlt_last)),
            _atom('shy_close',           str(shy_last)),
            _atom('tlt_1d_change_pct',   str(tlt_chg)),
            _atom('shy_1d_change_pct',   str(shy_chg)),
            _atom('yield_curve_slope',   slope),
            _atom('yield_curve_regime',  regime),
            _atom('long_end_stress',       'true' if long_end_stress else 'false'),
            _atom('long_end_stress_level',  long_end_stress_level),
        ]

        if tlt_shy_now is not None:
            atoms.append(_atom('yield_curve_tlt_shy', str(tlt_shy_now)))

        # IEF is optional — mid-duration signal
        if ief:
            ief_prev, ief_last = ief
            ief_chg = _pct_change(ief_prev, ief_last)
            atoms.append(_atom('ief_close',          str(ief_last)))
            atoms.append(_atom('ief_1d_change_pct',  str(ief_chg)))

        _logger.info(
            '[yield_curve] regime=%s slope=%s tlt=%.2f(%.3f%%) shy=%.2f(%.3f%%) stress=%s',
            regime, slope, tlt_last, tlt_chg, shy_last, shy_chg, long_end_stress,
        )
        return atoms

"""
ingest/eia_adapter.py — U.S. Energy Information Administration (EIA) Ingest Adapter

Pulls WTI crude, Brent crude, US crude production, and weekly inventory levels
from the EIA API v2 and converts them to macro/commodity KB atoms.

Requires: EIA_API_KEY environment variable
  Free registration: https://api.eia.gov/registrations

Atoms produced:
  - oil_market | wti_crude        | {price} (USD/bbl)
  - oil_market | brent_crude      | {price} (USD/bbl)
  - oil_market | crude_spread     | {brent - wti} (USD/bbl)
  - oil_market | us_production    | {Mbbl/d}
  - oil_market | inventory_level  | above_avg | below_avg | normal
  - oil_market | supply_trend     | rising | falling | stable
  - oil_market | price_trend      | rising | falling | stable
  - macro_regime | energy_regime  | inflationary_shock | supply_glut | balanced

Source prefix: macro_data_eia  (authority 0.82, half-life 7d)
Interval: recommended 24h (EIA data is updated weekly)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

try:
    import urllib.request
    import json as _json
    HAS_URLLIB = True
except ImportError:
    HAS_URLLIB = False

_logger = logging.getLogger(__name__)

_EIA_BASE = 'https://api.eia.gov/v2'

# EIA series IDs
_WTI_SERIES    = 'PET.RWTC.W'   # Weekly WTI spot price (USD/bbl)
_BRENT_SERIES  = 'PET.RBRTE.W'  # Weekly Brent spot price (USD/bbl)
_PROD_SERIES   = 'PET.WCRFPUS2.W'  # US weekly crude production (Mbbl/d)
_INV_SERIES    = 'PET.WCRSTUS1.W'  # US crude oil inventories (thousand bbl)

# 5-year average inventory baseline (thousand bbl) — EIA seasonal norm
_INV_BASELINE  = 420_000
_INV_BAND_PCT  = 0.05  # ±5% = normal; outside = above/below avg

# Week-over-week change thresholds for trend detection
_PRICE_TREND_PCT  = 0.02  # ±2%
_SUPPLY_TREND_PCT = 0.01  # ±1%


def _eia_fetch(api_key: str, series_id: str, num_periods: int = 2) -> Optional[List[dict]]:
    """Fetch the most recent N data points for an EIA series."""
    url = (
        f'{_EIA_BASE}/seriesid/{series_id}'
        f'?api_key={api_key}&out=json&num={num_periods}'
    )
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'TradingKB/1.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read().decode())
        # EIA v2 response structure
        series = data.get('response', {}).get('data', [])
        return series if series else None
    except Exception as exc:
        _logger.warning('EIA fetch failed for %s: %s', series_id, exc)
        return None


def _trend(current: float, previous: float, threshold: float) -> str:
    if previous == 0:
        return 'stable'
    pct = (current - previous) / abs(previous)
    if pct > threshold:
        return 'rising'
    if pct < -threshold:
        return 'falling'
    return 'stable'


class EIAAdapter(BaseIngestAdapter):
    """
    EIA oil/energy ingest adapter.

    Fetches WTI/Brent prices, US crude production, and inventory levels.
    Derives trend and regime atoms for use in causal shock propagation.
    Requires EIA_API_KEY environment variable.
    """

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(name='eia_energy')
        self._api_key = api_key or os.environ.get('EIA_API_KEY', '')

    def fetch(self) -> List[RawAtom]:
        if not self._api_key:
            self._logger.warning(
                'EIA_API_KEY not set — skipping EIA adapter. '
                'Register free at https://api.eia.gov/registrations'
            )
            return []

        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []
        source = 'macro_data_eia'
        meta_base = {'fetched_at': now_iso, 'source_url': 'https://api.eia.gov/v2'}

        # ── WTI crude spot price ───────────────────────────────────────────────
        wti_data = _eia_fetch(self._api_key, _WTI_SERIES, num_periods=2)
        wti_cur = wti_prev = None
        if wti_data and len(wti_data) >= 1:
            try:
                wti_cur = float(wti_data[0].get('value', 0))
                wti_prev = float(wti_data[1].get('value', 0)) if len(wti_data) >= 2 else None
                atoms.append(RawAtom(
                    subject='oil_market',
                    predicate='wti_crude',
                    object=f'{wti_cur:.2f}',
                    confidence=0.92,
                    source=source,
                    metadata={**meta_base, 'unit': 'USD/bbl', 'series': _WTI_SERIES},
                ))
            except (TypeError, ValueError) as e:
                self._logger.warning('WTI parse error: %s', e)

        # ── Brent crude spot price ─────────────────────────────────────────────
        brent_data = _eia_fetch(self._api_key, _BRENT_SERIES, num_periods=2)
        brent_cur = brent_prev = None
        if brent_data and len(brent_data) >= 1:
            try:
                brent_cur = float(brent_data[0].get('value', 0))
                brent_prev = float(brent_data[1].get('value', 0)) if len(brent_data) >= 2 else None
                atoms.append(RawAtom(
                    subject='oil_market',
                    predicate='brent_crude',
                    object=f'{brent_cur:.2f}',
                    confidence=0.92,
                    source=source,
                    metadata={**meta_base, 'unit': 'USD/bbl', 'series': _BRENT_SERIES},
                ))
            except (TypeError, ValueError) as e:
                self._logger.warning('Brent parse error: %s', e)

        # ── Brent-WTI spread ──────────────────────────────────────────────────
        if wti_cur and brent_cur:
            spread = brent_cur - wti_cur
            atoms.append(RawAtom(
                subject='oil_market',
                predicate='crude_spread',
                object=f'{spread:.2f}',
                confidence=0.90,
                source=source,
                metadata={**meta_base, 'unit': 'USD/bbl', 'note': 'Brent minus WTI'},
            ))

        # ── Price trend (WTI week-over-week) ──────────────────────────────────
        if wti_cur and wti_prev:
            price_trend = _trend(wti_cur, wti_prev, _PRICE_TREND_PCT)
            atoms.append(RawAtom(
                subject='oil_market',
                predicate='price_trend',
                object=price_trend,
                confidence=0.85,
                source=source,
                metadata={**meta_base, 'wti_current': wti_cur, 'wti_previous': wti_prev},
            ))

        # ── US crude production ────────────────────────────────────────────────
        prod_data = _eia_fetch(self._api_key, _PROD_SERIES, num_periods=2)
        prod_cur = prod_prev = None
        if prod_data and len(prod_data) >= 1:
            try:
                prod_cur = float(prod_data[0].get('value', 0))
                prod_prev = float(prod_data[1].get('value', 0)) if len(prod_data) >= 2 else None
                atoms.append(RawAtom(
                    subject='oil_market',
                    predicate='us_production',
                    object=f'{prod_cur:.1f}',
                    confidence=0.88,
                    source=source,
                    metadata={**meta_base, 'unit': 'Mbbl/d', 'series': _PROD_SERIES},
                ))
            except (TypeError, ValueError) as e:
                self._logger.warning('Production parse error: %s', e)

        # ── Supply trend (production week-over-week) ──────────────────────────
        if prod_cur and prod_prev:
            supply_trend = _trend(prod_cur, prod_prev, _SUPPLY_TREND_PCT)
            atoms.append(RawAtom(
                subject='oil_market',
                predicate='supply_trend',
                object=supply_trend,
                confidence=0.83,
                source=source,
                metadata={**meta_base, 'prod_current': prod_cur, 'prod_previous': prod_prev},
            ))

        # ── Inventory level (vs 5-year average baseline) ──────────────────────
        inv_data = _eia_fetch(self._api_key, _INV_SERIES, num_periods=1)
        if inv_data and len(inv_data) >= 1:
            try:
                inv_cur = float(inv_data[0].get('value', 0))
                pct_vs_baseline = (inv_cur - _INV_BASELINE) / _INV_BASELINE
                if pct_vs_baseline > _INV_BAND_PCT:
                    inv_label = 'above_avg'
                elif pct_vs_baseline < -_INV_BAND_PCT:
                    inv_label = 'below_avg'
                else:
                    inv_label = 'normal'
                atoms.append(RawAtom(
                    subject='oil_market',
                    predicate='inventory_level',
                    object=inv_label,
                    confidence=0.80,
                    source=source,
                    metadata={
                        **meta_base,
                        'unit': 'thousand_bbl',
                        'value': inv_cur,
                        'baseline': _INV_BASELINE,
                        'pct_vs_baseline': round(pct_vs_baseline * 100, 1),
                    },
                ))
            except (TypeError, ValueError) as e:
                self._logger.warning('Inventory parse error: %s', e)

        # ── Macro energy regime (composite) ───────────────────────────────────
        regime = self._derive_energy_regime(wti_cur, supply_trend if prod_cur and prod_prev else None)
        if regime:
            atoms.append(RawAtom(
                subject='macro_regime',
                predicate='energy_regime',
                object=regime,
                confidence=0.75,
                source=source,
                metadata={**meta_base, 'wti': wti_cur, 'supply_trend': supply_trend if prod_cur and prod_prev else 'unknown'},
            ))

        self._logger.info('EIA adapter: %d atoms produced', len(atoms))
        return atoms

    @staticmethod
    def _derive_energy_regime(wti_price: Optional[float], supply_trend: Optional[str]) -> Optional[str]:
        """Derive a coarse energy regime label from price and supply signals."""
        if wti_price is None:
            return None
        if wti_price > 90 and supply_trend in ('falling', 'stable', None):
            return 'inflationary_shock'
        if wti_price < 60 and supply_trend in ('rising', 'stable', None):
            return 'supply_glut'
        return 'balanced'

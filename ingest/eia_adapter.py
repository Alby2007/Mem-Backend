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

# 5-year average inventory baseline (thousand bbl) — EIA seasonal norm
_INV_BASELINE  = 420_000
_INV_BAND_PCT  = 0.05  # ±5% = normal; outside = above/below avg

# Week-over-week change thresholds for trend detection
_PRICE_TREND_PCT  = 0.02  # ±2%
_SUPPLY_TREND_PCT = 0.01  # ±1%


def _eia_fetch(
    api_key: str,
    path: str,
    facets: dict,
    num_periods: int = 2,
) -> Optional[List[dict]]:
    """Fetch the most recent N data points using EIA APIv2 path/facet routing."""
    import urllib.parse
    facet_params = '&'.join(
        f'facets[{k}][]={urllib.parse.quote(str(v))}'
        for k, vals in facets.items()
        for v in vals
    )
    url = (
        f'{_EIA_BASE}/{path}/?api_key={api_key}'
        f'&frequency=weekly&data[0]=value'
        f'&{facet_params}'
        f'&sort[0][column]=period&sort[0][direction]=desc'
        f'&offset=0&length={num_periods}'
    )
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'TradingKB/1.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read().decode())
        rows = data.get('response', {}).get('data', [])
        return rows if rows else None
    except Exception as exc:
        _logger.warning('EIA fetch failed for %s: %s', path, exc)
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
        wti_data = (_eia_fetch(self._api_key, 'petroleum/pri/spt/data', {'product': ['EPCWTI']}, 2)
                    or _eia_fetch(self._api_key, 'petroleum/pri/spt/data', {'series': ['RWTC']}, 2))
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
                    metadata={**meta_base, 'unit': 'USD/bbl'},
                ))
            except (TypeError, ValueError) as e:
                self._logger.warning('WTI parse error: %s', e)

        # ── Brent crude spot price ─────────────────────────────────────────────
        brent_data = (_eia_fetch(self._api_key, 'petroleum/pri/spt/data', {'product': ['EPBRENT']}, 2)
                      or _eia_fetch(self._api_key, 'petroleum/pri/spt/data', {'series': ['RBRTE']}, 2))
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
                    metadata={**meta_base, 'unit': 'USD/bbl'},
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
        prod_data = (_eia_fetch(self._api_key, 'petroleum/crd/crpdn/agg/mbbl/a/data', {'duoarea': ['NUS']}, 2)
                     or _eia_fetch(self._api_key, 'petroleum/crd/crpdn/data', {'duoarea': ['NUS'], 'process': ['FPD']}, 2))
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
                    metadata={**meta_base, 'unit': 'Mbbl/d'},
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
        inv_data = (_eia_fetch(self._api_key, 'petroleum/stoc/wstk/data', {'product': ['EPC0']}, 1)
                    or _eia_fetch(self._api_key, 'petroleum/stoc/wstk/data', {'product': ['EPC0'], 'duoarea': ['NUS']}, 1))
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

        # ── Energy ticker supply-risk linkage ────────────────────────────────
        # Write supply_disruption_risk atoms keyed by major energy tickers so
        # that BP/Shell/XOM queries surface EIA supply context directly.
        _ENERGY_TICKERS = ['bp.l', 'shel.l', 'xom', 'cvx', 'cop']
        _supply_trend_val = supply_trend if (prod_cur and prod_prev) else None
        if wti_cur is not None:
            _risk_label = None
            if wti_cur > 85 and _supply_trend_val in ('falling', 'stable', None):
                _risk_label = 'elevated'
            elif wti_cur > 75:
                _risk_label = 'moderate'
            elif wti_cur < 60:
                _risk_label = 'low_supply_risk'
            if _risk_label:
                for _eticker in _ENERGY_TICKERS:
                    atoms.append(RawAtom(
                        subject=_eticker,
                        predicate='supply_disruption_risk',
                        object=_risk_label,
                        confidence=0.72,
                        source=source,
                        metadata={
                            **meta_base,
                            'wti_crude': wti_cur,
                            'supply_trend': _supply_trend_val or 'unknown',
                        },
                        upsert=True,
                    ))

        # ── Henry Hub natural gas spot price ──────────────────────────────────
        # Note: 'process' facet on natural-gas/pri/sum returns 400 on EIA APIv2.
        # Use series facet (RNGWHHD) or the ng/cons/sum endpoint as primary.
        gas_data = (_eia_fetch(self._api_key, 'natural-gas/pri/sum/data', {'series': ['RNGWHHD']}, 2)
                    or _eia_fetch(self._api_key, 'natural-gas/pri/fut/data', {'series': ['RNGC1']}, 2))
        if gas_data and len(gas_data) >= 1:
            try:
                gas_cur = float(gas_data[0].get('value', 0))
                gas_prev = float(gas_data[1].get('value', 0)) if len(gas_data) >= 2 else None
                atoms.append(RawAtom(
                    subject='gas_market',
                    predicate='henry_hub_price',
                    object=f'{gas_cur:.2f}',
                    confidence=0.92,
                    source=source,
                    metadata={**meta_base, 'unit': 'USD/MMBtu'},
                ))
                if gas_prev:
                    gas_trend = _trend(gas_cur, gas_prev, _PRICE_TREND_PCT)
                    atoms.append(RawAtom(
                        subject='gas_market',
                        predicate='price_trend',
                        object=gas_trend,
                        confidence=0.85,
                        source=source,
                        metadata={**meta_base, 'gas_current': gas_cur, 'gas_previous': gas_prev},
                    ))
            except (TypeError, ValueError) as e:
                self._logger.warning('Henry Hub parse error: %s', e)

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

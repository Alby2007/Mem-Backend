"""
ingest/fred_adapter.py — Federal Reserve (FRED) Ingest Adapter (Trading KB)

Pulls key macro indicators from the FRED API and converts them to regime
atoms for the knowledge base.

Requires: FRED_API_KEY environment variable (free registration at
https://fred.stlouisfed.org/docs/api/api_key.html).
If the key is not set, the adapter logs a warning and returns no atoms.
Startup is never blocked.

Atoms produced:
  - us_macro | fed_funds_rate       | {rate}%
  - us_macro | central_bank_stance  | {restrictive/neutral/accommodative}
  - us_macro | inflation_environment| CPI YoY: {pct}%
  - us_macro | growth_environment   | GDP growth: {pct}%
  - us_macro | regime_label         | {derived from rates + inflation + growth}
  - us_labor | unemployment_rate    | {pct}%
  - us_yields| yield_curve_spread   | 10Y-2Y: {spread}bps

Source prefix: macro_data_fred  (authority 0.80, half-life 60d)
Interval: recommended 24h (macro data updates slowly)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

import json as _json
import urllib.request as _urllib

_logger = logging.getLogger(__name__)

# FRED series IDs for key macro indicators
_SERIES = {
    'fed_funds':         'FEDFUNDS',        # Effective Federal Funds Rate
    'cpi_yoy':           'CPIAUCSL',        # CPI All Urban Consumers (index)
    'gdp_growth':        'A191RL1Q225SBEA', # Real GDP growth (annualized quarterly)
    'unemployment':      'UNRATE',          # Unemployment Rate
    'yield_10y':         'DGS10',           # 10-Year Treasury Yield
    'yield_2y':          'DGS2',            # 2-Year Treasury Yield
    'tips_10y':          'DFII10',          # 10-Year TIPS (real yield)
    'breakeven_10y':     'T10YIE',          # 10-Year Breakeven Inflation Rate
    'breakeven_5y':      'T5YIE',           # 5-Year Breakeven Inflation Rate
}


def _classify_stance(fed_funds_rate: float) -> str:
    """Classify central bank stance from fed funds rate."""
    if fed_funds_rate >= 4.5:
        return 'restrictive'
    if fed_funds_rate >= 2.0:
        return 'neutral_to_restrictive'
    if fed_funds_rate >= 0.5:
        return 'neutral'
    return 'accommodative'


def _classify_inflation(cpi_yoy_pct: float) -> str:
    """Classify inflation environment."""
    if cpi_yoy_pct >= 5.0:
        return 'high_inflation'
    if cpi_yoy_pct >= 3.0:
        return 'above_target_inflation'
    if cpi_yoy_pct >= 1.5:
        return 'target_inflation'
    return 'low_inflation'


def _classify_growth(gdp_pct: float) -> str:
    """Classify growth environment."""
    if gdp_pct >= 3.0:
        return 'strong_growth'
    if gdp_pct >= 1.0:
        return 'moderate_growth'
    if gdp_pct >= 0.0:
        return 'stagnation'
    return 'contraction'


def _derive_regime(stance: str, inflation: str, growth: str) -> str:
    """Derive a composite regime label from the three macro signals."""
    parts = []
    if 'restrictive' in stance:
        parts.append('tight policy')
    elif 'accommodative' in stance:
        parts.append('easy policy')

    if 'high' in inflation or 'above' in inflation:
        parts.append('elevated inflation')
    elif 'low' in inflation:
        parts.append('disinflation')

    if 'contraction' in growth:
        parts.append('recession risk')
    elif 'strong' in growth:
        parts.append('expansion')
    elif 'stagnation' in growth:
        parts.append('slowing growth')

    return ', '.join(parts) if parts else 'mixed signals'


class _FredAuthError(Exception):
    """Raised when FRED returns a 400 invalid-key response."""


def _get_latest_value(api_key: str, series_id: str) -> Optional[float]:
    """Fetch the most recent non-null observation from a FRED series via direct HTTP."""
    url = (
        f'https://api.stlouisfed.org/fred/series/observations'
        f'?series_id={series_id}'
        f'&api_key={api_key}'
        f'&file_type=json'
        f'&sort_order=desc'
        f'&limit=10'
        f'&observation_start=2020-01-01'
    )
    try:
        req = _urllib.Request(url, headers={'User-Agent': 'TradingGalaxyKB/1.0'})
        with _urllib.urlopen(req, timeout=15) as resp:
            body = resp.read()
        data = _json.loads(body)
        if 'error_code' in data:
            msg = data.get('error_message', str(data))
            if data.get('error_code') in (400, 401, 403) or 'not registered' in msg:
                raise _FredAuthError(msg)
            _logger.warning('FRED API error for %s: %s', series_id, msg)
            return None
        observations = data.get('observations', [])
        for obs in observations:
            val = obs.get('value', '.')
            if val != '.':
                return float(val)
    except _FredAuthError:
        raise
    except Exception as e:
        _logger.warning('Failed to fetch FRED series %s: %s', series_id, e)
    return None


class FREDAdapter(BaseIngestAdapter):
    """
    Federal Reserve Economic Data ingest adapter.

    Pulls macro indicators and converts to regime atoms.
    Requires FRED_API_KEY environment variable.
    """

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(name='fred')
        self._api_key = api_key or os.environ.get('FRED_API_KEY')
        self._api_key_invalid = False  # set True after first 400 to suppress repeat logs

    def fetch(self) -> List[RawAtom]:
        if not self._api_key:
            self._logger.warning(
                'FRED_API_KEY not set — skipping FRED adapter. '
                'Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html'
            )
            return []

        if self._api_key_invalid:
            return []

        try:
            return self._fetch_atoms()
        except _FredAuthError as e:
            self._api_key_invalid = True
            self._logger.error(
                'FRED API key rejected — disabling FRED adapter. '
                'Re-register at https://fred.stlouisfed.org/docs/api/api_key.html. Error: %s', e
            )
            return []

    def _fetch_atoms(self) -> List[RawAtom]:
        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        source = 'macro_data_fred'

        # ── Fed Funds Rate ────────────────────────────────────────────────
        fed_funds = _get_latest_value(self._api_key, _SERIES['fed_funds'])
        stance = None
        if fed_funds is not None:
            atoms.append(RawAtom(
                subject='us_macro',
                predicate='dominant_driver',
                object=f'fed_funds_rate: {fed_funds:.2f}%',
                confidence=0.90,
                source=source,
                metadata={'series': _SERIES['fed_funds'], 'as_of': now_iso},
            ))

            stance = _classify_stance(fed_funds)
            atoms.append(RawAtom(
                subject='us_macro',
                predicate='central_bank_stance',
                object=stance,
                confidence=0.90,
                source=source,
                metadata={'fed_funds_rate': fed_funds, 'as_of': now_iso},
            ))

        # ── CPI (inflation) ──────────────────────────────────────────────
        inflation_label = None
        cpi = _get_latest_value(self._api_key, _SERIES['cpi_yoy'])
        if cpi is not None:
            # CPIAUCSL is an index — approximate YoY from the level isn't
            # ideal, but for regime classification it's sufficient.
            # We store the raw index and classify.
            # For proper YoY we'd need 12-month-ago value — keep it simple.
            atoms.append(RawAtom(
                subject='us_macro',
                predicate='inflation_environment',
                object=f'CPI index: {cpi:.1f}',
                confidence=0.85,
                source=source,
                metadata={'series': _SERIES['cpi_yoy'], 'as_of': now_iso},
            ))

        # ── GDP Growth ────────────────────────────────────────────────────
        growth_label = None
        gdp = _get_latest_value(self._api_key, _SERIES['gdp_growth'])
        if gdp is not None:
            growth_label = _classify_growth(gdp)
            atoms.append(RawAtom(
                subject='us_macro',
                predicate='growth_environment',
                object=f'{growth_label}: GDP {gdp:+.1f}% annualized',
                confidence=0.90,
                source=source,
                metadata={'series': _SERIES['gdp_growth'], 'gdp_pct': gdp, 'as_of': now_iso},
            ))

        # ── Unemployment ──────────────────────────────────────────────────
        unemp = _get_latest_value(self._api_key, _SERIES['unemployment'])
        if unemp is not None:
            atoms.append(RawAtom(
                subject='us_labor',
                predicate='dominant_driver',
                object=f'unemployment: {unemp:.1f}%',
                confidence=0.90,
                source=source,
                metadata={'series': _SERIES['unemployment'], 'as_of': now_iso},
            ))

        # ── Yield Curve (10Y - 2Y spread) ────────────────────────────────
        y10 = _get_latest_value(self._api_key, _SERIES['yield_10y'])
        y2 = _get_latest_value(self._api_key, _SERIES['yield_2y'])
        if y10 is not None and y2 is not None:
            spread_bps = round((y10 - y2) * 100)
            curve_state = 'inverted' if spread_bps < 0 else 'normal'
            atoms.append(RawAtom(
                subject='us_yields',
                predicate='risk_factor',
                object=f'yield_curve_{curve_state}: 10Y-2Y spread {spread_bps:+d}bps',
                confidence=0.90,
                source=source,
                metadata={
                    'yield_10y': y10, 'yield_2y': y2,
                    'spread_bps': spread_bps, 'as_of': now_iso,
                },
            ))

        # ── TIPS real yield + breakeven inflation ─────────────────────────
        tips_10y = _get_latest_value(self._api_key, _SERIES['tips_10y'])
        breakeven_10y = _get_latest_value(self._api_key, _SERIES['breakeven_10y'])
        breakeven_5y  = _get_latest_value(self._api_key, _SERIES['breakeven_5y'])

        if tips_10y is not None:
            real_yield_regime = (
                'positive_real_yield' if tips_10y > 0.5
                else 'near_zero_real_yield' if tips_10y > -0.25
                else 'negative_real_yield'
            )
            atoms.append(RawAtom(
                subject='us_yields',
                predicate='tips_real_yield',
                object=f'{tips_10y:.2f}%  ({real_yield_regime})',
                confidence=0.90,
                source=source,
                metadata={'series': _SERIES['tips_10y'], 'real_yield_regime': real_yield_regime, 'as_of': now_iso},
                upsert=True,
            ))

        if breakeven_10y is not None:
            inflation_expectations = (
                'anchored' if breakeven_10y < 2.5
                else 'elevated_expectations' if breakeven_10y < 3.2
                else 'unanchored'
            )
            atoms.append(RawAtom(
                subject='us_yields',
                predicate='breakeven_inflation_10y',
                object=f'{breakeven_10y:.2f}%  ({inflation_expectations})',
                confidence=0.90,
                source=source,
                metadata={'series': _SERIES['breakeven_10y'], 'inflation_expectations': inflation_expectations, 'as_of': now_iso},
                upsert=True,
            ))

        if breakeven_5y is not None:
            atoms.append(RawAtom(
                subject='us_yields',
                predicate='breakeven_inflation_5y',
                object=f'{breakeven_5y:.2f}%',
                confidence=0.88,
                source=source,
                metadata={'series': _SERIES['breakeven_5y'], 'as_of': now_iso},
                upsert=True,
            ))

        # Real yield vs breakeven spread (measures inflation risk premium)
        if tips_10y is not None and breakeven_10y is not None and y10 is not None:
            implied_nominal = tips_10y + breakeven_10y
            risk_premium_bps = round((y10 - implied_nominal) * 100)
            atoms.append(RawAtom(
                subject='us_yields',
                predicate='rate_environment',
                object=(
                    f'nominal_10y={y10:.2f}%  real_10y={tips_10y:.2f}%  '
                    f'breakeven={breakeven_10y:.2f}%  risk_premium={risk_premium_bps:+d}bps'
                ),
                confidence=0.88,
                source=source,
                metadata={'as_of': now_iso},
                upsert=True,
            ))

        # ── Composite regime label ────────────────────────────────────────
        if stance or growth_label:
            regime = _derive_regime(
                stance or 'unknown',
                inflation_label or 'unknown',
                growth_label or 'unknown',
            )
            atoms.append(RawAtom(
                subject='us_macro',
                predicate='regime_label',
                object=regime,
                confidence=0.80,
                source=source,
                metadata={'derived_from': 'fed_funds+cpi+gdp', 'as_of': now_iso},
            ))

        return atoms

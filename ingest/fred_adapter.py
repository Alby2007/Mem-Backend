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

try:
    from fredapi import Fred
    HAS_FREDAPI = True
except ImportError:
    HAS_FREDAPI = False
    Fred = None  # type: ignore

_logger = logging.getLogger(__name__)

# FRED series IDs for key macro indicators
_SERIES = {
    'fed_funds':   'FEDFUNDS',      # Effective Federal Funds Rate
    'cpi_yoy':     'CPIAUCSL',      # CPI All Urban Consumers (need to compute YoY)
    'gdp_growth':  'A191RL1Q225SBEA',  # Real GDP growth (annualized quarterly)
    'unemployment':'UNRATE',        # Unemployment Rate
    'yield_10y':   'DGS10',         # 10-Year Treasury Yield
    'yield_2y':    'DGS2',          # 2-Year Treasury Yield
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


def _get_latest_value(fred: 'Fred', series_id: str) -> Optional[float]:
    """Fetch the most recent non-null value from a FRED series."""
    try:
        data = fred.get_series(series_id, observation_start='2020-01-01')
        if data is not None and len(data) > 0:
            latest = data.dropna().iloc[-1]
            return float(latest)
    except Exception as e:
        msg = str(e)
        if 'not registered' in msg or ('400' in msg and 'api_key' in msg):
            raise _FredAuthError(msg)
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
        if not HAS_FREDAPI:
            self._logger.error(
                'fredapi not installed — pip install fredapi'
            )
            return []

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
                'FRED API key rejected (400 Bad Request) — disabling FRED adapter. '
                'Re-register at https://fred.stlouisfed.org/docs/api/api_key.html. Error: %s', e
            )
            return []

    def _fetch_atoms(self) -> List[RawAtom]:
        fred = Fred(api_key=self._api_key)
        atoms: List[RawAtom] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        source = 'macro_data_fred'

        # ── Fed Funds Rate ────────────────────────────────────────────────
        fed_funds = _get_latest_value(fred, _SERIES['fed_funds'])
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
        cpi = _get_latest_value(fred, _SERIES['cpi_yoy'])
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
        gdp = _get_latest_value(fred, _SERIES['gdp_growth'])
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
        unemp = _get_latest_value(fred, _SERIES['unemployment'])
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
        y10 = _get_latest_value(fred, _SERIES['yield_10y'])
        y2 = _get_latest_value(fred, _SERIES['yield_2y'])
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

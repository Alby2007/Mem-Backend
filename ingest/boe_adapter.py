"""
ingest/boe_adapter.py — Bank of England Macro Data Adapter (Trading KB)

Pulls UK macro indicators from the BoE Statistical Interactive Dataset API
and converts them to regime atoms. No API key required — the BoE publishes
all series publicly.

Atoms produced:
  - uk_macro | boe_base_rate        | {rate}%
  - uk_macro | central_bank_stance  | restrictive | neutral | accommodative
  - uk_macro | uk_cpi_yoy           | CPI YoY: {pct}%
  - uk_macro | inflation_environment| high_inflation | normalising | low_inflation
  - uk_macro | uk_gdp_growth        | GDP growth: {pct}%
  - uk_macro | growth_environment   | contraction | stagnation | moderate | strong
  - uk_macro | uk_unemployment      | {pct}%
  - uk_yields| uk_gilt_10y          | {yield}%
  - uk_yields| yield_environment    | low_rates | rising | elevated | peak
  - uk_macro | regime_label         | {derived composite label}

Source prefix: macro_data_boe  (authority 0.80, half-life 60d)
Interval: recommended 24h — BoE data updates daily/monthly

BoE API endpoint:
  https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp
  ?Travel=NIxSSxSUx&FromSeries=1&ToSeries=50&DAT=RNG
  &FD=1&FM=Jan&FY=2020&TD=31&TM=Dec&TY=2030
  &VFD=Y&html.x=66&html.y=26&C=<SERIES_CODE>&Filter=N

Rate limits: None enforced. We add a per-request sleep of 0.5s.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

# ── BoE series definitions ────────────────────────────────────────────────────
# Series code → (internal_key, description)
# All series are from the BoE Statistical Interactive Dataset.
# Documentation: https://www.bankofengland.co.uk/statistics/details/further-details-about-interest-rate-data
_BOE_SERIES: Dict[str, Tuple[str, str]] = {
    'IUDBEDR':  ('boe_base_rate',     'BoE Official Bank Rate (%)'),
    'IUMBV34':  ('uk_m4_money_supply','UK M4 Money Supply YoY (%)'),
    'IUQABEDR': ('uk_gilt_10y',       'UK 10-Year Gilt Yield (%)'),
    'IUAABEDR': ('uk_gilt_2y',        'UK 2-Year Gilt Yield (%)'),
}

# CPI and GDP from ONS via BoE database
_ONS_SERIES: Dict[str, Tuple[str, str]] = {
    'LMAKAA':   ('uk_cpi_yoy',        'UK CPI All Items YoY (%)'),
    'IHYP':     ('uk_gdp_growth',     'UK Real GDP Growth QoQ (%)'),
    'MGSX':     ('uk_unemployment',   'UK ILO Unemployment Rate (%)'),
}

_ALL_SERIES = {**_BOE_SERIES, **_ONS_SERIES}

# BoE statistical database API base
_BOE_API_BASE = (
    'https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp'
    '?Travel=NIxSSxSUx&FromSeries=1&ToSeries=50&DAT=RNG'
    '&FD=1&FM=Jan&FY=2020&TD=31&TM=Dec&TY=2030'
    '&VFD=Y&html.x=66&html.y=26&C={series}&Filter=N'
)

# Simpler JSON endpoint (returns CSV-like but parseable)
_BOE_JSON_API = (
    'https://www.bankofengland.co.uk/boeapps/database/_iadb-FromShowColumns.asp'
    '?csv.x=yes&Datefrom=01/Jan/2023&Dateto=now&SeriesCodes={series}&CSVF=TT&UsingCodes=Y'
)

_REQUEST_DELAY = 0.5   # seconds between requests
_TIMEOUT       = 15    # seconds per request
_SOURCE        = 'macro_data_boe'

# ── Stance/regime classifiers ─────────────────────────────────────────────────

def _classify_boe_stance(rate: float) -> str:
    """Classify BoE stance from the base rate."""
    if rate >= 4.5:
        return 'restrictive'
    if rate >= 2.5:
        return 'neutral'
    return 'accommodative'


def _classify_inflation(cpi_yoy: float) -> str:
    if cpi_yoy >= 4.0:
        return 'high_inflation'
    if cpi_yoy >= 2.5:
        return 'normalising'
    return 'low_inflation'


def _classify_growth(gdp_qoq: float) -> str:
    if gdp_qoq < 0:
        return 'contraction'
    if gdp_qoq < 0.3:
        return 'stagnation'
    if gdp_qoq < 0.7:
        return 'moderate'
    return 'strong'


def _classify_yield_env(gilt_10y: float) -> str:
    if gilt_10y < 2.0:
        return 'low_rates'
    if gilt_10y < 3.5:
        return 'rising'
    if gilt_10y < 5.0:
        return 'elevated'
    return 'peak'


def _derive_regime(
    base_rate: Optional[float],
    cpi_yoy: Optional[float],
    gdp_qoq: Optional[float],
) -> str:
    """Derive a composite UK macro regime label."""
    if base_rate is None and cpi_yoy is None:
        return 'data_unavailable'

    stance = _classify_boe_stance(base_rate) if base_rate is not None else 'unknown'
    inflation = _classify_inflation(cpi_yoy) if cpi_yoy is not None else 'unknown'
    growth = _classify_growth(gdp_qoq) if gdp_qoq is not None else 'unknown'

    if stance == 'restrictive' and inflation == 'high_inflation':
        return 'uk_restrictive_high_inflation'
    if stance == 'restrictive' and inflation == 'normalising':
        return 'uk_restrictive_normalising'
    if stance == 'neutral' and growth in ('moderate', 'strong'):
        return 'uk_neutral_growth'
    if stance == 'neutral' and growth in ('stagnation', 'contraction'):
        return 'uk_neutral_stagnation'
    if stance == 'accommodative':
        return 'uk_accommodative'
    return f'uk_{stance}_{inflation}'


# ── Data fetcher ──────────────────────────────────────────────────────────────

def _fetch_series(series_code: str) -> Optional[float]:
    """
    Fetch the most recent value for a BoE/ONS series.
    Returns the latest numeric value or None on failure.
    """
    url = _BOE_JSON_API.format(series=series_code)
    try:
        resp = requests.get(url, timeout=_TIMEOUT, headers={'User-Agent': 'TradingGalaxyKB/1.0'})
        resp.raise_for_status()
        text = resp.text.strip()

        # Parse CSV response — last non-empty data row has the latest value
        # Format: Date,SeriesCode\nDD Mon YYYY,value\n...
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        data_lines = []
        for line in lines:
            parts = line.split(',')
            if len(parts) >= 2:
                try:
                    float(parts[-1])
                    data_lines.append(parts)
                except ValueError:
                    continue

        if not data_lines:
            _logger.debug('No numeric data for series %s', series_code)
            return None

        latest = data_lines[-1]
        return float(latest[-1])

    except Exception as e:
        _logger.warning('Failed to fetch BoE series %s: %s', series_code, e)
        return None


# ── Adapter ───────────────────────────────────────────────────────────────────

class BoEAdapter(BaseIngestAdapter):
    """
    Bank of England macro data ingest adapter.

    Pulls UK macro indicators from the BoE/ONS statistical database and
    produces regime atoms for the Trading KB. No API key required.

    Complements the FREDAdapter — FRED covers US macro, BoE covers UK macro.
    Together they give a dual-market picture for GBP/USD, gilt/treasury
    spreads, and UK vs US growth divergence.
    """

    def __init__(self):
        super().__init__(name='boe')

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []

        # ── Fetch all series ─────────────────────────────────────────────────
        values: Dict[str, Optional[float]] = {}
        for series_code, (key, _desc) in _ALL_SERIES.items():
            val = _fetch_series(series_code)
            values[key] = val
            if val is not None:
                _logger.info('BoE %s (%s) = %.4f', series_code, key, val)
            time.sleep(_REQUEST_DELAY)

        meta_base = {'fetched_at': now_iso, 'source_url': 'bankofengland.co.uk'}

        # ── BoE Base Rate ────────────────────────────────────────────────────
        base_rate = values.get('boe_base_rate')
        if base_rate is not None:
            atoms.append(RawAtom(
                subject='uk_macro', predicate='boe_base_rate',
                object=f'{base_rate:.2f}%',
                confidence=0.95, source=_SOURCE,
                metadata={**meta_base, 'series': 'IUDBEDR'},
                upsert=True,
            ))
            stance = _classify_boe_stance(base_rate)
            atoms.append(RawAtom(
                subject='uk_macro', predicate='central_bank_stance',
                object=stance,
                confidence=0.90, source=_SOURCE,
                metadata={**meta_base, 'boe_base_rate': base_rate},
                upsert=True,
            ))

        # ── UK CPI ──────────────────────────────────────────────────────────
        cpi = values.get('uk_cpi_yoy')
        if cpi is not None:
            atoms.append(RawAtom(
                subject='uk_macro', predicate='uk_cpi_yoy',
                object=f'CPI YoY: {cpi:.1f}%',
                confidence=0.90, source=_SOURCE,
                metadata={**meta_base, 'series': 'LMAKAA'},
                upsert=True,
            ))
            atoms.append(RawAtom(
                subject='uk_macro', predicate='inflation_environment',
                object=_classify_inflation(cpi),
                confidence=0.85, source=_SOURCE,
                metadata={**meta_base, 'cpi_yoy': cpi},
                upsert=True,
            ))

        # ── UK GDP ──────────────────────────────────────────────────────────
        gdp = values.get('uk_gdp_growth')
        if gdp is not None:
            atoms.append(RawAtom(
                subject='uk_macro', predicate='uk_gdp_growth',
                object=f'GDP growth: {gdp:.2f}%',
                confidence=0.85, source=_SOURCE,
                metadata={**meta_base, 'series': 'IHYP'},
                upsert=True,
            ))
            atoms.append(RawAtom(
                subject='uk_macro', predicate='growth_environment',
                object=_classify_growth(gdp),
                confidence=0.80, source=_SOURCE,
                metadata={**meta_base, 'gdp_qoq': gdp},
                upsert=True,
            ))

        # ── UK Unemployment ──────────────────────────────────────────────────
        unemp = values.get('uk_unemployment')
        if unemp is not None:
            atoms.append(RawAtom(
                subject='uk_labor', predicate='uk_unemployment_rate',
                object=f'{unemp:.1f}%',
                confidence=0.90, source=_SOURCE,
                metadata={**meta_base, 'series': 'MGSX'},
                upsert=True,
            ))

        # ── UK Gilt 10Y ──────────────────────────────────────────────────────
        gilt_10y = values.get('uk_gilt_10y')
        if gilt_10y is not None:
            atoms.append(RawAtom(
                subject='uk_yields', predicate='uk_gilt_10y',
                object=f'{gilt_10y:.2f}%',
                confidence=0.95, source=_SOURCE,
                metadata={**meta_base, 'series': 'IUQABEDR'},
                upsert=True,
            ))
            atoms.append(RawAtom(
                subject='uk_yields', predicate='yield_environment',
                object=_classify_yield_env(gilt_10y),
                confidence=0.85, source=_SOURCE,
                metadata={**meta_base, 'gilt_10y': gilt_10y},
                upsert=True,
            ))

        # ── UK Gilt 2Y / yield curve spread ─────────────────────────────────
        gilt_2y = values.get('uk_gilt_2y')
        if gilt_10y is not None and gilt_2y is not None:
            spread_bps = round((gilt_10y - gilt_2y) * 100, 1)
            inverted = spread_bps < 0
            atoms.append(RawAtom(
                subject='uk_yields', predicate='uk_yield_curve_spread',
                object=f'10Y-2Y: {spread_bps}bps',
                confidence=0.90, source=_SOURCE,
                metadata={**meta_base, 'gilt_10y': gilt_10y, 'gilt_2y': gilt_2y},
                upsert=True,
            ))
            if inverted:
                atoms.append(RawAtom(
                    subject='uk_yields', predicate='risk_factor',
                    object='uk_yield_curve_inverted',
                    confidence=0.85, source=_SOURCE,
                    metadata={**meta_base, 'spread_bps': spread_bps},
                    upsert=True,
                ))

        # ── Composite UK regime label ─────────────────────────────────────────
        regime = _derive_regime(base_rate, cpi, gdp)
        atoms.append(RawAtom(
            subject='uk_macro', predicate='regime_label',
            object=regime,
            confidence=0.75, source=_SOURCE,
            metadata={
                **meta_base,
                'boe_base_rate': base_rate,
                'cpi_yoy': cpi,
                'gdp_qoq': gdp,
            },
            upsert=True,
        ))

        _logger.info('BoE adapter: produced %d atoms', len(atoms))
        return atoms

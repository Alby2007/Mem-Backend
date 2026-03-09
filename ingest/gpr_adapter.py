"""
ingest/gpr_adapter.py — Caldara-Iacoviello Geopolitical Risk (GPR) Index Adapter

Fetches the GPR Index from FRED (St. Louis Fed API), which mirrors the
Caldara-Iacoviello series. The GPR index is derived from newspaper article
counts across 10 major publications and is the institutional standard for
geopolitical risk measurement.

Paper: Caldara & Iacoviello (2022), "Measuring Geopolitical Risk", AER.
FRED series: GPRH (headline), GPRT (threats), GPRA (acts)

Requires: FRED_API_KEY env var (same key used by the existing FREDAdapter).
Skips gracefully if no key. Monthly update cadence — idempotent.

Atoms produced:
  - geopolitical_risk | gpr_index      | {float}         — headline GPR score
  - geopolitical_risk | gpr_threats    | {float}         — threat sub-index (forward-looking)
  - geopolitical_risk | gpr_acts       | {float}         — acts sub-index (realised conflict)
  - geopolitical_risk | gpr_trend      | rising | stable | falling
  - geopolitical_risk | gpr_level      | elevated | moderate | low

Source: geopolitical_data_gpr  (authority 0.80, half-life 30d — monthly index)
Schedule: 86400s (daily check, atoms only written when new month available)
"""

from __future__ import annotations

import json as _json
import logging
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_FRED_BASE = 'https://api.stlouisfed.org/fred/series/observations'

# FRED series IDs for GPR
_SERIES_GPR  = 'GPRH'   # Headline GPR index
_SERIES_GPRT = 'GPRT'   # Threats sub-index (forward-looking)
_SERIES_GPRA = 'GPRA'   # Acts sub-index (realised conflict)

# Risk level thresholds (GPR index historically averages ~100; 200+ = major shock)
_ELEVATED_THRESHOLD = 200
_MODERATE_THRESHOLD = 130

# Trend threshold (month-over-month change)
_TREND_THRESHOLD = 15.0


def _level_label(score: float) -> str:
    if score >= _ELEVATED_THRESHOLD:
        return 'elevated'
    if score >= _MODERATE_THRESHOLD:
        return 'moderate'
    return 'low'


def _trend_label(current: float, previous: Optional[float]) -> str:
    if previous is None:
        return 'stable'
    delta = current - previous
    if delta > _TREND_THRESHOLD:
        return 'rising'
    if delta < -_TREND_THRESHOLD:
        return 'falling'
    return 'stable'


def _fetch_fred_series_latest(series_id: str, api_key: str) -> Optional[Tuple[str, float]]:
    """
    Fetch the most recent observation for a FRED series.
    Returns (date_str, value) or None.
    """
    params = urllib.parse.urlencode({
        'series_id':  series_id,
        'api_key':    api_key,
        'sort_order': 'desc',
        'limit':      3,
        'file_type':  'json',
    })
    url = f'{_FRED_BASE}?{params}'
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'TradingGalaxyKB/1.0', 'Accept': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = _json.loads(resp.read().decode('utf-8', errors='replace'))
        for obs in data.get('observations', []):
            val_str = obs.get('value', '.')
            if val_str == '.' or not val_str:
                continue
            try:
                return (obs['date'], round(float(val_str), 2))
            except (ValueError, KeyError):
                continue
        return None
    except Exception as exc:
        _logger.warning('GPR: FRED fetch failed for %s: %s', series_id, exc)
        return None


class GPRAdapter(BaseIngestAdapter):
    """
    Caldara-Iacoviello Geopolitical Risk Index adapter.

    Fetches GPR series from FRED (GPRH, GPRT, GPRA) and emits headline,
    threat, and acts sub-index atoms. Requires FRED_API_KEY env var.
    Idempotent — skips write if last ingested period is unchanged.
    """

    def __init__(self, api_key: Optional[str] = None):
        super().__init__(name='gpr_index')
        self._api_key     = api_key or os.environ.get('FRED_API_KEY', '')
        self._last_period: Optional[str] = None
        self._last_gpr: Optional[float]  = None

    def fetch(self) -> List[RawAtom]:
        if not self._api_key:
            self._logger.warning(
                'GPR: FRED_API_KEY not set — skipping. '
                'Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html'
            )
            return []

        now_iso = datetime.now(timezone.utc).isoformat()
        source  = 'geopolitical_data_gpr'
        meta_base = {
            'fetched_at': now_iso,
            'source_url': _FRED_BASE,
            'paper':      'Caldara & Iacoviello (2022), AER',
            'authority':  0.80,
        }

        gpr_obs  = _fetch_fred_series_latest(_SERIES_GPR,  self._api_key)
        gprt_obs = _fetch_fred_series_latest(_SERIES_GPRT, self._api_key)
        gpra_obs = _fetch_fred_series_latest(_SERIES_GPRA, self._api_key)

        if not gpr_obs:
            self._logger.warning('GPR: failed to fetch GPRH from FRED')
            return []

        period, gpr   = gpr_obs
        gprt = gprt_obs[1] if gprt_obs else gpr
        gpra = gpra_obs[1] if gpra_obs else gpr

        # Idempotency — skip if we already wrote this month's data
        if period == self._last_period:
            self._logger.info('GPR: no new data (period=%s, gpr=%.1f) — skipping', period, gpr)
            return []

        trend = _trend_label(gpr, self._last_gpr)
        level = _level_label(gpr)
        self._last_period = period
        self._last_gpr    = gpr

        atoms: List[RawAtom] = [
            RawAtom(
                subject='geopolitical_risk',
                predicate='gpr_index',
                object=str(gpr),
                confidence=0.80,
                source=source,
                metadata={**meta_base, 'period': period, 'description': 'Headline GPR index (avg ~100, >200=major shock)'},
                upsert=True,
            ),
            RawAtom(
                subject='geopolitical_risk',
                predicate='gpr_threats',
                object=str(gprt),
                confidence=0.80,
                source=source,
                metadata={**meta_base, 'period': period, 'description': 'GPR threat sub-index — forward-looking language'},
                upsert=True,
            ),
            RawAtom(
                subject='geopolitical_risk',
                predicate='gpr_acts',
                object=str(gpra),
                confidence=0.80,
                source=source,
                metadata={**meta_base, 'period': period, 'description': 'GPR acts sub-index — realised conflict events'},
                upsert=True,
            ),
            RawAtom(
                subject='geopolitical_risk',
                predicate='gpr_trend',
                object=trend,
                confidence=0.75,
                source=source,
                metadata={**meta_base, 'period': period, 'prev_gpr': self._last_gpr},
                upsert=True,
            ),
            RawAtom(
                subject='geopolitical_risk',
                predicate='gpr_level',
                object=level,
                confidence=0.80,
                source=source,
                metadata={**meta_base, 'period': period, 'thresholds': f'elevated>={_ELEVATED_THRESHOLD}, moderate>={_MODERATE_THRESHOLD}'},
                upsert=True,
            ),
        ]

        self._logger.info('GPR adapter: period=%s gpr=%.1f gprt=%.1f gpra=%.1f trend=%s level=%s',
                          period, gpr, gprt, gpra, trend, level)
        return atoms

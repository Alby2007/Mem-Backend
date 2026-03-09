"""
ingest/gpr_adapter.py — Caldara-Iacoviello Geopolitical Risk (GPR) Index Adapter

Downloads the Federal Reserve GPR Index Excel/CSV published by Matteo Iacoviello.
The GPR index is derived from newspaper article counts across 10 major publications
and is the institutional standard for geopolitical risk measurement.

Paper: Caldara & Iacoviello (2022), "Measuring Geopolitical Risk", AER.
Data:  https://www.matteoiacoviello.com/gpr.htm

No API key required. Monthly update cadence (run daily — skips write if month unchanged).

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

import io
import logging
import time
import urllib.request
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

# Primary URL — Excel format published by the Fed researcher
_GPR_XLS_URL = 'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls'
# Fallback: direct CSV from FRED-linked mirror
_GPR_CSV_URL = 'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.csv'

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


def _fetch_gpr_csv() -> Optional[str]:
    """Attempt to download the GPR CSV. Falls back to CSV URL if XLS fails."""
    for url in (_GPR_XLS_URL, _GPR_CSV_URL):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    'User-Agent': 'TradingGalaxyKB/1.0 (research; admin@tradinggalaxy.dev)',
                    'Accept': '*/*',
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
            # Try to decode as text — works for CSV, also works for old XLS text export
            try:
                return raw.decode('utf-8', errors='replace')
            except Exception:
                return raw.decode('latin-1', errors='replace')
        except Exception as exc:
            _logger.debug('GPR fetch from %s failed: %s', url, exc)
            time.sleep(2)
    return None


def _parse_gpr_latest(text: str) -> Optional[Tuple[str, float, float, float]]:
    """
    Parse the GPR data file and return the most recent row.

    Expected CSV columns (order may vary):
      Year, Month, GPRC_GBR, ..., GPR, GPRT, GPRA, ...

    Returns (period_label, gpr, gpr_threats, gpr_acts) or None.
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return None

    # Find header line
    header_idx = None
    headers: List[str] = []
    for i, line in enumerate(lines):
        parts = [p.strip().strip('"').upper() for p in line.split(',')]
        if 'GPR' in parts or 'GPRT' in parts or 'GPRA' in parts:
            headers = parts
            header_idx = i
            break

    if header_idx is None or not headers:
        _logger.warning('GPR: could not find header row')
        return None

    # Column indices — try multiple naming conventions
    def _col(*names: str) -> Optional[int]:
        for n in names:
            if n in headers:
                return headers.index(n)
        return None

    col_year   = _col('YEAR', 'DATE_YEAR')
    col_month  = _col('MONTH', 'DATE_MONTH')
    col_gpr    = _col('GPR', 'GPRH')
    col_gprt   = _col('GPRT', 'GPR_THREATS', 'GPRTH')
    col_gpra   = _col('GPRA', 'GPR_ACTS', 'GPRAH')

    if col_year is None or col_month is None or col_gpr is None:
        _logger.warning('GPR: missing required columns — found: %s', headers[:10])
        return None

    # Scan data rows from the bottom to find the most recent non-empty row
    data_lines = lines[header_idx + 1:]
    for line in reversed(data_lines):
        parts = [p.strip().strip('"') for p in line.split(',')]
        if len(parts) <= max(filter(None, [col_gpr, col_gprt, col_gpra])):
            continue
        try:
            year  = int(float(parts[col_year]))
            month = int(float(parts[col_month]))
            gpr   = float(parts[col_gpr])
            if gpr <= 0:
                continue
            gprt = float(parts[col_gprt]) if col_gprt is not None and parts[col_gprt] else gpr
            gpra = float(parts[col_gpra]) if col_gpra is not None and parts[col_gpra] else gpr
            period = f'{year}-{month:02d}'
            return (period, round(gpr, 2), round(gprt, 2), round(gpra, 2))
        except (ValueError, IndexError):
            continue

    return None


class GPRAdapter(BaseIngestAdapter):
    """
    Caldara-Iacoviello Geopolitical Risk Index adapter.

    Downloads the Fed-published GPR Excel/CSV monthly and emits headline,
    threat, and acts sub-index atoms. Idempotent — skips write if last
    ingested period equals current month.
    """

    def __init__(self):
        super().__init__(name='gpr_index')
        self._last_period: Optional[str] = None
        self._last_gpr: Optional[float]  = None

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        source  = 'geopolitical_data_gpr'
        meta_base = {
            'fetched_at':  now_iso,
            'source_url':  _GPR_XLS_URL,
            'paper':       'Caldara & Iacoviello (2022), AER',
            'authority':   0.80,
        }

        text = _fetch_gpr_csv()
        if not text:
            self._logger.warning('GPR: failed to download data file')
            return []

        parsed = _parse_gpr_latest(text)
        if not parsed:
            self._logger.warning('GPR: failed to parse data file')
            return []

        period, gpr, gprt, gpra = parsed

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

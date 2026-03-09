"""
ingest/gpr_adapter.py — Caldara-Iacoviello Geopolitical Risk (GPR) Index Adapter

Downloads the GPR Index XLS published by Matteo Iacoviello (Federal Reserve).
The GPR index is derived from newspaper article counts across 10 major
publications and is the institutional standard for geopolitical risk.

Paper: Caldara & Iacoviello (2022), "Measuring Geopolitical Risk", AER.
Data:  https://www.matteoiacoviello.com/gpr.htm (BIFF8 .xls, parsed via xlrd)

No API key required. Monthly update cadence — idempotent.

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
import urllib.request
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_GPR_XLS_URL = 'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls'

# Risk level thresholds (GPR index historically averages ~100; 200+ = major shock)
_ELEVATED_THRESHOLD = 200
_MODERATE_THRESHOLD = 130
_TREND_THRESHOLD    = 15.0


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


def _fetch_xls_bytes() -> Optional[bytes]:
    """Download the GPR XLS file as raw bytes."""
    try:
        req = urllib.request.Request(
            _GPR_XLS_URL,
            headers={
                'User-Agent': 'TradingGalaxyKB/1.0 (research; admin@tradinggalaxy.dev)',
                'Accept': '*/*',
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except Exception as exc:
        _logger.warning('GPR: XLS download failed: %s', exc)
        return None


def _excel_serial_to_period(serial: float) -> Optional[str]:
    """
    Convert an Excel date serial number to a YYYY-MM period string.
    Excel epoch: Jan 1 1900 = serial 1 (with the erroneous leap year 1900 bug).
    """
    try:
        import xlrd
        tup = xlrd.xldate_as_tuple(serial, 0)  # datemode=0 = 1900-based
        return f'{tup[0]}-{tup[1]:02d}'
    except Exception:
        return None


def _parse_xls(raw: bytes) -> Optional[Tuple[str, float, float, float]]:
    """
    Parse the GPR XLS (BIFF8 format) using xlrd.
    Returns (period, gpr, gprt, gpra) for the most recent non-empty row, or None.

    Actual structure (from inspection):
      Row 0: header — quoted strings like 'month', 'GPR', 'GPRT', 'GPRA', 'GPRH', ...
      Col 0: Excel date serial (monthly, e.g. 46054.0 = 2026-01)
      Col 1: GPR  (headline, global)
      Col 2: GPRT (threats sub-index)
      Col 3: GPRA (acts sub-index)
      Col 4: GPRH (historical headline — alternative series)
    """
    try:
        import xlrd
    except ImportError:
        _logger.error('GPR: xlrd not installed — run: pip install xlrd')
        return None

    try:
        wb = xlrd.open_workbook(file_contents=raw)
    except Exception as exc:
        _logger.warning('GPR: xlrd failed to open workbook: %s', exc)
        return None

    ws = wb.sheet_by_index(0)
    if ws.nrows < 3:
        _logger.warning('GPR: sheet has too few rows (%d)', ws.nrows)
        return None

    # Parse header row (row 0) — strip quotes from cell values
    headers = [str(ws.cell_value(0, c)).strip().strip("'").upper() for c in range(ws.ncols)]
    _logger.debug('GPR headers: %s', headers[:10])

    def _col(*names: str) -> int:
        for n in names:
            if n in headers:
                return headers.index(n)
        return -1

    col_date = _col('MONTH', 'DATE', 'YEAR')
    col_gpr  = _col('GPR')
    col_gprt = _col('GPRT')
    col_gpra = _col('GPRA')

    if col_date < 0 or col_gpr < 0:
        _logger.warning('GPR: required columns not found — headers=%s', headers[:10])
        return None

    # Scan from bottom to find most recent row with a non-empty GPR value
    for r in range(ws.nrows - 1, 0, -1):
        try:
            date_serial = ws.cell_value(r, col_date)
            gpr_val     = ws.cell_value(r, col_gpr)

            if not date_serial or not gpr_val:
                continue

            gpr = round(float(gpr_val), 2)
            if gpr <= 0:
                continue

            period = _excel_serial_to_period(float(date_serial))
            if not period:
                continue

            gprt_val = ws.cell_value(r, col_gprt) if col_gprt >= 0 else None
            gpra_val = ws.cell_value(r, col_gpra) if col_gpra >= 0 else None

            gprt = round(float(gprt_val), 2) if gprt_val else gpr
            gpra = round(float(gpra_val), 2) if gpra_val else gpr

            _logger.info('GPR: parsed period=%s gpr=%.2f gprt=%.2f gpra=%.2f (row %d)',
                         period, gpr, gprt, gpra, r)
            return (period, gpr, gprt, gpra)
        except (ValueError, TypeError, IndexError):
            continue

    _logger.warning('GPR: no valid data rows found in XLS (checked %d rows)', ws.nrows)
    return None


class GPRAdapter(BaseIngestAdapter):
    """
    Caldara-Iacoviello Geopolitical Risk Index adapter.

    Downloads BIFF8 XLS from Iacoviello's site via xlrd and emits headline,
    threat, and acts sub-index atoms. No API key required.
    Idempotent — skips write if last ingested period is unchanged.
    """

    def __init__(self):
        super().__init__(name='gpr_index')
        self._last_period: Optional[str] = None
        self._last_gpr: Optional[float]  = None

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        source  = 'geopolitical_data_gpr'
        meta_base = {
            'fetched_at': now_iso,
            'source_url': _GPR_XLS_URL,
            'paper':      'Caldara & Iacoviello (2022), AER',
            'authority':  0.80,
        }

        raw = _fetch_xls_bytes()
        if not raw:
            self._logger.warning('GPR: failed to download XLS')
            return []

        parsed = _parse_xls(raw)
        if not parsed:
            self._logger.warning('GPR: failed to parse XLS')
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

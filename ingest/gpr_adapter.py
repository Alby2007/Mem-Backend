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


def _parse_xls(raw: bytes) -> Optional[Tuple[str, float, float, float]]:
    """
    Parse the GPR XLS (BIFF8 format) using xlrd.
    Returns (period, gpr, gprt, gpra) for the most recent non-empty row, or None.

    Expected columns (order varies by sheet version):
      year, month, ..., GPR (headline), GPRT (threats), GPRA (acts), ...
    """
    try:
        import xlrd  # installed via pip on OCI; stdlib fallback not feasible for BIFF8
    except ImportError:
        _logger.error(
            'GPR: xlrd not installed — run: pip install xlrd  '
            '(required to parse BIFF8 .xls files)'
        )
        return None

    try:
        wb = xlrd.open_workbook(file_contents=raw)
    except Exception as exc:
        _logger.warning('GPR: xlrd failed to open workbook: %s', exc)
        return None

    # Try each sheet — the main data is usually on sheet 0
    for sheet_idx in range(wb.nsheets):
        ws = wb.sheet_by_index(sheet_idx)
        if ws.nrows < 3:
            continue

        # Find header row — scan first 5 rows
        headers: List[str] = []
        header_row = -1
        for r in range(min(5, ws.nrows)):
            row_vals = [str(ws.cell_value(r, c)).strip().upper() for c in range(ws.ncols)]
            # Look for Year/Month columns + GPR
            if ('YEAR' in row_vals or 'DATE' in row_vals) and 'GPR' in row_vals:
                headers    = row_vals
                header_row = r
                break

        if header_row < 0 or not headers:
            continue

        def _col(*names: str) -> int:
            for n in names:
                if n in headers:
                    return headers.index(n)
            return -1

        col_year  = _col('YEAR', 'DATE_YEAR')
        col_month = _col('MONTH', 'DATE_MONTH')
        col_gpr   = _col('GPR', 'GPRH')
        col_gprt  = _col('GPRT', 'GPRTHREAT', 'GPR_THREATS')
        col_gpra  = _col('GPRA', 'GPRACT', 'GPR_ACTS')

        if col_year < 0 or col_month < 0 or col_gpr < 0:
            _logger.debug('GPR sheet %d: missing key columns (headers=%s)', sheet_idx, headers[:10])
            continue

        # Scan rows from bottom to find most recent non-empty GPR value
        for r in range(ws.nrows - 1, header_row, -1):
            try:
                year_val  = ws.cell_value(r, col_year)
                month_val = ws.cell_value(r, col_month)
                gpr_val   = ws.cell_value(r, col_gpr)

                if not year_val or not month_val or not gpr_val:
                    continue

                year  = int(float(year_val))
                month = int(float(month_val))
                gpr   = round(float(gpr_val), 2)

                if gpr <= 0 or year < 1985:
                    continue

                gprt = round(float(ws.cell_value(r, col_gprt)), 2) if col_gprt >= 0 else gpr
                gpra = round(float(ws.cell_value(r, col_gpra)), 2) if col_gpra >= 0 else gpr

                period = f'{year}-{month:02d}'
                return (period, gpr, gprt, gpra)
            except (ValueError, TypeError, IndexError):
                continue

    _logger.warning('GPR: no valid data rows found in XLS')
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

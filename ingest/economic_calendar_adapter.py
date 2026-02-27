"""
ingest/economic_calendar_adapter.py — Economic Calendar Event Risk Adapter

Tracks upcoming high-impact macro events (FOMC, CPI, NFP, GDP) and emits
event-risk atoms that feed into signal enrichment position-sizing logic.

SOURCES
=======
  1. FOMC meeting dates — hardcoded 2026 schedule (published annually by Fed)
  2. FRED release calendar API — CPI, NFP, GDP release dates
     Uses existing FRED_API_KEY env var (same as fred_adapter.py)
  3. Fallback: hardcoded monthly CPI/NFP schedule when FRED unavailable

ATOMS PRODUCED
==============
  market | macro_event_risk   | high | medium | low
  market | next_macro_event   | "FOMC 2026-03-19"
  market | days_to_macro_event| "20"
  market | macro_event_type   | fomc | cpi | nfp | gdp | earnings_season

SOURCE PREFIX
=============
  macro_data_calendar  (authority 0.80, half-life 7d)

INTERVAL
========
  86400s (24h) — event dates are known days/weeks in advance;
  daily refresh updates the countdown and risk level.

EVENT RISK CLASSIFICATION
=========================
  high   — ≤ 3 calendar days to FOMC, CPI, or NFP
  medium — ≤ 7 calendar days
  low    — > 7 calendar days
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_SOURCE = 'macro_data_calendar'

# Risk thresholds (calendar days)
_HIGH_THRESHOLD   = 3
_MEDIUM_THRESHOLD = 7

# ── 2026 FOMC meeting dates (decision day = Wednesday of meeting) ────────────
# Source: Federal Reserve press release (published Dec 2024)
_FOMC_2026 = [
    date(2026, 1, 29),
    date(2026, 3, 19),
    date(2026, 5,  7),
    date(2026, 6, 18),
    date(2026, 7, 30),
    date(2026, 9, 17),
    date(2026, 10, 29),
    date(2026, 12, 10),
]

# ── FRED release series IDs for economic events ──────────────────────────────
# Used to fetch scheduled release dates via FRED API
_FRED_RELEASE_SERIES = {
    'cpi':  'CPIAUCSL',          # Consumer Price Index
    'nfp':  'PAYEMS',            # Nonfarm Payrolls
    'gdp':  'A191RL1Q225SBEA',   # Real GDP Growth
}

# Fallback monthly schedule (day-of-month) when FRED API unavailable
# CPI typically released ~12th, NFP first Friday of month
_FALLBACK_CPI_DAY = 12   # approximate mid-month
_FALLBACK_NFP_DAY =  7   # approximate first week


def _days_until(target: date) -> int:
    """Calendar days from today to target date (negative if in the past)."""
    today = datetime.now(timezone.utc).date()
    return (target - today).days


def _next_fomc() -> Optional[Tuple[date, int]]:
    """Return (next_fomc_date, days_until) or None if none upcoming this year."""
    today = datetime.now(timezone.utc).date()
    for d in sorted(_FOMC_2026):
        if d >= today:
            return d, _days_until(d)
    return None


def _fetch_fred_release_dates(series_id: str) -> List[date]:
    """
    Fetch upcoming release dates for a FRED series using the release calendar API.
    Returns list of upcoming dates sorted ascending.
    """
    api_key = os.environ.get('FRED_API_KEY', '')
    if not api_key:
        return []
    try:
        import requests
        # Get series release info first
        resp = requests.get(
            'https://api.stlouisfed.org/fred/series',
            params={'series_id': series_id, 'api_key': api_key, 'file_type': 'json'},
            timeout=10,
        )
        resp.raise_for_status()
        release_id = resp.json()['seriess'][0].get('release_id')
        if not release_id:
            return []

        # Fetch upcoming release dates
        today = datetime.now(timezone.utc).date()
        resp2 = requests.get(
            'https://api.stlouisfed.org/fred/release/dates',
            params={
                'release_id': release_id,
                'api_key': api_key,
                'file_type': 'json',
                'realtime_start': today.isoformat(),
                'limit': 5,
                'sort_order': 'asc',
                'include_release_dates_with_no_data': 'true',
            },
            timeout=10,
        )
        resp2.raise_for_status()
        dates = []
        for entry in resp2.json().get('release_dates', []):
            try:
                d = date.fromisoformat(entry['date'])
                if d >= today:
                    dates.append(d)
            except (KeyError, ValueError):
                continue
        return sorted(dates)
    except Exception as exc:
        _logger.debug('EconomicCalendarAdapter: FRED release dates fetch failed for %s: %s', series_id, exc)
        return []


def _fallback_event_dates() -> List[Tuple[str, date]]:
    """
    Estimate CPI and NFP dates for current and next month when FRED unavailable.
    Returns list of (event_type, date).
    """
    today = datetime.now(timezone.utc).date()
    events = []
    for month_offset in range(0, 3):
        month = today.month + month_offset
        year  = today.year + (month - 1) // 12
        month = ((month - 1) % 12) + 1
        # CPI mid-month
        cpi_date = date(year, month, min(_FALLBACK_CPI_DAY, 28))
        if cpi_date >= today:
            events.append(('cpi', cpi_date))
        # NFP first week
        nfp_date = date(year, month, _FALLBACK_NFP_DAY)
        if nfp_date >= today:
            events.append(('nfp', nfp_date))
    return events


class EconomicCalendarAdapter(BaseIngestAdapter):
    """
    Tracks upcoming macro events (FOMC, CPI, NFP, GDP) and emits
    event-risk atoms for the subject='market' entity.
    """

    def __init__(self, db_path: Optional[str] = None):
        super().__init__(name='economic_calendar_macro')
        self.db_path = db_path

    def fetch(self) -> List[RawAtom]:
        atoms: List[RawAtom] = []
        now  = datetime.now(timezone.utc).isoformat()
        conf = 0.80

        # Collect all upcoming events: (type, date, days_until)
        upcoming: List[Tuple[str, date, int]] = []

        # FOMC
        fomc = _next_fomc()
        if fomc:
            fomc_date, fomc_days = fomc
            upcoming.append(('fomc', fomc_date, fomc_days))

        # CPI and NFP via FRED or fallback
        for event_type, series_id in _FRED_RELEASE_SERIES.items():
            if event_type == 'gdp':
                dates = _fetch_fred_release_dates(series_id)
            else:
                dates = _fetch_fred_release_dates(series_id)
            for d in dates[:2]:
                days = _days_until(d)
                if 0 <= days <= 60:
                    upcoming.append((event_type, d, days))

        # Fallback if no FRED dates available
        if not any(e[0] in ('cpi', 'nfp') for e in upcoming):
            for event_type, d in _fallback_event_dates():
                days = _days_until(d)
                if 0 <= days <= 60:
                    upcoming.append((event_type, d, days))

        if not upcoming:
            _logger.info('EconomicCalendarAdapter: no upcoming events found')
            return atoms

        # Sort by days until event
        upcoming.sort(key=lambda x: x[2])

        # Nearest event drives the overall risk level
        nearest_type, nearest_date, nearest_days = upcoming[0]

        if nearest_days <= _HIGH_THRESHOLD:
            risk_level = 'high'
        elif nearest_days <= _MEDIUM_THRESHOLD:
            risk_level = 'medium'
        else:
            risk_level = 'low'

        next_event_str = f'{nearest_type.upper()} {nearest_date.isoformat()}'

        meta = {
            'as_of': now,
            'next_event_date': nearest_date.isoformat(),
            'next_event_type': nearest_type,
            'days_until': nearest_days,
            'all_upcoming': [
                {'type': t, 'date': d.isoformat(), 'days': dy}
                for t, d, dy in upcoming[:5]
            ],
        }

        atoms += [
            RawAtom(subject='market', predicate='macro_event_risk',
                    object=risk_level, confidence=conf,
                    source=_SOURCE, metadata=meta),
            RawAtom(subject='market', predicate='next_macro_event',
                    object=next_event_str, confidence=conf,
                    source=_SOURCE, metadata=meta),
            RawAtom(subject='market', predicate='days_to_macro_event',
                    object=str(nearest_days), confidence=conf,
                    source=_SOURCE, metadata=meta),
            RawAtom(subject='market', predicate='macro_event_type',
                    object=nearest_type, confidence=conf,
                    source=_SOURCE, metadata=meta),
        ]

        # Emit per-event atoms for all upcoming events within 14 days
        for event_type, event_date, days in upcoming:
            if days > 14:
                continue
            atoms.append(RawAtom(
                subject='market',
                predicate=f'upcoming_{event_type}',
                object=event_date.isoformat(),
                confidence=conf,
                source=_SOURCE,
                metadata={'days_until': days, 'as_of': now},
            ))

        _logger.info(
            'EconomicCalendarAdapter: next=%s in %d days, risk=%s',
            next_event_str, nearest_days, risk_level,
        )

        return atoms

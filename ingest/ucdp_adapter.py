"""
ingest/ucdp_adapter.py — UCDP Armed Conflict Classification Ingest Adapter

Queries the Uppsala Conflict Data Program (UCDP) API to classify countries
as having active armed conflicts (1,000+ battle deaths/year threshold).
Provides a stable, authoritative binary baseline — complements ACLED's
dynamic protest/unrest signal.

No API key required. Data is updated annually; daily polling checks for
new conflict entries without wasting resources.

Atoms produced:
  - ucdp_conflict | {country_iso} | active_war    (ongoing high-intensity conflict)
  - ucdp_conflict | {country_iso} | minor_conflict (25–999 battle deaths/year)
  - ucdp_conflict | global_war_count | {N}         (number of active wars)

Source prefix: geopolitical_data_ucdp  (authority 0.80, half-life 90d)
Interval: recommended 24h
"""

from __future__ import annotations

import json as _json
import logging
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_UCDP_BASE = 'https://ucdpapi.pcr.uu.se/api'

# UCDP conflict type codes
# Type 1: Extra-systemic (colonial/imperial wars) — rare today
# Type 2: Interstate — between states
# Type 3: Intrastate — civil war
# Type 4: Internationalized intrastate — civil war with foreign involvement
_ACTIVE_TYPES = {1, 2, 3, 4}

# Intensity levels in UCDP
# 1 = minor conflict (25–999 battle deaths/year)
# 2 = war (1,000+ battle deaths/year)
_WAR_INTENSITY    = 2
_MINOR_INTENSITY  = 1

# ISO3 → readable name for metadata
_ISO3_NAMES: Dict[str, str] = {
    'UKR': 'Ukraine', 'RUS': 'Russia', 'SYR': 'Syria', 'YEM': 'Yemen',
    'MMR': 'Myanmar', 'SDN': 'Sudan', 'SSD': 'South Sudan', 'ETH': 'Ethiopia',
    'MLI': 'Mali', 'NER': 'Niger', 'NGA': 'Nigeria', 'MOZ': 'Mozambique',
    'SOM': 'Somalia', 'AFG': 'Afghanistan', 'PAK': 'Pakistan', 'IRQ': 'Iraq',
    'PSE': 'Palestine', 'ISR': 'Israel', 'COD': 'DR Congo', 'CAF': 'CAR',
    'LBY': 'Libya', 'MEX': 'Mexico', 'COL': 'Colombia', 'HTI': 'Haiti',
}


def _fetch_active_conflicts(year: int) -> Optional[List[dict]]:
    """Fetch active conflicts for a given year from UCDP API."""
    url = f'{_UCDP_BASE}/ucdpprioconflict/{year}?pagesize=200&page=1'
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'TradingKB/1.0', 'Accept': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = _json.loads(resp.read().decode('utf-8', errors='replace'))
        return data.get('Result', [])
    except Exception as exc:
        _logger.warning('UCDP fetch failed for year %d: %s', year, exc)
        return None


class UCDPAdapter(BaseIngestAdapter):
    """
    UCDP armed conflict classification adapter.

    Fetches active conflicts from UCDP and emits stable binary conflict atoms
    per country. Uses current year; falls back to prior year on failure.
    No API key required.
    """

    def __init__(self):
        super().__init__(name='ucdp_conflict')

    def fetch(self) -> List[RawAtom]:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        atoms: List[RawAtom] = []
        source = 'geopolitical_data_ucdp'
        meta_base = {'fetched_at': now_iso, 'source_url': _UCDP_BASE}

        current_year = now.year
        # Try current year first; UCDP may lag — fall back to prior year
        conflicts = _fetch_active_conflicts(current_year)
        used_year = current_year
        if not conflicts:
            conflicts = _fetch_active_conflicts(current_year - 1)
            used_year = current_year - 1
        if not conflicts:
            self._logger.warning('UCDP returned no data for %d or %d', current_year, current_year - 1)
            return []

        # Deduplicate by country ISO3 code, keeping highest intensity
        country_intensity: Dict[str, int] = {}
        country_meta: Dict[str, dict] = {}

        for conflict in conflicts:
            conflict_type = conflict.get('type_of_conflict')
            if conflict_type not in _ACTIVE_TYPES:
                continue
            intensity = conflict.get('intensity_level', 0)
            # SideA/SideB locations
            for loc_field in ('location', 'side_a', 'side_b'):
                loc = conflict.get(loc_field, '')
                # UCDP location field contains country names/codes
            # Use gwno_loc or location field for country mapping
            gwno = str(conflict.get('gwno_a', '') or conflict.get('gwno_loc', ''))
            location = conflict.get('location', '')
            # Map UCDP location string to ISO3 — use simple substring matching
            matched_iso = _match_location(location)
            if matched_iso:
                prev = country_intensity.get(matched_iso, 0)
                if intensity > prev:
                    country_intensity[matched_iso] = intensity
                    country_meta[matched_iso] = {
                        'conflict_name': conflict.get('conflict_name', ''),
                        'type': conflict_type,
                        'gwno': gwno,
                        'year': used_year,
                    }

        war_count = 0
        for iso3, intensity in country_intensity.items():
            label = 'active_war' if intensity >= _WAR_INTENSITY else 'minor_conflict'
            if intensity >= _WAR_INTENSITY:
                war_count += 1
            name = _ISO3_NAMES.get(iso3, iso3)
            atoms.append(RawAtom(
                subject='ucdp_conflict',
                predicate=iso3.lower(),
                object=label,
                confidence=0.88,
                source=source,
                metadata={
                    **meta_base,
                    'country': name,
                    'intensity': intensity,
                    **country_meta.get(iso3, {}),
                },
            ))

        # Global war count atom
        atoms.append(RawAtom(
            subject='ucdp_conflict',
            predicate='global_war_count',
            object=str(war_count),
            confidence=0.88,
            source=source,
            metadata={**meta_base, 'year': used_year},
        ))

        self._logger.info('UCDP adapter: %d conflict atoms (%d wars)', len(atoms), war_count)
        return atoms


def _match_location(location: str) -> Optional[str]:
    """Map a UCDP location string to an ISO3 country code."""
    loc_lower = location.lower()
    _LOC_MAP = {
        'ukraine':      'UKR', 'russia':       'RUS', 'syria':        'SYR',
        'yemen':        'YEM', 'myanmar':       'MMR', 'burma':        'MMR',
        'sudan':        'SDN', 'south sudan':   'SSD', 'ethiopia':     'ETH',
        'mali':         'MLI', 'niger':         'NER', 'nigeria':      'NGA',
        'mozambique':   'MOZ', 'somalia':       'SOM', 'afghanistan':  'AFG',
        'pakistan':     'PAK', 'iraq':          'IRQ', 'palestine':    'PSE',
        'israel':       'ISR', 'congo':         'COD', 'central african': 'CAF',
        'libya':        'LBY', 'mexico':        'MEX', 'colombia':     'COL',
        'haiti':        'HTI',
    }
    for keyword, iso3 in _LOC_MAP.items():
        if keyword in loc_lower:
            return iso3
    return None

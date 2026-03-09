"""
ingest/acled_adapter.py — ACLED Conflict & Protest Events Ingest Adapter

Fetches recent conflict and protest events from the Armed Conflict Location
& Event Data (ACLED) API. Provides a dynamic, frequently-updated unrest
signal that complements UCDP's stable annual conflict baseline.

Requires: ACLED_API_KEY and ACLED_EMAIL environment variables
  Free registration (research use): https://acleddata.com/register/

⚠️  LICENSING: ACLED free tier is for non-commercial research only.
    A commercial license is required for production use with paying users.
    Contact ACLED for commercial pricing before public launch.

Atoms produced:
  - acled_unrest | {country_iso} | protest_intensity:{high|medium|low}
  - acled_unrest | {country_iso} | conflict_events:{N}_last_30d
  - acled_unrest | {region}      | unrest_level:{high|medium|low}

Source prefix: geopolitical_data_acled  (authority 0.72, half-life 3d)
Interval: recommended 6h
"""

from __future__ import annotations

import json as _json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_ACLED_BASE = 'https://api.acleddata.com/acled/read'

# Event types in ACLED
_PROTEST_TYPES   = {'Protests', 'Riots'}
_CONFLICT_TYPES  = {'Battles', 'Explosions/Remote violence', 'Violence against civilians',
                    'Strategic developments'}

# Country → region mapping for roll-up atoms
_COUNTRY_REGION: Dict[str, str] = {
    'France': 'europe', 'Germany': 'europe', 'United Kingdom': 'europe',
    'Ukraine': 'europe_east', 'Russia': 'europe_east', 'Poland': 'europe',
    'Israel': 'middle_east', 'Iran': 'middle_east', 'Yemen': 'middle_east',
    'Saudi Arabia': 'middle_east', 'Iraq': 'middle_east', 'Syria': 'middle_east',
    'Nigeria': 'africa', 'Ethiopia': 'africa', 'Mali': 'africa',
    'Somalia': 'africa', 'Sudan': 'africa', 'South Africa': 'africa',
    'DR Congo': 'africa', 'Mozambique': 'africa',
    'Pakistan': 'asia_south', 'India': 'asia_south', 'Afghanistan': 'asia_south',
    'China': 'asia_east', 'Myanmar': 'asia_east',
    'Mexico': 'latam', 'Colombia': 'latam', 'Haiti': 'latam', 'Venezuela': 'latam',
    'United States': 'north_america',
}

# High-priority countries for trading alpha
_PRIORITY_COUNTRIES = {
    'France', 'United Kingdom', 'Germany', 'Ukraine', 'Russia', 'Israel',
    'Iran', 'Saudi Arabia', 'Nigeria', 'South Africa', 'China', 'India',
    'Pakistan', 'Myanmar', 'Mexico', 'Venezuela',
}


def _intensity_label(event_count: int) -> str:
    if event_count >= 20:
        return 'high'
    if event_count >= 5:
        return 'medium'
    return 'low'


class ACLEDAdapter(BaseIngestAdapter):
    """
    ACLED conflict and protest events adapter.

    Fetches events from the last 30 days for priority countries and emits
    per-country unrest intensity atoms.

    ⚠️ Non-commercial research license only. See module docstring.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        email: Optional[str] = None,
    ):
        super().__init__(name='acled_unrest')
        self._api_key = api_key or os.environ.get('ACLED_API_KEY', '')
        self._email   = email   or os.environ.get('ACLED_EMAIL', '')

    def fetch(self) -> List[RawAtom]:
        if not self._api_key or not self._email:
            self._logger.warning(
                'ACLED: ACLED_API_KEY and/or ACLED_EMAIL not set — skipping. '
                'Register (free for research) at https://acleddata.com/register/ '
                'then add ACLED_API_KEY and ACLED_EMAIL to your .env'
            )
            return []

        now = datetime.now(timezone.utc)
        since = (now - timedelta(days=30)).strftime('%Y-%m-%d')
        now_iso = now.isoformat()
        source = 'geopolitical_data_acled'
        meta_base = {'fetched_at': now_iso, 'since': since, 'source_url': _ACLED_BASE}

        params = {
            'key':        self._api_key,
            'email':      self._email,
            'event_date': since,
            'event_date_where': 'BETWEEN',
            'event_date_end': now.strftime('%Y-%m-%d'),
            'fields':     'country|event_type|fatalities|event_date',
            'limit':      5000,
            'format':     'json',
        }
        url = _ACLED_BASE + '?' + urllib.parse.urlencode(params)

        try:
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'TradingGalaxyKB/1.0 (research; admin@tradinggalaxy.dev)',
                         'Accept': 'application/json'},
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    status_code = resp.status
                    raw = resp.read()
            except urllib.error.HTTPError as http_err:
                status_code = http_err.code
                if status_code == 429:
                    retry_after = http_err.headers.get('Retry-After', '3600')
                    self._logger.warning(
                        'ACLED: rate limited (429) — Retry-After: %s seconds. '
                        'Adapter will retry on next scheduled run.',
                        retry_after,
                    )
                elif status_code == 401:
                    self._logger.error(
                        'ACLED: authentication failed (401) — check ACLED_API_KEY and ACLED_EMAIL. '
                        'Keys must match your registered ACLED account.'
                    )
                elif status_code == 403:
                    self._logger.error(
                        'ACLED: forbidden (403) — your API key may not have access to this endpoint. '
                        'Verify your ACLED license tier allows API access.'
                    )
                else:
                    self._logger.warning('ACLED: HTTP %d error: %s', status_code, http_err)
                return []

            data = _json.loads(raw.decode('utf-8', errors='replace'))

            # ACLED API returns error details in the response body even on 200
            if data.get('status') == 0 or data.get('error'):
                err_msg = data.get('error', data.get('message', 'unknown error'))
                self._logger.error('ACLED: API returned error: %s', err_msg)
                return []

            events = data.get('data', [])
            if not events and data.get('count', -1) == 0:
                self._logger.info('ACLED: API returned 0 events for the requested period/countries')
                return []

        except Exception as exc:
            self._logger.warning('ACLED: fetch failed (%s: %s)', type(exc).__name__, exc)
            return []

        # Tally events and fatalities per country
        protest_counts:  Dict[str, int] = defaultdict(int)
        conflict_counts: Dict[str, int] = defaultdict(int)
        fatalities:      Dict[str, int] = defaultdict(int)

        for ev in events:
            country    = ev.get('country', '')
            event_type = ev.get('event_type', '')
            fat        = int(ev.get('fatalities', 0) or 0)
            fatalities[country] += fat
            if event_type in _PROTEST_TYPES:
                protest_counts[country] += 1
            elif event_type in _CONFLICT_TYPES:
                conflict_counts[country] += 1

        atoms: List[RawAtom] = []
        region_protest: Dict[str, List[int]] = defaultdict(list)

        all_countries = set(protest_counts) | set(conflict_counts)
        for country in all_countries:
            # Only emit atoms for priority countries (reduces noise)
            if country not in _PRIORITY_COUNTRIES:
                continue

            p_count = protest_counts.get(country, 0)
            c_count = conflict_counts.get(country, 0)
            fat_count = fatalities.get(country, 0)
            region = _COUNTRY_REGION.get(country, 'other')

            if p_count > 0:
                intensity = _intensity_label(p_count)
                atoms.append(RawAtom(
                    subject='acled_unrest',
                    predicate=country.lower().replace(' ', '_'),
                    object=f'protest_intensity:{intensity}',
                    confidence=0.72,
                    source=source,
                    metadata={
                        **meta_base,
                        'country': country,
                        'protest_events_30d': p_count,
                        'conflict_events_30d': c_count,
                        'fatalities_30d': fat_count,
                    },
                ))
                region_protest[region].append(p_count)

            if c_count > 0:
                atoms.append(RawAtom(
                    subject='acled_unrest',
                    predicate=country.lower().replace(' ', '_') + '_conflict',
                    object=f'conflict_events:{c_count}_last_30d',
                    confidence=0.72,
                    source=source,
                    metadata={
                        **meta_base,
                        'country': country,
                        'conflict_events_30d': c_count,
                        'fatalities_30d': fat_count,
                    },
                ))

        # ── Region roll-up unrest atoms ───────────────────────────────────────
        for region, counts in region_protest.items():
            avg = sum(counts) / len(counts) if counts else 0
            level = _intensity_label(int(avg))
            atoms.append(RawAtom(
                subject='acled_unrest',
                predicate=f'{region}_unrest',
                object=f'unrest_level:{level}',
                confidence=0.65,
                source=source,
                metadata={**meta_base, 'region': region, 'avg_protest_count': round(avg, 1)},
            ))

        self._logger.info('ACLED adapter: %d atoms from %d events', len(atoms), len(events))
        return atoms

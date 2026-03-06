"""
ingest/ucdp_adapter.py — Country Conflict Signal Adapter (GDELT-derived)

The UCDP REST API now requires authentication (HTTP 401 on all endpoints).
This adapter uses the free GDELT Doc API (artlist mode) to derive per-country
conflict intensity by counting conflict/war/military articles over 7 days.
No API key required.

Atoms produced:
  - ucdp_conflict | {country_iso} | active_war    (>=50 conflict articles/7d)
  - ucdp_conflict | {country_iso} | minor_conflict (10-49 articles/7d)
  - ucdp_conflict | {country_iso} | stable         (<10 articles/7d)
  - ucdp_conflict | global_war_count | {N}

Source prefix: geopolitical_data_ucdp  (authority 0.75, half-life 3d)
Interval: recommended 12h
"""

from __future__ import annotations

import json as _json
import logging
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_GDELT_DOC_BASE = 'http://api.gdeltproject.org/api/v2/doc/doc'

# Countries to monitor with (ISO3, readable name, GDELT query)
_CONFLICT_COUNTRIES: List[tuple] = [
    ('UKR', 'Ukraine',     'Ukraine war military attack'),
    ('RUS', 'Russia',      'Russia military strike attack'),
    ('IRN', 'Iran',        'Iran military strikes attack'),
    ('ISR', 'Israel',      'Israel military Gaza strikes'),
    ('PSE', 'Palestine',   'Gaza Palestine strikes bombing'),
    ('SYR', 'Syria',       'Syria conflict war bombing'),
    ('YEM', 'Yemen',       'Yemen Houthi war missile'),
    ('SDN', 'Sudan',       'Sudan civil war conflict'),
    ('MMR', 'Myanmar',     'Myanmar military junta conflict'),
    ('AFG', 'Afghanistan', 'Afghanistan Taliban attack'),
    ('PAK', 'Pakistan',    'Pakistan military conflict attack'),
    ('ETH', 'Ethiopia',    'Ethiopia conflict war'),
    ('SOM', 'Somalia',     'Somalia conflict attack'),
    ('COD', 'DR Congo',    'Congo conflict war'),
    ('NGA', 'Nigeria',     'Nigeria conflict Boko Haram'),
    ('MLI', 'Mali',        'Mali conflict coup attack'),
]

# Article count thresholds over 7 days
_WAR_THRESHOLD   = 50   # >=50 → active_war
_MINOR_THRESHOLD = 10   # 10-49 → minor_conflict; <10 → stable

# ISO3 readable names dict
_ISO3_NAMES: Dict[str, str] = {r[0]: r[1] for r in _CONFLICT_COUNTRIES}


def _gdelt_article_count(query: str, timespan: str = '7d') -> Optional[int]:
    """
    Count articles matching query over timespan using GDELT artlist.
    Returns article count, or None on failure.
    """
    params = {
        'query':      query,
        'mode':       'artlist',
        'format':     'json',
        'timespan':   timespan,
        'maxrecords': '250',
    }
    url = _GDELT_DOC_BASE + '?' + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'TradingKB/1.0', 'Accept': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = _json.loads(resp.read().decode('utf-8', errors='replace'))
        articles = data.get('articles', [])
        return len(articles)
    except Exception as exc:
        _logger.debug('GDELT artlist failed for %r: %s', query, exc)
        return None


class UCDPAdapter(BaseIngestAdapter):
    """
    Country conflict intensity adapter — derived from GDELT article counts.

    Replaces broken UCDP REST API (now requires auth) with a free GDELT-based
    proxy: counts conflict/military/war articles per country over 7 days and
    classifies as active_war / minor_conflict / stable.
    """

    def __init__(self):
        super().__init__(name='ucdp_conflict')

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []
        source = 'geopolitical_data_ucdp'
        meta_base = {
            'fetched_at': now_iso,
            'source_url': _GDELT_DOC_BASE,
            'method': 'gdelt_artlist_proxy',
        }

        war_count = 0

        for iso3, name, query in _CONFLICT_COUNTRIES:
            time.sleep(3)  # rate limit: ~5 req/s max on GDELT free tier
            count = _gdelt_article_count(query, timespan='7d')
            if count is None:
                self._logger.debug('GDELT artlist returned None for %s — skipping', iso3)
                continue

            if count >= _WAR_THRESHOLD:
                label = 'active_war'
                war_count += 1
            elif count >= _MINOR_THRESHOLD:
                label = 'minor_conflict'
            else:
                label = 'stable'

            atoms.append(RawAtom(
                subject='ucdp_conflict',
                predicate=iso3.lower(),
                object=label,
                confidence=0.72,
                source=source,
                metadata={
                    **meta_base,
                    'country': name,
                    'article_count_7d': count,
                    'query': query,
                },
            ))

        if atoms:
            atoms.append(RawAtom(
                subject='ucdp_conflict',
                predicate='global_war_count',
                object=str(war_count),
                confidence=0.72,
                source=source,
                metadata=meta_base,
            ))

        self._logger.info(
            'UCDPAdapter (GDELT proxy): %d conflict atoms (%d active wars)',
            len(atoms), war_count,
        )
        return atoms

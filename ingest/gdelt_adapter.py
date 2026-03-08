"""
ingest/gdelt_adapter.py — GDELT GKG Bilateral Tension Ingest Adapter

Queries the GDELT 2.0 GKG Doc API to derive bilateral geopolitical tension
scores for 6 strategic country pairs. No API key required.

GDELT tone ranges from -100 (hostile) to +100 (positive). We invert and
normalise to 0–100 where 100 = maximum hostility.

Atoms produced:
  - gdelt_tension | {pair}_score  | {0–100}         (tension score)
  - gdelt_tension | {pair}_trend  | rising | stable | falling
  - gdelt_tension | {region}_risk | elevated | moderate | low  (region roll-up)

Source prefix: geopolitical_data_gdelt  (authority 0.65, half-life 2d)
Interval: recommended 1h (GDELT is noisy; 1h smoothing reduces single-headline spikes)
"""

from __future__ import annotations

import json as _json
import logging
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

_GDELT_DOC_BASE = 'http://api.gdeltproject.org/api/v2/doc/doc'  # http avoids SSL timeout on restricted outbound

# ── Country pairs: (label, query_string, region_tag) ─────────────────────────
# query_string uses GDELT location names / country codes
_PAIRS: List[Tuple[str, str, str]] = [
    ('us_russia',       'United States Russia',          'europe_east'),
    ('russia_ukraine',  'Russia Ukraine',                'europe_east'),
    ('us_china',        'United States China',           'asia_east'),
    ('china_taiwan',    'China Taiwan',                  'asia_east'),
    ('us_iran',         'United States Iran',            'middle_east'),
    ('us_venezuela',    'United States Venezuela',       'latam'),
]

# Tone thresholds for trend labelling (±5% of scale)
_TREND_THRESHOLD = 5.0

# Risk tier thresholds on 0–100 scale
_ELEVATED_THRESHOLD = 60
_MODERATE_THRESHOLD = 35


def _gdelt_tone_query(query: str, timespan: str = '1d') -> Optional[float]:
    """
    Fetch average tone for a query from GDELT GKG tonechart mode.
    Returns inverted/normalised tension score (0–100), or None on failure.
    """
    params = {
        'query':    query,
        'mode':     'tonechart',
        'format':   'json',
        'timespan': timespan,
    }
    url = _GDELT_DOC_BASE + '?' + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'TradingKB/1.0', 'Accept': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = _json.loads(resp.read().decode('utf-8', errors='replace'))

        # GDELT tonechart returns {'tonechart': [{'bin': <tone>, 'count': N, 'toparts': [...]}, ...]}
        # Each entry is a tone bucket: 'bin' is the tone midpoint (-100..+100), 'count' is article count.
        chart = data.get('tonechart', [])
        if not chart:
            return None

        # Count-weighted average of bin values
        total_weight = 0.0
        weighted_sum = 0.0
        for b in chart:
            tone_val = b.get('bin') if b.get('bin') is not None else b.get('avg')
            count = b.get('count', 1)
            if tone_val is not None:
                try:
                    weighted_sum += float(tone_val) * float(count)
                    total_weight += float(count)
                except (TypeError, ValueError):
                    pass
        if total_weight == 0:
            return None

        avg_tone = weighted_sum / total_weight
        # Invert (-100..+100) → tension score (0..100)
        # avg_tone = -100 (max hostility) → score = 100
        # avg_tone = +100 (max positive)  → score = 0
        tension = max(0.0, min(100.0, (-avg_tone + 100.0) / 2.0))
        return round(tension, 1)

    except Exception as exc:
        _logger.warning('GDELT tone query failed for %r: %s', query, exc)
        return None


def _trend_label(current: float, previous: Optional[float]) -> str:
    if previous is None:
        return 'stable'
    delta = current - previous
    if delta > _TREND_THRESHOLD:
        return 'rising'
    if delta < -_TREND_THRESHOLD:
        return 'falling'
    return 'stable'


def _risk_tier(score: float) -> str:
    if score >= _ELEVATED_THRESHOLD:
        return 'elevated'
    if score >= _MODERATE_THRESHOLD:
        return 'moderate'
    return 'low'


class GDELTAdapter(BaseIngestAdapter):
    """
    GDELT GKG bilateral tension ingest adapter.

    Queries GDELT tonechart API for 6 strategic country pairs.
    No API key required. Emits tension score and trend atoms.
    """

    def __init__(self):
        super().__init__(name='gdelt_tension')
        self._prev_scores: Dict[str, float] = {}

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()
        atoms: List[RawAtom] = []
        source = 'geopolitical_data_gdelt'
        meta_base = {'fetched_at': now_iso, 'source_url': _GDELT_DOC_BASE}

        region_scores: Dict[str, List[float]] = {}

        for pair_label, query, region in _PAIRS:
            time.sleep(15)  # 4 pairs/min to avoid 429 rate limit
            score = _gdelt_tone_query(query, timespan='1d')
            if score is None:
                self._logger.warning('No GDELT data for pair %s', pair_label)
                continue

            prev_score = self._prev_scores.get(pair_label)
            trend = _trend_label(score, prev_score)
            self._prev_scores[pair_label] = score

            # Score atom
            atoms.append(RawAtom(
                subject='gdelt_tension',
                predicate=f'{pair_label}_score',
                object=str(score),
                confidence=0.65,
                source=source,
                metadata={**meta_base, 'query': query, 'scale': '0-100 (100=max hostility)'},
            ))
            # Trend atom
            atoms.append(RawAtom(
                subject='gdelt_tension',
                predicate=f'{pair_label}_trend',
                object=trend,
                confidence=0.60,
                source=source,
                metadata={**meta_base, 'current': score, 'previous': prev_score},
            ))

            region_scores.setdefault(region, []).append(score)

        # ── Region roll-up risk atoms ─────────────────────────────────────────
        for region, scores in region_scores.items():
            avg = sum(scores) / len(scores)
            tier = _risk_tier(avg)
            atoms.append(RawAtom(
                subject='gdelt_tension',
                predicate=f'{region}_risk',
                object=tier,
                confidence=0.62,
                source=source,
                metadata={**meta_base, 'region_avg_score': round(avg, 1)},
            ))

        self._logger.info('GDELT adapter: %d atoms produced', len(atoms))
        return atoms

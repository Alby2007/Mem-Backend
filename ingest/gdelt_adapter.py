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

# ── GEO_LEXICON — conflict-tuned term overrides ───────────────────────────────
# Sourced from ashioyajotham/global-activity-monitor — 120+ terms that AFINN
# misscores in geopolitical context. Applied as post-processing adjustment.
# Positive value → raises tension score; negative → lowers it.
# Scale: adjustment in tension-score points (0–100 scale).
_GEO_LEXICON: dict = {
    # Conflict escalation terms (underweighted by AFINN — should raise tension)
    'strike':        +4.0,
    'strikes':       +4.0,
    'fire':          +3.0,
    'fired':         +2.0,
    'firing':        +3.0,
    'arms':          +5.0,
    'armed':         +4.0,
    'combat':        +7.0,
    'offensive':     +6.0,
    'invasion':      +9.0,
    'invaded':       +9.0,
    'invade':        +9.0,
    'troops':        +5.0,
    'troops massed': +8.0,
    'military':      +3.0,
    'missile':       +7.0,
    'missiles':      +7.0,
    'drone':         +4.0,
    'drones':        +4.0,
    'airstrike':     +8.0,
    'airstrikes':    +8.0,
    'shelling':      +7.0,
    'shell':         +5.0,
    'bombardment':   +8.0,
    'blockade':      +6.0,
    'siege':         +7.0,
    'casualties':    +6.0,
    'killed':        +5.0,
    'deaths':        +5.0,
    'fatalities':    +6.0,
    'wounded':       +4.0,
    'sanctions':     +5.0,
    'sanctioned':    +4.0,
    'embargo':       +5.0,
    'tariff':        +3.0,
    'tariffs':       +3.0,
    'expulsion':     +4.0,
    'expelled':      +4.0,
    'detained':      +3.0,
    'detention':     +3.0,
    'arrest':        +2.0,
    'arrested':      +2.0,
    'coup':          +8.0,
    'uprising':      +6.0,
    'revolt':        +6.0,
    'insurgency':    +6.0,
    'insurgent':     +5.0,
    'rebel':         +4.0,
    'rebels':        +4.0,
    'guerrilla':     +5.0,
    'terrorism':     +7.0,
    'terrorist':     +7.0,
    'attack':        +5.0,
    'attacks':       +5.0,
    'bomb':          +6.0,
    'bombing':       +7.0,
    'explosion':     +5.0,
    'explosions':    +5.0,
    'confrontation': +4.0,
    'skirmish':      +5.0,
    'escalation':    +6.0,
    'escalating':    +5.0,
    'hostile':       +4.0,
    'hostility':     +5.0,
    'hostilities':   +6.0,
    'threat':        +4.0,
    'threats':       +4.0,
    'threatening':   +4.0,
    'ultimatum':     +6.0,
    'provocation':   +5.0,
    'provoke':       +4.0,
    'provoked':      +4.0,
    'incursion':     +7.0,
    'occupation':    +5.0,
    'occupied':      +4.0,
    'annexed':       +7.0,
    'annexation':    +7.0,
    'sovereignty':   +3.0,
    'territorial':   +3.0,
    'disputed':      +3.0,
    'conflict':      +5.0,
    'war':           +8.0,
    'warfare':       +8.0,
    'wartime':       +7.0,
    'nuclear':       +6.0,
    'chemical':      +6.0,
    'biological':    +6.0,
    'warhead':       +8.0,
    'ballistic':     +7.0,
    'hypersonic':    +6.0,
    'detonation':    +8.0,
    'detonated':     +8.0,
    'crackdown':     +4.0,
    'suppression':   +4.0,
    'martial law':   +7.0,
    'curfew':        +4.0,
    'unrest':        +4.0,
    'protests':      +2.0,
    'riots':         +5.0,
    'riot':          +5.0,
    # De-escalation terms (overweighted by AFINN — should lower tension)
    'ceasefire':     -8.0,
    'truce':         -7.0,
    'peace':         -4.0,
    'peacekeeping':  -5.0,
    'negotiations':  -3.0,
    'negotiating':   -3.0,
    'diplomacy':     -3.0,
    'diplomatic':    -3.0,
    'agreement':     -4.0,
    'treaty':        -5.0,
    'withdrawal':    -4.0,
    'withdrew':      -4.0,
    'retreated':     -4.0,
    'de-escalation': -6.0,
    'reconciliation':-4.0,
    'talks':         -2.0,
    'dialogue':      -3.0,
}

# Maximum adjustment cap — prevents single-term overrides swinging scores wildly
_GEO_LEXICON_CAP = 15.0


def _apply_geo_lexicon(base_tension: float, query: str) -> float:
    """
    Apply GEO_LEXICON adjustments to a raw GDELT tension score.
    The query string is searched for lexicon terms; matching terms
    accumulate their adjustments (capped at _GEO_LEXICON_CAP).
    """
    query_lower = query.lower()
    adjustment = 0.0
    for term, delta in _GEO_LEXICON.items():
        if term in query_lower:
            adjustment += delta
    # Cap total adjustment
    adjustment = max(-_GEO_LEXICON_CAP, min(_GEO_LEXICON_CAP, adjustment))
    return max(0.0, min(100.0, base_tension + adjustment))

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
            time.sleep(30)  # 2 pairs/min — GDELT free tier hard limit ~120 req/hr per IP
            raw_score = _gdelt_tone_query(query, timespan='1d')
            if raw_score is None:
                self._logger.warning('No GDELT data for pair %s', pair_label)
                continue

            # Apply GEO_LEXICON post-processing correction
            score = _apply_geo_lexicon(raw_score, query)

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

        # ── Energy ticker geo-risk linkage ────────────────────────────────────
        # When Middle East or LatAm tension is elevated/moderate, write
        # geopolitical_risk_exposure atoms keyed by major energy tickers so
        # that BP/Shell/XOM queries surface geo context directly.
        _ENERGY_TICKERS = ['bp.l', 'shel.l', 'xom', 'cvx', 'cop']
        for region, scores in region_scores.items():
            if region not in ('middle_east', 'latam'):
                continue
            avg = sum(scores) / len(scores)
            tier = _risk_tier(avg)
            if tier in ('elevated', 'moderate'):
                for ticker in _ENERGY_TICKERS:
                    atoms.append(RawAtom(
                        subject=ticker,
                        predicate='geopolitical_risk_exposure',
                        object=f'{tier}_geo_risk:{region}',
                        confidence=0.62,
                        source=source,
                        metadata={
                            **meta_base,
                            'region': region,
                            'tension_score': round(avg, 1),
                            'risk_tier': tier,
                        },
                        upsert=True,
                    ))

        self._logger.info('GDELT adapter: %d atoms produced', len(atoms))
        return atoms

"""
analytics/state_matcher.py — Historical State Matching

Given a list of current KB atoms, reconstructs the current market state vector
and finds the most similar historical states in signal_calibration, weighted
by both state similarity and temporal recency.

USAGE
-----
    from analytics.state_matcher import match_historical_state
    precedent = match_historical_state(atoms, db_path)
    if precedent:
        print(f"{precedent.match_count} similar instances, "
              f"hit rate {precedent.weighted_hit_t1:.0%}")

ALGORITHM
---------
1. Extract current state vector from KB atoms
   {pattern_type, regime_label, volatility_regime, sector,
    central_bank_stance, gdelt_tension_level}

2. Load all signal_calibration rows (single query, scored in Python)

3. Compute state_similarity(current, historical) for each row —
   weighted fuzzy match with partial credit for adjacent values

4. Compute temporal_weight(last_updated) — exponential decay, 12-month half-life

5. Filter: similarity >= threshold (default 0.5)

6. Aggregate: combined_weight = similarity * temporal_weight
   Weighted hit rates, stop rate, avg R, best/worst regime

7. Return HistoricalPrecedent (or None if < min_samples matches)
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)

# ── State feature weights (must sum to 1.0) ────────────────────────────────────

_STATE_WEIGHTS: Dict[str, float] = {
    'pattern_type':        0.30,
    'regime_label':        0.25,
    'volatility_regime':   0.20,
    'sector':              0.10,
    'central_bank_stance': 0.10,
    'gdelt_tension_level': 0.05,
}

# Ordered adjacency for partial credit (0.5× weight)
_ADJACENCY: Dict[str, Dict[str, List[str]]] = {
    'volatility_regime': {
        'low':     ['medium'],
        'medium':  ['low', 'high'],
        'high':    ['medium', 'extreme'],
        'extreme': ['high'],
    },
    'central_bank_stance': {
        'dovish':                ['neutral'],
        'neutral':               ['dovish', 'neutral_to_restrictive'],
        'neutral_to_restrictive': ['neutral', 'restrictive'],
        'restrictive':           ['neutral_to_restrictive'],
    },
    'regime_label': {
        'recovery':             ['risk_on_expansion'],
        'risk_on_expansion':    ['recovery'],
        'risk_off_contraction': ['stagflation'],
        'stagflation':          ['risk_off_contraction'],
    },
}

# Atom predicate → state feature mapping
_PREDICATE_MAP: Dict[str, str] = {
    'pattern_type':        'pattern_type',
    'regime_label':        'regime_label',
    'market_regime':       'regime_label',       # alias
    'price_regime':        'regime_label',       # coarser alias
    'volatility_regime':   'volatility_regime',
    'sector':              'sector',
    'central_bank_stance': 'central_bank_stance',
    'gdelt_tension_level': 'gdelt_tension_level',
    'gdelt_tension':       'gdelt_tension_level', # raw score → bucketed below
}


# ── HistoricalPrecedent ────────────────────────────────────────────────────────

@dataclass
class HistoricalPrecedent:
    match_count:      int
    avg_similarity:   float
    weighted_hit_t1:  float
    weighted_hit_t2:  float
    weighted_stopped: float
    weighted_avg_r:   Optional[float]   # None when no r_multiple data yet
    best_regime:      Optional[str]
    worst_regime:     Optional[str]
    best_sector:      Optional[str]
    recency_note:     str
    confidence:       str               # 'high' | 'moderate' | 'low'
    current_state:    Dict[str, str]
    top_matches:      List[dict] = field(default_factory=list)


# ── Similarity helpers ─────────────────────────────────────────────────────────

def _is_adjacent(feature: str, val_a: str, val_b: str) -> bool:
    adj = _ADJACENCY.get(feature, {})
    return val_b in adj.get(val_a, [])


def state_similarity(current: dict, historical: dict) -> float:
    """
    Weighted fuzzy similarity between two state dicts.
    Returns 0.0–1.0. Missing features in either dict are excluded from
    the weight total so partial state vectors still produce meaningful scores.
    """
    score = 0.0
    total_weight = 0.0
    for feature, weight in _STATE_WEIGHTS.items():
        cur_val  = current.get(feature)
        hist_val = historical.get(feature)
        if cur_val is None or hist_val is None:
            continue
        total_weight += weight
        if cur_val == hist_val:
            score += weight
        elif _is_adjacent(feature, cur_val, hist_val):
            score += weight * 0.5
    return score / total_weight if total_weight > 0 else 0.0


# ── Temporal weighting ─────────────────────────────────────────────────────────

def temporal_weight(outcome_date: str, half_life_months: float = 12.0) -> float:
    """
    Exponential decay by age:
      2 months ago  → 0.89
      6 months ago  → 0.71
      1 year ago    → 0.50
      2 years ago   → 0.25
      3 years ago   → 0.13
    Unknown dates get neutral weight 0.5.
    """
    if not outcome_date:
        return 0.5
    try:
        dt = datetime.fromisoformat(outcome_date.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        months_ago  = (datetime.now(timezone.utc) - dt).days / 30.44
        decay_rate  = math.log(2) / half_life_months
        return math.exp(-decay_rate * months_ago)
    except Exception:
        return 0.5


# ── State extraction ───────────────────────────────────────────────────────────

def _bucket_gdelt(raw_val: str) -> Optional[str]:
    """Convert a raw GDELT tension score string to low/medium/high bucket."""
    try:
        v = float(raw_val)
        if v < 15:
            return 'low'
        if v < 30:
            return 'medium'
        return 'high'
    except Exception:
        return None


def _extract_current_state(atoms: list) -> dict:
    """
    Build state vector from a list of KB atom dicts
    (keys: subject, predicate, object, ...).
    Last-wins for duplicate predicates (higher-confidence atoms should be
    sorted first by the caller; retrieval already does this).
    """
    state: dict = {}
    for atom in atoms:
        pred = (atom.get('predicate') or '').lower().strip()
        val  = (atom.get('object')    or '').lower().strip()
        if not pred or not val:
            continue
        feature = _PREDICATE_MAP.get(pred)
        if feature is None:
            continue
        if pred == 'gdelt_tension':
            bucketed = _bucket_gdelt(val)
            if bucketed:
                state['gdelt_tension_level'] = bucketed
        else:
            state[feature] = val
    return state


# ── Historical state reconstruction ───────────────────────────────────────────

def _reconstruct_historical_state(row: dict) -> dict:
    """
    Reconstruct the state vector from a signal_calibration row dict.
    Uses columns written at outcome time.
    """
    state: dict = {}
    for col, feature in [
        ('pattern_type',        'pattern_type'),
        ('market_regime',       'regime_label'),
        ('volatility_regime',   'volatility_regime'),
        ('sector',              'sector'),
        ('central_bank_stance', 'central_bank_stance'),
        ('gdelt_tension_level', 'gdelt_tension_level'),
    ]:
        val = row.get(col)
        if val:
            state[feature] = str(val).lower().strip()
    return state


# ── Aggregation ────────────────────────────────────────────────────────────────

def _aggregate_weighted_outcomes(
    matches: List[tuple],   # (row_dict, similarity, t_weight)
) -> dict:
    """
    Compute combined-weight aggregated outcome statistics.
    combined_weight = similarity * temporal_weight
    """
    total_w    = 0.0
    w_hit_t1   = 0.0
    w_hit_t2   = 0.0
    w_stopped  = 0.0
    w_r_sum    = 0.0
    r_count    = 0.0

    regime_w:  Dict[str, float] = {}
    regime_h:  Dict[str, float] = {}
    sector_w:  Dict[str, float] = {}
    sector_h:  Dict[str, float] = {}

    most_recent_date = ''

    for row, sim, tw in matches:
        w = sim * tw
        total_w   += w
        w_hit_t1  += w * (row.get('hit_rate_t1')     or 0.0)
        w_hit_t2  += w * (row.get('hit_rate_t2')     or 0.0)
        w_stopped += w * (row.get('stopped_out_rate') or 0.0)

        r_val = row.get('outcome_r_multiple')
        if r_val is not None:
            w_r_sum += w * r_val
            r_count += w

        # Regime breakdown
        regime = row.get('market_regime')
        if regime:
            regime_w[regime] = regime_w.get(regime, 0.0) + w
            regime_h[regime] = regime_h.get(regime, 0.0) + w * (row.get('hit_rate_t1') or 0.0)

        # Sector breakdown
        sector = row.get('sector')
        if sector:
            sector_w[sector] = sector_w.get(sector, 0.0) + w
            sector_h[sector] = sector_h.get(sector, 0.0) + w * (row.get('hit_rate_t1') or 0.0)

        # Track most recent
        lu = row.get('last_updated') or ''
        if lu > most_recent_date:
            most_recent_date = lu

    if total_w == 0:
        return {}

    # Best/worst regime by weighted hit rate
    best_regime = worst_regime = None
    if regime_w:
        regime_rates = {r: regime_h[r] / regime_w[r] for r in regime_w}
        best_regime  = max(regime_rates, key=regime_rates.__getitem__)
        worst_regime = min(regime_rates, key=regime_rates.__getitem__)

    best_sector = None
    if sector_w:
        sector_rates = {s: sector_h[s] / sector_w[s] for s in sector_w}
        best_sector  = max(sector_rates, key=sector_rates.__getitem__)

    # Recency note
    recency_note = ''
    if most_recent_date:
        try:
            dt = datetime.fromisoformat(most_recent_date.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            days = (datetime.now(timezone.utc) - dt).days
            if days < 7:
                recency_note = 'Most recent: this week'
            elif days < 30:
                weeks = days // 7
                recency_note = f'Most recent: {weeks} week{"s" if weeks > 1 else ""} ago'
            elif days < 365:
                months = days // 30
                recency_note = f'Most recent: {months} month{"s" if months > 1 else ""} ago'
            else:
                years = days // 365
                recency_note = f'Most recent: {years} year{"s" if years > 1 else ""} ago'
        except Exception:
            pass

    return {
        'weighted_hit_t1':  w_hit_t1  / total_w,
        'weighted_hit_t2':  w_hit_t2  / total_w,
        'weighted_stopped': w_stopped / total_w,
        'weighted_avg_r':   (w_r_sum / r_count) if r_count > 0 else None,
        'best_regime':      best_regime,
        'worst_regime':     worst_regime,
        'best_sector':      best_sector,
        'recency_note':     recency_note,
    }


# ── Main entry point ───────────────────────────────────────────────────────────

def match_historical_state(
    current_atoms: list,
    db_path: str,
    similarity_threshold: float = 0.5,
    min_samples: int = 10,
    half_life_months: float = 12.0,
) -> Optional[HistoricalPrecedent]:
    """
    Main entry point called by retrieval.py Strategy 6 and the pattern
    precedent endpoint.

    Parameters
    ----------
    current_atoms       : list of atom dicts from retrieval
    db_path             : explicit SQLite file path (avoids conn.database compat issues)
    similarity_threshold: minimum state_similarity to include a match (default 0.5)
    min_samples         : minimum match count to return a result (default 10)
    half_life_months    : temporal decay half-life in months (default 12)

    Returns
    -------
    HistoricalPrecedent or None
    """
    if not current_atoms or not db_path:
        return None

    try:
        current_state = _extract_current_state(current_atoms)
        if not current_state:
            return None

        # Load all calibration rows in one query
        conn = sqlite3.connect(db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            from analytics.signal_calibration import _ensure_table
            _ensure_table(conn)
            rows = conn.execute(
                """SELECT ticker, pattern_type, timeframe, market_regime,
                          sample_size, hit_rate_t1, hit_rate_t2, stopped_out_rate,
                          outcome_r_multiple, volatility_regime, sector,
                          central_bank_stance, gdelt_tension_level, last_updated
                   FROM signal_calibration
                   WHERE sample_size > 0 AND hit_rate_t1 IS NOT NULL"""
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            return None

        # Score every row
        matches = []
        for row in rows:
            row_dict    = dict(row)
            hist_state  = _reconstruct_historical_state(row_dict)
            sim         = state_similarity(current_state, hist_state)
            if sim < similarity_threshold:
                continue
            tw = temporal_weight(row_dict.get('last_updated') or '', half_life_months)
            matches.append((row_dict, sim, tw))

        if len(matches) < min_samples:
            _log.debug(
                'state_matcher: only %d matches (need %d) for state %s',
                len(matches), min_samples, current_state,
            )
            return None

        # Aggregate
        agg = _aggregate_weighted_outcomes(matches)
        if not agg:
            return None

        avg_sim    = sum(m[1] for m in matches) / len(matches)
        match_count = len(matches)
        confidence  = (
            'high'     if match_count >= 30 else
            'moderate' if match_count >= 10 else
            'low'
        )

        # Top 5 most similar (for transparency / future UI expansion)
        top_matches = [
            {
                'ticker':       m[0].get('ticker'),
                'pattern_type': m[0].get('pattern_type'),
                'regime':       m[0].get('market_regime'),
                'similarity':   round(m[1], 2),
                'hit_rate_t1':  m[0].get('hit_rate_t1'),
            }
            for m in sorted(matches, key=lambda x: x[1], reverse=True)[:5]
        ]

        return HistoricalPrecedent(
            match_count      = match_count,
            avg_similarity   = round(avg_sim, 3),
            weighted_hit_t1  = round(agg['weighted_hit_t1'], 4),
            weighted_hit_t2  = round(agg['weighted_hit_t2'], 4),
            weighted_stopped = round(agg['weighted_stopped'], 4),
            weighted_avg_r   = round(agg['weighted_avg_r'], 3) if agg['weighted_avg_r'] is not None else None,
            best_regime      = agg['best_regime'],
            worst_regime     = agg['worst_regime'],
            best_sector      = agg['best_sector'],
            recency_note     = agg['recency_note'],
            confidence       = confidence,
            current_state    = current_state,
            top_matches      = top_matches,
        )

    except Exception as exc:
        _log.debug('state_matcher: match_historical_state failed: %s', exc)
        return None

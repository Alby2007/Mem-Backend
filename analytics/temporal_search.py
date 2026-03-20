"""
analytics/temporal_search.py — Temporal Market State Search Engine

Searches market_state_snapshots to find historical moments when conditions
matched a query, then computes what happened to prices afterwards.

Answers: "When has this situation happened before, and what happened next?"

USAGE
-----
    from analytics.temporal_search import TemporalStateSearch
    searcher = TemporalStateSearch(db_path)
    result = searcher.search_by_natural_language("when tech was bearish with high vol")
    result = searcher.search_for_ticker("NVDA", "when NVDA had high vol during Fed tightening")

ALGORITHM
---------
1. Parse natural language query → state dict (keyword matching, no LLM)
2. Load all snapshots for subject from market_state_snapshots
3. Score each snapshot: similarity (from state_matcher) × temporal_weight
4. For each match above threshold, look up forward OHLCV returns (1w, 1m)
5. Aggregate: outcome distribution, regime breakdown, best period
6. Return TemporalSearchSummary with top_matches
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

# ── Natural language → state dict keyword map ──────────────────────────────────

_NL_STATE_MAP: Dict[str, Tuple[str, str]] = {
    # Direction
    'bullish':              ('signal_direction', 'bullish'),
    'bearish':              ('signal_direction', 'bearish'),
    ' long ':               ('signal_direction', 'bullish'),
    ' short ':              ('signal_direction', 'bearish'),
    'going up':             ('signal_direction', 'bullish'),
    'going down':           ('signal_direction', 'bearish'),
    'rising':               ('signal_direction', 'bullish'),
    'falling':              ('signal_direction', 'bearish'),

    # Volatility
    'high vol':             ('volatility_regime', 'high'),
    'high volatility':      ('volatility_regime', 'high'),
    'elevated vol':         ('volatility_regime', 'high'),
    'volatile':             ('volatility_regime', 'high'),
    'low vol':              ('volatility_regime', 'low'),
    'low volatility':       ('volatility_regime', 'low'),
    'quiet market':         ('volatility_regime', 'low'),
    'extreme vol':          ('volatility_regime', 'extreme'),
    'extreme volatility':   ('volatility_regime', 'extreme'),

    # Market regime
    'risk on':              ('regime_label', 'risk_on_expansion'),
    'risk-on':              ('regime_label', 'risk_on_expansion'),
    'risk off':             ('regime_label', 'risk_off_contraction'),
    'risk-off':             ('regime_label', 'risk_off_contraction'),
    'recovery':             ('regime_label', 'recovery'),
    'stagflation':          ('regime_label', 'stagflation'),
    'expansion':            ('regime_label', 'risk_on_expansion'),
    'contraction':          ('regime_label', 'risk_off_contraction'),
    'recession':            ('regime_label', 'risk_off_contraction'),

    # Central bank / Fed
    'fed tightening':       ('central_bank_stance', 'restrictive'),
    'fed restrictive':      ('central_bank_stance', 'restrictive'),
    'hawkish':              ('central_bank_stance', 'restrictive'),
    'restrictive':          ('central_bank_stance', 'restrictive'),
    'fed easing':           ('central_bank_stance', 'dovish'),
    'easing':               ('central_bank_stance', 'dovish'),
    'dovish':               ('central_bank_stance', 'dovish'),
    'fed cuts':             ('central_bank_stance', 'dovish'),
    'rate cuts':            ('central_bank_stance', 'dovish'),
    'fed hikes':            ('central_bank_stance', 'restrictive'),
    'rate hikes':           ('central_bank_stance', 'restrictive'),
    'hiking':               ('central_bank_stance', 'restrictive'),
    'cutting':              ('central_bank_stance', 'dovish'),
    'neutral fed':          ('central_bank_stance', 'neutral'),
    'neutral central bank': ('central_bank_stance', 'neutral'),

    # Geopolitical
    'geopolitical tension': ('gdelt_tension_level', 'high'),
    'geopolitical risk':    ('gdelt_tension_level', 'high'),
    'elevated tension':     ('gdelt_tension_level', 'high'),
    'high tension':         ('gdelt_tension_level', 'high'),
    'geopolitical calm':    ('gdelt_tension_level', 'low'),
    'low tension':          ('gdelt_tension_level', 'low'),

    # Sectors
    'tech stocks':          ('sector', 'technology'),
    'technology stocks':    ('sector', 'technology'),
    'tech sector':          ('sector', 'technology'),
    ' tech ':               ('sector', 'technology'),
    'technology':           ('sector', 'technology'),
    'energy stocks':        ('sector', 'energy'),
    'energy sector':        ('sector', 'energy'),
    ' energy ':             ('sector', 'energy'),
    'financials':           ('sector', 'financials'),
    'banks':                ('sector', 'financials'),
    'healthcare':           ('sector', 'healthcare'),
    'consumer':             ('sector', 'consumer_cyclical'),
    'utilities':            ('sector', 'utilities'),

    # Patterns
    'fvg':                  ('pattern_types_active', 'fvg'),
    'fair value gap':       ('pattern_types_active', 'fvg'),
    'order block':          ('pattern_types_active', 'order_block'),
    'breaker':              ('pattern_types_active', 'breaker'),
    'ifvg':                 ('pattern_types_active', 'ifvg'),

    # Conviction
    'high conviction':      ('conviction_tier', 'high'),
    'low conviction':       ('conviction_tier', 'low'),
    'medium conviction':    ('conviction_tier', 'medium'),

    # Price regime
    'near high':            ('price_regime', 'near_52w_high'),
    '52 week high':         ('price_regime', 'near_52w_high'),
    'near low':             ('price_regime', 'near_52w_low'),
    '52 week low':          ('price_regime', 'near_52w_low'),
    'mid range':            ('price_regime', 'mid_range'),
}


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class TemporalSearchResult:
    snapshot_at:     str
    subject:         str
    similarity:      float
    temporal_weight: float
    combined_score:  float
    state:           dict
    outcome_1w:      Optional[float] = None
    outcome_1m:      Optional[float] = None
    pattern_outcome: Optional[str]   = None


@dataclass
class TemporalSearchSummary:
    query_state:          dict
    match_count:          int
    avg_similarity:       float
    avg_outcome_1w:       Optional[float]
    avg_outcome_1m:       Optional[float]
    best_period:          str
    worst_outcome_period: str
    outcome_distribution: dict
    regime_breakdown:     dict
    top_matches:          List[TemporalSearchResult] = field(default_factory=list)


# ── NL parser ──────────────────────────────────────────────────────────────────

def parse_nl_query(query_text: str) -> dict:
    """
    Parse a natural language query into a state dict.
    Multi-word phrases are checked before single words so longer phrases win.
    """
    text_lower = ' ' + query_text.lower() + ' '
    state: dict = {}

    # Sort by phrase length descending (longer phrases matched first)
    for phrase, (feature, value) in sorted(_NL_STATE_MAP.items(), key=lambda x: -len(x[0])):
        if phrase in text_lower:
            # For list features (pattern_types_active), append; else first-wins
            if feature == 'pattern_types_active':
                existing = state.get('pattern_types_active', [])
                if value not in existing:
                    existing.append(value)
                state['pattern_types_active'] = existing
            elif feature not in state:
                state[feature] = value

    return state


# ── Similarity helpers (delegate to state_matcher to avoid duplication) ────────

def _snapshot_similarity(query_state: dict, snapshot_state: dict) -> float:
    """
    Compare query_state against a snapshot state dict.
    Uses state_matcher.state_similarity() which handles weighted fuzzy matching.
    Falls back to simple key-overlap if import fails.
    """
    try:
        from analytics.state_matcher import state_similarity
        # pattern_types_active is a list — flatten to first element for similarity
        q = dict(query_state)
        s = dict(snapshot_state)
        if isinstance(q.get('pattern_types_active'), list):
            pts = q.pop('pattern_types_active')
            if pts:
                q['pattern_type'] = pts[0]
        if isinstance(s.get('pattern_types_active'), list):
            pts = s.pop('pattern_types_active')
            if pts:
                s['pattern_type'] = pts[0]
        return state_similarity(q, s)
    except Exception:
        # Fallback: simple overlap ratio
        q_keys = set(query_state.keys())
        s_keys = set(snapshot_state.keys())
        shared = q_keys & s_keys
        if not shared:
            return 0.0
        matches = sum(1 for k in shared if str(query_state[k]).lower() == str(snapshot_state.get(k, '')).lower())
        return matches / len(shared)


def _temporal_weight_for(snapshot_at: str, half_life_months: float = 12.0) -> float:
    """Reuse temporal_weight from state_matcher."""
    try:
        from analytics.state_matcher import temporal_weight
        return temporal_weight(snapshot_at, half_life_months)
    except Exception:
        return 0.5


# ── Forward outcome computation ────────────────────────────────────────────────

def compute_forward_outcomes(
    subject: str,
    snapshot_at: str,
    db_path: str,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Look up what happened to subject's price after snapshot_at.
    Returns (return_1w, return_1m) as fractions (e.g. 0.034 = +3.4%).
    Returns (None, None) if OHLCV data is unavailable for the period.
    """
    from db import HAS_POSTGRES, get_pg
    if HAS_POSTGRES:
        try:
            with get_pg() as pg:
                cur = pg.cursor()
                cur.execute(
                    "SELECT close FROM ohlcv_cache WHERE ticker=%s AND interval='1d' AND ts <= %s ORDER BY ts DESC LIMIT 1",
                    (subject, snapshot_at))
                snap_row = cur.fetchone()
                if not snap_row: return None, None
                snap_price = snap_row['close']
                if not snap_price or snap_price <= 0: return None, None
                cur.execute(
                    "SELECT close FROM ohlcv_cache WHERE ticker=%s AND interval='1d' AND ts > %s ORDER BY ts ASC LIMIT 1 OFFSET 4",
                    (subject, snapshot_at))
                row_1w = cur.fetchone()
                return_1w = (row_1w['close'] - snap_price) / snap_price if row_1w and row_1w['close'] else None
                cur.execute(
                    "SELECT close FROM ohlcv_cache WHERE ticker=%s AND interval='1d' AND ts > %s ORDER BY ts ASC LIMIT 1 OFFSET 20",
                    (subject, snapshot_at))
                row_1m = cur.fetchone()
                return_1m = (row_1m['close'] - snap_price) / snap_price if row_1m and row_1m['close'] else None
                return return_1w, return_1m
        except Exception:
            pass  # fall through to SQLite
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            snap_row = conn.execute(
                """SELECT close FROM ohlcv_cache
                   WHERE ticker=? AND interval='1d' AND ts <= ?
                   ORDER BY ts DESC LIMIT 1""",
                (subject, snapshot_at),
            ).fetchone()
            if not snap_row:
                return None, None
            snap_price = snap_row[0]
            if not snap_price or snap_price <= 0:
                return None, None
            return_1w = None
            return_1m = None
            row_1w = conn.execute(
                """SELECT close FROM ohlcv_cache
                   WHERE ticker=? AND interval='1d' AND ts > ?
                   ORDER BY ts ASC LIMIT 1 OFFSET 4""",
                (subject, snapshot_at),
            ).fetchone()
            if row_1w and row_1w[0]:
                return_1w = (row_1w[0] - snap_price) / snap_price
            row_1m = conn.execute(
                """SELECT close FROM ohlcv_cache
                   WHERE ticker=? AND interval='1d' AND ts > ?
                   ORDER BY ts ASC LIMIT 1 OFFSET 20""",
                (subject, snapshot_at),
            ).fetchone()
            if row_1m and row_1m[0]:
                return_1m = (row_1m[0] - snap_price) / snap_price
            return return_1w, return_1m
        finally:
            conn.close()
    except Exception as exc:
        _log.debug('compute_forward_outcomes(%s, %s): %s', subject, snapshot_at, exc)
        return None, None


# ── Aggregation ────────────────────────────────────────────────────────────────

def _format_period(snapshot_at: str) -> str:
    """Convert ISO timestamp to 'Nov 2025' style."""
    try:
        dt = datetime.fromisoformat(snapshot_at.replace('Z', '+00:00'))
        return dt.strftime('%b %Y')
    except Exception:
        return snapshot_at[:7]


def _aggregate_results(
    matches: List[TemporalSearchResult],
    query_state: dict,
) -> TemporalSearchSummary:
    """Aggregate a list of scored matches into a summary."""
    if not matches:
        return TemporalSearchSummary(
            query_state={}, match_count=0, avg_similarity=0.0,
            avg_outcome_1w=None, avg_outcome_1m=None,
            best_period='', worst_outcome_period='',
            outcome_distribution={}, regime_breakdown={},
        )

    avg_sim = sum(m.similarity for m in matches) / len(matches)

    # Outcome averages (only from matches with data)
    w_outcomes_1w = [(m.outcome_1w, m.combined_score) for m in matches if m.outcome_1w is not None]
    w_outcomes_1m = [(m.outcome_1m, m.combined_score) for m in matches if m.outcome_1m is not None]

    avg_1w = None
    avg_1m = None
    if w_outcomes_1w:
        total_w = sum(w for _, w in w_outcomes_1w)
        avg_1w  = sum(o * w for o, w in w_outcomes_1w) / total_w if total_w > 0 else None
    if w_outcomes_1m:
        total_w = sum(w for _, w in w_outcomes_1m)
        avg_1m  = sum(o * w for o, w in w_outcomes_1m) / total_w if total_w > 0 else None

    # Outcome distribution
    pos = sum(1 for m in matches if m.outcome_1m is not None and m.outcome_1m > 0.005)
    neg = sum(1 for m in matches if m.outcome_1m is not None and m.outcome_1m < -0.005)
    flat = sum(1 for m in matches if m.outcome_1m is not None and -0.005 <= m.outcome_1m <= 0.005)
    no_data = sum(1 for m in matches if m.outcome_1m is None)
    total_with_data = pos + neg + flat
    outcome_dist: dict = {}
    if total_with_data > 0:
        outcome_dist = {
            'positive': round(pos / total_with_data, 3),
            'negative': round(neg / total_with_data, 3),
            'flat':     round(flat / total_with_data, 3),
            'no_data':  no_data,
        }

    # Best/worst period by 1m outcome
    best_period = ''
    worst_period = ''
    matches_with_1m = [m for m in matches if m.outcome_1m is not None]
    if matches_with_1m:
        best_match  = max(matches_with_1m, key=lambda m: m.outcome_1m)  # type: ignore[arg-type]
        worst_match = min(matches_with_1m, key=lambda m: m.outcome_1m)  # type: ignore[arg-type]
        best_period  = _format_period(best_match.snapshot_at)
        worst_period = _format_period(worst_match.snapshot_at)
    elif matches:
        # Fall back to highest-similarity period
        best_match = max(matches, key=lambda m: m.combined_score)
        best_period = _format_period(best_match.snapshot_at)

    # Regime breakdown
    regime_breakdown: Dict[str, dict] = {}
    for m in matches:
        regime = m.state.get('regime_label') or m.state.get('market_regime') or 'unknown'
        if regime not in regime_breakdown:
            regime_breakdown[regime] = {'count': 0, 'returns': []}
        regime_breakdown[regime]['count'] += 1
        if m.outcome_1m is not None:
            regime_breakdown[regime]['returns'].append(m.outcome_1m)
    # Compute avg return per regime
    for regime, data in regime_breakdown.items():
        rets = data.pop('returns', [])
        data['avg_return'] = round(sum(rets) / len(rets), 4) if rets else None

    # Top 5 by combined score
    top = sorted(matches, key=lambda m: m.combined_score, reverse=True)[:5]

    return TemporalSearchSummary(
        query_state          = query_state,
        match_count          = len(matches),
        avg_similarity       = round(avg_sim, 3),
        avg_outcome_1w       = round(avg_1w, 4) if avg_1w is not None else None,
        avg_outcome_1m       = round(avg_1m, 4) if avg_1m is not None else None,
        best_period          = best_period,
        worst_outcome_period = worst_period,
        outcome_distribution = outcome_dist,
        regime_breakdown     = regime_breakdown,
        top_matches          = top,
    )


# ── Main search class ──────────────────────────────────────────────────────────

class TemporalStateSearch:
    """
    Search engine for historical market states.

    Two entry points:
      search_for_ticker(ticker, query_text) — ticker-specific NL search
      search_by_natural_language(query_text) — cross-ticker NL search (global snapshots)
    """

    def __init__(self, db_path: str):
        self._db_path = db_path

    def search_similar_states(
        self,
        query_state: dict,
        subject: str,
        scope: str = 'ticker',
        limit: int = 200,
        similarity_threshold: float = 0.35,
    ) -> Optional[TemporalSearchSummary]:
        """
        Core search: find historical snapshots for subject/scope matching query_state.

        Parameters
        ----------
        query_state          : state dict to match against (same keys as snapshot JSON)
        subject              : ticker symbol (scope='ticker') or 'market' (scope='global')
        scope                : 'ticker' or 'global'
        limit                : max snapshots to load (default 200, ~50 days at 4/day)
        similarity_threshold : min similarity to include a match (default 0.35)
        """
        if not query_state:
            return None

        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            try:
                rows = conn.execute(
                    """SELECT snapshot_at, state_json
                       FROM market_state_snapshots
                       WHERE scope=? AND subject=?
                       ORDER BY snapshot_at DESC
                       LIMIT ?""",
                    (scope, subject, limit),
                ).fetchall()
            finally:
                conn.close()
        except Exception as exc:
            _log.debug('TemporalStateSearch.search_similar_states DB error: %s', exc)
            return None

        if not rows:
            _log.debug('TemporalStateSearch: no snapshots for %s/%s', scope, subject)
            return None

        matches: List[TemporalSearchResult] = []
        for snapshot_at, state_json in rows:
            try:
                state = json.loads(state_json)
            except Exception:
                continue

            sim = _snapshot_similarity(query_state, state)
            if sim < similarity_threshold:
                continue

            tw   = _temporal_weight_for(snapshot_at)
            combined = sim * tw

            r1w, r1m = compute_forward_outcomes(subject, snapshot_at, self._db_path)

            matches.append(TemporalSearchResult(
                snapshot_at    = snapshot_at,
                subject        = subject,
                similarity     = round(sim, 3),
                temporal_weight= round(tw, 3),
                combined_score = round(combined, 4),
                state          = state,
                outcome_1w     = round(r1w, 4) if r1w is not None else None,
                outcome_1m     = round(r1m, 4) if r1m is not None else None,
            ))

        if not matches:
            return None

        return _aggregate_results(matches, query_state)

    def search_for_ticker(
        self,
        ticker: str,
        query_text: str,
        similarity_threshold: float = 0.35,
    ) -> Optional[TemporalSearchSummary]:
        """
        Search historical snapshots for a specific ticker.
        Parses query_text into a state dict, then searches ticker-scoped snapshots.
        Also incorporates global state features from the NL parse.
        """
        query_state = parse_nl_query(query_text)

        # Enrich with any ticker-level features already in query (direction, vol, etc.)
        # The NL parse already handles this — no extra enrichment needed.

        if not query_state:
            # If we can't parse anything meaningful, use the current ticker state as query
            query_state = self._current_ticker_state(ticker)

        if not query_state:
            _log.debug('search_for_ticker(%s): could not build query_state', ticker)
            return None

        return self.search_similar_states(
            query_state          = query_state,
            subject              = ticker,
            scope                = 'ticker',
            similarity_threshold = similarity_threshold,
        )

    def search_by_natural_language(
        self,
        query_text: str,
        similarity_threshold: float = 0.35,
    ) -> Optional[TemporalSearchSummary]:
        """
        Cross-ticker NL search.
        Searches global market_state_snapshots (scope='global', subject='market').
        """
        query_state = parse_nl_query(query_text)
        if not query_state:
            return None

        return self.search_similar_states(
            query_state          = query_state,
            subject              = 'market',
            scope                = 'global',
            similarity_threshold = similarity_threshold,
        )

    def _current_ticker_state(self, ticker: str) -> dict:
        """Read the most recent snapshot state for a ticker from the DB."""
        try:
            conn = sqlite3.connect(self._db_path, timeout=5)
            try:
                row = conn.execute(
                    """SELECT state_json FROM market_state_snapshots
                       WHERE scope='ticker' AND subject=?
                       ORDER BY snapshot_at DESC LIMIT 1""",
                    (ticker,),
                ).fetchone()
            finally:
                conn.close()
            if row:
                return json.loads(row[0])
        except Exception:
            pass
        return {}

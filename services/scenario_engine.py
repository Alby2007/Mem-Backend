"""
services/scenario_engine.py — Read-only Scenario Testing Engine

Wraps causal_graph.traverse_causal() for on-demand "what-if" queries.
Never writes to the KB; all KB mutations remain in CausalShockEngine.

Usage
-----
from services.scenario_engine import run_scenario
result = run_scenario("if the Fed cuts rates", db_path=DB_PATH, narrative=False)

API contract
------------
run_scenario() returns a ScenarioResult dataclass.
chain_confidence is the GEOMETRIC MEAN of hop confidences (not arithmetic),
so a weak link in a long chain is properly penalised.

Seed resolution
---------------
_resolve_seed() accepts:
  1. Free-text via _SHOCK_ALIASES keyword matching (case-insensitive substring)
  2. Direct seed concept name (exact match against adjacency dict)
  3. Fallback: difflib edit-distance on alias keys for closest-match suggestions
"""

from __future__ import annotations

import math
import sqlite3
import time
from dataclasses import dataclass, field
from difflib import get_close_matches
from typing import Dict, List, Optional, Tuple


# ── Alias table — user-friendly phrases → causal graph seed concepts ───────────

_SHOCK_ALIASES: Dict[str, str] = {
    'fed rate hike':        'fed_rate_hike',
    'fed hike':             'fed_rate_hike',
    'rate hike':            'fed_rate_hike',
    'fed raises rates':     'fed_rate_hike',
    'rates rise':           'fed_rate_hike',
    'fed rate cut':         'fed_rate_cut',
    'fed cut':              'fed_rate_cut',
    'rate cut':             'fed_rate_cut',
    'fed cuts rates':       'fed_rate_cut',
    'rates fall':           'fed_rate_cut',
    'boe hike':             'boe_base_rate_hike',
    'boe raises rates':     'boe_base_rate_hike',
    'bank of england hike': 'boe_base_rate_hike',
    'boe cut':              'boe_base_rate_cut',
    'boe cuts rates':       'boe_base_rate_cut',
    'bank of england cut':  'boe_base_rate_cut',
    'oil spike':            'energy_prices_rise',
    'oil rises':            'energy_prices_rise',
    'energy prices rise':   'energy_prices_rise',
    'oil crash':            'energy_prices_fall',
    'oil falls':            'energy_prices_fall',
    'energy prices fall':   'energy_prices_fall',
    'inflation rises':      'inflation_rises',
    'inflation up':         'inflation_rises',
    'cpi rises':            'inflation_rises',
    'dollar strengthens':   'dollar_strengthens',
    'dollar strength':      'dollar_strengthens',
    'usd rises':            'dollar_strengthens',
    'dollar weakens':       'dollar_weakens',
    'dollar weakness':      'dollar_weakens',
    'usd falls':            'dollar_weakens',
    'credit spreads widen': 'hyg_spreads_widen',
    'credit stress':        'hyg_spreads_widen',
    'high yield widens':    'hyg_spreads_widen',
    'credit spreads tighten': 'hyg_spreads_tighten',
    'risk off':             'risk_off_rotation',
    'flight to safety':     'risk_off_rotation',
    'risk on':              'risk_on_rotation',
    'risk appetite':        'risk_on_rotation',
    'commodities rise':     'commodities_rise',
    'commodities fall':     'commodities_decline',
}

# ── Seed categories for GET /scenario/seeds ────────────────────────────────────

SEED_CATEGORIES: Dict[str, List[str]] = {
    'central_bank': [
        'fed_rate_hike',
        'fed_rate_cut',
        'boe_base_rate_hike',
        'boe_base_rate_cut',
    ],
    'energy': [
        'energy_prices_rise',
        'energy_prices_fall',
    ],
    'macro': [
        'inflation_rises',
        'dollar_strengthens',
        'dollar_weakens',
        'hyg_spreads_widen',
        'hyg_spreads_tighten',
        'commodities_rise',
        'commodities_decline',
    ],
    'sentiment': [
        'risk_off_rotation',
        'risk_on_rotation',
    ],
}


# ── Result dataclass ───────────────────────────────────────────────────────────

@dataclass
class ScenarioResult:
    shock_input:       str
    seed_concept:      Optional[str]
    chain:             List[dict]
    concepts_reached:  List[str]
    affected_tickers:  Dict[str, List[str]]
    portfolio_impact:  List[dict]
    chain_confidence:  float                # geometric mean of hop confidences
    narrative:         Optional[str]
    resolved:          bool
    unresolved_message: Optional[str]
    elapsed_ms:        float


# ── Seed resolution ────────────────────────────────────────────────────────────

def _resolve_seed(
    shock: str,
    db_path: Optional[str] = None,
) -> Tuple[Optional[str], List[str]]:
    """
    Map a free-text shock to a causal graph seed concept.

    Returns (seed_concept, suggestions):
      - seed_concept: resolved concept string, or None if unresolved
      - suggestions:  closest alias matches for unresolved_message

    Resolution order:
      1. Substring match against _SHOCK_ALIASES keys (case-insensitive)
      2. Direct match against known seed concepts (all category values)
      3. Unresolved → difflib get_close_matches on alias keys for suggestions
    """
    shock_lower = shock.lower().strip()

    # Strip leading connectives ("if the", "what if", "suppose", etc.)
    for prefix in ('what if ', 'if the ', 'if ', 'suppose ', 'assume ', 'when '):
        if shock_lower.startswith(prefix):
            shock_lower = shock_lower[len(prefix):]
            break

    # 1. Substring alias match
    for alias, concept in _SHOCK_ALIASES.items():
        if alias in shock_lower:
            return concept, []

    # 2. Direct concept name match (underscore form)
    shock_underscored = shock_lower.replace(' ', '_')
    all_seeds = {c for cats in SEED_CATEGORIES.values() for c in cats}
    if shock_underscored in all_seeds:
        return shock_underscored, []

    # 3. Also check alias values directly
    if shock_lower in _SHOCK_ALIASES.values():
        return shock_lower, []

    # 4. Unresolved — suggest closest alias keys via edit distance
    suggestions_raw = get_close_matches(shock_lower, _SHOCK_ALIASES.keys(), n=3, cutoff=0.4)
    # Map suggestions to their seed concepts for the message
    suggestion_concepts = list(dict.fromkeys(
        _SHOCK_ALIASES[s] for s in suggestions_raw
    ))

    return None, suggestion_concepts


# ── Geometric mean confidence ──────────────────────────────────────────────────

def _geometric_mean(values: List[float]) -> float:
    """Geometric mean of a list of floats. Returns 1.0 for empty list."""
    if not values:
        return 1.0
    log_sum = sum(math.log(max(v, 1e-9)) for v in values)
    return math.exp(log_sum / len(values))


# ── Portfolio impact filtering ─────────────────────────────────────────────────

def _filter_portfolio_impact(
    affected_tickers: Dict[str, List[str]],
    chain: List[dict],
    portfolio_tickers: List[str],
) -> List[dict]:
    """
    Filter affected_tickers to the user's holdings.
    Returns sorted-by-confidence list of dicts.
    """
    if not portfolio_tickers:
        return []

    port_upper = {t.upper() for t in portfolio_tickers}

    # Build concept → best chain hop confidence
    concept_conf: Dict[str, float] = {}
    for hop in chain:
        eff = hop['effect']
        conf = hop['confidence']
        if eff not in concept_conf or conf > concept_conf[eff]:
            concept_conf[eff] = conf
        mech = hop.get('mechanism', '')
        concept_conf[(eff, hop['cause'], mech)] = conf

    results: List[dict] = []
    seen: set = set()

    for concept, tickers in affected_tickers.items():
        conf = concept_conf.get(concept, 0.5)
        # Find the hop that produced this concept for mechanism
        mechanism = ''
        for hop in chain:
            if hop['effect'] == concept:
                mechanism = hop.get('mechanism', '')
                break

        for ticker in tickers:
            if ticker.upper() in port_upper and ticker not in seen:
                seen.add(ticker)
                results.append({
                    'ticker':       ticker,
                    'via_concept':  concept,
                    'mechanism':    mechanism,
                    'confidence':   round(conf, 3),
                })

    results.sort(key=lambda x: x['confidence'], reverse=True)
    return results


# ── Main entry point ───────────────────────────────────────────────────────────

def run_scenario(
    shock: str,
    db_path: str,
    max_depth: int = 4,
    min_confidence: float = 0.5,
    portfolio_tickers: Optional[List[str]] = None,
    narrative: bool = False,
    trader_level: str = 'developing',
) -> ScenarioResult:
    """
    Run a read-only causal scenario from a free-text or concept-name shock.

    Parameters
    ----------
    shock               Free-text ("if the Fed cuts rates") or concept name
    db_path             Path to SQLite KB
    max_depth           BFS depth (default 4)
    min_confidence      Minimum edge confidence to traverse (default 0.5)
    portfolio_tickers   User's holdings — used to compute portfolio_impact
    narrative           If True, call LLM to generate a 3-sentence narrative
    trader_level        Passed to build_prompt for tone calibration

    Returns
    -------
    ScenarioResult
    """
    t0 = time.perf_counter()

    seed_concept, suggestions = _resolve_seed(shock, db_path)

    # Unresolved seed
    if seed_concept is None:
        elapsed = (time.perf_counter() - t0) * 1000
        if suggestions:
            unresolved_msg = (
                f"Unrecognised shock \u2014 did you mean: "
                + ", ".join(f"'{s}'" for s in suggestions)
                + "? Try one of the seeds from GET /scenario/seeds."
            )
        else:
            unresolved_msg = (
                "Unrecognised shock. Try GET /scenario/seeds for valid inputs."
            )
        return ScenarioResult(
            shock_input=shock,
            seed_concept=None,
            chain=[],
            concepts_reached=[],
            affected_tickers={},
            portfolio_impact=[],
            chain_confidence=0.0,
            narrative=None,
            resolved=False,
            unresolved_message=unresolved_msg,
            elapsed_ms=round(elapsed, 1),
        )

    # Traverse causal graph
    from knowledge.causal_graph import traverse_causal
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        result = traverse_causal(
            conn, seed_concept,
            max_depth=max_depth,
            min_confidence=min_confidence,
        )
    finally:
        conn.close()

    chain            = result.get('chain', [])
    concepts_reached = result.get('concepts_reached', [])
    affected_tickers = result.get('affected_tickers', {})

    # Geometric mean of all hop confidences (not the greedy-path product)
    hop_confidences = [hop['confidence'] for hop in chain]
    chain_confidence = round(_geometric_mean(hop_confidences), 4)

    # Portfolio impact
    portfolio_impact = _filter_portfolio_impact(
        affected_tickers, chain, portfolio_tickers or []
    )

    # Optional LLM narrative
    narr: Optional[str] = None
    if narrative:
        try:
            import extensions as ext  # type: ignore
            if ext.HAS_LLM or (ext.HAS_GROQ and ext.groq_available and ext.groq_available()):
                sup_str = '; '.join(
                    f"{h['cause']} → {h['effect']} ({h['mechanism']})"
                    for h in chain[:5]
                )
                port_str = (
                    ', '.join(f"{p['ticker']} via {p['via_concept']}" for p in portfolio_impact[:3])
                    if portfolio_impact else 'none in portfolio'
                )
                snippet = (
                    f"SCENARIO:\n"
                    f"shock: {shock}\n"
                    f"seed_concept: {seed_concept}\n"
                    f"causal_chain (first 5 hops): {sup_str}\n"
                    f"affected_tickers (sample): "
                    + ', '.join(list(affected_tickers.keys())[:5])
                    + f"\nportfolio_impact: {port_str}\n"
                    f"chain_confidence (geometric mean): {chain_confidence:.4f}\n"
                )
                messages = ext.build_prompt(
                    user_message=f"Explain the market impact of this scenario: {shock}",
                    snippet=snippet,
                    briefing_mode='scenario',
                    atom_count=len(chain),
                    trader_level=trader_level,
                )
                narr = ext.llm_chat(messages)
        except Exception:
            narr = None

    elapsed = (time.perf_counter() - t0) * 1000

    return ScenarioResult(
        shock_input=shock,
        seed_concept=seed_concept,
        chain=chain,
        concepts_reached=concepts_reached,
        affected_tickers=affected_tickers,
        portfolio_impact=portfolio_impact,
        chain_confidence=chain_confidence,
        narrative=narr,
        resolved=True,
        unresolved_message=None,
        elapsed_ms=round(elapsed, 1),
    )

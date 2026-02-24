"""
knowledge/kb_domain_schemas.py — Domain predicate schemas (ontology layer) — Trading KB

Handcrafted, intentional. Not derived statistically.
Each domain defines the predicate vocabulary that SHOULD exist for topics
of that type. Used by:
  - kb_repair_proposals.py  → introduce_predicates schema completion
  - kb_insufficiency_classifier.py → qualitative MISSING_SCHEMA detection

Design invariants:
  - No imports from other knowledge/ modules (no circular risk)
  - Predicates here are PROPOSED only — never auto-adopted
  - Adoption is human-gated via predicate_vocabulary.status = 'proposed'
  - Adding a new domain here automatically propagates to both classifier and executor
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional


# ── Domain predicate schemas ──────────────────────────────────────────────────
#
# Ordered: most evaluative/semantic first within each domain.
# The first 5 are proposed when introduce_predicates fires (Path A).

DOMAIN_PREDICATE_SCHEMAS: Dict[str, List[str]] = {
    'trading_instrument': [
        'has_ticker',
        'signal_direction',
        'signal_confidence',
        'signal_source',
        'time_horizon',
        'sector',
        'price_target',
        'catalyst',
        'risk_factor',
        'invalidation_condition',
        'correlation_to',
        'liquidity_profile',
        'volatility_regime',
    ],
    'market_thesis': [
        'premise',
        'supporting_evidence',
        'contradicting_evidence',
        'entry_condition',
        'exit_condition',
        'invalidated_by',
        'time_horizon',
        'confidence_level',
        'risk_reward_ratio',
        'position_sizing_note',
    ],
    'macro_regime': [
        'regime_label',
        'dominant_driver',
        'asset_class_bias',
        'sector_rotation',
        'risk_on_off',
        'central_bank_stance',
        'inflation_environment',
        'growth_environment',
    ],
    'research_report': [
        'publisher',
        'analyst',
        'rating',
        'price_target',
        'key_finding',
        'compared_to_consensus',
        'dataset_used',
        'time_horizon',
    ],
    'company': [
        'sector',
        'market_cap_tier',
        'revenue_trend',
        'earnings_quality',
        'management_assessment',
        'competitive_moat',
        'debt_profile',
        'catalyst',
    ],
    'concept': [
        'defined_as',
        'related_to',
        'contrasts_with',
        'example_of',
        'used_in',
    ],
}


# ── Domain detection ──────────────────────────────────────────────────────────

def detect_topic_domain(topic: str, atoms: List[Dict]) -> Optional[str]:
    """
    Deterministically classify a topic into a domain type.
    No LLM. Uses structural signals from the topic string and atom predicates.

    Returns a key from DOMAIN_PREDICATE_SCHEMAS, or None if unclassifiable.

    Detection priority:
      1. Ticker pattern (1-5 uppercase letters, or exchange:ticker)
      2. Atom predicate evidence (trading predicates already present)
      3. Topic string keyword heuristics
    """
    # 1. Ticker pattern — e.g. 'AAPL', 'BTC', 'NYSE:TSLA'
    if re.match(r'^[A-Z]{1,5}$', topic) or re.match(r'^[\w]+:[A-Z]{1,5}$', topic):
        return 'trading_instrument'

    # 2. Atom predicate evidence
    if atoms:
        preds = {(a.get('predicate') or '').lower() for a in atoms}
        _INSTRUMENT_PREDS = {'has_ticker', 'signal_direction', 'signal_confidence',
                             'price_target', 'invalidation_condition', 'volatility_regime',
                             'catalyst', 'risk_factor', 'time_horizon'}
        _THESIS_PREDS = {'premise', 'supporting_evidence', 'contradicting_evidence',
                         'entry_condition', 'exit_condition', 'invalidated_by'}
        _MACRO_PREDS = {'regime_label', 'dominant_driver', 'asset_class_bias',
                        'central_bank_stance', 'risk_on_off'}
        _COMPANY_PREDS = {'market_cap_tier', 'earnings_quality', 'competitive_moat',
                          'revenue_trend', 'debt_profile'}
        _REPORT_PREDS = {'rating', 'compared_to_consensus', 'analyst', 'publisher'}
        if preds & _INSTRUMENT_PREDS:
            return 'trading_instrument'
        if preds & _THESIS_PREDS:
            return 'market_thesis'
        if preds & _MACRO_PREDS:
            return 'macro_regime'
        if preds & _COMPANY_PREDS:
            return 'company'
        if preds & _REPORT_PREDS:
            return 'research_report'

    # 3. Topic string keyword heuristics
    t = topic.lower()
    if any(kw in t for kw in ('thesis', 'trade idea', 'setup', 'position', 'long', 'short')):
        return 'market_thesis'
    if any(kw in t for kw in ('macro', 'regime', 'fed', 'rates', 'inflation', 'gdp', 'recesssion')):
        return 'macro_regime'
    if any(kw in t for kw in ('earnings', 'revenue', 'ebitda', 'guidance', 'beat', 'miss')):
        return 'company'
    if any(kw in t for kw in ('report', 'research', 'note', 'initiate', 'upgrade', 'downgrade')):
        return 'research_report'

    return None


# ── Semantic validation constants ─────────────────────────────────────────────
#
# Used by kb_validation.py Layer 2 (semantic validation).
# Constraints are per-predicate value bounds. Antonym signals detect gross
# intra-topic contradictions without embeddings.

PREDICATE_VALUE_CONSTRAINTS: Dict[str, Dict] = {
    'signal_direction':       {'allowed_values': ['long', 'short', 'neutral', 'bullish', 'bearish',
                                                   'near_high', 'mid_range', 'near_low',
                                                   'strong_uptrend', 'weak_uptrend',
                                                   'strong_downtrend', 'weak_downtrend']},
    'signal_confidence':      {'min_length': 1,  'max_length': 20},
    'time_horizon':           {'min_length': 2,  'max_length': 50},
    'price_target':           {'min_length': 1,  'max_length': 50},
    'invalidation_condition': {'min_length': 10, 'max_length': 400},
    'premise':                {'min_length': 15, 'max_length': 500},
    'supporting_evidence':    {'min_length': 10, 'max_length': 500},
    'contradicting_evidence': {'min_length': 10, 'max_length': 500},
    'entry_condition':        {'min_length': 5,  'max_length': 300},
    'exit_condition':         {'min_length': 5,  'max_length': 300},
    'risk_reward_ratio':      {'min_length': 2,  'max_length': 30},
    'regime_label':           {'min_length': 3,  'max_length': 100},
    'rating':                 {'allowed_values': ['buy', 'sell', 'hold', 'overweight', 'underweight', 'neutral']},
    'risk_on_off':            {'allowed_values': ['risk_on', 'risk_off', 'neutral']},
}

# Antonym keyword pairs for intra-topic contradiction detection.
# Format: {(pred_a, pred_b): [(kw_set_in_a, kw_set_in_b), ...]}
# A contradiction fires when pred_a's object contains kw_set_a AND pred_b's
# object contains kw_set_b (or vice versa).
_ANTONYM_SIGNALS: Dict = {
    ('signal_direction', 'signal_direction'): [
        ({'long', 'bullish', 'buy'},
         {'short', 'bearish', 'sell'}),
    ],
    ('supporting_evidence', 'contradicting_evidence'): [
        ({'strong', 'confirmed', 'clear', 'high conviction'},
         {'weak', 'conflicting', 'unclear', 'mixed'}),
    ],
    ('entry_condition', 'invalidation_condition'): [
        ({'above', 'breakout', 'momentum', 'trend continuing'},
         {'below', 'breakdown', 'reversal', 'trend failing'}),
    ],
    ('risk_on_off', 'asset_class_bias'): [
        ({'risk_on', 'risk on'},
         {'defensive', 'safe haven', 'bonds', 'gold'}),
        ({'risk_off', 'risk off'},
         {'equities', 'growth', 'cyclical', 'high beta'}),
    ],
}


def missing_schema_predicates(domain: str, existing_preds: set) -> List[str]:
    """
    Return the required predicates for a domain that are absent from existing_preds.
    Ordered as defined in DOMAIN_PREDICATE_SCHEMAS (most evaluative first).
    """
    schema = DOMAIN_PREDICATE_SCHEMAS.get(domain, [])
    return [p for p in schema if p not in existing_preds]


def schema_completeness(domain: str, existing_preds: set) -> float:
    """
    Fraction of required domain predicates that are present (0.0 = none, 1.0 = complete).
    Returns 1.0 if domain is unknown (no schema to check against).
    """
    schema = DOMAIN_PREDICATE_SCHEMAS.get(domain, [])
    if not schema:
        return 1.0
    present = sum(1 for p in schema if p in existing_preds)
    return present / len(schema)

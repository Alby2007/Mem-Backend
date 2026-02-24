"""
knowledge/authority.py — Source Authority Weights (Trading KB)

Assigns epistemic authority to KB atoms based on their source prefix.
Authority reflects how curated, stable, and trustworthy a source is
for trading and market intelligence use cases.

Used in retrieval re-ranking:
    effective_score = confidence_effective × authority

This is NOT a retrieval filter — all atoms remain retrievable.
Authority only affects final ranking within a retrieved set.

Zero-LLM, pure Python, <1ms per call.
"""

from __future__ import annotations
from typing import Dict


# ── Authority table ────────────────────────────────────────────────────────────
# Scale: 0.0 (untrusted) → 1.0 (authoritative)
# Prefix-matched: longest prefix wins.
# Default for unknown sources: 0.5

_AUTHORITY_TABLE: Dict[str, float] = {
    # Primary market data — direct, unmediated
    'exchange_feed':       1.0,   # direct exchange price/volume/OI data
    'regulatory_filing':   0.95,  # SEC/FCA/ESMA filings, official disclosures

    # Curated research & analysis
    'curated_':            0.90,  # hand-authored facts by the analyst team
    'broker_research':     0.80,  # institutional research reports
    'macro_data':          0.80,  # central bank, government macro releases
    'earnings_':           0.85,  # verified earnings/guidance data

    # Derived / model outputs
    'model_signal_':       0.70,  # quantitative model signals
    'cross_asset_gnn':     0.70,  # GNN-derived cross-asset relationships
    'derived_signal_':     0.65,  # second-order signals computed over KB atoms
    'technical_':          0.65,  # technical analysis indicators

    # External scraped
    'news_wire_':          0.60,  # Reuters, Bloomberg wire feeds
    'alt_data_':           0.55,  # alternative data (satellite, web-scrape)

    # Low-signal / noisy
    'social_signal_':      0.35,  # Twitter/Reddit/StockTwits sentiment
    'unverified_':         0.30,  # unverified or anonymous sources
}

_DEFAULT_AUTHORITY = 0.5


def get_authority(source: str) -> float:
    """
    Return authority weight for a given source string.

    Matches by longest prefix. Falls back to _DEFAULT_AUTHORITY.

    Examples:
        get_authority('exchange_feed')          → 1.0
        get_authority('broker_research')        → 0.8
        get_authority('news_wire_reuters')      → 0.6
        get_authority('social_signal_reddit')   → 0.35
        get_authority('unknown_source')         → 0.5
    """
    if not source:
        return _DEFAULT_AUTHORITY

    best_match = ''
    best_authority = _DEFAULT_AUTHORITY

    for prefix, authority in _AUTHORITY_TABLE.items():
        if source.startswith(prefix) and len(prefix) > len(best_match):
            best_match = prefix
            best_authority = authority

    return best_authority


def effective_score(fact: dict) -> float:
    """
    Composite retrieval score for a fact dict.

    Uses confidence_effective if available (decay-adjusted), otherwise confidence.
    Multiplied by source authority.

    Score is in [0, 1] — higher = more epistemically trustworthy.

    Args:
        fact: dict with at least 'confidence' and 'source' keys.
              Optionally 'confidence_effective' (from decay worker).
    """
    base_conf = fact.get('confidence_effective') or fact.get('confidence', 0.5)
    authority = get_authority(fact.get('source', ''))
    return float(base_conf) * authority


def conflict_winner(fact_a: dict, fact_b: dict) -> dict:
    """
    Given two conflicting atoms (same subject + predicate, different object),
    return the one that should survive.

    Resolution policy:
      Primary:   epistemic strength = effective_score() — confidence × authority
      Secondary: recency (tiebreaker, only within same source class)

    Never pure recency — that would make the graph reactive instead of governed.
    """
    score_a = effective_score(fact_a)
    score_b = effective_score(fact_b)

    # Primary: epistemic strength
    if abs(score_a - score_b) > 0.05:  # meaningful difference
        return fact_a if score_a >= score_b else fact_b

    # Secondary: recency tiebreaker (same source class only)
    src_a = fact_a.get('source', '')
    src_b = fact_b.get('source', '')
    auth_a = get_authority(src_a)
    auth_b = get_authority(src_b)

    if abs(auth_a - auth_b) < 0.05:
        # Same source class — recency wins
        ts_a = fact_a.get('timestamp', '')
        ts_b = fact_b.get('timestamp', '')
        return fact_a if ts_a >= ts_b else fact_b

    # Different source classes with near-equal scores — higher authority wins
    return fact_a if auth_a >= auth_b else fact_b

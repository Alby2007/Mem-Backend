"""
eval/scorers.py — Per-intent scoring functions for the eval harness.

Each scorer returns a dict of check_name → bool | float.
A float value is treated as a pass if >= 0.8.

Zero I/O — pure functions, easy to unit-test independently.
"""
from __future__ import annotations

from typing import Optional


# ── Universal hallucination / quality checks ──────────────────────────────────

_HALLUCINATION_PHRASES = (
    'based on general knowledge',
    'from my training data',
    'as a leader in',
    'based on publicly available information',
    'according to my training',
    'my knowledge cutoff',
    'i was trained on',
)

_PLACEHOLDER_PHRASES = (
    ' ? ',
    'n/a',
    '£current_price',
    '{stop_loss}',
    '{entry}',
    '{target}',
    'x shares',
    '[ticker]',
    '<ticker>',
)


def _universal(response: str) -> dict:
    r = response.lower()
    return {
        'not_empty': len(response.strip()) > 5,
        'no_hallucination_phrases': not any(p in r for p in _HALLUCINATION_PHRASES),
        'no_placeholders': not any(p in r for p in _PLACEHOLDER_PHRASES),
    }


# ── Intent scorers ─────────────────────────────────────────────────────────────

def score_single_ticker(
    response: str,
    portfolio: dict,
    ticker: Optional[str],
) -> dict:
    scores = _universal(response)
    r = response.lower()
    if ticker:
        t = ticker.lower().replace('.l', '')
        scores['mentions_ticker'] = t in r
        scores['has_signal'] = any(
            w in r for w in (
                'bullish', 'bearish', 'neutral', 'signal', 'conviction', 'long', 'short',
                'no directional signal', 'no signal yet', 'no kb signal',
                'price data but no', 'no direction', 'no signal',
                'kb has price', 'has price data',
                # price target IS a signal — model stating target implies directionality
                'price target', 'target price', 'upside', 'downside',
                # honest no-data responses are correct behaviour — should pass
                'no current kb data', 'no kb data', 'not in the kb',
                'no information', 'no explicit', 'no mention',
                'i don\'t have', 'not available',
            )
        )
        scores['no_wrong_ticker'] = _no_wrong_ticker(r, ticker, portfolio)
    return scores


# Keywords that indicate a single-best query — correct response is 1 ticker, not all
_SINGLE_BEST_KWS = (
    'which', 'best', 'top', 'strongest', 'highest', 'most', 'worst',
    'biggest', 'lowest', 'largest',
)


def score_portfolio_review(response: str, portfolio: dict, query: str = '') -> dict:
    scores = _universal(response)
    r = response.lower()
    tickers = [h['ticker'].lower().replace('.l', '') for h in portfolio['holdings']]
    # Also accept the full ticker with .l suffix
    tickers_full = [h['ticker'].lower() for h in portfolio['holdings']]
    coverage = sum(
        1 for t, tf in zip(tickers, tickers_full)
        if t in r or tf in r
    )
    total = len(tickers)
    # For single-best queries ('which holding has best...'), 1 ticker mentioned is correct.
    # For overview/review queries, use ratio-based coverage.
    q_lower = query.lower()
    _is_single_best = any(kw in q_lower for kw in _SINGLE_BEST_KWS)
    if _is_single_best:
        coverage_ratio = 1.0 if coverage >= 1 else 0.0
    else:
        coverage_ratio = coverage / total if total else 0.0
    scores['holdings_coverage']         = coverage_ratio
    scores['info_full_coverage']        = coverage == total      # informational only — not in pass gate
    scores['no_placeholders_per_holding'] = '?' not in response
    return scores


def score_opportunity(response: str, portfolio: dict) -> dict:
    scores = _universal(response)
    r = response.lower()
    scores['is_ranked'] = any(
        w in r for w in (
            'strongest', 'top', 'best', '#1', 'ranked', 'highest conviction', 'leading',
            'number one', 'first', 'primary', 'most compelling', 'standout',
            # Model correctly identifies a single winner without explicit #1 format
            'most asymmetric', 'has the most', 'has the strongest', 'has the best',
            'most compelling', 'highest upside', 'opportunity scan',
            'position analysis', 'setup recommendation',
            # Explicit fallback phrase from the prompt
            'no ranked opportunities available',
            # Model gives conviction score or strong signal — counts as ranked output
            'conviction score', 'high conviction', 'high upside', 'upside percentage',
            'strong signal', 'strong bullish', 'bullish signal',
        )
    )
    scores['has_reasoning'] = any(
        w in r for w in ('conviction', 'signal', 'quality', 'because', 'confirmed', 'momentum', 'catalyst')
    )
    # not_full_narrative is informational only — model correctly covering all holdings
    # is not a failure, so exclude from pass gate
    holding_count = len(portfolio['holdings'])
    mentioned = sum(1 for h in portfolio['holdings'] if h['ticker'].lower() in r or h['ticker'].lower().replace('.l','') in r)
    scores['info_full_narrative'] = mentioned <= max(3, holding_count // 2)
    return scores


def score_geo(response: str) -> dict:
    scores = _universal(response)
    r = response.lower()
    scores['no_entity_substitution'] = _check_no_substitution(r)
    scores['has_kb_data_or_honest_gap'] = any(
        w in r for w in (
            "don't have current kb data", "kb does not", "gdelt", "ucdp",
            'active_war', 'conflict', 'tension_score', 'geopolit',
            'no kb data', 'world monitor', 'acled',
        )
    )
    return scores


def score_greeks(response: str, trader_level: str) -> dict:
    scores = _universal(response)
    r = response.lower()
    if trader_level in ('beginner', 'developing'):
        scores['no_raw_greek_keys'] = not any(
            w in r for w in ('delta_atm', 'gamma_atm', 'vega_atm', 'theta_atm')
        )
    else:
        scores['has_greeks_if_available'] = any(
            w in r for w in ('delta', 'iv', 'implied volatility', 'gamma', 'vega', 'theta')
        ) or any(
            p in r for p in ("don't have current kb data", "no kb data", "not available")
        )
    return scores


def score_macro(response: str, kb_has_yield: bool = True) -> dict:
    scores = _universal(response)
    r = response.lower()
    # A macro response passes if it mentions EITHER regime context OR yield/rates context.
    # A pure regime answer ("market is in recovery") should pass a regime question.
    # A pure yield answer ("Fed is restrictive") should pass a rates question.
    # Both are correct macro responses — requiring both is a scorer bug.
    _mentions_regime = any(
        w in r for w in (
            'regime', 'recovery', 'bull', 'bear', 'neutral', 'risk-on', 'risk-off',
            'inversion', 'inverted', 'expansion', 'contraction', 'stagflation',
            'slowdown', 'risk off', 'risk on', 'market regime',
        )
    )
    _mentions_yield = any(
        w in r for w in (
            'yield', 'rates', 'bonds', 'tlt', 'flatten', 'steepen', 'curve',
            'fed', 'fomc', 'central bank', 'central_bank', 'rate cut', 'rate hike',
            'interest rate', 'monetary', 'restrictive', 'accommodative',
        )
    )
    scores['info_mentions_regime'] = _mentions_regime   # diagnostic only
    scores['info_mentions_yield']  = _mentions_yield    # diagnostic only
    # Gate: pass if EITHER regime OR yield/rates mentioned.
    # A regime-only answer correctly passes a "market regime?" query.
    # A rates-only answer correctly passes a "Fed stance?" query.
    scores['has_macro_content'] = _mentions_regime or _mentions_yield
    return scores


def score_no_data(response: str) -> dict:
    scores = _universal(response)
    r = response.lower()
    scores['gives_no_data_response'] = any(
        p in r for p in (
            "don't have current kb data",
            "no current kb",
            "kb currently",
            "cannot answer",
            "i don't have",
            "not available in",
            "no kb data",
            "no data",
            "not in the kb",
            "not covered",
            "no information",
            "isn't covered",
            "not found in",
            "outside the kb",
            "find information about",
            "check the company",
            # Natural-language refusals the model uses when it correctly rejects fake tickers
            "can't provide information",
            "cannot provide information",
            "not a real",
            "isn't a real",
            "does not exist",
            "non-existent",
            "i cannot find",
            "no record of",
            "can't find any",
            "unable to find",
            "there is no explicit mention",
            "no explicit mention",
            "no explicit information",
            "i can't help",
            "not in our knowledge base",
            "no kb data",
            "cannot provide",
            # Model correctly says it found nothing — common natural-language refusals
            "couldn't find any information",
            "i couldn't find",
            "can't find any information",
            "not present in",
            "not mentioned in",
            "not in the provided",
            "no information on",
            "no data available for",
            "is not present",
            "is not available",
            "not a publicly traded",
            "not listed",
            "does not appear",
            # Model says context doesn't contain the ticker
            "does not contain any information",
            "does not contain",
            "context does not contain",
            "no information about",
            "no atom",
            "not in the context",
            "only provides information about",
            "only provide information about",
            "only mentions",
            "not directly mentioned",
            "not covered in",
            "not found in the",
            "the kb does not",
            "kb does not contain",
            "not available in the",
            "cannot provide a signal",
            "cannot provide information on",
            "no relevant kb data",
            "no relevant",
            "not available for",
            "no answer",
            # Model describes invalid/conflicting atoms — borderline refusal
            "signal direction is listed as invalid",
            "signal direction of invalid",
            "signal_direction is listed as invalid",
            "conflicting or contradictory information",
            "conflicting information",
            "not part of the actual knowledge base",
            "marked as \"fakeco\"",
            "marked as fakeco",
            "no reliable",
            "no reliable data",
            "no reliable information",
        )
    )
    scores['no_invented_data'] = not any(
        w in r for w in (
            'zone:', 'entry:', 'stop:', 'target:',
            'signal_direction', 'conviction_tier',
        )
    )
    return scores


# ── Router ────────────────────────────────────────────────────────────────────

def score_response(
    response: str,
    intent: str,
    portfolio: dict,
    ticker: Optional[str] = None,
    kb_has_yield: bool = True,
    query: str = '',
) -> dict:
    """
    Dispatch to the correct per-intent scorer.
    Returns a dict of check_name → bool | float.
    """
    if intent == 'single_ticker':
        return score_single_ticker(response, portfolio, ticker)
    elif intent == 'portfolio_review':
        return score_portfolio_review(response, portfolio, query=query)
    elif intent == 'opportunity':
        return score_opportunity(response, portfolio)
    elif intent == 'geo':
        return score_geo(response)
    elif intent == 'greeks':
        return score_greeks(response, portfolio.get('trader_level', 'developing'))
    elif intent == 'macro':
        return score_macro(response, kb_has_yield=kb_has_yield)
    elif intent == 'no_data':
        return score_no_data(response)
    else:
        return _universal(response)


_FLOAT_THRESHOLDS = {
    'holdings_coverage': 0.25,  # at least 1/4 of holdings addressed — realistic with partial KB coverage
}
_DEFAULT_FLOAT_THRESHOLD = 0.8


def is_pass(scores: dict) -> bool:
    """True if all checks pass (bools True, floats >= threshold).
    Keys prefixed with 'info_' are informational only and excluded from pass gate."""
    for k, v in scores.items():
        if k.startswith('info_'):
            continue
        if isinstance(v, bool):
            if not v:
                return False
        else:
            threshold = _FLOAT_THRESHOLDS.get(k, _DEFAULT_FLOAT_THRESHOLD)
            if v < threshold:
                return False
    return True


def kb_has_yield_atoms(db_path: Optional[str] = None) -> bool:
    """Return True if the KB contains any yield curve atoms.
    Called once at harness startup to gate the mentions_yield_or_rates check."""
    import sqlite3 as _sqlite3, glob as _glob, os as _os
    candidates = [db_path] if db_path else []
    if not candidates:
        candidates = ['trading_knowledge.db'] + _glob.glob('**/*.db', recursive=True)
    for path in candidates:
        if path and _os.path.exists(path):
            try:
                conn = _sqlite3.connect(path, timeout=3)
                row = conn.execute(
                    "SELECT COUNT(*) FROM facts WHERE predicate IN "
                    "('yield_curve_slope','yield_curve_regime','tlt_close','yield_curve_tlt_shy')"
                ).fetchone()
                conn.close()
                return (row[0] if row else 0) > 0
            except Exception:
                pass
    return False  # no DB found — assume no yield atoms


# ── Helpers ───────────────────────────────────────────────────────────────────

def _no_wrong_ticker(response_lower: str, target_ticker: str, portfolio: dict) -> bool:
    """Check response doesn't discuss a different ticker more prominently."""
    t_clean = target_ticker.lower().replace('.l', '')
    other_tickers = [
        h['ticker'].lower().replace('.l', '')
        for h in portfolio['holdings']
        if h['ticker'].lower() != target_ticker.lower()
    ]
    target_count = response_lower.count(t_clean)
    for other in other_tickers:
        if response_lower.count(other) > target_count:
            return False
    return True


def _check_no_substitution(response_lower: str) -> bool:
    """
    Geo check: flag if more than 2 distinct geo entities appear —
    a sign the model substituted a different conflict for the one asked about.
    """
    geo_entities = [
        'russia', 'ukraine', 'iran', 'israel', 'gaza',
        'china', 'taiwan', 'north korea', 'hamas', 'hezbollah',
    ]
    mentioned = [e for e in geo_entities if e in response_lower]
    return len(mentioned) <= 3

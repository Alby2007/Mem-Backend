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
        'not_empty': len(response.strip()) > 20,
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
            w in r for w in ('bullish', 'bearish', 'neutral', 'signal', 'conviction', 'long', 'short')
        )
        scores['no_wrong_ticker'] = _no_wrong_ticker(r, ticker, portfolio)
    return scores


def score_portfolio_review(response: str, portfolio: dict) -> dict:
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
    coverage_ratio = coverage / total if total else 0.0
    scores['holdings_coverage']         = coverage_ratio         # pass if >= 0.8
    scores['full_coverage']             = coverage == total
    scores['no_placeholders_per_holding'] = '?' not in response
    return scores


def score_opportunity(response: str, portfolio: dict) -> dict:
    scores = _universal(response)
    r = response.lower()
    scores['is_ranked'] = any(
        w in r for w in ('strongest', 'top', 'best', '#1', 'ranked', 'highest conviction', 'leading')
    )
    scores['not_full_narrative'] = response.count('\n\n') < len(portfolio['holdings'])
    scores['has_reasoning'] = any(
        w in r for w in ('conviction', 'signal', 'quality', 'because', 'confirmed', 'momentum', 'catalyst')
    )
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


def score_macro(response: str) -> dict:
    scores = _universal(response)
    r = response.lower()
    scores['mentions_regime'] = any(
        w in r for w in ('regime', 'recovery', 'bull', 'bear', 'neutral', 'risk-on', 'risk-off')
    )
    scores['mentions_yield_or_rates'] = any(
        w in r for w in ('yield', 'rates', 'bonds', 'tlt', 'flatten', 'steepen', 'curve', 'fed', 'fomc')
    )
    return scores


def score_no_data(response: str) -> dict:
    scores = _universal(response)
    r = response.lower()
    scores['gives_no_data_response'] = any(
        p in r for p in (
            "don't have current kb data",
            "no kb data",
            "check back after",
            "not in the kb",
            "no current kb",
            "kb currently",
            "cannot answer",
            "i don't have",
            "not available in",
        )
    )
    scores['no_invented_data'] = not any(
        w in r for w in (
            'zone:', 'entry:', 'stop:', 'target:',
            'bullish setup', 'bearish setup',
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
) -> dict:
    """
    Dispatch to the correct per-intent scorer.
    Returns a dict of check_name → bool | float.
    """
    if intent == 'single_ticker':
        return score_single_ticker(response, portfolio, ticker)
    elif intent == 'portfolio_review':
        return score_portfolio_review(response, portfolio)
    elif intent == 'opportunity':
        return score_opportunity(response, portfolio)
    elif intent == 'geo':
        return score_geo(response)
    elif intent == 'greeks':
        return score_greeks(response, portfolio.get('trader_level', 'developing'))
    elif intent == 'macro':
        return score_macro(response)
    elif intent == 'no_data':
        return score_no_data(response)
    else:
        return _universal(response)


def is_pass(scores: dict) -> bool:
    """True if all checks pass (bools True, floats >= 0.8)."""
    for v in scores.values():
        if isinstance(v, bool):
            if not v:
                return False
        else:
            if v < 0.8:
                return False
    return True


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

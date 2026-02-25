"""
analytics/adversarial_stress.py — Adversarial Signal Stress Tester

Injects pre-committed synthetic contradictory atoms into an in-memory DB
copy, re-runs the signal enrichment classification pipeline on that copy,
and measures how much conviction_tier degrades per ticker.

DESIGN PRINCIPLE
================
The live DB is NEVER modified. The stress tester:
  1. Reads current KB state via _read_kb_atoms() (same function used in
     production enrichment cycle).
  2. Applies a scenario's atom overrides to an in-memory copy of the
     ticker_atoms dict (no DB copy needed — enrichment is pure-function).
  3. Re-runs _classify_signal_quality + _classify_macro_confirmation +
     _compute_position_sizing_atoms on the modified state.
  4. Diffs the output conviction_tier against the baseline.

SCENARIOS (6 pre-committed)
============================
Each scenario is defined as a set of atom overrides applied to the in-memory
ticker_atoms snapshot:

  bear_analyst       — forces signal_quality=conflicted on all equity tickers
                       (simulates a bearish broker note overriding direction)

  risk_off_regime    — sets SPY/HYG/TLT macro signals to risk-off
                       (macro_confirmation→unconfirmed for all longs)

  earnings_miss      — adds earnings_miss predicate on all equity tickers
                       and sets signal_quality→conflicted via enrichment

  macro_flip         — all three macro proxies (SPY, HYG, TLT) flipped to
                       near_low (extreme risk-off)

  guidance_lowered   — adds guidance_lowered on all equity tickers, forces
                       signal_direction→neutral

  credit_downgrade   — HYG→near_low + TLT→near_high (credit spreads blow out,
                       bonds bid — classic risk-off credit stress)

ROBUSTNESS SCORE
================
Per-ticker:
    robustness_score = 1.0 - (tier_delta / max_possible_delta)
    where tier_delta = numeric(tier_before) - numeric(tier_after)
    and _TIER_NUMERIC = { high:3, medium:2, low:1, avoid:0 }

Portfolio fragility:
    fragility = mean(tier_delta across all tickers) / 3.0  (normalised 0→1)

Zero-LLM, pure Python, <50ms on typical 30-ticker portfolio.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

try:
    from ingest.signal_enrichment_adapter import (
        _read_kb_atoms,
        _classify_signal_quality,
        _classify_macro_confirmation,
        _classify_price_regime,
        _classify_market_regime,
        _CREDIT_PROXY,
        _RATES_PROXY,
        _MARKET_PROXY,
        _BULLISH_SIGNALS,
        _BEARISH_SIGNALS,
    )
    _HAS_ENRICHMENT = True
except ImportError:
    _HAS_ENRICHMENT = False

try:
    from ingest.signal_enrichment_adapter import _compute_position_sizing_atoms
    _HAS_SIZING = True
except ImportError:
    _HAS_SIZING = False


# ── Tier ordering ─────────────────────────────────────────────────────────────

_TIER_NUMERIC: Dict[str, int] = {'high': 3, 'medium': 2, 'low': 1, 'avoid': 0}
_TIER_FROM_NUMERIC: Dict[int, str] = {v: k for k, v in _TIER_NUMERIC.items()}


# ── Scenario definitions ──────────────────────────────────────────────────────

#   Each scenario is a callable:
#       fn(ticker_atoms, macro_signals) -> (modified_ticker_atoms, modified_macro_signals)
#   Modifications are made on shallow copies — originals untouched.

def _scenario_bear_analyst(
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Force signal_quality=conflicted on all equity tickers.
    Simulates a bearish broker note that directly contradicts each long signal.
    """
    modified = {t: dict(preds) for t, preds in ticker_atoms.items()}
    for ticker, preds in modified.items():
        if ticker in (_CREDIT_PROXY, _RATES_PROXY, _MARKET_PROXY,
                      'gld', 'uup', 'eem', 'vwo', 'iwm', 'qqq',
                      'hyg', 'tlt', 'spy'):
            continue
        if preds.get('signal_direction'):
            preds['signal_quality'] = 'conflicted'
    return modified, dict(macro_signals)


def _scenario_risk_off_regime(
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Flip macro signals to risk-off: SPY=near_low, HYG=near_low, TLT=near_high.
    macro_confirmation becomes 'unconfirmed' for all long equity signals.
    """
    modified_macro = dict(macro_signals)
    modified_macro[_MARKET_PROXY]  = 'near_low'   # SPY bearish
    modified_macro[_CREDIT_PROXY]  = 'near_low'   # HYG bearish (spreads wide)
    modified_macro[_RATES_PROXY]   = 'near_high'  # TLT bullish (rates falling = risk-off)

    modified_atoms = {t: dict(p) for t, p in ticker_atoms.items()}
    # Update the macro proxy tickers' signal_direction in ticker_atoms too
    for proxy, sig in [(_MARKET_PROXY, 'near_low'), (_CREDIT_PROXY, 'near_low'),
                       (_RATES_PROXY, 'near_high')]:
        if proxy in modified_atoms:
            modified_atoms[proxy]['signal_direction'] = sig
        else:
            modified_atoms[proxy] = {'signal_direction': sig}

    return modified_atoms, modified_macro


def _scenario_earnings_miss(
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Inject earnings_miss on all equity tickers and force signal_direction=neutral.
    An earnings miss directly undermines any bullish thesis.
    """
    _MACRO_PROXIES = {_CREDIT_PROXY, _RATES_PROXY, _MARKET_PROXY,
                     'gld', 'uup', 'eem', 'vwo', 'spy', 'hyg', 'tlt'}
    modified = {t: dict(preds) for t, preds in ticker_atoms.items()}
    for ticker, preds in modified.items():
        if ticker in _MACRO_PROXIES:
            continue
        if preds.get('signal_direction') in _BULLISH_SIGNALS:
            preds['signal_direction'] = 'neutral'
            preds['earnings_miss'] = 'q_current'
    return modified, dict(macro_signals)


def _scenario_macro_flip(
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Extreme risk-off: all three macro proxies flipped to near_low.
    The most severe macro stress test — simulates a 2008/2020-style event.
    """
    modified_macro = dict(macro_signals)
    for proxy in (_MARKET_PROXY, _CREDIT_PROXY, _RATES_PROXY):
        modified_macro[proxy] = 'near_low'

    modified_atoms = {t: dict(p) for t, p in ticker_atoms.items()}
    for proxy in (_MARKET_PROXY, _CREDIT_PROXY, _RATES_PROXY):
        if proxy in modified_atoms:
            modified_atoms[proxy]['signal_direction'] = 'near_low'
        else:
            modified_atoms[proxy] = {'signal_direction': 'near_low'}

    return modified_atoms, modified_macro


def _scenario_guidance_lowered(
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Forward guidance cut on all equity names: signal_direction→neutral.
    Simulates a guidance-lowering earnings cycle (e.g. margin compression).
    """
    _MACRO_PROXIES = {_CREDIT_PROXY, _RATES_PROXY, _MARKET_PROXY,
                     'gld', 'uup', 'eem', 'vwo', 'spy', 'hyg', 'tlt'}
    modified = {t: dict(preds) for t, preds in ticker_atoms.items()}
    for ticker, preds in modified.items():
        if ticker in _MACRO_PROXIES:
            continue
        if preds.get('signal_direction') in _BULLISH_SIGNALS:
            preds['guidance_lowered'] = 'fy_current'
            preds['signal_direction'] = 'neutral'
    return modified, dict(macro_signals)


def _scenario_credit_downgrade(
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Credit stress: HYG→near_low + TLT→near_high.
    Spreads blow out, treasuries bid — the pre-recession credit warning signal.
    SPY is left unchanged (equity hasn't reacted yet — tests early-warning sensitivity).
    """
    modified_macro = dict(macro_signals)
    modified_macro[_CREDIT_PROXY] = 'near_low'   # HYG credit spreads wide
    modified_macro[_RATES_PROXY]  = 'near_high'  # TLT bonds bid (risk-off)

    modified_atoms = {t: dict(p) for t, p in ticker_atoms.items()}
    for proxy, sig in [(_CREDIT_PROXY, 'near_low'), (_RATES_PROXY, 'near_high')]:
        if proxy in modified_atoms:
            modified_atoms[proxy]['signal_direction'] = sig
        else:
            modified_atoms[proxy] = {'signal_direction': sig}

    return modified_atoms, modified_macro


_SCENARIOS: Dict[str, callable] = {
    'bear_analyst':      _scenario_bear_analyst,
    'risk_off_regime':   _scenario_risk_off_regime,
    'earnings_miss':     _scenario_earnings_miss,
    'macro_flip':        _scenario_macro_flip,
    'guidance_lowered':  _scenario_guidance_lowered,
    'credit_downgrade':  _scenario_credit_downgrade,
}


# ── Baseline conviction tier reader ──────────────────────────────────────────

def _compute_conviction_tier(
    ticker: str,
    preds: Dict[str, str],
    macro_signals: Dict[str, str],
) -> Optional[str]:
    """
    Re-run the signal enrichment classification for a single ticker using
    the pure functions from signal_enrichment_adapter.

    Returns the conviction_tier string, or None if insufficient data.
    """
    if not _HAS_ENRICHMENT or not _HAS_SIZING:
        return None

    signal_dir   = preds.get('signal_direction', '')
    vol_regime   = preds.get('volatility_regime', '')
    thesis_risk  = preds.get('thesis_risk_level', '')

    last_price: Optional[float]   = None
    price_target: Optional[float] = None
    vol_30d: Optional[float]      = None

    try:
        last_price = float(preds['last_price'])
    except (KeyError, ValueError, TypeError):
        pass
    try:
        price_target = float(preds['price_target'])
    except (KeyError, ValueError, TypeError):
        pass
    try:
        vol_30d = float(preds['volatility_30d'])
    except (KeyError, ValueError, TypeError):
        pass

    upside_pct: Optional[float] = None
    if last_price and price_target and last_price > 0:
        upside_pct = (price_target - last_price) / last_price * 100

    price_regime = _classify_price_regime(last_price, price_target, signal_dir)
    sig_quality  = _classify_signal_quality(signal_dir, vol_regime, price_regime, upside_pct)
    macro_conf   = _classify_macro_confirmation(signal_dir, macro_signals)

    # Use any overridden signal_quality from the scenario (bear_analyst injects it directly)
    if 'signal_quality' in preds:
        sig_quality = preds['signal_quality']

    if not sig_quality or not thesis_risk:
        return None

    sizing_atoms = _compute_position_sizing_atoms(
        ticker=ticker,
        signal_quality=sig_quality,
        thesis_risk_level=thesis_risk,
        macro_confirmation=macro_conf,
        vol_30d=vol_30d,
        src_base=f'stress_{ticker}',
        meta={},
    )

    for atom in sizing_atoms:
        if atom.predicate == 'conviction_tier':
            return atom.object

    return None


def _read_baseline(db_path: str) -> Tuple[Dict[str, Optional[str]], Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Read the current KB state and compute baseline conviction tiers.

    Returns:
        baseline_tiers:  { ticker: conviction_tier_str | None }
        ticker_atoms:    raw atom dict from _read_kb_atoms
        macro_signals:   macro proxy signals dict
    """
    if not _HAS_ENRICHMENT:
        return {}, {}, {}

    ticker_atoms, macro_signals = _read_kb_atoms(db_path)
    baseline_tiers: Dict[str, Optional[str]] = {}

    for ticker, preds in ticker_atoms.items():
        # Skip macro proxies and subjects with no signal direction
        if not preds.get('signal_direction') and not preds.get('last_price'):
            continue
        tier = _compute_conviction_tier(ticker, preds, macro_signals)
        if tier is not None:
            baseline_tiers[ticker] = tier

    return baseline_tiers, ticker_atoms, macro_signals


# ── Core stress test runner ───────────────────────────────────────────────────

def _run_scenario(
    scenario_name: str,
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
    baseline_tiers: Dict[str, Optional[str]],
) -> dict:
    """
    Run a single scenario and return per-ticker delta results.
    """
    if scenario_name not in _SCENARIOS:
        return {'error': f'unknown scenario: {scenario_name!r}'}

    scenario_fn = _SCENARIOS[scenario_name]
    stressed_atoms, stressed_macro = scenario_fn(ticker_atoms, macro_signals)

    results: List[dict] = []
    tier_deltas: List[float] = []

    for ticker, baseline_tier in baseline_tiers.items():
        preds = stressed_atoms.get(ticker, {})
        if not preds.get('signal_direction') and not preds.get('last_price'):
            continue

        stressed_tier = _compute_conviction_tier(ticker, preds, stressed_macro)
        if stressed_tier is None:
            continue

        b_num = _TIER_NUMERIC.get(baseline_tier or '', 1)
        s_num = _TIER_NUMERIC.get(stressed_tier, 1)
        delta = b_num - s_num  # positive = degraded, 0 = unchanged, negative = improved
        robust = delta == 0
        tier_deltas.append(float(delta))

        results.append({
            'ticker':       ticker.upper(),
            'tier_before':  baseline_tier,
            'tier_after':   stressed_tier,
            'delta':        delta,
            'robust':       robust,
        })

    # Sort by delta desc (most fragile first)
    results.sort(key=lambda r: r['delta'], reverse=True)

    n_tested   = len(results)
    n_degraded = sum(1 for r in results if r['delta'] > 0)
    n_robust   = sum(1 for r in results if r['robust'])

    fragility = round(
        (sum(tier_deltas) / (3.0 * max(n_tested, 1))),
        4,
    )

    return {
        'scenario':            scenario_name,
        'n_tickers_tested':    n_tested,
        'n_degraded':          n_degraded,
        'n_robust':            n_robust,
        'fragility_score':     fragility,   # 0 = robust, 1 = complete collapse
        'ticker_results':      results,
    }


def run_stress_test(
    db_path: str,
    scenarios: Optional[List[str]] = None,
) -> dict:
    """
    Run adversarial stress scenarios against the current KB signal state.

    Parameters
    ----------
    db_path   path to trading_knowledge.db
    scenarios list of scenario names to run; default = all 6

    Returns
    -------
    {
        "as_of":           ISO-8601 timestamp,
        "baseline_tickers": int,
        "scenarios_run":   [...],
        "results": {
            "bear_analyst": {
                "scenario": "bear_analyst",
                "n_tickers_tested": N,
                "n_degraded": M,
                "n_robust": K,
                "fragility_score": 0.42,
                "ticker_results": [
                    { "ticker": "AAPL", "tier_before": "high",
                      "tier_after": "avoid", "delta": 3, "robust": false },
                    ...
                ]
            },
            ...
        },
        "portfolio_fragility": {   -- cross-scenario summary
            "most_fragile_ticker": "AAPL",
            "mean_fragility": 0.38,
            "scenario_fragility": { "bear_analyst": 0.42, ... }
        },
        "error": "..."   -- only present if enrichment module unavailable
    }
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    if not _HAS_ENRICHMENT or not _HAS_SIZING:
        return {
            'as_of': now_iso,
            'error': 'signal_enrichment_adapter not available',
        }

    # Default: run all 6 scenarios
    if scenarios is None:
        scenarios = list(_SCENARIOS.keys())
    else:
        unknown = [s for s in scenarios if s not in _SCENARIOS]
        if unknown:
            return {
                'as_of': now_iso,
                'error': f'unknown scenarios: {unknown}',
                'valid_scenarios': list(_SCENARIOS.keys()),
            }

    baseline_tiers, ticker_atoms, macro_signals = _read_baseline(db_path)

    results: Dict[str, dict] = {}
    for scenario_name in scenarios:
        results[scenario_name] = _run_scenario(
            scenario_name, ticker_atoms, macro_signals, baseline_tiers
        )

    # Portfolio fragility summary
    scenario_fragility = {
        s: r['fragility_score']
        for s, r in results.items()
        if 'fragility_score' in r
    }

    # Most fragile ticker: ticker with highest total delta across all scenarios
    ticker_total_delta: Dict[str, int] = {}
    for scenario_result in results.values():
        for tr in scenario_result.get('ticker_results', []):
            t = tr['ticker']
            ticker_total_delta[t] = ticker_total_delta.get(t, 0) + tr['delta']

    most_fragile = (
        max(ticker_total_delta, key=ticker_total_delta.get)
        if ticker_total_delta else None
    )
    mean_fragility = round(
        sum(scenario_fragility.values()) / max(len(scenario_fragility), 1),
        4,
    )

    return {
        'as_of':             now_iso,
        'baseline_tickers':  len(baseline_tiers),
        'scenarios_run':     scenarios,
        'results':           results,
        'portfolio_fragility': {
            'most_fragile_ticker': most_fragile,
            'mean_fragility':      mean_fragility,
            'scenario_fragility':  scenario_fragility,
        },
    }

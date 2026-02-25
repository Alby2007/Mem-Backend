"""
analytics/counterfactual.py — Counterfactual Reasoning Engine

POST /analytics/counterfactual — "What would conviction tiers look like
if the Fed cut rates tomorrow?"

The caller provides a scenario dict of direct atom overrides. The engine:
  1. Reads current KB state via _read_kb_atoms() (same as enrichment cycle).
  2. Applies the direct overrides to the in-memory ticker_atoms copy.
  3. Optionally propagates the overrides through the causal graph (if the
     causal_graph module is available and the DB has causal_edges seeded).
     Causal propagation converts abstract macro shifts (e.g. fed_funds_rate
     delta) into concrete signal_direction changes via the edge map.
  4. Re-runs conviction tier classification on the modified state.
  5. Returns the delta: which tickers changed tier and in which direction.

CAUSAL PROPAGATION (optional enhancement)
==========================================
If a scenario key matches a causal concept (e.g. 'fed_rate_cut'), the
engine traverses the causal graph up to depth 2 and applies the downstream
signal_direction consequences to the macro proxy atoms. Without the causal
graph it degrades gracefully to direct atom overrides only.

SCENARIO FORMAT
===============
The scenario dict supports two types of keys:

  a) Macro signal overrides (applied to macro_signals dict):
       "spy_signal":           "near_high" | "near_low" | "neutral"
       "hyg_signal":           "near_high" | "near_low" | "neutral"
       "tlt_signal":           "near_high" | "near_low" | "neutral"
       "market_regime":        "risk_on_expansion" | ...

  b) Scalar shifts (converted to signal_direction consequences):
       "fed_funds_rate":       float delta in % (e.g. -0.25 = cut 25bps)
                               → fed_rate_cut in causal graph
       "credit_spreads_bps":   float delta in bps (e.g. +50 = spread
                               widening) → hyg_signal → near_low

  c) Per-ticker overrides (applied to ticker_atoms for that ticker):
       "tickers": { "AAPL": { "thesis_risk_level": "tight" } }

RESPONSE FORMAT
===============
{
  "as_of":             ISO-8601,
  "scenario_applied":  { ... input scenario dict ... },
  "causal_propagation": [ { concept, propagated_to, mechanism } ],
  "baseline_tickers":  int,
  "tier_changes": [
    { "ticker": "GS", "from": "medium", "to": "high",
      "delta": 1, "direction": "upgrade" },
    ...
  ],
  "upgrades":    int,
  "downgrades":  int,
  "unchanged":   int,
  "regime_change": { "from": "risk_off_contraction",
                     "to":   "risk_on_expansion" },
  "methodology": "direct_override" | "causal_graph_propagation"
}

Zero-LLM, pure Python, <100ms on typical 30-ticker portfolio.
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

try:
    from knowledge.causal_graph import traverse_causal, ensure_causal_edges_table
    _HAS_CAUSAL = True
except ImportError:
    _HAS_CAUSAL = False


# ── Tier ordering ─────────────────────────────────────────────────────────────

_TIER_NUMERIC: Dict[str, int] = {'high': 3, 'medium': 2, 'low': 1, 'avoid': 0}

# Causal concepts that map from scalar shifts to graph seeds
_SCALAR_CAUSAL_MAP = {
    'fed_funds_rate': {
        'positive': 'fed_rate_hike',   # rate > 0 → hike
        'negative': 'fed_rate_cut',    # rate < 0 → cut
    },
    'credit_spreads_bps': {
        'positive': 'hyg_spreads_widen',
        'negative': 'hyg_spreads_tighten',
    },
}

# Causal effects that translate to macro signal overrides
_CAUSAL_EFFECT_TO_MACRO: Dict[str, Tuple[str, str]] = {
    'hyg_spreads_widen':    (_CREDIT_PROXY, 'near_low'),
    'hyg_spreads_tighten':  (_CREDIT_PROXY, 'near_high'),
    'risk_off_rotation':    (_MARKET_PROXY, 'near_low'),
    'risk_on_rotation':     (_MARKET_PROXY, 'near_high'),
    'yield_curve_inverts':  (_RATES_PROXY,  'near_high'),
    'yield_curve_steepens': (_RATES_PROXY,  'near_low'),
    'dollar_strengthens':   ('uup', 'near_high'),
    'dollar_weakens':       ('uup', 'near_low'),
    'commodities_rise':     ('gld', 'near_high'),
    'commodities_decline':  ('gld', 'near_low'),
}


# ── Conviction tier re-computation (same logic as adversarial_stress.py) ──────

def _compute_conviction_tier(
    ticker: str,
    preds: Dict[str, str],
    macro_signals: Dict[str, str],
) -> Optional[str]:
    if not _HAS_ENRICHMENT or not _HAS_SIZING:
        return None

    signal_dir   = preds.get('signal_direction', '')
    vol_regime   = preds.get('volatility_regime', '')
    thesis_risk  = preds.get('thesis_risk_level', '')

    last_price: Optional[float] = None
    price_target: Optional[float] = None
    vol_30d: Optional[float] = None

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

    # Honour any injected signal_quality override
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
        src_base=f'counterfactual_{ticker}',
        meta={},
    )
    for atom in sizing_atoms:
        if atom.predicate == 'conviction_tier':
            return atom.object
    return None


# ── Baseline reader ───────────────────────────────────────────────────────────

def _get_baseline(
    db_path: str,
) -> Tuple[Dict[str, Optional[str]], Dict[str, Dict[str, str]], Dict[str, str]]:
    if not _HAS_ENRICHMENT:
        return {}, {}, {}

    ticker_atoms, macro_signals = _read_kb_atoms(db_path)
    baseline_tiers: Dict[str, Optional[str]] = {}

    for ticker, preds in ticker_atoms.items():
        if not preds.get('signal_direction') and not preds.get('last_price'):
            continue
        tier = _compute_conviction_tier(ticker, preds, macro_signals)
        if tier is not None:
            baseline_tiers[ticker] = tier

    return baseline_tiers, ticker_atoms, macro_signals


# ── Causal propagation ────────────────────────────────────────────────────────

def _propagate_causal(
    db_path: str,
    seed_concepts: List[str],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, str], List[dict]]:
    """
    Traverse the causal graph from each seed concept and apply downstream
    macro signal consequences.

    Returns:
        updated macro_signals dict (copy — originals untouched)
        propagation_log: list of { concept, propagated_to, mechanism }
    """
    if not _HAS_CAUSAL or not seed_concepts:
        return dict(macro_signals), []

    updated_macro = dict(macro_signals)
    propagation_log: List[dict] = []

    try:
        conn = sqlite3.connect(db_path, timeout=10)
        ensure_causal_edges_table(conn)

        for seed in seed_concepts:
            result = traverse_causal(conn, seed, max_depth=2, min_confidence=0.6)
            for hop in result.get('chain', []):
                effect = hop['effect']
                if effect in _CAUSAL_EFFECT_TO_MACRO:
                    proxy, signal = _CAUSAL_EFFECT_TO_MACRO[effect]
                    updated_macro[proxy] = signal
                    propagation_log.append({
                        'seed':         seed,
                        'concept':      hop['cause'],
                        'propagated_to': effect,
                        'mechanism':    hop['mechanism'],
                        'proxy_updated': proxy,
                        'new_signal':   signal,
                    })
        conn.close()
    except Exception:
        pass  # causal propagation is optional — fall back to direct overrides

    return updated_macro, propagation_log


# ── Scenario application ──────────────────────────────────────────────────────

def _apply_scenario(
    db_path: str,
    scenario: dict,
    ticker_atoms: Dict[str, Dict[str, str]],
    macro_signals: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str], List[dict], str]:
    """
    Apply a scenario to the in-memory state.

    Returns:
        modified_ticker_atoms, modified_macro_signals,
        causal_propagation_log, methodology_str
    """
    modified_macro  = dict(macro_signals)
    modified_atoms  = {t: dict(p) for t, p in ticker_atoms.items()}
    causal_seeds: List[str] = []
    methodology = 'direct_override'

    # ── Direct macro signal overrides ─────────────────────────────────────────
    if 'spy_signal' in scenario:
        modified_macro[_MARKET_PROXY] = scenario['spy_signal']
        if _MARKET_PROXY in modified_atoms:
            modified_atoms[_MARKET_PROXY]['signal_direction'] = scenario['spy_signal']

    if 'hyg_signal' in scenario:
        modified_macro[_CREDIT_PROXY] = scenario['hyg_signal']
        if _CREDIT_PROXY in modified_atoms:
            modified_atoms[_CREDIT_PROXY]['signal_direction'] = scenario['hyg_signal']

    if 'tlt_signal' in scenario:
        modified_macro[_RATES_PROXY] = scenario['tlt_signal']
        if _RATES_PROXY in modified_atoms:
            modified_atoms[_RATES_PROXY]['signal_direction'] = scenario['tlt_signal']

    # ── Scalar shifts → causal seeds ──────────────────────────────────────────
    for key, causal_map in _SCALAR_CAUSAL_MAP.items():
        if key in scenario:
            try:
                delta = float(scenario[key])
            except (TypeError, ValueError):
                continue
            direction = 'positive' if delta >= 0 else 'negative'
            seed = causal_map[direction]
            causal_seeds.append(seed)

    # ── Causal propagation ─────────────────────────────────────────────────────
    propagation_log: List[dict] = []
    if causal_seeds and _HAS_CAUSAL:
        updated_macro, propagation_log = _propagate_causal(
            db_path, causal_seeds, modified_macro
        )
        modified_macro = updated_macro
        methodology = 'causal_graph_propagation'

    # ── Per-ticker atom overrides ──────────────────────────────────────────────
    ticker_overrides = scenario.get('tickers', {})
    for ticker, overrides in ticker_overrides.items():
        ticker_lower = ticker.lower()
        if ticker_lower in modified_atoms:
            modified_atoms[ticker_lower].update(overrides)
        else:
            modified_atoms[ticker_lower] = dict(overrides)

    return modified_atoms, modified_macro, propagation_log, methodology


# ── Main counterfactual runner ────────────────────────────────────────────────

def run_counterfactual(
    db_path: str,
    scenario: dict,
) -> dict:
    """
    Run a counterfactual scenario against the current KB signal state.

    Parameters
    ----------
    db_path   path to trading_knowledge.db
    scenario  dict of overrides (see module docstring for format)

    Returns
    -------
    See module docstring for response format.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    if not _HAS_ENRICHMENT or not _HAS_SIZING:
        return {
            'as_of': now_iso,
            'error': 'signal_enrichment_adapter not available',
        }

    if not scenario:
        return {
            'as_of': now_iso,
            'error': 'scenario is required and must not be empty',
        }

    # Read baseline
    baseline_tiers, ticker_atoms, macro_signals = _get_baseline(db_path)

    # Compute baseline market regime
    baseline_regime = None
    if _HAS_ENRICHMENT:
        try:
            baseline_regime = _classify_market_regime(macro_signals, ticker_atoms)
        except Exception:
            pass

    # Apply scenario
    mod_atoms, mod_macro, propagation_log, methodology = _apply_scenario(
        db_path, scenario, ticker_atoms, macro_signals
    )

    # Compute counterfactual market regime
    cf_regime = None
    if _HAS_ENRICHMENT:
        try:
            cf_regime = _classify_market_regime(mod_macro, mod_atoms)
        except Exception:
            pass

    # Recompute conviction tiers under counterfactual state
    tier_changes: List[dict] = []
    upgrades   = 0
    downgrades = 0
    unchanged  = 0

    for ticker, baseline_tier in baseline_tiers.items():
        preds = mod_atoms.get(ticker, {})
        if not preds.get('signal_direction') and not preds.get('last_price'):
            continue

        cf_tier = _compute_conviction_tier(ticker, preds, mod_macro)
        if cf_tier is None:
            continue

        b_num = _TIER_NUMERIC.get(baseline_tier or '', 1)
        c_num = _TIER_NUMERIC.get(cf_tier, 1)
        delta = c_num - b_num  # positive = upgrade, negative = downgrade

        if delta > 0:
            upgrades += 1
            direction = 'upgrade'
        elif delta < 0:
            downgrades += 1
            direction = 'downgrade'
        else:
            unchanged += 1
            direction = 'unchanged'

        if delta != 0:
            tier_changes.append({
                'ticker':    ticker.upper(),
                'from':      baseline_tier,
                'to':        cf_tier,
                'delta':     delta,
                'direction': direction,
            })

    # Sort: upgrades first (delta desc), then downgrades (delta asc)
    tier_changes.sort(key=lambda r: r['delta'], reverse=True)

    regime_change = None
    if baseline_regime and cf_regime and baseline_regime != cf_regime:
        regime_change = {'from': baseline_regime, 'to': cf_regime}
    elif baseline_regime and cf_regime:
        regime_change = {'from': baseline_regime, 'to': cf_regime}

    return {
        'as_of':               now_iso,
        'scenario_applied':    scenario,
        'causal_propagation':  propagation_log,
        'baseline_tickers':    len(baseline_tiers),
        'tier_changes':        tier_changes,
        'upgrades':            upgrades,
        'downgrades':          downgrades,
        'unchanged':           unchanged,
        'regime_change':       regime_change,
        'methodology':         methodology,
    }

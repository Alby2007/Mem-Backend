"""
ingest/signal_enrichment_adapter.py — Second-Order Signal Enrichment

Reads existing KB atoms and computes regime-conditional, interpretable
signal atoms. No external API calls. Runs after yfinance_adapter each cycle.

ATOMS PRODUCED
==============

  price_regime   — where the current price sits within its 52-week range
    Values: near_52w_high | mid_range | near_52w_low
    Source: derived_signal_price_regime_{ticker}
    Logic: uses 52w_high / 52w_low from yfinance metadata if present;
           falls back to estimating from last_price vs price_target ratio.

  upside_pct     — percentage upside from last_price to consensus price_target
    Values: e.g. "34.2" (as string, LLM-readable)
    Source: derived_signal_upside_{ticker}
    Note: negative values = downside. Stored as plain percent string.

  signal_quality — coherence assessment across signal_direction,
                   volatility_regime, price_regime, and upside_pct
    Values: strong | confirmed | extended | conflicted | weak
    Source: derived_signal_quality_{ticker}

  macro_confirmation — whether equity signal aligns with cross-asset macro
    Values: confirmed | partial | unconfirmed | no_data
    Source: derived_signal_macro_confirm_{ticker}

SIGNAL QUALITY DECISION RULES
==============================
Rules are explicit and documented here so the classification logic is
auditable when revisited. Each rule applies in priority order (first match wins).

  STRONG: all four conditions supportive
    - signal_direction IN (long, near_high)   AND
    - upside_pct >= 15                         AND
    - price_regime != near_52w_high            AND   ← not already extended
    - volatility_regime IN (low_volatility, medium_volatility)

  CONFIRMED: signal and upside aligned, price not extended
    - signal_direction IN (long, near_high)   AND
    - upside_pct >= 8                          AND
    - price_regime != near_52w_high

  EXTENDED: bullish signal but price is already near the top
    - signal_direction IN (long, near_high)   AND
    - price_regime == near_52w_high

  CONFLICTED: opposing signals between direction and other dimensions
    - signal_direction IN (long, near_high)   AND upside_pct < 0  ← price > target
    OR
    - signal_direction IN (short, near_low)   AND upside_pct >= 15 ← target much higher
    OR
    - signal_direction IN (long, near_high)   AND
      volatility_regime == high_volatility    AND
      price_regime == near_52w_high           ← extended + high vol = conflicted

  WEAK: neutral signal or insufficient data
    - signal_direction == neutral             OR
    - upside_pct data missing

MACRO CONFIRMATION RULES
=========================
Checks alignment between equity signal and three cross-asset macro proxies
available in the KB: HYG (credit), TLT (rates), SPY (broad market).

  CONFIRMED: all three proxies align with equity signal
    - equity signal is long/bullish
    - HYG signal_direction != near_low  (credit NOT selling off)
    - TLT signal_direction != near_high (rates NOT spiking / bond rally = risk-off)
    - SPY signal_direction IN (near_high, long)

  PARTIAL: majority (2/3) proxies align
  UNCONFIRMED: majority proxies contradict equity signal
  NO_DATA: insufficient macro proxy atoms in KB

SOURCE PREFIX
=============
All atoms use 'derived_signal_' prefix → authority 0.65 in authority.py.
Correctly scores below exchange_feed (1.0) and broker_research (0.80)
since these are computed from observed data, not directly observed.

INTERVAL
========
Register at same interval as yfinance_adapter (300s) so enrichment
always reflects the latest price/signal cycle.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date as _date, datetime, timezone
from typing import Dict, List, Optional, Tuple

from ingest.base import BaseIngestAdapter, RawAtom

_logger = logging.getLogger(__name__)

# Macro proxy tickers used for cross-asset confirmation
_CREDIT_PROXY  = 'hyg'   # credit spreads — near_high = risk-on
_RATES_PROXY   = 'tlt'   # long-duration treasuries — near_high = risk-off (rates falling)
_MARKET_PROXY  = 'spy'   # broad equity market

# signal_direction values that indicate bullish positioning
_BULLISH_SIGNALS = {'long', 'near_high', 'near_52w_high', 'bullish'}
# signal_direction values that indicate bearish positioning
_BEARISH_SIGNALS = {'short', 'near_low', 'near_52w_low', 'bearish'}

# upside_pct thresholds (in percent)
_UPSIDE_STRONG    = 15.0   # >= 15%: meaningful analyst conviction
_UPSIDE_CONFIRMED =  8.0   # >=  8%: moderate upside
_UPSIDE_CONFLICT  =  0.0   # <  0%: price already above target (downside)

# volatility regimes that permit 'strong' classification
_LOW_VOL_REGIMES  = {'low_volatility', 'medium_volatility'}
_HIGH_VOL_REGIME  = 'high_volatility'


# ── KB snapshot reader ────────────────────────────────────────────────────────

def _read_kb_atoms(
    kg_db_path: str,
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """
    Read current KB atoms directly from SQLite.

    Returns:
        ticker_atoms: { ticker_lower: { predicate: object_value } }
                      Only the most recent (highest-confidence) row per
                      (subject, predicate) pair is kept.
        macro_signals: { proxy_ticker: signal_direction_value }
    """
    ticker_atoms: Dict[str, Dict[str, str]] = {}
    conn = sqlite3.connect(kg_db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    c = conn.cursor()

    try:
        # Fetch all relevant predicates ordered so highest confidence wins.
        # low_52w and volatility_30d come from HistoricalBackfillAdapter —
        # present after first backfill run; fallback rules fire if absent.
        # skew_regime / iv_skew_ratio come from OptionsAdapter (1800s interval)
        # — may be absent on startup; skew filter is skipped when missing.
        c.execute("""
            SELECT subject, predicate, object, confidence
            FROM facts
            WHERE predicate IN (
                'last_price', 'price_target', 'signal_direction',
                'conviction_tier',
                'volatility_regime', 'market_cap_tier', 'sector',
                'earnings_quality', 'low_52w', 'volatility_30d',
                'signal_quality', 'thesis_risk_level', 'macro_confirmation',
                'next_earnings',
                'skew_regime', 'iv_skew_ratio',
                'tail_risk', 'spy_skew_regime', 'spy_skew_ratio',
                'insider_conviction', 'short_squeeze_potential',
                'macro_event_risk', 'sector_tailwind', 'risk_appetite',
                'causal_signal', 'signal_conflicted',
                'best_regime', 'worst_regime',
                'return_in_risk_on_expansion', 'return_in_risk_off_contraction',
                'return_in_stagflation', 'return_in_recovery',
                'regime_hit_rate_risk_on_expansion', 'regime_hit_rate_risk_off_contraction',
                'regime_hit_rate_stagflation', 'regime_hit_rate_recovery'
            )
            ORDER BY subject, predicate, confidence DESC
        """)
        for row in c.fetchall():
            subj = row['subject'].lower().strip()
            pred = row['predicate'].strip()
            obj  = row['object'].strip()
            if subj not in ticker_atoms:
                ticker_atoms[subj] = {}
            # First row per (subj, pred) is highest confidence — keep it
            if pred not in ticker_atoms[subj]:
                ticker_atoms[subj][pred] = obj
    finally:
        conn.close()

    # Extract macro proxy signals separately for clarity
    macro_signals = {
        proxy: ticker_atoms.get(proxy, {}).get('signal_direction', '')
        for proxy in (_CREDIT_PROXY, _RATES_PROXY, _MARKET_PROXY)
    }

    return ticker_atoms, macro_signals


# ── Classification logic ───────────────────────────────────────────────────────

# ── Position sizing constants ─────────────────────────────────────────────────
# Base allocations by conviction tier (% of portfolio, pre-vol-adjustment)
_CT_BASE_ALLOC: dict = {'high': 5.0, 'medium': 3.0, 'low': 1.5, 'avoid': 0.0}
# Reference volatility: SPY long-run realised annualised vol (~20%)
_VOL_REF           = 20.0
_VOL_SCALAR_FLOOR  = 0.2   # extreme vol names still get a minimal non-zero output
_VOL_SCALAR_CAP    = 1.0   # low vol never inflates above base allocation

# ── Invalidation thresholds ───────────────────────────────────────────────────
# IP2/IP3: percentage-floor used when price is already near 52w low
_NEAR_LOW_BUFFER     = 1.15   # price within 15% above low_52w → use floor
_FLOOR_NEAR_LOW      = 0.85   # 15% below current price when near low
_FLOOR_FALLBACK      = 0.80   # 20% below current price when low_52w missing

# thesis_risk_level distance thresholds (invalidation_distance is always negative)
_TIGHT_ABS           = -15.0  # Rule R-TIGHT primary: < 15% gap
_TIGHT_VOL_DIST      = -25.0  # Rule R-TIGHT secondary: < 25% gap with high vol
_TIGHT_VOL_THRESH    =  50.0  # annualised vol threshold for secondary tight rule
_MOD_ABS             = -30.0  # Rule R-MODERATE primary: < 30% gap
_MOD_VOL_DIST        = -40.0  # Rule R-MODERATE secondary: < 40% gap with med-high vol
_MOD_VOL_THRESH      =  35.0  # annualised vol threshold for secondary moderate rule


# ── Skew filter constants ─────────────────────────────────────────────────────
# Applied when skew_regime or market tail_risk indicates elevated put demand.
# Encoded as pipe-delimited string: "multiplier|stop_tighten_pct|reason"
# Parseable by position_calculator.py in future without schema change.

_SKEW_FILTER_SPIKE_MULTIPLIER    = 0.0    # block long entirely
_SKEW_FILTER_ELEVATED_MULTIPLIER = 0.5    # halve long size
_SKEW_FILTER_STOP_TIGHTEN_PCT    = 20.0   # tighten stop by 20%

# Market tail_risk levels that trigger filter regardless of per-ticker skew
_TAIL_RISK_BLOCK  = {'extreme'}           # same as spike — full block on longs
_TAIL_RISK_REDUCE = {'elevated'}          # same as elevated — half size + tighten


def _compute_skew_filter_atoms(
    ticker: str,
    preds: Dict[str, str],
    market_atoms: Dict[str, str],
    signal_direction: str,
    src_base: str,
    meta: dict,
) -> List[RawAtom]:
    """
    Compute skew_filter atom for a ticker based on per-ticker and market skew.

    Encoding: "multiplier|stop_tighten_pct|reason"  (pipe-delimited)
      e.g.  "0.5|20.0|elevated_skew"
            "0.0|0.0|spike_skew"
            "0.0|0.0|market_tail_risk_extreme"

    Priority (worst case wins):
      1. Market tail_risk == extreme  → block all longs  (0.0|0.0|market_tail_risk_extreme)
      2. ticker skew_regime == spike  → block long       (0.0|0.0|spike_skew)
      3. Market tail_risk == elevated → reduce long      (0.5|20.0|market_tail_risk_elevated)
      4. ticker skew_regime == elevated → reduce long    (0.5|20.0|elevated_skew)

    Filter only applies to long/bullish directions — short positions are
    unaffected (elevated put demand can confirm bearish thesis).

    Returns empty list when:
      - No skew_regime atom exists for this ticker AND market tail_risk is normal
        (scheduler order: options_adapter may not have run yet — no false signals)
      - direction is not long/bullish
    """
    from ingest.base import RawAtom  # local to avoid circular import

    is_long = signal_direction in ('long', 'near_high', 'near_52w_high')

    ticker_skew = preds.get('skew_regime', '')       # '' = not yet computed
    tail_risk   = market_atoms.get('tail_risk', '')  # '' = not yet computed

    # No skew data at all → skip (options_adapter hasn't run yet)
    if not ticker_skew and not tail_risk:
        return []

    # Only filter long directions
    if not is_long:
        return []

    # Determine worst-case filter (priority order)
    multiplier    = 1.0
    stop_tighten  = 0.0
    reason        = ''

    if tail_risk in _TAIL_RISK_BLOCK:
        multiplier   = _SKEW_FILTER_SPIKE_MULTIPLIER
        stop_tighten = 0.0
        reason       = 'market_tail_risk_extreme'
    elif ticker_skew == 'spike':
        multiplier   = _SKEW_FILTER_SPIKE_MULTIPLIER
        stop_tighten = 0.0
        reason       = 'spike_skew'
    elif tail_risk in _TAIL_RISK_REDUCE:
        multiplier   = _SKEW_FILTER_ELEVATED_MULTIPLIER
        stop_tighten = _SKEW_FILTER_STOP_TIGHTEN_PCT
        reason       = 'market_tail_risk_elevated'
    elif ticker_skew == 'elevated':
        multiplier   = _SKEW_FILTER_ELEVATED_MULTIPLIER
        stop_tighten = _SKEW_FILTER_STOP_TIGHTEN_PCT
        reason       = 'elevated_skew'
    else:
        # Both normal — no filter needed
        return []

    encoded = f'{multiplier}|{stop_tighten}|{reason}'
    filter_meta = {
        **meta,
        'ticker_skew_regime': ticker_skew,
        'market_tail_risk':   tail_risk,
        'signal_direction':   signal_direction,
    }
    return [RawAtom(
        subject    = ticker,
        predicate  = 'skew_filter',
        object     = encoded,
        confidence = 0.70,
        source     = f'{src_base}_skew_filter',
        metadata   = filter_meta,
        upsert     = True,
    )]


def _compute_position_sizing_atoms(
    ticker: str,
    signal_quality: str,
    thesis_risk_level: str,
    macro_confirmation: str,
    vol_30d: Optional[float],
    src_base: str,
    meta: dict,
    insider_conviction: str = '',
    short_squeeze: str = '',
    macro_event_risk: str = '',
    sector_tailwind: str = '',
    db_path: str = '',
    dominant_pattern_type: str = '',
    dominant_timeframe: str = '',
) -> List[RawAtom]:
    """
    Compute conviction_tier, volatility_scalar, position_size_pct.

    CONVICTION TIER RULES (priority order, first match wins):
      CT-AVOID:
        CT-A1: signal_quality=weak   AND thesis_risk_level=tight
        CT-A2: signal_quality=conflicted (any risk level)
      CT-LOW:
        CT-L1: signal_quality=weak   (any risk level)
        CT-L2: thesis_risk_level=tight (any signal quality)
        CT-L3: macro_confirmation=unconfirmed (any signal quality)
      CT-MEDIUM:
        CT-M1: signal_quality=confirmed (any risk level)
        CT-M2: signal_quality=strong AND thesis_risk_level=tight
      CT-HIGH:
        CT-H1: signal_quality=strong
               AND thesis_risk_level IN (moderate, wide)
               AND macro_confirmation IN (confirmed, partial)
      no_data: any required dependency atom missing from KB

    VOLATILITY SCALAR:
      scalar = min(1.0, max(0.2, 20.0 / volatility_30d))
      Missing vol_30d → emit no_data, skip scalar and position_size_pct.

    POSITION SIZE PCT:
      base = {high: 5.0, medium: 3.0, low: 1.5, avoid: 0.0}[conviction_tier]
      size = round(base * scalar, 2)
      avoid always → 0.0 (not multiplied, hard-zeroed).
    """
    from ingest.base import RawAtom  # local to avoid circular import at module level
    atoms: List[RawAtom] = []

    sq  = signal_quality.lower()   if signal_quality   else ''
    rl  = thesis_risk_level.lower() if thesis_risk_level else ''
    mc  = macro_confirmation.lower() if macro_confirmation else ''

    # no_data guard: skip if any core dependency is missing
    if not sq or not rl:
        return atoms

    # Normalise new inputs
    ic  = (insider_conviction or '').lower()   # high|moderate|low|none
    ss  = (short_squeeze      or '').lower()   # high|moderate|low|minimal
    mer = (macro_event_risk   or '').lower()   # high|medium|low
    st  = (sector_tailwind    or '').lower()   # positive|negative|neutral

    # ── Classify conviction_tier ───────────────────────────────────────────────
    # CT-AVOID (highest priority)
    if sq == 'conflicted':
        tier = 'avoid'
        ct_rule = 'CT-A2_conflicted'
    elif sq == 'weak' and rl == 'tight':
        tier = 'avoid'
        ct_rule = 'CT-A1_weak_tight'
    # CT-LOW (single-bad-dimension cases)
    elif sq == 'weak':
        tier = 'low'
        ct_rule = 'CT-L1_weak'
    # CT-MEDIUM is checked BEFORE CT-L2 so that strong+tight → medium not low
    elif sq == 'strong' and rl == 'tight':
        tier = 'medium'
        ct_rule = 'CT-M2_strong_tight'
    elif sq == 'confirmed':
        tier = 'medium'
        ct_rule = 'CT-M1_confirmed'
    # CT-LOW continued (tight but not strong, extended, or macro unconfirmed)
    elif sq == 'extended':
        # Extended signal (bullish but near_52w_high) — legitimate setup only at
        # reduced size. 'medium' was giving these the same weight as confirmed signals;
        # ledger data shows near_52w_high bullish hit rate of 41.7% vs 65% overall.
        tier = 'low'
        ct_rule = 'CT-L4_extended'
    elif rl == 'tight':
        tier = 'low'
        ct_rule = 'CT-L2_tight_risk'
    elif mc == 'unconfirmed':
        tier = 'low'
        ct_rule = 'CT-L3_macro_unconfirmed'
    # CT-HIGH
    elif sq == 'strong' and rl in ('moderate', 'wide') and mc in ('confirmed', 'partial'):
        tier = 'high'
        ct_rule = 'CT-H1_strong_moderate_wide_macro'
    # Residual: strong signal but macro missing or no_data → medium
    else:
        tier = 'medium'
        ct_rule = 'CT-M_residual'

    # ── Conviction upgrade from new signals (applied after base tier) ──────────
    # Upgrade rule U1: insider_conviction=high + short_squeeze=high
    # (insider buy INTO a squeeze setup = very strong combo) → upgrade by one
    _TIER_ORDER = ['avoid', 'low', 'medium', 'high']
    if ic == 'high' and ss in ('high', 'moderate') and tier in ('low', 'medium'):
        idx = _TIER_ORDER.index(tier)
        tier = _TIER_ORDER[min(idx + 1, len(_TIER_ORDER) - 1)]
        ct_rule += '+U1_insider_squeeze'
    # Upgrade rule U2: sector_tailwind=positive + insider_conviction >= moderate
    elif st == 'positive' and ic in ('high', 'moderate') and tier == 'low':
        tier = 'medium'
        ct_rule += '+U2_sector_tailwind_insider'

    # ── Calibration-adjusted conviction (P2) ─────────────────────────────────
    # Uses a continuous log-ratio score against a *dynamic* pattern-level
    # baseline, with Bayesian prior smoothing to stabilise early samples.
    #
    # STEP 1 — Bayesian smoothing of the raw hit rate
    # ------------------------------------------------
    # adjusted_hr = (wins + prior_wins) / (n + prior_total)
    #
    # Prior encodes the global pattern baseline so early samples are pulled
    # toward the population mean rather than 0 or 1:
    #   prior_total = 10   (equivalent to 10 phantom observations)
    #   prior_wins  = baseline * prior_total
    #
    # Effect: n=2, wins=2 (raw hr=1.0) → adjusted_hr ≈ baseline, not 1.0.
    # Effect: n=200, wins=140 (raw hr=0.70) → adjusted_hr ≈ 0.698 (barely moved).
    #
    # STEP 2 — Dynamic baseline
    # -------------------------
    # baseline = get_global_baseline(pattern_type, timeframe)
    #          = sample-weighted mean hit_rate_t1 across ALL tickers (≥30 samples each)
    # Falls back to 0.50 when the calibration table is still sparse.
    #
    # STEP 3 — Log-ratio score
    # ------------------------
    # raw_score = log(adjusted_hr / baseline)   positive = outperforms baseline
    # clamped to [-1.0, +1.0] to cap leverage
    #
    # STEP 4 — Sample-size confidence weight
    # ---------------------------------------
    # weight = 1 - exp(-n / 30)
    # → n=10: 0.28 · n=30: 0.63 · n=100: 0.96
    # Prevents a 10-sample run from claiming the same confidence as 200 samples.
    #
    # STEP 5 — Tier decision (threshold gate)
    # ----------------------------------------
    # weighted_score = raw_score × weight
    #   ≥ +_CAL_THRESHOLD  → upgrade tier by 1   (max: high)
    #   ≤ -_CAL_THRESHOLD  → downgrade tier by 1 (min: avoid)
    #   |score| < threshold → label only, no shift (surfaced in ct_rule for LLM)
    #
    # Constants
    # TODO(prior-scaling): once baseline_sample_count is exposed by get_global_baseline(),
    #   replace the fixed prior with: _CAL_PRIOR_TOTAL = min(20, baseline_n // 10)
    #   This makes the prior stronger when the global baseline is itself well-supported
    #   (e.g. baseline built from 2000 samples → prior_total=20; from 50 → prior_total=5).
    #   Defer until baseline_n is routinely ≥200 across pattern types.
    _CAL_PRIOR_TOTAL = 10     # phantom sample count for Bayesian smoothing
    _CAL_FALLBACK    = 0.50   # used when global baseline unavailable
    _CAL_THRESHOLD   = 0.30   # |weighted_score| needed to shift a tier
    _CAL_TIER_ORDER  = ['avoid', 'low', 'medium', 'high']

    _cal_boost_label = ''
    if tier not in ('avoid',) and db_path:
        try:
            from analytics.signal_calibration import (
                get_calibration as _get_cal,
                get_global_baseline as _get_baseline,
            )
            import math as _math
            _pat_type = dominant_pattern_type or 'mitigation'
            _tf       = dominant_timeframe    or '1d'
            _cal = _get_cal(
                ticker        = ticker,
                pattern_type  = _pat_type,
                timeframe     = _tf,
                db_path       = db_path,
                market_regime = None,
            )
            _baseline = _get_baseline(_pat_type, _tf, db_path) or _CAL_FALLBACK
        except Exception:
            _cal      = None
            _baseline = _CAL_FALLBACK

        if _cal is not None and _cal.sample_size >= 10 and _cal.hit_rate_t1 is not None:
            n        = _cal.sample_size
            raw_hr   = _cal.hit_rate_t1

            # ── Bayesian smoothing ────────────────────────────────────────────
            # Reconstruct win count from incremental mean (best estimate),
            # then apply prior centred on the global baseline.
            prior_wins  = _baseline * _CAL_PRIOR_TOTAL
            wins_est    = raw_hr * n                          # estimated win count
            adj_hr      = (wins_est + prior_wins) / (n + _CAL_PRIOR_TOTAL)
            adj_hr      = max(0.01, min(0.99, adj_hr))       # guard log(0)

            # ── Log-ratio score ───────────────────────────────────────────────
            raw_score = _math.log(adj_hr / _baseline)
            raw_score = max(-1.0, min(1.0, raw_score))       # cap leverage

            # ── Sample-size confidence weight ─────────────────────────────────
            weight = 1.0 - _math.exp(-n / 30.0)

            weighted_score = raw_score * weight

            idx = _CAL_TIER_ORDER.index(tier) if tier in _CAL_TIER_ORDER else 1

            if weighted_score >= _CAL_THRESHOLD:
                if idx < 3:
                    tier = _CAL_TIER_ORDER[idx + 1]
                _cal_boost_label = (
                    f'cal_boost_lr{weighted_score:+.2f}'
                    f'_ahr{adj_hr:.2f}_base{_baseline:.2f}_n{n}'
                )
            elif weighted_score <= -_CAL_THRESHOLD:
                if idx > 0:
                    tier = _CAL_TIER_ORDER[idx - 1]
                _cal_boost_label = (
                    f'cal_penalise_lr{weighted_score:+.2f}'
                    f'_ahr{adj_hr:.2f}_base{_baseline:.2f}_n{n}'
                )
            else:
                _cal_boost_label = (
                    f'cal_watch_lr{weighted_score:+.2f}'
                    f'_ahr{adj_hr:.2f}_base{_baseline:.2f}_n{n}'
                )

    if _cal_boost_label:
        ct_rule += f'+{_cal_boost_label}'

    # ── Macro event risk position reduction ───────────────────────────────────
    # If FOMC/CPI/NFP within 3 days, reduce position size by 30% (size adjusted below)
    _macro_event_reduction = 0.70 if mer == 'high' else (0.85 if mer == 'medium' else 1.0)

    # ── Compute volatility_scalar ──────────────────────────────────────────────
    if vol_30d is None or vol_30d <= 0:
        # Can't compute scalar — emit conviction_tier only
        ct_meta = {**meta, 'ct_rule': ct_rule, 'vol_30d': 'missing',
                   'insider_conviction': ic, 'short_squeeze': ss,
                   'macro_event_risk': mer, 'sector_tailwind': st}
        atoms.append(RawAtom(
            subject    = ticker,
            predicate  = 'conviction_tier',
            object     = tier,
            confidence = 0.70,
            source     = f'{src_base}_conviction',
            metadata   = ct_meta,
            upsert     = True,
        ))
        return atoms

    scalar = round(min(_VOL_SCALAR_CAP, max(_VOL_SCALAR_FLOOR, _VOL_REF / vol_30d)), 4)

    # ── Compute position_size_pct ──────────────────────────────────────────────
    if tier == 'avoid':
        size = 0.0
    else:
        size = round(_CT_BASE_ALLOC[tier] * scalar * _macro_event_reduction, 2)

    ps_meta = {**meta, 'ct_rule': ct_rule, 'vol_scalar': str(scalar),
               'base_alloc': str(_CT_BASE_ALLOC.get(tier, 0.0)),
               'vol_30d_input': str(vol_30d),
               'insider_conviction': ic, 'short_squeeze': ss,
               'macro_event_risk': mer, 'sector_tailwind': st,
               'macro_event_reduction': str(_macro_event_reduction)}

    atoms.append(RawAtom(
        subject    = ticker,
        predicate  = 'conviction_tier',
        object     = tier,
        confidence = 0.70,
        source     = f'{src_base}_conviction',
        metadata   = ps_meta,
        upsert     = True,
    ))
    atoms.append(RawAtom(
        subject    = ticker,
        predicate  = 'volatility_scalar',
        object     = str(scalar),
        confidence = 0.75,
        source     = f'{src_base}_vol_scalar',
        metadata   = ps_meta,
        upsert     = True,
    ))
    atoms.append(RawAtom(
        subject    = ticker,
        predicate  = 'position_size_pct',
        object     = str(size),
        confidence = 0.75,
        source     = f'{src_base}_position_size',
        metadata   = ps_meta,
        upsert     = True,
    ))
    return atoms


def _compute_invalidation_atoms(
    ticker: str,
    last_price: Optional[float],
    low_52w: Optional[float],
    vol_30d: Optional[float],
    price_regime: str,
    src_base: str,
    meta: dict,
) -> List[RawAtom]:
    """
    Compute invalidation_price, invalidation_distance, thesis_risk_level.

    INVALIDATION PRICE RULES (priority order):
      IP1 — 52w low anchor: if low_52w available AND price is not already
            within _NEAR_LOW_BUFFER (15%) of that low, use low_52w.
            This is the structural support level; break below it invalidates
            the long thesis definitively.
      IP2 — Near-low floor: if price IS within 15% of low_52w (already near
            support), use last_price * _FLOOR_NEAR_LOW (15% below current).
            Avoids setting an invalidation level that's already nearly breached.
      IP3 — Fallback: low_52w not in KB yet (pre-backfill). Use
            last_price * _FLOOR_FALLBACK (20% below current).

    THESIS RISK LEVEL RULES (priority order, first match wins):
      R-TIGHT:
        - invalidation_distance > _TIGHT_ABS (-15%)                 [R-T1]
        - OR distance > _TIGHT_VOL_DIST (-25%) AND vol > 50%        [R-T2]
        - OR price_regime == near_52w_low                           [R-T3]
      R-MODERATE:
        - distance > _MOD_ABS (-30%)                                [R-M1]
        - OR distance > _MOD_VOL_DIST (-40%) AND vol > 35%          [R-M2]
      R-WIDE: all remaining cases                                   [R-W1]
    """
    from ingest.base import RawAtom  # local to avoid circular import at module level
    atoms: List[RawAtom] = []

    if last_price is None or last_price <= 0:
        return atoms

    # ── Compute invalidation_price ─────────────────────────────────────────────
    if low_52w is not None and low_52w > 0:
        if last_price > low_52w * _NEAR_LOW_BUFFER:
            # Rule IP1: structural 52w low anchor
            inv_price = round(low_52w, 2)
            inv_rule  = 'IP1_52w_low'
        else:
            # Rule IP2: near-low floor (15% below current)
            inv_price = round(last_price * _FLOOR_NEAR_LOW, 2)
            inv_rule  = 'IP2_near_low_floor'
    else:
        # Rule IP3: no 52w low in KB yet — 20% floor
        inv_price = round(last_price * _FLOOR_FALLBACK, 2)
        inv_rule  = 'IP3_fallback_floor'

    # ── Compute invalidation_distance ──────────────────────────────────────────
    inv_dist = round((inv_price - last_price) / last_price * 100, 2)

    # ── Classify thesis_risk_level ─────────────────────────────────────────────
    vol = vol_30d  # annualised %, may be None

    # Rule R-T3: already near 52w low — always tight regardless of distance
    if price_regime == 'near_52w_low':
        risk_level = 'tight'
        risk_rule  = 'R-T3_near_52w_low'
    # Rule R-T1: very close to invalidation
    elif inv_dist > _TIGHT_ABS:
        risk_level = 'tight'
        risk_rule  = 'R-T1_dist_lt15pct'
    # Rule R-T2: moderate distance but very high vol closes gap quickly
    elif inv_dist > _TIGHT_VOL_DIST and vol is not None and vol > _TIGHT_VOL_THRESH:
        risk_level = 'tight'
        risk_rule  = 'R-T2_vol_gt50_dist_lt25pct'
    # Rule R-M1: meaningful but not large distance
    elif inv_dist > _MOD_ABS:
        risk_level = 'moderate'
        risk_rule  = 'R-M1_dist_lt30pct'
    # Rule R-M2: larger distance but elevated vol can close it within weeks
    elif inv_dist > _MOD_VOL_DIST and vol is not None and vol > _MOD_VOL_THRESH:
        risk_level = 'moderate'
        risk_rule  = 'R-M2_vol_gt35_dist_lt40pct'
    # Rule R-W1: wide invalidation
    else:
        risk_level = 'wide'
        risk_rule  = 'R-W1_wide'

    inv_meta = {**meta, 'inv_rule': inv_rule, 'risk_rule': risk_rule,
                'inv_price': str(inv_price), 'inv_dist': str(inv_dist),
                'vol_30d': str(vol)}

    atoms.append(RawAtom(
        subject    = ticker,
        predicate  = 'invalidation_price',
        object     = str(inv_price),
        confidence = 0.80,
        source     = f'{src_base}_invalidation_price',
        metadata   = inv_meta,
        upsert     = True,
    ))
    atoms.append(RawAtom(
        subject    = ticker,
        predicate  = 'invalidation_distance',
        object     = str(inv_dist),
        confidence = 0.80,
        source     = f'{src_base}_invalidation_dist',
        metadata   = inv_meta,
        upsert     = True,
    ))
    atoms.append(RawAtom(
        subject    = ticker,
        predicate  = 'thesis_risk_level',
        object     = risk_level,
        confidence = 0.70,
        source     = f'{src_base}_thesis_risk',
        metadata   = inv_meta,
        upsert     = True,
    ))
    return atoms


def _classify_price_regime(
    last_price: Optional[float],
    price_target: Optional[float],
    signal_direction: str,
) -> str:
    """
    Classify where current price sits within its expected range.

    Uses price_target as a proxy for fair value when 52w high/low is
    unavailable (yfinance metadata not stored as KB atoms).

    Decision rules:
      near_52w_high  → price >= 95% of price_target  (at or above fair value)
      near_52w_low   → price <= 75% of price_target  (well below fair value)
      mid_range      → everything between

    Falls back to signal_direction mapping when price/target missing:
      long/near_high → mid_range (assume not yet extended)
      neutral        → mid_range
      short/near_low → near_52w_low
    """
    if last_price is not None and price_target is not None and price_target > 0:
        ratio = last_price / price_target
        if ratio >= 0.95:
            return 'near_52w_high'
        elif ratio <= 0.75:
            return 'near_52w_low'
        else:
            return 'mid_range'

    # Fallback: infer from signal_direction
    if signal_direction in _BEARISH_SIGNALS:
        return 'near_52w_low'
    return 'mid_range'


def _classify_signal_quality(
    signal_direction: str,
    volatility_regime: str,
    price_regime: str,
    upside_pct: Optional[float],
) -> str:
    """
    Classify the coherence of a ticker's signal composite.

    Rules are applied in priority order — first match wins.
    See module docstring for full decision rule documentation.
    """
    is_bullish = signal_direction in _BULLISH_SIGNALS
    is_bearish = signal_direction in _BEARISH_SIGNALS
    is_neutral = not is_bullish and not is_bearish

    # ── CONFLICTED: internal contradictions take highest priority ────────────
    # Rule C1: bullish signal but price already above consensus target
    if is_bullish and upside_pct is not None and upside_pct < _UPSIDE_CONFLICT:
        return 'conflicted'
    # Rule C2: bearish signal but large upside gap remaining (target >> price)
    if is_bearish and upside_pct is not None and upside_pct >= _UPSIDE_STRONG:
        return 'conflicted'
    # Rule C3: bullish signal + high volatility + price already extended
    if (is_bullish
            and volatility_regime == _HIGH_VOL_REGIME
            and price_regime == 'near_52w_high'):
        return 'conflicted'

    # ── EXTENDED: price already near top, signal still bullish ───────────────
    # Rule E1: bullish signal but price regime says we're already near the high
    if is_bullish and price_regime == 'near_52w_high':
        return 'extended'

    # ── STRONG: all conditions clearly supportive ─────────────────────────────
    # Rule S1: bullish + large upside + not extended + not high vol
    if (is_bullish
            and upside_pct is not None and upside_pct >= _UPSIDE_STRONG
            and price_regime != 'near_52w_high'
            and volatility_regime in _LOW_VOL_REGIMES):
        return 'strong'

    # ── CONFIRMED: signal and upside aligned, price not extended ─────────────
    # Rule CF1: bullish + moderate upside + not extended
    if (is_bullish
            and upside_pct is not None and upside_pct >= _UPSIDE_CONFIRMED
            and price_regime != 'near_52w_high'):
        return 'confirmed'

    # ── WEAK: neutral signal or missing data ─────────────────────────────────
    # Rule W1: neutral direction or no upside data available
    if is_neutral or upside_pct is None:
        return 'weak'

    # ── Bearish cases ─────────────────────────────────────────────────────────
    # Bearish + not conflicted = confirmed (downside confirmed)
    if is_bearish:
        return 'confirmed'

    return 'weak'


def _classify_macro_confirmation(
    equity_signal: str,
    macro_signals: Dict[str, str],
) -> str:
    """
    Assess whether equity signal direction aligns with cross-asset macro proxies.

    Proxies checked:
      HYG (credit):   near_high / not near_low  → risk-on  → confirms long equity
      TLT (rates):    near_high = rates falling (risk-off)  → contradicts long equity
                      NOT near_high (mid_range/near_low)   → confirms long equity
      SPY (market):   near_high / long           → confirms long equity

    Scoring:
      Each proxy that confirms adds +1, contradicts adds -1, missing = 0.
      confirmed   : score == 3   (all three proxies confirm)
      partial     : score == 1 or 2
      unconfirmed : score <= 0
      no_data     : all three proxies missing from KB
    """
    hyg_sig = macro_signals.get(_CREDIT_PROXY, '')
    tlt_sig = macro_signals.get(_RATES_PROXY, '')
    spy_sig = macro_signals.get(_MARKET_PROXY, '')

    if not any([hyg_sig, tlt_sig, spy_sig]):
        return 'no_data'

    is_bullish_equity = equity_signal in _BULLISH_SIGNALS
    score = 0
    proxies_present = 0

    # HYG: near_high = credit spreads tight = risk-on → confirms long equity
    if hyg_sig:
        proxies_present += 1
        if is_bullish_equity:
            score += 1 if hyg_sig in _BULLISH_SIGNALS else -1
        else:
            score += 1 if hyg_sig in _BEARISH_SIGNALS else -1

    # TLT: near_high = rates falling = risk-off → CONTRADICTS long equity
    # TLT NOT near_high (rates stable/rising) → confirms long equity
    if tlt_sig:
        proxies_present += 1
        if is_bullish_equity:
            # Rising bonds (near_high) = risk-off = bad for longs
            score += -1 if tlt_sig in _BULLISH_SIGNALS else 1
        else:
            # Falling bonds = risk-on = bad for shorts
            score += 1 if tlt_sig in _BULLISH_SIGNALS else -1

    # SPY: near_high = broad market up → confirms long equity
    if spy_sig:
        proxies_present += 1
        if is_bullish_equity:
            score += 1 if spy_sig in _BULLISH_SIGNALS else -1
        else:
            score += 1 if spy_sig in _BEARISH_SIGNALS else -1

    if proxies_present == 0:
        return 'no_data'
    if score >= proxies_present:
        return 'confirmed'
    elif score > 0:
        return 'partial'
    else:
        return 'unconfirmed'


# ── Market regime constants ───────────────────────────────────────────────────

# Additional macro proxy tickers for regime model
_GOLD_PROXY  = 'gld'   # gold — near_high = inflation / risk-off hedge bid
_USD_PROXY   = 'uup'   # US dollar index — near_high = strong USD (risk-off)

# Regime labels
_REGIME_RISK_ON_EXPANSION   = 'risk_on_expansion'
_REGIME_RISK_OFF_CONTRACTION = 'risk_off_contraction'
_REGIME_STAGFLATION         = 'stagflation'
_REGIME_RECOVERY            = 'recovery'
_REGIME_NO_DATA             = 'no_data'


def _classify_market_regime(
    macro_signals: Dict[str, str],
    ticker_atoms:  Dict[str, Dict[str, str]],
) -> str:
    """
    Classify the current market into one of four macro regimes.

    Uses existing KB atoms — no external API calls.

    INPUTS
    ======
    Proxy signals (from ticker_atoms):
      SPY  — broad equity market:   near_high/long = bullish equity
      HYG  — credit spreads:        near_high = tight spreads = risk-on
      TLT  — long-duration rates:   near_high = rates falling = risk-off
      GLD  — gold:                  near_high = inflation / risk-off hedge
      UUP  — US dollar:             near_high = strong USD = risk-off

    REGIME DECISION MATRIX (priority order, first match wins)
    ==========================================================
    RISK_ON_EXPANSION:
      SPY=bullish + HYG=bullish + TLT≠bullish
      "Equities up, credit tight, rates not collapsing — normal expansion"

    RISK_OFF_CONTRACTION:
      SPY=bearish + HYG=bearish
      "Equities down AND credit selling off — genuine risk-off"

    STAGFLATION:
      GLD=bullish + SPY≠bullish
      "Gold bidding without equity support — inflation / stagflation regime"

    RECOVERY:
      SPY=bullish + HYG=bullish + TLT=bullish
      "Equities up + credit tight + rates falling — early recovery / Fed pivot"
      Note: TLT bullish here distinguishes recovery from expansion (rates still
      easing in early recovery).

    NO_DATA:
      Insufficient proxy signals in KB (SPY and HYG both missing)

    RESIDUAL (no rule matches):
      Returns 'recovery' as the neutral/uncertain state.

    Returns
    -------
    str — one of: risk_on_expansion | risk_off_contraction |
                  stagflation | recovery | no_data
    """
    def _sig_from_atoms(proxy: str) -> str:
        """Get signal direction, falling back to conviction_tier derivation."""
        atoms = ticker_atoms.get(proxy, {})
        sig = atoms.get('signal_direction', '')
        if sig:
            return sig
        ct = atoms.get('conviction_tier', '')
        if ct in ('high', 'confirmed', 'strong', 'medium'):
            return 'bullish'
        if ct in ('avoid', 'low'):  # low = weak/not-tradeable = bearish lean for regime
            return 'bearish'
        return ''

    # Build extended proxy signals — prefer signal_direction, fall back to conviction_tier
    spy_sig = macro_signals.get(_MARKET_PROXY) or _sig_from_atoms(_MARKET_PROXY)
    hyg_sig = macro_signals.get(_CREDIT_PROXY) or _sig_from_atoms(_CREDIT_PROXY)
    tlt_sig = macro_signals.get(_RATES_PROXY)  or _sig_from_atoms(_RATES_PROXY)
    gld_sig = _sig_from_atoms(_GOLD_PROXY)
    uup_sig = _sig_from_atoms(_USD_PROXY)

    if not spy_sig and not hyg_sig:
        # Only true no_data if conviction_tier also absent for both proxies
        if not ticker_atoms.get(_MARKET_PROXY) and not ticker_atoms.get(_CREDIT_PROXY):
            return _REGIME_NO_DATA
        # Partial data — fall through to classification

    spy_bull = spy_sig in _BULLISH_SIGNALS
    spy_bear = spy_sig in _BEARISH_SIGNALS
    hyg_bull = hyg_sig in _BULLISH_SIGNALS
    hyg_bear = hyg_sig in _BEARISH_SIGNALS
    tlt_bull = tlt_sig in _BULLISH_SIGNALS
    gld_bull = gld_sig in _BULLISH_SIGNALS

    # RECOVERY: equities up + credit tight + rates falling (Fed-pivot / early cycle)
    if spy_bull and hyg_bull and tlt_bull:
        return _REGIME_RECOVERY

    # RISK_ON_EXPANSION: equities up + credit tight + rates NOT falling
    if spy_bull and hyg_bull and not tlt_bull:
        return _REGIME_RISK_ON_EXPANSION

    # RISK_OFF_CONTRACTION: equities down + credit selling off
    if spy_bear and hyg_bear:
        return _REGIME_RISK_OFF_CONTRACTION

    # STAGFLATION: gold bidding without equity support
    if gld_bull and not spy_bull:
        return _REGIME_STAGFLATION

    # Residual: equities up but mixed credit, or data too thin
    if spy_bull:
        return _REGIME_RISK_ON_EXPANSION

    return _REGIME_RECOVERY


def _classify_earnings_proximity(next_earnings_str: Optional[str]) -> Optional[str]:
    """
    Classify how close today is to the next earnings event.

    Uses the earnings_quality atom value which yfinance stores as an ISO date
    string (e.g. "2026-04-30").  Computes calendar days from today UTC.

    Values (first match wins):
      pre_earnings_3d  — earnings in ≤ 3 calendar days  (binary event risk)
      pre_earnings_2w  — earnings in 4–14 days           (event risk window)
      pre_earnings_8w  — earnings in 15–56 days          (approaching)
      post_earnings    — earnings date is in the past    (catalyst clear)
      no_catalyst      — no earnings date in KB          (or date unparseable)

    Returns None when earnings date missing so the caller can skip emission.
    """
    if not next_earnings_str:
        return None
    try:
        earnings_date = _date.fromisoformat(str(next_earnings_str).strip())
    except (ValueError, TypeError):
        return None
    today = datetime.now(timezone.utc).date()
    delta = (earnings_date - today).days
    if delta < 0:
        return 'post_earnings'
    if delta <= 3:
        return 'pre_earnings_3d'
    if delta <= 14:
        return 'pre_earnings_2w'
    if delta <= 56:
        return 'pre_earnings_8w'
    return 'no_catalyst'


# Minimum LLM atoms required before emitting a sentiment atom
_SENTIMENT_MIN_ATOMS = 3

# Predicates that carry a bullish lean when present in LLM-extracted atoms
_SENTIMENT_BULLISH_PREDS = frozenset({
    'catalyst', 'earnings_beat', 'revenue_beat', 'guidance_raised',
    'buyback', 'dividend_increase', 'new_product', 'market_share_gain',
})
# Predicates that carry a bearish lean
_SENTIMENT_BEARISH_PREDS = frozenset({
    'risk_factor', 'earnings_miss', 'revenue_miss', 'guidance_lowered',
    'restructuring', 'investigation', 'litigation', 'leadership_change',
})


def _compute_news_sentiment(
    ticker: str,
    db_path: str,
) -> Optional[str]:
    """
    Derive news_sentiment from the most recent LLM-extracted atoms for ticker.

    Reads up to 10 most recent facts rows where source LIKE 'llm_extracted_%'
    for this ticker, ordered by timestamp DESC.  Scores each row by predicate:
      bullish predicate → +confidence
      bearish predicate → -confidence
      other predicate   → +0  (no contribution)

    Net score:
      > +0.5  → 'bullish'
      < -0.5  → 'bearish'
      else    → 'neutral'

    Returns None if fewer than _SENTIMENT_MIN_ATOMS rows found, so the caller
    can skip emission rather than emit a noisy neutral on thin data.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        c = conn.cursor()
        c.execute("""
            SELECT predicate, confidence
            FROM facts
            WHERE subject = ?
              AND source LIKE 'llm_extracted_%'
            ORDER BY timestamp DESC
            LIMIT 10
        """, (ticker.lower(),))
        rows = c.fetchall()
    finally:
        conn.close()

    if len(rows) < _SENTIMENT_MIN_ATOMS:
        return None

    score = 0.0
    for pred, conf in rows:
        pred_lower = (pred or '').lower()
        try:
            w = float(conf) if conf is not None else 0.5
        except (ValueError, TypeError):
            w = 0.5
        if pred_lower in _SENTIMENT_BULLISH_PREDS:
            score += w
        elif pred_lower in _SENTIMENT_BEARISH_PREDS:
            score -= w

    if score > 0.5:
        return 'bullish'
    if score < -0.5:
        return 'bearish'
    return 'neutral'


# ── Geopolitical risk enrichment ─────────────────────────────────────────────

def _read_geo_atoms(db_path: str) -> Dict[str, str]:
    """Read gdelt_tension, ucdp_conflict, acled_unrest, and eia_energy atoms from KB."""
    geo: Dict[str, str] = {}
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT subject, predicate, object
            FROM facts
            WHERE subject IN ('gdelt_tension', 'ucdp_conflict', 'acled_unrest', 'oil_market', 'macro_regime')
            ORDER BY subject, predicate, confidence DESC
        """)
        for row in c.fetchall():
            key = f"{row['subject']}:{row['predicate']}"
            if key not in geo:
                geo[key] = row['object']
        conn.close()
    except Exception as exc:
        _logger.warning('_read_geo_atoms failed: %s', exc)
    return geo


def _compute_geo_risk_atoms(db_path: str, now_iso: str) -> List[RawAtom]:
    """
    Derive geopolitical_risk_exposure per ticker and energy_shock_risk macro atom.

    Logic:
    - For each ticker in TICKER_GEO_EXPOSURE, check all mapped regions
    - Elevated if any GDELT pair score >= 60 OR any UCDP active_war in region
    - Moderate if any GDELT pair score >= 35
    - Low otherwise
    - energy_shock_risk: EIA wti_crude > 90 AND (Middle East GDELT score >= 55 OR supply_trend tight)
    """
    try:
        from ingest.geo_exposure import (
            TICKER_GEO_EXPOSURE, REGION_TO_GDELT_PAIRS,
            GEO_RISK_ELEVATED_THRESHOLD, GEO_RISK_MODERATE_THRESHOLD,
        )
    except ImportError:
        _logger.warning('geo_exposure.py not found — skipping geo-risk pass')
        return []

    geo = _read_geo_atoms(db_path)
    atoms: List[RawAtom] = []
    source = 'derived_signal_geo_risk'
    meta_base = {'as_of': now_iso}

    # Pre-compute region risk levels from GDELT and UCDP atoms
    region_risk: Dict[str, str] = {}

    for region, gdelt_pair_keys in REGION_TO_GDELT_PAIRS.items():
        max_score = 0.0
        has_active_war = False

        for pair_key in gdelt_pair_keys:
            val = geo.get(f'gdelt_tension:{pair_key}')
            if val is not None:
                try:
                    max_score = max(max_score, float(val))
                except ValueError:
                    pass

        # Check UCDP active wars mapped to this region
        _REGION_COUNTRIES: Dict[str, List[str]] = {
            'europe_east': ['ukr', 'rus'],
            'middle_east': ['syr', 'yem', 'irq', 'pse'],
            'africa':      ['sdn', 'ssd', 'eth', 'mli', 'ner', 'nga', 'moz', 'som', 'caf', 'cod'],
            'asia_south':  ['afg', 'pak'],
            'asia_east':   ['mmr'],
            'latam':       ['mex', 'col', 'hti'],
            'global_defence': [],  # no country filter — any war counts
        }
        war_countries = _REGION_COUNTRIES.get(region, [])
        if region == 'global_defence':
            # Elevated if any country has active_war globally
            has_active_war = any(
                v == 'active_war'
                for k, v in geo.items()
                if k.startswith('ucdp_conflict:')
            )
        else:
            has_active_war = any(
                geo.get(f'ucdp_conflict:{iso}') == 'active_war'
                for iso in war_countries
            )

        if has_active_war or max_score >= GEO_RISK_ELEVATED_THRESHOLD:
            region_risk[region] = 'elevated'
        elif max_score >= GEO_RISK_MODERATE_THRESHOLD:
            region_risk[region] = 'moderate'
        else:
            region_risk[region] = 'low'

    # Emit geopolitical_risk_exposure per ticker
    for ticker, regions in TICKER_GEO_EXPOSURE.items():
        # Take the worst risk level across all mapped regions
        levels = [region_risk.get(r, 'low') for r in regions]
        if 'elevated' in levels:
            exposure = 'elevated'
        elif 'moderate' in levels:
            exposure = 'moderate'
        else:
            exposure = 'low'

        # Only emit non-low — avoids flooding KB with low-value atoms
        if exposure in ('elevated', 'moderate'):
            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = 'geopolitical_risk_exposure',
                object     = exposure,
                confidence = 0.68,
                source     = source,
                metadata   = {
                    **meta_base,
                    'regions': regions,
                    'region_risks': {r: region_risk.get(r, 'low') for r in regions},
                },
                upsert     = True,
            ))

    # ── Commodity-macro cross signal: energy_shock_risk ───────────────────────
    # Elevated when: WTI > 90 AND (Middle East GDELT tension elevated OR supply tight)
    wti_str     = geo.get('oil_market:wti_crude')
    supply_str  = geo.get('oil_market:supply_trend')
    me_risk     = region_risk.get('middle_east', 'low')

    try:
        wti = float(wti_str) if wti_str else None
    except ValueError:
        wti = None

    if wti is not None:
        if wti > 90 and (me_risk == 'elevated' or supply_str == 'falling'):
            shock_level = 'elevated'
        elif wti > 80 and me_risk in ('elevated', 'moderate'):
            shock_level = 'moderate'
        else:
            shock_level = 'low'

        if shock_level in ('elevated', 'moderate'):
            atoms.append(RawAtom(
                subject    = 'macro_regime',
                predicate  = 'energy_shock_risk',
                object     = shock_level,
                confidence = 0.72,
                source     = source,
                metadata   = {
                    **meta_base,
                    'wti_crude': wti,
                    'supply_trend': supply_str,
                    'middle_east_tension': me_risk,
                },
                upsert     = True,
            ))

    _logger.info('[signal_enrichment] geo-risk pass: %d atoms (%d tickers checked)',
                 len(atoms), len(TICKER_GEO_EXPOSURE))
    return atoms


class SignalEnrichmentAdapter(BaseIngestAdapter):
    """
    Second-order signal enrichment. Reads current KB atoms and emits
    regime-conditional, interpretable signal atoms:

      price_regime       — where price sits vs fair value range
      upside_pct         — % upside to consensus target
      signal_quality     — coherence of signal composite (strong/confirmed/
                           extended/conflicted/weak)
      macro_confirmation — cross-asset alignment (confirmed/partial/
                           unconfirmed/no_data)

    All atoms: upsert=True, source prefix 'derived_signal_' (authority 0.65).
    No external API calls — reads only from the KB itself.
    """

    def __init__(self, tickers: Optional[List[str]] = None, db_path: str = 'trading_knowledge.db'):
        super().__init__(name='signal_enrichment')
        self._db_path = db_path
        # If tickers supplied, only enrich those; otherwise enrich everything in KB
        self._tickers = [t.lower() for t in tickers] if tickers else None

    def fetch(self) -> List[RawAtom]:
        now_iso = datetime.now(timezone.utc).isoformat()

        ticker_atoms, macro_signals = _read_kb_atoms(self._db_path)

        # Extract market-level atoms (subject='market') for skew filter.
        # tail_risk and spy_skew_regime are written by OptionsAdapter — may be
        # absent on startup (1800s interval vs 300s here).  Empty dict is safe;
        # _compute_skew_filter_atoms handles missing keys gracefully.
        market_atoms: Dict[str, str] = ticker_atoms.get('market', {})

        atoms: List[RawAtom] = []
        enriched = 0
        skipped  = 0

        # Pre-fetch dominant pattern type per ticker (most common open pattern)
        _dominant_patterns: dict = {}
        try:
            _pconn = sqlite3.connect(self._db_path, timeout=10)
            _rows = _pconn.execute("""
                SELECT ticker, pattern_type, timeframe, COUNT(*) as cnt
                FROM pattern_signals
                WHERE status NOT IN ('filled','broken','expired')
                GROUP BY ticker, pattern_type, timeframe
                ORDER BY ticker, cnt DESC
            """).fetchall()
            _pconn.close()
            for _pticker, _ptype, _ptf, _ in _rows:
                if _pticker.lower() not in _dominant_patterns:
                    _dominant_patterns[_pticker.lower()] = (_ptype, _ptf)
        except Exception:
            pass

        for ticker, preds in ticker_atoms.items():
            # Skip empty or invalid ticker subjects
            if not ticker or not ticker.strip():
                skipped += 1
                continue
            # Skip subjects that have no signal_direction (not an equity/ETF)
            if 'signal_direction' not in preds and 'last_price' not in preds:
                skipped += 1
                continue
            # Honour ticker filter if set
            if self._tickers and ticker not in self._tickers:
                continue

            # ── Extract raw values ─────────────────────────────────────────
            signal_dir  = preds.get('signal_direction', '')
            vol_regime  = preds.get('volatility_regime', '')

            last_price: Optional[float] = None
            price_target: Optional[float] = None
            upside_pct_val: Optional[float] = None
            low_52w: Optional[float] = None
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
                low_52w = float(preds['low_52w'])
            except (KeyError, ValueError, TypeError):
                pass

            try:
                vol_30d = float(preds['volatility_30d'])
            except (KeyError, ValueError, TypeError):
                pass

            if last_price is not None and price_target is not None and last_price > 0:
                upside_pct_val = round((price_target - last_price) / last_price * 100, 2)

            # ── Compute derived atoms ──────────────────────────────────────
            price_regime = _classify_price_regime(last_price, price_target, signal_dir)
            sig_quality  = _classify_signal_quality(
                signal_dir, vol_regime, price_regime, upside_pct_val
            )

            # ── Cross-engine input 1: signal_conflicted → force 'conflicted' ──
            # Written by contradiction.py when signal_direction conflicts are detected.
            # Overrides any quality computed above — resolves on next enrichment cycle
            # once the stale atom decays or new evidence breaks the tie.
            if preds.get('signal_conflicted'):
                sig_quality = 'conflicted'

            # ── Cross-engine input 2: causal_signal → boost direction quality ─
            # CausalShockEngine writes causal_signal atoms when a macro shock
            # propagates through the causal graph to this ticker.
            # If the causal direction matches the existing signal, upgrade quality.
            causal_sig = preds.get('causal_signal', '')
            if causal_sig and sig_quality not in ('conflicted',):
                causal_lower = causal_sig.lower()
                sig_is_bull = signal_dir in _BULLISH_SIGNALS
                sig_is_bear = signal_dir in _BEARISH_SIGNALS
                causal_bull = any(kw in causal_lower for kw in ('bullish', 'positive', 'expand', 'upside', 'boost'))
                causal_bear = any(kw in causal_lower for kw in ('bearish', 'negative', 'contract', 'downside', 'drag'))
                _TIER_Q = ['weak', 'confirmed', 'strong']
                if (sig_is_bull and causal_bull) or (sig_is_bear and causal_bear):
                    idx_q = _TIER_Q.index(sig_quality) if sig_quality in _TIER_Q else 1
                    sig_quality = _TIER_Q[min(idx_q + 1, len(_TIER_Q) - 1)]

            macro_conf   = _classify_macro_confirmation(signal_dir, macro_signals)

            src_base = f'derived_signal_{ticker}'
            meta = {
                'as_of':           now_iso,
                'input_signal':    signal_dir,
                'input_vol':       vol_regime,
                'input_price':     str(last_price),
                'input_target':    str(price_target),
                'upside_pct':      str(upside_pct_val),
                'price_regime':    price_regime,
                'macro_signals':   str(macro_signals),
            }

            # price_regime atom
            atoms.append(RawAtom(
                subject    = ticker,
                predicate  = 'price_regime',
                object     = price_regime,
                confidence = 0.75,   # derived, not observed
                source     = f'{src_base}_price_regime',
                metadata   = meta,
                upsert     = True,
            ))

            # upside_pct atom (only emit when computable)
            if upside_pct_val is not None:
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'upside_pct',
                    object     = str(upside_pct_val),
                    confidence = 0.75,
                    source     = f'{src_base}_upside',
                    metadata   = meta,
                    upsert     = True,
                ))

            # signal_quality atom (only emit when signal_direction exists)
            if signal_dir:
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'signal_quality',
                    object     = sig_quality,
                    confidence = 0.70,   # composite of three inputs
                    source     = f'{src_base}_quality',
                    metadata   = meta,
                    upsert     = True,
                ))

            # macro_confirmation atom (only emit when signal_direction exists)
            if signal_dir:
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'macro_confirmation',
                    object     = macro_conf,
                    confidence = 0.65,   # depends on proxy coverage
                    source     = f'{src_base}_macro_confirm',
                    metadata   = meta,
                    upsert     = True,
                ))

            # ── Invalidation layer atoms ───────────────────────────────────
            # Requires last_price; emits invalidation_price, invalidation_distance,
            # thesis_risk_level. Uses low_52w from HistoricalBackfillAdapter if
            # present, otherwise falls back to percentage-based floors.
            inv_atoms = _compute_invalidation_atoms(
                ticker, last_price, low_52w, vol_30d, price_regime, src_base, meta
            )
            atoms.extend(inv_atoms)

            # ── Position sizing atoms ──────────────────────────────────────
            # Depends on: signal_quality (just computed), thesis_risk_level
            # (freshly emitted by inv_atoms above or KB round-trip if present),
            # macro_confirmation (just computed), and volatility_30d.
            # thesis_risk_level: prefer the freshly computed value from inv_atoms
            # (inv_atoms[2].object) — falls back to KB round-trip via preds.
            thesis_rl = (
                inv_atoms[2].object if len(inv_atoms) >= 3
                else preds.get('thesis_risk_level', '')
            )
            _dom = _dominant_patterns.get(ticker, ('', ''))
            pos_atoms = _compute_position_sizing_atoms(
                ticker, sig_quality, thesis_rl, macro_conf, vol_30d, src_base, meta,
                insider_conviction=preds.get('insider_conviction', ''),
                short_squeeze=preds.get('short_squeeze_potential', ''),
                macro_event_risk=market_atoms.get('macro_event_risk', ''),
                sector_tailwind=preds.get('sector_tailwind', ''),
                db_path=self._db_path,
                dominant_pattern_type=_dom[0],
                dominant_timeframe=_dom[1],
            )

            # ── Cross-engine input 3: regime-conditional conviction modulation ─
            # RegimeHistoryClassifier writes best_regime / worst_regime atoms.
            # If current regime matches best_regime → upgrade tier by 1.
            # If current regime matches worst_regime → downgrade tier by 1.
            # Also reads regime_hit_rate_{current_regime} for position size scalar.
            if pos_atoms:
                current_regime = ticker_atoms.get('market', {}).get('market_regime', '')
                best_r  = preds.get('best_regime', '')
                worst_r = preds.get('worst_regime', '')
                _TIER_O = ['avoid', 'low', 'medium', 'high']
                _ct_atom = next((a for a in pos_atoms if a.predicate == 'conviction_tier'), None)
                if _ct_atom and current_regime and current_regime not in ('no_data', ''):
                    ct_val = _ct_atom.object
                    if ct_val in _TIER_O:
                        ct_idx = _TIER_O.index(ct_val)
                        if best_r and current_regime == best_r and ct_idx < 3:
                            _ct_atom.object = _TIER_O[ct_idx + 1]
                            if _ct_atom.metadata:
                                _ct_atom.metadata['ct_rule'] = _ct_atom.metadata.get('ct_rule', '') + '+RH_best_regime'
                        elif worst_r and current_regime == worst_r and ct_idx > 0:
                            _ct_atom.object = _TIER_O[ct_idx - 1]
                            if _ct_atom.metadata:
                                _ct_atom.metadata['ct_rule'] = _ct_atom.metadata.get('ct_rule', '') + '+RH_worst_regime'

            # ── Cross-engine input 4: epistemic stress → conviction penalty ────
            # High composite stress = conflicting sources, stale data, authority
            # disagreement. Degrades conviction so the system self-corrects when
            # its own data quality is poor.
            # compute_stress() takes retrieved_atoms (list of fact dicts) and
            # message_key_terms (list of str). Build both from the already-loaded preds.
            if pos_atoms:
                try:
                    from knowledge.epistemic_stress import compute_stress as _compute_stress
                    _ticker_fact_dicts = [
                        {'confidence': 0.7, 'source': f'kb_{pred}', 'predicate': pred, 'object': obj}
                        for pred, obj in preds.items()
                    ]
                    _stress = _compute_stress(
                        retrieved_atoms   = _ticker_fact_dicts,
                        message_key_terms = [ticker],
                    )
                    if _stress and getattr(_stress, 'composite_stress', 0.0) > 0.5:
                        _stress_penalty = (_stress.composite_stress - 0.5) * 0.4
                        _ct_atom2 = next((a for a in pos_atoms if a.predicate == 'conviction_tier'), None)
                        _ps_atom  = next((a for a in pos_atoms if a.predicate == 'position_size_pct'), None)
                        if _ct_atom2 and _ct_atom2.object in ('high', 'medium'):
                            _TIER_ES = ['avoid', 'low', 'medium', 'high']
                            _es_idx = _TIER_ES.index(_ct_atom2.object)
                            if _stress_penalty > 0.15 and _es_idx > 0:
                                _ct_atom2.object = _TIER_ES[_es_idx - 1]
                                if _ct_atom2.metadata:
                                    _ct_atom2.metadata['ct_rule'] = _ct_atom2.metadata.get('ct_rule', '') + f'+ES_stress{_stress.composite_stress:.2f}'
                        if _ps_atom:
                            try:
                                _ps_val = float(_ps_atom.object)
                                _ps_atom.object = str(round(_ps_val * (1.0 - _stress_penalty), 2))
                            except (ValueError, TypeError):
                                pass
                except Exception:
                    pass

            atoms.extend(pos_atoms)

            # ── Earnings proximity atom ────────────────────────────────────
            # Reads earnings_quality (ISO date) OR next_earnings atom.
            # Emits earnings_proximity only when a parseable date exists.
            next_earn = preds.get('next_earnings') or preds.get('earnings_quality')
            ep = _classify_earnings_proximity(next_earn)
            if ep is not None:
                atoms.append(RawAtom(
                    subject    = ticker,
                    predicate  = 'earnings_proximity',
                    object     = ep,
                    confidence = 0.90,   # deterministic date math
                    source     = f'{src_base}_earnings_proximity',
                    metadata   = {**meta, 'next_earnings': str(next_earn)},
                    upsert     = True,
                ))

            # ── Skew filter atom ───────────────────────────────────────────
            # Only emitted when skew data exists (options_adapter may not have
            # run yet on startup — skipped cleanly when no skew atoms present).
            if signal_dir:
                skew_atoms = _compute_skew_filter_atoms(
                    ticker, preds, market_atoms, signal_dir, src_base, meta
                )
                atoms.extend(skew_atoms)

            # ── News sentiment atom ────────────────────────────────────────────
            # Skipped if fewer than _SENTIMENT_MIN_ATOMS LLM atoms for ticker.
            # Only called for equity tickers that have signal_direction set
            # to avoid DB queries for ETF/macro proxy subjects.
            if signal_dir:
                ns = _compute_news_sentiment(ticker, self._db_path)
                if ns is not None:
                    atoms.append(RawAtom(
                        subject    = ticker,
                        predicate  = 'news_sentiment',
                        object     = ns,
                        confidence = 0.60,   # soft signal from LLM extractions
                        source     = f'{src_base}_news_sentiment',
                        metadata   = meta,
                        upsert     = True,
                    ))

            enriched += 1

        # ── Market regime atom (single, subject='market') ─────────────────
        # Emitted once per enrichment cycle — not per-ticker.
        # Uses the same macro_signals and ticker_atoms already loaded above.
        regime = _classify_market_regime(macro_signals, ticker_atoms)
        atoms.append(RawAtom(
            subject    = 'market',
            predicate  = 'market_regime',
            object     = regime,
            confidence = 0.70,
            source     = 'derived_signal_regime',
            metadata   = {
                'as_of':       now_iso,
                'spy':         macro_signals.get(_MARKET_PROXY, ''),
                'hyg':         macro_signals.get(_CREDIT_PROXY, ''),
                'tlt':         macro_signals.get(_RATES_PROXY, ''),
                'gld':         ticker_atoms.get(_GOLD_PROXY, {}).get('signal_direction', ''),
            },
            upsert     = True,
        ))

        # ── Commodity cross-correlation atom ─────────────────────────────
        # Surfaces GLD/USO vs equity and rates vs real estate cross-signal.
        # Emitted as subject='macro' so LLM can cite it in portfolio responses.
        try:
            gld_sig = ticker_atoms.get('gld', {}).get('signal_direction', '')
            uso_sig = ticker_atoms.get('uso', {}).get('signal_direction', '')
            tlt_sig_cc = macro_signals.get(_RATES_PROXY, '')
            spy_sig_cc = macro_signals.get(_MARKET_PROXY, '')
            xlre_sig   = ticker_atoms.get('xlre', {}).get('signal_direction', '')

            _bull = _BULLISH_SIGNALS
            _bear = _BEARISH_SIGNALS

            parts = []
            # Gold vs equity divergence
            if gld_sig in _bull and spy_sig_cc in _bear:
                parts.append('gold_equity_divergence:risk_off_hedge_active')
            elif gld_sig in _bear and spy_sig_cc in _bull:
                parts.append('gold_equity_convergence:risk_on_rotation')
            elif gld_sig in _bull and spy_sig_cc in _bull:
                parts.append('gold_equity_both_bid:inflation_hedge_with_growth')

            # Rates vs real estate: TLT inverse to yields; falling TLT = rising rates = REIT headwind
            if tlt_sig_cc in _bear and xlre_sig in _bear:
                parts.append('rates_rising:real_estate_headwind_confirmed')
            elif tlt_sig_cc in _bull and xlre_sig in _bull:
                parts.append('rates_falling:real_estate_tailwind_confirmed')
            elif tlt_sig_cc in _bear and xlre_sig in _bull:
                parts.append('rates_rising:real_estate_resilient_divergence')

            # Energy cross-signal
            if uso_sig in _bull and gld_sig in _bull:
                parts.append('commodities_broadly_bid:inflation_pressure')
            elif uso_sig in _bull and spy_sig_cc in _bear:
                parts.append('energy_bid_equity_weak:stagflation_signal')

            if parts:
                atoms.append(RawAtom(
                    subject    = 'macro',
                    predicate  = 'commodity_cross_correlation',
                    object     = ' | '.join(parts),
                    confidence = 0.65,
                    source     = 'derived_signal_commodity_xasset',
                    metadata   = {
                        'as_of': now_iso,
                        'gld': gld_sig, 'uso': uso_sig,
                        'tlt': tlt_sig_cc, 'spy': spy_sig_cc, 'xlre': xlre_sig,
                    },
                    upsert     = True,
                ))
        except Exception as _cx:
            _logger.debug('[signal_enrichment] commodity cross-correlation failed: %s', _cx)

        # ── Geopolitical risk exposure pass ───────────────────────────────
        # Reads gdelt_tension and ucdp_conflict atoms from KB and emits
        # geopolitical_risk_exposure per ticker using geo_exposure.py config.
        # Also emits energy_shock_risk macro cross signal (EIA + GDELT).
        try:
            geo_atoms = _compute_geo_risk_atoms(self._db_path, now_iso)
            atoms.extend(geo_atoms)
        except Exception as _ge:
            _logger.warning('[signal_enrichment] geo-risk pass failed: %s', _ge)

        _logger.info(
            '[signal_enrichment] enriched %d tickers (%d atoms), skipped %d subjects',
            enriched, len(atoms), skipped,
        )

        # ── Retroactive pattern enrichment ────────────────────────────────────
        # Re-stamp kb_conviction/kb_regime/kb_signal_dir on ALL open patterns
        # with the current KB values.  COALESCE(NULLIF) guard intentionally
        # removed — we always overwrite so stale values (Issue 2) are corrected
        # every enrichment cycle, not just when the field was previously null.
        try:
            _pconn = sqlite3.connect(self._db_path, timeout=30)
            _pconn.execute('PRAGMA journal_mode=WAL')
            _pconn.execute('PRAGMA busy_timeout=30000')
            _pconn.row_factory = sqlite3.Row

            _open_tickers = _pconn.execute("""
                SELECT DISTINCT ticker FROM pattern_signals
                WHERE status NOT IN ('filled','broken','expired')
            """).fetchall()

            _pat_updated = 0
            for (_pticker,) in _open_tickers:
                _patoms: dict = {}
                for _prow in _pconn.execute(
                    "SELECT predicate, object FROM facts WHERE LOWER(subject)=? "
                    "AND predicate IN ('conviction_tier','signal_direction','price_regime') "
                    "ORDER BY timestamp DESC",
                    (_pticker.lower(),)
                ).fetchall():
                    if _prow[0] not in _patoms:
                        _patoms[_prow[0]] = _prow[1]

                _conviction = _patoms.get('conviction_tier', '')
                _signal_dir = _patoms.get('signal_direction', '')
                _regime     = _patoms.get('price_regime', '')

                if _conviction or _signal_dir or _regime:
                    _n = _pconn.execute("""
                        UPDATE pattern_signals
                        SET kb_conviction = CASE WHEN ? != '' THEN ? ELSE kb_conviction END,
                            kb_signal_dir = CASE WHEN ? != '' THEN ? ELSE kb_signal_dir END,
                            kb_regime     = CASE WHEN ? != '' THEN ? ELSE kb_regime END
                        WHERE ticker = ?
                          AND status NOT IN ('filled','broken','expired')
                    """, (
                        _conviction, _conviction,
                        _signal_dir, _signal_dir,
                        _regime,     _regime,
                        _pticker,
                    )).rowcount
                    _pat_updated += _n

            _pconn.commit()
            _pconn.close()
            if _pat_updated:
                _logger.info(
                    '[signal_enrichment] re-stamped %d patterns across %d open tickers',
                    _pat_updated, len(_open_tickers),
                )
        except Exception as _pe:
            _logger.warning('[signal_enrichment] retroactive pattern enrichment failed: %s', _pe)

        return atoms


# ── Standalone stale-pattern re-enrichment ────────────────────────────────────

def enrich_stale_patterns(db_path: str = 'trading_knowledge.db', dry_run: bool = False) -> int:
    """
    Re-stamp kb_conviction / kb_signal_dir / kb_regime on every open
    pattern_signals row using current KB facts.

    Fixes Issue 2: patterns with stale kb_conviction that the retroactive block
    previously skipped because it used COALESCE(NULLIF) — skipping non-null rows.

    Usage:
        python -m ingest.signal_enrichment_adapter          # update
        python -m ingest.signal_enrichment_adapter --dry-run
    """
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.row_factory = sqlite3.Row
    try:
        open_tickers = conn.execute(
            "SELECT DISTINCT ticker FROM pattern_signals "
            "WHERE status NOT IN ('filled','broken','expired')"
        ).fetchall()

        updated = 0
        stale   = 0

        for (ticker,) in open_tickers:
            patoms: dict = {}
            for row in conn.execute(
                "SELECT predicate, object FROM facts WHERE LOWER(subject)=? "
                "AND predicate IN ('conviction_tier','signal_direction','price_regime') "
                "ORDER BY timestamp DESC",
                (ticker.lower(),)
            ).fetchall():
                if row[0] not in patoms:
                    patoms[row[0]] = row[1]

            conviction = patoms.get('conviction_tier', '')
            signal_dir = patoms.get('signal_direction', '')
            regime     = patoms.get('price_regime', '')

            if not (conviction or signal_dir or regime):
                continue

            stale_count = conn.execute("""
                SELECT COUNT(*) FROM pattern_signals
                WHERE ticker = ?
                  AND status NOT IN ('filled','broken','expired')
                  AND (
                    (? != '' AND (kb_conviction IS NULL OR kb_conviction != ?)) OR
                    (? != '' AND (kb_signal_dir IS NULL OR kb_signal_dir != ?)) OR
                    (? != '' AND (kb_regime     IS NULL OR kb_regime     != ?))
                  )
            """, (
                ticker,
                conviction, conviction,
                signal_dir, signal_dir,
                regime,     regime,
            )).fetchone()[0]
            stale += stale_count

            if dry_run:
                continue

            n = conn.execute("""
                UPDATE pattern_signals
                SET kb_conviction = CASE WHEN ? != '' THEN ? ELSE kb_conviction END,
                    kb_signal_dir = CASE WHEN ? != '' THEN ? ELSE kb_signal_dir END,
                    kb_regime     = CASE WHEN ? != '' THEN ? ELSE kb_regime     END
                WHERE ticker = ?
                  AND status NOT IN ('filled','broken','expired')
            """, (
                conviction, conviction,
                signal_dir, signal_dir,
                regime,     regime,
                ticker,
            )).rowcount
            updated += n

        if not dry_run:
            conn.commit()

        _logger.info(
            '[enrich_stale_patterns] %s %d pattern rows across %d tickers (stale detected=%d)',
            'dry_run:' if dry_run else 'updated',
            stale if dry_run else updated,
            len(open_tickers), stale,
        )
        return stale if dry_run else updated
    finally:
        conn.close()


if __name__ == '__main__':
    import sys as _sys
    import os as _os
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    _db  = _os.environ.get('DB_PATH', 'trading_knowledge.db')
    _dry = '--dry-run' in _sys.argv
    _n   = enrich_stale_patterns(db_path=_db, dry_run=_dry)
    print(f'{"[DRY RUN] Would update" if _dry else "Updated"} {_n} pattern rows.')

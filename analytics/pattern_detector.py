"""
analytics/pattern_detector.py — Smart Money Concept Pattern Detector

Detects 7 price-action patterns from a List[OHLCV] in a single scan pass:
  fvg              — Fair Value Gap (3-candle imbalance)
  ifvg             — Inverse FVG (partially-filled FVG acting as S/R)
  bpr              — Balanced Price Range (overlapping opposing FVGs)
  order_block      — Last candle before a strong impulsive move
  breaker          — Order block that price has since broken through
  liquidity_void   — Large single-candle move with minimal wicks
  mitigation       — Bearish candle within bullish structure price returns to

All detection is purely deterministic OHLCV arithmetic — no LLM needed.
A single call to detect_all_patterns() runs all 7 detectors in one pass and
returns a List[PatternSignal] sorted by quality_score descending.

OHLCV input format
==================
Each candle must be a dict or OHLCV dataclass with fields:
  open, high, low, close  — floats
  timestamp               — ISO 8601 string (candle open time)
  volume                  — float (optional, used for quality weight)

Quality score weights
=====================
  kb_conviction_alignment  0.25
  kb_regime_alignment      0.20
  kb_signal_alignment      0.15
  gap_size_vs_atr          0.25   (zone size relative to ATR14)
  recency                  0.15   (exponential decay from candle index)
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class OHLCV:
    """Single candlestick with open time."""
    timestamp: str
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float = 0.0

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def total_range(self) -> float:
        return self.high - self.low if self.high != self.low else 1e-9

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open

    @property
    def body_ratio(self) -> float:
        return self.body_size / self.total_range


@dataclass
class PatternSignal:
    """One detected pattern zone."""
    pattern_type:   str    # fvg / ifvg / bpr / order_block / breaker / liquidity_void / mitigation
    ticker:         str
    direction:      str    # bullish / bearish
    zone_high:      float
    zone_low:       float
    zone_size_pct:  float  # (zone_high - zone_low) / zone_low * 100
    timeframe:      str    # 15m / 1h / 4h / 1d
    formed_at:      str    # ISO timestamp of the candle that completed the pattern
    quality_score:  float  # 0.0 – 1.0 composite
    status:         str    # open / partially_filled / filled / broken
    kb_conviction:  str = ''
    kb_regime:      str = ''
    kb_signal_dir:  str = ''
    # Internal: candle index within the scan window (used for recency scoring)
    _candle_idx:    int = field(default=0, repr=False, compare=False)


# ── Helpers ────────────────────────────────────────────────────────────────────

_STRONG_MOVE_MULTIPLIER = 1.5   # body must be X× avg body to qualify as impulse
_LV_BODY_RATIO_MIN      = 0.85  # liquidity void: body/range threshold
_LV_BODY_ATR_MULTIPLIER = 2.0   # liquidity void: body must exceed X× avg body
_RECENCY_DECAY          = 0.05  # exponential decay per candle from the right


def _avg_body(candles: List[OHLCV], window: int = 20) -> float:
    """Rolling average body size over `window` most recent candles."""
    sample = [c.body_size for c in candles[-window:] if c.body_size > 0]
    return sum(sample) / len(sample) if sample else 1e-9


def _atr(candles: List[OHLCV], window: int = 14) -> float:
    """Average True Range over `window` most recent candles."""
    trs = []
    for i in range(1, min(window + 1, len(candles))):
        c, p = candles[i], candles[i - 1]
        tr = max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 1e-9


def _zone_size_pct(zone_high: float, zone_low: float) -> float:
    base = zone_low if zone_low > 0 else 1e-9
    return (zone_high - zone_low) / base * 100.0


def _gap_score(zone_high: float, zone_low: float, atr_val: float,
               impulse_volume: float = 0.0, avg_volume: float = 0.0) -> float:
    """Normalised gap size vs ATR, capped at 1.0. Optionally scaled by volume ratio."""
    gap = zone_high - zone_low
    base = min(gap / (atr_val * 2.0), 1.0) if atr_val > 0 else 0.0
    if impulse_volume > 0 and avg_volume > 0:
        vol_mult = min(1.5, max(0.5, impulse_volume / avg_volume))
    else:
        vol_mult = 1.0
    return min(base * vol_mult, 1.0)


def _recency_score(candle_idx: int, total: int) -> float:
    """Exponential decay — patterns closer to the current candle score higher."""
    distance = total - 1 - candle_idx
    return math.exp(-_RECENCY_DECAY * distance)


def _kb_scores(
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
    direction:     str,
    timeframe:     str = '',
) -> Tuple[float, float, float]:
    """
    Return (conviction_score, regime_score, signal_score) each 0.0 or 1.0.
    """
    # Neutral (absent KB) → 0.2 penalty weight, not 0.5 neutral.
    # 0.5 allowed an unenriched pattern (gap=1.0, recency=0.95) to score ~0.74,
    # above every bot floor and the 0.72 quality-only gate. 0.2 puts it at ~0.53.
    # KB data upgrades (1.0) or downgrades (0.0) from this low baseline.
    #
    # Conviction inversion for bearish short-TF patterns:
    # Live data shows IFVG bearish high conviction WR=46% vs medium WR=92%.
    # High analyst coverage = more dip buyers = harder to fall on 5m/15m.
    short_tf = timeframe in ('1m', '5m', '15m')
    if direction == 'bearish' and short_tf:
        conv = 0.0 if kb_conviction in ('high', 'strong', 'confirmed') else (
               1.0 if kb_conviction in ('medium',) else (
               0.6 if kb_conviction in ('low',) else 0.3))
    else:
        conv  = 1.0 if kb_conviction in ('high', 'strong', 'confirmed') else (
                0.0 if kb_conviction in ('low', 'weak', 'avoid') else 0.2)
    if direction == 'bearish':
        regime = 1.0 if kb_regime and any(x in kb_regime.lower() for x in ('risk_off', 'bearish', 'near_52w_low', 'near_low', 'contraction')) else (
                 0.0 if kb_regime and any(x in kb_regime.lower() for x in ('risk_on', 'near_52w_high', 'near_high', 'expansion')) else 0.2)
    else:
        regime = 1.0 if kb_regime and any(x in kb_regime.lower() for x in ('risk_on', 'bullish', 'near_52w_high', 'near_high', 'mid_range')) else (
                 0.0 if kb_regime and any(x in kb_regime.lower() for x in ('risk_off', 'bearish', 'near_52w_low', 'near_low', 'contraction')) else 0.2)
    sig   = 1.0 if (
        (direction == 'bullish' and kb_signal_dir in ('long', 'bullish', 'buy')) or
        (direction == 'bearish' and kb_signal_dir in ('short', 'bearish', 'sell'))
    ) else 0.0 if (
        (direction == 'bullish' and kb_signal_dir in ('short', 'bearish', 'sell')) or
        (direction == 'bearish' and kb_signal_dir in ('long', 'bullish', 'buy'))
    ) else 0.2
    return conv, regime, sig


# Instrument classes where KB conviction/signal enrichment is structurally unavailable.
# For these, the quality formula redistributes the conv+sig weights onto gap+recency
# so strong technical zones aren't capped below the bot quality floor (0.75).
_TECHNICAL_ONLY_SECTORS = frozenset({
    'fx', 'metals', 'oil', 'commodities', 'futures', 'crypto',
    'silver', 'gold', 'platinum', 'energy',
    # ETFs: no analyst KB (no conviction/signal direction) — use technical formula
    # Major index ETFs (SPY, QQQ, HYG, TLT) have structural liq_void 1d edges
    # that the standard formula masks due to missing KB enrichment.
    'etf', 'index',
})

# Pattern-type × timeframe quality nudges derived from 5.1M calibration samples.
# liq_void 15m is the structurally strongest pattern (HR=55-64% across exchanges).
# fvg is consistently worst (HR=11-22%); order_block 15m has a 60% stop rate.
_PATTERN_BOOSTS: dict[str, dict[str, float]] = {
    'liquidity_void': {'15m': +0.05, '1h': +0.02},
    'mitigation':     {'1d':  +0.02},
    'fvg':            {'15m': -0.10, '1h': -0.08},
    'order_block':    {'15m': -0.05},
}


def _quality(
    pattern_type:  str,
    direction:     str,
    zone_high:     float,
    zone_low:      float,
    candle_idx:    int,
    total_candles: int,
    atr_val:       float,
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
    sector:        str = '',
    timeframe:     str = '',
    db_path:       str = '',
    ticker:        str = '',
    volume_vs_avg: float = 0.0,
) -> float:
    """Compute composite quality score 0.0–1.0.

    Standard equity formula (full KB enrichment available):
      conv×0.25 | regime×0.20 | sig×0.15 | gap×0.25 | rec×0.15

    Technical-only formula (FX, metals, commodities — no analyst KB):
      gap×0.50 | rec×0.25 | regime×0.25
    Redistributes conv+sig weights to gap+recency so strong FX/commodity
    zones aren't penalised for missing analyst data that doesn't exist for them.
    """
    gap = _gap_score(zone_high, zone_low, atr_val)
    rec = _recency_score(candle_idx, total_candles)

    _sec = (sector or '').lower().strip()
    if _sec in _TECHNICAL_ONLY_SECTORS:
        # Technical-only formula: pure zone quality + recency, no KB components.
        score = gap * 0.65 + rec * 0.35
    elif pattern_type == 'mitigation' and timeframe in ('1d', '4h'):
        # Mitigation 1d/4h formula: zone-quality-dominant.
        # The best mitigation 1d edges are on stocks with LOW KB conviction —
        # beaten-down tickers near 52w lows. Standard formula inverts the edge.
        # Data: n=341,990 NULL-regime samples, gap=+33.9%, best edge in the DB.
        # Redistribution: conv down from 0.25→0.10, gap up 0.25→0.40, rec up 0.15→0.25
        conv, regime_s, sig = _kb_scores(kb_conviction, kb_regime, kb_signal_dir, direction, timeframe)
        score = (
            conv     * 0.10 +
            regime_s * 0.15 +
            sig      * 0.10 +
            gap      * 0.40 +
            rec      * 0.25
        )
    else:
        conv, regime_s, sig = _kb_scores(kb_conviction, kb_regime, kb_signal_dir, direction, timeframe)
        score = (
            conv    * 0.25 +
            regime_s * 0.20 +
            sig     * 0.15 +
            gap     * 0.25 +
            rec     * 0.15
        )
    boost = _PATTERN_BOOSTS.get(pattern_type, {}).get(timeframe, 0.0)

    # Zone age + IFVG width modifier (combined into _age_boost)
    # Age data: IFVG day-0 WR=52.6% vs day3-7 WR=79.2% — penalise fresh zones
    # Width data: IFVG 2.0-2.5% WR=97.2% | <1.5% noisy | >3.5% WR=44%
    # IFVG: fresh zones (day 0) WR=52.6% vs day 3-7 WR=79.2% → penalise new IFVG entries.
    # Breaker: zones decay after day 5 (WR=15.4% at 3-7d) → penalise stale breakers.
    # Order_block: old zones perform best (>7d WR=77.8%) → reward aged OBs.
    # Data: live trade analysis, 182 trades across pattern×age buckets.
    _age_boost = 0.0
    # IFVG zone width: 2.0-2.5% is sweet spot (WR=97.2%), tiny or huge is bad
    if pattern_type == 'ifvg' and zone_low > 0:
        _zsp = (zone_high - zone_low) / zone_low * 100
        if _zsp < 1.5:
            _age_boost -= 0.15   # tiny IFVG — noise, WR collapses
        elif _zsp <= 2.5:
            _age_boost += 0.10   # sweet spot — WR=97.2%
        elif _zsp > 3.5:
            _age_boost -= 0.15   # oversized IFVG — WR=44%, erratic
    if total_candles > 0:
        _age_frac = candle_idx / total_candles   # 0 = fresh, 1 = near-expiry
        if pattern_type == 'ifvg' and _age_frac < 0.10:
            _age_boost -= 0.15   # same-day IFVG: WR=52.6%, penalise
        elif pattern_type == 'breaker' and _age_frac > 0.60:
            _age_boost -= 0.15   # stale breaker decays badly
        elif pattern_type == 'order_block' and _age_frac > 0.50:
            _age_boost += 0.10   # aged OB confirms level validity

    # Regime conditioning: penalise bullish setups in risk_on regimes.
    # In a correction/expansion, 'near_52w_low + bullish' is a falling knife.
    # These score highly from the formula but fail live (q=0.8 WR=23% observed).
    # Exempt: technical-only sectors (FX/ETF/commodities) — their formula is already
    # regime-agnostic and these instruments structurally ignore equity regime labels.
    _regime_penalty = 0.0
    # Regime penalty for TREND-FOLLOWING bullish entries in risk_on environments.
    # Exempt: mitigation and liquidity_void — these are MEAN REVERSION patterns.
    # Price returning to a support zone is structurally positive in corrections.
    # The regime gate in bot_runner already blocks risk_on_expansion/recovery.
    # Only penalise directional patterns that require ongoing momentum: ifvg, breaker,
    # order_block, fvg, bos, choch.
    _TREND_FOLLOWING_PATTERNS = {'ifvg', 'breaker', 'order_block', 'fvg', 'bos', 'choch'}
    if (direction == 'bullish'
            and pattern_type in _TREND_FOLLOWING_PATTERNS
            and _sec not in _TECHNICAL_ONLY_SECTORS
            and db_path):
        try:
            _rc = sqlite3.connect(db_path, timeout=3)
            _rrow = _rc.execute(
                "SELECT object FROM facts WHERE predicate='market_regime' "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            _rc.close()
            if _rrow and 'risk_on' in (_rrow[0] or '').lower():
                _regime_penalty = -0.08
        except Exception:
            pass

    # Relative volume boost: high-volume zone = institutional conviction.
    # Spike volume (2×+ avg) adds +0.05; low liquidity (≤0.3×) deducts −0.05.
    # Only applies when db_path provided; silent fail if atom missing.
    _rvol_adj = 0.0
    if db_path and ticker:
        try:
            _rv_conn = sqlite3.connect(db_path, timeout=2)
            _rv_row = _rv_conn.execute(
                "SELECT object FROM facts WHERE UPPER(subject)=UPPER(?) "
                "AND predicate='relative_volume' ORDER BY timestamp DESC LIMIT 1",
                (ticker,),
            ).fetchone()
            _rv_conn.close()
            if _rv_row:
                _rv = float(_rv_row[0])
                if _rv >= 2.0:
                    _rvol_adj = 0.05
                elif _rv <= 0.3:
                    _rvol_adj = -0.05
        except Exception:
            pass

    # Volume at formation boost: 2×+ volume at zone = institutional conviction in the level.
    # High volume zones have stronger S/R — price is more likely to react at them.
    _vol_adj = 0.0
    if volume_vs_avg >= 2.0:
        _vol_adj = 0.08
    elif volume_vs_avg >= 1.5:
        _vol_adj = 0.04

    # RSI gate: penalise entries that fight momentum extremes.
    # Bullish entry when RSI >= 75 = chasing overbought price → high stop rate.
    # Bearish entry when RSI <= 25 = shorting oversold price → high bounce rate.
    # −0.12 is deliberately large — these setups have WR < 30% in live data.
    # Only applies when db_path + ticker provided; skipped silently if atom absent.
    _rsi_adj = 0.0
    if db_path and ticker and direction in ('bullish', 'bearish'):
        try:
            _rsi_conn = sqlite3.connect(db_path, timeout=2)
            _rsi_row = _rsi_conn.execute(
                "SELECT object FROM facts WHERE UPPER(subject)=UPPER(?) "
                "AND predicate='rsi_14' ORDER BY timestamp DESC LIMIT 1",
                (ticker,),
            ).fetchone()
            _rsi_conn.close()
            if _rsi_row:
                _rsi_val = float(_rsi_row[0])
                if direction == 'bullish' and _rsi_val >= 75:
                    _rsi_adj = -0.12
                elif direction == 'bearish' and _rsi_val <= 25:
                    _rsi_adj = -0.12
        except Exception:
            pass

    return round(min(max(score + boost + _regime_penalty + _age_boost + _vol_adj + _rvol_adj + _rsi_adj, 0.0), 1.0), 4)


# ── Individual detectors ───────────────────────────────────────────────────────

def _detect_fvg(
    candles:  List[OHLCV],
    ticker:   str,
    timeframe: str,
    atr_val:  float,
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
    sector:        str = '',
    db_path:       str = '',
) -> List[PatternSignal]:
    """
    Fair Value Gap — 3-candle imbalance.
    Bullish: candle[i-1].high < candle[i+1].low  (gap between left wick and right wick)
    Bearish: candle[i-1].low  > candle[i+1].high
    """
    signals = []
    n = len(candles)
    for i in range(1, n - 1):
        left, mid, right = candles[i - 1], candles[i], candles[i + 1]

        # Bullish FVG
        if left.high < right.low:
            zh = right.low
            zl = left.high
            # Guard: reject zero/negative zones (e.g. .KS partial candle data)
            if zh > 0 and zl > 0 and zh > zl:
                q = _quality('fvg', 'bullish', zh, zl, i, n, atr_val,
                             kb_conviction, kb_regime, kb_signal_dir,
                             sector=sector, timeframe=timeframe, db_path=db_path, ticker=ticker)
                signals.append(PatternSignal(
                    pattern_type  = 'fvg',
                    ticker        = ticker,
                    direction     = 'bullish',
                    zone_high     = round(zh, 6),
                    zone_low      = round(zl, 6),
                    zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                    timeframe     = timeframe,
                    formed_at     = right.timestamp,
                    quality_score = q,
                    status        = 'open',
                    kb_conviction = kb_conviction,
                    kb_regime     = kb_regime,
                    kb_signal_dir = kb_signal_dir,
                    _candle_idx   = i,
                ))

        # Bearish FVG
        if left.low > right.high:
            zh = left.low
            zl = right.high
            # Guard: reject zero/negative zones (e.g. .KS partial candle data)
            if zh > 0 and zl > 0 and zh > zl:
                q = _quality('fvg', 'bearish', zh, zl, i, n, atr_val,
                             kb_conviction, kb_regime, kb_signal_dir,
                             sector=sector, timeframe=timeframe, db_path=db_path, ticker=ticker)
                signals.append(PatternSignal(
                    pattern_type  = 'fvg',
                    ticker        = ticker,
                    direction     = 'bearish',
                    zone_high     = round(zh, 6),
                    zone_low      = round(zl, 6),
                    zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                    timeframe     = timeframe,
                    formed_at     = right.timestamp,
                    quality_score = q,
                    status        = 'open',
                    kb_conviction = kb_conviction,
                    kb_regime     = kb_regime,
                    kb_signal_dir = kb_signal_dir,
                    _candle_idx   = i,
                ))
    return signals


def _update_fvg_status(
    fvg_signals: List[PatternSignal],
    candles:     List[OHLCV],
) -> List[PatternSignal]:
    """
    Walk all candles AFTER each FVG's formation index and update status:
      partially_filled — price entered zone but didn't close through it
      filled           — price closed fully through the zone
    Returns updated list (IFVGs are the partially-filled ones).
    """
    updated = []
    n = len(candles)
    for sig in fvg_signals:
        status = 'open'
        # _candle_idx = middle candle (i); right candle = i+1; scan starts at i+2
        scan_start = sig._candle_idx + 2
        for j in range(scan_start, n):
            c = candles[j]
            if sig.direction == 'bullish':
                if c.low <= sig.zone_low:
                    status = 'filled'
                    break
                if c.low < sig.zone_high:
                    status = 'partially_filled'
            else:  # bearish
                if c.high >= sig.zone_high:
                    status = 'filled'
                    break
                if c.high > sig.zone_low:
                    status = 'partially_filled'
        sig.status = status
        updated.append(sig)
    return updated


def _detect_ifvg(fvg_signals: List[PatternSignal]) -> Tuple[List[PatternSignal], List[PatternSignal]]:
    """
    Inverse FVG — a partially-filled FVG becomes an IFVG acting as S/R.
    Returns (ifvgs, partially_filled_ids) where partially_filled_ids is the
    set of _candle_idx values for FVGs that were promoted to IFVG so the
    caller can suppress the parent FVG from the output pool.
    """
    ifvgs = []
    promoted_idxs: List[int] = []
    for sig in fvg_signals:
        if sig.status == 'partially_filled':
            promoted_idxs.append(sig._candle_idx)
            ifvg = PatternSignal(
                pattern_type  = 'ifvg',
                ticker        = sig.ticker,
                direction     = sig.direction,
                zone_high     = sig.zone_high,
                zone_low      = sig.zone_low,
                zone_size_pct = sig.zone_size_pct,
                timeframe     = sig.timeframe,
                formed_at     = sig.formed_at,
                quality_score = round(min(sig.quality_score * 1.1, 1.0), 4),  # slight boost
                status        = 'open',
                kb_conviction = sig.kb_conviction,
                kb_regime     = sig.kb_regime,
                kb_signal_dir = sig.kb_signal_dir,
                _candle_idx   = sig._candle_idx,
            )
            ifvgs.append(ifvg)
    return ifvgs, promoted_idxs


def _detect_bpr(
    fvg_signals: List[PatternSignal],
    ticker:      str,
    timeframe:   str,
) -> List[PatternSignal]:
    """
    Balanced Price Range — overlap zone of a bullish + bearish FVG.
    bull.zone_low < bear.zone_high AND bull.zone_high > bear.zone_low
    Only open FVGs qualify.
    """
    bulls = [s for s in fvg_signals if s.direction == 'bullish' and s.status == 'open']
    bears = [s for s in fvg_signals if s.direction == 'bearish' and s.status == 'open']
    bprs  = []
    for bull in bulls:
        for bear in bears:
            if bull.zone_low < bear.zone_high and bull.zone_high > bear.zone_low:
                # Overlap zone
                zh = min(bull.zone_high, bear.zone_high)
                zl = max(bull.zone_low,  bear.zone_low)
                if zh <= zl:
                    continue
                # BPR inherits the higher quality of the two constituent FVGs
                q = round(min(max(bull.quality_score, bear.quality_score) * 1.15, 1.0), 4)
                bprs.append(PatternSignal(
                    pattern_type  = 'bpr',
                    ticker        = ticker,
                    direction     = 'bullish',   # BPR is directionally ambiguous; default bullish
                    zone_high     = round(zh, 6),
                    zone_low      = round(zl, 6),
                    zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                    timeframe     = timeframe,
                    formed_at     = max(bull.formed_at, bear.formed_at),
                    quality_score = q,
                    status        = 'open',
                    kb_conviction = bull.kb_conviction,
                    kb_regime     = bull.kb_regime,
                    kb_signal_dir = bull.kb_signal_dir,
                    _candle_idx   = max(bull._candle_idx, bear._candle_idx),
                ))
    return bprs


def _detect_order_blocks(
    candles:      List[OHLCV],
    ticker:       str,
    timeframe:    str,
    avg_body_val: float,
    atr_val:      float,
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
    sector:        str = '',
    db_path:       str = '',
) -> List[PatternSignal]:
    """
    Order Block — last opposite-direction candle before a strong impulsive move.
    Bullish OB: last bearish candle before next candle body > 1.5× avg_body (bullish impulse)
    Bearish OB: last bullish candle before next candle body > 1.5× avg_body (bearish impulse)
    """
    signals = []
    n = len(candles)
    threshold = avg_body_val * _STRONG_MOVE_MULTIPLIER

    for i in range(1, n - 1):
        curr  = candles[i]
        nxt   = candles[i + 1]

        # Bullish OB: current is bearish, next is strong bullish impulse
        if curr.is_bearish and nxt.is_bullish and nxt.body_size >= threshold:
            zh = curr.high
            zl = curr.low
            if zh > zl:
                q = _quality('order_block', 'bullish', zh, zl, i, n, atr_val,
                             kb_conviction, kb_regime, kb_signal_dir,
                             sector=sector, timeframe=timeframe, db_path=db_path, ticker=ticker)
                signals.append(PatternSignal(
                    pattern_type  = 'order_block',
                    ticker        = ticker,
                    direction     = 'bullish',
                    zone_high     = round(zh, 6),
                    zone_low      = round(zl, 6),
                    zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                    timeframe     = timeframe,
                    formed_at     = curr.timestamp,
                    quality_score = q,
                    status        = 'open',
                    kb_conviction = kb_conviction,
                    kb_regime     = kb_regime,
                    kb_signal_dir = kb_signal_dir,
                    _candle_idx   = i,
                ))

        # Bearish OB: current is bullish, next is strong bearish impulse
        if curr.is_bullish and nxt.is_bearish and nxt.body_size >= threshold:
            zh = curr.high
            zl = curr.low
            if zh > zl:
                q = _quality('order_block', 'bearish', zh, zl, i, n, atr_val,
                             kb_conviction, kb_regime, kb_signal_dir,
                             sector=sector, timeframe=timeframe, db_path=db_path, ticker=ticker)
                signals.append(PatternSignal(
                    pattern_type  = 'order_block',
                    ticker        = ticker,
                    direction     = 'bearish',
                    zone_high     = round(zh, 6),
                    zone_low      = round(zl, 6),
                    zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                    timeframe     = timeframe,
                    formed_at     = curr.timestamp,
                    quality_score = q,
                    status        = 'open',
                    kb_conviction = kb_conviction,
                    kb_regime     = kb_regime,
                    kb_signal_dir = kb_signal_dir,
                    _candle_idx   = i,
                ))
    return signals


def _update_ob_status(
    ob_signals: List[PatternSignal],
    candles:    List[OHLCV],
) -> Tuple[List[PatternSignal], List[PatternSignal]]:
    """
    Walk candles after OB formation and:
      - mark broken if price closes beyond the OB zone
      - return (updated_obs, breakers) where breakers are broken OBs
    """
    updated_obs = []
    breakers    = []
    n = len(candles)

    for sig in ob_signals:
        status = 'open'
        for j in range(sig._candle_idx + 2, n):
            c = candles[j]
            if sig.direction == 'bullish':
                if c.close < sig.zone_low:   # price closed below OB → broken
                    status = 'broken'
                    break
            else:  # bearish OB
                if c.close > sig.zone_high:  # price closed above OB → broken
                    status = 'broken'
                    break

        sig.status = status
        updated_obs.append(sig)

        if status == 'broken':
            # Breaker Block — the broken OB flips to opposite role
            breaker_direction = 'bearish' if sig.direction == 'bullish' else 'bullish'
            breakers.append(PatternSignal(
                pattern_type  = 'breaker',
                ticker        = sig.ticker,
                direction     = breaker_direction,
                zone_high     = sig.zone_high,
                zone_low      = sig.zone_low,
                zone_size_pct = sig.zone_size_pct,
                timeframe     = sig.timeframe,
                formed_at     = sig.formed_at,
                quality_score = round(min(sig.quality_score * 1.05, 1.0), 4),
                status        = 'open',
                kb_conviction = sig.kb_conviction,
                kb_regime     = sig.kb_regime,
                kb_signal_dir = sig.kb_signal_dir,
                _candle_idx   = sig._candle_idx,
            ))

    return updated_obs, breakers


def _detect_liquidity_voids(
    candles:      List[OHLCV],
    ticker:       str,
    timeframe:    str,
    avg_body_val: float,
    atr_val:      float,
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
    sector:        str = '',
    db_path:       str = '',
) -> List[PatternSignal]:
    """
    Liquidity Void — large single-candle move with minimal wicks.
    body_size / total_range > 0.85  AND  body_size > 2× avg_body
    """
    signals = []
    n = len(candles)
    body_threshold = avg_body_val * _LV_BODY_ATR_MULTIPLIER

    for i, c in enumerate(candles):
        if (c.body_ratio > _LV_BODY_RATIO_MIN and
                c.body_size > body_threshold and
                c.total_range > 0):
            direction = 'bullish' if c.is_bullish else 'bearish'
            # Tradeable zone = unfilled range between body edge and wick tip.
            # Bullish void: price launched from body_open upward, unfilled gap is
            # from open (body bottom) down to candle low (wick).
            # Bearish void: unfilled gap is from open (body top) up to candle high.
            if direction == 'bullish':
                zh = max(c.open, c.close)  # body top (open of a bullish candle = bottom)
                zl = c.low                 # wick low — that's the unfilled pullback zone
            else:
                zh = c.high                # wick high — unfilled pullback zone
                zl = min(c.open, c.close)  # body bottom
            if zh <= zl or zl <= 0:
                continue
            q = _quality('liquidity_void', direction, zh, zl, i, n, atr_val,
                         kb_conviction, kb_regime, kb_signal_dir,
                         sector=sector, timeframe=timeframe, db_path=db_path, ticker=ticker)
            signals.append(PatternSignal(
                pattern_type  = 'liquidity_void',
                ticker        = ticker,
                direction     = direction,
                zone_high     = round(zh, 6),
                zone_low      = round(zl, 6),
                zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                timeframe     = timeframe,
                formed_at     = c.timestamp,
                quality_score = q,
                status        = 'open',
                kb_conviction = kb_conviction,
                kb_regime     = kb_regime,
                kb_signal_dir = kb_signal_dir,
                _candle_idx   = i,
            ))
    return signals


_MIT_SWING_MIN      = 5    # minimum candles of confirmed swing before mitigation candidate
_MIT_BODY_RATIO_MIN = 0.4  # mitigation candle body must be >=40% of its total range


def _detect_mitigation_blocks(
    candles:      List[OHLCV],
    ticker:       str,
    timeframe:    str,
    avg_body_val: float,
    atr_val:      float,
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
    sector:        str = '',
    db_path:       str = '',
) -> List[PatternSignal]:
    """
    Mitigation Block — a counter-swing candle embedded in a confirmed trend that
    price returns to from the trend side.

    Bullish mitigation (long setup):
      1. Confirmed bullish swing: >= _MIT_SWING_MIN candles with ALL higher lows
         (every candle's low >= previous candle's low), ensuring we're in a real
         uptrend not a 3-candle bounce in ranging price.
      2. A bearish candle within that swing with meaningful body (body_ratio >= 0.40)
         — filters out doji wicks and near-doji candles that aren't real pullbacks.
      3. A later candle must revisit the zone FROM ABOVE:
         - candle's high must be above zone_high before touching (approaching from above)
         - candle must close WITHIN the zone or bounce off zone_low (not blow through)
         - must be at least 2 candles after formation (no immediate-next-candle revisit)

    Bearish mitigation (short setup, symmetric):
      1. Confirmed bearish swing: >= _MIT_SWING_MIN candles with ALL lower highs
      2. A bullish candle within that swing with meaningful body
      3. A later candle revisits the zone from BELOW with close within/bouncing off zone_high
    """
    signals = []
    n = len(candles)

    for i in range(_MIT_SWING_MIN, n - 1):
        c = candles[i]

        # ── Bullish mitigation ────────────────────────────────────────────────
        if c.is_bearish and c.body_ratio >= _MIT_BODY_RATIO_MIN:
            preceding = candles[i - _MIT_SWING_MIN:i]
            # All higher lows across the full swing window
            if all(preceding[k + 1].low >= preceding[k].low
                   for k in range(len(preceding) - 1)):
                zh = c.high
                zl = c.low
                if zh <= 0 or zl <= 0 or zh <= zl:
                    continue
                # Revisit check: must approach from above, close inside zone
                # Skip candle immediately after formation (j >= i + 2)
                for j in range(i + 2, n):
                    later = candles[j]
                    # Price must have been above zone before touching (approaching from above)
                    if later.high < zh:
                        continue
                    # Close must be inside the zone (not blow through)
                    if zl <= later.close <= zh:
                        q = _quality('mitigation', 'bullish', zh, zl, i, n, atr_val,
                                     kb_conviction, kb_regime, kb_signal_dir,
                                     sector=sector, timeframe=timeframe, db_path=db_path, ticker=ticker)
                        signals.append(PatternSignal(
                            pattern_type  = 'mitigation',
                            ticker        = ticker,
                            direction     = 'bullish',
                            zone_high     = round(zh, 6),
                            zone_low      = round(zl, 6),
                            zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                            timeframe     = timeframe,
                            formed_at     = c.timestamp,
                            quality_score = q,
                            status        = 'open',
                            kb_conviction = kb_conviction,
                            kb_regime     = kb_regime,
                            kb_signal_dir = kb_signal_dir,
                            _candle_idx   = i,
                        ))
                        break

        # ── Bearish mitigation ────────────────────────────────────────────────
        elif c.is_bullish and c.body_ratio >= _MIT_BODY_RATIO_MIN:
            preceding = candles[i - _MIT_SWING_MIN:i]
            # All lower highs across the full swing window
            if all(preceding[k + 1].high <= preceding[k].high
                   for k in range(len(preceding) - 1)):
                zh = c.high
                zl = c.low
                if zh <= 0 or zl <= 0 or zh <= zl:
                    continue
                # Revisit from below: close must be inside zone
                for j in range(i + 2, n):
                    later = candles[j]
                    if later.low > zl:
                        continue
                    if zl <= later.close <= zh:
                        q = _quality('mitigation', 'bearish', zh, zl, i, n, atr_val,
                                     kb_conviction, kb_regime, kb_signal_dir,
                                     sector=sector, timeframe=timeframe, db_path=db_path, ticker=ticker)
                        signals.append(PatternSignal(
                            pattern_type  = 'mitigation',
                            ticker        = ticker,
                            direction     = 'bearish',
                            zone_high     = round(zh, 6),
                            zone_low      = round(zl, 6),
                            zone_size_pct = round(_zone_size_pct(zh, zl), 4),
                            timeframe     = timeframe,
                            formed_at     = c.timestamp,
                            quality_score = q,
                            status        = 'open',
                            kb_conviction = kb_conviction,
                            kb_regime     = kb_regime,
                            kb_signal_dir = kb_signal_dir,
                            _candle_idx   = i,
                        ))
                        break

    return signals


# ── Main public API ────────────────────────────────────────────────────────────

def detect_all_patterns(
    candles:       List[OHLCV],
    ticker:        str,
    timeframe:     str  = '1h',
    kb_conviction: str  = '',
    kb_regime:     str  = '',
    kb_signal_dir: str  = '',
    db_path:       str  = '',
    sector:        str  = '',
) -> List[PatternSignal]:
    """
    Run all 7 detectors in a single pass over `candles`.

    Parameters
    ----------
    candles        List[OHLCV] sorted oldest→newest (index 0 = oldest).
    ticker         Ticker symbol, e.g. 'NVDA'.
    timeframe      Candle interval string, e.g. '1h', '15m', '4h', '1d'.
    kb_conviction  KB conviction atom for this ticker (optional).
    kb_regime      KB market_regime atom (optional).
    kb_signal_dir  KB signal_direction atom (optional).
    sector         Sector label from KB (optional). Used to apply technical-only
                   quality formula for FX/commodities that lack analyst enrichment.

    Returns
    -------
    List[PatternSignal] sorted by quality_score descending.
    Filled patterns are excluded; only open / partially_filled returned.
    """
    if len(candles) < 3:
        return []

    avg_body_val = _avg_body(candles)
    atr_val      = _atr(candles)

    # ── FVG detection + status update ─────────────────────────────────────────
    raw_fvgs = _detect_fvg(candles, ticker, timeframe, atr_val,
                           kb_conviction, kb_regime, kb_signal_dir, sector=sector, db_path=db_path)
    fvgs = _update_fvg_status(raw_fvgs, candles)

    # ── IFVG (from partially-filled FVGs) ─────────────────────────────────────
    # promoted_idxs: parent FVGs at these candle indices have been converted to
    # IFVGs — suppress the parent from the output to avoid duplicate zone coverage.
    ifvgs, promoted_idxs = _detect_ifvg(fvgs)
    promoted_set = set(promoted_idxs)

    # ── BPR (overlapping open FVGs) ───────────────────────────────────────────
    bprs = _detect_bpr(fvgs, ticker, timeframe)

    # ── Order Blocks + Breaker Blocks ─────────────────────────────────────────
    raw_obs = _detect_order_blocks(candles, ticker, timeframe, avg_body_val, atr_val,
                                   kb_conviction, kb_regime, kb_signal_dir, sector=sector, db_path=db_path)
    obs, breakers = _update_ob_status(raw_obs, candles)

    # ── Liquidity Voids ───────────────────────────────────────────────────────
    lv_signals = _detect_liquidity_voids(candles, ticker, timeframe, avg_body_val, atr_val,
                                         kb_conviction, kb_regime, kb_signal_dir, sector=sector, db_path=db_path)

    # ── Mitigation Blocks ─────────────────────────────────────────────────────
    mit_signals = _detect_mitigation_blocks(candles, ticker, timeframe, avg_body_val, atr_val,
                                            kb_conviction, kb_regime, kb_signal_dir, sector=sector, db_path=db_path)

    # ── Combine, filter filled, sort ──────────────────────────────────────────
    # Exclude parent FVGs that were promoted to IFVGs (duplicate zone coverage)
    all_signals: List[PatternSignal] = (
        [s for s in fvgs if s.status != 'filled' and s._candle_idx not in promoted_set] +
        ifvgs +
        bprs +
        [s for s in obs if s.status != 'broken'] +
        breakers +
        lv_signals +
        mit_signals
    )

    all_signals.sort(key=lambda s: s.quality_score, reverse=True)

    # ── Calibration lift (optional, requires db_path) ──────────────────────────
    if db_path and all_signals:
        try:
            import sqlite3 as _sq
            import logging as _logging
            _cal_logger = _logging.getLogger(__name__)
            _conn = _sq.connect(db_path, timeout=5)
            _lifted_count = 0
            for sig in all_signals:
                _row = _conn.execute("""
                    SELECT hit_rate_t1, sample_size
                    FROM signal_calibration
                    WHERE UPPER(ticker) = UPPER(?)
                      AND pattern_type = ?
                      AND timeframe    = ?
                      AND sample_size >= 10
                    ORDER BY sample_size DESC
                    LIMIT 1
                """, (sig.ticker, sig.pattern_type, sig.timeframe)).fetchone()
                if _row:
                    _hr, _n = _row
                    if _hr >= 0.6:
                        _lift = (_hr - 0.6) * 0.4 * min(1.0, math.log(_n + 1) / math.log(201))
                        sig.quality_score = round(min(1.0, sig.quality_score + _lift), 4)
                        _lifted_count += 1
                    elif _hr < 0.35 and _n >= 20:
                        _penalty = (0.35 - _hr) * 0.3 * min(1.0, math.log(_n + 1) / math.log(201))
                        sig.quality_score = round(max(0.0, sig.quality_score - _penalty), 4)
                        _lifted_count += 1
            _conn.close()
            if _lifted_count > 0:
                _cal_logger.debug(
                    'pattern_detector: calibration lift applied to %d/%d patterns for %s',
                    _lifted_count, len(all_signals), ticker,
                )
        except Exception:
            pass

    return all_signals

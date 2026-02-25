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


def _gap_score(zone_high: float, zone_low: float, atr_val: float) -> float:
    """Normalised gap size vs ATR, capped at 1.0."""
    gap = zone_high - zone_low
    return min(gap / (atr_val * 2.0), 1.0) if atr_val > 0 else 0.0


def _recency_score(candle_idx: int, total: int) -> float:
    """Exponential decay — patterns closer to the current candle score higher."""
    distance = total - 1 - candle_idx
    return math.exp(-_RECENCY_DECAY * distance)


def _kb_scores(
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
    direction:     str,
) -> Tuple[float, float, float]:
    """
    Return (conviction_score, regime_score, signal_score) each 0.0 or 1.0.
    """
    conv  = 1.0 if kb_conviction in ('high', 'strong', 'confirmed') else 0.5 if kb_conviction else 0.0
    regime = 1.0 if kb_regime and 'risk_on' in kb_regime.lower() else 0.5 if kb_regime else 0.0
    sig   = 1.0 if (
        (direction == 'bullish' and kb_signal_dir in ('long', 'bullish', 'buy')) or
        (direction == 'bearish' and kb_signal_dir in ('short', 'bearish', 'sell'))
    ) else 0.0 if kb_signal_dir else 0.5
    return conv, regime, sig


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
) -> float:
    """Compute composite quality score 0.0–1.0."""
    conv, regime, sig = _kb_scores(kb_conviction, kb_regime, kb_signal_dir, direction)
    gap   = _gap_score(zone_high, zone_low, atr_val)
    rec   = _recency_score(candle_idx, total_candles)

    score = (
        conv   * 0.25 +
        regime * 0.20 +
        sig    * 0.15 +
        gap    * 0.25 +
        rec    * 0.15
    )
    return round(min(max(score, 0.0), 1.0), 4)


# ── Individual detectors ───────────────────────────────────────────────────────

def _detect_fvg(
    candles:  List[OHLCV],
    ticker:   str,
    timeframe: str,
    atr_val:  float,
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
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
            if zh > zl:
                q = _quality('fvg', 'bullish', zh, zl, i, n, atr_val,
                             kb_conviction, kb_regime, kb_signal_dir)
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
            if zh > zl:
                q = _quality('fvg', 'bearish', zh, zl, i, n, atr_val,
                             kb_conviction, kb_regime, kb_signal_dir)
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


def _detect_ifvg(fvg_signals: List[PatternSignal]) -> List[PatternSignal]:
    """
    Inverse FVG — a partially-filled FVG becomes an IFVG acting as S/R.
    The unfilled portion of the zone is the IFVG zone.
    """
    ifvgs = []
    for sig in fvg_signals:
        if sig.status == 'partially_filled':
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
    return ifvgs


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
                             kb_conviction, kb_regime, kb_signal_dir)
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
                             kb_conviction, kb_regime, kb_signal_dir)
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
            zh = c.high
            zl = c.low
            q = _quality('liquidity_void', direction, zh, zl, i, n, atr_val,
                         kb_conviction, kb_regime, kb_signal_dir)
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


def _detect_mitigation_blocks(
    candles:      List[OHLCV],
    ticker:       str,
    timeframe:    str,
    avg_body_val: float,
    atr_val:      float,
    kb_conviction: str,
    kb_regime:     str,
    kb_signal_dir: str,
) -> List[PatternSignal]:
    """
    Mitigation Block — bearish candle within a bullish swing structure that
    price subsequently returns to from above.

    Detection:
    1. Identify a bullish swing: N consecutive candles trending up (≥3 higher lows)
    2. A bearish candle within that swing is the mitigation candidate
    3. If a later candle's low enters the bearish candle's range, it's a mitigation block
    """
    signals = []
    n = len(candles)
    swing_min = 3

    # Find bearish candles embedded in locally bullish swings
    for i in range(swing_min, n - 1):
        c = candles[i]
        if not c.is_bearish:
            continue

        # Check for bullish swing context: preceding candles show rising lows
        preceding = candles[max(0, i - swing_min):i]
        if len(preceding) < 2:
            continue
        rising_lows = all(
            preceding[j + 1].low >= preceding[j].low
            for j in range(len(preceding) - 1)
        )
        if not rising_lows:
            continue

        zh = c.high
        zl = c.low

        # Check if a later candle revisits the zone from above
        revisited = False
        for j in range(i + 1, n):
            later = candles[j]
            if later.low <= zh and later.high >= zl:
                revisited = True
                break

        if revisited:
            q = _quality('mitigation', 'bullish', zh, zl, i, n, atr_val,
                         kb_conviction, kb_regime, kb_signal_dir)
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
    return signals


# ── Main public API ────────────────────────────────────────────────────────────

def detect_all_patterns(
    candles:       List[OHLCV],
    ticker:        str,
    timeframe:     str  = '1h',
    kb_conviction: str  = '',
    kb_regime:     str  = '',
    kb_signal_dir: str  = '',
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
                           kb_conviction, kb_regime, kb_signal_dir)
    fvgs = _update_fvg_status(raw_fvgs, candles)

    # ── IFVG (from partially-filled FVGs) ─────────────────────────────────────
    ifvgs = _detect_ifvg(fvgs)

    # ── BPR (overlapping open FVGs) ───────────────────────────────────────────
    bprs = _detect_bpr(fvgs, ticker, timeframe)

    # ── Order Blocks + Breaker Blocks ─────────────────────────────────────────
    raw_obs = _detect_order_blocks(candles, ticker, timeframe, avg_body_val, atr_val,
                                   kb_conviction, kb_regime, kb_signal_dir)
    obs, breakers = _update_ob_status(raw_obs, candles)

    # ── Liquidity Voids ───────────────────────────────────────────────────────
    lv_signals = _detect_liquidity_voids(candles, ticker, timeframe, avg_body_val, atr_val,
                                         kb_conviction, kb_regime, kb_signal_dir)

    # ── Mitigation Blocks ─────────────────────────────────────────────────────
    mit_signals = _detect_mitigation_blocks(candles, ticker, timeframe, avg_body_val, atr_val,
                                            kb_conviction, kb_regime, kb_signal_dir)

    # ── Combine, filter filled, sort ──────────────────────────────────────────
    all_signals: List[PatternSignal] = (
        [s for s in fvgs if s.status != 'filled'] +
        ifvgs +
        bprs +
        [s for s in obs if s.status != 'broken'] +
        breakers +
        lv_signals +
        mit_signals
    )

    all_signals.sort(key=lambda s: s.quality_score, reverse=True)
    return all_signals

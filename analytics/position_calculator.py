"""
analytics/position_calculator.py — Account-Aware Position Sizing

Given a PatternSignal and a user's account preferences, computes:
  - Entry zone midpoint
  - Stop loss (10% of zone size beyond the zone boundary)
  - Units to buy so that a stop hit == exactly risk_amount
  - R:R targets at 1:1, 1:2, 1:3
  - Expected profit at T2
  - KB conviction/regime alignment flags

No external dependencies. Pure arithmetic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from analytics.pattern_detector import PatternSignal

if TYPE_CHECKING:
    from analytics.signal_forecaster import ForecastResult


@dataclass
class PositionRecommendation:
    """Full position sizing recommendation for one pattern."""
    ticker:          str
    pattern_type:    str
    direction:       str

    # Entry context
    entry_zone_high: float
    entry_zone_low:  float
    suggested_entry: float    # midpoint of zone

    # Risk parameters
    stop_loss:             float
    stop_distance_pct:     float  # % from entry to stop
    stop_distance_currency: float  # absolute |entry - stop|

    # Account sizing
    account_size:              float
    account_currency:          str
    risk_pct:                  float   # max_risk_per_trade_pct
    risk_amount:               float   # account_size * risk_pct / 100
    position_size_units:       float   # units so stop_hit_loss == risk_amount
    position_value:            float   # position_size_units * suggested_entry
    position_pct_of_account:   float   # position_value / account_size * 100

    # Targets
    target_1:           float   # 1:1 R/R
    target_2:           float   # 1:2 R/R
    target_3:           float   # 1:3 R/R
    expected_profit_t1: float
    expected_profit_t2: float
    expected_profit_t3: float

    # KB alignment
    kb_conviction_alignment: bool
    kb_regime_alignment:     bool
    composite_score:         float

    # Probabilistic forecast — attached by TipScheduler after calculate_position()
    forecast: Optional['ForecastResult'] = field(default=None, compare=False)


def calculate_position(
    pattern:    PatternSignal,
    user_prefs: dict,
) -> Optional[PositionRecommendation]:
    """
    Compute position sizing for a pattern given user account preferences.

    Parameters
    ----------
    pattern     A PatternSignal (from pattern_detector.detect_all_patterns).
    user_prefs  Dict containing account_size, max_risk_per_trade_pct,
                account_currency keys (all optional with safe defaults).

    Returns
    -------
    PositionRecommendation or None if account_size is zero/unset
    (can't size without an account).

    Stop placement
    ---------------
    10% of the zone size beyond the zone boundary:
      bullish: stop = zone_low  * (1 - buffer_pct/100)
      bearish: stop = zone_high * (1 + buffer_pct/100)
    where buffer_pct = zone_size_pct * 0.10
    """
    _raw_account = user_prefs.get('account_size')
    account_size = float(_raw_account) if _raw_account else 0.0
    if account_size <= 0:
        return None

    risk_pct = float(user_prefs.get('max_risk_per_trade_pct') or 1.0)
    currency = str(user_prefs.get('account_currency') or 'GBP')

    # Validate zone
    if pattern.zone_high <= pattern.zone_low:
        return None

    entry = (pattern.zone_high + pattern.zone_low) / 2.0

    # Stop with 10% buffer beyond zone boundary
    buffer_pct = pattern.zone_size_pct * 0.10
    if pattern.direction == 'bullish':
        stop = pattern.zone_low * (1.0 - buffer_pct / 100.0)
    else:
        stop = pattern.zone_high * (1.0 + buffer_pct / 100.0)

    stop_distance_currency = abs(entry - stop)
    stop_distance_pct      = (stop_distance_currency / entry * 100.0
                               if entry > 0 else 0.0)

    # Position sizing — units so that hitting stop == losing exactly risk_amount
    risk_amount = account_size * risk_pct / 100.0 if account_size > 0 else 0.0
    if stop_distance_currency > 0 and account_size > 0:
        position_size_units = risk_amount / stop_distance_currency
    else:
        position_size_units = 0.0

    position_value = position_size_units * entry

    # Sanity cap — position must never exceed 20% of portfolio
    max_position = account_size * 0.20
    if position_value > max_position:
        position_size_units = max_position / entry if entry > 0 else 0.0
        position_value      = position_size_units * entry

    position_pct   = (position_value / account_size * 100.0
                      if account_size > 0 else 0.0)

    # R:R targets — move from entry by the same distance as entry→stop
    r = abs(entry - stop)
    if pattern.direction == 'bullish':
        t1 = entry + r
        t2 = entry + 2 * r
        t3 = entry + 3 * r
    else:
        t1 = entry - r
        t2 = entry - 2 * r
        t3 = entry - 3 * r

    ep_t1 = round(position_size_units * r, 2)
    ep_t2 = round(position_size_units * 2 * r, 2)
    ep_t3 = round(position_size_units * 3 * r, 2)

    # KB alignment checks
    conv_align   = pattern.kb_conviction in ('high', 'strong', 'confirmed')
    regime_align = 'risk_on' in (pattern.kb_regime or '').lower()

    return PositionRecommendation(
        ticker                  = pattern.ticker,
        pattern_type            = pattern.pattern_type,
        direction               = pattern.direction,
        entry_zone_high         = round(pattern.zone_high, 4),
        entry_zone_low          = round(pattern.zone_low, 4),
        suggested_entry         = round(entry, 4),
        stop_loss               = round(stop, 4),
        stop_distance_pct       = round(stop_distance_pct, 2),
        stop_distance_currency  = round(stop_distance_currency, 4),
        account_size            = round(account_size, 2),
        account_currency        = currency,
        risk_pct                = risk_pct,
        risk_amount             = round(risk_amount, 2),
        position_size_units     = round(position_size_units, 4),
        position_value          = round(position_value, 2),
        position_pct_of_account = round(position_pct, 2),
        target_1                = round(t1, 4),
        target_2                = round(t2, 4),
        target_3                = round(t3, 4),
        expected_profit_t1      = ep_t1,
        expected_profit_t2      = ep_t2,
        expected_profit_t3      = ep_t3,
        kb_conviction_alignment = conv_align,
        kb_regime_alignment     = regime_align,
        composite_score         = pattern.quality_score,
    )

"""
core/tiers.py — Single source of truth for tier configuration.

All other modules (scheduler, API, frontend data endpoints) import from here.
One change propagates everywhere.
"""

from __future__ import annotations

from typing import Optional

_ALL_PATTERNS = [
    'fvg', 'ifvg', 'bpr', 'order_block', 'breaker', 'liquidity_void', 'mitigation',
]

# ── Calibration edge gate ─────────────────────────────────────────────────────
# Derived from 5.1M sample calibration dataset: AVG(hit_rate_t1 - stopped_out_rate)
# per (pattern_type, timeframe) across universe tickers with sample_size >= 100.
#
# A pattern×TF is allowed in tips only if its edge gap is positive (HR > stop rate).
# Negative gap = structural loser — the stop rate exceeds the win rate on average.
# Any tip sent on a negative-gap pattern is statistically likely to lose.
#
# Update this table when calibration accumulates new data (run edge_miner.scan()).
# NULL means "no calibration data — allow by default (don't penalise unknown)".

TIP_EDGE_GATE: dict[tuple[str, str], float] = {
    # pattern_type           timeframe   edge_gap
    ('liquidity_void',       '4h'):      +0.288,  # best single cell (small n, watch)
    ('liquidity_void',       '15m'):     +0.241,  # ✅ 445k samples — confirmed
    ('liquidity_void',       '1d'):      +0.224,  # ✅ 155k samples — confirmed
    ('liquidity_void',       '1h'):      +0.146,  # ✅ 131k samples — confirmed
    ('mitigation',           '1d'):      +0.169,  # ✅ 706k samples — confirmed
    ('mitigation',           '15m'):     +0.103,  # 📈 371k samples — modest
    ('mitigation',           '4h'):      +0.250,  # 📈 small n, monitor
    ('mitigation',           '1h'):      +0.007,  # ⚠️  near-zero — borderline
    ('breaker',              '4h'):      +0.006,  # ⚠️  near-zero — borderline
    ('breaker',              '15m'):     -0.002,  # 🚫 negative — block
    ('breaker',              '1d'):      -0.013,  # 🚫 negative — block
    ('breaker',              '1h'):      -0.117,  # 🚫 negative — block
    ('ifvg',                 '15m'):     -0.161,  # 🚫 confirmed loser
    ('ifvg',                 '1h'):      -0.340,  # 🚫 confirmed loser
    ('ifvg',                 '1d'):      -0.463,  # 🚫 confirmed loser
    ('order_block',          '15m'):     -0.223,  # 🚫 confirmed loser
    ('order_block',          '4h'):      -0.227,  # 🚫 confirmed loser
    ('order_block',          '1d'):      -0.298,  # 🚫 confirmed loser
    ('order_block',          '1h'):      -0.424,  # 🚫 confirmed loser
    ('fvg',                  '1d'):      -0.569,  # 🚫 worst pattern
    ('fvg',                  '15m'):     -0.626,  # 🚫 worst pattern
    ('fvg',                  '1h'):      -0.705,  # 🚫 worst pattern
    ('fvg',                  '4h'):      -0.925,  # 🚫 worst pattern
}

# Minimum edge gap for a pattern×TF to be sent as a tip.
# Set to 0.0 = must be net-positive expected value. Raise to tighten quality.
TIP_MIN_EDGE_GAP: float = 0.0


def tip_pattern_tf_allowed(pattern_type: str, timeframe: str) -> bool:
    """
    Return True if the pattern×timeframe combination has a positive calibration
    edge gap (hit_rate > stop_rate on average across the universe).
    Unknown combinations (not in TIP_EDGE_GATE) are allowed by default.
    """
    gap = TIP_EDGE_GATE.get((pattern_type, timeframe))
    if gap is None:
        return True  # no data → don't penalise, allow through
    return gap >= TIP_MIN_EDGE_GAP

TIER_CONFIG: dict = {
    'free': {
        'price_monthly':          0,
        'price_annual':           0,
        'delivery_days':          [],
        'briefing_days':          [],
        'batch_size':             0,
        'patterns':               [],
        'timeframes':             [],
        'targets':                0,
        'chat_queries_per_day':   0,
        'live_price_fetch':       False,
        'opportunity_scan':       False,
        'daily_briefing':         False,
        'alerts':                 [],
        'min_pattern_quality':    0.45,
        'min_asymmetry':          2.0,
    },
    'basic': {
        'price_monthly':          9,
        'price_annual':           86,
        'delivery_days':          ['monday', 'wednesday'],
        'briefing_days':          ['monday', 'wednesday'],
        'batch_size':             2,
        'patterns':               ['liquidity_void', 'mitigation'],  # fvg/ifvg have negative edge gaps
        'timeframes':             ['4h', '1d'],
        'targets':                2,
        'chat_queries_per_day':   10,
        'live_price_fetch':       False,
        'opportunity_scan':       False,
        'daily_briefing':         False,
        'alerts':                 ['zone', 'thesis'],
        'min_pattern_quality':    0.45,
        'min_asymmetry':          2.0,
    },
    'pro': {
        'price_monthly':          29,
        'price_annual':           278,
        'delivery_days':          ['monday'],
        'briefing_days':          ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'],
        'batch_size':             3,
        'patterns':               _ALL_PATTERNS,
        'timeframes':             ['4h', '1d'],
        'targets':                2,
        'chat_queries_per_day':   None,
        'live_price_fetch':       True,
        'opportunity_scan':       True,
        'daily_briefing':         True,
        'alerts':                 ['zone', 'thesis', 'profit_lock', 'trailing'],
        'min_pattern_quality':    0.45,
        'min_asymmetry':          2.0,
    },
    'premium': {
        'price_monthly':          79,
        'price_annual':           758,
        'delivery_days':          ['monday'],
        'briefing_days':          ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'],
        'batch_size':             5,
        'patterns':               _ALL_PATTERNS,
        'timeframes':             ['15m', '1h', '4h', '1d'],
        'targets':                3,
        'chat_queries_per_day':   None,
        'live_price_fetch':       True,
        'opportunity_scan':       True,
        'daily_briefing':         True,
        'alerts':                 ['zone', 'thesis', 'profit_lock', 'trailing', 't3', 'realtime'],
        'min_pattern_quality':    0.45,
        'min_asymmetry':          2.0,
    },
}


def get_tier(tier_name: str) -> dict:
    """Return the config dict for a tier, defaulting to 'free'."""
    return TIER_CONFIG.get(tier_name, TIER_CONFIG['free'])


def check_feature(tier_name: str, feature: str) -> bool:
    """
    Return True if the feature is enabled for the given tier.

    - bool values: returned directly
    - int values: True if > 0
    - None: True (None = unlimited = allowed)
    - list/other: True if non-empty
    """
    config = get_tier(tier_name)
    val = config.get(feature)
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return val > 0
    if val is None:
        return True
    return bool(val)


def _next_tier(tier_name: str) -> Optional[str]:
    """Return the next tier above tier_name, or None if already at top."""
    order = ['free', 'basic', 'pro', 'premium']
    try:
        idx = order.index(tier_name)
        return order[idx + 1] if idx + 1 < len(order) else None
    except ValueError:
        return 'basic'

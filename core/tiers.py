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
        'min_pattern_quality':    0.75,
        'min_asymmetry':          2.0,
    },
    'basic': {
        'price_monthly':          9,
        'price_annual':           86,
        'delivery_days':          ['monday', 'wednesday'],
        'briefing_days':          ['monday', 'wednesday'],
        'batch_size':             2,
        'patterns':               ['fvg', 'ifvg'],
        'timeframes':             ['4h', '1d'],
        'targets':                2,
        'chat_queries_per_day':   10,
        'live_price_fetch':       False,
        'opportunity_scan':       False,
        'daily_briefing':         False,
        'alerts':                 ['zone', 'thesis'],
        'min_pattern_quality':    0.75,
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
        'min_pattern_quality':    0.75,
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
        'min_pattern_quality':    0.75,
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

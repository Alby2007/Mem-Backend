"""
core/levels.py — Trader Experience Level Configuration

Single source of truth for the four trader_level values.
Controls communication style across tips, chat responses, and briefings.
Level is independent of subscription tier — a beginner on Premium still
gets plain English; a quant on Basic still gets dense output.

Valid levels: beginner | developing | experienced | quant
Default:      developing  (applied to all existing users via DB migration)
"""

from __future__ import annotations

from typing import Optional

TRADER_LEVELS: dict = {
    'beginner': {
        'jargon':          False,
        'explain_terms':   True,
        'show_greeks':     False,
        'show_raw_atoms':  False,
        'risk_warnings':   True,
        'tip_format':      'narrative',
        'chat_style':      'educational',
        'max_risk_pct':    1.0,
    },
    'developing': {
        'jargon':          'partial',
        'explain_terms':   False,
        'show_greeks':     False,
        'show_raw_atoms':  False,
        'risk_warnings':   True,
        'tip_format':      'standard',
        'chat_style':      'standard',
        'max_risk_pct':    2.0,
    },
    'experienced': {
        'jargon':          True,
        'explain_terms':   False,
        'show_greeks':     True,
        'show_raw_atoms':  False,
        'risk_warnings':   False,
        'tip_format':      'dense',
        'chat_style':      'direct',
        'max_risk_pct':    3.0,
    },
    'quant': {
        'jargon':          True,
        'explain_terms':   False,
        'show_greeks':     True,
        'show_raw_atoms':  True,
        'risk_warnings':   False,
        'tip_format':      'raw',
        'chat_style':      'analytical',
        'max_risk_pct':    5.0,
    },
}

VALID_LEVELS = frozenset(TRADER_LEVELS.keys())

# Human-readable labels for the profile UI
LEVEL_LABELS: dict = {
    'beginner':    'Beginner — New to trading, learning the basics',
    'developing':  'Developing — Some experience, building a strategy',
    'experienced': 'Experienced — Comfortable with technical analysis and risk management',
    'quant':       'Quant — Data-driven, comfortable with statistics and Greeks',
}


def get_level(name: Optional[str]) -> dict:
    """Return the config dict for a trader level, defaulting to 'developing'."""
    return TRADER_LEVELS.get(name or 'developing', TRADER_LEVELS['developing'])


def tip_format(name: Optional[str]) -> str:
    """Return the tip_format string for a level: 'narrative' | 'standard' | 'dense' | 'raw'."""
    return get_level(name).get('tip_format', 'standard')


def show_greeks(name: Optional[str]) -> bool:
    """Return True if this level should show options Greeks in tips and chat."""
    return bool(get_level(name).get('show_greeks', False))


def max_risk_pct(name: Optional[str]) -> float:
    """Return the maximum suggested risk % per trade for this level."""
    return float(get_level(name).get('max_risk_pct', 2.0))

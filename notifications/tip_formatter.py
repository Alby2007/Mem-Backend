"""
notifications/tip_formatter.py — Pattern Tip Telegram Formatter

Renders a PatternSignal + PositionRecommendation into a Telegram MarkdownV2
message suitable for direct send via TelegramNotifier.

Tier gating
===========
  basic: T1 and T2 targets only
  pro:   T1, T2, and T3 targets

All dynamic numeric and text values are escaped via _escape_mdv2() before
insertion. Static structural characters (*, _, ─, ━) are left unescaped
as they are intentional MarkdownV2 formatting.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from analytics.pattern_detector import PatternSignal
from analytics.position_calculator import PositionRecommendation

# ── MarkdownV2 escaping ────────────────────────────────────────────────────────

_MDV2_RESERVED = set(r'\_*[]()~`>#+-=|{}.!')

def _escape_mdv2(s: str) -> str:
    result = []
    for ch in str(s):
        if ch in _MDV2_RESERVED:
            result.append('\\')
        result.append(ch)
    return ''.join(result)


# ── Display helpers ────────────────────────────────────────────────────────────

_PATTERN_LABELS = {
    'fvg':            'Fair Value Gap',
    'ifvg':           'Inverse Fair Value Gap',
    'bpr':            'Balanced Price Range',
    'order_block':    'Order Block',
    'breaker':        'Breaker Block',
    'liquidity_void': 'Liquidity Void',
    'mitigation':     'Mitigation Block',
}

_TF_LABELS = {
    '15m': '15M',
    '1h':  '1H',
    '4h':  '4H',
    '1d':  'Daily',
}

_STATUS_EMOJI = {
    'open':             '✅',
    'partially_filled': '⚠️',
    'filled':           '❌',
    'broken':           '❌',
}

_DIRECTION_EMOJI = {
    'bullish': '📈',
    'bearish': '📉',
}


def _fmt_price(price: float) -> str:
    """Format a price with up to 4 significant decimal places."""
    if price >= 100:
        return f'{price:.2f}'
    if price >= 1:
        return f'{price:.4f}'
    return f'{price:.6f}'


def _fmt_currency(amount: float, currency: str = 'GBP') -> str:
    symbols = {'GBP': '£', 'USD': '$', 'EUR': '€'}
    sym = symbols.get(currency.upper(), currency)
    return f'{sym}{amount:,.2f}'


def _conviction_line(pattern: PatternSignal) -> str:
    conv = pattern.kb_conviction or 'unknown'
    icon = '✅' if conv in ('high', 'strong', 'confirmed') else '⚠️' if conv else '❓'
    return f'Conviction: {_escape_mdv2(conv.title())} {icon}'


def _regime_line(pattern: PatternSignal) -> str:
    regime = pattern.kb_regime or 'unknown'
    label  = regime.replace('_', ' ').title()
    icon   = '✅' if 'risk_on' in regime.lower() else '⚠️'
    return f'Regime: {_escape_mdv2(label)} {icon}'


def _signal_dir_line(pattern: PatternSignal) -> str:
    sig  = pattern.kb_signal_dir or 'unknown'
    icon = '✅' if (
        (pattern.direction == 'bullish' and sig in ('long', 'bullish', 'buy')) or
        (pattern.direction == 'bearish' and sig in ('short', 'bearish', 'sell'))
    ) else '⚠️'
    return f'Signal: {_escape_mdv2(sig.title())} {icon}'


# ── TIER_LIMITS ────────────────────────────────────────────────────────────────

TIER_LIMITS = {
    'basic': {
        'tips_per_day': 1,
        'patterns':     ['fvg', 'ifvg'],
        'timeframes':   ['1h'],
        'targets':      2,
    },
    'pro': {
        'tips_per_day': None,
        'patterns':     ['fvg', 'ifvg', 'bpr', 'order_block',
                         'breaker', 'liquidity_void', 'mitigation'],
        'timeframes':   ['15m', '1h', '4h', '1d'],
        'targets':      3,
    },
}


def pattern_allowed_for_tier(pattern_type: str, tier: str) -> bool:
    """Return True if this pattern_type is accessible on the given tier."""
    limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    return pattern_type in limits['patterns']


def timeframe_allowed_for_tier(timeframe: str, tier: str) -> bool:
    """Return True if this timeframe is accessible on the given tier."""
    limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    return timeframe in limits['timeframes']


# ── Formatter ──────────────────────────────────────────────────────────────────

def _parse_skew_filter(skew_filter: Optional[dict]) -> Optional[dict]:
    """
    Normalise the skew_filter argument into a usable dict.

    Accepts either:
      - A dict with keys: multiplier, stop_tighten_pct, reason
      - A dict with key 'skew_filter' whose value is the pipe-encoded string
        "multiplier|stop_tighten_pct|reason" from the KB atom

    Returns None when not active (multiplier == 1.0 or no data).
    Never raises.
    """
    if not skew_filter:
        return None
    try:
        # If the dict contains the raw KB atom string
        encoded = skew_filter.get('skew_filter', '')
        if encoded and '|' in encoded:
            parts = encoded.split('|')
            if len(parts) >= 3:
                multiplier   = float(parts[0])
                stop_tighten = float(parts[1])
                reason       = parts[2]
                skew_filter  = {'multiplier': multiplier,
                                'stop_tighten_pct': stop_tighten,
                                'reason': reason}

        multiplier = float(skew_filter.get('multiplier', 1.0))
        if multiplier >= 1.0:
            return None  # no reduction — filter not active
        return skew_filter
    except Exception:
        return None


def format_tip(
    pattern:     PatternSignal,
    position:    Optional[PositionRecommendation],
    tier:        str = 'basic',
    skew_filter: Optional[dict] = None,
) -> str:
    """
    Render a complete Telegram MarkdownV2 tip message.

    Parameters
    ----------
    pattern      The best PatternSignal for this user's tip.
    position     PositionRecommendation (None if account_size not configured).
    tier         'basic' or 'pro' — controls T3 visibility.
    skew_filter  Optional dict from KB skew_filter atom or
                 _compute_skew_filter_atoms output.  When active (multiplier
                 < 1.0), appends a warning block showing why position was
                 sized down.  Accepts pipe-encoded KB string or plain dict.

    Returns
    -------
    MarkdownV2-escaped string ready for Telegram sendMessage.
    """
    limits       = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    show_t3      = limits['targets'] >= 3
    tf_label     = _TF_LABELS.get(pattern.timeframe, pattern.timeframe.upper())
    pat_label    = _PATTERN_LABELS.get(pattern.pattern_type, pattern.pattern_type.replace('_', ' ').title())
    dir_emoji    = _DIRECTION_EMOJI.get(pattern.direction, '')
    status_emoji = _STATUS_EMOJI.get(pattern.status, '❓')
    direction_label = pattern.direction.title()

    now_str = datetime.now(timezone.utc).strftime('%A %d %b, %H:%M UTC')

    # Normalise skew_filter — None when not active
    active_skew = _parse_skew_filter(skew_filter)

    lines = [
        f'⚡ *YOUR DAILY TIP — {_escape_mdv2(pattern.ticker)}*',
        f'_{_escape_mdv2(now_str)}_',
        '',
        f'{dir_emoji} *{_escape_mdv2(pat_label)}* — {_escape_mdv2(tf_label)}',
        f'Zone: {_escape_mdv2(_fmt_price(pattern.zone_low))} — {_escape_mdv2(_fmt_price(pattern.zone_high))}',
        f'Gap size: {_escape_mdv2(f"+{pattern.zone_size_pct:.1f}%")} \\| Status: {_escape_mdv2(pattern.status.replace("_"," ").title())} {status_emoji}',
    ]

    if position:
        currency = position.account_currency
        lines += [
            '',
            '━━━━━━━━━━━━━━━',
            f'💼 *YOUR POSITION*',
            '━━━━━━━━━━━━━━━',
            f'Account: {_escape_mdv2(_fmt_currency(position.account_size, currency))}',
            f'Risk per trade: {_escape_mdv2(str(position.risk_pct))}% = {_escape_mdv2(_fmt_currency(position.risk_amount, currency))}',
            '',
            f'Suggested entry: {_escape_mdv2(_fmt_price(position.suggested_entry))} \\(zone midpoint\\)',
            f'Stop loss: {_escape_mdv2(_fmt_price(position.stop_loss))} \\({_escape_mdv2(f"-{position.stop_distance_pct:.2f}%")} below zone\\)',
            f'Units to buy: {_escape_mdv2(str(int(position.position_size_units)))} shares',
            f'Position value: {_escape_mdv2(_fmt_currency(position.position_value, currency))} \\({_escape_mdv2(f"{position.position_pct_of_account:.1f}%")} of account\\)',
            '',
            f'Target 1: {_escape_mdv2(_fmt_price(position.target_1))} → \\+{_escape_mdv2(_fmt_currency(position.expected_profit_t1, currency))} \\(1:1\\)',
            f'Target 2: {_escape_mdv2(_fmt_price(position.target_2))} → \\+{_escape_mdv2(_fmt_currency(position.expected_profit_t2, currency))} \\(1:2\\) ⭐',
        ]
        if show_t3:
            lines.append(
                f'Target 3: {_escape_mdv2(_fmt_price(position.target_3))} → \\+{_escape_mdv2(_fmt_currency(position.expected_profit_t3, currency))} \\(1:3\\)'
            )
        else:
            lines.append('_Target 3: Pro tier only_')

        # ── Skew filter warning block ─────────────────────────────────────
        # Only rendered when skew_filter is active (multiplier < 1.0).
        if active_skew:
            multiplier   = active_skew.get('multiplier', 1.0)
            stop_tighten = active_skew.get('stop_tighten_pct', 0.0)
            reason       = active_skew.get('reason', 'skew_filter')
            reason_label = reason.replace('_', ' ').title()

            size_note = ('Position blocked' if multiplier == 0.0
                         else f'Size reduced {int((1.0 - multiplier) * 100)}%')
            stop_note = (f' \\| Stop tightened {int(stop_tighten)}%'
                         if stop_tighten > 0 else '')

            lines += [
                '',
                f'⚠️ *Skew filter active — {_escape_mdv2(reason_label)}*',
                f'{_escape_mdv2(size_note)}{stop_note}',
            ]

    lines += [
        '',
        '━━━━━━━━━━━━━━━',
        f'📊 *KB CONTEXT*',
        '━━━━━━━━━━━━━━━',
        _conviction_line(pattern),
        _signal_dir_line(pattern),
        _regime_line(pattern),
        f'Pattern score: {_escape_mdv2(f"{pattern.quality_score:.2f}/1.0")}',
    ]

    return '\n'.join(lines)


def tip_to_dict(
    pattern:  PatternSignal,
    position: Optional[PositionRecommendation],
    tier:     str = 'basic',
) -> dict:
    """
    Serialise a tip to a plain dict (for the preview endpoint, no Telegram send).
    """
    limits   = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    show_t3  = limits['targets'] >= 3
    result = {
        'ticker':        pattern.ticker,
        'pattern_type':  pattern.pattern_type,
        'direction':     pattern.direction,
        'timeframe':     pattern.timeframe,
        'zone_high':     pattern.zone_high,
        'zone_low':      pattern.zone_low,
        'zone_size_pct': pattern.zone_size_pct,
        'formed_at':     pattern.formed_at,
        'status':        pattern.status,
        'quality_score': pattern.quality_score,
        'kb_conviction': pattern.kb_conviction,
        'kb_regime':     pattern.kb_regime,
        'kb_signal_dir': pattern.kb_signal_dir,
        'tier':          tier,
    }
    if position:
        result['position'] = {
            'suggested_entry':          position.suggested_entry,
            'stop_loss':                position.stop_loss,
            'stop_distance_pct':        position.stop_distance_pct,
            'account_size':             position.account_size,
            'account_currency':         position.account_currency,
            'risk_pct':                 position.risk_pct,
            'risk_amount':              position.risk_amount,
            'position_size_units':      position.position_size_units,
            'position_value':           position.position_value,
            'position_pct_of_account':  position.position_pct_of_account,
            'target_1':                 position.target_1,
            'target_2':                 position.target_2,
            'target_3':                 position.target_3 if show_t3 else None,
            'expected_profit_t1':       position.expected_profit_t1,
            'expected_profit_t2':       position.expected_profit_t2,
            'expected_profit_t3':       position.expected_profit_t3 if show_t3 else None,
            'kb_conviction_alignment':  position.kb_conviction_alignment,
            'kb_regime_alignment':      position.kb_regime_alignment,
        }
    return result

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


def _fmt_pct(p: float) -> str:
    """Format a 0.0–1.0 probability as a percentage string."""
    return f'{int(round(p * 100))}%'


def _forecast_block(forecast: object, currency: str) -> list:
    """
    Render the probabilistic forecast block lines for a tip.
    Only called when position.forecast is not None.
    """
    lines = [
        '',
        '━━━━━━━━━━━━━━━',
        '🎯 *PROBABILITY FORECAST*',
        '━━━━━━━━━━━━━━━',
    ]
    try:
        p_t1  = getattr(forecast, 'p_hit_t1', None)
        p_t2  = getattr(forecast, 'p_hit_t2', None)
        p_stp = getattr(forecast, 'p_stopped_out', None)
        ev    = getattr(forecast, 'expected_value_gbp', None)
        ci_lo = getattr(forecast, 'ci_90_low', None)
        ci_hi = getattr(forecast, 'ci_90_high', None)
        days  = getattr(forecast, 'days_to_target_median', None)
        reg_adj = getattr(forecast, 'regime_adjustment_pct', 0.0)
        iv_adj  = getattr(forecast, 'iv_adjustment_pct', 0.0)
        regime  = getattr(forecast, 'market_regime', None)
        used_prior = getattr(forecast, 'used_prior', False)

        sym = {'GBP': '£', 'USD': '$', 'EUR': '€'}.get(currency.upper(), currency)

        if p_t1 is not None and p_t2 is not None and p_stp is not None:
            lines.append(
                'P\\(T1\\): ' + _escape_mdv2(_fmt_pct(p_t1)) + ' \u00b7 '
                'P\\(T2\\): ' + _escape_mdv2(_fmt_pct(p_t2)) + ' \u00b7 '
                'P\\(Stop\\): ' + _escape_mdv2(_fmt_pct(p_stp))
            )
        if ev is not None:
            ev_str = f'{sym}{abs(ev):,.0f}'
            sign   = '\\+' if ev >= 0 else '\\-'
            lines.append('EV: ' + sign + _escape_mdv2(ev_str) + ' at 1% risk')
        if days is not None:
            lines.append('Median days to target: ' + _escape_mdv2(str(days)))
        if ci_lo is not None and ci_hi is not None:
            sym_lo = '\\-' if ci_lo < 0 else '\\+'
            sym_hi = '\\-' if ci_hi < 0 else '\\+'
            lines.append(
                '90% CI: ' + sym_lo + _escape_mdv2(f'{sym}{abs(ci_lo):,.0f}') +
                ' \u2192 ' + sym_hi + _escape_mdv2(f'{sym}{abs(ci_hi):,.0f}')
            )
        adj_parts = []
        if reg_adj and regime:
            sign = '\\+' if reg_adj >= 0 else ''
            adj_parts.append(
                'Regime adj: ' + sign + _escape_mdv2(f'{reg_adj:.0f}%') +
                ' \\(' + _escape_mdv2(regime.replace('_', ' ')) + '\\)'
            )
        if iv_adj:
            sign = '\\+' if iv_adj >= 0 else ''
            adj_parts.append('IV adj: ' + sign + _escape_mdv2(f'{iv_adj:.0f}%'))
        if adj_parts:
            lines.append(' \u00b7 '.join(adj_parts))
        if used_prior:
            lines.append('_\\(Using population\\-level priors \u2014 insufficient history\\)_')
    except Exception:
        pass
    return lines


_TIP_SOURCE_LABELS = {
    'watchlist':   ('🎯', 'Signal from your watchlist'),
    'portfolio':   ('📂', 'Signal from your portfolio — your watchlist had no qualifying patterns today'),
    'connected':   ('🔗', 'Signal from a sector-correlated ticker — your portfolio was quiet today'),
    'market-wide': ('🌐', 'Market-wide signal — your watchlist and portfolio were quiet today'),
}


def format_tip(
    pattern:     PatternSignal,
    position:    Optional[PositionRecommendation],
    tier:        str = 'basic',
    skew_filter: Optional[dict] = None,
    calibration: Optional[object] = None,
    tip_source:  Optional[str] = None,
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
    tip_source   One of 'watchlist', 'portfolio', 'connected', 'market-wide',
                 or None (silent — All Markets mode).

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

    kb_lines = [
        '',
        '━━━━━━━━━━━━━━━',
        f'📊 *KB CONTEXT*',
        '━━━━━━━━━━━━━━━',
        _conviction_line(pattern),
        _signal_dir_line(pattern),
        _regime_line(pattern),
        f'Pattern score: {_escape_mdv2(f"{pattern.quality_score:.2f}/1.0")}',
    ]

    # Calibration hit rate line — only shown if calibration_confidence >= 0.50
    if calibration is not None:
        try:
            conf = getattr(calibration, 'calibration_confidence', 0.0) or 0.0
            if conf >= 0.50:
                hr2  = getattr(calibration, 'hit_rate_t2', None)
                n    = getattr(calibration, 'sample_size', 0)
                regime = getattr(calibration, 'market_regime', None) or 'all regimes'
                if hr2 is not None:
                    kb_lines.append(
                        f'Historical hit rate: {_escape_mdv2(f"{hr2*100:.0f}%")} to T2 '
                        f'\\({_escape_mdv2(str(n))} trades, {_escape_mdv2(regime.replace("_"," "))}\\)'
                    )
        except Exception:
            pass

    # Probability forecast block — rendered when SignalForecaster ran successfully
    if position and getattr(position, 'forecast', None) is not None:
        currency = position.account_currency
        lines += _forecast_block(position.forecast, currency)

    lines += kb_lines

    # ── Fallback source label ─────────────────────────────────────────────────
    if tip_source and tip_source in _TIP_SOURCE_LABELS:
        icon, label = _TIP_SOURCE_LABELS[tip_source]
        lines += [
            '',
            f'_{icon} {_escape_mdv2(label)}_',
        ]

    return '\n'.join(lines)


def format_position_update(alert_type: str, pos: dict, current_price: float) -> str:
    """
    Render a Telegram MarkdownV2 position update alert.

    Parameters
    ----------
    alert_type    One of: t1_zone_reached, t2_zone_reached, stop_loss_zone_reached,
                  pattern_invalidated, conviction_tier_dropped, regime_shift_detected,
                  earnings_within_2_days, sector_tailwind_reversed, short_squeeze_developing
    pos           tip_followups row dict
    current_price Current market price
    """
    ticker     = pos.get('ticker', '?')
    entry      = pos.get('entry_price') or 0.0
    stop       = pos.get('stop_loss')
    t1         = pos.get('target_1')
    t2         = pos.get('target_2')
    direction  = pos.get('direction', 'bullish')
    bullish    = direction != 'bearish'

    pnl_raw    = ((current_price - entry) / entry * 100) if entry else 0.0
    if not bullish:
        pnl_raw = -pnl_raw
    pnl_sign   = '\\+' if pnl_raw >= 0 else ''
    pnl_str    = f'{pnl_sign}{pnl_raw:.1f}%'

    _e  = _escape_mdv2
    _ep = lambda p: _e(_fmt_price(p)) if p else 'N/A'

    _ALERT_HEADERS = {
        't1_zone_reached':          ('⚡', 'POSITION UPDATE', 'T1 zone reached ✅'),
        't2_zone_reached':          ('🎯', 'POSITION UPDATE', 'T2 zone reached ✅'),
        'stop_loss_zone_reached':   ('🛑', 'POSITION ALERT',  'Approaching stop zone ⚠️'),
        'pattern_invalidated':      ('❌', 'POSITION ALERT',  'Pattern invalidated'),
        'conviction_tier_dropped':  ('⚠️', 'POSITION UPDATE', 'Conviction tier dropped'),
        'regime_shift_detected':    ('🔄', 'POSITION UPDATE', 'Regime shift detected'),
        'earnings_within_2_days':   ('📋', 'POSITION ALERT',  'Earnings within 2 days'),
        'sector_tailwind_reversed': ('↩️', 'POSITION UPDATE', 'Sector tailwind reversed'),
        'short_squeeze_developing': ('🔥', 'POSITION UPDATE', 'Short squeeze developing'),
    }

    icon, header, subtitle = _ALERT_HEADERS.get(
        alert_type, ('📍', 'POSITION UPDATE', alert_type.replace('_', ' ').title())
    )

    lines = [
        f'{icon} *{_e(header)} — {_e(ticker)}*',
        f'_{_e(subtitle)}_',
        '',
        f'📍 Price: {_ep(current_price)}',
        f'Entry was: {_ep(entry)} \\| P&L: {_e(pnl_str)}',
    ]

    if alert_type == 't1_zone_reached' and t2:
        lines += [
            '',
            '💡 *Recommended action:*',
            f'Take 50% at current price \\({_ep(current_price)}\\)',
            f'Move stop on remainder to breakeven \\({_ep(entry)}\\)',
            f'Next target: T2 at {_ep(t2)}',
        ]
    elif alert_type == 't2_zone_reached':
        lines += [
            '',
            '💡 *Recommended action:*',
            'Consider closing position or trailing stop',
            f'T2 target at {_ep(t2)} has been reached',
        ]
    elif alert_type == 'stop_loss_zone_reached' and stop:
        dist = abs((current_price - stop) / entry * 100) if entry else 0
        lines += [
            f'Stop loss: {_ep(stop)} \\({_e(f"{dist:.1f}%")} away\\)',
            '',
            '💡 *Recommended action:*',
            'Close position — approaching your defined risk level',
        ]
    elif alert_type == 'pattern_invalidated':
        lines += [
            '',
            '💡 *Recommended action:*',
            'Setup invalidated — KB signal has reversed',
            'Consider closing to preserve capital',
        ]
    elif alert_type == 'conviction_tier_dropped':
        entry_conv   = pos.get('conviction_at_entry', 'unknown')
        lines += [
            f'Conviction at entry: {_e(entry_conv.title())}',
            'Current conviction has decreased',
            '',
            '💡 *Recommended action:*',
            'Consider reducing position size or tightening stop',
        ]
    elif alert_type == 'regime_shift_detected':
        entry_regime = pos.get('regime_at_entry', 'unknown')
        lines += [
            f'Regime at entry: {_e(entry_regime.replace("_", " ").title())}',
            'Market regime has changed — reassess setup validity',
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

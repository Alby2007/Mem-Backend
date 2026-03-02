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
# Single source of truth lives in core/tiers.py — re-exported here for
# backwards compatibility with any direct TIER_LIMITS imports.

from core.tiers import TIER_CONFIG as TIER_LIMITS, _ALL_PATTERNS


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


def _format_tip_narrative(
    pattern:  PatternSignal,
    position: Optional[PositionRecommendation],
) -> str:
    """
    Beginner-friendly tip: plain English, no jargon, risk-first framing.
    No quality scores, no Greek letters, no raw atom values.
    """
    _e = _escape_mdv2
    direction_word = 'rising' if pattern.direction == 'bullish' else 'falling'
    direction_label = 'Bullish \\(upward\\)' if pattern.direction == 'bullish' else 'Bearish \\(downward\\)'
    now_str = datetime.now(timezone.utc).strftime('%A %d %b, %H:%M UTC')

    lines = [
        f'📋 *TRADE IDEA — {_e(pattern.ticker)}*',
        f'_{_e(now_str)}_',
        '',
        f'*What is this?*',
        f'A {direction_label} setup has been detected on {_e(pattern.ticker)}\\.',
        f'The price has created a support zone \\(an area where buyers have previously stepped in\\) '
        f'between {_e(_fmt_price(pattern.zone_low))} and {_e(_fmt_price(pattern.zone_high))}\\.',
        '',
        f'*What does this mean?*',
        f'If the price returns to this zone, it may be a good area to consider buying',
        f'\\(for a {_e(direction_word)} move\\)\\. This is not guaranteed — the market can always move against you\\.',
    ]

    if position:
        _e2 = _escape_mdv2
        lines += [
            '',
            f'*If you decide to act:*',
            f'• Suggested entry \\(where to buy\\): {_e2(_fmt_price(position.suggested_entry))}',
            f'• Stop loss \\(where to exit if wrong\\): {_e2(_fmt_price(position.stop_loss))}',
            f'  ↳ A stop loss is an automatic exit point that limits your losses if the trade goes against you\\.',
            f'• First profit target: {_e2(_fmt_price(position.target_1))}',
            f'• Risk per trade: {_e2(str(position.risk_pct))}% of your account',
        ]
    else:
        lines += [
            '',
            '_Set your account size in Settings → Tips to see position sizing_',
        ]

    lines += [
        '',
        '⚠️ *Important risk reminder*',
        'Only risk money you can genuinely afford to lose\\. Never risk more than you are',
        'comfortable losing entirely\\. This is not financial advice\\.',
    ]
    return '\n'.join(lines)


def _format_tip_raw(
    pattern:    PatternSignal,
    position:   Optional[PositionRecommendation],
    tier:       str,
    db_path:    Optional[str] = None,
) -> str:
    """
    Quant/dense tip: maximum information density, raw atom values, greeks inline.
    Single-line header format where possible.
    """
    _e = _escape_mdv2
    tf   = _TF_LABELS.get(pattern.timeframe, pattern.timeframe.upper())
    pat  = pattern.pattern_type.upper()
    qs   = f'{pattern.quality_score:.2f}'
    conv = (pattern.kb_conviction or '?').upper()
    sig  = (pattern.kb_signal_dir or '?')
    reg  = (pattern.kb_regime or '?').replace('_', ' ')
    now_str = datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')

    lines = [
        f'⚡ *{_e(pattern.ticker)}* \\| {_e(pat)} \\| {_e(tf)} \\| Q:{_e(qs)} \\| Conv:{_e(conv)}',
        f'`{_e(now_str)}`',
        f'Zone: {_e(_fmt_price(pattern.zone_low))}–{_e(_fmt_price(pattern.zone_high))} '
        f'\\| Dir: {_e(sig)} \\| Regime: {_e(reg)}',
    ]

    if position:
        limits   = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
        show_t3  = limits['targets'] >= 3
        currency = position.account_currency
        asym = (position.target_2 - position.suggested_entry) / max(
            abs(position.suggested_entry - position.stop_loss), 0.0001
        ) if position.suggested_entry and position.stop_loss else 0.0
        lines += [
            f'Entry: {_e(_fmt_price(position.suggested_entry))} \\| SL: {_e(_fmt_price(position.stop_loss))} '
            f'\\| T1: {_e(_fmt_price(position.target_1))} \\| T2: {_e(_fmt_price(position.target_2))}'
            + (f' \\| T3: {_e(_fmt_price(position.target_3))}' if show_t3 else ''),
            f'Asymmetry: 1:{_e(f"{asym:.1f}")} \\| Size: {_e(str(position.risk_pct))}% \\| '
            f'Risk: {_e(_fmt_currency(position.risk_amount, currency))}',
        ]
    else:
        lines.append('_No position sizing — set account size in Settings → Tips_')

    # Inline greeks from DB if available
    if db_path:
        try:
            import sqlite3 as _sq
            _gc = _sq.connect(db_path, timeout=5)
            _greek_preds = ['delta_atm', 'iv_true', 'put_call_oi_ratio', 'gamma_exposure']
            _greek_vals = {}
            for _p in _greek_preds:
                _row = _gc.execute(
                    "SELECT object FROM facts WHERE subject=? AND predicate=? ORDER BY timestamp DESC LIMIT 1",
                    (pattern.ticker.lower(), _p)
                ).fetchone()
                if _row:
                    _greek_vals[_p] = _row[0]
            _gc.close()
            if _greek_vals:
                _g_parts = []
                if 'delta_atm' in _greek_vals:
                    _g_parts.append(f'Δ:{_e(_greek_vals["delta_atm"])}')
                if 'iv_true' in _greek_vals:
                    _g_parts.append(f'IV:{_e(_greek_vals["iv_true"])}%')
                if 'put_call_oi_ratio' in _greek_vals:
                    _g_parts.append(f'PCR:{_e(_greek_vals["put_call_oi_ratio"])}')
                if 'gamma_exposure' in _greek_vals:
                    try:
                        _gex = float(_greek_vals['gamma_exposure'])
                        _gex_dir = 'long\u03b3' if _gex >= 0 else 'short\u03b3'
                        _g_parts.append(f'GEX:{_e(f"{_gex:,.0f}")} \\({_e(_gex_dir)}\\)')
                    except Exception:
                        pass
                if _g_parts:
                    lines.append('Greeks: ' + ' \\| '.join(_g_parts))
        except Exception:
            pass

    return '\n'.join(lines)


def format_tip(
    pattern:      PatternSignal,
    position:     Optional[PositionRecommendation],
    tier:         str = 'basic',
    skew_filter:  Optional[dict] = None,
    calibration:  Optional[object] = None,
    tip_source:   Optional[str] = None,
    trader_level: str = 'developing',
    db_path:      Optional[str] = None,
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
    # ── Trader level branching ────────────────────────────────────────────────
    _level = (trader_level or 'developing').lower()
    if _level == 'beginner':
        return _format_tip_narrative(pattern, position)
    if _level == 'quant':
        return _format_tip_raw(pattern, position, tier, db_path=db_path)

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


def _format_single_setup(
    pattern_row: dict,
    position,
    tier: str,
    idx: int,
    tip_source: Optional[str] = None,
) -> list:
    """
    Render one setup block for a weekly batch message.
    Returns a list of MarkdownV2 lines (no trailing newline).
    """
    limits    = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    show_t3   = limits['targets'] >= 3
    tf_label  = _TF_LABELS.get(pattern_row['timeframe'], pattern_row['timeframe'].upper())
    pat_label = _PATTERN_LABELS.get(
        pattern_row['pattern_type'],
        pattern_row['pattern_type'].replace('_', ' ').title()
    )
    dir_emoji = _DIRECTION_EMOJI.get(pattern_row['direction'], '')
    ticker    = pattern_row['ticker']
    zone_low  = pattern_row['zone_low']
    zone_high = pattern_row['zone_high']
    quality   = pattern_row.get('quality_score') or 0.0

    lines = [
        f'*Setup {idx} — {_escape_mdv2(ticker)}*',
        f'{dir_emoji} {_escape_mdv2(pat_label)} \\| {_escape_mdv2(tf_label)} \\| Q: {_escape_mdv2(f"{quality:.2f}")}',
        f'Zone: {_escape_mdv2(_fmt_price(zone_low))} — {_escape_mdv2(_fmt_price(zone_high))}',
    ]

    if position:
        currency = position.account_currency
        lines += [
            f'Entry: {_escape_mdv2(_fmt_price(position.suggested_entry))} \\| '
            f'Stop: {_escape_mdv2(_fmt_price(position.stop_loss))}',
            f'T1: {_escape_mdv2(_fmt_price(position.target_1))} \\| '
            f'T2: {_escape_mdv2(_fmt_price(position.target_2))}',
        ]
        if show_t3:
            lines.append(f'T3: {_escape_mdv2(_fmt_price(position.target_3))}')
        lines.append(
            f'Size: {_escape_mdv2(str(int(position.position_size_units)))} units \\| '
            f'Risk: {_escape_mdv2(_fmt_currency(position.risk_amount, currency))}'
        )
    else:
        lines.append(f'_Set account size in tips config for position sizing_')

    return lines


def format_weekly_batch(
    batch: list,
    tier: str,
    weekday: str,
    monday_status: Optional[list] = None,
    tip_source: Optional[str] = None,
) -> str:
    """
    Render a weekly batch Telegram MarkdownV2 message.

    Parameters
    ----------
    batch         List of (pattern_row, position) tuples.
    tier          'basic' or 'pro'.
    weekday       'monday' or 'wednesday'.
    monday_status List of dicts from _check_monday_status(), Wednesday only.
    tip_source    Fallback source label (same as format_tip).
    """
    from datetime import datetime, timezone as _tz
    now   = datetime.now(_tz.utc)
    date_str = _escape_mdv2(now.strftime('%a %d %b'))

    if weekday == 'monday':
        header = f'📅 *YOUR WEEK AHEAD — {date_str}*'
        footer_count = len(batch)
        footer = f'_{_escape_mdv2(str(footer_count))} setup{"s" if footer_count != 1 else ""} for the week — act when price reaches the zone\\._'
    else:
        header = f'📊 *MIDWEEK UPDATE — {date_str}*'
        footer_count = len(batch)
        footer = f'_{_escape_mdv2(str(footer_count))} fresh setup{"s" if footer_count != 1 else ""} — plus Monday status below\\._'

    lines = [header, '']

    # ── Monday status block (Wednesday only) ─────────────────────────────────
    if monday_status:
        lines += ['━━━━━━━━━━━━━━━', '🔄 *MONDAY SETUPS — STATUS*', '━━━━━━━━━━━━━━━']
        for entry in monday_status:
            tkr    = _escape_mdv2(entry['ticker'])
            status = entry['status']   # 'in_zone', 'not_triggered', 'zone_broken'
            price  = entry.get('last_price')
            if status == 'in_zone':
                price_str = f' \\({_escape_mdv2(_fmt_price(price))}\\)' if price else ''
                stop_str  = f' — tighten stop to {_escape_mdv2(_fmt_price(entry["stop_loss"]))}' if entry.get('stop_loss') else ''
                lines.append(f'  {tkr}: ✅ Price in zone{price_str}{stop_str}')
            elif status == 'not_triggered':
                lines.append(f'  {tkr}: ⏳ Not yet triggered \\(price away from zone\\)')
            else:
                lines.append(f'  {tkr}: ❌ Zone broken — setup invalidated')
        lines.append('')

    # ── Setup blocks ─────────────────────────────────────────────────────────
    for i, (pattern_row, position) in enumerate(batch, start=1):
        lines.append('━━━━━━━━━━━━━━━')
        lines += _format_single_setup(pattern_row, position, tier, i)

    lines += ['━━━━━━━━━━━━━━━', '', footer]

    # ── Fallback source label ─────────────────────────────────────────────────
    if tip_source and tip_source in _TIP_SOURCE_LABELS:
        icon, label = _TIP_SOURCE_LABELS[tip_source]
        lines += ['', f'_{icon} {_escape_mdv2(label)}_']

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
        't1_zone_reached':          ('\u26a1', 'POSITION UPDATE', 'T1 zone reached \u2705'),
        't2_zone_reached':          ('\U0001f3af', 'POSITION UPDATE', 'T2 zone reached \u2705'),
        'stop_loss_zone_reached':   ('\U0001f6d1', 'POSITION ALERT',  'Approaching stop zone \u26a0\ufe0f'),
        'pattern_invalidated':      ('\u274c', 'POSITION ALERT',  'Pattern invalidated'),
        'conviction_tier_dropped':  ('\u26a0\ufe0f', 'POSITION UPDATE', 'Conviction tier dropped'),
        'regime_shift_detected':    ('\U0001f504', 'POSITION UPDATE', 'Regime shift detected'),
        'earnings_within_2_days':   ('\U0001f4cb', 'POSITION ALERT',  'Earnings within 2 days'),
        'sector_tailwind_reversed': ('\u21a9\ufe0f', 'POSITION UPDATE', 'Sector tailwind reversed'),
        'short_squeeze_developing': ('\U0001f525', 'POSITION UPDATE', 'Short squeeze developing'),
        't1_profit_lock':           ('\U0001f4b0', 'PROFIT ALERT',   'T1 reached \u2014 KB signals weakening'),
        't2_profit_lock':           ('\U0001f4b0', 'PROFIT ALERT',   'T2 reached \u2014 KB signals weakening'),
        'trailing_pullback':        ('\u26a1', 'PROFIT ALERT',   'Pulled back from session high'),
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
            '\U0001f4a1 *Recommended action:*',
            f'Take 50% at current price \\({_ep(current_price)}\\)',
            f'Move stop on remainder to breakeven \\({_ep(entry)}\\)',
            f'Next target: T2 at {_ep(t2)}',
        ]
    elif alert_type == 't1_profit_lock':
        conf_str = f'{int(confidence * 100)}%' if confidence is not None else 'low'
        lines += [
            '',
            f'KB confidence: {_e(conf_str)} and deteriorating',
            '',
            '\U0001f4a1 *Recommended action:*',
            f'T1 is on the table \\({_ep(current_price)} \\+{_e(pnl_str)}\\)',
            'KB signals weakening \\— consider full exit rather than holding for T2',
            f'Stop at {_ep(pos.get("stop_loss"))} if holding',
        ] if t2 else [
            '',
            f'KB confidence: {_e(conf_str)} and deteriorating',
            '',
            '\U0001f4a1 *Recommended action:*',
            f'Take profit here \\({_ep(current_price)}\\) \\— KB signals weakening',
        ]
    elif alert_type == 't2_zone_reached':
        lines += [
            '',
            '\U0001f4a1 *Recommended action:*',
            'Consider closing position or trailing stop',
            f'T2 target at {_ep(t2)} has been reached',
        ]
    elif alert_type == 'stop_loss_zone_reached' and stop:
        dist = abs((current_price - stop) / entry * 100) if entry else 0
        lines += [
            f'Stop loss: {_ep(stop)} \\({_e(f"{dist:.1f}%")} away\\)',
            '',
            '\U0001f4a1 *Recommended action:*',
            'Close position — approaching your defined risk level',
        ]
    elif alert_type == 'pattern_invalidated':
        lines += [
            '',
            '\U0001f4a1 *Recommended action:*',
            'Setup invalidated — KB signal has reversed',
            'Consider closing to preserve capital',
        ]
    elif alert_type == 'conviction_tier_dropped':
        entry_conv   = pos.get('conviction_at_entry', 'unknown')
        lines += [
            f'Conviction at entry: {_e(entry_conv.title())}',
            'Current conviction has decreased',
            '',
            '\U0001f4a1 *Recommended action:*',
            'Consider reducing position size or tightening stop',
        ]
    elif alert_type == 'regime_shift_detected':
        entry_regime = pos.get('regime_at_entry', 'unknown')
        lines += [
            f'Regime at entry: {_e(entry_regime.replace("_", " ").title())}',
            'Market regime has changed \\— reassess setup validity',
        ]
    elif alert_type == 't2_profit_lock':
        conf_str = f'{int(confidence * 100)}%' if confidence is not None else 'low'
        lines += [
            '',
            f'KB confidence: {_e(conf_str)} and deteriorating',
            '',
            '\U0001f4a1 *Recommended action:*',
            f'T2 is on the table \\({_ep(current_price)}\\) \\+{_e(pnl_str)}\\)',
            "Don't give this back \\— KB showing weakness",
            'Consider closing or moving stop to T1',
        ]
    elif alert_type == 'trailing_pullback':
        peak = pos.get('peak_price')
        pullback_pct = 0.0
        if peak and peak > 0:
            pullback_pct = abs(peak - current_price) / peak * 100
        peak_pnl_str = ''
        if peak and entry:
            peak_pnl = (peak - entry) / entry * 100
            if pos.get('direction') == 'bearish':
                peak_pnl = -peak_pnl
            sign = '\\+' if peak_pnl >= 0 else ''
            peak_pnl_str = f' \\({_e(sign)}{_e(f"{peak_pnl:.1f}%")} at peak\\)'
        lines += [
            '',
            f'Session high: {_ep(peak)}{peak_pnl_str}',
            f'Pullback: {_e(f"{pullback_pct:.1f}%")} from that level',
            '',
            '\U0001f4a1 *Decision point:*',
            'Partial exit locks in profit \\— position may continue or reverse',
            f'Full exit at {_ep(current_price)} secures {_e(pnl_str)}',
            f'Stop at {_ep(pos.get("stop_loss"))} if holding for T2',
        ]

    return '\n'.join(lines)


_PREDICATE_LABELS = {
    'regime_label':      'Regime',
    'market_regime':     'Market regime',
    'conviction_tier':   'Conviction',
    'macro_signal':      'Macro signal',
    'geopolitical_risk': 'Geopolitical risk',
    'sector_tailwind':   'Sector tailwind',
    'pre_earnings_flag': 'Earnings flag',
    'signal_direction':  'Signal direction',
}


def _format_open_position_line(pos: dict, current_price: Optional[float] = None) -> str:
    """
    Render one open position as a single MarkdownV2 line.
    watching → 📍 On radar
    active   → 🔓 In position
    """
    _e = _escape_mdv2
    ticker    = pos.get('ticker', '?')
    status    = pos.get('status', 'watching')
    direction = pos.get('direction', 'bullish')
    bullish   = direction != 'bearish'
    zone_low  = pos.get('zone_low')
    zone_high = pos.get('zone_high')
    entry     = pos.get('entry_price')
    stop      = pos.get('stop_loss')
    t1        = pos.get('target_1')
    t2        = pos.get('target_2')

    icon = '🔓' if status == 'active' else '📍'
    label = 'In position' if status == 'active' else 'On radar'

    parts = [f'{icon} *{_e(ticker)}* \\— {_e(label)}']

    if current_price is not None and zone_low is not None and zone_high is not None:
        if zone_low <= current_price <= zone_high:
            parts.append(f'✅ Price in zone \\({_e(_fmt_price(current_price))}\\)')
            if stop:
                parts.append(f'Tighten stop → {_e(_fmt_price(stop))}')
            if t2:
                parts.append(f'T2 at {_e(_fmt_price(t2))} still in play')
        elif (bullish and current_price < zone_low) or (not bullish and current_price > zone_high):
            dist_pct = abs(current_price - (zone_low if bullish else zone_high)) / current_price * 100
            parts.append(f'Approaching zone \\({_e(_fmt_price(current_price))}p, zone {_e(_fmt_price(zone_low))}\\–{_e(_fmt_price(zone_high))}\\) — {_e(f"{dist_pct:.1f}%")} away')
        else:
            parts.append(f'Price {_e(_fmt_price(current_price))} — zone {_e(_fmt_price(zone_low))}\\–{_e(_fmt_price(zone_high))}')
    elif entry:
        parts.append(f'Entry {_e(_fmt_price(entry))}')
        if stop:
            parts.append(f'Stop {_e(_fmt_price(stop))}')

    return ' \\| '.join(parts)


def _format_closed_position_line(pos: dict) -> str:
    """Render one recently-closed/expired position as a single MarkdownV2 line."""
    _e = _escape_mdv2
    ticker = pos.get('ticker', '?')
    status = pos.get('status', 'expired')
    entry  = pos.get('entry_price') or 0.0

    _STATUS_OUTCOME = {
        'hit_t1':      ('✅', 'T1 hit'),
        'hit_t2':      ('🎯', 'T2 hit'),
        'stopped_out': ('🛑', 'Stopped out'),
        'expired':     ('⏰', 'Expired — price never reached zone'),
        'closed':      ('📕', 'Closed'),
    }
    icon, outcome_label = _STATUS_OUTCOME.get(status, ('📕', status.replace('_', ' ').title()))

    pnl_str = ''
    if status in ('hit_t1', 'hit_t2') and entry:
        target = pos.get('target_1') if status == 'hit_t1' else pos.get('target_2')
        if target:
            pnl = (target - entry) / entry * 100
            if pos.get('direction') == 'bearish':
                pnl = -pnl
            sign = '\\+' if pnl >= 0 else ''
            pnl_str = f' \\({_e(sign)}{_e(f"{pnl:.1f}%")}\\)'

    return f'{icon} *{_e(ticker)}* \\— {_e(outcome_label)}{pnl_str}'


def format_monday_briefing(
    open_positions: list,
    new_setups: list,
    closed_last_week: list,
    tier: str,
    get_price_fn=None,
) -> str:
    """
    Render the Monday "Your Week Ahead" living portfolio briefing.

    Parameters
    ----------
    open_positions   List of tip_followups dicts (status watching/active).
    new_setups       List of (pattern_row, position) tuples for new tips.
    closed_last_week List of tip_followups dicts closed since last Monday.
    tier             User's tier string.
    get_price_fn     Optional callable(ticker) -> float | None for current prices.
    """
    from datetime import datetime, timezone as _tz
    _e = _escape_mdv2
    now      = datetime.now(_tz.utc)
    date_str = _e(now.strftime('%a %d %b'))
    lines    = [f'📅 *YOUR WEEK AHEAD \\— {date_str}*', '']

    # ── Open positions ────────────────────────────────────────────────────────
    if open_positions:
        lines += ['━━━━━━━━━━━━━━━', f'🔓 *OPEN POSITIONS \\({_e(str(len(open_positions)))}\\)*', '━━━━━━━━━━━━━━━']
        for pos in open_positions:
            price = None
            if get_price_fn:
                try:
                    price = get_price_fn(pos['ticker'])
                except Exception:
                    pass
            lines.append(_format_open_position_line(pos, price))
        lines.append('')

    # ── New setups this week ──────────────────────────────────────────────────
    if new_setups:
        lines += ['━━━━━━━━━━━━━━━', f'🆕 *NEW THIS WEEK \\({_e(str(len(new_setups)))}\\)*', '━━━━━━━━━━━━━━━']
        for i, (pattern_row, position) in enumerate(new_setups, start=1):
            lines += _format_single_setup(pattern_row, position, tier, i)
            lines.append('')

    # ── Closed last week ──────────────────────────────────────────────────────
    if closed_last_week:
        lines += ['━━━━━━━━━━━━━━━', f'📕 *CLOSED LAST WEEK \\({_e(str(len(closed_last_week)))}\\)*', '━━━━━━━━━━━━━━━']
        for pos in closed_last_week:
            lines.append(_format_closed_position_line(pos))
        lines.append('')

    if not open_positions and not new_setups and not closed_last_week:
        lines.append('_No new setups or open positions this week\\._')
    else:
        total_new = len(new_setups)
        footer = f'_{_e(str(total_new))} new setup{"s" if total_new != 1 else ""} this week'
        if open_positions:
            footer += f' \\+ {_e(str(len(open_positions)))} open'
        footer += ' — act when price reaches the zone\\._'
        lines.append(footer)

    return '\n'.join(lines)


def format_wednesday_update(
    open_positions: list,
    kb_changes: list,
    expired_this_cycle: list,
    tier: str,
    get_price_fn=None,
) -> str:
    """
    Render the Wednesday compound update — status of all open positions +
    notable KB changes since Monday. No new setups unless caller passes them.

    Parameters
    ----------
    open_positions       List of tip_followups dicts.
    kb_changes           List of dicts from get_kb_changes_since().
    expired_this_cycle   Positions just expired (from expire_stale_followups).
    tier                 User's tier.
    get_price_fn         Optional callable(ticker) -> float | None.
    """
    from datetime import datetime, timezone as _tz
    _e = _escape_mdv2
    now      = datetime.now(_tz.utc)
    date_str = _e(now.strftime('%a %d %b'))
    lines    = [f'📊 *MIDWEEK UPDATE \\— {date_str}*', '']

    # ── All open positions ────────────────────────────────────────────────────
    if open_positions:
        lines += ['━━━━━━━━━━━━━━━', f'🔓 *ALL OPEN POSITIONS \\({_e(str(len(open_positions)))}\\)*', '━━━━━━━━━━━━━━━']
        for pos in open_positions:
            price = None
            if get_price_fn:
                try:
                    price = get_price_fn(pos['ticker'])
                except Exception:
                    pass
            lines.append(_format_open_position_line(pos, price))
        lines.append('')

    # ── Notable KB changes since Monday ──────────────────────────────────────
    if kb_changes:
        lines += ['━━━━━━━━━━━━━━━', '📰 *KB CHANGES THIS WEEK*', '━━━━━━━━━━━━━━━']
        for change in kb_changes[:8]:
            pred_label = _PREDICATE_LABELS.get(change['predicate'], change['predicate'].replace('_', ' ').title())
            val = change['value'] or 'updated'
            lines.append(f'  {_e(change["ticker"])}: {_e(pred_label)} → {_e(str(val))}')
        lines.append('')

    # ── Expired positions ─────────────────────────────────────────────────────
    if expired_this_cycle:
        lines += ['━━━━━━━━━━━━━━━', '⏰ *EXPIRED THIS WEEK*', '━━━━━━━━━━━━━━━']
        for pos in expired_this_cycle:
            lines.append(_format_closed_position_line(pos))
        lines.append('')

    if not open_positions and not kb_changes:
        lines.append('_No open positions or notable changes this week\\._')
    else:
        lines.append('_Wednesday update — focus on open positions above\\._')

    return '\n'.join(lines)


def format_emergency_alert_with_confidence(
    alert_type: str,
    pos: dict,
    current_price: float,
    confidence_score: Optional[float] = None,
) -> str:
    """
    Enhanced emergency alert with normalised confidence score.
    confidence_score: 0.0–1.0 ratio (confirming / total relevant atoms).
    Only displayed if >= 0.5 and at least 2 atoms found (caller sets None otherwise).
    """
    base_msg = format_position_update(alert_type, pos, current_price)
    if confidence_score is None:
        return base_msg

    _e = _escape_mdv2
    pct = int(confidence_score * 100)
    bar_filled  = round(confidence_score * 10)
    bar_empty   = 10 - bar_filled
    bar         = '█' * bar_filled + '░' * bar_empty
    conf_line   = f'\n\n📊 KB confidence: {_e(bar)} {_e(str(pct))}%'
    if confidence_score >= 0.75:
        conf_line += ' — _High confidence_'
    elif confidence_score >= 0.5:
        conf_line += ' — _Moderate confidence_'
    return base_msg + conf_line


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

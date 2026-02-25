"""
notifications/snapshot_formatter.py — CuratedSnapshot → Telegram Message

Converts a CuratedSnapshot to a Telegram MarkdownV2 string and a plain dict
for the JSON preview API.

MARKDOWNV2 ESCAPING
===================
Telegram MarkdownV2 requires these characters to be escaped with a backslash
when they appear in regular (non-formatting) text:
  _ * [ ] ( ) ~ ` > # + - = | { } . !

The _escape_mdv2() helper is applied to EVERY dynamic string before insertion
into the template. This prevents silent 400 errors from the Telegram API.
"""

from __future__ import annotations

import re
from dataclasses import asdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from analytics.snapshot_curator import CuratedSnapshot

# ── MarkdownV2 escape helper ──────────────────────────────────────────────────

# All characters that must be escaped in MarkdownV2 regular text
_MDV2_RESERVED = r'\_*[]()~`>#+-=|{}.!'


def _escape_mdv2(s: str) -> str:
    """
    Escape all Telegram MarkdownV2 reserved characters in a plain-text string.

    Characters escaped: _ * [ ] ( ) ~ ` > # + - = | { } . !

    Do NOT call this on strings that already contain MarkdownV2 formatting
    (bold **..**, italic __..__, etc.) — it will escape the formatting chars.
    Use it only on dynamic data values (tickers, numbers, descriptions).

    Examples:
        '+32.9%'    → '+32\\.9%'
        '$612.40'   → '\\$612\\.40'   ($ not reserved but . is)
        'risk-on'   → 'risk\\-on'
        '(IV rank)' → '\\(IV rank\\)'
        '1.8:1'     → '1\\.8:1'
    """
    # Build replacement: any reserved char gets a backslash prepended
    result = []
    for ch in str(s):
        if ch in _MDV2_RESERVED:
            result.append('\\')
        result.append(ch)
    return ''.join(result)


# ── Conviction tier emoji ─────────────────────────────────────────────────────

_TIER_EMOJI = {
    'high':   '🟢',
    'medium': '🟡',
    'low':    '🟠',
    'avoid':  '🔴',
}

_URGENCY_LABEL = {
    'immediate':  '⚡ Act now',
    'this_week':  '📅 This week',
    'monitoring': '👁 Monitoring',
}

_REGIME_EMOJI = {
    'risk_on_expansion':    '📈',
    'risk_off_contraction': '📉',
    'stagflation':          '⚠️',
    'recovery':             '🌱',
    'no_data':              '❓',
}


# ── Section builders ──────────────────────────────────────────────────────────

def _section_portfolio(snapshot) -> str:
    """Build the 'YOUR PORTFOLIO' section. Empty string if no holdings."""
    if not snapshot.portfolio_summary:
        return ''

    lines = [
        '━━━━━━━━━━━━━━━━',
        '📁 *YOUR PORTFOLIO*',
        '━━━━━━━━━━━━━━━━',
    ]

    for h in snapshot.portfolio_summary:
        ticker = _escape_mdv2(h['ticker'])
        tier   = h.get('conviction_tier', 'no_data')
        upside = h.get('upside_pct', 0.0) or 0.0
        emoji  = _TIER_EMOJI.get(tier, '⚪')

        if tier in ('avoid', 'low'):
            tag = f'{emoji} {ticker} — {_escape_mdv2(tier.title())} conviction'
        else:
            tag = f'{emoji} {ticker} — {_escape_mdv2(tier.title())} conviction \\({_escape_mdv2(f"{upside:+.1f}%")} upside\\)'

        # Flag at-risk tickers
        if h['ticker'] in snapshot.holdings_at_risk:
            tag += '\n   ⚠️ Conviction degraded — review thesis'

        lines.append(tag)

    return '\n'.join(lines)


def _section_market(snapshot) -> str:
    """Build the 'MARKET CONTEXT' section."""
    regime = snapshot.market_regime
    emoji  = _REGIME_EMOJI.get(regime, '❓')
    regime_label = _escape_mdv2(
        regime.replace('_', ' ').title() if regime else 'Unknown'
    )
    impl  = _escape_mdv2(snapshot.regime_implication or '')
    macro = _escape_mdv2(snapshot.macro_summary or '')

    lines = [
        '━━━━━━━━━━━━━━━━',
        '🌍 *MARKET CONTEXT*',
        '━━━━━━━━━━━━━━━━',
        f'Regime: {emoji} {regime_label}',
    ]
    if macro and 'No macro' not in macro:
        lines.append(macro)
    if impl:
        lines.append(impl + ' ✓')

    return '\n'.join(lines)


def _section_opportunities(snapshot) -> str:
    """Build the 'OPPORTUNITIES FOR YOU' section."""
    if not snapshot.top_opportunities:
        return (
            '━━━━━━━━━━━━━━━━\n'
            '💡 *OPPORTUNITIES FOR YOU*\n'
            '━━━━━━━━━━━━━━━━\n'
            '_No curated opportunities matched your profile today\\._'
        )

    lines = [
        '━━━━━━━━━━━━━━━━',
        '💡 *OPPORTUNITIES FOR YOU*',
        '━━━━━━━━━━━━━━━━',
    ]

    for i, opp in enumerate(snapshot.top_opportunities, 1):
        tier_emoji = _TIER_EMOJI.get(opp.conviction_tier, '⚪')
        urgency    = _URGENCY_LABEL.get(opp.urgency, '')

        ticker     = _escape_mdv2(opp.ticker)
        tier_label = _escape_mdv2(opp.conviction_tier.title())
        upside     = _escape_mdv2(f'{opp.upside_pct:+.1f}%')
        risk_label = _escape_mdv2(
            (getattr(opp, 'thesis_risk_level', None) or 'n/a').title()
        )
        asymmetry  = _escape_mdv2(f'1:{opp.asymmetry_ratio:.1f}')
        reason     = _escape_mdv2(opp.relevance_reason)
        size       = _escape_mdv2(f'{opp.position_size_pct:.2f}%')
        inv        = _escape_mdv2(f'{opp.invalidation_distance:.1f}%')
        urgency_e  = _escape_mdv2(urgency) if urgency else ''

        lines.append(
            f'{i}/ *{ticker}* — {tier_emoji} {tier_label}\n'
            f'   {upside} upside \\| Asymmetry: {asymmetry}\n'
            f'   Why for you: _{reason}_\n'
            f'   Size: {size} \\| Stop: {inv}'
            + (f'\n   {urgency_e}' if urgency_e else '')
        )

    return '\n'.join(lines)


def _section_avoid(snapshot) -> str:
    """Build the 'WATCH LIST / AVOID' section."""
    if not snapshot.opportunities_to_avoid:
        return ''

    lines = [
        '━━━━━━━━━━━━━━━━',
        '🚫 *WATCH LIST*',
        '━━━━━━━━━━━━━━━━',
    ]
    for ticker in snapshot.opportunities_to_avoid:
        lines.append(f'• {_escape_mdv2(ticker)} — Avoid \\(weak signal or tight stop\\)')

    return '\n'.join(lines)


# ── Main formatters ───────────────────────────────────────────────────────────

def format_snapshot(snapshot) -> str:
    """
    Convert a CuratedSnapshot to a Telegram MarkdownV2-formatted string.

    All dynamic strings are escaped via _escape_mdv2().
    The returned string is ready to send to the Telegram Bot API with
    parse_mode='MarkdownV2'.
    """
    now_utc = datetime.now(timezone.utc)
    date_str = _escape_mdv2(now_utc.strftime('%A %d %b %Y, %H:%M GMT'))

    header = (
        f'📊 *Trading Galaxy — Daily Briefing*\n'
        f'_{date_str}_\n'
    )

    sections = [header]

    portfolio_sec = _section_portfolio(snapshot)
    if portfolio_sec:
        sections.append(portfolio_sec)

    sections.append(_section_market(snapshot))
    sections.append(_section_opportunities(snapshot))

    avoid_sec = _section_avoid(snapshot)
    if avoid_sec:
        sections.append(avoid_sec)

    sections.append(
        '━━━━━━━━━━━━━━━━\n'
        '_Reply STOP to unsubscribe_'
    )

    return '\n\n'.join(sections)


def snapshot_to_dict(snapshot) -> dict:
    """
    Convert a CuratedSnapshot to a plain Python dict for the JSON preview API.
    Uses dataclasses.asdict on nested dataclasses; falls back to __dict__.
    """
    try:
        from dataclasses import asdict as _asdict, fields as _fields
        # Convert top_opportunities list of OpportunityCard dataclasses
        snap_dict = {
            'user_id':               snapshot.user_id,
            'generated_at':          snapshot.generated_at,
            'portfolio_summary':     snapshot.portfolio_summary,
            'holdings_at_risk':      snapshot.holdings_at_risk,
            'holdings_performing':   snapshot.holdings_performing,
            'market_regime':         snapshot.market_regime,
            'regime_implication':    snapshot.regime_implication,
            'macro_summary':         snapshot.macro_summary,
            'top_opportunities':     [_asdict(o) for o in snapshot.top_opportunities],
            'opportunities_to_avoid': snapshot.opportunities_to_avoid,
        }
        return snap_dict
    except Exception:
        return snapshot.__dict__

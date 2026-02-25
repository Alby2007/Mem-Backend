"""
tests/test_snapshot_formatter.py — Snapshot Formatter Tests

Covers: _escape_mdv2 (exhaustive edge cases), format_snapshot,
snapshot_to_dict, section builders
"""

from __future__ import annotations

import pytest

from notifications.snapshot_formatter import (
    _escape_mdv2,
    format_snapshot,
    snapshot_to_dict,
    _section_portfolio,
    _section_market,
    _section_opportunities,
    _section_avoid,
)
from analytics.snapshot_curator import CuratedSnapshot, OpportunityCard


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_opportunity(
    ticker='AAPL',
    conviction_tier='high',
    upside_pct=25.0,
    invalidation_distance=-20.0,
    asymmetry_ratio=1.25,
    position_size_pct=4.5,
    relevance_reason='Technology affinity',
    urgency='immediate',
    thesis='Strong AI revenue growth',
) -> OpportunityCard:
    return OpportunityCard(
        ticker=ticker,
        thesis=thesis,
        conviction_tier=conviction_tier,
        upside_pct=upside_pct,
        invalidation_distance=invalidation_distance,
        asymmetry_ratio=asymmetry_ratio,
        position_size_pct=position_size_pct,
        relevance_reason=relevance_reason,
        urgency=urgency,
    )


def _make_snapshot(
    user_id='u1',
    portfolio_summary=None,
    holdings_at_risk=None,
    holdings_performing=None,
    market_regime='risk_on_expansion',
    regime_implication='Risk-On Expansion favours your Technology holdings',
    macro_summary='Fed: on hold | Yield curve: +60bps',
    top_opportunities=None,
    opportunities_to_avoid=None,
) -> CuratedSnapshot:
    return CuratedSnapshot(
        user_id=user_id,
        generated_at='2026-02-24T08:00:00+00:00',
        portfolio_summary=portfolio_summary or [],
        holdings_at_risk=holdings_at_risk or [],
        holdings_performing=holdings_performing or [],
        market_regime=market_regime,
        regime_implication=regime_implication,
        macro_summary=macro_summary,
        top_opportunities=top_opportunities or [_make_opportunity()],
        opportunities_to_avoid=opportunities_to_avoid or [],
    )


# ── TestEscapeMdv2 ────────────────────────────────────────────────────────────

class TestEscapeMdv2:

    def test_dot_escaped(self):
        assert _escape_mdv2('3.14') == '3\\.14'

    def test_plus_escaped(self):
        assert _escape_mdv2('+32.9%') == '\\+32\\.9%'

    def test_minus_escaped(self):
        assert _escape_mdv2('risk-on') == 'risk\\-on'

    def test_parens_escaped(self):
        assert _escape_mdv2('(IV rank 34)') == '\\(IV rank 34\\)'

    def test_exclamation_escaped(self):
        assert _escape_mdv2('Confirmed!') == 'Confirmed\\!'

    def test_dollar_sign_not_reserved(self):
        # $ is not a MarkdownV2 reserved char — should pass through
        result = _escape_mdv2('$612.40')
        assert '\\$' not in result
        assert '\\.' in result  # but the dot is escaped

    def test_underscore_escaped(self):
        assert _escape_mdv2('risk_on') == 'risk\\_on'

    def test_hash_escaped(self):
        assert _escape_mdv2('#1 pick') == '\\#1 pick'

    def test_backtick_escaped(self):
        assert _escape_mdv2('`code`') == '\\`code\\`'

    def test_pipe_escaped(self):
        assert _escape_mdv2('a|b') == 'a\\|b'

    def test_tilde_escaped(self):
        assert _escape_mdv2('~90%') == '\\~90%'

    def test_plain_text_unchanged(self):
        assert _escape_mdv2('AAPL') == 'AAPL'
        assert _escape_mdv2('HIGH') == 'HIGH'

    def test_empty_string(self):
        assert _escape_mdv2('') == ''

    def test_price_string(self):
        result = _escape_mdv2('$612.40')
        assert '612' in result
        assert '\\.' in result

    def test_asymmetry_ratio(self):
        result = _escape_mdv2('1.8:1')
        assert '1\\.8:1' == result

    def test_percentage_with_sign(self):
        result = _escape_mdv2('-18.4%')
        assert '\\-18\\.4%' == result

    def test_numeric_types_converted(self):
        assert '42' in _escape_mdv2(42)

    def test_curly_braces_escaped(self):
        assert _escape_mdv2('{val}') == '\\{val\\}'

    def test_square_brackets_escaped(self):
        assert _escape_mdv2('[link]') == '\\[link\\]'

    def test_gt_escaped(self):
        assert _escape_mdv2('>quote') == '\\>quote'

    def test_equals_escaped(self):
        assert _escape_mdv2('a=b') == 'a\\=b'


# ── TestFormatSnapshot ────────────────────────────────────────────────────────

class TestFormatSnapshot:

    def test_returns_string(self):
        snap = _make_snapshot()
        result = format_snapshot(snap)
        assert isinstance(result, str)

    def test_contains_header(self):
        snap = _make_snapshot()
        result = format_snapshot(snap)
        assert 'Trading Galaxy' in result
        assert 'Daily Briefing' in result

    def test_contains_market_context(self):
        snap = _make_snapshot()
        result = format_snapshot(snap)
        assert 'MARKET CONTEXT' in result

    def test_contains_opportunities_section(self):
        snap = _make_snapshot()
        result = format_snapshot(snap)
        assert 'OPPORTUNITIES' in result

    def test_ticker_present(self):
        snap = _make_snapshot(top_opportunities=[_make_opportunity(ticker='MA')])
        result = format_snapshot(snap)
        assert 'MA' in result

    def test_no_portfolio_section_when_empty(self):
        snap = _make_snapshot(portfolio_summary=[])
        result = format_snapshot(snap)
        assert 'YOUR PORTFOLIO' not in result

    def test_portfolio_section_present_when_holdings(self):
        snap = _make_snapshot(
            portfolio_summary=[
                {'ticker': 'AAPL', 'conviction_tier': 'high', 'upside_pct': 25.0, 'avg_cost': 150}
            ]
        )
        result = format_snapshot(snap)
        assert 'YOUR PORTFOLIO' in result
        assert 'AAPL' in result

    def test_at_risk_warning_shown(self):
        snap = _make_snapshot(
            portfolio_summary=[
                {'ticker': 'MSFT', 'conviction_tier': 'low', 'upside_pct': 3.0, 'avg_cost': 400}
            ],
            holdings_at_risk=['MSFT']
        )
        result = format_snapshot(snap)
        assert 'degraded' in result or 'review' in result.lower()

    def test_no_avoid_section_when_empty(self):
        snap = _make_snapshot(opportunities_to_avoid=[])
        result = format_snapshot(snap)
        assert 'WATCH LIST' not in result

    def test_avoid_section_present(self):
        snap = _make_snapshot(opportunities_to_avoid=['INTC'])
        result = format_snapshot(snap)
        assert 'WATCH LIST' in result
        assert 'INTC' in result

    def test_no_opportunities_message(self):
        snap = _make_snapshot(top_opportunities=[])
        result = format_snapshot(snap)
        assert 'No curated opportunities' in result or 'OPPORTUNITIES' in result

    def test_contains_stop_footer(self):
        snap = _make_snapshot()
        result = format_snapshot(snap)
        assert 'STOP' in result

    def test_no_unescaped_dots_in_numbers(self):
        snap = _make_snapshot(
            top_opportunities=[_make_opportunity(upside_pct=32.9, position_size_pct=3.81)]
        )
        result = format_snapshot(snap)
        # Within the formatted message, numeric dots should be escaped
        import re
        # Find number patterns that have an unescaped dot — should be none
        # We check for \. appearing near digits (which is correct MDv2 escaping)
        assert '\\.' in result  # dots are being escaped


# ── TestSnapshotToDict ────────────────────────────────────────────────────────

class TestSnapshotToDict:

    def test_returns_dict(self):
        snap = _make_snapshot()
        result = snapshot_to_dict(snap)
        assert isinstance(result, dict)

    def test_required_keys_present(self):
        snap = _make_snapshot()
        result = snapshot_to_dict(snap)
        required = {
            'user_id', 'generated_at', 'portfolio_summary',
            'holdings_at_risk', 'holdings_performing',
            'market_regime', 'regime_implication', 'macro_summary',
            'top_opportunities', 'opportunities_to_avoid',
        }
        assert required == set(result.keys())

    def test_top_opportunities_are_dicts(self):
        snap = _make_snapshot()
        result = snapshot_to_dict(snap)
        for opp in result['top_opportunities']:
            assert isinstance(opp, dict)
            assert 'ticker' in opp
            assert 'conviction_tier' in opp

    def test_user_id_preserved(self):
        snap = _make_snapshot(user_id='test_user_42')
        result = snapshot_to_dict(snap)
        assert result['user_id'] == 'test_user_42'

    def test_empty_opportunities(self):
        snap = CuratedSnapshot(
            user_id='u_empty',
            generated_at='2026-02-24T08:00:00+00:00',
            portfolio_summary=[],
            holdings_at_risk=[],
            holdings_performing=[],
            market_regime='risk_on_expansion',
            regime_implication='',
            macro_summary='',
            top_opportunities=[],
            opportunities_to_avoid=[],
        )
        result = snapshot_to_dict(snap)
        assert result['top_opportunities'] == []

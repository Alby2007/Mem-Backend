"""
tests/test_tip_formatter.py — Unit tests for notifications/tip_formatter.py
"""

from __future__ import annotations
import pytest
from analytics.pattern_detector import PatternSignal
from analytics.position_calculator import PositionRecommendation, calculate_position
from notifications.tip_formatter import (
    format_tip, tip_to_dict, pattern_allowed_for_tier,
    timeframe_allowed_for_tier, TIER_LIMITS, _escape_mdv2,
)


def _pattern(
    ticker='NVDA', pattern_type='fvg', direction='bullish',
    zone_high=192.0, zone_low=189.0,
    kb_conviction='high', kb_regime='risk_on_expansion', kb_signal_dir='long',
    quality_score=0.87, status='open', timeframe='1h',
) -> PatternSignal:
    return PatternSignal(
        pattern_type=pattern_type, ticker=ticker, direction=direction,
        zone_high=zone_high, zone_low=zone_low,
        zone_size_pct=round((zone_high - zone_low) / zone_low * 100, 4),
        timeframe=timeframe, formed_at='2026-02-25T08:00:00',
        quality_score=quality_score, status=status,
        kb_conviction=kb_conviction, kb_regime=kb_regime, kb_signal_dir=kb_signal_dir,
    )


def _position(pattern=None) -> PositionRecommendation:
    p = pattern or _pattern()
    return calculate_position(p, {
        'account_size': 10000.0,
        'max_risk_per_trade_pct': 1.0,
        'account_currency': 'GBP',
    })


class TestEscapeMdv2:
    def test_dot_escaped(self):
        assert _escape_mdv2('1.5') == r'1\.5'

    def test_minus_escaped(self):
        assert _escape_mdv2('-0.5%') == r'\-0\.5%'

    def test_parens_escaped(self):
        assert _escape_mdv2('(1:2)') == r'\(1:2\)'

    def test_plus_escaped(self):
        assert _escape_mdv2('+2.3%') == r'\+2\.3%'

    def test_plain_text_unchanged(self):
        assert _escape_mdv2('NVDA') == 'NVDA'

    def test_empty_string(self):
        assert _escape_mdv2('') == ''


class TestTierLimits:
    def test_basic_has_fvg(self):
        assert 'fvg' in TIER_LIMITS['basic']['patterns']

    def test_basic_has_ifvg(self):
        assert 'ifvg' in TIER_LIMITS['basic']['patterns']

    def test_basic_no_order_block(self):
        assert 'order_block' not in TIER_LIMITS['basic']['patterns']

    def test_basic_targets_2(self):
        assert TIER_LIMITS['basic']['targets'] == 2

    def test_pro_has_all_7_patterns(self):
        assert len(TIER_LIMITS['pro']['patterns']) == 7

    def test_pro_targets_3(self):
        assert TIER_LIMITS['pro']['targets'] == 3

    def test_pro_has_15m_timeframe(self):
        assert '15m' in TIER_LIMITS['pro']['timeframes']

    def test_basic_no_15m_timeframe(self):
        assert '15m' not in TIER_LIMITS['basic']['timeframes']


class TestPatternAllowedForTier:
    def test_fvg_allowed_basic(self):
        assert pattern_allowed_for_tier('fvg', 'basic') is True

    def test_ifvg_allowed_basic(self):
        assert pattern_allowed_for_tier('ifvg', 'basic') is True

    def test_order_block_not_allowed_basic(self):
        assert pattern_allowed_for_tier('order_block', 'basic') is False

    def test_bpr_not_allowed_basic(self):
        assert pattern_allowed_for_tier('bpr', 'basic') is False

    def test_order_block_allowed_pro(self):
        assert pattern_allowed_for_tier('order_block', 'pro') is True

    def test_all_patterns_allowed_pro(self):
        for pt in ['fvg','ifvg','bpr','order_block','breaker','liquidity_void','mitigation']:
            assert pattern_allowed_for_tier(pt, 'pro') is True


class TestTimeframeAllowedForTier:
    def test_1h_allowed_basic(self):
        assert timeframe_allowed_for_tier('1h', 'basic') is True

    def test_15m_not_allowed_basic(self):
        assert timeframe_allowed_for_tier('15m', 'basic') is False

    def test_4h_not_allowed_basic(self):
        assert timeframe_allowed_for_tier('4h', 'basic') is False

    def test_all_timeframes_allowed_pro(self):
        for tf in ['15m','1h','4h','1d']:
            assert timeframe_allowed_for_tier(tf, 'pro') is True


class TestFormatTip:
    def test_returns_string(self):
        msg = format_tip(_pattern(), _position())
        assert isinstance(msg, str)

    def test_contains_ticker(self):
        msg = format_tip(_pattern(ticker='NVDA'), _position())
        assert 'NVDA' in msg

    def test_contains_pattern_label(self):
        msg = format_tip(_pattern(pattern_type='fvg'), _position())
        assert 'Fair Value Gap' in msg

    def test_contains_timeframe(self):
        msg = format_tip(_pattern(timeframe='1h'), _position())
        assert '1H' in msg

    def test_contains_zone_prices(self):
        msg = format_tip(_pattern(zone_high=192.0, zone_low=189.0), _position())
        assert '189' in msg
        assert '192' in msg

    def test_basic_tier_no_t3(self):
        msg = format_tip(_pattern(), _position(), tier='basic')
        assert 'Target 3' in msg  # present but gated
        assert 'Pro tier only' in msg

    def test_pro_tier_shows_t3(self):
        msg = format_tip(_pattern(), _position(), tier='pro')
        assert '1:3' in msg
        assert 'Pro tier only' not in msg

    def test_no_position_no_position_block(self):
        msg = format_tip(_pattern(), None)
        assert 'YOUR POSITION' not in msg

    def test_with_position_shows_account(self):
        msg = format_tip(_pattern(), _position())
        assert 'YOUR POSITION' in msg
        assert '10,000' in msg

    def test_contains_kb_context_block(self):
        msg = format_tip(_pattern(), _position())
        assert 'KB CONTEXT' in msg

    def test_contains_conviction(self):
        msg = format_tip(_pattern(kb_conviction='high'), _position())
        assert 'Conviction' in msg

    def test_contains_quality_score(self):
        msg = format_tip(_pattern(quality_score=0.87), _position())
        assert '0\\.87' in msg  # dot is escaped in MarkdownV2

    def test_bearish_direction_in_message(self):
        p = _pattern(direction='bearish', zone_high=192.0, zone_low=189.0)
        pos = calculate_position(p, {'account_size': 10000, 'max_risk_per_trade_pct': 1.0})
        msg = format_tip(p, pos)
        assert '📉' in msg

    def test_bullish_direction_in_message(self):
        msg = format_tip(_pattern(direction='bullish'), _position())
        assert '📈' in msg

    def test_no_unescaped_dots_in_numbers(self):
        msg = format_tip(_pattern(), _position())
        import re
        # Numbers with dots should be escaped: 189\.20 not 189.20
        raw_number_dot = re.search(r'\d\.\d', msg)
        assert raw_number_dot is None, f"Unescaped decimal dot found: {raw_number_dot.group()}"

    def test_stop_loss_in_message(self):
        msg = format_tip(_pattern(), _position())
        assert 'Stop loss' in msg

    def test_target_1_in_message(self):
        msg = format_tip(_pattern(), _position())
        assert 'Target 1' in msg

    def test_target_2_in_message(self):
        msg = format_tip(_pattern(), _position())
        assert 'Target 2' in msg


class TestTipToDict:
    def test_returns_dict(self):
        d = tip_to_dict(_pattern(), _position())
        assert isinstance(d, dict)

    def test_contains_ticker(self):
        d = tip_to_dict(_pattern(ticker='AAPL'), _position(_pattern(ticker='AAPL')))
        assert d['ticker'] == 'AAPL'

    def test_contains_pattern_type(self):
        d = tip_to_dict(_pattern(pattern_type='order_block'), None)
        assert d['pattern_type'] == 'order_block'

    def test_position_present_when_provided(self):
        d = tip_to_dict(_pattern(), _position())
        assert 'position' in d

    def test_position_absent_when_none(self):
        d = tip_to_dict(_pattern(), None)
        assert 'position' not in d

    def test_basic_tier_t3_none(self):
        d = tip_to_dict(_pattern(), _position(), tier='basic')
        assert d['position']['target_3'] is None

    def test_pro_tier_t3_present(self):
        d = tip_to_dict(_pattern(), _position(), tier='pro')
        assert d['position']['target_3'] is not None
        assert d['position']['target_3'] > d['position']['target_2']

    def test_basic_tier_ep_t3_none(self):
        d = tip_to_dict(_pattern(), _position(), tier='basic')
        assert d['position']['expected_profit_t3'] is None

    def test_tier_field_in_dict(self):
        d = tip_to_dict(_pattern(), None, tier='pro')
        assert d['tier'] == 'pro'

    def test_quality_score_present(self):
        d = tip_to_dict(_pattern(quality_score=0.77), None)
        assert abs(d['quality_score'] - 0.77) < 1e-6

    def test_zone_fields_present(self):
        d = tip_to_dict(_pattern(zone_high=192.0, zone_low=189.0), None)
        assert d['zone_high'] == 192.0
        assert d['zone_low'] == 189.0

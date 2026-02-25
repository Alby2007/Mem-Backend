"""
tests/test_position_calculator.py — Unit tests for analytics/position_calculator.py
"""

from __future__ import annotations
import pytest
from analytics.pattern_detector import PatternSignal
from analytics.position_calculator import PositionRecommendation, calculate_position


def _pattern(
    direction='bullish',
    zone_high=102.0,
    zone_low=100.0,
    quality_score=0.75,
    kb_conviction='high',
    kb_regime='risk_on_expansion',
    kb_signal_dir='long',
    ticker='NVDA',
    pattern_type='fvg',
) -> PatternSignal:
    return PatternSignal(
        pattern_type=pattern_type,
        ticker=ticker,
        direction=direction,
        zone_high=zone_high,
        zone_low=zone_low,
        zone_size_pct=round((zone_high - zone_low) / zone_low * 100, 4),
        timeframe='1h',
        formed_at='2026-01-01T10:00:00',
        quality_score=quality_score,
        status='open',
        kb_conviction=kb_conviction,
        kb_regime=kb_regime,
        kb_signal_dir=kb_signal_dir,
    )


def _prefs(account_size=10000.0, risk_pct=1.0, currency='GBP') -> dict:
    return {
        'account_size': account_size,
        'max_risk_per_trade_pct': risk_pct,
        'account_currency': currency,
    }


class TestCalculatePositionBasic:
    def test_returns_recommendation(self):
        rec = calculate_position(_pattern(), _prefs())
        assert isinstance(rec, PositionRecommendation)

    def test_returns_none_zero_account(self):
        rec = calculate_position(_pattern(), _prefs(account_size=0))
        assert rec is None

    def test_returns_none_invalid_zone(self):
        p = _pattern(zone_high=100.0, zone_low=100.0)
        assert calculate_position(p, _prefs()) is None

    def test_returns_none_inverted_zone(self):
        p = _pattern(zone_high=99.0, zone_low=100.0)
        assert calculate_position(p, _prefs()) is None

    def test_ticker_passed_through(self):
        rec = calculate_position(_pattern(ticker='AAPL'), _prefs())
        assert rec.ticker == 'AAPL'

    def test_pattern_type_passed_through(self):
        rec = calculate_position(_pattern(pattern_type='order_block'), _prefs())
        assert rec.pattern_type == 'order_block'

    def test_direction_passed_through(self):
        rec = calculate_position(_pattern(direction='bearish', zone_high=102.0, zone_low=100.0), _prefs())
        assert rec.direction == 'bearish'


class TestEntryAndStop:
    def test_suggested_entry_is_midpoint(self):
        rec = calculate_position(_pattern(zone_high=102.0, zone_low=100.0), _prefs())
        assert abs(rec.suggested_entry - 101.0) < 0.01

    def test_bullish_stop_below_zone_low(self):
        rec = calculate_position(_pattern(direction='bullish', zone_high=102.0, zone_low=100.0), _prefs())
        assert rec.stop_loss < 100.0

    def test_bearish_stop_above_zone_high(self):
        rec = calculate_position(_pattern(direction='bearish', zone_high=102.0, zone_low=100.0), _prefs())
        assert rec.stop_loss > 102.0

    def test_stop_distance_pct_positive(self):
        rec = calculate_position(_pattern(), _prefs())
        assert rec.stop_distance_pct > 0

    def test_stop_distance_currency_positive(self):
        rec = calculate_position(_pattern(), _prefs())
        assert rec.stop_distance_currency > 0

    def test_zone_high_low_preserved(self):
        rec = calculate_position(_pattern(zone_high=192.5, zone_low=189.2), _prefs())
        assert abs(rec.entry_zone_high - 192.5) < 1e-4
        assert abs(rec.entry_zone_low - 189.2) < 1e-4


class TestPositionSizing:
    def test_risk_amount_equals_account_times_risk_pct(self):
        rec = calculate_position(_pattern(), _prefs(account_size=10000, risk_pct=1.0))
        assert abs(rec.risk_amount - 100.0) < 0.01

    def test_risk_amount_2pct(self):
        rec = calculate_position(_pattern(), _prefs(account_size=10000, risk_pct=2.0))
        assert abs(rec.risk_amount - 200.0) < 0.01

    def test_units_times_stop_distance_equals_risk_amount(self):
        rec = calculate_position(_pattern(), _prefs(account_size=10000, risk_pct=1.0))
        implied_loss = rec.position_size_units * rec.stop_distance_currency
        assert abs(implied_loss - rec.risk_amount) < 0.01

    def test_position_value_equals_units_times_entry(self):
        rec = calculate_position(_pattern(), _prefs())
        expected = rec.position_size_units * rec.suggested_entry
        assert abs(rec.position_value - expected) < 0.10

    def test_position_pct_of_account_positive(self):
        rec = calculate_position(_pattern(), _prefs())
        assert rec.position_pct_of_account > 0

    def test_zero_account_returns_none(self):
        assert calculate_position(_pattern(), {'account_size': 0}) is None

    def test_account_currency_passed_through(self):
        rec = calculate_position(_pattern(), _prefs(currency='USD'))
        assert rec.account_currency == 'USD'


class TestTargets:
    def test_bullish_t1_above_entry(self):
        rec = calculate_position(_pattern(direction='bullish'), _prefs())
        assert rec.target_1 > rec.suggested_entry

    def test_bullish_t2_above_t1(self):
        rec = calculate_position(_pattern(direction='bullish'), _prefs())
        assert rec.target_2 > rec.target_1

    def test_bullish_t3_above_t2(self):
        rec = calculate_position(_pattern(direction='bullish'), _prefs())
        assert rec.target_3 > rec.target_2

    def test_bearish_t1_below_entry(self):
        rec = calculate_position(_pattern(direction='bearish'), _prefs())
        assert rec.target_1 < rec.suggested_entry

    def test_bearish_t2_below_t1(self):
        rec = calculate_position(_pattern(direction='bearish'), _prefs())
        assert rec.target_2 < rec.target_1

    def test_bearish_t3_below_t2(self):
        rec = calculate_position(_pattern(direction='bearish'), _prefs())
        assert rec.target_3 < rec.target_2

    def test_t1_equals_1r(self):
        rec = calculate_position(_pattern(direction='bullish'), _prefs())
        r = abs(rec.suggested_entry - rec.stop_loss)
        assert abs(rec.target_1 - (rec.suggested_entry + r)) < 0.01

    def test_t2_equals_2r(self):
        rec = calculate_position(_pattern(direction='bullish'), _prefs())
        r = abs(rec.suggested_entry - rec.stop_loss)
        assert abs(rec.target_2 - (rec.suggested_entry + 2 * r)) < 0.01

    def test_t3_equals_3r(self):
        rec = calculate_position(_pattern(direction='bullish'), _prefs())
        r = abs(rec.suggested_entry - rec.stop_loss)
        assert abs(rec.target_3 - (rec.suggested_entry + 3 * r)) < 0.01

    def test_expected_profit_t2_equals_2x_t1(self):
        rec = calculate_position(_pattern(), _prefs())
        assert abs(rec.expected_profit_t2 - 2 * rec.expected_profit_t1) < 0.01

    def test_expected_profit_t3_equals_3x_t1(self):
        rec = calculate_position(_pattern(), _prefs())
        assert abs(rec.expected_profit_t3 - 3 * rec.expected_profit_t1) < 0.01


class TestKBAlignment:
    def test_high_conviction_aligned(self):
        rec = calculate_position(_pattern(kb_conviction='high'), _prefs())
        assert rec.kb_conviction_alignment is True

    def test_confirmed_conviction_aligned(self):
        rec = calculate_position(_pattern(kb_conviction='confirmed'), _prefs())
        assert rec.kb_conviction_alignment is True

    def test_low_conviction_not_aligned(self):
        rec = calculate_position(_pattern(kb_conviction='low'), _prefs())
        assert rec.kb_conviction_alignment is False

    def test_empty_conviction_not_aligned(self):
        rec = calculate_position(_pattern(kb_conviction=''), _prefs())
        assert rec.kb_conviction_alignment is False

    def test_risk_on_regime_aligned(self):
        rec = calculate_position(_pattern(kb_regime='risk_on_expansion'), _prefs())
        assert rec.kb_regime_alignment is True

    def test_risk_off_regime_not_aligned(self):
        rec = calculate_position(_pattern(kb_regime='risk_off_contraction'), _prefs())
        assert rec.kb_regime_alignment is False

    def test_empty_regime_not_aligned(self):
        rec = calculate_position(_pattern(kb_regime=''), _prefs())
        assert rec.kb_regime_alignment is False

    def test_composite_score_matches_pattern(self):
        rec = calculate_position(_pattern(quality_score=0.82), _prefs())
        assert abs(rec.composite_score - 0.82) < 1e-6


class TestEdgeCases:
    def test_missing_account_size_returns_none(self):
        assert calculate_position(_pattern(), {}) is None

    def test_none_account_size_returns_none(self):
        assert calculate_position(_pattern(), {'account_size': None}) is None

    def test_large_account(self):
        rec = calculate_position(_pattern(), _prefs(account_size=1_000_000, risk_pct=0.5))
        assert rec.risk_amount == 5000.0
        assert rec.position_size_units > 0

    def test_fractional_units_possible(self):
        # Forex-like: small pip movement → large units
        rec = calculate_position(
            _pattern(zone_high=1.1002, zone_low=1.1000),
            _prefs(account_size=10000, risk_pct=1.0)
        )
        assert rec.position_size_units > 0

"""
tests/test_pattern_detector.py — Unit tests for analytics/pattern_detector.py
"""

from __future__ import annotations
import pytest
from analytics.pattern_detector import (
    OHLCV, PatternSignal, detect_all_patterns,
    _avg_body, _atr, _detect_fvg, _detect_ifvg, _detect_bpr,
    _detect_order_blocks, _update_ob_status, _detect_liquidity_voids,
    _detect_mitigation_blocks, _update_fvg_status,
)


def _c(o, h, l, c, ts='2026-01-01T00:00:00') -> OHLCV:
    return OHLCV(timestamp=ts, open=o, high=h, low=l, close=c)

def _ts(i: int) -> str:
    return f'2026-01-01T{i:02d}:00:00'

def make_candles(*tuples) -> list:
    return [OHLCV(timestamp=_ts(i), open=t[0], high=t[1], low=t[2], close=t[3])
            for i, t in enumerate(tuples)]


class TestOHLCV:
    def test_body_size_bullish(self):
        assert abs(_c(100, 110, 98, 108).body_size - 8.0) < 1e-9

    def test_body_size_bearish(self):
        assert abs(_c(110, 115, 100, 105).body_size - 5.0) < 1e-9

    def test_total_range(self):
        assert abs(_c(100, 115, 95, 110).total_range - 20.0) < 1e-9

    def test_is_bullish(self):
        assert _c(100, 110, 98, 108).is_bullish is True
        assert _c(108, 110, 98, 100).is_bullish is False

    def test_is_bearish(self):
        assert _c(108, 110, 98, 100).is_bearish is True

    def test_body_ratio(self):
        c = _c(100, 120, 100, 118)  # body=18, range=20
        assert abs(c.body_ratio - 0.9) < 1e-6

    def test_doji_body_ratio(self):
        assert _c(100, 105, 95, 100).body_ratio == 0.0


class TestHelpers:
    def test_avg_body(self):
        candles = [_c(100, 110, 98, 105)] * 5
        assert abs(_avg_body(candles) - 5.0) < 1e-9

    def test_avg_body_empty_returns_epsilon(self):
        assert _avg_body([]) > 0

    def test_atr_basic(self):
        candles = make_candles((100,105,98,103),(103,108,101,106),(106,111,104,109))
        assert _atr(candles) > 0


class TestFVGDetection:
    def bullish_candles(self):
        # candle[0].high=100 < candle[2].low=102 → bullish FVG
        return make_candles((95,100,94,99),(99,105,98,103),(103,107,102,106))

    def bearish_candles(self):
        # candle[0].low=106 > candle[2].high=104 → bearish FVG
        return make_candles((110,115,106,108),(108,110,105,107),(107,104,101,102))

    def test_bullish_fvg_detected(self):
        sigs = _detect_fvg(self.bullish_candles(), 'NVDA', '1h', 2.0, 'high','risk_on','long')
        assert any(s.direction == 'bullish' for s in sigs)

    def test_bullish_fvg_zone_correct(self):
        sigs = _detect_fvg(self.bullish_candles(), 'NVDA', '1h', 2.0, '','','')
        b = [s for s in sigs if s.direction == 'bullish'][0]
        assert b.zone_low == 100.0 and b.zone_high == 102.0

    def test_bearish_fvg_detected(self):
        sigs = _detect_fvg(self.bearish_candles(), 'NVDA', '1h', 2.0, '','','')
        assert any(s.direction == 'bearish' for s in sigs)

    def test_bearish_fvg_zone_correct(self):
        sigs = _detect_fvg(self.bearish_candles(), 'NVDA', '1h', 2.0, '','','')
        b = [s for s in sigs if s.direction == 'bearish'][0]
        assert b.zone_high == 106.0 and b.zone_low == 104.0

    def test_no_fvg_when_overlapping(self):
        candles = make_candles((100,105,99,103),(103,108,101,106),(106,110,104,108))
        assert _detect_fvg(candles, 'NVDA', '1h', 2.0, '','','') == []

    def test_fvg_pattern_type(self):
        sigs = _detect_fvg(self.bullish_candles(), 'NVDA', '1h', 2.0, '','','')
        assert all(s.pattern_type == 'fvg' for s in sigs)

    def test_fvg_quality_range(self):
        sigs = _detect_fvg(self.bullish_candles(), 'NVDA', '1h', 2.0, 'high','risk_on','long')
        assert all(0.0 <= s.quality_score <= 1.0 for s in sigs)

    def test_two_candles_returns_empty(self):
        assert _detect_fvg(make_candles((100,105,99,103),(103,108,101,106)),
                           'NVDA', '1h', 2.0, '','','') == []


class TestFVGStatusUpdate:
    def test_bullish_fvg_stays_open(self):
        candles = make_candles((95,100,94,99),(99,105,98,103),(103,107,102,106),(106,110,103,108))
        raw = _detect_fvg(candles,'NVDA','1h',2.0,'','','')
        updated = _update_fvg_status(raw, candles)
        b = [s for s in updated if s.direction=='bullish'][0]
        assert b.status == 'open'

    def test_bullish_fvg_partially_filled(self):
        candles = make_candles((95,100,94,99),(99,105,98,103),(103,107,102,106),(106,108,101,105))
        raw = _detect_fvg(candles,'NVDA','1h',2.0,'','','')
        updated = _update_fvg_status(raw, candles)
        b = [s for s in updated if s.direction=='bullish'][0]
        assert b.status == 'partially_filled'

    def test_bullish_fvg_filled(self):
        candles = make_candles((95,100,94,99),(99,105,98,103),(103,107,102,106),(106,108,98,104))
        raw = _detect_fvg(candles,'NVDA','1h',2.0,'','','')
        updated = _update_fvg_status(raw, candles)
        b = [s for s in updated if s.direction=='bullish'][0]
        assert b.status == 'filled'


class TestIFVG:
    def _partial_fvg(self) -> PatternSignal:
        return PatternSignal(pattern_type='fvg',ticker='NVDA',direction='bullish',
            zone_high=102.0,zone_low=100.0,zone_size_pct=2.0,timeframe='1h',
            formed_at='2026-01-01T02:00:00',quality_score=0.7,status='partially_filled',_candle_idx=1)

    def test_ifvg_created(self):
        assert len(_detect_ifvg([self._partial_fvg()])) == 1

    def test_ifvg_pattern_type(self):
        assert _detect_ifvg([self._partial_fvg()])[0].pattern_type == 'ifvg'

    def test_ifvg_inherits_zone(self):
        ifvg = _detect_ifvg([self._partial_fvg()])[0]
        assert ifvg.zone_high == 102.0 and ifvg.zone_low == 100.0

    def test_ifvg_quality_boosted(self):
        ifvg = _detect_ifvg([self._partial_fvg()])[0]
        assert ifvg.quality_score >= 0.7

    def test_open_fvg_not_converted(self):
        fvg = PatternSignal(pattern_type='fvg',ticker='NVDA',direction='bullish',
            zone_high=102.0,zone_low=100.0,zone_size_pct=2.0,timeframe='1h',
            formed_at='2026-01-01T02:00:00',quality_score=0.7,status='open',_candle_idx=1)
        assert len(_detect_ifvg([fvg])) == 0


class TestBPR:
    def _bull(self):
        return PatternSignal(pattern_type='fvg',ticker='NVDA',direction='bullish',
            zone_high=104.0,zone_low=100.0,zone_size_pct=4.0,timeframe='1h',
            formed_at='2026-01-01T02:00:00',quality_score=0.6,status='open',_candle_idx=2)

    def _bear(self):
        return PatternSignal(pattern_type='fvg',ticker='NVDA',direction='bearish',
            zone_high=103.0,zone_low=101.0,zone_size_pct=2.0,timeframe='1h',
            formed_at='2026-01-01T03:00:00',quality_score=0.6,status='open',_candle_idx=3)

    def test_bpr_detected(self):
        assert len(_detect_bpr([self._bull(), self._bear()], 'NVDA', '1h')) == 1

    def test_bpr_zone_is_overlap(self):
        bpr = _detect_bpr([self._bull(), self._bear()], 'NVDA', '1h')[0]
        assert bpr.zone_low == 101.0 and bpr.zone_high == 103.0

    def test_bpr_pattern_type(self):
        bpr = _detect_bpr([self._bull(), self._bear()], 'NVDA', '1h')[0]
        assert bpr.pattern_type == 'bpr'

    def test_no_bpr_without_overlap(self):
        bull = PatternSignal(pattern_type='fvg',ticker='NVDA',direction='bullish',
            zone_high=100.0,zone_low=98.0,zone_size_pct=2.0,timeframe='1h',
            formed_at='2026-01-01T02:00:00',quality_score=0.6,status='open',_candle_idx=2)
        bear = PatternSignal(pattern_type='fvg',ticker='NVDA',direction='bearish',
            zone_high=105.0,zone_low=103.0,zone_size_pct=2.0,timeframe='1h',
            formed_at='2026-01-01T03:00:00',quality_score=0.6,status='open',_candle_idx=3)
        assert len(_detect_bpr([bull, bear], 'NVDA', '1h')) == 0


class TestOrderBlock:
    def bullish_ob_candles(self):
        # [1] bearish; [2] strong bullish impulse
        return make_candles((100,103,99,102),(102,104,100,101),(101,116,100,116))

    def bearish_ob_candles(self):
        return make_candles((100,103,99,101),(101,110,100,109),(109,110,94,95))

    def test_bullish_ob_detected(self):
        c = self.bullish_ob_candles()
        obs = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert any(s.direction=='bullish' for s in obs)

    def test_bearish_ob_detected(self):
        c = self.bearish_ob_candles()
        obs = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert any(s.direction=='bearish' for s in obs)

    def test_ob_zone_matches_candle(self):
        c = self.bullish_ob_candles()
        obs = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        b = [s for s in obs if s.direction=='bullish'][0]
        assert b.zone_low == 100.0 and b.zone_high == 104.0

    def test_ob_pattern_type(self):
        c = self.bullish_ob_candles()
        obs = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert all(s.pattern_type == 'order_block' for s in obs)

    def test_no_ob_without_impulse(self):
        c = make_candles((100,102,99,101),(101,103,100,100),(100,102,99,101))
        obs = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert len(obs) == 0


class TestBreakerBlock:
    def _candles_with_break(self):
        return make_candles(
            (100,103,99,102),
            (102,108,100,101),  # bearish OB
            (101,116,100,116),  # strong bullish impulse → bullish OB at [1]
            (116,117,99,98),    # close < 100 → breaks bullish OB
        )

    def test_breaker_detected(self):
        c = self._candles_with_break()
        raw = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        _, breakers = _update_ob_status(raw, c)
        assert len(breakers) >= 1

    def test_breaker_pattern_type(self):
        c = self._candles_with_break()
        raw = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        _, breakers = _update_ob_status(raw, c)
        assert all(s.pattern_type == 'breaker' for s in breakers)

    def test_breaker_flips_direction(self):
        c = self._candles_with_break()
        raw = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        _, breakers = _update_ob_status(raw, c)
        assert any(b.direction == 'bearish' for b in breakers)

    def test_no_breaker_if_not_broken(self):
        c = make_candles((100,103,99,102),(102,108,100,101),(101,120,100,120),(120,125,118,122))
        raw = _detect_order_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        _, breakers = _update_ob_status(raw, c)
        assert len(breakers) == 0


class TestLiquidityVoid:
    def _lv_candles(self):
        normal = [OHLCV(timestamp=_ts(i),open=100+i,high=102+i,low=99+i,close=101+i) for i in range(5)]
        lv = OHLCV(timestamp=_ts(5),open=105,high=126,low=105,close=125)  # body=20/range=21>0.85
        return normal + [lv]

    def test_lv_detected(self):
        c = self._lv_candles()
        obs = _detect_liquidity_voids(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert len(obs) >= 1

    def test_lv_pattern_type(self):
        c = self._lv_candles()
        obs = _detect_liquidity_voids(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert all(s.pattern_type == 'liquidity_void' for s in obs)

    def test_lv_direction_bullish(self):
        c = self._lv_candles()
        obs = _detect_liquidity_voids(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert obs[-1].direction == 'bullish'

    def test_lv_zone_correct(self):
        c = self._lv_candles()
        obs = _detect_liquidity_voids(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        lv = obs[-1]
        assert lv.zone_high == 126.0 and lv.zone_low == 105.0

    def test_no_lv_small_candles(self):
        c = [OHLCV(timestamp=_ts(i),open=100+i,high=102+i,low=99+i,close=101+i) for i in range(10)]
        obs = _detect_liquidity_voids(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert len(obs) == 0


class TestMitigationBlock:
    def _mit_candles(self):
        return make_candles(
            (100,103,100,102),(102,105,101,104),(104,107,103,106),
            (106,108,105,105),  # bearish in bullish structure
            (105,106,103,104),
            (104,109,104,108),  # revisits zone of [3]
        )

    def test_mitigation_detected(self):
        c = self._mit_candles()
        obs = _detect_mitigation_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert len(obs) >= 1

    def test_mitigation_pattern_type(self):
        c = self._mit_candles()
        obs = _detect_mitigation_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert all(s.pattern_type == 'mitigation' for s in obs)

    def test_mitigation_direction_bullish(self):
        c = self._mit_candles()
        obs = _detect_mitigation_blocks(c,'NVDA','1h',_avg_body(c),_atr(c),'','','')
        assert all(s.direction == 'bullish' for s in obs)


class TestDetectAllPatterns:
    def _rich_candles(self, n=30):
        """Generates a trending up sequence with a gap and an impulse."""
        candles = []
        price = 100.0
        for i in range(n):
            if i == 10:
                # Bullish FVG: prev.high=110 < next.low=112 at i=11
                candles.append(OHLCV(_ts(i),price,price+10,price-1,price+9))
                price += 9
            elif i == 11:
                candles.append(OHLCV(_ts(i),price+3,price+10,price+3,price+8))
                price += 8
            elif i == 12:
                candles.append(OHLCV(_ts(i),price,price+5,price+2,price+4))
                price += 4
            else:
                candles.append(OHLCV(_ts(i),price,price+2,price-1,price+1))
                price += 1
        return candles

    def test_returns_list(self):
        assert isinstance(detect_all_patterns([], 'NVDA'), list)

    def test_too_few_candles_returns_empty(self):
        assert detect_all_patterns(make_candles((100,105,99,103),(103,108,101,106)), 'NVDA') == []

    def test_sorted_by_quality_desc(self):
        c = self._rich_candles()
        sigs = detect_all_patterns(c, 'NVDA', '1h', 'high', 'risk_on_expansion', 'long')
        for i in range(len(sigs) - 1):
            assert sigs[i].quality_score >= sigs[i+1].quality_score

    def test_filled_fvgs_excluded(self):
        c = self._rich_candles()
        sigs = detect_all_patterns(c, 'NVDA')
        assert all(s.status != 'filled' for s in sigs if s.pattern_type == 'fvg')

    def test_quality_scores_in_range(self):
        c = self._rich_candles()
        sigs = detect_all_patterns(c, 'NVDA', '1h', 'high', 'risk_on', 'long')
        assert all(0.0 <= s.quality_score <= 1.0 for s in sigs)

    def test_ticker_propagated(self):
        c = self._rich_candles()
        sigs = detect_all_patterns(c, 'TSLA', '1h')
        assert all(s.ticker == 'TSLA' for s in sigs)

    def test_timeframe_propagated(self):
        c = self._rich_candles()
        sigs = detect_all_patterns(c, 'NVDA', '4h')
        assert all(s.timeframe == '4h' for s in sigs)

    def test_kb_context_propagated(self):
        c = self._rich_candles()
        sigs = detect_all_patterns(c, 'NVDA', '1h', 'high', 'risk_on', 'long')
        for s in sigs:
            assert s.kb_conviction == 'high'
            assert s.kb_regime == 'risk_on'
            assert s.kb_signal_dir == 'long'

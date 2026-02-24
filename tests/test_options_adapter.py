"""
tests/test_options_adapter.py — Unit tests for ingest/options_adapter.py

Tests:
  - _iv_rank_from_chain: boundary cases, no-anchor fallback
  - _classify_options_regime: compressed / normal / elevated_vol thresholds
  - _put_call_ratio: empty OI, zero calls, normal ratios
  - _detect_sweep: call_sweep, put_sweep, none
  - OptionsAdapter.fetch(): mocked yfinance chain — atoms shape, values,
    empty chain handling, per-ticker error isolation

No live network calls — yfinance is fully mocked.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingest.options_adapter import (
    OptionsAdapter,
    _classify_options_regime,
    _detect_sweep,
    _iv_rank_from_chain,
    _put_call_ratio,
    _IV_RANK_COMPRESSED,
    _IV_RANK_ELEVATED,
    _SWEEP_VOLUME_RATIO,
)


# ── _iv_rank_from_chain ───────────────────────────────────────────────────────

class TestIvRankFromChain:
    def test_none_when_no_iv_data(self):
        assert _iv_rank_from_chain([], [], vol_30d_kb=30.0) is None

    def test_none_when_no_kb_anchor(self):
        assert _iv_rank_from_chain([0.30], [0.28], vol_30d_kb=None) is None

    def test_none_when_zero_kb_anchor(self):
        assert _iv_rank_from_chain([0.30], [0.28], vol_30d_kb=0.0) is None

    def test_rank_clamped_to_0_100(self):
        # Very low but above filter threshold (>0.01) → rank clamps to 0
        rank = _iv_rank_from_chain([0.02], [0.02], vol_30d_kb=30.0)
        assert rank is not None
        assert rank >= 0.0

        # Extremely high IV → rank clamps to 100
        rank = _iv_rank_from_chain([5.0], [5.0], vol_30d_kb=30.0)
        assert rank is not None
        assert rank <= 100.0

    def test_mid_iv_gives_mid_rank(self):
        # vol_30d=30 → iv_low=21, iv_high=54; current IV ~30% → midish rank
        rank = _iv_rank_from_chain([0.30], [0.30], vol_30d_kb=30.0)
        assert rank is not None
        assert 0.0 < rank < 100.0

    def test_multiple_iv_values_uses_median(self):
        # Should not raise; median is used
        rank = _iv_rank_from_chain(
            [0.20, 0.30, 0.40, 0.50],
            [0.25, 0.35],
            vol_30d_kb=25.0,
        )
        assert rank is not None

    def test_filters_near_zero_iv(self):
        # IV values <= 0.01 should be filtered out
        rank = _iv_rank_from_chain([0.001, 0.002], [0.001], vol_30d_kb=30.0)
        assert rank is None  # all filtered → no valid IV left


# ── _classify_options_regime ──────────────────────────────────────────────────

class TestClassifyOptionsRegime:
    def test_none_returns_unknown(self):
        assert _classify_options_regime(None) == 'unknown'

    def test_below_compressed_threshold(self):
        assert _classify_options_regime(_IV_RANK_COMPRESSED - 0.1) == 'compressed'

    def test_at_compressed_threshold(self):
        assert _classify_options_regime(_IV_RANK_COMPRESSED) == 'normal'

    def test_mid_range_is_normal(self):
        assert _classify_options_regime(50.0) == 'normal'

    def test_at_elevated_threshold(self):
        assert _classify_options_regime(_IV_RANK_ELEVATED) == 'normal'

    def test_above_elevated_threshold(self):
        assert _classify_options_regime(_IV_RANK_ELEVATED + 0.1) == 'elevated_vol'

    def test_zero_is_compressed(self):
        assert _classify_options_regime(0.0) == 'compressed'

    def test_100_is_elevated(self):
        assert _classify_options_regime(100.0) == 'elevated_vol'


# ── _put_call_ratio ───────────────────────────────────────────────────────────

class TestPutCallRatio:
    def test_none_when_empty(self):
        assert _put_call_ratio([], []) is None

    def test_none_when_zero_calls(self):
        assert _put_call_ratio([0, 0], [100, 200]) is None

    def test_basic_ratio(self):
        # 300 puts / 200 calls = 1.5
        r = _put_call_ratio([100, 100], [150, 150])
        assert r == pytest.approx(1.5)

    def test_bullish_ratio_below_1(self):
        r = _put_call_ratio([300, 200], [100, 50])
        assert r is not None
        assert r < 1.0

    def test_bearish_ratio_above_1(self):
        r = _put_call_ratio([50, 50], [200, 300])
        assert r is not None
        assert r > 1.0

    def test_none_values_treated_as_zero(self):
        # fillna(0) should handle None but here we pass ints
        r = _put_call_ratio([0, 100], [50, 50])
        assert r is not None  # second call has 100 OI


# ── _detect_sweep ─────────────────────────────────────────────────────────────

class TestDetectSweep:
    def test_no_sweep_returns_none_str(self):
        assert _detect_sweep([100], [100], [80], [80]) == 'none'

    def test_call_sweep_fires(self):
        # volume = 400 > 3 * OI (100) → call_sweep
        assert _detect_sweep([400], [100], [10], [100]) == 'call_sweep'

    def test_put_sweep_fires(self):
        # put volume = 600 > 3 * put OI (150) → put_sweep
        assert _detect_sweep([10], [100], [600], [150]) == 'put_sweep'

    def test_call_sweep_takes_priority(self):
        # Both fire — call checked first
        assert _detect_sweep([400], [100], [600], [150]) == 'call_sweep'

    def test_exactly_at_ratio_fires(self):
        # 300 == 3 * 100 → >= threshold → sweep fires (boundary is >=, not >)
        assert _detect_sweep([300], [100], [10], [100]) == 'call_sweep'

    def test_just_above_ratio_fires(self):
        assert _detect_sweep([301], [100], [10], [100]) == 'call_sweep'

    def test_zero_oi_skipped(self):
        # OI = 0 → division guard → no false positive
        assert _detect_sweep([1000], [0], [10], [100]) == 'none'

    def test_empty_lists(self):
        assert _detect_sweep([], [], [], []) == 'none'


# ── OptionsAdapter.fetch() ────────────────────────────────────────────────────

def _make_chain(
    n_strikes: int = 5,
    iv: float = 0.30,
    oi: int = 1000,
    vol: int = 500,
) -> SimpleNamespace:
    """Build a minimal mock option_chain namespace."""
    calls = pd.DataFrame({
        'impliedVolatility': [iv] * n_strikes,
        'openInterest':      [oi] * n_strikes,
        'volume':            [vol] * n_strikes,
    })
    puts = pd.DataFrame({
        'impliedVolatility': [iv * 1.05] * n_strikes,
        'openInterest':      [oi] * n_strikes,
        'volume':            [vol] * n_strikes,
    })
    return SimpleNamespace(calls=calls, puts=puts)


def _make_mock_yf_ticker(
    expiries=('2026-03-21', '2026-04-18'),
    chain=None,
    chain_raises=False,
):
    """Return a mock yf.Ticker instance."""
    mock_ticker = MagicMock()
    mock_ticker.options = expiries

    if chain_raises:
        mock_ticker.option_chain.side_effect = Exception('chain fetch error')
    else:
        mock_ticker.option_chain.return_value = chain or _make_chain()

    return mock_ticker


class TestOptionsAdapterFetch:
    @patch('ingest.options_adapter.yf')
    def test_returns_atoms_for_ticker(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter.fetch()

        assert len(atoms) > 0
        preds = {a.predicate for a in atoms}
        # options_regime and smart_money_signal always emitted when chain data present
        assert 'options_regime' in preds
        assert 'smart_money_signal' in preds

    @patch('ingest.options_adapter.yf')
    def test_subject_is_lowercase(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['NVDA'], sleep_sec=0)
        atoms = adapter.fetch()

        for a in atoms:
            assert a.subject == 'nvda'

    @patch('ingest.options_adapter.yf')
    def test_options_regime_values_valid(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['MSFT'], sleep_sec=0)
        atoms = adapter.fetch()

        regimes = [a.object for a in atoms if a.predicate == 'options_regime']
        assert len(regimes) == 1
        assert regimes[0] in ('compressed', 'normal', 'elevated_vol', 'unknown')

    @patch('ingest.options_adapter.yf')
    def test_smart_money_signal_values_valid(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['AMZN'], sleep_sec=0)
        atoms = adapter.fetch()

        sweeps = [a.object for a in atoms if a.predicate == 'smart_money_signal']
        assert len(sweeps) == 1
        assert sweeps[0] in ('call_sweep', 'put_sweep', 'none')

    @patch('ingest.options_adapter.yf')
    def test_upsert_true_on_all_atoms(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['JPM'], sleep_sec=0)
        atoms = adapter.fetch()

        for a in atoms:
            assert a.upsert is True

    @patch('ingest.options_adapter.yf')
    def test_empty_expiries_returns_no_atoms(self, mock_yf):
        mock_ticker = MagicMock()
        mock_ticker.options = []
        mock_yf.Ticker.return_value = mock_ticker

        adapter = OptionsAdapter(tickers=['XYZ'], sleep_sec=0)
        atoms = adapter.fetch()
        assert atoms == []

    @patch('ingest.options_adapter.yf')
    def test_chain_error_does_not_crash_adapter(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker(chain_raises=True)

        adapter = OptionsAdapter(tickers=['FAIL'], sleep_sec=0)
        atoms = adapter.fetch()
        # No atoms (chain failed) but no exception raised
        assert isinstance(atoms, list)

    @patch('ingest.options_adapter.yf')
    def test_one_ticker_failure_does_not_block_others(self, mock_yf):
        good_ticker = _make_mock_yf_ticker()
        fail_ticker = _make_mock_yf_ticker(chain_raises=True)

        def side_effect(sym):
            return fail_ticker if sym == 'FAIL' else good_ticker

        mock_yf.Ticker.side_effect = side_effect

        adapter = OptionsAdapter(tickers=['AAPL', 'FAIL', 'MSFT'], sleep_sec=0)
        atoms = adapter.fetch()

        # AAPL and MSFT should produce atoms despite FAIL erroring
        subjects = {a.subject for a in atoms}
        assert 'aapl' in subjects
        assert 'msft' in subjects

    @patch('ingest.options_adapter.yf')
    def test_call_sweep_detected(self, mock_yf):
        # Volume >> OI on calls → call_sweep
        sweep_chain = _make_chain(n_strikes=3, oi=100, vol=500)  # vol=500 > 3*100
        mock_yf.Ticker.return_value = _make_mock_yf_ticker(chain=sweep_chain)

        adapter = OptionsAdapter(tickers=['TSLA'], sleep_sec=0)
        atoms = adapter.fetch()

        sweeps = [a.object for a in atoms if a.predicate == 'smart_money_signal']
        assert sweeps == ['call_sweep']

    @patch('ingest.options_adapter.yf')
    def test_put_call_ratio_emitted(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['V'], sleep_sec=0)
        atoms = adapter.fetch()

        pcr_atoms = [a for a in atoms if a.predicate == 'put_call_ratio']
        assert len(pcr_atoms) == 1
        pcr_val = float(pcr_atoms[0].object)
        assert pcr_val > 0

    @patch('ingest.options_adapter.yf')
    def test_source_prefix_correct(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['GS'], sleep_sec=0)
        atoms = adapter.fetch()

        for a in atoms:
            assert a.source.startswith('options_feed_')

    @patch('ingest.options_adapter.yf')
    def test_multiple_tickers_isolated(self, mock_yf):
        mock_yf.Ticker.return_value = _make_mock_yf_ticker()

        adapter = OptionsAdapter(tickers=['AAPL', 'MSFT', 'NVDA'], sleep_sec=0)
        atoms = adapter.fetch()

        subjects = {a.subject for a in atoms}
        assert 'aapl' in subjects
        assert 'msft' in subjects
        assert 'nvda' in subjects

    def test_has_yfinance_false_returns_empty(self):
        with patch('ingest.options_adapter.HAS_YFINANCE', False):
            adapter = OptionsAdapter(tickers=['AAPL'], sleep_sec=0)
            atoms = adapter.fetch()
            assert atoms == []

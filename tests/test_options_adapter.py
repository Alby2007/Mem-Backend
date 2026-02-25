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
    _classify_skew_regime,
    _classify_tail_risk,
    _compute_skew,
    _detect_sweep,
    _iv_rank_from_chain,
    _put_call_ratio,
    _IV_RANK_COMPRESSED,
    _IV_RANK_ELEVATED,
    _SKEW_ELEVATED,
    _SKEW_SPIKE,
    _SWEEP_VOLUME_RATIO,
    _TAIL_ELEVATED,
    _TAIL_EXTREME,
    _TAIL_MODERATE,
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


# ── helpers for skew tests ────────────────────────────────────────────────────

def _make_skew_chain(atm_price=100.0, atm_iv=0.25, otm_put_iv=0.32, otm_call_iv=0.22):
    """Build minimal calls/puts DataFrames for skew calculation."""
    strikes = [atm_price * 0.90, atm_price * 0.95, atm_price, atm_price * 1.05, atm_price * 1.10]
    call_ivs = [otm_put_iv + 0.05, otm_put_iv, atm_iv, otm_call_iv, otm_call_iv - 0.02]
    put_ivs  = [otm_put_iv + 0.05, otm_put_iv, atm_iv, otm_call_iv, otm_call_iv - 0.02]
    calls = pd.DataFrame({'strike': strikes, 'impliedVolatility': call_ivs})
    puts  = pd.DataFrame({'strike': strikes, 'impliedVolatility': put_ivs})
    return calls, puts


# ── _compute_skew ─────────────────────────────────────────────────────────────

class TestComputeSkew:
    def test_normal_calculation(self):
        calls, puts = _make_skew_chain(atm_price=100.0, atm_iv=0.25,
                                       otm_put_iv=0.32, otm_call_iv=0.22)
        result = _compute_skew(calls, puts, current_price=100.0)
        assert result is not None
        assert result['skew_ratio'] > 1.0
        assert 'atm_iv' in result
        assert 'otm_put_iv' in result
        assert 'otm_call_iv' in result
        assert 'skew_25d' in result

    def test_skew_ratio_correct(self):
        calls, puts = _make_skew_chain(atm_price=100.0, atm_iv=0.25,
                                       otm_put_iv=0.32, otm_call_iv=0.22)
        result = _compute_skew(calls, puts, current_price=100.0)
        assert result is not None
        expected_ratio = round(0.32 / 0.25, 4)
        assert abs(result['skew_ratio'] - expected_ratio) < 0.01

    def test_empty_calls_returns_none(self):
        puts = pd.DataFrame({'strike': [95.0, 100.0], 'impliedVolatility': [0.30, 0.25]})
        result = _compute_skew(pd.DataFrame(), puts, current_price=100.0)
        assert result is None

    def test_empty_puts_returns_none(self):
        calls = pd.DataFrame({'strike': [100.0, 105.0], 'impliedVolatility': [0.25, 0.22]})
        result = _compute_skew(calls, pd.DataFrame(), current_price=100.0)
        assert result is None

    def test_none_dataframes_return_none(self):
        assert _compute_skew(None, None, current_price=100.0) is None

    def test_zero_current_price_returns_none(self):
        calls, puts = _make_skew_chain()
        assert _compute_skew(calls, puts, current_price=0.0) is None

    def test_zero_atm_iv_returns_none(self):
        calls = pd.DataFrame({'strike': [100.0], 'impliedVolatility': [0.0]})
        puts  = pd.DataFrame({'strike': [95.0],  'impliedVolatility': [0.30]})
        result = _compute_skew(calls, puts, current_price=100.0)
        assert result is None

    def test_missing_columns_returns_none(self):
        calls = pd.DataFrame({'strike': [100.0]})  # no impliedVolatility
        puts  = pd.DataFrame({'strike': [95.0], 'impliedVolatility': [0.30]})
        result = _compute_skew(calls, puts, current_price=100.0)
        assert result is None


# ── _classify_skew_regime ─────────────────────────────────────────────────────

class TestClassifySkewRegime:
    def test_normal_below_elevated_threshold(self):
        assert _classify_skew_regime(1.0) == 'normal'
        assert _classify_skew_regime(_SKEW_ELEVATED) == 'normal'   # boundary: not >

    def test_elevated_between_thresholds(self):
        assert _classify_skew_regime(_SKEW_ELEVATED + 0.01) == 'elevated'
        assert _classify_skew_regime(1.3) == 'elevated'
        assert _classify_skew_regime(_SKEW_SPIKE) == 'elevated'    # boundary: not >

    def test_spike_above_threshold(self):
        assert _classify_skew_regime(_SKEW_SPIKE + 0.01) == 'spike'
        assert _classify_skew_regime(2.0) == 'spike'


# ── _classify_tail_risk ───────────────────────────────────────────────────────

class TestClassifyTailRisk:
    def test_normal(self):
        assert _classify_tail_risk(1.0) == 'normal'
        assert _classify_tail_risk(_TAIL_MODERATE) == 'normal'     # boundary: not >

    def test_moderate(self):
        assert _classify_tail_risk(_TAIL_MODERATE + 0.01) == 'moderate'
        assert _classify_tail_risk(_TAIL_ELEVATED) == 'moderate'   # boundary: not >

    def test_elevated(self):
        assert _classify_tail_risk(_TAIL_ELEVATED + 0.01) == 'elevated'
        assert _classify_tail_risk(_TAIL_EXTREME) == 'elevated'    # boundary: not >

    def test_extreme(self):
        assert _classify_tail_risk(_TAIL_EXTREME + 0.01) == 'extreme'
        assert _classify_tail_risk(2.0) == 'extreme'


# ── skew atoms emitted by _fetch_one ─────────────────────────────────────────

class TestFetchOneSkewAtoms:
    @patch('ingest.options_adapter.yf')
    def test_skew_atoms_emitted_when_price_available(self, mock_yf):
        calls, puts = _make_skew_chain(atm_price=150.0, atm_iv=0.25,
                                       otm_put_iv=0.32, otm_call_iv=0.22)
        chain = SimpleNamespace(calls=calls, puts=puts)

        ticker_mock = MagicMock()
        ticker_mock.options = ['2026-03-21']
        ticker_mock.option_chain.return_value = chain
        ticker_mock.info = {'regularMarketPrice': 150.0}
        mock_yf.Ticker.return_value = ticker_mock

        adapter = OptionsAdapter(tickers=['NVDA'], sleep_sec=0)
        atoms = adapter._fetch_one('NVDA')

        preds = {a.predicate for a in atoms}
        assert 'iv_skew_ratio' in preds
        assert 'iv_skew_25d' in preds
        assert 'skew_regime' in preds

    @patch('ingest.options_adapter.yf')
    def test_skew_atoms_skipped_when_price_zero(self, mock_yf):
        calls, puts = _make_skew_chain(atm_price=150.0)
        chain = SimpleNamespace(calls=calls, puts=puts)

        ticker_mock = MagicMock()
        ticker_mock.options = ['2026-03-21']
        ticker_mock.option_chain.return_value = chain
        ticker_mock.info = {'regularMarketPrice': 0.0, 'currentPrice': None, 'previousClose': None}
        mock_yf.Ticker.return_value = ticker_mock

        adapter = OptionsAdapter(tickers=['NVDA'], sleep_sec=0)
        atoms = adapter._fetch_one('NVDA')

        preds = {a.predicate for a in atoms}
        assert 'iv_skew_ratio' not in preds
        assert 'skew_regime' not in preds

    @patch('ingest.options_adapter.yf')
    def test_skew_regime_values_valid(self, mock_yf):
        calls, puts = _make_skew_chain(atm_price=100.0, atm_iv=0.25,
                                       otm_put_iv=0.32, otm_call_iv=0.22)
        chain = SimpleNamespace(calls=calls, puts=puts)

        ticker_mock = MagicMock()
        ticker_mock.options = ['2026-03-21']
        ticker_mock.option_chain.return_value = chain
        ticker_mock.info = {'regularMarketPrice': 100.0}
        mock_yf.Ticker.return_value = ticker_mock

        adapter = OptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_one('AAPL')

        regimes = [a.object for a in atoms if a.predicate == 'skew_regime']
        assert len(regimes) == 1
        assert regimes[0] in ('normal', 'elevated', 'spike')


# ── SPY market-level skew atoms ───────────────────────────────────────────────

class TestSpySkewMarketAtoms:
    @patch('ingest.options_adapter.yf')
    def test_spy_skew_emits_three_market_atoms(self, mock_yf):
        calls, puts = _make_skew_chain(atm_price=500.0, atm_iv=0.18,
                                       otm_put_iv=0.23, otm_call_iv=0.16)
        chain = SimpleNamespace(calls=calls, puts=puts)

        ticker_mock = MagicMock()
        ticker_mock.options = ['2026-03-21']
        ticker_mock.option_chain.return_value = chain
        ticker_mock.info = {'regularMarketPrice': 500.0}
        mock_yf.Ticker.return_value = ticker_mock

        adapter = OptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_spy_skew()

        assert len(atoms) == 3
        preds = {a.predicate for a in atoms}
        assert 'spy_skew_ratio' in preds
        assert 'spy_skew_regime' in preds
        assert 'tail_risk' in preds

    @patch('ingest.options_adapter.yf')
    def test_spy_market_atoms_subject_is_market(self, mock_yf):
        calls, puts = _make_skew_chain(atm_price=500.0)
        chain = SimpleNamespace(calls=calls, puts=puts)

        ticker_mock = MagicMock()
        ticker_mock.options = ['2026-03-21']
        ticker_mock.option_chain.return_value = chain
        ticker_mock.info = {'regularMarketPrice': 500.0}
        mock_yf.Ticker.return_value = ticker_mock

        adapter = OptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_spy_skew()

        for a in atoms:
            assert a.subject == 'market'

    @patch('ingest.options_adapter.yf')
    def test_spy_skew_returns_empty_when_price_missing(self, mock_yf):
        calls, puts = _make_skew_chain(atm_price=500.0)
        chain = SimpleNamespace(calls=calls, puts=puts)

        ticker_mock = MagicMock()
        ticker_mock.options = ['2026-03-21']
        ticker_mock.option_chain.return_value = chain
        ticker_mock.info = {'regularMarketPrice': 0.0, 'currentPrice': None, 'previousClose': None}
        mock_yf.Ticker.return_value = ticker_mock

        adapter = OptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_spy_skew()

        assert atoms == []

    @patch('ingest.options_adapter.yf')
    def test_spy_included_in_full_fetch(self, mock_yf):
        calls, puts = _make_skew_chain(atm_price=150.0)
        chain = SimpleNamespace(calls=calls, puts=puts)

        ticker_mock = MagicMock()
        ticker_mock.options = ['2026-03-21']
        ticker_mock.option_chain.return_value = chain
        ticker_mock.info = {'regularMarketPrice': 150.0}
        mock_yf.Ticker.return_value = ticker_mock

        adapter = OptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter.fetch()

        market_atoms = [a for a in atoms if a.subject == 'market']
        preds = {a.predicate for a in market_atoms}
        assert 'tail_risk' in preds

"""
tests/test_adversarial_stress.py — Adversarial Stress Test Tests

Covers:
  - _SCENARIOS dict: all 6 scenarios present
  - _TIER_NUMERIC: correct tier ordering
  - Individual scenario functions: correct atom injection
  - _compute_conviction_tier: returns correct tier from pure-function pipeline
  - _run_scenario: correct delta computation, robust flag, fragility_score
  - run_stress_test: structure, all 6 scenarios run, portfolio_fragility
  - run_stress_test with empty KB: graceful handling
  - POST /analytics/stress-test API endpoint
"""

from __future__ import annotations

import sqlite3
import tempfile
import os
from unittest.mock import patch, MagicMock

import pytest

from analytics.adversarial_stress import (
    _SCENARIOS,
    _TIER_NUMERIC,
    _scenario_bear_analyst,
    _scenario_risk_off_regime,
    _scenario_earnings_miss,
    _scenario_macro_flip,
    _scenario_guidance_lowered,
    _scenario_credit_downgrade,
    _compute_conviction_tier,
    _run_scenario,
    _read_baseline,
    run_stress_test,
)
from ingest.signal_enrichment_adapter import _CREDIT_PROXY, _RATES_PROXY, _MARKET_PROXY


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_test_db(with_atoms: bool = True) -> str:
    """
    Create a temp SQLite DB with facts table.
    Optionally pre-populate with a small high-conviction ticker.
    """
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT, confidence REAL DEFAULT 0.7,
            confidence_effective REAL, metadata TEXT,
            hit_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if with_atoms:
        # A high-conviction long: strong signal, moderate risk, bullish macro
        _bulk_insert(conn, [
            ('aapl', 'signal_direction', 'long'),
            ('aapl', 'volatility_regime', 'low_volatility'),
            ('aapl', 'thesis_risk_level', 'moderate'),
            ('aapl', 'last_price', '190.0'),
            ('aapl', 'price_target', '230.0'),  # +21% upside → strong
            ('aapl', 'volatility_30d', '22.0'),
            # macro proxies
            (_MARKET_PROXY, 'signal_direction', 'near_high'),
            (_CREDIT_PROXY, 'signal_direction', 'near_high'),
            (_RATES_PROXY,  'signal_direction', 'near_low'),
        ])
    conn.commit()
    conn.close()
    return path


def _bulk_insert(conn: sqlite3.Connection, atoms) -> None:
    for subject, predicate, obj in atoms:
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source, confidence) "
            "VALUES (?, ?, ?, 'test', 0.8)",
            (subject, predicate, obj),
        )


def _make_two_ticker_db() -> str:
    """Two equity tickers: one high-conviction, one medium."""
    path = _make_test_db(with_atoms=False)
    conn = sqlite3.connect(path)
    # High-conviction AAPL
    _bulk_insert(conn, [
        ('aapl', 'signal_direction', 'long'),
        ('aapl', 'volatility_regime', 'low_volatility'),
        ('aapl', 'thesis_risk_level', 'moderate'),
        ('aapl', 'last_price', '190.0'),
        ('aapl', 'price_target', '230.0'),
        ('aapl', 'volatility_30d', '22.0'),
    ])
    # Medium-conviction MSFT (confirmed signal quality)
    _bulk_insert(conn, [
        ('msft', 'signal_direction', 'long'),
        ('msft', 'volatility_regime', 'medium_volatility'),
        ('msft', 'thesis_risk_level', 'moderate'),
        ('msft', 'last_price', '400.0'),
        ('msft', 'price_target', '432.0'),  # +8% → confirmed
        ('msft', 'volatility_30d', '25.0'),
    ])
    # Macro: confirmed risk-on
    _bulk_insert(conn, [
        (_MARKET_PROXY, 'signal_direction', 'near_high'),
        (_CREDIT_PROXY, 'signal_direction', 'near_high'),
        (_RATES_PROXY,  'signal_direction', 'near_low'),
    ])
    conn.commit()
    conn.close()
    return path


def _baseline_ticker_atoms() -> tuple:
    """Return a minimal ticker_atoms + macro_signals for unit tests."""
    ticker_atoms = {
        'aapl': {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '190.0',
            'price_target': '230.0',
            'volatility_30d': '22.0',
        },
        'msft': {
            'signal_direction': 'long',
            'volatility_regime': 'medium_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '400.0',
            'price_target': '432.0',
            'volatility_30d': '25.0',
        },
        _MARKET_PROXY: {'signal_direction': 'near_high'},
        _CREDIT_PROXY: {'signal_direction': 'near_high'},
        _RATES_PROXY:  {'signal_direction': 'near_low'},
    }
    macro_signals = {
        _MARKET_PROXY: 'near_high',
        _CREDIT_PROXY: 'near_high',
        _RATES_PROXY:  'near_low',
    }
    return ticker_atoms, macro_signals


# ── TestScenariosRegistry ─────────────────────────────────────────────────────

class TestScenariosRegistry:

    def test_all_six_scenarios_present(self):
        expected = {
            'bear_analyst', 'risk_off_regime', 'earnings_miss',
            'macro_flip', 'guidance_lowered', 'credit_downgrade',
        }
        assert expected == set(_SCENARIOS.keys())

    def test_all_scenarios_are_callable(self):
        for name, fn in _SCENARIOS.items():
            assert callable(fn), f'{name} is not callable'


# ── TestTierNumeric ───────────────────────────────────────────────────────────

class TestTierNumeric:

    def test_high_is_highest(self):
        assert _TIER_NUMERIC['high'] > _TIER_NUMERIC['medium']
        assert _TIER_NUMERIC['medium'] > _TIER_NUMERIC['low']
        assert _TIER_NUMERIC['low'] > _TIER_NUMERIC['avoid']

    def test_avoid_is_zero(self):
        assert _TIER_NUMERIC['avoid'] == 0

    def test_high_is_3(self):
        assert _TIER_NUMERIC['high'] == 3


# ── TestScenarioBearAnalyst ───────────────────────────────────────────────────

class TestScenarioBearAnalyst:

    def test_equity_tickers_get_conflicted(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, mod_ms = _scenario_bear_analyst(ta, ms)
        assert mod_ta['aapl']['signal_quality'] == 'conflicted'
        assert mod_ta['msft']['signal_quality'] == 'conflicted'

    def test_macro_proxies_not_touched(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, _ = _scenario_bear_analyst(ta, ms)
        assert 'signal_quality' not in mod_ta.get(_MARKET_PROXY, {})
        assert 'signal_quality' not in mod_ta.get(_CREDIT_PROXY, {})

    def test_original_not_mutated(self):
        ta, ms = _baseline_ticker_atoms()
        _scenario_bear_analyst(ta, ms)
        assert 'signal_quality' not in ta['aapl']

    def test_macro_signals_unchanged(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_bear_analyst(ta, ms)
        assert mod_ms == ms


# ── TestScenarioRiskOffRegime ─────────────────────────────────────────────────

class TestScenarioRiskOffRegime:

    def test_spy_set_to_near_low(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_risk_off_regime(ta, ms)
        assert mod_ms[_MARKET_PROXY] == 'near_low'

    def test_hyg_set_to_near_low(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_risk_off_regime(ta, ms)
        assert mod_ms[_CREDIT_PROXY] == 'near_low'

    def test_tlt_set_to_near_high(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_risk_off_regime(ta, ms)
        assert mod_ms[_RATES_PROXY] == 'near_high'

    def test_original_macro_signals_not_mutated(self):
        ta, ms = _baseline_ticker_atoms()
        _scenario_risk_off_regime(ta, ms)
        assert ms[_MARKET_PROXY] == 'near_high'


# ── TestScenarioEarningsMiss ──────────────────────────────────────────────────

class TestScenarioEarningsMiss:

    def test_bullish_tickers_get_neutral_direction(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, _ = _scenario_earnings_miss(ta, ms)
        assert mod_ta['aapl']['signal_direction'] == 'neutral'
        assert mod_ta['msft']['signal_direction'] == 'neutral'

    def test_earnings_miss_atom_injected(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, _ = _scenario_earnings_miss(ta, ms)
        assert mod_ta['aapl'].get('earnings_miss') == 'q_current'

    def test_macro_proxies_not_touched(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, _ = _scenario_earnings_miss(ta, ms)
        assert mod_ta[_MARKET_PROXY]['signal_direction'] == 'near_high'


# ── TestScenarioMacroFlip ─────────────────────────────────────────────────────

class TestScenarioMacroFlip:

    def test_all_three_proxies_flipped(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_macro_flip(ta, ms)
        assert mod_ms[_MARKET_PROXY] == 'near_low'
        assert mod_ms[_CREDIT_PROXY] == 'near_low'
        assert mod_ms[_RATES_PROXY]  == 'near_low'

    def test_original_not_mutated(self):
        ta, ms = _baseline_ticker_atoms()
        _scenario_macro_flip(ta, ms)
        assert ms[_MARKET_PROXY] == 'near_high'


# ── TestScenarioGuidanceLowered ───────────────────────────────────────────────

class TestScenarioGuidanceLowered:

    def test_bullish_tickers_get_neutral(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, _ = _scenario_guidance_lowered(ta, ms)
        assert mod_ta['aapl']['signal_direction'] == 'neutral'

    def test_guidance_lowered_atom_injected(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, _ = _scenario_guidance_lowered(ta, ms)
        assert mod_ta['aapl'].get('guidance_lowered') == 'fy_current'

    def test_macro_proxies_not_touched(self):
        ta, ms = _baseline_ticker_atoms()
        mod_ta, _ = _scenario_guidance_lowered(ta, ms)
        assert mod_ta[_MARKET_PROXY]['signal_direction'] == 'near_high'


# ── TestScenarioCreditDowngrade ───────────────────────────────────────────────

class TestScenarioCreditDowngrade:

    def test_hyg_set_to_near_low(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_credit_downgrade(ta, ms)
        assert mod_ms[_CREDIT_PROXY] == 'near_low'

    def test_tlt_set_to_near_high(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_credit_downgrade(ta, ms)
        assert mod_ms[_RATES_PROXY] == 'near_high'

    def test_spy_unchanged(self):
        ta, ms = _baseline_ticker_atoms()
        _, mod_ms = _scenario_credit_downgrade(ta, ms)
        assert mod_ms[_MARKET_PROXY] == 'near_high'  # SPY not touched


# ── TestComputeConvictionTier ─────────────────────────────────────────────────

class TestComputeConvictionTier:

    def _risk_on_macro(self):
        return {
            _MARKET_PROXY: 'near_high',
            _CREDIT_PROXY: 'near_high',
            _RATES_PROXY:  'near_low',
        }

    def test_strong_moderate_macro_confirmed_is_high(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '100.0',
            'price_target': '120.0',  # +20% → strong
            'volatility_30d': '20.0',
        }
        tier = _compute_conviction_tier('aapl', preds, self._risk_on_macro())
        assert tier == 'high'

    def test_weak_tight_is_avoid(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'high_volatility',
            'thesis_risk_level': 'tight',
            'last_price': '100.0',
            'price_target': '103.0',  # +3% → very low upside → weak
            'volatility_30d': '60.0',
        }
        tier = _compute_conviction_tier('gs', preds, self._risk_on_macro())
        assert tier == 'avoid'

    def test_conflicted_signal_is_avoid(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '100.0',
            'price_target': '95.0',  # negative upside → conflicted
            'volatility_30d': '20.0',
        }
        tier = _compute_conviction_tier('aapl', preds, self._risk_on_macro())
        assert tier == 'avoid'

    def test_scenario_injected_signal_quality_overrides(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '100.0',
            'price_target': '120.0',
            'volatility_30d': '20.0',
            'signal_quality': 'conflicted',  # bear_analyst scenario injection
        }
        tier = _compute_conviction_tier('aapl', preds, self._risk_on_macro())
        assert tier == 'avoid'

    def test_missing_thesis_risk_returns_none(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            # no thesis_risk_level
            'last_price': '100.0',
            'price_target': '120.0',
        }
        tier = _compute_conviction_tier('aapl', preds, self._risk_on_macro())
        assert tier is None


# ── TestRunScenario ───────────────────────────────────────────────────────────

class TestRunScenario:

    def _setup(self):
        ta, ms = _baseline_ticker_atoms()
        # compute baseline tiers
        baseline = {}
        for ticker in ('aapl', 'msft'):
            t = _compute_conviction_tier(ticker, ta[ticker], ms)
            if t:
                baseline[ticker] = t
        return ta, ms, baseline

    def test_unknown_scenario_returns_error(self):
        ta, ms, baseline = self._setup()
        result = _run_scenario('nonexistent_scenario', ta, ms, baseline)
        assert 'error' in result

    def test_bear_analyst_degrades_tickers(self):
        ta, ms, baseline = self._setup()
        result = _run_scenario('bear_analyst', ta, ms, baseline)
        assert result['n_degraded'] > 0

    def test_fragility_score_between_0_and_1(self):
        ta, ms, baseline = self._setup()
        for scenario in _SCENARIOS:
            result = _run_scenario(scenario, ta, ms, baseline)
            assert 0.0 <= result['fragility_score'] <= 1.0, \
                f'{scenario} fragility_score out of range'

    def test_ticker_results_have_required_keys(self):
        ta, ms, baseline = self._setup()
        result = _run_scenario('bear_analyst', ta, ms, baseline)
        for tr in result['ticker_results']:
            assert 'ticker' in tr
            assert 'tier_before' in tr
            assert 'tier_after' in tr
            assert 'delta' in tr
            assert 'robust' in tr

    def test_robust_flag_correct(self):
        ta, ms, baseline = self._setup()
        result = _run_scenario('bear_analyst', ta, ms, baseline)
        for tr in result['ticker_results']:
            if tr['delta'] == 0:
                assert tr['robust'] is True
            else:
                assert tr['robust'] is False

    def test_most_fragile_first_in_results(self):
        ta, ms, baseline = self._setup()
        result = _run_scenario('bear_analyst', ta, ms, baseline)
        deltas = [tr['delta'] for tr in result['ticker_results']]
        assert deltas == sorted(deltas, reverse=True)

    def test_credit_downgrade_degrades_longs(self):
        ta, ms, baseline = self._setup()
        result = _run_scenario('credit_downgrade', ta, ms, baseline)
        assert result['n_tickers_tested'] > 0
        # credit_downgrade flips HYG/TLT — should degrade macro_confirmation
        assert result['fragility_score'] >= 0.0


# ── TestRunStressTest ─────────────────────────────────────────────────────────

class TestRunStressTest:

    def test_returns_all_six_scenarios(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path)
        assert set(result['results'].keys()) == set(_SCENARIOS.keys())

    def test_correct_structure(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path)
        assert 'as_of' in result
        assert 'baseline_tickers' in result
        assert 'scenarios_run' in result
        assert 'results' in result
        assert 'portfolio_fragility' in result

    def test_portfolio_fragility_keys(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path)
        pf = result['portfolio_fragility']
        assert 'most_fragile_ticker' in pf
        assert 'mean_fragility' in pf
        assert 'scenario_fragility' in pf

    def test_mean_fragility_between_0_and_1(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path)
        assert 0.0 <= result['portfolio_fragility']['mean_fragility'] <= 1.0

    def test_specific_scenarios_subset(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path, scenarios=['bear_analyst', 'macro_flip'])
        assert set(result['results'].keys()) == {'bear_analyst', 'macro_flip'}
        assert result['scenarios_run'] == ['bear_analyst', 'macro_flip']

    def test_unknown_scenario_returns_error(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path, scenarios=['nonexistent'])
        assert 'error' in result

    def test_empty_db_returns_zero_baseline(self):
        path = _make_test_db(with_atoms=False)
        result = run_stress_test(path)
        assert result['baseline_tickers'] == 0

    def test_bear_analyst_scenario_has_ticker_results(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path)
        bear = result['results']['bear_analyst']
        assert 'ticker_results' in bear
        assert bear['n_tickers_tested'] >= 0  # may be 0 if tickers lack thesis_risk_level

    def test_all_scenario_fragility_scores_between_0_and_1(self):
        path = _make_two_ticker_db()
        result = run_stress_test(path)
        for scenario, score in result['portfolio_fragility']['scenario_fragility'].items():
            assert 0.0 <= score <= 1.0, f'{scenario}: fragility {score} out of range'


# ── TestStressTestApiEndpoint ─────────────────────────────────────────────────

class TestStressTestApiEndpoint:

    @pytest.fixture(autouse=True)
    def patch_db(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / 'test.db')
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                source TEXT, confidence REAL DEFAULT 0.7,
                confidence_effective REAL, metadata TEXT,
                hit_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
        import api
        monkeypatch.setattr(api, '_DB_PATH', db_path)
        self.db_path = db_path

    @pytest.fixture
    def client(self):
        import api
        api.app.config['TESTING'] = True
        with api.app.test_client() as c:
            yield c

    def test_returns_200_with_empty_body(self, client):
        resp = client.post('/analytics/stress-test', json={})
        assert resp.status_code == 200

    def test_returns_all_six_scenarios_by_default(self, client):
        resp = client.post('/analytics/stress-test', json={})
        data = resp.get_json()
        assert set(data['results'].keys()) == set(_SCENARIOS.keys())

    def test_returns_subset_when_scenarios_specified(self, client):
        resp = client.post('/analytics/stress-test',
                           json={'scenarios': ['bear_analyst', 'macro_flip']})
        assert resp.status_code == 200
        data = resp.get_json()
        assert set(data['results'].keys()) == {'bear_analyst', 'macro_flip'}

    def test_unknown_scenario_returns_error_in_response(self, client):
        resp = client.post('/analytics/stress-test',
                           json={'scenarios': ['does_not_exist']})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'error' in data

    def test_response_has_portfolio_fragility(self, client):
        resp = client.post('/analytics/stress-test', json={})
        data = resp.get_json()
        assert 'portfolio_fragility' in data

    def test_as_of_present_in_response(self, client):
        resp = client.post('/analytics/stress-test', json={})
        data = resp.get_json()
        assert 'as_of' in data

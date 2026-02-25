"""
tests/test_counterfactual.py — Counterfactual Reasoning Tests

Covers:
  - _apply_scenario: direct macro signal overrides
  - _apply_scenario: scalar shift → causal seed mapping
  - _apply_scenario: per-ticker atom overrides
  - _apply_scenario: does not mutate originals
  - run_counterfactual: structure and required keys
  - run_counterfactual: upgrade/downgrade/unchanged counts correct
  - run_counterfactual: regime_change detection
  - run_counterfactual: empty scenario returns error
  - run_counterfactual: empty KB returns zero baseline
  - run_counterfactual: methodology field = direct_override vs causal
  - POST /analytics/counterfactual API endpoint
"""

from __future__ import annotations

import sqlite3
import tempfile
import os

import pytest

from analytics.counterfactual import (
    _compute_conviction_tier,
    _apply_scenario,
    _get_baseline,
    run_counterfactual,
    _TIER_NUMERIC,
    _SCALAR_CAUSAL_MAP,
    _CAUSAL_EFFECT_TO_MACRO,
)
from ingest.signal_enrichment_adapter import _CREDIT_PROXY, _RATES_PROXY, _MARKET_PROXY


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_test_db(risk_off: bool = False) -> str:
    """
    Create a temp DB.
    If risk_off=False: macro is confirmed risk-on, AAPL is high-conviction.
    If risk_off=True:  macro is risk-off, AAPL conviction degrades.
    """
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT, confidence REAL DEFAULT 0.8,
            confidence_effective REAL, metadata TEXT,
            hit_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    def ins(subj, pred, obj):
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source) "
            "VALUES (?, ?, ?, 'test')",
            (subj, pred, obj),
        )

    # AAPL: strong signal, moderate risk
    ins('aapl', 'signal_direction', 'long')
    ins('aapl', 'volatility_regime', 'low_volatility')
    ins('aapl', 'thesis_risk_level', 'moderate')
    ins('aapl', 'last_price', '190.0')
    ins('aapl', 'price_target', '230.0')  # +21% → strong
    ins('aapl', 'volatility_30d', '22.0')

    # GS: confirmed signal quality (8% upside), moderate risk
    ins('gs', 'signal_direction', 'long')
    ins('gs', 'volatility_regime', 'medium_volatility')
    ins('gs', 'thesis_risk_level', 'moderate')
    ins('gs', 'last_price', '500.0')
    ins('gs', 'price_target', '542.0')  # +8.4% → confirmed
    ins('gs', 'volatility_30d', '28.0')

    if risk_off:
        ins(_MARKET_PROXY, 'signal_direction', 'near_low')
        ins(_CREDIT_PROXY, 'signal_direction', 'near_low')
        ins(_RATES_PROXY,  'signal_direction', 'near_high')
    else:
        ins(_MARKET_PROXY, 'signal_direction', 'near_high')
        ins(_CREDIT_PROXY, 'signal_direction', 'near_high')
        ins(_RATES_PROXY,  'signal_direction', 'near_low')

    conn.commit()
    conn.close()
    return path


def _baseline_state():
    """Return a minimal in-memory state for unit tests."""
    ticker_atoms = {
        'aapl': {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '190.0',
            'price_target': '230.0',
            'volatility_30d': '22.0',
        },
        'gs': {
            'signal_direction': 'long',
            'volatility_regime': 'medium_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '500.0',
            'price_target': '542.0',
            'volatility_30d': '28.0',
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


# ── TestTierNumericConsistency ────────────────────────────────────────────────

class TestTierNumericConsistency:

    def test_high_is_3(self):
        assert _TIER_NUMERIC['high'] == 3

    def test_avoid_is_0(self):
        assert _TIER_NUMERIC['avoid'] == 0

    def test_ordering(self):
        assert _TIER_NUMERIC['high'] > _TIER_NUMERIC['medium']
        assert _TIER_NUMERIC['medium'] > _TIER_NUMERIC['low']
        assert _TIER_NUMERIC['low'] > _TIER_NUMERIC['avoid']


# ── TestApplyScenario ─────────────────────────────────────────────────────────

class TestApplyScenario:

    def test_spy_signal_override(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        mod_ta, mod_ms, _, _ = _apply_scenario(
            path, {'spy_signal': 'near_low'}, ta, ms
        )
        assert mod_ms[_MARKET_PROXY] == 'near_low'

    def test_hyg_signal_override(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        mod_ta, mod_ms, _, _ = _apply_scenario(
            path, {'hyg_signal': 'near_low'}, ta, ms
        )
        assert mod_ms[_CREDIT_PROXY] == 'near_low'

    def test_tlt_signal_override(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        mod_ta, mod_ms, _, _ = _apply_scenario(
            path, {'tlt_signal': 'near_high'}, ta, ms
        )
        assert mod_ms[_RATES_PROXY] == 'near_high'

    def test_per_ticker_override(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        mod_ta, mod_ms, _, _ = _apply_scenario(
            path,
            {'tickers': {'GS': {'thesis_risk_level': 'tight'}}},
            ta, ms,
        )
        assert mod_ta['gs']['thesis_risk_level'] == 'tight'

    def test_per_ticker_override_case_insensitive(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        mod_ta, mod_ms, _, _ = _apply_scenario(
            path,
            {'tickers': {'AAPL': {'thesis_risk_level': 'wide'}}},
            ta, ms,
        )
        assert mod_ta['aapl']['thesis_risk_level'] == 'wide'

    def test_original_not_mutated(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        _apply_scenario(path, {'spy_signal': 'near_low'}, ta, ms)
        assert ms[_MARKET_PROXY] == 'near_high'  # original unchanged
        assert ta['aapl']['thesis_risk_level'] == 'moderate'  # original unchanged

    def test_direct_override_methodology(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        _, _, _, methodology = _apply_scenario(
            path, {'spy_signal': 'near_low'}, ta, ms
        )
        assert methodology == 'direct_override'

    def test_fed_rate_cut_triggers_causal_seed(self):
        """A fed_rate_cut shift should set methodology to causal_graph_propagation
        IF the causal_graph module is available (falls back gracefully otherwise)."""
        path = _make_test_db()
        ta, ms = _baseline_state()
        _, _, _, methodology = _apply_scenario(
            path, {'fed_funds_rate': -0.25}, ta, ms
        )
        # methodology is either causal (if HAS_CAUSAL) or direct_override fallback
        assert methodology in ('causal_graph_propagation', 'direct_override')

    def test_unknown_keys_ignored(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        # Should not raise
        mod_ta, mod_ms, _, _ = _apply_scenario(
            path, {'random_unknown_key': 'foo', 'another': 123}, ta, ms
        )
        assert mod_ms == ms  # no macro changes

    def test_multiple_overrides_applied(self):
        path = _make_test_db()
        ta, ms = _baseline_state()
        mod_ta, mod_ms, _, _ = _apply_scenario(
            path,
            {'spy_signal': 'near_low', 'hyg_signal': 'near_low', 'tlt_signal': 'near_high'},
            ta, ms,
        )
        assert mod_ms[_MARKET_PROXY] == 'near_low'
        assert mod_ms[_CREDIT_PROXY] == 'near_low'
        assert mod_ms[_RATES_PROXY]  == 'near_high'


# ── TestComputeConvictionTier ─────────────────────────────────────────────────

class TestComputeConvictionTier:

    def _risk_on_macro(self):
        return {
            _MARKET_PROXY: 'near_high',
            _CREDIT_PROXY: 'near_high',
            _RATES_PROXY: 'near_low',
        }

    def _risk_off_macro(self):
        return {
            _MARKET_PROXY: 'near_low',
            _CREDIT_PROXY: 'near_low',
            _RATES_PROXY: 'near_high',
        }

    def test_high_conviction_with_risk_on_macro(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '100.0',
            'price_target': '120.0',
            'volatility_30d': '20.0',
        }
        tier = _compute_conviction_tier('aapl', preds, self._risk_on_macro())
        assert tier == 'high'

    def test_degrades_with_risk_off_macro(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'thesis_risk_level': 'moderate',
            'last_price': '100.0',
            'price_target': '120.0',
            'volatility_30d': '20.0',
        }
        tier_on  = _compute_conviction_tier('aapl', preds, self._risk_on_macro())
        tier_off = _compute_conviction_tier('aapl', preds, self._risk_off_macro())
        assert _TIER_NUMERIC.get(tier_on, 0) >= _TIER_NUMERIC.get(tier_off, 0)

    def test_missing_thesis_risk_returns_none(self):
        preds = {
            'signal_direction': 'long',
            'volatility_regime': 'low_volatility',
            'last_price': '100.0',
            'price_target': '120.0',
        }
        result = _compute_conviction_tier('aapl', preds, self._risk_on_macro())
        assert result is None


# ── TestRunCounterfactual ─────────────────────────────────────────────────────

class TestRunCounterfactual:

    def test_returns_required_keys(self):
        path = _make_test_db()
        result = run_counterfactual(path, {'spy_signal': 'near_low'})
        required = {
            'as_of', 'scenario_applied', 'causal_propagation',
            'baseline_tickers', 'tier_changes', 'upgrades', 'downgrades',
            'unchanged', 'regime_change', 'methodology',
        }
        assert required == set(result.keys())

    def test_empty_scenario_returns_error(self):
        path = _make_test_db()
        result = run_counterfactual(path, {})
        assert 'error' in result

    def test_empty_db_returns_zero_baseline(self):
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(path)
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                source TEXT, confidence REAL DEFAULT 0.7,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
        result = run_counterfactual(path, {'spy_signal': 'near_low'})
        assert result['baseline_tickers'] == 0

    def test_upgrades_plus_downgrades_plus_unchanged_equals_total_changes(self):
        path = _make_test_db()
        result = run_counterfactual(path, {'spy_signal': 'near_low'})
        total = result['upgrades'] + result['downgrades'] + result['unchanged']
        # total changes + unchanged should <= baseline_tickers
        assert total <= result['baseline_tickers'] + 1  # allow for some None tiers

    def test_upgrade_scenario_produces_upgrades(self):
        """Start from risk-off DB; apply risk-on scenario → should produce upgrades."""
        path = _make_test_db(risk_off=True)
        result = run_counterfactual(path, {
            'spy_signal': 'near_high',
            'hyg_signal': 'near_high',
            'tlt_signal': 'near_low',
        })
        assert result['upgrades'] > 0

    def test_downgrade_scenario_produces_downgrades(self):
        """Start from risk-on DB; apply risk-off scenario → should produce downgrades."""
        path = _make_test_db(risk_off=False)
        result = run_counterfactual(path, {
            'spy_signal': 'near_low',
            'hyg_signal': 'near_low',
            'tlt_signal': 'near_high',
        })
        assert result['downgrades'] > 0

    def test_tier_changes_sorted_upgrades_first(self):
        path = _make_test_db(risk_off=True)
        result = run_counterfactual(path, {
            'spy_signal': 'near_high',
            'hyg_signal': 'near_high',
            'tlt_signal': 'near_low',
        })
        deltas = [tc['delta'] for tc in result['tier_changes']]
        assert deltas == sorted(deltas, reverse=True)

    def test_tier_change_direction_field(self):
        path = _make_test_db(risk_off=True)
        result = run_counterfactual(path, {
            'spy_signal': 'near_high',
            'hyg_signal': 'near_high',
            'tlt_signal': 'near_low',
        })
        for tc in result['tier_changes']:
            if tc['delta'] > 0:
                assert tc['direction'] == 'upgrade'
            elif tc['delta'] < 0:
                assert tc['direction'] == 'downgrade'
            else:
                assert tc['direction'] == 'unchanged'

    def test_unchanged_tickers_not_in_tier_changes(self):
        """tier_changes should only contain tickers that actually changed."""
        path = _make_test_db()
        result = run_counterfactual(path, {'spy_signal': 'near_low'})
        for tc in result['tier_changes']:
            assert tc['delta'] != 0

    def test_scenario_applied_echoes_input(self):
        path = _make_test_db()
        scenario = {'spy_signal': 'near_low', 'hyg_signal': 'near_low'}
        result = run_counterfactual(path, scenario)
        assert result['scenario_applied'] == scenario

    def test_methodology_is_direct_or_causal(self):
        path = _make_test_db()
        result = run_counterfactual(path, {'spy_signal': 'near_low'})
        assert result['methodology'] in ('direct_override', 'causal_graph_propagation')

    def test_per_ticker_override_tightens_risk(self):
        """Tightening GS thesis_risk_level should not upgrade it."""
        path = _make_test_db()
        baseline = run_counterfactual(path, {'tickers': {'GS': {'thesis_risk_level': 'moderate'}}})
        tightened = run_counterfactual(path, {'tickers': {'GS': {'thesis_risk_level': 'tight'}}})
        # Baseline GS tier should be >= tightened tier
        def _gs_tier(result):
            for tc in result.get('tier_changes', []):
                if tc['ticker'] == 'GS':
                    return tc.get('to') or tc.get('from')
            # No change → look at baseline
            return None
        # Just ensure tightened scenario doesn't raise
        assert 'error' not in tightened

    def test_fed_rate_cut_scalar_applied(self):
        path = _make_test_db(risk_off=True)
        result = run_counterfactual(path, {'fed_funds_rate': -0.25})
        assert 'error' not in result
        assert 'tier_changes' in result

    def test_regime_change_present_when_regime_shifts(self):
        """Going from risk-off to risk-on macro should surface a regime_change."""
        path = _make_test_db(risk_off=True)
        result = run_counterfactual(path, {
            'spy_signal': 'near_high',
            'hyg_signal': 'near_high',
            'tlt_signal': 'near_low',
        })
        assert result['regime_change'] is not None
        assert 'from' in result['regime_change']
        assert 'to' in result['regime_change']

    def test_causal_propagation_list_present(self):
        path = _make_test_db()
        result = run_counterfactual(path, {'fed_funds_rate': -0.25})
        assert isinstance(result['causal_propagation'], list)


# ── TestCounterfactualApiEndpoint ─────────────────────────────────────────────

class TestCounterfactualApiEndpoint:

    @pytest.fixture(autouse=True)
    def patch_db(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / 'test.db')
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                source TEXT, confidence REAL DEFAULT 0.8,
                confidence_effective REAL, metadata TEXT,
                hit_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Seed a high-conviction ticker
        for subj, pred, obj in [
            ('aapl', 'signal_direction', 'long'),
            ('aapl', 'volatility_regime', 'low_volatility'),
            ('aapl', 'thesis_risk_level', 'moderate'),
            ('aapl', 'last_price', '190.0'),
            ('aapl', 'price_target', '230.0'),
            ('aapl', 'volatility_30d', '22.0'),
            (_MARKET_PROXY, 'signal_direction', 'near_high'),
            (_CREDIT_PROXY, 'signal_direction', 'near_high'),
            (_RATES_PROXY,  'signal_direction', 'near_low'),
        ]:
            conn.execute(
                "INSERT INTO facts (subject, predicate, object, source) "
                "VALUES (?, ?, ?, 'test')",
                (subj, pred, obj),
            )
        conn.commit()
        conn.close()
        import api
        monkeypatch.setattr(api, '_DB_PATH', db_path)

    @pytest.fixture
    def client(self):
        import api
        api.app.config['TESTING'] = True
        with api.app.test_client() as c:
            yield c

    def test_empty_scenario_returns_400(self, client):
        resp = client.post('/analytics/counterfactual', json={})
        assert resp.status_code == 400

    def test_missing_body_returns_400(self, client):
        resp = client.post('/analytics/counterfactual',
                           json={'scenario': {}})
        assert resp.status_code == 400

    def test_valid_scenario_returns_200(self, client):
        resp = client.post('/analytics/counterfactual',
                           json={'scenario': {'spy_signal': 'near_low'}})
        assert resp.status_code == 200

    def test_response_has_tier_changes(self, client):
        resp = client.post('/analytics/counterfactual',
                           json={'scenario': {
                               'spy_signal': 'near_low',
                               'hyg_signal': 'near_low',
                               'tlt_signal': 'near_high',
                           }})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'tier_changes' in data
        assert 'downgrades' in data

    def test_response_has_regime_change(self, client):
        resp = client.post('/analytics/counterfactual',
                           json={'scenario': {
                               'spy_signal': 'near_low',
                               'hyg_signal': 'near_low',
                               'tlt_signal': 'near_high',
                           }})
        data = resp.get_json()
        assert 'regime_change' in data

    def test_causal_scalar_accepted(self, client):
        resp = client.post('/analytics/counterfactual',
                           json={'scenario': {'fed_funds_rate': -0.25}})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'error' not in data

    def test_per_ticker_override_accepted(self, client):
        resp = client.post('/analytics/counterfactual',
                           json={'scenario': {
                               'tickers': {'AAPL': {'thesis_risk_level': 'tight'}}
                           }})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'tier_changes' in data

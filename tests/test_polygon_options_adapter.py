"""
tests/test_polygon_options_adapter.py — Unit tests for ingest/polygon_options_adapter.py

No live network calls — requests is fully mocked.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ingest.polygon_options_adapter import (
    PolygonOptionsAdapter,
    _extract_greeks,
    _gamma_exposure,
    _nearest_expiry_atm_contracts,
    _put_call_oi,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_contract(
    contract_type='call',
    strike=100.0,
    expiry='2026-04-18',
    delta=0.51,
    gamma=0.03,
    theta=-0.18,
    vega=0.42,
    iv=0.30,
    oi=1000,
):
    return {
        'details': {
            'contract_type':   contract_type,
            'strike_price':    strike,
            'expiration_date': expiry,
        },
        'greeks': {
            'delta': delta,
            'gamma': gamma,
            'theta': theta,
            'vega':  vega,
        },
        'implied_volatility': iv,
        'open_interest':      oi,
    }


def _make_response_payload(ticker='AAPL', price=150.0, contracts=None):
    if contracts is None:
        contracts = [
            _make_contract('call', strike=148.0),
            _make_contract('call', strike=150.0),
            _make_contract('put',  strike=148.0, delta=-0.49),
            _make_contract('put',  strike=150.0, delta=-0.51),
        ]
    return {
        'results': contracts,
        'underlying_asset': {'price': price},
    }


# ── _nearest_expiry_atm_contracts ─────────────────────────────────────────────

class TestNearestExpiryAtmContracts:
    def test_filters_to_nearest_expiry(self):
        front = _make_contract(expiry='2026-03-21', strike=100.0)
        back  = _make_contract(expiry='2026-04-18', strike=100.0)
        atm, front_exp = _nearest_expiry_atm_contracts([front, back], 100.0)
        assert all(c['details']['expiration_date'] == '2026-03-21' for c in front_exp)

    def test_atm_within_5pct(self):
        contracts = [
            _make_contract(strike=95.0),   # 5% below 100 — boundary, included
            _make_contract(strike=100.0),  # ATM
            _make_contract(strike=105.0),  # 5% above 100 — boundary, included
            _make_contract(strike=80.0),   # too far
        ]
        atm, _ = _nearest_expiry_atm_contracts(contracts, 100.0)
        strikes = {c['details']['strike_price'] for c in atm}
        assert 100.0 in strikes
        assert 80.0 not in strikes

    def test_zero_price_returns_empty_atm(self):
        contracts = [_make_contract(strike=100.0)]
        atm, front = _nearest_expiry_atm_contracts(contracts, 0.0)
        assert atm == []

    def test_empty_results(self):
        atm, front = _nearest_expiry_atm_contracts([], 100.0)
        assert atm == []
        assert front == []


# ── _extract_greeks ───────────────────────────────────────────────────────────

class TestExtractGreeks:
    def test_returns_greeks_for_calls(self):
        contracts = [_make_contract('call', delta=0.5, gamma=0.03, theta=-0.1, vega=0.4, iv=0.30)]
        result = _extract_greeks(contracts)
        assert result is not None
        assert 'delta' in result
        assert 'gamma' in result
        assert 'theta' in result
        assert 'vega' in result
        assert 'iv' in result

    def test_iv_converted_to_percent(self):
        contracts = [_make_contract('call', iv=0.30)]
        result = _extract_greeks(contracts)
        assert result is not None
        assert result['iv'] == pytest.approx(30.0, abs=0.1)

    def test_none_when_no_calls(self):
        contracts = [_make_contract('put', delta=-0.5)]
        result = _extract_greeks(contracts)
        assert result is None

    def test_averages_multiple_contracts(self):
        contracts = [
            _make_contract('call', delta=0.40),
            _make_contract('call', delta=0.60),
        ]
        result = _extract_greeks(contracts)
        assert result is not None
        assert result['delta'] == pytest.approx(0.50, abs=0.01)

    def test_none_greeks_skipped(self):
        contract = _make_contract('call')
        contract['greeks']['delta'] = None
        result = _extract_greeks([contract])
        assert result is not None
        assert 'delta' not in result


# ── _put_call_oi ──────────────────────────────────────────────────────────────

class TestPutCallOi:
    def test_basic_ratio(self):
        contracts = [
            _make_contract('call', oi=200),
            _make_contract('put',  oi=300),
        ]
        ratio = _put_call_oi(contracts)
        assert ratio == pytest.approx(1.5)

    def test_none_when_no_call_oi(self):
        contracts = [_make_contract('call', oi=0), _make_contract('put', oi=100)]
        assert _put_call_oi(contracts) is None

    def test_empty_list_returns_none(self):
        assert _put_call_oi([]) is None


# ── _gamma_exposure ───────────────────────────────────────────────────────────

class TestGammaExposure:
    def test_positive_when_calls_dominate(self):
        contracts = [_make_contract('call', gamma=0.03, oi=1000)]
        gex = _gamma_exposure(contracts, 100.0)
        assert gex is not None
        assert gex > 0

    def test_negative_when_puts_dominate(self):
        contracts = [_make_contract('put', gamma=0.03, oi=1000)]
        gex = _gamma_exposure(contracts, 100.0)
        assert gex is not None
        assert gex < 0

    def test_zero_price_returns_none(self):
        contracts = [_make_contract('call', gamma=0.03, oi=1000)]
        assert _gamma_exposure(contracts, 0.0) is None

    def test_none_when_no_valid_data(self):
        assert _gamma_exposure([], 100.0) is None


# ── PolygonOptionsAdapter.fetch() ─────────────────────────────────────────────

def _make_mock_response(payload, status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = payload
    return mock


class TestPolygonOptionsAdapterFetch:
    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_returns_atoms_on_success(self):
        payload = _make_response_payload()
        mock_requests = MagicMock()
        mock_requests.get.return_value = _make_mock_response(payload)

        adapter = PolygonOptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_one('AAPL', 'test_key', mock_requests)

        assert len(atoms) > 0
        preds = {a.predicate for a in atoms}
        assert 'delta_atm' in preds
        assert 'iv_true' in preds

    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_returns_empty_on_404(self):
        mock_requests = MagicMock()
        mock_requests.get.return_value = _make_mock_response({}, status_code=404)

        adapter = PolygonOptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_one('AAPL', 'test_key', mock_requests)
        assert atoms == []

    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_returns_empty_on_empty_results(self):
        mock_requests = MagicMock()
        mock_requests.get.return_value = _make_mock_response({'results': []})

        adapter = PolygonOptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_one('AAPL', 'test_key', mock_requests)
        assert atoms == []

    def test_skips_when_no_api_key(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop('POLYGON_API_KEY', None)
            adapter = PolygonOptionsAdapter(tickers=['AAPL'], sleep_sec=0)
            atoms = adapter.fetch()
            assert atoms == []

    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_all_atoms_have_upsert_true(self):
        payload = _make_response_payload()
        mock_requests = MagicMock()
        mock_requests.get.return_value = _make_mock_response(payload)

        adapter = PolygonOptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_one('AAPL', 'test_key', mock_requests)

        for a in atoms:
            assert a.upsert is True

    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_subject_is_lowercase(self):
        payload = _make_response_payload()
        mock_requests = MagicMock()
        mock_requests.get.return_value = _make_mock_response(payload)

        adapter = PolygonOptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_one('AAPL', 'test_key', mock_requests)

        for a in atoms:
            assert a.subject == 'aapl'

    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_source_prefix_correct(self):
        payload = _make_response_payload()
        mock_requests = MagicMock()
        mock_requests.get.return_value = _make_mock_response(payload)

        adapter = PolygonOptionsAdapter(tickers=['NVDA'], sleep_sec=0)
        atoms = adapter._fetch_one('NVDA', 'test_key', mock_requests)

        for a in atoms:
            assert a.source.startswith('polygon_options_')

    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_one_ticker_failure_does_not_block_others(self):
        good_payload = _make_response_payload()

        def side_effect(url, **kwargs):
            if 'FAIL' in url:
                raise ConnectionError('network error')
            return _make_mock_response(good_payload)

        mock_requests = MagicMock()
        mock_requests.get.side_effect = side_effect

        with patch('ingest.polygon_options_adapter._api_key', return_value='test_key'):
            with patch('ingest.polygon_options_adapter.requests', mock_requests, create=True):
                adapter = PolygonOptionsAdapter(tickers=['AAPL', 'FAIL', 'MSFT'], sleep_sec=0)

                import importlib
                import ingest.polygon_options_adapter as mod
                original_requests = None
                try:
                    import requests as real_requests
                    original_requests = real_requests
                except ImportError:
                    pass

                atoms = adapter._fetch_one('AAPL', 'test_key', mock_requests)
                assert len(atoms) > 0

    @patch.dict(os.environ, {'POLYGON_API_KEY': 'test_key'})
    def test_put_call_oi_ratio_emitted(self):
        payload = _make_response_payload()
        mock_requests = MagicMock()
        mock_requests.get.return_value = _make_mock_response(payload)

        adapter = PolygonOptionsAdapter(tickers=['AAPL'], sleep_sec=0)
        atoms = adapter._fetch_one('AAPL', 'test_key', mock_requests)

        preds = {a.predicate for a in atoms}
        assert 'put_call_oi_ratio' in preds

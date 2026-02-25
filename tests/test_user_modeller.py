"""
tests/test_user_modeller.py — User Modeller Tests
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from analytics.user_modeller import (
    infer_risk_tolerance,
    infer_sector_affinity,
    infer_holding_style,
    infer_concentration_risk,
    score_portfolio_health,
    build_user_model,
    _read_kb_atoms_for_tickers,
)
from users.user_store import upsert_portfolio


def _tmp_db() -> str:
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT DEFAULT 'test', confidence REAL DEFAULT 0.8,
            confidence_effective REAL, metadata TEXT,
            hit_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    return path


def _insert_atom(conn, subject, predicate, obj):
    conn.execute(
        "INSERT INTO facts (subject, predicate, object) VALUES (?, ?, ?)",
        (subject, predicate, obj),
    )
    conn.commit()


# ── TestInferRiskTolerance ────────────────────────────────────────────────────

class TestInferRiskTolerance:

    def test_empty_holdings_is_moderate(self):
        assert infer_risk_tolerance([], {}) == 'moderate'

    def test_all_tech_is_aggressive(self):
        holdings = [
            {'ticker': 'AAPL', 'sector': 'Technology'},
            {'ticker': 'MSFT', 'sector': 'Technology'},
            {'ticker': 'NVDA', 'sector': 'Technology'},
        ]
        assert infer_risk_tolerance(holdings, {}) == 'aggressive'

    def test_all_utilities_is_conservative(self):
        holdings = [
            {'ticker': 'NEE', 'sector': 'Utilities'},
            {'ticker': 'SO',  'sector': 'Utilities'},
            {'ticker': 'DUK', 'sector': 'Utilities'},
        ]
        assert infer_risk_tolerance(holdings, {}) == 'conservative'

    def test_mixed_sectors_is_moderate(self):
        holdings = [
            {'ticker': 'AAPL', 'sector': 'Technology'},
            {'ticker': 'NEE',  'sector': 'Utilities'},
            {'ticker': 'JPM',  'sector': 'Financials'},
        ]
        assert infer_risk_tolerance(holdings, {}) == 'moderate'

    def test_40pct_threshold_aggressive(self):
        holdings = [
            {'ticker': 'A', 'sector': 'Technology'},
            {'ticker': 'B', 'sector': 'Technology'},
            {'ticker': 'C', 'sector': 'Financials'},
            {'ticker': 'D', 'sector': 'Healthcare'},
            {'ticker': 'E', 'sector': 'Energy'},
        ]
        assert infer_risk_tolerance(holdings, {}) == 'aggressive'

    def test_no_sector_falls_back_to_volatility(self):
        holdings = [{'ticker': 'AAPL'}, {'ticker': 'MSFT'}]
        kb_atoms = {
            'aapl': {'volatility_30d': '45.0'},
            'msft': {'volatility_30d': '40.0'},
        }
        assert infer_risk_tolerance(holdings, kb_atoms) == 'aggressive'

    def test_low_vol_no_sector_is_conservative(self):
        holdings = [{'ticker': 'NEE'}, {'ticker': 'SO'}]
        kb_atoms = {
            'nee': {'volatility_30d': '10.0'},
            'so':  {'volatility_30d': '12.0'},
        }
        assert infer_risk_tolerance(holdings, kb_atoms) == 'conservative'


# ── TestInferSectorAffinity ───────────────────────────────────────────────────

class TestInferSectorAffinity:

    def test_empty_holdings_returns_empty(self):
        assert infer_sector_affinity([]) == []

    def test_sectors_with_two_or_more_holdings(self):
        holdings = [
            {'ticker': 'A', 'sector': 'Technology'},
            {'ticker': 'B', 'sector': 'Technology'},
            {'ticker': 'C', 'sector': 'Financials'},
        ]
        result = infer_sector_affinity(holdings)
        assert 'technology' in result
        assert 'financials' not in result

    def test_no_duplicate_sectors_falls_back_to_top2(self):
        holdings = [
            {'ticker': 'A', 'sector': 'Technology'},
            {'ticker': 'B', 'sector': 'Financials'},
            {'ticker': 'C', 'sector': 'Healthcare'},
        ]
        result = infer_sector_affinity(holdings)
        assert len(result) <= 2

    def test_missing_sector_ignored(self):
        holdings = [
            {'ticker': 'A', 'sector': ''},
            {'ticker': 'B'},
            {'ticker': 'C', 'sector': 'Technology'},
            {'ticker': 'D', 'sector': 'Technology'},
        ]
        result = infer_sector_affinity(holdings)
        assert 'technology' in result
        assert '' not in result


# ── TestInferHoldingStyle ─────────────────────────────────────────────────────

class TestInferHoldingStyle:

    def test_empty_is_mixed(self):
        assert infer_holding_style([], {}) == 'mixed'

    def test_low_upside_is_value(self):
        holdings = [{'ticker': 'AAPL'}, {'ticker': 'MSFT'}]
        kb_atoms = {
            'aapl': {'upside_pct': '5.0', 'conviction_tier': 'medium'},
            'msft': {'upside_pct': '7.0', 'conviction_tier': 'medium'},
        }
        assert infer_holding_style(holdings, kb_atoms) == 'value'

    def test_high_conviction_is_momentum(self):
        holdings = [{'ticker': 'AAPL'}, {'ticker': 'MSFT'}]
        kb_atoms = {
            'aapl': {'upside_pct': '25.0', 'conviction_tier': 'high'},
            'msft': {'upside_pct': '20.0', 'conviction_tier': 'high'},
        }
        assert infer_holding_style(holdings, kb_atoms) == 'momentum'

    def test_mixed_fallback(self):
        holdings = [{'ticker': 'AAPL'}, {'ticker': 'MSFT'}]
        kb_atoms = {
            'aapl': {'upside_pct': '12.0', 'conviction_tier': 'medium'},
            'msft': {'upside_pct': '11.0', 'conviction_tier': 'low'},
        }
        assert infer_holding_style(holdings, kb_atoms) == 'mixed'


# ── TestInferConcentrationRisk ────────────────────────────────────────────────

class TestInferConcentrationRisk:

    def test_five_sectors_is_diversified(self):
        holdings = [
            {'ticker': 'A', 'sector': 'Technology'},
            {'ticker': 'B', 'sector': 'Financials'},
            {'ticker': 'C', 'sector': 'Healthcare'},
            {'ticker': 'D', 'sector': 'Utilities'},
            {'ticker': 'E', 'sector': 'Energy'},
        ]
        assert infer_concentration_risk(holdings) == 'diversified'

    def test_four_sectors_is_concentrated(self):
        holdings = [
            {'ticker': 'A', 'sector': 'Technology'},
            {'ticker': 'B', 'sector': 'Financials'},
            {'ticker': 'C', 'sector': 'Healthcare'},
            {'ticker': 'D', 'sector': 'Utilities'},
        ]
        assert infer_concentration_risk(holdings) == 'concentrated'

    def test_no_sectors_is_concentrated(self):
        holdings = [{'ticker': 'A'}, {'ticker': 'B'}]
        assert infer_concentration_risk(holdings) == 'concentrated'


# ── TestScorePortfolioHealth ──────────────────────────────────────────────────

class TestScorePortfolioHealth:

    def test_avg_conviction_threshold(self):
        holdings = [{'ticker': 'AAPL'}, {'ticker': 'MSFT'}]
        kb_atoms = {
            'aapl': {'conviction_tier': 'high'},
            'msft': {'conviction_tier': 'medium'},
        }
        health = score_portfolio_health(holdings, kb_atoms)
        assert health['avg_conviction_threshold'] == pytest.approx(0.835, abs=0.01)

    def test_holdings_at_risk_detected(self):
        holdings = [{'ticker': 'AAPL'}, {'ticker': 'MSFT'}]
        kb_atoms = {
            'aapl': {'conviction_tier': 'avoid'},
            'msft': {'conviction_tier': 'high'},
        }
        health = score_portfolio_health(holdings, kb_atoms)
        assert 'AAPL' in health['holdings_at_risk']
        assert 'MSFT' not in health['holdings_at_risk']

    def test_holdings_performing_detected(self):
        holdings = [{'ticker': 'AAPL'}]
        kb_atoms = {'aapl': {'conviction_tier': 'high', 'upside_pct': '25.0'}}
        health = score_portfolio_health(holdings, kb_atoms)
        assert 'AAPL' in health['holdings_performing']

    def test_portfolio_beta_computed(self):
        holdings = [{'ticker': 'AAPL'}]
        kb_atoms = {'aapl': {'volatility_30d': '40.0', 'conviction_tier': 'high'}}
        health = score_portfolio_health(holdings, kb_atoms)
        assert health['portfolio_beta'] == pytest.approx(2.0, abs=0.1)

    def test_no_atoms_returns_none_values(self):
        holdings = [{'ticker': 'AAPL'}]
        health = score_portfolio_health(holdings, {})
        assert health['avg_conviction_threshold'] is None
        assert health['portfolio_beta'] is None


# ── TestBuildUserModel ────────────────────────────────────────────────────────

class TestBuildUserModel:

    def test_builds_model_from_portfolio(self):
        path = _tmp_db()
        upsert_portfolio(path, 'u_model_1', [
            {'ticker': 'AAPL', 'sector': 'Technology'},
            {'ticker': 'MSFT', 'sector': 'Technology'},
            {'ticker': 'NVDA', 'sector': 'Technology'},
        ])
        conn = sqlite3.connect(path)
        _insert_atom(conn, 'aapl', 'conviction_tier', 'high')
        _insert_atom(conn, 'msft', 'conviction_tier', 'high')
        _insert_atom(conn, 'nvda', 'conviction_tier', 'medium')
        conn.close()

        model = build_user_model('u_model_1', path)
        assert model['risk_tolerance'] == 'aggressive'
        assert 'technology' in model['sector_affinity']

    def test_empty_portfolio_returns_empty_model(self):
        path = _tmp_db()
        upsert_portfolio(path, 'u_model_empty', [])
        model = build_user_model('u_model_empty', path)
        assert model['risk_tolerance'] == 'moderate'
        assert model['concentration_risk'] == 'concentrated'

    def test_model_persisted_to_db(self):
        from users.user_store import get_user_model
        path = _tmp_db()
        upsert_portfolio(path, 'u_model_2', [
            {'ticker': 'JPM', 'sector': 'Financials'},
            {'ticker': 'GS',  'sector': 'Financials'},
        ])
        build_user_model('u_model_2', path)
        m = get_user_model(path, 'u_model_2')
        assert m is not None
        assert m['concentration_risk'] == 'concentrated'

    def test_diversified_portfolio(self):
        path = _tmp_db()
        upsert_portfolio(path, 'u_model_3', [
            {'ticker': 'A', 'sector': 'Technology'},
            {'ticker': 'B', 'sector': 'Financials'},
            {'ticker': 'C', 'sector': 'Healthcare'},
            {'ticker': 'D', 'sector': 'Utilities'},
            {'ticker': 'E', 'sector': 'Energy'},
        ])
        model = build_user_model('u_model_3', path)
        assert model['concentration_risk'] == 'diversified'

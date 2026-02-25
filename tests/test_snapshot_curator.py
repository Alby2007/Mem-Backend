"""
tests/test_snapshot_curator.py — Snapshot Curator Tests
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from analytics.snapshot_curator import (
    curate_snapshot,
    CuratedSnapshot,
    OpportunityCard,
    _score_opportunity,
    _build_opportunity_card,
    _regime_implication,
    _macro_summary,
    _portfolio_health_section,
)
from users.user_store import (
    upsert_portfolio, update_preferences, upsert_user_model, ensure_user_tables
)


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


def _ins(conn, subject, predicate, obj):
    conn.execute(
        "INSERT INTO facts (subject, predicate, object) VALUES (?, ?, ?)",
        (subject, predicate, obj),
    )
    conn.commit()


def _make_full_db() -> str:
    """DB with two high-conviction tickers and risk-on macro."""
    path = _tmp_db()
    conn = sqlite3.connect(path)
    ensure_user_tables(conn)
    conn.commit()
    conn.close()

    conn = sqlite3.connect(path)
    # AAPL — high conviction
    for pred, val in [
        ('conviction_tier', 'high'),
        ('upside_pct', '25.0'),
        ('invalidation_distance', '-20.0'),
        ('position_size_pct', '4.5'),
        ('signal_quality', 'strong'),
        ('thesis_risk_level', 'moderate'),
        ('macro_confirmation', 'confirmed'),
        ('sector', 'Technology'),
    ]:
        _ins(conn, 'aapl', pred, val)

    # GS — medium conviction
    for pred, val in [
        ('conviction_tier', 'medium'),
        ('upside_pct', '12.0'),
        ('invalidation_distance', '-15.0'),
        ('position_size_pct', '2.0'),
        ('signal_quality', 'confirmed'),
        ('thesis_risk_level', 'moderate'),
        ('macro_confirmation', 'partial'),
        ('sector', 'Financials'),
    ]:
        _ins(conn, 'gs', pred, val)

    # Market regime
    _ins(conn, 'market', 'market_regime', 'risk_on_expansion')
    conn.close()
    return path


# ── TestScoreOpportunity ──────────────────────────────────────────────────────

class TestScoreOpportunity:

    def _atoms(self, **kwargs):
        base = {
            'conviction_tier': 'high',
            'sector': 'Technology',
            'options_regime': 'normal',
            'thesis_risk_level': 'moderate',
            'macro_confirmation': 'confirmed',
        }
        base.update(kwargs)
        return base

    def test_high_tier_base_score_is_1(self):
        score = _score_opportunity('aapl', self._atoms(), [], 'moderate', [])
        assert score >= 1.0

    def test_sector_affinity_boost(self):
        score_with    = _score_opportunity('aapl', self._atoms(), ['technology'], 'moderate', [])
        score_without = _score_opportunity('aapl', self._atoms(), [], 'moderate', [])
        assert score_with > score_without

    def test_avoid_tier_is_negative(self):
        atoms = self._atoms(conviction_tier='avoid')
        score = _score_opportunity('aapl', atoms, [], 'moderate', [])
        assert score < 0

    def test_compressed_options_boost(self):
        score_normal     = _score_opportunity('aapl', self._atoms(options_regime='normal'), [], 'moderate', [])
        score_compressed = _score_opportunity('aapl', self._atoms(options_regime='compressed'), [], 'moderate', [])
        assert score_compressed > score_normal

    def test_macro_confirmed_boost(self):
        score_conf   = _score_opportunity('aapl', self._atoms(macro_confirmation='confirmed'), [], 'moderate', [])
        score_unconf = _score_opportunity('aapl', self._atoms(macro_confirmation='unconfirmed'), [], 'moderate', [])
        assert score_conf > score_unconf


# ── TestBuildOpportunityCard ──────────────────────────────────────────────────

class TestBuildOpportunityCard:

    def _atoms(self):
        return {
            'conviction_tier': 'high',
            'upside_pct': '25.0',
            'invalidation_distance': '-20.0',
            'position_size_pct': '4.5',
            'sector': 'Technology',
            'options_regime': 'normal',
        }

    def test_returns_opportunity_card(self):
        card = _build_opportunity_card('AAPL', self._atoms(), ['technology'], [])
        assert isinstance(card, OpportunityCard)

    def test_ticker_uppercased(self):
        card = _build_opportunity_card('aapl', self._atoms(), [], [])
        assert card.ticker == 'AAPL'

    def test_asymmetry_ratio_computed(self):
        card = _build_opportunity_card('AAPL', self._atoms(), [], [])
        assert card.asymmetry_ratio == pytest.approx(1.25, abs=0.1)

    def test_urgency_high_normal_is_immediate(self):
        card = _build_opportunity_card('AAPL', self._atoms(), [], [])
        assert card.urgency == 'immediate'

    def test_sector_affinity_in_relevance(self):
        card = _build_opportunity_card('AAPL', self._atoms(), ['technology'], [])
        assert 'technology' in card.relevance_reason.lower() or 'Technology' in card.relevance_reason

    def test_catalyst_atom_used_for_thesis(self):
        atoms = self._atoms()
        atoms['catalyst'] = 'Strong AI revenue growth expected'
        card = _build_opportunity_card('AAPL', atoms, [], [])
        assert 'AI' in card.thesis

    def test_zero_invalidation_no_crash(self):
        atoms = self._atoms()
        atoms['invalidation_distance'] = '0'
        card = _build_opportunity_card('AAPL', atoms, [], [])
        assert card.asymmetry_ratio == 0.0


# ── TestRegimeImplication ─────────────────────────────────────────────────────

class TestRegimeImplication:

    def test_risk_on_expansion(self):
        result = _regime_implication('risk_on_expansion', ['technology'])
        assert 'favour' in result.lower() or 'expansion' in result.lower()

    def test_risk_off_contraction(self):
        result = _regime_implication('risk_off_contraction', ['financials'])
        assert 'risk' in result.lower() or 'defensive' in result.lower()

    def test_no_sector_affinity(self):
        result = _regime_implication('risk_on_expansion', [])
        assert 'Regime' in result or 'expansion' in result.lower()

    def test_unknown_regime(self):
        result = _regime_implication('some_unknown_regime', [])
        assert 'some_unknown_regime' in result or 'Regime' in result


# ── TestMacroSummary ──────────────────────────────────────────────────────────

class TestMacroSummary:

    def test_empty_macro_returns_no_data(self):
        result = _macro_summary({})
        assert 'No macro' in result

    def test_fed_stance_included(self):
        result = _macro_summary({'central_bank_stance': 'neutral_to_restrictive'})
        assert 'Fed' in result

    def test_yield_curve_included(self):
        result = _macro_summary({'yield_curve_spread': '+60bps'})
        assert 'yield' in result.lower() or '+60bps' in result


# ── TestPortfolioHealthSection ────────────────────────────────────────────────

class TestPortfolioHealthSection:

    def test_at_risk_detected(self):
        holdings = [{'ticker': 'AAPL', 'avg_cost': 150}]
        atoms = {'aapl': {'conviction_tier': 'avoid', 'upside_pct': '-5.0'}}
        health = _portfolio_health_section(holdings, atoms)
        assert 'AAPL' in health['holdings_at_risk']

    def test_performing_detected(self):
        holdings = [{'ticker': 'MSFT', 'avg_cost': 400}]
        atoms = {'msft': {'conviction_tier': 'high', 'upside_pct': '25.0'}}
        health = _portfolio_health_section(holdings, atoms)
        assert 'MSFT' in health['holdings_performing']

    def test_empty_holdings(self):
        health = _portfolio_health_section([], {})
        assert health['summary'] == []
        assert health['holdings_at_risk'] == []


# ── TestCurateSnapshot ────────────────────────────────────────────────────────

class TestCurateSnapshot:

    def test_returns_curated_snapshot(self):
        path = _make_full_db()
        update_preferences(path, 'u1', selected_sectors=['technology'], selected_risk='moderate')
        snap = curate_snapshot('u1', path)
        assert isinstance(snap, CuratedSnapshot)

    def test_has_required_fields(self):
        path = _make_full_db()
        update_preferences(path, 'u2', selected_sectors=['technology'])
        snap = curate_snapshot('u2', path)
        assert snap.user_id == 'u2'
        assert snap.generated_at
        assert snap.market_regime == 'risk_on_expansion'
        assert isinstance(snap.top_opportunities, list)

    def test_top_opportunities_are_opportunity_cards(self):
        path = _make_full_db()
        update_preferences(path, 'u3', selected_sectors=['technology'])
        snap = curate_snapshot('u3', path)
        for opp in snap.top_opportunities:
            assert isinstance(opp, OpportunityCard)

    def test_max_5_opportunities(self):
        path = _make_full_db()
        update_preferences(path, 'u4')
        snap = curate_snapshot('u4', path)
        assert len(snap.top_opportunities) <= 5

    def test_portfolio_path_populates_health(self):
        path = _make_full_db()
        upsert_portfolio(path, 'u5', [{'ticker': 'AAPL', 'avg_cost': 150}])
        upsert_user_model(path, 'u5', 'moderate', ['technology'], 0.8, 'momentum', 1.1, 'concentrated')
        snap = curate_snapshot('u5', path)
        assert len(snap.portfolio_summary) > 0

    def test_fallback_path_empty_portfolio_health(self):
        path = _make_full_db()
        update_preferences(path, 'u6', selected_sectors=['technology'], selected_risk='moderate')
        snap = curate_snapshot('u6', path)
        assert snap.portfolio_summary == []

    def test_conservative_user_only_sees_high_conviction(self):
        path = _make_full_db()
        update_preferences(path, 'u7', selected_risk='conservative')
        snap = curate_snapshot('u7', path)
        for opp in snap.top_opportunities:
            assert opp.conviction_tier == 'high'

    def test_avoid_tickers_excluded_from_opportunities(self):
        path = _make_full_db()
        conn = sqlite3.connect(path)
        _ins(conn, 'bad_stock', 'conviction_tier', 'avoid')
        conn.close()
        update_preferences(path, 'u8')
        snap = curate_snapshot('u8', path)
        tickers = [o.ticker for o in snap.top_opportunities]
        assert 'BAD_STOCK' not in tickers

    def test_avoid_tickers_in_portfolio_shown_in_avoid_list(self):
        path = _make_full_db()
        conn = sqlite3.connect(path)
        _ins(conn, 'badname', 'conviction_tier', 'avoid')
        conn.close()
        upsert_portfolio(path, 'u9', [{'ticker': 'BADNAME'}])
        upsert_user_model(path, 'u9', 'moderate', [], None, 'mixed', None, 'concentrated')
        snap = curate_snapshot('u9', path)
        assert 'BADNAME' in snap.opportunities_to_avoid

    def test_regime_implication_personalised(self):
        path = _make_full_db()
        update_preferences(path, 'u10', selected_sectors=['technology'])
        snap = curate_snapshot('u10', path)
        assert snap.regime_implication  # non-empty

    def test_no_user_no_crash(self):
        path = _make_full_db()
        snap = curate_snapshot('brand_new_user', path)
        assert isinstance(snap, CuratedSnapshot)

    def test_empty_kb_no_crash(self):
        path = _tmp_db()
        conn = sqlite3.connect(path)
        ensure_user_tables(conn)
        conn.commit()
        conn.close()
        update_preferences(path, 'u11')
        snap = curate_snapshot('u11', path)
        assert snap.top_opportunities == []

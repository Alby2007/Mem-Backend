"""
tests/test_portfolio.py — Unit tests for analytics/portfolio.py

Tests aggregation math, sector weights, macro alignment, avoid book,
total_position_pct note, top_conviction sorting, and edge cases.

No live DB required — tests build an in-memory SQLite DB.
"""
import os
import sqlite3
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from analytics.portfolio import build_portfolio_summary


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_db(rows: list) -> str:
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            confidence REAL DEFAULT 0.5, source TEXT DEFAULT 'test',
            timestamp TEXT DEFAULT '2026-01-01T00:00:00+00:00'
        )
    """)
    conn.executemany(
        "INSERT INTO facts (subject, predicate, object) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()
    return path


def _ticker_rows(ticker, conviction, signal_quality, sector='technology',
                 position_size=2.0, upside=20.0, macro='partial',
                 risk='moderate', return_1m=3.0, return_vs_spy=1.0):
    return [
        (ticker, 'conviction_tier',    conviction),
        (ticker, 'signal_quality',     signal_quality),
        (ticker, 'sector',             sector),
        (ticker, 'position_size_pct',  str(position_size)),
        (ticker, 'upside_pct',         str(upside)),
        (ticker, 'macro_confirmation', macro),
        (ticker, 'thesis_risk_level',  risk),
        (ticker, 'return_1m',          str(return_1m)),
        (ticker, 'return_vs_spy_1m',   str(return_vs_spy)),
        (ticker, 'last_price',         '100.0'),
    ]


# ── Test fixtures ─────────────────────────────────────────────────────────────

def _standard_db():
    rows = []
    # 3 high conviction
    rows.extend(_ticker_rows('aapl', 'high', 'strong', 'technology', 3.5, 30.0, 'partial'))
    rows.extend(_ticker_rows('msft', 'high', 'strong', 'technology', 3.0, 25.0, 'partial'))
    rows.extend(_ticker_rows('jpm',  'high', 'strong', 'financials', 3.3, 20.0, 'partial'))
    # 2 medium conviction
    rows.extend(_ticker_rows('nvda', 'medium', 'confirmed', 'technology', 1.5, 40.0, 'partial'))
    rows.extend(_ticker_rows('crm',  'medium', 'strong',    'technology', 1.2, 35.0, 'unconfirmed'))
    # 3 low conviction
    rows.extend(_ticker_rows('xom', 'low', 'weak', 'energy',    1.0, 5.0, 'unconfirmed'))
    rows.extend(_ticker_rows('ko',  'low', 'weak', 'consumer',  1.0, 3.0, 'unconfirmed'))
    rows.extend(_ticker_rows('pg',  'low', 'weak', 'consumer',  1.0, 4.0, 'unconfirmed'))
    # 2 avoid
    rows.extend(_ticker_rows('duk', 'avoid', 'weak', 'utilities', 0.0, 6.0))
    rows.extend(_ticker_rows('so',  'avoid', 'weak', 'utilities', 0.0, 3.0))
    return _make_db(rows)


# ── Structure tests ───────────────────────────────────────────────────────────

class TestPortfolioStructure:
    def setup_method(self):
        self.db_path = _standard_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_returns_dict(self):
        result = build_portfolio_summary(self.db_path)
        assert isinstance(result, dict)

    def test_top_level_keys(self):
        result = build_portfolio_summary(self.db_path)
        for key in ('as_of', 'long_book', 'avoid_book', 'sector_weights',
                    'macro_alignment', 'top_conviction', 'all_tickers'):
            assert key in result, f"Missing key: {key}"

    def test_as_of_present(self):
        result = build_portfolio_summary(self.db_path)
        assert result['as_of']

    def test_long_book_keys(self):
        result = build_portfolio_summary(self.db_path)
        lb = result['long_book']
        for key in ('tickers', 'total_position_pct', 'total_position_pct_note',
                    'avg_conviction_weighted_upside', 'conviction_tier', 'signal_quality'):
            assert key in lb, f"Missing long_book key: {key}"

    def test_avoid_book_keys(self):
        result = build_portfolio_summary(self.db_path)
        ab = result['avoid_book']
        assert 'tickers' in ab
        assert 'names' in ab


# ── Long book aggregation ─────────────────────────────────────────────────────

class TestLongBook:
    def setup_method(self):
        self.db_path = _standard_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_long_book_count(self):
        result = build_portfolio_summary(self.db_path)
        assert result['long_book']['tickers'] == 8  # 3+2+3, not avoid

    def test_avoid_excluded_from_long_book(self):
        result = build_portfolio_summary(self.db_path)
        # 2 avoid tickers should not be in long_book count
        assert result['long_book']['tickers'] == 8

    def test_total_position_pct(self):
        result = build_portfolio_summary(self.db_path)
        # 3.5+3.0+3.3+1.5+1.2+1.0+1.0+1.0 = 15.5
        assert result['long_book']['total_position_pct'] == pytest.approx(15.5, abs=0.1)

    def test_total_position_pct_note_present(self):
        result = build_portfolio_summary(self.db_path)
        note = result['long_book']['total_position_pct_note']
        assert note
        assert 'ranked menu' in note.lower() or '100%' in note or 'not all' in note.lower()

    def test_total_position_pct_note_explains_over_100(self):
        # Note must address that it exceeds 100%
        result = build_portfolio_summary(self.db_path)
        note = result['long_book']['total_position_pct_note']
        assert 'simultaneously' in note.lower() or '100' in note

    def test_conviction_tier_breakdown(self):
        result = build_portfolio_summary(self.db_path)
        ct = result['long_book']['conviction_tier']
        assert ct.get('high', 0) == 3
        assert ct.get('medium', 0) == 2
        assert ct.get('low', 0) == 3

    def test_signal_quality_breakdown(self):
        result = build_portfolio_summary(self.db_path)
        sq = result['long_book']['signal_quality']
        assert sq.get('strong', 0) >= 2
        assert sq.get('weak', 0) >= 3

    def test_weighted_upside_computed(self):
        result = build_portfolio_summary(self.db_path)
        assert result['long_book']['avg_conviction_weighted_upside'] is not None
        # high names (30, 25, 20) weighted by 3.5, 3.0, 3.3 should dominate
        assert result['long_book']['avg_conviction_weighted_upside'] > 10


# ── Avoid book ────────────────────────────────────────────────────────────────

class TestAvoidBook:
    def setup_method(self):
        self.db_path = _standard_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_avoid_count(self):
        result = build_portfolio_summary(self.db_path)
        assert result['avoid_book']['tickers'] == 2

    def test_avoid_names(self):
        result = build_portfolio_summary(self.db_path)
        names = result['avoid_book']['names']
        assert 'DUK' in names
        assert 'SO' in names

    def test_avoid_names_sorted(self):
        result = build_portfolio_summary(self.db_path)
        names = result['avoid_book']['names']
        assert names == sorted(names)


# ── Sector weights ────────────────────────────────────────────────────────────

class TestSectorWeights:
    def setup_method(self):
        self.db_path = _standard_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_sectors_present(self):
        result = build_portfolio_summary(self.db_path)
        sw = result['sector_weights']
        assert 'technology' in sw
        assert 'financials' in sw
        assert 'energy' in sw

    def test_technology_dominates(self):
        result = build_portfolio_summary(self.db_path)
        sw = result['sector_weights']
        # 4 tech names: aapl, msft, nvda, crm
        assert sw['technology']['tickers'] == 4

    def test_avoid_excluded_from_sectors(self):
        result = build_portfolio_summary(self.db_path)
        sw = result['sector_weights']
        # utilities (duk, so) are avoid with position_size=0 — should not dominate
        # They may appear with weight 0 or not at all
        if 'utilities' in sw:
            assert sw['utilities']['position_pct_sum'] == pytest.approx(0.0, abs=0.01)

    def test_weight_pct_sums_to_100(self):
        result = build_portfolio_summary(self.db_path)
        sw = result['sector_weights']
        total = sum(s['weight_pct'] for s in sw.values())
        assert total == pytest.approx(100.0, abs=1.0)

    def test_sector_weight_structure(self):
        result = build_portfolio_summary(self.db_path)
        for sector, data in result['sector_weights'].items():
            assert 'position_pct_sum' in data
            assert 'weight_pct' in data
            assert 'tickers' in data


# ── Macro alignment ───────────────────────────────────────────────────────────

class TestMacroAlignment:
    def setup_method(self):
        self.db_path = _standard_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_macro_alignment_keys(self):
        result = build_portfolio_summary(self.db_path)
        ma = result['macro_alignment']
        assert isinstance(ma, dict)

    def test_partial_count(self):
        result = build_portfolio_summary(self.db_path)
        # 3 high + 1 medium = 4 partial
        assert result['macro_alignment'].get('partial', 0) == 4

    def test_unconfirmed_count(self):
        result = build_portfolio_summary(self.db_path)
        # 1 medium (crm) + 3 low = 4 unconfirmed
        assert result['macro_alignment'].get('unconfirmed', 0) == 4


# ── Top conviction ────────────────────────────────────────────────────────────

class TestTopConviction:
    def setup_method(self):
        self.db_path = _standard_db()

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_top_conviction_is_list(self):
        result = build_portfolio_summary(self.db_path)
        assert isinstance(result['top_conviction'], list)

    def test_top_conviction_max_20(self):
        result = build_portfolio_summary(self.db_path)
        assert len(result['top_conviction']) <= 20

    def test_high_tier_first(self):
        result = build_portfolio_summary(self.db_path)
        tc = result['top_conviction']
        # First entries should be high conviction
        assert tc[0]['conviction_tier'] == 'high'

    def test_top_conviction_fields(self):
        result = build_portfolio_summary(self.db_path)
        for entry in result['top_conviction']:
            for field in ('ticker', 'conviction_tier', 'signal_quality',
                          'position_size_pct', 'upside_pct'):
                assert field in entry

    def test_sorted_by_upside_within_tier(self):
        result = build_portfolio_summary(self.db_path)
        tc = result['top_conviction']
        high_entries = [e for e in tc if e['conviction_tier'] == 'high']
        upsides = [e['upside_pct'] for e in high_entries if e['upside_pct'] is not None]
        assert upsides == sorted(upsides, reverse=True)


# ── Edge cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_db(self):
        rows = []
        db_path = _make_db(rows)
        try:
            result = build_portfolio_summary(db_path)
            assert result['long_book']['tickers'] == 0
            assert result['avoid_book']['tickers'] == 0
            assert result['top_conviction'] == []
        finally:
            os.unlink(db_path)

    def test_all_avoid(self):
        rows = []
        rows.extend(_ticker_rows('duk', 'avoid', 'weak', position_size=0.0))
        rows.extend(_ticker_rows('so',  'avoid', 'weak', position_size=0.0))
        db_path = _make_db(rows)
        try:
            result = build_portfolio_summary(db_path)
            assert result['long_book']['tickers'] == 0
            assert result['avoid_book']['tickers'] == 2
            assert result['long_book']['avg_conviction_weighted_upside'] is None
        finally:
            os.unlink(db_path)

    def test_no_position_size_atoms(self):
        rows = [
            ('aapl', 'conviction_tier', 'high'),
            ('aapl', 'signal_quality',  'strong'),
            ('aapl', 'upside_pct',      '25.0'),
            # No position_size_pct atom
        ]
        db_path = _make_db(rows)
        try:
            result = build_portfolio_summary(db_path)
            assert result['long_book']['tickers'] == 1
            assert result['long_book']['total_position_pct'] == pytest.approx(0.0, abs=0.01)
        finally:
            os.unlink(db_path)

    def test_no_sector_atoms(self):
        rows = [
            ('aapl', 'conviction_tier',   'high'),
            ('aapl', 'signal_quality',    'strong'),
            ('aapl', 'position_size_pct', '2.0'),
        ]
        db_path = _make_db(rows)
        try:
            result = build_portfolio_summary(db_path)
            # Should not crash; sector may appear as 'unknown' or not at all
            assert isinstance(result['sector_weights'], dict)
        finally:
            os.unlink(db_path)

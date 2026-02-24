"""
tests/test_backtest.py — Unit tests for analytics/backtest.py

Tests cohort grouping, return math, alpha threshold, portfolio weighting,
methodology disclaimer, and edge cases (missing data, empty cohorts).

No live DB required — tests build an in-memory SQLite DB.
"""
import math
import sqlite3
import tempfile
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from analytics.backtest import run_backtest, ALPHA_THRESHOLD_PP, _cohort_stats, _safe_float


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_db(rows: list) -> str:
    """Create a temp SQLite DB with a facts table populated from rows."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT,
            predicate TEXT,
            object TEXT,
            confidence REAL DEFAULT 0.5,
            source TEXT DEFAULT 'test',
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


def _base_rows(ticker, conviction, signal_quality, return_1m,
               position_size=2.0, upside=20.0, return_vs_spy=1.0,
               return_1w=0.5, return_3m=5.0):
    return [
        (ticker, 'conviction_tier',    conviction),
        (ticker, 'signal_quality',     signal_quality),
        (ticker, 'return_1m',          str(return_1m)),
        (ticker, 'return_1w',          str(return_1w)),
        (ticker, 'return_3m',          str(return_3m)),
        (ticker, 'position_size_pct',  str(position_size)),
        (ticker, 'upside_pct',         str(upside)),
        (ticker, 'return_vs_spy_1m',   str(return_vs_spy)),
        (ticker, 'return_vs_spy_3m',   str(return_vs_spy + 0.5)),
        (ticker, 'sector',             'technology'),
    ]


# ── _cohort_stats ─────────────────────────────────────────────────────────────

class TestCohortStats:
    def test_empty(self):
        s = _cohort_stats([])
        assert s['n'] == 0
        assert s['mean_return'] is None

    def test_single(self):
        s = _cohort_stats([5.0])
        assert s['n'] == 1
        assert s['mean_return'] == 5.0
        assert s['hit_rate'] == 1.0

    def test_basic_stats(self):
        s = _cohort_stats([1.0, 3.0, 5.0])
        assert s['n'] == 3
        assert s['mean_return'] == 3.0
        assert s['median_return'] == 3.0
        assert s['hit_rate'] == 1.0
        assert s['min_return'] == 1.0
        assert s['max_return'] == 5.0

    def test_hit_rate(self):
        s = _cohort_stats([-2.0, -1.0, 1.0, 3.0])
        assert s['hit_rate'] == pytest.approx(0.5)

    def test_all_negative(self):
        s = _cohort_stats([-3.0, -1.0, -2.0])
        assert s['hit_rate'] == 0.0
        assert s['mean_return'] == pytest.approx(-2.0)


# ── _safe_float ───────────────────────────────────────────────────────────────

class TestSafeFloat:
    def test_numeric_string(self):
        assert _safe_float('3.14') == pytest.approx(3.14)

    def test_none(self):
        assert _safe_float(None) is None

    def test_non_numeric(self):
        assert _safe_float('n/a') is None

    def test_float(self):
        assert _safe_float(1.5) == pytest.approx(1.5)


# ── run_backtest ──────────────────────────────────────────────────────────────

class TestRunBacktest:
    def setup_method(self):
        rows = []
        # 3 high+strong names with good returns
        for i, (tkr, ret) in enumerate([('aapl', 4.0), ('msft', 5.0), ('nvda', 6.0)]):
            rows.extend(_base_rows(tkr, 'high', 'strong', ret, return_vs_spy=2.0))
        # 2 medium names
        for tkr, ret in [('crm', 2.0), ('snow', 3.0)]:
            rows.extend(_base_rows(tkr, 'medium', 'confirmed', ret, return_vs_spy=0.5))
        # 4 low+weak names with poor returns
        for tkr, ret in [('xom', 0.5), ('jnj', 0.2), ('ko', -0.3), ('pg', 0.1)]:
            rows.extend(_base_rows(tkr, 'low', 'weak', ret, position_size=1.5,
                                   return_vs_spy=-0.5))
        # 1 avoid name
        rows.extend(_base_rows('wmt', 'avoid', 'weak', -1.0, position_size=0.0))
        self.db_path = _make_db(rows)

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_returns_dict(self):
        result = run_backtest(self.db_path, window='1m')
        assert isinstance(result, dict)

    def test_methodology_field(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['methodology'] == 'point_in_time_snapshot'
        assert 'methodology_note' in result
        assert 'walk-forward' in result['methodology_note'].lower()

    def test_alpha_threshold_documented(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['alpha_threshold_pp'] == ALPHA_THRESHOLD_PP
        assert ALPHA_THRESHOLD_PP == 1.0  # pre-committed value

    def test_cohorts_present(self):
        result = run_backtest(self.db_path, window='1m')
        for cohort in ('high_all', 'high_strong', 'medium_all', 'low_all', 'avoid'):
            assert cohort in result['cohorts']

    def test_high_cohort_counts(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['cohorts']['high_all']['n'] == 3
        assert result['cohorts']['high_strong']['n'] == 3

    def test_low_cohort_counts(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['cohorts']['low_all']['n'] == 4

    def test_avoid_cohort(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['cohorts']['avoid']['n'] == 1

    def test_alpha_signal_fires(self):
        # high mean ≈ 5.0, low mean ≈ 0.125 → diff ≈ 4.875 > 1.0
        result = run_backtest(self.db_path, window='1m')
        assert result['alpha_signal'] is True
        assert 'PASS' in result['alpha_explanation']

    def test_alpha_explanation_contains_threshold(self):
        result = run_backtest(self.db_path, window='1m')
        assert '1.0' in result['alpha_explanation']

    def test_portfolio_return_computed(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['portfolio_return'] is not None
        assert isinstance(result['portfolio_return'], float)

    def test_avoid_excluded_from_portfolio(self):
        # Portfolio return should not be dragged by the avoid name's -1.0 return
        result = run_backtest(self.db_path, window='1m')
        assert result['portfolio_return'] > 0  # avoid at -1.0 should be excluded

    def test_portfolio_vs_spy(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['portfolio_vs_spy'] is not None

    def test_ticker_detail_sorted(self):
        result = run_backtest(self.db_path, window='1m')
        tiers = [td['conviction_tier'] for td in result['ticker_detail']]
        tier_order = {'high': 0, 'medium': 1, 'low': 2, 'avoid': 3}
        numeric = [tier_order[t] for t in tiers]
        assert numeric == sorted(numeric)

    def test_ticker_detail_contains_return(self):
        result = run_backtest(self.db_path, window='1m')
        for td in result['ticker_detail']:
            assert 'return_1m' in td

    def test_window_1w(self):
        result = run_backtest(self.db_path, window='1w')
        assert result['window'] == '1w'
        assert 'return_1w' in result['ticker_detail'][0]

    def test_window_3m(self):
        result = run_backtest(self.db_path, window='3m')
        assert result['window'] == '3m'

    def test_invalid_window(self):
        with pytest.raises(ValueError):
            run_backtest(self.db_path, window='6m')

    def test_total_tickers(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['total_tickers'] == 10  # 3+2+4+1

    def test_as_of_present(self):
        result = run_backtest(self.db_path, window='1m')
        assert 'as_of' in result
        assert result['as_of']


class TestRunBacktestEdgeCases:
    def test_empty_db(self):
        db_path = _make_db([])
        try:
            result = run_backtest(db_path, window='1m')
            assert result['total_tickers'] == 0
            assert result['alpha_signal'] is False
            assert 'Insufficient data' in result['alpha_explanation']
        finally:
            os.unlink(db_path)

    def test_no_high_conviction(self):
        rows = []
        for tkr, ret in [('xom', 0.5), ('ko', 0.3)]:
            rows.extend(_base_rows(tkr, 'low', 'weak', ret))
        db_path = _make_db(rows)
        try:
            result = run_backtest(db_path, window='1m')
            assert result['cohorts']['high_all']['n'] == 0
            assert result['alpha_signal'] is False
        finally:
            os.unlink(db_path)

    def test_missing_return_skipped(self):
        rows = [
            ('aapl', 'conviction_tier', 'high'),
            ('aapl', 'signal_quality',  'strong'),
            # No return_1m atom — should be excluded
        ]
        db_path = _make_db(rows)
        try:
            result = run_backtest(db_path, window='1m')
            assert result['total_tickers'] == 0
        finally:
            os.unlink(db_path)

    def test_alpha_does_not_fire_below_threshold(self):
        rows = []
        # high mean = 1.5, low mean = 1.4 → diff = 0.1 < 1.0
        for tkr, ret in [('aapl', 1.5), ('msft', 1.5)]:
            rows.extend(_base_rows(tkr, 'high', 'strong', ret))
        for tkr, ret in [('xom', 1.4), ('ko', 1.4)]:
            rows.extend(_base_rows(tkr, 'low', 'weak', ret))
        db_path = _make_db(rows)
        try:
            result = run_backtest(db_path, window='1m')
            assert result['alpha_signal'] is False
            assert 'FAIL' in result['alpha_explanation']
        finally:
            os.unlink(db_path)

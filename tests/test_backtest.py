"""
tests/test_backtest.py — Unit tests for analytics/backtest.py

Tests:
  - _cohort_stats, _safe_float helpers
  - take_snapshot: writes correct rows, idempotent within same day
  - list_snapshots: returns sorted date list
  - run_backtest backward-looking path (< 2 snapshots):
      backward_looking=True, warning field, snapshot_count, methodology
  - run_backtest forward-looking path (>= 2 snapshots):
      backward_looking=False, snapshot_start/end/days, price-based returns
  - alpha_signal pre-committed threshold (1.0pp)
  - Edge cases: empty DB, no high conviction, alpha below threshold

No live DB required — tests build temp SQLite DBs.
"""
import math
import sqlite3
import tempfile
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from analytics.backtest import (
    run_backtest, take_snapshot, list_snapshots,
    ALPHA_THRESHOLD_PP, _cohort_stats, _safe_float,
    _ensure_snapshot_table, _forward_return,
)


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


def _inject_snapshot(db_path: str, date: str, rows: list) -> None:
    """Directly insert rows into signal_snapshots for a given date."""
    conn = sqlite3.connect(db_path)
    _ensure_snapshot_table(conn)
    for ticker, ct, sq, pos, up, price, risk in rows:
        conn.execute(
            """INSERT OR IGNORE INTO signal_snapshots
               (ticker, snapshot_date, conviction_tier, signal_quality,
                position_size_pct, upside_pct, last_price, thesis_risk_level)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, date, ct, sq, pos, up, price, risk),
        )
    conn.commit()
    conn.close()


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


# ── _safe_float / _forward_return ─────────────────────────────────────────────

class TestSafeFloat:
    def test_numeric_string(self):
        assert _safe_float('3.14') == pytest.approx(3.14)

    def test_none(self):
        assert _safe_float(None) is None

    def test_non_numeric(self):
        assert _safe_float('n/a') is None

    def test_float(self):
        assert _safe_float(1.5) == pytest.approx(1.5)


class TestForwardReturn:
    def test_basic(self):
        assert _forward_return(100.0, 110.0) == pytest.approx(10.0)

    def test_negative(self):
        assert _forward_return(100.0, 90.0) == pytest.approx(-10.0)

    def test_none_start(self):
        assert _forward_return(None, 110.0) is None

    def test_none_end(self):
        assert _forward_return(100.0, None) is None

    def test_zero_start(self):
        assert _forward_return(0.0, 110.0) is None

    def test_same_price(self):
        assert _forward_return(100.0, 100.0) == pytest.approx(0.0)


# ── take_snapshot / list_snapshots ────────────────────────────────────────────

class TestTakeSnapshot:
    def setup_method(self):
        rows = []
        for tkr, ct in [('aapl', 'high'), ('nvda', 'high'), ('xom', 'low')]:
            rows.extend([
                (tkr, 'conviction_tier',   ct),
                (tkr, 'signal_quality',    'strong'),
                (tkr, 'position_size_pct', '2.0'),
                (tkr, 'upside_pct',        '20.0'),
                (tkr, 'last_price',        '100.0'),
                (tkr, 'thesis_risk_level', 'moderate'),
            ])
        self.db_path = _make_db(rows)

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_snapshot_inserts_rows(self):
        result = take_snapshot(self.db_path)
        assert result['inserted'] == 3
        assert result['snapshot_date']

    def test_snapshot_idempotent_same_day(self):
        take_snapshot(self.db_path)
        result2 = take_snapshot(self.db_path)
        assert result2['inserted'] == 0

    def test_list_snapshots_empty(self):
        db_path = _make_db([])
        try:
            assert list_snapshots(db_path) == []
        finally:
            os.unlink(db_path)

    def test_list_snapshots_after_take(self):
        take_snapshot(self.db_path)
        snaps = list_snapshots(self.db_path)
        assert len(snaps) == 1
        assert snaps[0]

    def test_list_snapshots_sorted(self):
        _inject_snapshot(self.db_path, '2026-01-01',
                         [('AAPL', 'high', 'strong', 2.0, 20.0, 100.0, 'moderate')])
        _inject_snapshot(self.db_path, '2026-02-01',
                         [('AAPL', 'high', 'strong', 2.0, 25.0, 110.0, 'moderate')])
        snaps = list_snapshots(self.db_path)
        assert snaps == sorted(snaps)
        assert snaps[0] == '2026-01-01'
        assert snaps[1] == '2026-02-01'

    def test_no_conviction_tier_skipped(self):
        rows = [('jnj', 'signal_quality', 'weak'), ('jnj', 'last_price', '50.0')]
        db_path = _make_db(rows)
        try:
            result = take_snapshot(db_path)
            assert result['inserted'] == 0
        finally:
            os.unlink(db_path)


# ── run_backtest — backward-looking path (< 2 snapshots) ─────────────────────

class TestRunBacktestBackwardLooking:
    def setup_method(self):
        rows = []
        for tkr, ret in [('aapl', 4.0), ('msft', 5.0), ('nvda', 6.0)]:
            rows.extend(_base_rows(tkr, 'high', 'strong', ret, return_vs_spy=2.0))
        for tkr, ret in [('crm', 2.0), ('snow', 3.0)]:
            rows.extend(_base_rows(tkr, 'medium', 'confirmed', ret, return_vs_spy=0.5))
        for tkr, ret in [('xom', 0.5), ('jnj', 0.2), ('ko', -0.3), ('pg', 0.1)]:
            rows.extend(_base_rows(tkr, 'low', 'weak', ret, position_size=1.5,
                                   return_vs_spy=-0.5))
        rows.extend(_base_rows('wmt', 'avoid', 'weak', -1.0, position_size=0.0))
        self.db_path = _make_db(rows)

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_backward_looking_flag(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['backward_looking'] is True

    def test_warning_field_present(self):
        result = run_backtest(self.db_path, window='1m')
        assert 'warning' in result
        assert 'backward-looking' in result['warning']

    def test_snapshot_count_zero(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['snapshot_count'] == 0

    def test_methodology_is_snapshot(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['methodology'] == 'point_in_time_snapshot'
        assert 'methodology_note' in result

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
        result = run_backtest(self.db_path, window='1m')
        assert result['alpha_signal'] is True
        assert 'PASS' in result['alpha_explanation']

    def test_alpha_threshold_documented(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['alpha_threshold_pp'] == ALPHA_THRESHOLD_PP
        assert ALPHA_THRESHOLD_PP == 1.0

    def test_portfolio_return_computed(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['portfolio_return'] is not None
        assert result['portfolio_return'] > 0

    def test_portfolio_vs_spy(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['portfolio_vs_spy'] is not None

    def test_ticker_detail_sorted(self):
        result = run_backtest(self.db_path, window='1m')
        tiers = [td['conviction_tier'] for td in result['ticker_detail']]
        tier_order = {'high': 0, 'medium': 1, 'low': 2, 'avoid': 3}
        numeric = [tier_order[t] for t in tiers]
        assert numeric == sorted(numeric)

    def test_ticker_detail_has_return_key(self):
        result = run_backtest(self.db_path, window='1m')
        for td in result['ticker_detail']:
            assert 'return' in td

    def test_window_1w(self):
        result = run_backtest(self.db_path, window='1w')
        assert result['window'] == '1w'

    def test_window_3m(self):
        result = run_backtest(self.db_path, window='3m')
        assert result['window'] == '3m'

    def test_invalid_window(self):
        with pytest.raises(ValueError):
            run_backtest(self.db_path, window='6m')

    def test_total_tickers(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['total_tickers'] == 10

    def test_as_of_present(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['as_of']


# ── run_backtest — forward-looking path (>= 2 snapshots) ─────────────────────

class TestRunBacktestForwardLooking:
    def setup_method(self):
        self.db_path = _make_db([])
        _inject_snapshot(self.db_path, '2026-01-01', [
            ('AAPL', 'high', 'strong', 2.0, 30.0, 100.0, 'moderate'),
            ('MSFT', 'high', 'strong', 2.0, 25.0, 100.0, 'moderate'),
            ('NVDA', 'high', 'strong', 2.0, 40.0, 100.0, 'moderate'),
            ('XOM',  'low',  'weak',   1.0,  5.0, 100.0, 'wide'),
            ('KO',   'low',  'weak',   1.0,  3.0, 100.0, 'moderate'),
            ('JNJ',  'low',  'weak',   1.0,  4.0, 100.0, 'moderate'),
            ('WMT',  'avoid','weak',   0.0,  2.0, 100.0, 'wide'),
        ])
        _inject_snapshot(self.db_path, '2026-02-01', [
            ('AAPL', 'high', 'strong', 2.0, 30.0, 110.0, 'moderate'),
            ('MSFT', 'high', 'strong', 2.0, 25.0, 111.0, 'moderate'),
            ('NVDA', 'high', 'strong', 2.0, 40.0, 109.0, 'moderate'),
            ('XOM',  'low',  'weak',   1.0,  5.0, 101.0, 'wide'),
            ('KO',   'low',  'weak',   1.0,  3.0, 100.5, 'moderate'),
            ('JNJ',  'low',  'weak',   1.0,  4.0, 101.5, 'moderate'),
            ('WMT',  'avoid','weak',   0.0,  2.0,  98.0, 'wide'),
        ])

    def teardown_method(self):
        os.unlink(self.db_path)

    def test_forward_looking_flag(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['backward_looking'] is False

    def test_no_warning_field(self):
        result = run_backtest(self.db_path, window='1m')
        assert 'warning' not in result

    def test_methodology_forward(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['methodology'] == 'forward_looking_snapshot'

    def test_snapshot_count(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['snapshot_count'] == 2

    def test_snapshot_start_end(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['snapshot_start'] == '2026-01-01'
        assert result['snapshot_end']   == '2026-02-01'

    def test_days_between(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['days_between_snapshots'] == 31

    def test_forward_returns_computed(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['total_tickers'] == 7

    def test_high_cohort_returns_positive(self):
        result = run_backtest(self.db_path, window='1m')
        high_mean = result['cohorts']['high_all']['mean_return']
        assert high_mean == pytest.approx(10.0, abs=0.5)

    def test_low_cohort_returns_near_1pct(self):
        result = run_backtest(self.db_path, window='1m')
        low_mean = result['cohorts']['low_all']['mean_return']
        assert 0.0 < low_mean < 3.0

    def test_alpha_signal_fires_forward(self):
        result = run_backtest(self.db_path, window='1m')
        assert result['alpha_signal'] is True
        assert 'PASS' in result['alpha_explanation']

    def test_ticker_detail_has_return_key(self):
        result = run_backtest(self.db_path, window='1m')
        for td in result['ticker_detail']:
            assert 'return' in td

    def test_conviction_tier_from_earlier_snapshot(self):
        result = run_backtest(self.db_path, window='1m')
        tickers = {td['ticker']: td for td in result['ticker_detail']}
        assert tickers['AAPL']['conviction_tier'] == 'high'
        assert tickers['XOM']['conviction_tier'] == 'low'

    def test_three_snapshots_uses_oldest_and_newest(self):
        _inject_snapshot(self.db_path, '2026-01-15', [
            ('AAPL', 'medium', 'weak', 1.0, 10.0, 105.0, 'moderate'),
        ])
        result = run_backtest(self.db_path, window='1m')
        assert result['snapshot_start'] == '2026-01-01'
        assert result['snapshot_end']   == '2026-02-01'
        assert result['snapshot_count'] == 3


# ── Remaining edge cases ──────────────────────────────────────────────────────

class TestRunBacktestEdgeCases:
    def test_empty_db_backward(self):
        db_path = _make_db([])
        try:
            result = run_backtest(db_path, window='1m')
            assert result['total_tickers'] == 0
            assert result['alpha_signal'] is False
            assert result['backward_looking'] is True
            assert 'Insufficient data' in result['alpha_explanation']
        finally:
            os.unlink(db_path)

    def test_no_high_conviction_backward(self):
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

    def test_missing_return_skipped_backward(self):
        rows = [
            ('aapl', 'conviction_tier', 'high'),
            ('aapl', 'signal_quality',  'strong'),
        ]
        db_path = _make_db(rows)
        try:
            result = run_backtest(db_path, window='1m')
            assert result['total_tickers'] == 0
        finally:
            os.unlink(db_path)

    def test_alpha_does_not_fire_below_threshold(self):
        rows = []
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

    def test_forward_one_ticker_missing_price(self):
        db_path = _make_db([])
        _inject_snapshot(db_path, '2026-01-01', [
            ('AAPL', 'high', 'strong', 2.0, 30.0, 100.0, 'moderate'),
            ('XOM',  'low',  'weak',   1.0,  5.0, None,   'wide'),
        ])
        _inject_snapshot(db_path, '2026-02-01', [
            ('AAPL', 'high', 'strong', 2.0, 30.0, 110.0, 'moderate'),
            # XOM missing from T1 too
        ])
        try:
            result = run_backtest(db_path, window='1m')
            # AAPL has valid prices in both → 1 ticker
            # XOM has None start price → excluded
            assert result['backward_looking'] is False
            assert result['total_tickers'] == 1
        finally:
            os.unlink(db_path)

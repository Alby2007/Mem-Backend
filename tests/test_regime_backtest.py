"""
tests/test_regime_backtest.py — Regime-Conditional Backtest Tests

Covers:
  - _read_current_market_regime: reads regime from facts table
  - take_snapshot: captures market_regime column
  - list_snapshot_regimes: returns date→regime map
  - run_regime_backtest: warning path (< 2 snapshots) and full path
  - regime partitioning: tickers split by market_regime at snap_start
  - no_regime_recorded bucket: tickers with NULL regime
  - unconditional cohorts match across-all tickers
  - alpha_signal computed independently per regime
  - GET /analytics/backtest/regime API endpoint
"""

from __future__ import annotations

import sqlite3
import tempfile
import os
from datetime import date, timedelta

import pytest

from analytics.backtest import (
    _ensure_snapshot_table,
    _read_current_market_regime,
    take_snapshot,
    list_snapshot_regimes,
    run_regime_backtest,
    _VALID_REGIMES,
    ALPHA_THRESHOLD_PP,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_db(regime: str | None = 'risk_on_expansion') -> str:
    """
    Create a temp SQLite DB with:
      - facts table (with market_regime atom if regime is not None)
      - signal_snapshots table
    Returns the db_path.
    """
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT, confidence REAL,
            confidence_effective REAL, metadata TEXT,
            hit_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if regime is not None:
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source, confidence) "
            "VALUES ('market', 'market_regime', ?, 'derived_signal_regime', 0.70)",
            (regime,),
        )
    conn.commit()
    conn.close()
    return path


def _insert_ticker_facts(db_path: str, ticker: str, ct: str,
                         sq: str = 'strong', pos: float = 5.0,
                         price: float = 100.0, upside: float = 20.0) -> None:
    conn = sqlite3.connect(db_path)
    for pred, val in [
        ('conviction_tier', ct),
        ('signal_quality', sq),
        ('position_size_pct', str(pos)),
        ('last_price', str(price)),
        ('upside_pct', str(upside)),
    ]:
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source, confidence) "
            "VALUES (?, ?, ?, 'derived_signal_test', 0.80)",
            (ticker.lower(), pred, val),
        )
    conn.commit()
    conn.close()


def _inject_snapshot_row(db_path: str, ticker: str, snap_date: str,
                         ct: str, price: float,
                         regime: str | None = 'risk_on_expansion',
                         sq: str = 'strong', pos: float = 5.0) -> None:
    """Directly insert a row into signal_snapshots."""
    conn = sqlite3.connect(db_path)
    _ensure_snapshot_table(conn)
    conn.execute(
        """INSERT OR REPLACE INTO signal_snapshots
           (ticker, snapshot_date, conviction_tier, signal_quality,
            position_size_pct, upside_pct, last_price, thesis_risk_level,
            market_regime)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, snap_date, ct, sq, pos, 20.0, price, 'moderate', regime),
    )
    conn.commit()
    conn.close()


# ── TestReadCurrentMarketRegime ───────────────────────────────────────────────

class TestReadCurrentMarketRegime:

    def test_returns_regime_when_present(self):
        path = _make_db('risk_on_expansion')
        conn = sqlite3.connect(path)
        result = _read_current_market_regime(conn)
        conn.close()
        assert result == 'risk_on_expansion'

    def test_returns_none_when_no_regime_atom(self):
        path = _make_db(regime=None)
        conn = sqlite3.connect(path)
        result = _read_current_market_regime(conn)
        conn.close()
        assert result is None

    def test_returns_latest_when_multiple_rows(self):
        path = _make_db(regime=None)
        conn = sqlite3.connect(path)
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source, confidence) "
            "VALUES ('market', 'market_regime', 'stagflation', 'x', 0.7)"
        )
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source, confidence) "
            "VALUES ('market', 'market_regime', 'recovery', 'x', 0.7)"
        )
        conn.commit()
        result = _read_current_market_regime(conn)
        conn.close()
        assert result == 'recovery'

    def test_passes_through_unknown_regime(self):
        path = _make_db(regime=None)
        conn = sqlite3.connect(path)
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source, confidence) "
            "VALUES ('market', 'market_regime', 'unknown_future_regime', 'x', 0.7)"
        )
        conn.commit()
        result = _read_current_market_regime(conn)
        conn.close()
        assert result == 'unknown_future_regime'

    def test_returns_none_on_empty_facts_table(self):
        path = _make_db(regime=None)
        conn = sqlite3.connect(path)
        result = _read_current_market_regime(conn)
        conn.close()
        assert result is None


# ── TestTakeSnapshotWithRegime ────────────────────────────────────────────────

class TestTakeSnapshotWithRegime:

    def test_regime_captured_in_snapshot(self):
        path = _make_db('risk_off_contraction')
        _insert_ticker_facts(path, 'AAPL', 'high')
        result = take_snapshot(path)
        assert result['market_regime'] == 'risk_off_contraction'

    def test_regime_none_when_no_atom(self):
        path = _make_db(regime=None)
        _insert_ticker_facts(path, 'AAPL', 'high')
        result = take_snapshot(path)
        assert result['market_regime'] is None

    def test_regime_written_to_snapshot_row(self):
        path = _make_db('stagflation')
        _insert_ticker_facts(path, 'MSFT', 'high')
        take_snapshot(path)
        conn = sqlite3.connect(path)
        c = conn.cursor()
        c.execute("SELECT market_regime FROM signal_snapshots WHERE ticker='MSFT'")
        row = c.fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 'stagflation'

    def test_all_tickers_get_same_regime(self):
        path = _make_db('recovery')
        for ticker in ['AAPL', 'MSFT', 'GOOGL']:
            _insert_ticker_facts(path, ticker, 'high')
        take_snapshot(path)
        conn = sqlite3.connect(path)
        c = conn.cursor()
        c.execute("SELECT DISTINCT market_regime FROM signal_snapshots")
        regimes = [r[0] for r in c.fetchall()]
        conn.close()
        assert regimes == ['recovery']

    def test_idempotent_on_same_day(self):
        path = _make_db('risk_on_expansion')
        _insert_ticker_facts(path, 'AAPL', 'high')
        r1 = take_snapshot(path)
        r2 = take_snapshot(path)
        assert r1['inserted'] == 1
        assert r2['inserted'] == 0
        assert r2['skipped'] >= 1


# ── TestListSnapshotRegimes ───────────────────────────────────────────────────

class TestListSnapshotRegimes:

    def test_returns_empty_when_no_snapshots(self):
        path = _make_db()
        result = list_snapshot_regimes(path)
        assert result == {}

    def test_single_snapshot_date(self):
        path = _make_db()
        _inject_snapshot_row(path, 'AAPL', '2026-01-01', 'high', 100.0, 'risk_on_expansion')
        result = list_snapshot_regimes(path)
        assert result == {'2026-01-01': 'risk_on_expansion'}

    def test_multiple_dates_ordered(self):
        path = _make_db()
        _inject_snapshot_row(path, 'AAPL', '2026-02-01', 'high', 100.0, 'risk_off_contraction')
        _inject_snapshot_row(path, 'AAPL', '2026-01-01', 'high', 90.0, 'risk_on_expansion')
        result = list_snapshot_regimes(path)
        keys = list(result.keys())
        assert keys == ['2026-01-01', '2026-02-01']
        assert result['2026-01-01'] == 'risk_on_expansion'
        assert result['2026-02-01'] == 'risk_off_contraction'

    def test_null_regime_returned_as_none(self):
        path = _make_db()
        _inject_snapshot_row(path, 'AAPL', '2026-01-01', 'high', 100.0, regime=None)
        result = list_snapshot_regimes(path)
        assert result['2026-01-01'] is None


# ── TestRunRegimeBacktestWarning ──────────────────────────────────────────────

class TestRunRegimeBacktestWarning:

    def test_zero_snapshots_returns_warning(self):
        path = _make_db()
        result = run_regime_backtest(path)
        assert result['snapshot_count'] == 0
        assert 'warning' in result
        assert 'insufficient_snapshots' in result['warning']
        assert result['methodology'] == 'regime_conditional_forward_looking'
        assert 'by_regime' not in result

    def test_one_snapshot_returns_warning(self):
        path = _make_db()
        _inject_snapshot_row(path, 'AAPL', '2026-01-01', 'high', 100.0)
        result = run_regime_backtest(path)
        assert result['snapshot_count'] == 1
        assert 'warning' in result
        assert 'by_regime' not in result


# ── TestRunRegimeBacktestFull ─────────────────────────────────────────────────

class TestRunRegimeBacktestFull:

    def _make_two_snapshot_db(self,
                               snap1_regime: str = 'risk_on_expansion',
                               snap2_regime: str = 'risk_on_expansion') -> str:
        """
        Build a DB with 2 snapshots 30 days apart.
        snap1: AAPL=high/100, MSFT=high/100, SPY=medium/200
        snap2: AAPL=120 (+20%), MSFT=95 (-5%), SPY=210 (+5%)
        """
        path = _make_db(regime=None)
        d1 = '2026-01-01'
        d2 = '2026-02-01'
        # snap1
        _inject_snapshot_row(path, 'AAPL', d1, 'high',   100.0, snap1_regime, 'strong', 5.0)
        _inject_snapshot_row(path, 'MSFT', d1, 'high',   100.0, snap1_regime, 'strong', 5.0)
        _inject_snapshot_row(path, 'GS',   d1, 'low',    200.0, snap1_regime, 'weak',   2.0)
        # snap2
        _inject_snapshot_row(path, 'AAPL', d2, 'high',   120.0, snap2_regime, 'strong', 5.0)
        _inject_snapshot_row(path, 'MSFT', d2, 'high',    95.0, snap2_regime, 'strong', 5.0)
        _inject_snapshot_row(path, 'GS',   d2, 'low',    210.0, snap2_regime, 'weak',   2.0)
        return path

    def test_returns_correct_structure(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        assert result['snapshot_count'] == 2
        assert result['methodology'] == 'regime_conditional_forward_looking'
        assert 'warning' not in result
        assert 'by_regime' in result
        assert 'unconditional_cohorts' in result
        assert 'unconditional_alpha' in result
        assert 'regimes_observed' in result
        assert 'total_tickers' in result

    def test_single_regime_all_tickers_in_one_bucket(self):
        path = self._make_two_snapshot_db('risk_on_expansion', 'risk_on_expansion')
        result = run_regime_backtest(path)
        assert 'risk_on_expansion' in result['by_regime']
        assert result['by_regime']['risk_on_expansion']['n_tickers'] == 3

    def test_two_regimes_partition_correctly(self):
        """snap_start has two different regimes across tickers."""
        path = _make_db(regime=None)
        d1 = '2026-01-01'
        d2 = '2026-02-01'
        _inject_snapshot_row(path, 'AAPL', d1, 'high', 100.0, 'risk_on_expansion')
        _inject_snapshot_row(path, 'MSFT', d1, 'high', 100.0, 'risk_off_contraction')
        _inject_snapshot_row(path, 'AAPL', d2, 'high', 120.0, 'risk_on_expansion')
        _inject_snapshot_row(path, 'MSFT', d2, 'high', 95.0,  'risk_off_contraction')
        result = run_regime_backtest(path)
        assert 'risk_on_expansion' in result['by_regime']
        assert 'risk_off_contraction' in result['by_regime']
        assert result['by_regime']['risk_on_expansion']['n_tickers'] == 1
        assert result['by_regime']['risk_off_contraction']['n_tickers'] == 1
        assert sorted(result['regimes_observed']) == ['risk_off_contraction', 'risk_on_expansion']

    def test_no_regime_bucket_for_null_regime_tickers(self):
        path = _make_db(regime=None)
        d1 = '2026-01-01'
        d2 = '2026-02-01'
        _inject_snapshot_row(path, 'AAPL', d1, 'high', 100.0, None)
        _inject_snapshot_row(path, 'AAPL', d2, 'high', 120.0, None)
        result = run_regime_backtest(path)
        assert 'no_regime_recorded' in result['by_regime']
        assert result['by_regime']['no_regime_recorded']['n_tickers'] == 1
        assert result['regimes_observed'] == []

    def test_forward_returns_correct(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        bucket = result['by_regime']['risk_on_expansion']
        detail = {td['ticker']: td for td in bucket['ticker_detail']}
        assert detail['AAPL']['return'] == pytest.approx(20.0, abs=0.01)
        assert detail['MSFT']['return'] == pytest.approx(-5.0, abs=0.01)
        assert detail['GS']['return'] == pytest.approx(5.0, abs=0.01)

    def test_days_between_snapshots(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        assert result['days_between_snapshots'] == 31

    def test_snapshot_start_and_end(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        assert result['snapshot_start'] == '2026-01-01'
        assert result['snapshot_end']   == '2026-02-01'

    def test_unconditional_cohorts_include_all_tickers(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        high_n = result['unconditional_cohorts']['high_all']['n']
        low_n  = result['unconditional_cohorts']['low_all']['n']
        assert high_n == 2   # AAPL + MSFT
        assert low_n  == 1   # GS

    def test_alpha_threshold_correct(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        assert result['alpha_threshold_pp'] == ALPHA_THRESHOLD_PP

    def test_regime_alpha_computed_independently(self):
        """Two regimes, one with alpha and one without, should be independent."""
        path = _make_db(regime=None)
        d1 = '2026-01-01'
        d2 = '2026-02-01'
        # risk_on: high=+10%, low=-1% → alpha = True (diff=11pp > 1pp)
        _inject_snapshot_row(path, 'AAPL', d1, 'high', 100.0, 'risk_on_expansion', 'strong', 5.0)
        _inject_snapshot_row(path, 'GS',   d1, 'low',  100.0, 'risk_on_expansion', 'weak',   2.0)
        _inject_snapshot_row(path, 'AAPL', d2, 'high', 110.0, 'risk_on_expansion')
        _inject_snapshot_row(path, 'GS',   d2, 'low',   99.0, 'risk_on_expansion')
        # risk_off: high=-2%, low=-1.5% → alpha = False (diff=-0.5pp < 1pp)
        _inject_snapshot_row(path, 'MSFT', d1, 'high', 100.0, 'risk_off_contraction', 'strong', 5.0)
        _inject_snapshot_row(path, 'BAC',  d1, 'low',  100.0, 'risk_off_contraction', 'weak',   2.0)
        _inject_snapshot_row(path, 'MSFT', d2, 'high',  98.0, 'risk_off_contraction')
        _inject_snapshot_row(path, 'BAC',  d2, 'low',   98.5, 'risk_off_contraction')
        result = run_regime_backtest(path)
        assert result['by_regime']['risk_on_expansion']['alpha_signal'] is True
        assert result['by_regime']['risk_off_contraction']['alpha_signal'] is False

    def test_portfolio_return_computed_per_regime(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        pr = result['by_regime']['risk_on_expansion']['portfolio_return']
        assert pr is not None

    def test_cohort_shape_per_regime(self):
        path = self._make_two_snapshot_db()
        result = run_regime_backtest(path)
        cohorts = result['by_regime']['risk_on_expansion']['cohorts']
        expected_keys = {'high_all', 'high_strong', 'medium_all',
                         'medium_strong', 'low_all', 'avoid'}
        assert set(cohorts.keys()) == expected_keys


# ── TestRegimeBacktestApiEndpoint ─────────────────────────────────────────────

class TestRegimeBacktestApiEndpoint:

    @pytest.fixture(autouse=True)
    def patch_db(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / 'test.db')
        # Create a minimal valid DB
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                source TEXT, confidence REAL,
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
        return db_path

    @pytest.fixture
    def client(self):
        import api
        api.app.config['TESTING'] = True
        with api.app.test_client() as c:
            yield c

    def test_returns_warning_with_no_snapshots(self, client):
        resp = client.get('/analytics/backtest/regime')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['snapshot_count'] == 0
        assert 'warning' in data

    def test_returns_full_result_with_two_snapshots(self, client):
        d1 = '2026-01-01'
        d2 = '2026-02-01'
        _inject_snapshot_row(self.db_path, 'AAPL', d1, 'high',  100.0, 'risk_on_expansion')
        _inject_snapshot_row(self.db_path, 'GS',   d1, 'low',   100.0, 'risk_on_expansion')
        _inject_snapshot_row(self.db_path, 'AAPL', d2, 'high',  110.0, 'risk_on_expansion')
        _inject_snapshot_row(self.db_path, 'GS',   d2, 'low',    98.0, 'risk_on_expansion')
        resp = client.get('/analytics/backtest/regime')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['snapshot_count'] == 2
        assert 'by_regime' in data
        assert 'risk_on_expansion' in data['by_regime']

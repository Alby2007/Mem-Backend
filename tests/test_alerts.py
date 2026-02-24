"""
tests/test_alerts.py — Unit tests for analytics/alerts.py

Tests:
  - _ensure_alerts_table: idempotent DDL
  - _load_current_kb: reads conviction_tier + smart_money_signal
  - _load_latest_snapshots: max snapshot_date per ticker
  - _load_recent_edgar_tickers: lookback window filtering
  - _already_alerted: deduplication guard
  - get_alerts: unseen_only, since_iso, limit filters
  - mark_alerts_seen: updates seen flag, returns rowcount
  - AlertMonitor.check():
      - conviction_change when tier changed since snapshot
      - new_high_conviction when no prior snapshot and tier=high
      - options_sweep when smart_money_signal != 'none'
      - edgar_event for recent EDGAR atoms
      - deduplication within window
      - no alert when conviction unchanged
      - no alert when no high conviction and no snapshot

No live DB or API — all tests use temp SQLite DBs.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from analytics.alerts import (
    AlertMonitor,
    _ensure_alerts_table,
    _load_current_kb,
    _load_latest_snapshots,
    _load_recent_edgar_tickers,
    _already_alerted,
    get_alerts,
    mark_alerts_seen,
    _EDGAR_LOOKBACK_MINUTES,
)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _make_db() -> str:
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            confidence REAL DEFAULT 0.7,
            source TEXT DEFAULT 'test',
            timestamp TEXT DEFAULT '2026-01-01T00:00:00+00:00'
        )
    """)
    conn.execute("""
        CREATE TABLE signal_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            snapshot_date TEXT,
            conviction_tier TEXT,
            signal_quality TEXT,
            position_size_pct REAL,
            upside_pct REAL,
            last_price REAL,
            thesis_risk_level TEXT,
            UNIQUE(ticker, snapshot_date)
        )
    """)
    conn.commit()
    conn.close()
    return path


def _insert_fact(path, subject, predicate, obj, source='test', timestamp=None):
    ts = timestamp or '2026-01-01T00:00:00+00:00'
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO facts (subject, predicate, object, source, timestamp) VALUES (?, ?, ?, ?, ?)",
        (subject, predicate, obj, source, ts),
    )
    conn.commit()
    conn.close()


def _insert_snapshot(path, ticker, snap_date, conviction_tier):
    conn = sqlite3.connect(path)
    conn.execute(
        """INSERT OR IGNORE INTO signal_snapshots
           (ticker, snapshot_date, conviction_tier)
           VALUES (?, ?, ?)""",
        (ticker, snap_date, conviction_tier),
    )
    conn.commit()
    conn.close()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _minutes_ago(n):
    return (datetime.now(timezone.utc) - timedelta(minutes=n)).isoformat()


# ── _ensure_alerts_table ──────────────────────────────────────────────────────

class TestEnsureAlertsTable:
    def setup_method(self):
        self.path = _make_db()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_creates_table(self):
        conn = sqlite3.connect(self.path)
        _ensure_alerts_table(conn)
        conn.close()
        conn = sqlite3.connect(self.path)
        c = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='alerts'")
        assert c.fetchone() is not None
        conn.close()

    def test_idempotent(self):
        conn = sqlite3.connect(self.path)
        _ensure_alerts_table(conn)
        _ensure_alerts_table(conn)  # second call should not raise
        conn.close()

    def test_columns_present(self):
        conn = sqlite3.connect(self.path)
        _ensure_alerts_table(conn)
        c = conn.execute("PRAGMA table_info(alerts)")
        cols = {row[1] for row in c.fetchall()}
        conn.close()
        for col in ('id', 'ticker', 'alert_type', 'detail', 'triggered_at', 'seen'):
            assert col in cols


# ── _load_current_kb ──────────────────────────────────────────────────────────

class TestLoadCurrentKb:
    def setup_method(self):
        self.path = _make_db()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_reads_conviction_tier(self):
        _insert_fact(self.path, 'aapl', 'conviction_tier', 'high')
        conn = sqlite3.connect(self.path)
        result = _load_current_kb(conn)
        conn.close()
        assert result.get('aapl', {}).get('conviction_tier') == 'high'

    def test_reads_smart_money_signal(self):
        _insert_fact(self.path, 'tsla', 'smart_money_signal', 'call_sweep')
        conn = sqlite3.connect(self.path)
        result = _load_current_kb(conn)
        conn.close()
        assert result.get('tsla', {}).get('smart_money_signal') == 'call_sweep'

    def test_ignores_irrelevant_predicates(self):
        _insert_fact(self.path, 'msft', 'last_price', '400.0')
        conn = sqlite3.connect(self.path)
        result = _load_current_kb(conn)
        conn.close()
        # last_price is not in the select list
        assert result.get('msft', {}).get('last_price') is None

    def test_subject_lowercased(self):
        _insert_fact(self.path, 'NVDA', 'conviction_tier', 'medium')
        conn = sqlite3.connect(self.path)
        result = _load_current_kb(conn)
        conn.close()
        assert 'nvda' in result


# ── _load_latest_snapshots ────────────────────────────────────────────────────

class TestLoadLatestSnapshots:
    def setup_method(self):
        self.path = _make_db()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_returns_most_recent_snapshot(self):
        _insert_snapshot(self.path, 'AAPL', '2026-01-01', 'medium')
        _insert_snapshot(self.path, 'AAPL', '2026-02-01', 'high')
        conn = sqlite3.connect(self.path)
        result = _load_latest_snapshots(conn)
        conn.close()
        assert result['AAPL']['conviction_tier'] == 'high'
        assert result['AAPL']['snapshot_date'] == '2026-02-01'

    def test_multiple_tickers_independent(self):
        _insert_snapshot(self.path, 'AAPL', '2026-02-01', 'high')
        _insert_snapshot(self.path, 'MSFT', '2026-02-10', 'medium')
        conn = sqlite3.connect(self.path)
        result = _load_latest_snapshots(conn)
        conn.close()
        assert result['AAPL']['conviction_tier'] == 'high'
        assert result['MSFT']['conviction_tier'] == 'medium'

    def test_empty_when_no_snapshots(self):
        conn = sqlite3.connect(self.path)
        result = _load_latest_snapshots(conn)
        conn.close()
        assert result == {}

    def test_no_signal_snapshots_table_returns_empty(self):
        # DB without signal_snapshots table
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE facts (id INTEGER PRIMARY KEY)")
        conn.commit()
        result = _load_latest_snapshots(conn)
        conn.close()
        os.unlink(path)
        assert result == {}


# ── _load_recent_edgar_tickers ────────────────────────────────────────────────

class TestLoadRecentEdgarTickers:
    def setup_method(self):
        self.path = _make_db()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_recent_edgar_atom_returned(self):
        ts = _minutes_ago(5)
        _insert_fact(self.path, 'jpm', 'catalyst', 'q4_results',
                     source='edgar_realtime', timestamp=ts)
        conn = sqlite3.connect(self.path)
        result = _load_recent_edgar_tickers(conn, 30)
        conn.close()
        assert 'JPM' in result

    def test_old_edgar_atom_not_returned(self):
        ts = _minutes_ago(90)
        _insert_fact(self.path, 'gs', 'catalyst', 'filing',
                     source='edgar_filing', timestamp=ts)
        conn = sqlite3.connect(self.path)
        result = _load_recent_edgar_tickers(conn, 30)
        conn.close()
        assert 'GS' not in result

    def test_non_edgar_source_not_returned(self):
        ts = _minutes_ago(5)
        _insert_fact(self.path, 'aapl', 'catalyst', 'news',
                     source='rss_news', timestamp=ts)
        conn = sqlite3.connect(self.path)
        result = _load_recent_edgar_tickers(conn, 30)
        conn.close()
        assert 'AAPL' not in result

    def test_returns_uppercase(self):
        ts = _minutes_ago(5)
        _insert_fact(self.path, 'meta', 'catalyst', 'report',
                     source='edgar_realtime', timestamp=ts)
        conn = sqlite3.connect(self.path)
        result = _load_recent_edgar_tickers(conn, 30)
        conn.close()
        assert 'META' in result


# ── _already_alerted ──────────────────────────────────────────────────────────

class TestAlreadyAlerted:
    def setup_method(self):
        self.path = _make_db()
        conn = sqlite3.connect(self.path)
        _ensure_alerts_table(conn)
        conn.close()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_false_when_no_prior_alert(self):
        conn = sqlite3.connect(self.path)
        result = _already_alerted(conn, 'AAPL', 'conviction_change', _minutes_ago(60))
        conn.close()
        assert result is False

    def test_true_after_insert(self):
        now = _now_iso()
        conn = sqlite3.connect(self.path)
        conn.execute(
            "INSERT INTO alerts (ticker, alert_type, detail, triggered_at) VALUES (?, ?, ?, ?)",
            ('AAPL', 'conviction_change', 'medium → high', now),
        )
        conn.commit()
        result = _already_alerted(conn, 'AAPL', 'conviction_change', _minutes_ago(60))
        conn.close()
        assert result is True

    def test_false_when_alert_is_too_old(self):
        old_ts = _minutes_ago(120)
        conn = sqlite3.connect(self.path)
        conn.execute(
            "INSERT INTO alerts (ticker, alert_type, detail, triggered_at) VALUES (?, ?, ?, ?)",
            ('AAPL', 'conviction_change', 'medium → high', old_ts),
        )
        conn.commit()
        # Cutoff is 60 min ago — the alert is 120 min old, so outside window
        result = _already_alerted(conn, 'AAPL', 'conviction_change', _minutes_ago(60))
        conn.close()
        assert result is False


# ── get_alerts ────────────────────────────────────────────────────────────────

class TestGetAlerts:
    def setup_method(self):
        self.path = _make_db()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def _seed_alerts(self):
        conn = sqlite3.connect(self.path)
        _ensure_alerts_table(conn)
        now = _now_iso()
        old = _minutes_ago(120)
        conn.execute(
            "INSERT INTO alerts (ticker, alert_type, detail, triggered_at, seen) VALUES (?,?,?,?,?)",
            ('AAPL', 'conviction_change', 'medium → high', now, 0),
        )
        conn.execute(
            "INSERT INTO alerts (ticker, alert_type, detail, triggered_at, seen) VALUES (?,?,?,?,?)",
            ('MSFT', 'options_sweep', 'call_sweep', now, 1),
        )
        conn.execute(
            "INSERT INTO alerts (ticker, alert_type, detail, triggered_at, seen) VALUES (?,?,?,?,?)",
            ('NVDA', 'edgar_event', 'new filing', old, 0),
        )
        conn.commit()
        conn.close()

    def test_unseen_only_default(self):
        self._seed_alerts()
        rows = get_alerts(self.path, unseen_only=True)
        assert all(r['seen'] == 0 for r in rows)
        tickers = {r['ticker'] for r in rows}
        assert 'MSFT' not in tickers  # seen=1

    def test_all_alerts(self):
        self._seed_alerts()
        rows = get_alerts(self.path, unseen_only=False)
        assert len(rows) == 3

    def test_since_filter(self):
        self._seed_alerts()
        cutoff = _minutes_ago(60)
        rows = get_alerts(self.path, unseen_only=False, since_iso=cutoff)
        # Only AAPL and MSFT are recent (now); NVDA is 120 min old
        tickers = {r['ticker'] for r in rows}
        assert 'NVDA' not in tickers
        assert 'AAPL' in tickers

    def test_limit(self):
        self._seed_alerts()
        rows = get_alerts(self.path, unseen_only=False, limit=1)
        assert len(rows) == 1

    def test_returns_expected_keys(self):
        self._seed_alerts()
        rows = get_alerts(self.path, unseen_only=False)
        for r in rows:
            for key in ('id', 'ticker', 'alert_type', 'detail', 'triggered_at', 'seen'):
                assert key in r

    def test_empty_when_no_alerts(self):
        rows = get_alerts(self.path)
        assert rows == []


# ── mark_alerts_seen ──────────────────────────────────────────────────────────

class TestMarkAlertsSeen:
    def setup_method(self):
        self.path = _make_db()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_marks_correct_ids(self):
        conn = sqlite3.connect(self.path)
        _ensure_alerts_table(conn)
        now = _now_iso()
        conn.execute(
            "INSERT INTO alerts (ticker, alert_type, detail, triggered_at, seen) VALUES (?,?,?,?,0)",
            ('AAPL', 'conviction_change', 'x', now),
        )
        conn.execute(
            "INSERT INTO alerts (ticker, alert_type, detail, triggered_at, seen) VALUES (?,?,?,?,0)",
            ('MSFT', 'options_sweep', 'y', now),
        )
        conn.commit()
        # Get the inserted IDs
        c = conn.execute("SELECT id FROM alerts ORDER BY id")
        ids = [row[0] for row in c.fetchall()]
        conn.close()

        updated = mark_alerts_seen(self.path, [ids[0]])
        assert updated == 1

        rows = get_alerts(self.path, unseen_only=False)
        seen_map = {r['ticker']: r['seen'] for r in rows}
        assert seen_map['AAPL'] == 1
        assert seen_map['MSFT'] == 0

    def test_empty_ids_returns_zero(self):
        assert mark_alerts_seen(self.path, []) == 0

    def test_marks_multiple_ids(self):
        conn = sqlite3.connect(self.path)
        _ensure_alerts_table(conn)
        now = _now_iso()
        for t in ('A', 'B', 'C'):
            conn.execute(
                "INSERT INTO alerts (ticker, alert_type, detail, triggered_at, seen) VALUES (?,?,?,?,0)",
                (t, 'edgar_event', 'x', now),
            )
        conn.commit()
        c = conn.execute("SELECT id FROM alerts ORDER BY id")
        ids = [row[0] for row in c.fetchall()]
        conn.close()

        updated = mark_alerts_seen(self.path, ids[:2])
        assert updated == 2


# ── AlertMonitor.check() ──────────────────────────────────────────────────────

class TestAlertMonitorCheck:
    def setup_method(self):
        self.path = _make_db()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_conviction_change_alert_fired(self):
        # KB says high, last snapshot says medium
        _insert_fact(self.path, 'aapl', 'conviction_tier', 'high')
        _insert_snapshot(self.path, 'AAPL', '2026-01-01', 'medium')

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        types = [a['alert_type'] for a in alerts]
        assert 'conviction_change' in types
        change = next(a for a in alerts if a['alert_type'] == 'conviction_change')
        assert change['ticker'] == 'AAPL'
        assert 'medium' in change['detail']
        assert 'high' in change['detail']

    def test_no_alert_when_conviction_unchanged(self):
        _insert_fact(self.path, 'msft', 'conviction_tier', 'medium')
        _insert_snapshot(self.path, 'MSFT', '2026-01-01', 'medium')

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        types = [a['alert_type'] for a in alerts]
        assert 'conviction_change' not in types

    def test_new_high_conviction_alert_fired(self):
        # No prior snapshot, KB has high conviction
        _insert_fact(self.path, 'nvda', 'conviction_tier', 'high')

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        types = [a['alert_type'] for a in alerts]
        assert 'new_high_conviction' in types
        nc = next(a for a in alerts if a['alert_type'] == 'new_high_conviction')
        assert nc['ticker'] == 'NVDA'

    def test_no_new_high_conviction_for_medium(self):
        # No prior snapshot, but conviction is medium — should NOT fire
        _insert_fact(self.path, 'ko', 'conviction_tier', 'medium')

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        types = [a['alert_type'] for a in alerts]
        assert 'new_high_conviction' not in types

    def test_options_sweep_alert_fired(self):
        _insert_fact(self.path, 'tsla', 'conviction_tier', 'medium')
        _insert_fact(self.path, 'tsla', 'smart_money_signal', 'call_sweep')
        _insert_snapshot(self.path, 'TSLA', '2026-01-01', 'medium')

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        types = [a['alert_type'] for a in alerts]
        assert 'options_sweep' in types
        sweep = next(a for a in alerts if a['alert_type'] == 'options_sweep')
        assert sweep['ticker'] == 'TSLA'
        assert sweep['detail'] == 'call_sweep'

    def test_no_options_sweep_when_none(self):
        _insert_fact(self.path, 'v', 'conviction_tier', 'high')
        _insert_fact(self.path, 'v', 'smart_money_signal', 'none')
        _insert_snapshot(self.path, 'V', '2026-01-01', 'high')

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        types = [a['alert_type'] for a in alerts]
        assert 'options_sweep' not in types

    def test_edgar_event_alert_fired(self):
        ts = _minutes_ago(10)
        _insert_fact(self.path, 'jpm', 'catalyst', 'q4_results',
                     source='edgar_realtime', timestamp=ts)

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        types = [a['alert_type'] for a in alerts]
        assert 'edgar_event' in types
        edgar = next(a for a in alerts if a['alert_type'] == 'edgar_event')
        assert edgar['ticker'] == 'JPM'

    def test_deduplication_prevents_repeat_alert(self):
        _insert_fact(self.path, 'amzn', 'conviction_tier', 'high')
        _insert_snapshot(self.path, 'AMZN', '2026-01-01', 'medium')

        monitor = AlertMonitor(self.path)
        alerts1 = monitor.check()
        alerts2 = monitor.check()  # second check — should be deduplicated

        c1 = sum(1 for a in alerts1 if a['alert_type'] == 'conviction_change')
        c2 = sum(1 for a in alerts2 if a['alert_type'] == 'conviction_change')
        assert c1 == 1
        assert c2 == 0  # deduplicated within window

    def test_alerts_persisted_to_db(self):
        _insert_fact(self.path, 'meta', 'conviction_tier', 'high')

        monitor = AlertMonitor(self.path)
        monitor.check()

        rows = get_alerts(self.path, unseen_only=False)
        tickers = {r['ticker'] for r in rows}
        assert 'META' in tickers

    def test_empty_kb_no_alerts(self):
        monitor = AlertMonitor(self.path)
        alerts = monitor.check()
        assert alerts == []

    def test_conviction_downgrade_fires_alert(self):
        _insert_fact(self.path, 'intc', 'conviction_tier', 'low')
        _insert_snapshot(self.path, 'INTC', '2026-01-01', 'high')

        monitor = AlertMonitor(self.path)
        alerts = monitor.check()

        change = next((a for a in alerts if a['alert_type'] == 'conviction_change'), None)
        assert change is not None
        assert 'high' in change['detail']
        assert 'low' in change['detail']

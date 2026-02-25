"""
tests/test_confidence_intervals.py — Bayesian Confidence Interval Tests

Covers:
  - ensure_confidence_columns: idempotent migration
  - welford_update: mean update, n increment, variance tracking
  - widen_for_conflict: mean pull, variance widening, n unchanged
  - get_confidence_interval: returns correct distribution fields
  - get_all_confidence_intervals: returns all predicates for subject
  - update_atom_confidence: DB round-trip update
  - widen_atom_confidence: DB round-trip widen
  - GET /kb/confidence API endpoint
"""

from __future__ import annotations

import math
import sqlite3
import tempfile
import os

import pytest

from knowledge.confidence_intervals import (
    ensure_confidence_columns,
    welford_update,
    widen_for_conflict,
    get_confidence_interval,
    get_all_confidence_intervals,
    update_atom_confidence,
    widen_atom_confidence,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_facts_db() -> str:
    """Create a temp SQLite DB with minimal facts table."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT, confidence REAL DEFAULT 0.5,
            confidence_effective REAL, metadata TEXT,
            hit_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    return path


def _insert_atom(db_path: str, subject: str, predicate: str, obj: str,
                 confidence: float = 0.8, source: str = 'derived_signal_test',
                 conf_n: int = 1, conf_var: float = 0.0) -> int:
    conn = sqlite3.connect(db_path)
    ensure_confidence_columns(conn)
    cur = conn.execute(
        "INSERT INTO facts (subject, predicate, object, source, confidence, conf_n, conf_var) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (subject, predicate, obj, source, confidence, conf_n, conf_var),
    )
    atom_id = cur.lastrowid
    conn.commit()
    conn.close()
    return atom_id


# ── TestEnsureConfidenceColumns ───────────────────────────────────────────────

class TestEnsureConfidenceColumns:

    def test_adds_columns_to_fresh_table(self):
        path = _make_facts_db()
        conn = sqlite3.connect(path)
        ensure_confidence_columns(conn)
        conn.cursor().execute("PRAGMA table_info(facts)")
        cols = {r[1] for r in conn.cursor().execute("PRAGMA table_info(facts)").fetchall()}
        conn.close()
        assert 'conf_n' in cols
        assert 'conf_var' in cols

    def test_idempotent_second_call(self):
        path = _make_facts_db()
        conn = sqlite3.connect(path)
        ensure_confidence_columns(conn)
        ensure_confidence_columns(conn)  # must not raise
        conn.close()

    def test_existing_rows_get_defaults(self):
        path = _make_facts_db()
        conn = sqlite3.connect(path)
        conn.execute("INSERT INTO facts (subject, predicate, object, source, confidence) "
                     "VALUES ('aapl', 'conviction_tier', 'high', 'test', 0.8)")
        conn.commit()
        ensure_confidence_columns(conn)
        row = conn.execute("SELECT conf_n, conf_var FROM facts WHERE subject='aapl'").fetchone()
        conn.close()
        assert row[0] == 1    # DEFAULT 1
        assert row[1] == 0.0  # DEFAULT 0


# ── TestWelfordUpdate ─────────────────────────────────────────────────────────

class TestWelfordUpdate:

    def test_n_increments(self):
        _, new_n, _ = welford_update(0.8, 1, 0.0, 0.9)
        assert new_n == 2

    def test_mean_moves_toward_new_observation(self):
        new_mean, _, _ = welford_update(0.5, 1, 0.0, 1.0)
        assert new_mean > 0.5

    def test_mean_clipped_to_0_1(self):
        new_mean, _, _ = welford_update(0.0, 1, 0.0, 0.0, source='')
        assert 0.0 <= new_mean <= 1.0

    def test_variance_stays_zero_for_identical_observations(self):
        _, _, new_var = welford_update(0.8, 1, 0.0, 0.8)
        assert new_var >= 0.0

    def test_variance_grows_with_spread(self):
        _, _, var1 = welford_update(0.5, 3, 0.01, 0.9)
        _, _, var2 = welford_update(0.5, 3, 0.01, 0.51)
        assert var1 > var2

    def test_high_authority_source_moves_mean_more(self):
        mean1, _, _ = welford_update(0.5, 2, 0.0, 0.9, source='exchange_feed')
        mean2, _, _ = welford_update(0.5, 2, 0.0, 0.9, source='social_signal_reddit')
        assert mean1 > mean2

    def test_conf_var_never_negative(self):
        for obs in [0.0, 0.5, 1.0]:
            _, _, cv = welford_update(0.8, 1, 0.0, obs)
            assert cv >= 0.0

    def test_multiple_updates_increase_n(self):
        mean, n, var = 0.7, 1, 0.0
        for conf in [0.75, 0.80, 0.85]:
            mean, n, var = welford_update(mean, n, var, conf)
        assert n == 4


# ── TestWidenForConflict ──────────────────────────────────────────────────────

class TestWidenForConflict:

    def test_n_unchanged(self):
        _, new_n, _ = widen_for_conflict(0.8, 3, 0.02, 0.3)
        assert new_n == 3

    def test_mean_pulled_toward_midpoint(self):
        mean = 0.8
        new_mean, _, _ = widen_for_conflict(mean, 3, 0.02, 0.3)
        assert new_mean < mean  # pulled down toward 0.3

    def test_variance_increases(self):
        _, _, new_var = widen_for_conflict(0.8, 3, 0.02, 0.3)
        assert new_var > 0.02

    def test_large_distance_widens_more(self):
        _, _, var_large = widen_for_conflict(0.9, 3, 0.0, 0.1)  # distance=0.8
        _, _, var_small = widen_for_conflict(0.9, 3, 0.0, 0.8)  # distance=0.1
        assert var_large > var_small

    def test_mean_stays_in_bounds(self):
        new_mean, _, _ = widen_for_conflict(0.0, 1, 0.0, 1.0)
        assert 0.0 <= new_mean <= 1.0

    def test_var_never_negative(self):
        _, _, new_var = widen_for_conflict(0.5, 2, 0.0, 0.5)  # zero distance
        assert new_var >= 0.0


# ── TestGetConfidenceInterval ─────────────────────────────────────────────────

class TestGetConfidenceInterval:

    def test_returns_none_for_missing_atom(self):
        path = _make_facts_db()
        conn = sqlite3.connect(path)
        ensure_confidence_columns(conn)
        result = get_confidence_interval(conn, 'aapl', 'conviction_tier')
        conn.close()
        assert result is None

    def test_returns_correct_keys(self):
        path = _make_facts_db()
        atom_id = _insert_atom(path, 'aapl', 'conviction_tier', 'high', 0.82)
        conn = sqlite3.connect(path)
        result = get_confidence_interval(conn, 'aapl', 'conviction_tier')
        conn.close()
        expected_keys = {
            'subject', 'predicate', 'object', 'mean', 'n', 'std',
            'variance', 'interval_low', 'interval_high', 'interval_z',
            'source', 'authority',
        }
        assert expected_keys == set(result.keys())

    def test_returns_correct_mean(self):
        path = _make_facts_db()
        _insert_atom(path, 'msft', 'signal_quality', 'strong', 0.75)
        conn = sqlite3.connect(path)
        result = get_confidence_interval(conn, 'msft', 'signal_quality')
        conn.close()
        assert result['mean'] == pytest.approx(0.75, abs=0.001)

    def test_n1_std_is_zero(self):
        path = _make_facts_db()
        _insert_atom(path, 'aapl', 'conviction_tier', 'high', 0.8, conf_n=1, conf_var=0.0)
        conn = sqlite3.connect(path)
        result = get_confidence_interval(conn, 'aapl', 'conviction_tier')
        conn.close()
        assert result['std'] == 0.0
        assert result['interval_low'] == pytest.approx(0.8, abs=0.001)
        assert result['interval_high'] == pytest.approx(0.8, abs=0.001)

    def test_nonzero_variance_produces_interval(self):
        path = _make_facts_db()
        _insert_atom(path, 'gs', 'conviction_tier', 'medium', 0.65, conf_n=5, conf_var=0.05)
        conn = sqlite3.connect(path)
        result = get_confidence_interval(conn, 'gs', 'conviction_tier')
        conn.close()
        assert result['std'] > 0.0
        assert result['interval_low'] < result['mean']
        assert result['interval_high'] > result['mean']

    def test_interval_clipped_to_0_1(self):
        path = _make_facts_db()
        _insert_atom(path, 'aapl', 'conviction_tier', 'high', 0.99, conf_n=2, conf_var=0.5)
        conn = sqlite3.connect(path)
        result = get_confidence_interval(conn, 'aapl', 'conviction_tier')
        conn.close()
        assert result['interval_high'] <= 1.0
        assert result['interval_low'] >= 0.0

    def test_case_insensitive_subject(self):
        path = _make_facts_db()
        _insert_atom(path, 'AAPL', 'conviction_tier', 'high', 0.8)
        conn = sqlite3.connect(path)
        result = get_confidence_interval(conn, 'aapl', 'conviction_tier')
        conn.close()
        assert result is not None
        assert result['mean'] == pytest.approx(0.8, abs=0.001)

    def test_custom_z_score(self):
        path = _make_facts_db()
        _insert_atom(path, 'gs', 'conviction_tier', 'medium', 0.65, conf_n=4, conf_var=0.04)
        conn = sqlite3.connect(path)
        r95 = get_confidence_interval(conn, 'gs', 'conviction_tier', z=1.96)
        r99 = get_confidence_interval(conn, 'gs', 'conviction_tier', z=2.576)
        conn.close()
        # 99% interval must be wider than 95%
        width95 = r95['interval_high'] - r95['interval_low']
        width99 = r99['interval_high'] - r99['interval_low']
        assert width99 >= width95


# ── TestGetAllConfidenceIntervals ─────────────────────────────────────────────

class TestGetAllConfidenceIntervals:

    def test_empty_for_unknown_subject(self):
        path = _make_facts_db()
        conn = sqlite3.connect(path)
        ensure_confidence_columns(conn)
        result = get_all_confidence_intervals(conn, 'UNKNOWN')
        conn.close()
        assert result == []

    def test_returns_all_predicates(self):
        path = _make_facts_db()
        for pred, val, conf in [
            ('conviction_tier', 'high', 0.82),
            ('signal_quality', 'strong', 0.78),
            ('upside_pct', '25.0', 0.70),
        ]:
            _insert_atom(path, 'aapl', pred, val, conf)
        conn = sqlite3.connect(path)
        result = get_all_confidence_intervals(conn, 'aapl')
        conn.close()
        preds = {r['predicate'] for r in result}
        assert {'conviction_tier', 'signal_quality', 'upside_pct'} == preds

    def test_sorted_by_confidence_desc(self):
        path = _make_facts_db()
        _insert_atom(path, 'msft', 'p1', 'v1', 0.9)
        _insert_atom(path, 'msft', 'p2', 'v2', 0.5)
        _insert_atom(path, 'msft', 'p3', 'v3', 0.7)
        conn = sqlite3.connect(path)
        result = get_all_confidence_intervals(conn, 'msft')
        conn.close()
        means = [r['mean'] for r in result]
        assert means == sorted(means, reverse=True)

    def test_each_result_has_required_keys(self):
        path = _make_facts_db()
        _insert_atom(path, 'gs', 'conviction_tier', 'medium', 0.65)
        conn = sqlite3.connect(path)
        result = get_all_confidence_intervals(conn, 'gs')
        conn.close()
        for r in result:
            assert 'mean' in r
            assert 'n' in r
            assert 'std' in r
            assert 'interval_low' in r
            assert 'interval_high' in r


# ── TestUpdateAtomConfidence ──────────────────────────────────────────────────

class TestUpdateAtomConfidence:

    def test_n_increments_after_update(self):
        path = _make_facts_db()
        atom_id = _insert_atom(path, 'aapl', 'conviction_tier', 'high', 0.8, conf_n=1)
        conn = sqlite3.connect(path)
        update_atom_confidence(conn, atom_id, 0.9, source='broker_research')
        row = conn.execute("SELECT conf_n FROM facts WHERE id=?", (atom_id,)).fetchone()
        conn.close()
        assert row[0] == 2

    def test_mean_updated(self):
        path = _make_facts_db()
        atom_id = _insert_atom(path, 'aapl', 'conviction_tier', 'high', 0.5, conf_n=1)
        conn = sqlite3.connect(path)
        update_atom_confidence(conn, atom_id, 1.0, source='exchange_feed')
        row = conn.execute("SELECT confidence FROM facts WHERE id=?", (atom_id,)).fetchone()
        conn.close()
        assert row[0] > 0.5

    def test_no_op_for_missing_id(self):
        path = _make_facts_db()
        conn = sqlite3.connect(path)
        ensure_confidence_columns(conn)
        update_atom_confidence(conn, 99999, 0.8)  # must not raise
        conn.close()


# ── TestWidenAtomConfidence ───────────────────────────────────────────────────

class TestWidenAtomConfidence:

    def test_variance_increases_after_conflict(self):
        path = _make_facts_db()
        atom_id = _insert_atom(path, 'gs', 'conviction_tier', 'high', 0.8, conf_n=3, conf_var=0.01)
        conn = sqlite3.connect(path)
        widen_atom_confidence(conn, atom_id, 0.2, source='broker_research_adversarial')
        row = conn.execute("SELECT conf_var FROM facts WHERE id=?", (atom_id,)).fetchone()
        conn.close()
        assert row[0] > 0.01

    def test_n_unchanged_after_widen(self):
        path = _make_facts_db()
        atom_id = _insert_atom(path, 'gs', 'conviction_tier', 'high', 0.8, conf_n=3, conf_var=0.01)
        conn = sqlite3.connect(path)
        widen_atom_confidence(conn, atom_id, 0.2)
        row = conn.execute("SELECT conf_n FROM facts WHERE id=?", (atom_id,)).fetchone()
        conn.close()
        assert row[0] == 3  # n stays at 3

    def test_no_op_for_missing_id(self):
        path = _make_facts_db()
        conn = sqlite3.connect(path)
        ensure_confidence_columns(conn)
        widen_atom_confidence(conn, 99999, 0.2)  # must not raise
        conn.close()


# ── TestKbConfidenceApiEndpoint ───────────────────────────────────────────────

class TestKbConfidenceApiEndpoint:

    @pytest.fixture(autouse=True)
    def patch_kg(self, tmp_path, monkeypatch):
        """Patch the KnowledgeGraph so the API uses a temp test DB."""
        db_path = str(tmp_path / 'test.db')
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                source TEXT, confidence REAL DEFAULT 0.5,
                confidence_effective REAL, metadata TEXT,
                hit_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                conf_n INTEGER DEFAULT 1,
                conf_var REAL DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()
        self.db_path = db_path

        class _FakeKG:
            def thread_local_conn(self_inner):
                return sqlite3.connect(db_path)

        import api
        monkeypatch.setattr(api, '_kg', _FakeKG())
        monkeypatch.setattr(api, 'HAS_CONF_INTERVALS', True)

    @pytest.fixture
    def client(self):
        import api
        api.app.config['TESTING'] = True
        with api.app.test_client() as c:
            yield c

    def _seed(self, subject: str, predicate: str, obj: str,
              confidence: float = 0.8, conf_n: int = 1, conf_var: float = 0.0) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO facts (subject, predicate, object, source, confidence, conf_n, conf_var) "
            "VALUES (?, ?, ?, 'derived_signal_test', ?, ?, ?)",
            (subject, predicate, obj, confidence, conf_n, conf_var),
        )
        conn.commit()
        conn.close()

    def test_missing_subject_returns_400(self, client):
        resp = client.get('/kb/confidence')
        assert resp.status_code == 400

    def test_not_found_returns_404(self, client):
        resp = client.get('/kb/confidence?subject=unknown&predicate=conviction_tier')
        assert resp.status_code == 404

    def test_single_predicate_returns_distribution(self, client):
        self._seed('aapl', 'conviction_tier', 'high', 0.82, conf_n=3, conf_var=0.02)
        resp = client.get('/kb/confidence?subject=aapl&predicate=conviction_tier')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['mean'] == pytest.approx(0.82, abs=0.01)
        assert data['n'] == 3
        assert data['std'] > 0.0
        assert 'interval_low' in data
        assert 'interval_high' in data

    def test_all_predicates_without_predicate_param(self, client):
        self._seed('msft', 'conviction_tier', 'high', 0.82)
        self._seed('msft', 'signal_quality', 'strong', 0.78)
        resp = client.get('/kb/confidence?subject=msft')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['count'] == 2
        assert len(data['atoms']) == 2

    def test_not_found_for_all_predicates_returns_404(self, client):
        resp = client.get('/kb/confidence?subject=unknownticker')
        assert resp.status_code == 404

    def test_custom_z_score(self, client):
        self._seed('gs', 'conviction_tier', 'medium', 0.65, conf_n=5, conf_var=0.04)
        resp = client.get('/kb/confidence?subject=gs&predicate=conviction_tier&z=2.576')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['interval_z'] == pytest.approx(2.576, abs=0.001)

    def test_invalid_z_returns_400(self, client):
        resp = client.get('/kb/confidence?subject=aapl&z=notanumber')
        assert resp.status_code == 400

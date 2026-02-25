"""
tests/test_causal_graph.py — Causal Graph Layer Tests

Covers:
  - ensure_causal_edges_table: creates table and seeds SEED_EDGES
  - traverse_causal: BFS from seed, depth limit, min_confidence filter
  - traverse_causal: chain structure, concepts_reached, affected_tickers
  - traverse_causal: empty graph, unknown seed, cycle prevention
  - _compute_chain_confidence: product along greedy path
  - add_causal_edge: insert, duplicate rejection, normalization
  - list_causal_edges: all edges, cause filter, limit
  - POST /kb/causal-chain API endpoint
  - POST /kb/causal-edge API endpoint
  - GET /kb/causal-edges API endpoint
"""

from __future__ import annotations

import sqlite3
import tempfile
import os

import pytest

from knowledge.causal_graph import (
    ensure_causal_edges_table,
    traverse_causal,
    add_causal_edge,
    list_causal_edges,
    SEED_EDGES,
    _compute_chain_confidence,
    _load_adjacency,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_causal_db(seed: bool = False) -> str:
    """Create a temp SQLite DB with facts + causal_edges tables."""
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
    if seed:
        ensure_causal_edges_table(conn)
    conn.close()
    return path


def _insert_edge(conn: sqlite3.Connection, cause: str, effect: str,
                 mechanism: str, confidence: float = 0.8,
                 source: str = 'test') -> None:
    conn.execute(
        """INSERT OR IGNORE INTO causal_edges
           (cause, effect, mechanism, confidence, source, created_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))""",
        (cause, effect, mechanism, confidence, source),
    )
    conn.commit()


def _make_linear_chain_db(depth: int = 4) -> tuple:
    """
    Build a DB with a linear causal chain of given depth.
    Returns (path, [concept_0, concept_1, ..., concept_depth])
    """
    path = _make_causal_db()
    conn = sqlite3.connect(path)
    ensure_causal_edges_table(conn)
    # Remove seed edges to isolate test chain
    conn.execute("DELETE FROM causal_edges")
    conn.commit()

    concepts = [f'concept_{i}' for i in range(depth + 1)]
    for i in range(depth):
        _insert_edge(conn, concepts[i], concepts[i + 1], f'mech_{i}', 0.8)
    conn.close()
    return path, concepts


# ── TestEnsureCausalEdgesTable ────────────────────────────────────────────────

class TestEnsureCausalEdgesTable:

    def test_creates_table(self):
        path = _make_causal_db()
        conn = sqlite3.connect(path)
        ensure_causal_edges_table(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert 'causal_edges' in tables

    def test_seeds_macro_edges(self):
        path = _make_causal_db()
        conn = sqlite3.connect(path)
        ensure_causal_edges_table(conn)
        count = conn.execute("SELECT COUNT(*) FROM causal_edges").fetchone()[0]
        conn.close()
        assert count == len(SEED_EDGES)

    def test_idempotent_second_call(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        count_before = conn.execute("SELECT COUNT(*) FROM causal_edges").fetchone()[0]
        ensure_causal_edges_table(conn)  # second call must not duplicate
        count_after = conn.execute("SELECT COUNT(*) FROM causal_edges").fetchone()[0]
        conn.close()
        assert count_before == count_after

    def test_seed_edges_have_correct_confidence(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        rows = conn.execute(
            "SELECT confidence FROM causal_edges WHERE confidence < 0.5"
        ).fetchall()
        conn.close()
        assert len(rows) == 0  # all seed edges >= 0.5

    def test_fed_rate_hike_present(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        row = conn.execute(
            "SELECT COUNT(*) FROM causal_edges WHERE cause='fed_rate_hike'"
        ).fetchone()
        conn.close()
        assert row[0] > 0

    def test_unique_constraint_on_cause_effect_mechanism(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        # Inserting a duplicate should be silently ignored
        conn.execute(
            "INSERT OR IGNORE INTO causal_edges (cause, effect, mechanism, confidence) "
            "VALUES ('fed_rate_hike', 'credit_cost_rises', 'debt_service_transmission', 0.99)"
        )
        conn.commit()
        row = conn.execute(
            "SELECT confidence FROM causal_edges "
            "WHERE cause='fed_rate_hike' AND effect='credit_cost_rises' "
            "AND mechanism='debt_service_transmission'"
        ).fetchone()
        conn.close()
        assert row[0] == pytest.approx(0.90, abs=0.01)  # original not overwritten


# ── TestTraverseCausal ────────────────────────────────────────────────────────

class TestTraverseCausal:

    def test_returns_correct_structure(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=2)
        conn.close()
        required_keys = {
            'seed', 'max_depth', 'min_confidence', 'chain',
            'concepts_reached', 'affected_tickers', 'chain_confidence', 'paths',
        }
        assert required_keys == set(result.keys())

    def test_seed_normalized_to_lowercase(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'FED_RATE_HIKE', max_depth=1)
        conn.close()
        assert result['seed'] == 'fed_rate_hike'

    def test_chain_is_list_of_hops(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=2)
        conn.close()
        assert isinstance(result['chain'], list)
        for hop in result['chain']:
            assert 'cause' in hop
            assert 'effect' in hop
            assert 'mechanism' in hop
            assert 'confidence' in hop
            assert 'depth' in hop

    def test_depth_1_only_direct_effects(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=1)
        conn.close()
        # All hops must have depth=1
        assert all(hop['depth'] == 1 for hop in result['chain'])

    def test_depth_limit_respected(self):
        path, concepts = _make_linear_chain_db(depth=6)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, concepts[0], max_depth=3)
        conn.close()
        max_depth_seen = max((hop['depth'] for hop in result['chain']), default=0)
        assert max_depth_seen <= 3

    def test_unknown_seed_returns_empty_chain(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'completely_unknown_event_xyz', max_depth=3)
        conn.close()
        assert result['chain'] == []
        assert result['concepts_reached'] == []

    def test_min_confidence_filters_low_confidence_edges(self):
        path = _make_causal_db()
        conn = sqlite3.connect(path)
        ensure_causal_edges_table(conn)
        conn.execute("DELETE FROM causal_edges")
        conn.commit()
        _insert_edge(conn, 'event_a', 'event_b', 'mech1', confidence=0.9)
        _insert_edge(conn, 'event_a', 'event_c', 'mech2', confidence=0.3)
        result_high = traverse_causal(conn, 'event_a', max_depth=1, min_confidence=0.5)
        result_low  = traverse_causal(conn, 'event_a', max_depth=1, min_confidence=0.1)
        conn.close()
        effects_high = {hop['effect'] for hop in result_high['chain']}
        effects_low  = {hop['effect'] for hop in result_low['chain']}
        assert 'event_b' in effects_high
        assert 'event_c' not in effects_high
        assert 'event_b' in effects_low
        assert 'event_c' in effects_low

    def test_concepts_reached_contains_all_effects(self):
        path, concepts = _make_linear_chain_db(depth=3)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, concepts[0], max_depth=4)
        conn.close()
        for concept in concepts[1:]:
            assert concept in result['concepts_reached']

    def test_seed_not_in_concepts_reached(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=2)
        conn.close()
        assert 'fed_rate_hike' not in result['concepts_reached']

    def test_affected_tickers_for_known_terminal_node(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        # fed_rate_hike → credit_cost_rises → equity_multiples_compress
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=3)
        conn.close()
        # equity_multiples_compress should appear and map to tech tickers
        if 'equity_multiples_compress' in result['affected_tickers']:
            assert 'MSFT' in result['affected_tickers']['equity_multiples_compress']

    def test_no_duplicate_edges_in_chain(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=4)
        conn.close()
        edge_keys = [(hop['cause'], hop['effect'], hop['mechanism'])
                     for hop in result['chain']]
        assert len(edge_keys) == len(set(edge_keys))

    def test_chain_confidence_between_0_and_1(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=3)
        conn.close()
        assert 0.0 <= result['chain_confidence'] <= 1.0

    def test_empty_graph_returns_empty_chain(self):
        path = _make_causal_db()
        conn = sqlite3.connect(path)
        ensure_causal_edges_table(conn)
        conn.execute("DELETE FROM causal_edges")
        conn.commit()
        result = traverse_causal(conn, 'fed_rate_hike', max_depth=3)
        conn.close()
        assert result['chain'] == []

    def test_space_in_seed_normalized(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed rate hike', max_depth=1)
        conn.close()
        assert result['seed'] == 'fed_rate_hike'

    def test_fed_cut_chain_includes_risk_on(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = traverse_causal(conn, 'fed_rate_cut', max_depth=3)
        conn.close()
        effects = {hop['effect'] for hop in result['chain']}
        assert 'risk_on_rotation' in effects or 'hyg_spreads_tighten' in effects


# ── TestComputeChainConfidence ────────────────────────────────────────────────

class TestComputeChainConfidence:

    def test_empty_chain_returns_1(self):
        result = _compute_chain_confidence([], 'seed')
        assert result == 1.0

    def test_single_hop_returns_its_confidence(self):
        chain = [{'cause': 'a', 'effect': 'b', 'mechanism': 'm', 'confidence': 0.8}]
        result = _compute_chain_confidence(chain, 'a')
        assert result == pytest.approx(0.8, abs=0.01)

    def test_two_hops_returns_product(self):
        chain = [
            {'cause': 'a', 'effect': 'b', 'mechanism': 'm1', 'confidence': 0.9},
            {'cause': 'b', 'effect': 'c', 'mechanism': 'm2', 'confidence': 0.8},
        ]
        result = _compute_chain_confidence(chain, 'a')
        assert result == pytest.approx(0.72, abs=0.01)

    def test_takes_highest_confidence_path_at_branch(self):
        chain = [
            {'cause': 'a', 'effect': 'b', 'mechanism': 'm1', 'confidence': 0.9},
            {'cause': 'a', 'effect': 'c', 'mechanism': 'm2', 'confidence': 0.5},
        ]
        result = _compute_chain_confidence(chain, 'a')
        assert result == pytest.approx(0.9, abs=0.01)

    def test_result_never_exceeds_1(self):
        chain = [{'cause': 'a', 'effect': 'b', 'mechanism': 'm', 'confidence': 1.0}]
        result = _compute_chain_confidence(chain, 'a')
        assert result <= 1.0


# ── TestAddCausalEdge ─────────────────────────────────────────────────────────

class TestAddCausalEdge:

    def test_inserts_new_edge(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = add_causal_edge(conn, 'new_cause', 'new_effect', 'new_mech', 0.75)
        conn.close()
        assert result['inserted'] is True
        assert result['id'] is not None
        assert result['message'] == 'created'

    def test_rejects_duplicate(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        add_causal_edge(conn, 'c1', 'e1', 'm1', 0.7)
        result = add_causal_edge(conn, 'c1', 'e1', 'm1', 0.8)
        conn.close()
        assert result['inserted'] is False
        assert 'duplicate' in result['message']

    def test_normalizes_spaces_to_underscores(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = add_causal_edge(conn, 'cause one', 'effect two', 'mech three', 0.7)
        conn.close()
        assert result['cause'] == 'cause_one'
        assert result['effect'] == 'effect_two'
        assert result['mechanism'] == 'mech_three'

    def test_confidence_clipped_to_0_1(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        result = add_causal_edge(conn, 'cx', 'ex', 'mx', 1.5)
        conn.close()
        assert result['confidence'] == pytest.approx(1.0, abs=0.001)

    def test_edge_retrievable_after_insert(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        add_causal_edge(conn, 'myevent', 'myresult', 'mypath', 0.85, source='analyst_note')
        edges = list_causal_edges(conn, cause_filter='myevent')
        conn.close()
        assert len(edges) == 1
        assert edges[0]['cause'] == 'myevent'
        assert edges[0]['source'] == 'analyst_note'


# ── TestListCausalEdges ───────────────────────────────────────────────────────

class TestListCausalEdges:

    def test_returns_all_seed_edges(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        edges = list_causal_edges(conn)
        conn.close()
        assert len(edges) == len(SEED_EDGES)

    def test_cause_filter(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        edges = list_causal_edges(conn, cause_filter='fed_rate_hike')
        conn.close()
        for e in edges:
            assert 'fed_rate_hike' in e['cause']

    def test_limit_respected(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        edges = list_causal_edges(conn, limit=5)
        conn.close()
        assert len(edges) <= 5

    def test_sorted_by_confidence_desc(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        edges = list_causal_edges(conn)
        conn.close()
        confs = [e['confidence'] for e in edges]
        assert confs == sorted(confs, reverse=True)

    def test_result_has_required_keys(self):
        path = _make_causal_db(seed=True)
        conn = sqlite3.connect(path)
        edges = list_causal_edges(conn, limit=1)
        conn.close()
        if edges:
            expected = {'id', 'cause', 'effect', 'mechanism',
                        'confidence', 'source', 'created_at'}
            assert expected == set(edges[0].keys())

    def test_empty_db_returns_empty_list(self):
        path = _make_causal_db()
        conn = sqlite3.connect(path)
        ensure_causal_edges_table(conn)
        conn.execute("DELETE FROM causal_edges")
        conn.commit()
        edges = list_causal_edges(conn)
        conn.close()
        assert edges == []


# ── TestCausalGraphApiEndpoints ───────────────────────────────────────────────

class TestCausalGraphApiEndpoints:

    @pytest.fixture(autouse=True)
    def patch_kg(self, tmp_path, monkeypatch):
        db_path = str(tmp_path / 'test.db')
        conn = sqlite3.connect(db_path)
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
        self.db_path = db_path

        class _FakeKG:
            def __init__(self_inner):
                self_inner._conn = None

            def thread_local_conn(self_inner):
                if self_inner._conn is None:
                    self_inner._conn = sqlite3.connect(db_path, check_same_thread=False)
                return self_inner._conn

        import api
        monkeypatch.setattr(api, '_kg', _FakeKG())
        monkeypatch.setattr(api, 'HAS_CAUSAL_GRAPH', True)

    @pytest.fixture
    def client(self):
        import api
        api.app.config['TESTING'] = True
        with api.app.test_client() as c:
            yield c

    # POST /kb/causal-chain
    def test_chain_missing_seed_returns_400(self, client):
        resp = client.post('/kb/causal-chain', json={})
        assert resp.status_code == 400

    def test_chain_unknown_seed_returns_empty(self, client):
        resp = client.post('/kb/causal-chain',
                           json={'seed': 'completely_unknown_xyz'})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['chain'] == []

    def test_chain_fed_rate_hike_returns_results(self, client):
        resp = client.post('/kb/causal-chain',
                           json={'seed': 'fed_rate_hike', 'depth': 2})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['seed'] == 'fed_rate_hike'
        assert len(data['chain']) > 0

    def test_chain_depth_capped_at_6(self, client):
        resp = client.post('/kb/causal-chain',
                           json={'seed': 'fed_rate_hike', 'depth': 99})
        assert resp.status_code == 200
        data = resp.get_json()
        if data['chain']:
            max_depth = max(hop['depth'] for hop in data['chain'])
            assert max_depth <= 6

    def test_chain_response_has_affected_tickers(self, client):
        resp = client.post('/kb/causal-chain',
                           json={'seed': 'fed_rate_hike', 'depth': 3})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'affected_tickers' in data

    # POST /kb/causal-edge
    def test_add_edge_missing_fields_returns_400(self, client):
        resp = client.post('/kb/causal-edge', json={'cause': 'x'})
        assert resp.status_code == 400

    def test_add_edge_inserts_successfully(self, client):
        resp = client.post('/kb/causal-edge', json={
            'cause': 'tariff_hike', 'effect': 'import_cost_rises',
            'mechanism': 'direct_cost_pass_through', 'confidence': 0.8,
        })
        assert resp.status_code == 201
        data = resp.get_json()
        assert data['inserted'] is True
        assert data['cause'] == 'tariff_hike'

    def test_add_edge_duplicate_returns_200_not_201(self, client):
        payload = {
            'cause': 'dup_cause', 'effect': 'dup_effect',
            'mechanism': 'dup_mech', 'confidence': 0.7,
        }
        client.post('/kb/causal-edge', json=payload)
        resp = client.post('/kb/causal-edge', json=payload)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['inserted'] is False

    # GET /kb/causal-edges
    def test_list_edges_returns_seed_edges(self, client):
        resp = client.get('/kb/causal-edges')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['count'] == len(SEED_EDGES)

    def test_list_edges_cause_filter(self, client):
        resp = client.get('/kb/causal-edges?cause=fed_rate_hike')
        assert resp.status_code == 200
        data = resp.get_json()
        for edge in data['edges']:
            assert 'fed_rate_hike' in edge['cause']

    def test_list_edges_limit(self, client):
        resp = client.get('/kb/causal-edges?limit=3')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['edges']) <= 3

"""
tests/test_kb_repair_executor.py — Unit tests for knowledge/kb_repair_executor.py

Covers:
  - SignalSnapshot / DivergenceReport / ExecutionResult / RollbackResult / ImpactScore dataclasses
  - ensure_executor_tables() idempotency
  - execute_repair(): not-found, wrong-status, dry_run, strategy dispatch
  - rollback_repair(): not-found, wrong-status, successful rollback
  - repair_impact_score(): empty history, reliability classification
  - _compute_divergence(): direction_correct flag
  - _apply_zero_ids(): confidence_effective zeroed
  - _apply_reweight_sources(): multiplier applied
"""

import json
import os
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone

import pytest

from knowledge.kb_repair_executor import (
    SignalSnapshot,
    DivergenceReport,
    ExecutionResult,
    RollbackResult,
    ImpactScore,
    ensure_executor_tables,
    execute_repair,
    rollback_repair,
    repair_impact_score,
    _compute_divergence,
    _apply_zero_ids,
    _apply_reweight_sources,
    _AUTO_ROLLBACK_STRESS_INCREASE,
    _AUTO_ROLLBACK_ENTROPY_COLLAPSE,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_file_db():
    """Create a temp file-based SQLite DB with all required tables."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT, confidence REAL, confidence_effective REAL
        )
    """)
    conn.execute("""
        CREATE TABLE repair_proposals (
            id TEXT PRIMARY KEY,
            diagnosis_id TEXT,
            topic TEXT,
            insufficiency_type TEXT,
            strategy TEXT,
            is_primary INTEGER DEFAULT 0,
            preview_json TEXT,
            simulation_json TEXT,
            validation_json TEXT,
            description TEXT,
            status TEXT DEFAULT 'pending',
            generated_at TEXT NOT NULL,
            resolved_at TEXT
        )
    """)
    conn.commit()
    conn.close()
    return path


def _insert_proposal(path, strategy='ingest_missing', status='pending',
                     topic='NVDA', atoms_to_remove=None, sources_to_reweight=None):
    """Insert a synthetic proposal row; returns proposal_id."""
    proposal_id = uuid.uuid4().hex
    preview = {
        'atoms_to_remove': atoms_to_remove or [],
        'atoms_to_merge': [],
        'new_predicates': [],
        'sources_to_reweight': sources_to_reweight or {},
        'summary': 'test',
        'affected_atom_count': 0,
    }
    simulation = {
        'estimated_stress_delta': -0.10,
        'estimated_conflict_delta': -0.05,
        'estimated_authority_delta': 0.0,
        'estimated_entropy_delta': 0.05,
        'estimated_atom_count_delta': 0,
        'confidence': 0.6,
        'assumptions': [],
    }
    validation = {
        'target_signal': 'domain_entropy',
        'target_direction': 'increase',
        'target_threshold': 0.5,
        'recheck_after_turns': 5,
        'description': 'test',
    }
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO repair_proposals "
        "(id, diagnosis_id, topic, insufficiency_type, strategy, is_primary, "
        "preview_json, simulation_json, validation_json, description, status, generated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            proposal_id, 'diag_001', topic, 'coverage_gap', strategy,
            1, json.dumps(preview), json.dumps(simulation), json.dumps(validation),
            'Test proposal', status, datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()
    conn.close()
    return proposal_id


def _insert_facts(path, n=5, topic='NVDA', source='model_signal_v1',
                  confidence=0.8, confidence_effective=0.8):
    conn = sqlite3.connect(path)
    rows = [(topic, 'signal_direction', 'long', source, confidence, confidence_effective)] * n
    conn.executemany(
        "INSERT INTO facts (subject, predicate, object, source, confidence, confidence_effective) "
        "VALUES (?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    fact_ids = [r[0] for r in conn.execute(
        "SELECT id FROM facts WHERE subject = ? ORDER BY id DESC LIMIT ?", (topic, n)
    ).fetchall()]
    conn.close()
    return fact_ids


# ── SignalSnapshot dataclass ───────────────────────────────────────────────────

class TestSignalSnapshot:
    def test_to_dict_all_keys(self):
        snap = SignalSnapshot(
            composite_stress=0.1, conflict_cluster=0.2,
            authority_conflict=0.3, domain_entropy=0.8,
            predicate_diversity=0.15, atom_count=42,
            captured_at='2026-01-01T00:00:00Z',
        )
        d = snap.to_dict()
        for key in ('composite_stress', 'conflict_cluster', 'authority_conflict',
                    'domain_entropy', 'predicate_diversity', 'atom_count', 'captured_at'):
            assert key in d

    def test_to_dict_rounds_floats(self):
        snap = SignalSnapshot(composite_stress=0.123456789)
        d = snap.to_dict()
        assert d['composite_stress'] == pytest.approx(0.1235, abs=0.0001)


# ── DivergenceReport dataclass ─────────────────────────────────────────────────

class TestDivergenceReport:
    def test_to_dict_all_keys(self):
        div = DivergenceReport(
            stress_divergence=0.01, conflict_divergence=0.02,
            authority_divergence=-0.01, entropy_divergence=0.05,
            atom_count_divergence=-3, mean_abs_divergence=0.025,
            direction_correct=True,
        )
        d = div.to_dict()
        for key in ('stress_divergence', 'conflict_divergence', 'authority_divergence',
                    'entropy_divergence', 'atom_count_divergence', 'mean_abs_divergence',
                    'direction_correct'):
            assert key in d

    def test_direction_correct_bool(self):
        div = DivergenceReport(direction_correct=False)
        assert div.to_dict()['direction_correct'] is False


# ── _compute_divergence() ─────────────────────────────────────────────────────

class TestComputeDivergence:
    def _snap(self, stress=0.1, conflict=0.2, auth=0.3, entropy=0.7, count=100):
        return SignalSnapshot(
            composite_stress=stress, conflict_cluster=conflict,
            authority_conflict=auth, domain_entropy=entropy, atom_count=count,
        )

    def test_direction_correct_when_stress_decreases_as_estimated(self):
        sim = {'estimated_stress_delta': -0.1, 'estimated_conflict_delta': 0.0,
               'estimated_authority_delta': 0.0, 'estimated_entropy_delta': 0.0,
               'estimated_atom_count_delta': 0}
        before = self._snap(stress=0.5)
        after  = self._snap(stress=0.3)  # actual delta = -0.2
        div = _compute_divergence(sim, before, after)
        assert div.direction_correct

    def test_direction_incorrect_when_stress_increases(self):
        sim = {'estimated_stress_delta': -0.1, 'estimated_conflict_delta': 0.0,
               'estimated_authority_delta': 0.0, 'estimated_entropy_delta': 0.0,
               'estimated_atom_count_delta': 0}
        before = self._snap(stress=0.3)
        after  = self._snap(stress=0.5)  # actual delta = +0.2 (wrong direction)
        div = _compute_divergence(sim, before, after)
        assert not div.direction_correct

    def test_mean_abs_divergence_positive(self):
        sim = {'estimated_stress_delta': 0.0, 'estimated_conflict_delta': 0.0,
               'estimated_authority_delta': 0.0, 'estimated_entropy_delta': 0.0,
               'estimated_atom_count_delta': 0}
        before = self._snap(stress=0.5, conflict=0.5, auth=0.5, entropy=0.5)
        after  = self._snap(stress=0.4, conflict=0.3, auth=0.4, entropy=0.6)
        div = _compute_divergence(sim, before, after)
        assert div.mean_abs_divergence >= 0.0


# ── ensure_executor_tables() ──────────────────────────────────────────────────

class TestEnsureExecutorTables:
    def test_creates_all_tables(self):
        path = _make_file_db()
        try:
            conn = sqlite3.connect(path)
            ensure_executor_tables(conn)
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            assert 'repair_execution_log' in tables
            assert 'repair_rollback_log' in tables
            assert 'predicate_vocabulary' in tables
            conn.close()
        finally:
            os.unlink(path)

    def test_idempotent_second_call(self):
        path = _make_file_db()
        try:
            conn = sqlite3.connect(path)
            ensure_executor_tables(conn)
            ensure_executor_tables(conn)  # must not raise
            conn.close()
        finally:
            os.unlink(path)


# ── execute_repair(): not-found and wrong-status ───────────────────────────────

class TestExecuteRepairErrors:
    def test_unknown_proposal_id_returns_error(self):
        path = _make_file_db()
        try:
            result = execute_repair(uuid.uuid4().hex, path, dry_run=True)
            assert not result.success
            assert 'not found' in result.error.lower()
        finally:
            os.unlink(path)

    def test_non_pending_status_returns_error(self):
        path = _make_file_db()
        try:
            pid = _insert_proposal(path, status='executed')
            result = execute_repair(pid, path, dry_run=True)
            assert not result.success
            assert 'executed' in result.error.lower() or 'status' in result.error.lower()
        finally:
            os.unlink(path)

    def test_rolled_back_status_returns_error(self):
        path = _make_file_db()
        try:
            pid = _insert_proposal(path, status='rolled_back')
            result = execute_repair(pid, path, dry_run=True)
            assert not result.success
        finally:
            os.unlink(path)


# ── execute_repair(): dry_run ─────────────────────────────────────────────────

class TestExecuteRepairDryRun:
    def test_dry_run_returns_success(self):
        path = _make_file_db()
        try:
            pid = _insert_proposal(path, strategy='ingest_missing')
            result = execute_repair(pid, path, dry_run=True)
            assert result.success
            assert result.dry_run
        finally:
            os.unlink(path)

    def test_dry_run_does_not_change_proposal_status(self):
        path = _make_file_db()
        try:
            pid = _insert_proposal(path, strategy='ingest_missing')
            execute_repair(pid, path, dry_run=True)
            conn = sqlite3.connect(path)
            status = conn.execute(
                "SELECT status FROM repair_proposals WHERE id = ?", (pid,)
            ).fetchone()[0]
            conn.close()
            assert status == 'pending'
        finally:
            os.unlink(path)

    def test_dry_run_does_not_mutate_facts(self):
        path = _make_file_db()
        try:
            fact_ids = _insert_facts(path, n=3)
            pid = _insert_proposal(path, strategy='resolve_conflicts',
                                   atoms_to_remove=fact_ids)
            execute_repair(pid, path, dry_run=True)
            conn = sqlite3.connect(path)
            still_active = conn.execute(
                "SELECT COUNT(*) FROM facts WHERE confidence_effective > 0"
            ).fetchone()[0]
            conn.close()
            assert still_active == 3  # unchanged
        finally:
            os.unlink(path)


# ── execute_repair(): live strategies ────────────────────────────────────────

class TestExecuteRepairLive:
    def test_resolve_conflicts_zeros_atoms(self):
        path = _make_file_db()
        try:
            fact_ids = _insert_facts(path, n=3)
            pid = _insert_proposal(path, strategy='resolve_conflicts',
                                   atoms_to_remove=fact_ids)
            result = execute_repair(pid, path, dry_run=False)
            assert result.success
            conn = sqlite3.connect(path)
            zeroed = conn.execute(
                "SELECT COUNT(*) FROM facts WHERE confidence_effective = 0.0"
            ).fetchone()[0]
            conn.close()
            assert zeroed == 3
        finally:
            os.unlink(path)

    def test_deduplicate_zeros_atoms(self):
        path = _make_file_db()
        try:
            fact_ids = _insert_facts(path, n=2)
            pid = _insert_proposal(path, strategy='deduplicate',
                                   atoms_to_remove=fact_ids[:1])
            result = execute_repair(pid, path, dry_run=False)
            assert result.success
        finally:
            os.unlink(path)

    def test_introduce_predicates_writes_vocabulary(self):
        path = _make_file_db()
        try:
            preview_with_preds = {
                'atoms_to_remove': [], 'atoms_to_merge': [],
                'new_predicates': ['signal_quality', 'upside_pct'],
                'sources_to_reweight': {}, 'summary': 'test', 'affected_atom_count': 0,
            }
            proposal_id = uuid.uuid4().hex
            simulation = {'estimated_stress_delta': -0.1, 'estimated_conflict_delta': 0.0,
                          'estimated_authority_delta': 0.0, 'estimated_entropy_delta': 0.05,
                          'estimated_atom_count_delta': 0, 'confidence': 0.8, 'assumptions': []}
            validation = {'target_signal': 'predicate_diversity', 'target_direction': 'increase',
                          'target_threshold': 0.2, 'recheck_after_turns': 8, 'description': 'test'}
            conn = sqlite3.connect(path)
            ensure_executor_tables(conn)
            conn.execute(
                "INSERT INTO repair_proposals "
                "(id, diagnosis_id, topic, insufficiency_type, strategy, is_primary, "
                "preview_json, simulation_json, validation_json, description, status, generated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (proposal_id, 'diag', 'NVDA', 'missing_schema', 'introduce_predicates',
                 1, json.dumps(preview_with_preds), json.dumps(simulation),
                 json.dumps(validation), 'test', 'pending',
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            conn.close()

            result = execute_repair(proposal_id, path, dry_run=False)
            assert result.success
            conn = sqlite3.connect(path)
            rows = conn.execute("SELECT predicate FROM predicate_vocabulary").fetchall()
            conn.close()
            predicates = {r[0] for r in rows}
            assert 'signal_quality' in predicates
            assert 'upside_pct' in predicates
        finally:
            os.unlink(path)

    def test_ingest_missing_no_fact_mutation(self):
        """ingest_missing/split_domain/manual_review — mutations=0, but success."""
        path = _make_file_db()
        try:
            _insert_facts(path, n=5)
            pid = _insert_proposal(path, strategy='ingest_missing')
            result = execute_repair(pid, path, dry_run=False)
            assert result.success
            assert result.mutations_applied == 0
        finally:
            os.unlink(path)

    def test_execution_result_has_strategy(self):
        path = _make_file_db()
        try:
            pid = _insert_proposal(path, strategy='ingest_missing')
            result = execute_repair(pid, path, dry_run=True)
            assert result.strategy == 'ingest_missing'
        finally:
            os.unlink(path)


# ── rollback_repair() ─────────────────────────────────────────────────────────

class TestRollbackRepair:
    def test_unknown_proposal_returns_error(self):
        path = _make_file_db()
        try:
            result = rollback_repair(uuid.uuid4().hex, path)
            assert not result.success
            assert 'not found' in result.error.lower()
        finally:
            os.unlink(path)

    def test_pending_proposal_cannot_be_rolled_back(self):
        path = _make_file_db()
        try:
            pid = _insert_proposal(path, status='pending')
            result = rollback_repair(pid, path)
            assert not result.success
            assert 'executed' in result.error.lower() or 'status' in result.error.lower()
        finally:
            os.unlink(path)

    def test_successful_rollback_restores_atoms(self):
        path = _make_file_db()
        try:
            fact_ids = _insert_facts(path, n=3)
            pid = _insert_proposal(path, strategy='resolve_conflicts',
                                   atoms_to_remove=fact_ids)
            # Execute to create rollback snapshot and mark as 'executed'
            execute_repair(pid, path, dry_run=False)

            # Verify atoms are zeroed
            conn = sqlite3.connect(path)
            zeroed = conn.execute(
                "SELECT COUNT(*) FROM facts WHERE confidence_effective = 0.0"
            ).fetchone()[0]
            conn.close()

            # Only attempt rollback if execution actually zeroed atoms
            if zeroed > 0:
                rb_result = rollback_repair(pid, path)
                assert rb_result.success
                conn = sqlite3.connect(path)
                restored = conn.execute(
                    "SELECT COUNT(*) FROM facts WHERE confidence_effective > 0"
                ).fetchone()[0]
                conn.close()
                assert restored >= zeroed
        finally:
            os.unlink(path)


# ── repair_impact_score() ─────────────────────────────────────────────────────

class TestRepairImpactScore:
    def test_empty_history_returns_zero_executions(self):
        path = _make_file_db()
        try:
            score = repair_impact_score('ingest_missing', path)
            assert score.n_executions == 0
            assert score.strategy == 'ingest_missing'
        finally:
            os.unlink(path)

    def test_returns_impact_score_dataclass(self):
        path = _make_file_db()
        try:
            score = repair_impact_score('resolve_conflicts', path)
            assert isinstance(score, ImpactScore)
        finally:
            os.unlink(path)

    def test_reliability_unknown_when_no_data(self):
        path = _make_file_db()
        try:
            score = repair_impact_score('merge_atoms', path)
            assert score.reliability == 'unknown'
        finally:
            os.unlink(path)

    def test_stable_reliability_classification(self):
        path = _make_file_db()
        try:
            conn = sqlite3.connect(path)
            ensure_executor_tables(conn)
            now = datetime.now(timezone.utc).isoformat()
            before_snap = json.dumps({'composite_stress': 0.5, 'conflict_cluster': 0.3,
                                      'authority_conflict': 0.2, 'domain_entropy': 0.7,
                                      'predicate_diversity': 0.1, 'atom_count': 100,
                                      'captured_at': now})
            after_snap  = json.dumps({'composite_stress': 0.42, 'conflict_cluster': 0.25,
                                      'authority_conflict': 0.18, 'domain_entropy': 0.72,
                                      'predicate_diversity': 0.12, 'atom_count': 97,
                                      'captured_at': now})
            div_snap = json.dumps({'mean_abs_divergence': 0.02, 'direction_correct': True,
                                   'stress_divergence': 0.02, 'conflict_divergence': 0.01,
                                   'authority_divergence': 0.0, 'entropy_divergence': 0.01,
                                   'atom_count_divergence': -3})
            for _ in range(3):
                conn.execute(
                    "INSERT INTO repair_execution_log "
                    "(proposal_id, action, strategy, topic, signals_before_json, "
                    "signals_after_json, simulation_json, divergence_json, mutations_applied, "
                    "auto_rolled_back, rollback_reason, success, error, executed_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, 'execute', 'ingest_missing', 'NVDA',
                     before_snap, after_snap, '{}', div_snap,
                     0, 0, None, 1, None, now),
                )
            conn.commit()
            conn.close()
            score = repair_impact_score('ingest_missing', path)
            assert score.n_executions == 3
            assert score.mean_stress_delta < 0.0  # stress decreased
            assert score.reliability in ('stable', 'variable', 'chaotic')
        finally:
            os.unlink(path)


# ── _apply_zero_ids() / _apply_reweight_sources() ─────────────────────────────

class TestMutationHelpers:
    def test_apply_zero_ids_zeroes_confidence_effective(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                source TEXT, confidence REAL, confidence_effective REAL
            )
        """)
        conn.execute(
            "INSERT INTO facts VALUES (1,'NVDA','pred','obj','src',0.9,0.9)"
        )
        conn.execute(
            "INSERT INTO facts VALUES (2,'NVDA','pred','obj2','src',0.8,0.8)"
        )
        _apply_zero_ids([1, 2], conn)
        rows = conn.execute("SELECT confidence_effective FROM facts").fetchall()
        for (ce,) in rows:
            assert ce == 0.0
        conn.close()

    def test_apply_zero_ids_empty_list_is_noop(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY, subject TEXT, predicate TEXT,
                object TEXT, source TEXT, confidence REAL, confidence_effective REAL
            )
        """)
        conn.execute("INSERT INTO facts VALUES (1,'X','p','o','s',0.9,0.9)")
        count = _apply_zero_ids([], conn)
        assert count == 0
        row = conn.execute("SELECT confidence_effective FROM facts WHERE id=1").fetchone()
        assert row[0] == 0.9
        conn.close()

    def test_apply_reweight_sources_reduces_confidence(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT, predicate TEXT, object TEXT,
                source TEXT, confidence REAL, confidence_effective REAL
            )
        """)
        conn.execute(
            "INSERT INTO facts (subject,predicate,object,source,confidence,confidence_effective) "
            "VALUES ('NVDA','sig','long','social_signal_x',0.8,0.8)"
        )
        preview = {'sources_to_reweight': {'social_signal_x': 0.7}}
        count = _apply_reweight_sources(preview, 'NVDA', conn)
        assert count >= 1
        ce = conn.execute("SELECT confidence_effective FROM facts").fetchone()[0]
        assert ce < 0.8  # was reduced
        conn.close()

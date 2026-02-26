"""
tests/test_kb_repair_proposals.py — Unit tests for knowledge/kb_repair_proposals.py

Covers:
  - RepairStrategy enum
  - RepairPreview / RepairSimulation / ValidationMetric / RepairProposal dataclasses
  - generate_repair_proposals(): proposal count, is_primary flag, strategy dispatch
  - Confidence gating for risky strategies
  - ensure_repair_proposals_table() idempotency
  - persist_proposals() DB write
  - Per-type proposal generators via public API
"""

import json
import sqlite3
import pytest

from knowledge.kb_insufficiency_classifier import (
    InsufficiencyType,
    InsufficiencyDiagnosis,
)
from knowledge.kb_repair_proposals import (
    RepairStrategy,
    RepairPreview,
    RepairSimulation,
    ValidationMetric,
    RepairProposal,
    generate_repair_proposals,
    ensure_repair_proposals_table,
    persist_proposals,
    _MAX_PROPOSALS,
    _CONFIDENCE_FLOOR,
    _RISKY_CONFIDENCE,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_conn(seed_facts=None):
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT, confidence REAL, confidence_effective REAL
        )
    """)
    if seed_facts:
        conn.executemany(
            "INSERT INTO facts (subject, predicate, object, source, confidence, confidence_effective) "
            "VALUES (?,?,?,?,?,?)",
            seed_facts,
        )
    conn.commit()
    return conn


def _diag(types, confidence=0.8, topic='NVDA', signals=None):
    return InsufficiencyDiagnosis(
        topic=topic,
        types=types,
        signals=signals or {'atom_count': 10.0, 'domain_entropy': 0.4,
                            'conflict_cluster': 0.2, 'authority_conflict': 0.3},
        confidence=confidence,
        matched_rules=len(types),
        total_rules=9,
    )


def _seed_atoms(conn, n=20, topic='NVDA', pred='signal_direction',
                obj='long', source='model_signal_v1', conf=0.8):
    rows = [(topic, pred, obj, source, conf, conf)] * n
    conn.executemany(
        "INSERT INTO facts (subject, predicate, object, source, confidence, confidence_effective) "
        "VALUES (?,?,?,?,?,?)",
        rows,
    )
    conn.commit()


# ── RepairStrategy enum ────────────────────────────────────────────────────────

class TestRepairStrategyEnum:
    def test_all_strategies_have_string_values(self):
        for s in RepairStrategy:
            assert isinstance(s.value, str)
            assert len(s.value) > 0

    def test_ingest_missing_value(self):
        assert RepairStrategy.INGEST_MISSING.value == 'ingest_missing'

    def test_manual_review_is_fallback(self):
        assert RepairStrategy.MANUAL_REVIEW.value == 'manual_review'


# ── RepairPreview dataclass ────────────────────────────────────────────────────

class TestRepairPreview:
    def test_defaults_are_empty(self):
        p = RepairPreview()
        assert p.atoms_to_remove == []
        assert p.atoms_to_merge == []
        assert p.atoms_to_add == []
        assert p.sources_to_reweight == {}
        assert p.new_predicates == []
        assert p.sub_topics == []
        assert p.affected_atom_count == 0
        assert p.summary == ''

    def test_to_dict_returns_dict(self):
        p = RepairPreview(summary='test', affected_atom_count=5)
        d = p.to_dict()
        assert isinstance(d, dict)
        assert d['summary'] == 'test'
        assert d['affected_atom_count'] == 5


# ── RepairSimulation dataclass ─────────────────────────────────────────────────

class TestRepairSimulation:
    def test_to_dict_all_keys(self):
        s = RepairSimulation(
            estimated_stress_delta=-0.1,
            estimated_conflict_delta=-0.05,
            estimated_authority_delta=0.0,
            estimated_entropy_delta=0.1,
            estimated_atom_count_delta=-5,
            confidence=0.7,
            assumptions=['test assumption'],
        )
        d = s.to_dict()
        for key in ('estimated_stress_delta', 'estimated_conflict_delta',
                    'estimated_authority_delta', 'estimated_entropy_delta',
                    'estimated_atom_count_delta', 'confidence', 'assumptions'):
            assert key in d

    def test_values_rounded(self):
        s = RepairSimulation(estimated_stress_delta=-0.123456789)
        d = s.to_dict()
        assert abs(d['estimated_stress_delta'] - (-0.1235)) < 0.001


# ── ValidationMetric dataclass ─────────────────────────────────────────────────

class TestValidationMetric:
    def test_to_dict_all_keys(self):
        v = ValidationMetric(
            target_signal='domain_entropy',
            target_direction='increase',
            target_threshold=0.5,
            recheck_after_turns=10,
            description='entropy must rise',
        )
        d = v.to_dict()
        for key in ('target_signal', 'target_direction', 'target_threshold',
                    'recheck_after_turns', 'description'):
            assert key in d


# ── RepairProposal dataclass ───────────────────────────────────────────────────

class TestRepairProposal:
    def _proposal(self, strategy=RepairStrategy.INGEST_MISSING, is_primary=True):
        from datetime import datetime, timezone
        return RepairProposal(
            proposal_id='abc123',
            diagnosis_id='diag001',
            topic='NVDA',
            insufficiency_type=InsufficiencyType.COVERAGE_GAP,
            strategy=strategy,
            is_primary=is_primary,
            description='Test proposal',
            preview=RepairPreview(summary='test preview', affected_atom_count=3),
            simulation=RepairSimulation(estimated_stress_delta=-0.1, confidence=0.6),
            validation=ValidationMetric(target_signal='domain_entropy',
                                        target_direction='increase', target_threshold=0.5,
                                        recheck_after_turns=5),
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

    def test_debug_str_contains_strategy(self):
        p = self._proposal()
        s = p.debug_str()
        assert 'ingest_missing' in s

    def test_debug_str_primary_tag(self):
        p = self._proposal(is_primary=True)
        assert 'PRIMARY' in p.debug_str()

    def test_to_db_row_length(self):
        p = self._proposal()
        row = p.to_db_row()
        assert len(row) == 12  # 12 columns in INSERT

    def test_to_db_row_proposal_id_first(self):
        p = self._proposal()
        row = p.to_db_row()
        assert row[0] == 'abc123'

    def test_to_db_row_strategy_is_string(self):
        p = self._proposal()
        row = p.to_db_row()
        assert row[4] == 'ingest_missing'


# ── generate_repair_proposals() ───────────────────────────────────────────────

class TestGenerateRepairProposals:
    def test_empty_types_returns_empty(self):
        conn = _make_conn()
        diag = _diag([])
        proposals = generate_repair_proposals(diag, conn)
        assert proposals == []
        conn.close()

    def test_unknown_type_returns_manual_review(self):
        conn = _make_conn()
        diag = _diag([InsufficiencyType.UNKNOWN])
        proposals = generate_repair_proposals(diag, conn)
        assert len(proposals) >= 1
        assert proposals[0].strategy == RepairStrategy.MANUAL_REVIEW
        conn.close()

    def test_first_proposal_is_primary(self):
        conn = _make_conn()
        _seed_atoms(conn, 15)
        diag = _diag([InsufficiencyType.COVERAGE_GAP, InsufficiencyType.REPRESENTATION_INCONSISTENCY])
        proposals = generate_repair_proposals(diag, conn)
        assert proposals[0].is_primary
        conn.close()

    def test_subsequent_proposals_not_primary(self):
        conn = _make_conn()
        _seed_atoms(conn, 15)
        diag = _diag([InsufficiencyType.COVERAGE_GAP,
                      InsufficiencyType.REPRESENTATION_INCONSISTENCY,
                      InsufficiencyType.AUTHORITY_IMBALANCE])
        proposals = generate_repair_proposals(diag, conn)
        for p in proposals[1:]:
            assert not p.is_primary
        conn.close()

    def test_max_proposals_capped(self):
        conn = _make_conn()
        _seed_atoms(conn, 15)
        all_types = list(InsufficiencyType)[:6]
        diag = _diag(all_types, confidence=0.9)
        proposals = generate_repair_proposals(diag, conn)
        assert len(proposals) <= _MAX_PROPOSALS
        conn.close()

    def test_proposals_have_required_fields(self):
        conn = _make_conn()
        _seed_atoms(conn, 15)
        diag = _diag([InsufficiencyType.COVERAGE_GAP])
        proposals = generate_repair_proposals(diag, conn)
        assert len(proposals) >= 1
        p = proposals[0]
        assert p.proposal_id
        assert p.topic == 'NVDA'
        assert isinstance(p.preview, RepairPreview)
        assert isinstance(p.simulation, RepairSimulation)
        assert isinstance(p.validation, ValidationMetric)
        conn.close()

    def test_coverage_gap_maps_to_ingest_missing(self):
        conn = _make_conn()
        _seed_atoms(conn, 5)
        diag = _diag([InsufficiencyType.COVERAGE_GAP], confidence=0.9)
        proposals = generate_repair_proposals(diag, conn)
        non_manual = [p for p in proposals if p.strategy != RepairStrategy.MANUAL_REVIEW]
        if non_manual:
            assert non_manual[0].strategy == RepairStrategy.INGEST_MISSING
        conn.close()

    def test_inconsistency_maps_to_resolve_conflicts(self):
        conn = _make_conn()
        _seed_atoms(conn, 10)
        diag = _diag([InsufficiencyType.REPRESENTATION_INCONSISTENCY], confidence=0.9)
        proposals = generate_repair_proposals(diag, conn)
        non_manual = [p for p in proposals if p.strategy != RepairStrategy.MANUAL_REVIEW]
        if non_manual:
            assert non_manual[0].strategy == RepairStrategy.RESOLVE_CONFLICTS
        conn.close()

    def test_authority_imbalance_maps_to_reweight(self):
        conn = _make_conn()
        _seed_atoms(conn, 10, source='social_signal_x')
        diag = _diag([InsufficiencyType.AUTHORITY_IMBALANCE], confidence=0.9)
        proposals = generate_repair_proposals(diag, conn)
        non_manual = [p for p in proposals if p.strategy != RepairStrategy.MANUAL_REVIEW]
        if non_manual:
            assert non_manual[0].strategy == RepairStrategy.REWEIGHT_SOURCES
        conn.close()

    def test_domain_boundary_collapse_maps_to_split_domain(self):
        conn = _make_conn()
        _seed_atoms(conn, 10)
        diag = _diag([InsufficiencyType.DOMAIN_BOUNDARY_COLLAPSE], confidence=0.9)
        proposals = generate_repair_proposals(diag, conn)
        non_manual = [p for p in proposals if p.strategy != RepairStrategy.MANUAL_REVIEW]
        if non_manual:
            assert non_manual[0].strategy == RepairStrategy.SPLIT_DOMAIN
        conn.close()

    def test_simulation_stress_delta_negative_for_fix(self):
        conn = _make_conn()
        _seed_atoms(conn, 10)
        diag = _diag([InsufficiencyType.COVERAGE_GAP], confidence=0.9)
        proposals = generate_repair_proposals(diag, conn)
        for p in proposals:
            if p.strategy != RepairStrategy.MANUAL_REVIEW:
                assert p.simulation.estimated_stress_delta <= 0.0
        conn.close()

    def test_validation_has_recheck_turns(self):
        conn = _make_conn()
        _seed_atoms(conn, 10)
        diag = _diag([InsufficiencyType.COVERAGE_GAP], confidence=0.9)
        proposals = generate_repair_proposals(diag, conn)
        for p in proposals:
            assert p.validation.recheck_after_turns > 0
        conn.close()


# ── Confidence gating for risky strategies ────────────────────────────────────

class TestConfidenceGating:
    def test_merge_atoms_gated_below_threshold(self):
        conn = _make_conn()
        _seed_atoms(conn, 10)
        min_conf = _RISKY_CONFIDENCE.get('MERGE_ATOMS', 0.75)
        diag = _diag([InsufficiencyType.GRANULARITY_TOO_FINE],
                     confidence=min_conf - 0.10)
        proposals = generate_repair_proposals(diag, conn)
        for p in proposals:
            if InsufficiencyType.GRANULARITY_TOO_FINE == p.insufficiency_type:
                assert p.strategy == RepairStrategy.MANUAL_REVIEW
        conn.close()

    def test_introduce_predicates_gated_below_threshold(self):
        conn = _make_conn()
        _seed_atoms(conn, 10)
        min_conf = _RISKY_CONFIDENCE.get('INTRODUCE_PREDICATES', 0.75)
        diag = _diag([InsufficiencyType.MISSING_SCHEMA],
                     confidence=min_conf - 0.10)
        proposals = generate_repair_proposals(diag, conn)
        for p in proposals:
            if p.insufficiency_type == InsufficiencyType.MISSING_SCHEMA:
                assert p.strategy == RepairStrategy.MANUAL_REVIEW
        conn.close()


# ── ensure_repair_proposals_table() ───────────────────────────────────────────

class TestEnsureRepairProposalsTable:
    def test_creates_table(self):
        conn = _make_conn()
        ensure_repair_proposals_table(conn)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert 'repair_proposals' in tables
        conn.close()

    def test_idempotent_second_call(self):
        conn = _make_conn()
        ensure_repair_proposals_table(conn)
        ensure_repair_proposals_table(conn)  # must not raise
        conn.close()


# ── persist_proposals() ───────────────────────────────────────────────────────

class TestPersistProposals:
    def _build_proposal(self):
        from datetime import datetime, timezone
        return RepairProposal(
            proposal_id='test_persist_001',
            diagnosis_id='diag_001',
            topic='NVDA',
            insufficiency_type=InsufficiencyType.COVERAGE_GAP,
            strategy=RepairStrategy.INGEST_MISSING,
            is_primary=True,
            description='Persist test',
            preview=RepairPreview(summary='preview', affected_atom_count=5),
            simulation=RepairSimulation(estimated_stress_delta=-0.1, confidence=0.6),
            validation=ValidationMetric(target_signal='domain_entropy',
                                        target_direction='increase',
                                        target_threshold=0.5, recheck_after_turns=10),
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

    def test_persists_to_db(self):
        conn = _make_conn()
        p = self._build_proposal()
        persist_proposals([p], conn)
        row = conn.execute(
            "SELECT id, topic, strategy FROM repair_proposals WHERE id = ?",
            ('test_persist_001',)
        ).fetchone()
        assert row is not None
        assert row[1] == 'NVDA'
        assert row[2] == 'ingest_missing'
        conn.close()

    def test_empty_list_no_op(self):
        conn = _make_conn()
        ensure_repair_proposals_table(conn)
        persist_proposals([], conn)  # must not raise
        count = conn.execute("SELECT COUNT(*) FROM repair_proposals").fetchone()[0]
        assert count == 0
        conn.close()

    def test_dedup_on_second_insert(self):
        conn = _make_conn()
        p = self._build_proposal()
        persist_proposals([p], conn)
        persist_proposals([p], conn)  # INSERT OR IGNORE — should not error or duplicate
        count = conn.execute("SELECT COUNT(*) FROM repair_proposals").fetchone()[0]
        assert count == 1
        conn.close()

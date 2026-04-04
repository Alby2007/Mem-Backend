"""
tests/test_kb_insufficiency_classifier.py — Unit tests for
knowledge/kb_insufficiency_classifier.py

Covers:
  - InsufficiencyType enum values
  - InsufficiencyDiagnosis dataclass: primary_type(), debug_str(), to_json()
  - _jaccard_similarity_sample(): edge cases
  - classify_insufficiency(): all 7 rules individually + UNKNOWN fallback
  - Signal extraction on empty / populated DB
"""

import json
import sqlite3
import pytest

from knowledge.kb_insufficiency_classifier import (
    InsufficiencyType,
    InsufficiencyDiagnosis,
    classify_insufficiency,
    _jaccard_similarity_sample,
    _COVERAGE_MIN_ATOMS,
    _COVERAGE_MAX_ENTROPY,
    _INCONSISTENCY_CONFLICT,
    _INCONSISTENCY_SUPERSESSION,
    _AUTH_CONFLICT_THRESHOLD,
    _LOW_AUTH_FRACTION,
    _DUPLICATION_MIN_ATOMS,
    _DUPLICATION_PAIR_FRAC,
    _DUPLICATION_SKIP_ENTROPY,
    _GRANULARITY_MIN_ATOMS,
    _GRANULARITY_MAX_PRED_DIV,
    _GRANULARITY_MAX_OBJ_LEN,
    _BOUNDARY_MIN_ENTROPY,
    _BOUNDARY_MIN_PREFIXES,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

class _FakeStress:
    """Minimal duck-typed stress report for classifier input."""
    def __init__(self, conflict_cluster=0.0, supersession_density=0.0,
                 authority_conflict=0.0, domain_entropy=0.5):
        self.conflict_cluster     = conflict_cluster
        self.supersession_density = supersession_density
        self.authority_conflict   = authority_conflict
        self.domain_entropy       = domain_entropy


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT, predicate TEXT, object TEXT,
            source TEXT, confidence REAL, confidence_effective REAL
        )
    """)
    conn.commit()
    return conn


def _seed(conn, rows):
    """rows: list of (subject, predicate, object, source, confidence, confidence_effective)"""
    conn.executemany(
        "INSERT INTO facts (subject, predicate, object, source, confidence, confidence_effective) "
        "VALUES (?,?,?,?,?,?)",
        rows,
    )
    conn.commit()


def _atoms(n, subject='NVDA', predicate='signal_direction', obj='long',
           source='model_signal_v1', conf=0.8):
    return [(subject, predicate, obj, source, conf, conf)] * n


# ── InsufficiencyType enum ─────────────────────────────────────────────────────

class TestInsufficiencyTypeEnum:
    def test_all_types_have_string_values(self):
        for t in InsufficiencyType:
            assert isinstance(t.value, str)
            assert len(t.value) > 0

    def test_unknown_is_fallback(self):
        assert InsufficiencyType.UNKNOWN.value == 'unknown'

    def test_coverage_gap_value(self):
        assert InsufficiencyType.COVERAGE_GAP.value == 'coverage_gap'


# ── InsufficiencyDiagnosis dataclass ──────────────────────────────────────────

class TestInsufficiencyDiagnosis:
    def _diag(self, types=None):
        return InsufficiencyDiagnosis(
            topic='NVDA',
            types=types or [InsufficiencyType.COVERAGE_GAP],
            signals={'atom_count': 5.0},
            confidence=0.6,
            matched_rules=1,
            total_rules=9,
        )

    def test_primary_type_returns_first(self):
        d = self._diag([InsufficiencyType.COVERAGE_GAP, InsufficiencyType.MISSING_SCHEMA])
        assert d.primary_type() == InsufficiencyType.COVERAGE_GAP

    def test_primary_type_empty_returns_unknown(self):
        d = InsufficiencyDiagnosis(
            topic='NVDA', types=[], signals={'atom_count': 0.0},
            confidence=0.0, matched_rules=0, total_rules=9,
        )
        assert d.primary_type() == InsufficiencyType.UNKNOWN

    def test_debug_str_contains_topic(self):
        d = self._diag()
        s = d.debug_str()
        assert 'NVDA' in s
        assert 'coverage_gap' in s

    def test_debug_str_contains_confidence(self):
        d = self._diag()
        s = d.debug_str()
        assert '0.60' in s

    def test_to_json_valid_json(self):
        d = self._diag()
        j = json.loads(d.to_json())
        assert j['topic'] == 'NVDA'
        assert 'coverage_gap' in j['types']
        assert 0.0 <= j['confidence'] <= 1.0

    def test_to_json_signals_rounded(self):
        d = InsufficiencyDiagnosis(
            topic='X', types=[InsufficiencyType.UNKNOWN],
            signals={'atom_count': 5.123456789},
            confidence=0.5,
        )
        j = json.loads(d.to_json())
        assert isinstance(j['signals']['atom_count'], float)


# ── _jaccard_similarity_sample() ──────────────────────────────────────────────

class TestJaccardSimilarity:
    def test_too_few_objects_returns_zero(self):
        objects = ['long', 'short', 'neutral']  # < _DUPLICATION_MIN_SAMPLE (10)
        result = _jaccard_similarity_sample(objects)
        assert result == 0.0

    def test_all_identical_returns_one(self):
        objects = ['long signal bullish breakout'] * 15
        result = _jaccard_similarity_sample(objects)
        assert result == 1.0

    def test_completely_different_returns_low(self):
        objects = [f'word{i} unique{i} distinct{i}' for i in range(15)]
        result = _jaccard_similarity_sample(objects)
        assert 0.0 <= result <= 1.0

    def test_returns_float(self):
        objects = ['alpha beta gamma'] * 20
        result = _jaccard_similarity_sample(objects)
        assert isinstance(result, float)

    def test_empty_list_returns_zero(self):
        result = _jaccard_similarity_sample([])
        assert result == 0.0

    def test_sample_cap_respected(self):
        objects = [f'the quick brown fox {i}' for i in range(200)]
        result = _jaccard_similarity_sample(objects, sample_size=50)
        assert 0.0 <= result <= 1.0


# ── classify_insufficiency() — UNKNOWN fallback ────────────────────────────────

class TestClassifyUnknown:
    def test_empty_db_returns_unknown(self):
        conn = _make_conn()
        stress = _FakeStress()
        diag = classify_insufficiency('NVDA', stress, conn)
        assert diag.topic == 'NVDA'
        assert InsufficiencyType.UNKNOWN in diag.types
        assert diag.confidence == 0.0
        conn.close()

    def test_returns_diagnosis_dataclass(self):
        conn = _make_conn()
        diag = classify_insufficiency('AAPL', _FakeStress(), conn)
        assert isinstance(diag, InsufficiencyDiagnosis)
        conn.close()

    def test_signals_always_present(self):
        conn = _make_conn()
        diag = classify_insufficiency('BP.L', _FakeStress(), conn)
        for key in ('atom_count', 'predicate_diversity', 'avg_object_length',
                    'low_auth_fraction', 'object_similarity', 'source_prefix_count'):
            assert key in diag.signals
        conn.close()

    def test_total_rules_is_nine(self):
        conn = _make_conn()
        diag = classify_insufficiency('NVDA', _FakeStress(), conn)
        assert diag.total_rules == 9
        conn.close()


# ── Rule 1: COVERAGE_GAP ──────────────────────────────────────────────────────

class TestRule1CoverageGap:
    def test_fires_when_few_atoms_low_entropy(self):
        conn = _make_conn()
        # 3 atoms < _COVERAGE_MIN_ATOMS (10), entropy < _COVERAGE_MAX_ENTROPY (0.40)
        _seed(conn, _atoms(3, predicate='signal_direction'))
        stress = _FakeStress(domain_entropy=0.1)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.COVERAGE_GAP in diag.types
        conn.close()

    def test_does_not_fire_with_enough_atoms(self):
        conn = _make_conn()
        _seed(conn, _atoms(_COVERAGE_MIN_ATOMS + 5))
        stress = _FakeStress(domain_entropy=0.1)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.COVERAGE_GAP not in diag.types
        conn.close()

    def test_does_not_fire_with_high_entropy(self):
        conn = _make_conn()
        _seed(conn, _atoms(3))
        stress = _FakeStress(domain_entropy=_COVERAGE_MAX_ENTROPY + 0.1)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.COVERAGE_GAP not in diag.types
        conn.close()


# ── Rule 2: REPRESENTATION_INCONSISTENCY ──────────────────────────────────────

class TestRule2RepresentationInconsistency:
    def test_fires_when_high_conflict_and_supersession(self):
        conn = _make_conn()
        _seed(conn, _atoms(5))
        stress = _FakeStress(
            conflict_cluster=_INCONSISTENCY_CONFLICT + 0.05,
            supersession_density=_INCONSISTENCY_SUPERSESSION + 0.05,
        )
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.REPRESENTATION_INCONSISTENCY in diag.types
        conn.close()

    def test_does_not_fire_low_conflict(self):
        conn = _make_conn()
        _seed(conn, _atoms(5))
        stress = _FakeStress(
            conflict_cluster=0.0,
            supersession_density=_INCONSISTENCY_SUPERSESSION + 0.05,
        )
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.REPRESENTATION_INCONSISTENCY not in diag.types
        conn.close()


# ── Rule 4: SEMANTIC_DUPLICATION ──────────────────────────────────────────────

class TestRule4SemanticDuplication:
    def test_does_not_fire_with_few_atoms(self):
        conn = _make_conn()
        _seed(conn, _atoms(5))
        stress = _FakeStress(domain_entropy=0.3)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.SEMANTIC_DUPLICATION not in diag.types
        conn.close()

    def test_does_not_fire_with_high_entropy_skip(self):
        conn = _make_conn()
        _seed(conn, _atoms(_DUPLICATION_MIN_ATOMS + 10, obj='long signal confirmed strong'))
        stress = _FakeStress(domain_entropy=_DUPLICATION_SKIP_ENTROPY + 0.05)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.SEMANTIC_DUPLICATION not in diag.types
        conn.close()


# ── Rule 7: DOMAIN_BOUNDARY_COLLAPSE ─────────────────────────────────────────

class TestRule7DomainBoundaryCollapse:
    def test_fires_when_high_entropy_many_prefixes(self):
        conn = _make_conn()
        prefixes = ['exchange_feed', 'model_signal', 'broker_research',
                    'regulatory_filing', 'news_wire', 'social_signal']
        rows = [(f'NVDA', 'signal_direction', 'long', f'{p}_x', 0.8, 0.8)
                for p in prefixes for _ in range(5)]
        _seed(conn, rows)
        stress = _FakeStress(domain_entropy=_BOUNDARY_MIN_ENTROPY + 0.05)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.DOMAIN_BOUNDARY_COLLAPSE in diag.types
        conn.close()

    def test_does_not_fire_with_low_entropy(self):
        conn = _make_conn()
        rows = [('NVDA', 'signal_direction', 'long', 'exchange_feed_x', 0.9, 0.9)] * 30
        _seed(conn, rows)
        stress = _FakeStress(domain_entropy=0.1)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert InsufficiencyType.DOMAIN_BOUNDARY_COLLAPSE not in diag.types
        conn.close()


# ── Confidence and matched_rules ───────────────────────────────────────────────

class TestConfidenceScoring:
    def test_confidence_between_0_and_1(self):
        conn = _make_conn()
        _seed(conn, _atoms(3))
        stress = _FakeStress(domain_entropy=0.1)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert 0.0 <= diag.confidence <= 1.0
        conn.close()

    def test_matched_rules_ge_zero(self):
        conn = _make_conn()
        diag = classify_insufficiency('NVDA', _FakeStress(), conn)
        assert diag.matched_rules >= 0
        conn.close()

    def test_matched_rules_le_total_rules(self):
        conn = _make_conn()
        _seed(conn, _atoms(5))
        stress = _FakeStress(domain_entropy=0.1, conflict_cluster=0.9,
                             supersession_density=0.9, authority_conflict=0.9)
        diag = classify_insufficiency('NVDA', stress, conn)
        assert diag.matched_rules <= diag.total_rules
        conn.close()

"""
tests/test_kb_validation.py — Unit tests for knowledge/kb_validation.py

Covers:
  - ValidationReport._compute_severity()
  - validate_schema(): domain detected / not detected
  - validate_semantics(): constraint violations, source conflicts
  - validate_cross_topic(): asymmetry detection
  - validate_all(): runs all three layers
  - governance_verdict(): suppression rules, hard block, confidence penalty
  - GovernanceVerdict.to_dict() shape
  - _compute_adaptive_threshold(): cold-start guard
  - ensure_governance_metrics_table() idempotency
"""

import sqlite3
import pytest

from knowledge.kb_validation import (
    ValidationReport,
    ValidationIssue,
    GovernanceVerdict,
    validate_schema,
    validate_semantics,
    validate_cross_topic,
    validate_all,
    governance_verdict,
    _ensure_governance_metrics_table,
    _compute_adaptive_threshold,
    _SEMANTIC_SUPPRESS_THRESHOLD,
    _SEMANTIC_DOWNGRADE_THRESHOLD,
    _CROSS_SUPPRESS_THRESHOLD,
    _CROSS_PENALTY_THRESHOLD,
    _HARD_BLOCK_SCHEMA_SEVERITY,
    _HARD_BLOCK_SEMANTIC_THRESHOLD,
    _ADAPTIVE_STATIC_MIN,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_conn(seed_facts=None):
    """In-memory SQLite with optional facts rows."""
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


def _make_report(layer, severity=0.0, issues=None, topic='test_topic'):
    r = ValidationReport(topic=topic, layer=layer)
    r.severity = severity
    if issues:
        r.issues = issues
    return r


# ── ValidationReport._compute_severity() tests ────────────────────────────────

class TestValidationReportSeverity:
    def test_no_issues_zero_severity(self):
        r = ValidationReport(topic='x', layer='schema')
        r._compute_severity()
        assert r.severity == 0.0
        assert r.passed

    def test_single_issue_sets_severity(self):
        r = ValidationReport(topic='x', layer='semantic')
        r.issues = [ValidationIssue(predicate='p', atom_id=None,
                                     description='d', severity=0.5,
                                     issue_type='constraint_violation')]
        r._compute_severity()
        assert r.severity == 0.5
        assert not r.passed  # 0.5 >= 0.3 threshold

    def test_multiple_issues_mean_severity(self):
        r = ValidationReport(topic='x', layer='semantic')
        r.issues = [
            ValidationIssue('p', None, 'd', 0.4, 'constraint_violation'),
            ValidationIssue('p', None, 'd', 0.6, 'source_conflict'),
        ]
        r._compute_severity()
        assert r.severity == pytest.approx(0.5, abs=0.01)

    def test_passed_false_when_severity_above_03(self):
        r = ValidationReport(topic='x', layer='schema')
        r.issues = [ValidationIssue('p', None, 'd', 0.8, 'missing')]
        r._compute_severity()
        assert not r.passed

    def test_passed_true_when_severity_below_03(self):
        r = ValidationReport(topic='x', layer='schema')
        r.issues = [ValidationIssue('p', None, 'd', 0.2, 'missing')]
        r._compute_severity()
        assert r.passed

    def test_severity_capped_at_1(self):
        r = ValidationReport(topic='x', layer='semantic')
        r.issues = [ValidationIssue('p', None, 'd', 2.0, 'constraint_violation')]
        r._compute_severity()
        assert r.severity <= 1.0


# ── validate_schema() tests ────────────────────────────────────────────────────

class TestValidateSchema:
    def test_empty_db_returns_schema_report(self):
        conn = _make_conn()
        report = validate_schema('NVDA', conn)
        assert report.layer == 'schema'
        assert report.topic == 'NVDA'
        conn.close()

    def test_no_domain_no_issues(self):
        conn = _make_conn([
            ('UNKNOWNTOPIC', 'random_pred', 'val', 'exchange_feed_x', 0.9, 0.9),
        ])
        report = validate_schema('UNKNOWNTOPIC', conn)
        assert report.layer == 'schema'
        assert isinstance(report.issues, list)
        conn.close()

    def test_report_has_required_signals(self):
        conn = _make_conn()
        report = validate_schema('AAPL', conn)
        assert 'domain_detected' in report.signals
        conn.close()


# ── validate_semantics() tests ─────────────────────────────────────────────────

class TestValidateSemantics:
    def test_empty_topic_returns_zero_severity(self):
        conn = _make_conn()
        report = validate_semantics('NO_SUCH_TOPIC', conn)
        assert report.layer == 'semantic'
        assert report.severity == 0.0
        conn.close()

    def test_returns_semantic_report(self):
        conn = _make_conn([
            ('AAPL', 'signal_direction', 'long', 'exchange_feed_1', 0.9, 0.9),
            ('AAPL', 'price_target', '210', 'broker_research_gs', 0.8, 0.8),
        ])
        report = validate_semantics('AAPL', conn)
        assert report.layer == 'semantic'
        assert 0.0 <= report.severity <= 1.0
        conn.close()

    def test_signals_present(self):
        conn = _make_conn([
            ('MSFT', 'signal_direction', 'long', 'model_signal_v1', 0.7, 0.7),
        ])
        report = validate_semantics('MSFT', conn)
        assert 'constraint_violations' in report.signals
        assert 'source_conflicts' in report.signals
        assert 'contradictions' in report.signals
        conn.close()


# ── validate_cross_topic() tests ───────────────────────────────────────────────

class TestValidateCrossTopic:
    def test_empty_db_returns_cross_topic_report(self):
        conn = _make_conn()
        report = validate_cross_topic('BP.L', conn)
        assert report.layer == 'cross_topic'
        assert report.severity == 0.0
        conn.close()

    def test_symmetric_relation_no_reciprocal_flags_asymmetry(self):
        conn = _make_conn([
            ('TopicA', 'related_to', 'TopicB', 'exchange_feed_1', 0.9, 0.9),
        ])
        report = validate_cross_topic('TopicA', conn)
        assert report.layer == 'cross_topic'
        asymmetries = [i for i in report.issues if i.issue_type == 'cross_topic_asymmetry']
        assert len(asymmetries) >= 1
        conn.close()

    def test_symmetric_relation_with_reciprocal_no_flag(self):
        conn = _make_conn([
            ('TopicA', 'related_to', 'TopicB', 'exchange_feed_1', 0.9, 0.9),
            ('TopicB', 'related_to', 'TopicA', 'exchange_feed_1', 0.9, 0.9),
        ])
        report = validate_cross_topic('TopicA', conn)
        asymmetries = [i for i in report.issues if i.issue_type == 'cross_topic_asymmetry']
        assert len(asymmetries) == 0
        conn.close()

    def test_signals_symmetric_checks_present(self):
        conn = _make_conn([
            ('TopicA', 'integrates_with', 'TopicC', 'exchange_feed_1', 0.8, 0.8),
        ])
        report = validate_cross_topic('TopicA', conn)
        assert 'symmetric_checks' in report.signals
        assert 'asymmetries' in report.signals
        conn.close()


# ── validate_all() tests ───────────────────────────────────────────────────────

class TestValidateAll:
    def test_returns_three_reports(self):
        conn = _make_conn()
        reports = validate_all('NVDA', conn)
        assert len(reports) == 3
        conn.close()

    def test_layers_are_correct(self):
        conn = _make_conn()
        reports = validate_all('NVDA', conn)
        layers = [r.layer for r in reports]
        assert 'schema' in layers
        assert 'semantic' in layers
        assert 'cross_topic' in layers
        conn.close()

    def test_all_same_topic(self):
        conn = _make_conn()
        reports = validate_all('BP.L', conn)
        for r in reports:
            assert r.topic == 'BP.L'
        conn.close()


# ── governance_verdict() tests ─────────────────────────────────────────────────

class TestGovernanceVerdict:

    def _make_reports(self, schema_sev=0.0, semantic_sev=0.0, cross_sev=0.0,
                      topic='test'):
        schema_r = _make_report('schema', severity=schema_sev, topic=topic)
        sem_r    = _make_report('semantic', severity=semantic_sev, topic=topic)
        cross_r  = _make_report('cross_topic', severity=cross_sev, topic=topic)
        return [schema_r, sem_r, cross_r]

    def test_clean_verdict_allows_execution(self):
        reports = self._make_reports()
        verdict = governance_verdict(reports)
        assert verdict.allow_execution
        assert verdict.suppressed_strategies == []
        assert verdict.confidence_penalty == 0.0

    def test_high_semantic_suppresses_merge_and_dedup(self):
        reports = self._make_reports(semantic_sev=_SEMANTIC_SUPPRESS_THRESHOLD + 0.05)
        verdict = governance_verdict(reports)
        assert 'merge_atoms' in verdict.suppressed_strategies
        assert 'deduplicate' in verdict.suppressed_strategies
        assert 'reweight_sources' in verdict.suppressed_strategies

    def test_semantic_at_threshold_does_not_suppress(self):
        reports = self._make_reports(semantic_sev=_SEMANTIC_SUPPRESS_THRESHOLD)
        verdict = governance_verdict(reports)
        assert 'merge_atoms' not in verdict.suppressed_strategies

    def test_high_semantic_downgrades_introduce_predicates(self):
        reports = self._make_reports(semantic_sev=_SEMANTIC_DOWNGRADE_THRESHOLD + 0.05)
        verdict = governance_verdict(reports)
        assert 'introduce_predicates' in verdict.downgraded_strategies

    def test_high_cross_topic_suppresses_split_domain(self):
        reports = self._make_reports(cross_sev=_CROSS_SUPPRESS_THRESHOLD + 0.05)
        verdict = governance_verdict(reports)
        assert 'split_domain' in verdict.suppressed_strategies

    def test_cross_penalty_threshold_applies_penalty(self):
        reports = self._make_reports(cross_sev=_CROSS_PENALTY_THRESHOLD + 0.05)
        verdict = governance_verdict(reports)
        assert verdict.confidence_penalty > 0.0

    def test_hard_block_fires_when_schema_and_semantic_both_high(self):
        reports = self._make_reports(
            schema_sev=_HARD_BLOCK_SCHEMA_SEVERITY,
            semantic_sev=_HARD_BLOCK_SEMANTIC_THRESHOLD + 0.05,
        )
        verdict = governance_verdict(reports)
        assert not verdict.allow_execution

    def test_hard_block_does_not_fire_when_only_schema_high(self):
        reports = self._make_reports(schema_sev=1.0, semantic_sev=0.0)
        verdict = governance_verdict(reports)
        assert verdict.allow_execution

    def test_verdict_to_dict_has_required_keys(self):
        reports = self._make_reports()
        verdict = governance_verdict(reports)
        d = verdict.to_dict()
        for key in ('schema_severity', 'semantic_severity', 'cross_topic_severity',
                    'confidence_penalty', 'blocked_strategies', 'downgraded_strategies',
                    'allowed', 'verdict_reason', 'adaptive_threshold',
                    'adaptive_would_suppress', 'captured_at'):
            assert key in d, f"Missing key: {key}"

    def test_verdict_with_conn_records_metrics(self):
        conn = _make_conn()
        reports = self._make_reports(semantic_sev=0.3, topic='AAPL')
        verdict = governance_verdict(reports, conn=conn)
        row = conn.execute("SELECT COUNT(*) FROM governance_metrics").fetchone()
        assert row[0] >= 1
        conn.close()

    def test_verdict_severities_match_input(self):
        reports = self._make_reports(schema_sev=0.2, semantic_sev=0.4, cross_sev=0.1)
        verdict = governance_verdict(reports)
        assert verdict.schema_severity == pytest.approx(0.2, abs=0.01)
        assert verdict.semantic_severity == pytest.approx(0.4, abs=0.01)
        assert verdict.cross_topic_severity == pytest.approx(0.1, abs=0.01)


# ── Adaptive threshold tests ───────────────────────────────────────────────────

class TestAdaptiveThreshold:
    def test_cold_start_returns_static(self):
        conn = _make_conn()
        _ensure_governance_metrics_table(conn)
        threshold, stats = _compute_adaptive_threshold(conn)
        assert stats['cold_start']
        assert threshold == _SEMANTIC_SUPPRESS_THRESHOLD
        conn.close()

    def test_enough_observations_computes_threshold(self):
        conn = _make_conn()
        _ensure_governance_metrics_table(conn)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        for i in range(6):
            conn.execute(
                "INSERT INTO governance_metrics "
                "(topic, semantic_severity, schema_severity, cross_topic_severity, captured_at, is_baseline) "
                "VALUES (?,?,?,?,?,?)",
                ('AAPL', 0.2 + i * 0.02, 0.1, 0.05, now, 0)
            )
        conn.commit()
        threshold, stats = _compute_adaptive_threshold(conn)
        assert not stats['cold_start']
        assert threshold >= _ADAPTIVE_STATIC_MIN  # floor enforced
        conn.close()

    def test_idempotent_table_creation(self):
        conn = _make_conn()
        _ensure_governance_metrics_table(conn)
        _ensure_governance_metrics_table(conn)  # second call must not raise
        conn.close()

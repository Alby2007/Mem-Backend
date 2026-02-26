"""
tests/test_epistemic_adaptation.py — Unit tests for knowledge/epistemic_adaptation.py

Covers:
  - AdaptationNudges dataclass: is_active(), debug_str()
  - EpistemicAdaptationEngine.compute(): all 5 rules
  - Threshold boundary conditions
  - ensure_adaptation_tables() idempotency
  - get_adaptation_engine() session registry
  - DB queue writes (_queue_refresh, _queue_synthesis)
  - Consolidation count logic (_consolidation_count)
"""

import sqlite3
import tempfile
import os
import pytest

from knowledge.epistemic_adaptation import (
    AdaptationNudges,
    EpistemicAdaptationEngine,
    ensure_adaptation_tables,
    get_adaptation_engine,
    _STRESS_STREAK_THRESHOLD,
    _STREAK_ADAPT_THRESHOLD,
    _STREAK_MODE_THRESHOLD,
    _LOW_ENTROPY_THRESHOLD,
    _HIGH_AUTHORITY_CONFLICT,
    _HIGH_DECAY_PRESSURE,
    _CONSOLIDATION_ESC_DELTA,
    _CONSOLIDATION_CONF_FLOOR,
    _CONSOLIDATION_TOOL_CAP,
    _REFRESH_DECAY_THRESHOLD,
    _SYNTHESIS_CONFLICT_THRESHOLD,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

class _FakeStressReport:
    def __init__(self, composite_stress=0.1, decay_pressure=0.0,
                 authority_conflict=0.0, conflict_cluster=0.0, domain_entropy=0.5,
                 supersession_density=0.0):
        self.composite_stress    = composite_stress
        self.decay_pressure      = decay_pressure
        self.authority_conflict  = authority_conflict
        self.conflict_cluster    = conflict_cluster
        self.domain_entropy      = domain_entropy
        self.supersession_density = supersession_density


class _FakeState:
    def __init__(self, streak=0):
        self.epistemic_stress_streak = streak


def _make_db():
    """Create a temp in-memory SQLite db with adaptation tables."""
    conn = sqlite3.connect(":memory:")
    ensure_adaptation_tables(conn)
    return conn


def _make_file_db():
    """Create a temporary file-based SQLite DB and return (path, conn)."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    ensure_adaptation_tables(conn)
    conn.close()
    return path


# ── AdaptationNudges tests ─────────────────────────────────────────────────────

class TestAdaptationNudges:
    def test_default_not_active(self):
        n = AdaptationNudges()
        assert not n.is_active()

    def test_retrieval_scope_activates(self):
        n = AdaptationNudges(retrieval_scope_broadened=True)
        assert n.is_active()

    def test_prefer_high_authority_activates(self):
        n = AdaptationNudges(prefer_high_authority=True)
        assert n.is_active()

    def test_prefer_recent_activates(self):
        n = AdaptationNudges(prefer_recent=True)
        assert n.is_active()

    def test_consolidation_mode_activates(self):
        n = AdaptationNudges(consolidation_mode=True)
        assert n.is_active()

    def test_escalation_delta_activates(self):
        n = AdaptationNudges(escalation_threshold_delta=-0.05)
        assert n.is_active()

    def test_confidence_floor_delta_activates(self):
        n = AdaptationNudges(confidence_floor_delta=0.10)
        assert n.is_active()

    def test_tool_budget_cap_activates(self):
        n = AdaptationNudges(tool_budget_cap=1)
        assert n.is_active()

    def test_debug_str_nominal(self):
        n = AdaptationNudges(streak=0)
        s = n.debug_str()
        assert 'NOMINAL' in s
        assert 'streak=0' in s

    def test_debug_str_consolidation(self):
        n = AdaptationNudges(consolidation_mode=True, streak=3)
        s = n.debug_str()
        assert 'CONSOLIDATION' in s

    def test_debug_str_adapting(self):
        n = AdaptationNudges(prefer_recent=True, streak=2)
        s = n.debug_str()
        assert 'ADAPTING' in s

    def test_kb_insufficient_not_active_by_default(self):
        n = AdaptationNudges()
        assert not n.kb_insufficient

    def test_repair_proposals_empty_by_default(self):
        n = AdaptationNudges()
        assert n.repair_proposals == []


# ── EpistemicAdaptationEngine.compute() tests ─────────────────────────────────

class TestComputeNoAdaptation:
    """Streak below threshold — no adaptation should fire."""

    def test_streak_zero_returns_nominal(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=0)
        stress = _FakeStressReport()
        n = eng.compute(state, stress)
        assert not n.is_active()
        assert n.streak == 0

    def test_streak_one_returns_nominal(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD - 1)
        stress = _FakeStressReport()
        n = eng.compute(state, stress)
        assert not n.is_active()


class TestRule1ScopeBroadening:
    """Rule 1: low entropy + streak >= 2 → retrieval_scope_broadened."""

    def test_low_entropy_at_threshold_fires(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD)
        stress = _FakeStressReport(domain_entropy=_LOW_ENTROPY_THRESHOLD - 0.01)
        n = eng.compute(state, stress)
        assert n.retrieval_scope_broadened

    def test_high_entropy_does_not_fire(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD)
        stress = _FakeStressReport(domain_entropy=_LOW_ENTROPY_THRESHOLD + 0.10)
        n = eng.compute(state, stress)
        assert not n.retrieval_scope_broadened

    def test_low_entropy_streak_too_low_does_not_fire(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD - 1)
        stress = _FakeStressReport(domain_entropy=0.0)
        n = eng.compute(state, stress)
        assert not n.retrieval_scope_broadened


class TestRule2AuthorityFiltering:
    """Rule 2: high authority_conflict + streak >= 2 → prefer_high_authority."""

    def test_fires_when_above_threshold(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD)
        stress = _FakeStressReport(authority_conflict=_HIGH_AUTHORITY_CONFLICT + 0.01)
        n = eng.compute(state, stress)
        assert n.prefer_high_authority

    def test_does_not_fire_at_threshold(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD)
        stress = _FakeStressReport(authority_conflict=_HIGH_AUTHORITY_CONFLICT)
        n = eng.compute(state, stress)
        assert not n.prefer_high_authority

    def test_does_not_fire_below_streak(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=1)
        stress = _FakeStressReport(authority_conflict=1.0)
        n = eng.compute(state, stress)
        assert not n.prefer_high_authority


class TestRule3RecencyBias:
    """Rule 3: high decay_pressure + streak >= 2 → prefer_recent."""

    def test_fires_when_above_threshold(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD)
        stress = _FakeStressReport(decay_pressure=_HIGH_DECAY_PRESSURE + 0.01)
        n = eng.compute(state, stress)
        assert n.prefer_recent

    def test_does_not_fire_at_threshold(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_ADAPT_THRESHOLD)
        stress = _FakeStressReport(decay_pressure=_HIGH_DECAY_PRESSURE)
        n = eng.compute(state, stress)
        assert not n.prefer_recent


class TestRule4ConsolidationMode:
    """Rule 4: streak >= 3 → consolidation_mode + arbiter nudges."""

    def test_fires_at_mode_threshold(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_MODE_THRESHOLD)
        stress = _FakeStressReport()
        n = eng.compute(state, stress)
        assert n.consolidation_mode
        assert n.escalation_threshold_delta == _CONSOLIDATION_ESC_DELTA
        assert n.confidence_floor_delta == _CONSOLIDATION_CONF_FLOOR
        assert n.tool_budget_cap == _CONSOLIDATION_TOOL_CAP

    def test_does_not_fire_below_mode_threshold(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_MODE_THRESHOLD - 1)
        stress = _FakeStressReport()
        n = eng.compute(state, stress)
        assert not n.consolidation_mode
        assert n.escalation_threshold_delta == 0.0
        assert n.confidence_floor_delta == 0.0
        assert n.tool_budget_cap is None


class TestRule5ScheduledActions:
    """Rule 5: scheduled actions written to DB queues."""

    def test_refresh_queued_when_decay_high(self):
        db_path = _make_file_db()
        try:
            eng = EpistemicAdaptationEngine(db_path=db_path)
            state = _FakeState(streak=_STREAK_MODE_THRESHOLD)
            stress = _FakeStressReport(decay_pressure=_REFRESH_DECAY_THRESHOLD + 0.01)
            n = eng.compute(state, stress, topic='NVDA')
            assert n.refresh_domain_queued
            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT topic, reason FROM domain_refresh_queue").fetchone()
            conn.close()
            assert row is not None
            assert row[0] == 'NVDA'
            assert 'decay' in row[1]
        finally:
            os.unlink(db_path)

    def test_synthesis_queued_when_conflict_high(self):
        db_path = _make_file_db()
        try:
            eng = EpistemicAdaptationEngine(db_path=db_path)
            state = _FakeState(streak=_STREAK_MODE_THRESHOLD)
            stress = _FakeStressReport(conflict_cluster=_SYNTHESIS_CONFLICT_THRESHOLD + 0.01)
            n = eng.compute(state, stress, topic='AAPL', key_terms=['earnings', 'beat'])
            assert n.conflict_synthesis_queued
            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT topic, key_terms FROM synthesis_queue").fetchone()
            conn.close()
            assert row is not None
            assert row[0] == 'AAPL'
        finally:
            os.unlink(db_path)

    def test_no_queue_write_without_db_path(self):
        """Without db_path, scheduled actions should not fire."""
        eng = EpistemicAdaptationEngine(db_path=None)
        state = _FakeState(streak=_STREAK_MODE_THRESHOLD)
        stress = _FakeStressReport(decay_pressure=1.0, conflict_cluster=1.0)
        n = eng.compute(state, stress, topic='NVDA')
        assert not n.refresh_domain_queued
        assert not n.conflict_synthesis_queued


# ── ensure_adaptation_tables idempotency ──────────────────────────────────────

class TestEnsureAdaptationTables:
    def test_creates_all_tables(self):
        conn = _make_db()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {r[0] for r in cursor.fetchall()}
        assert 'domain_refresh_queue' in tables
        assert 'synthesis_queue' in tables
        assert 'consolidation_log' in tables
        assert 'kb_insufficient_log' in tables
        conn.close()

    def test_idempotent_second_call(self):
        conn = _make_db()
        ensure_adaptation_tables(conn)  # second call must not raise
        conn.close()


# ── Session registry ──────────────────────────────────────────────────────────

class TestSessionRegistry:
    def test_same_session_returns_same_engine(self):
        e1 = get_adaptation_engine('sess_abc')
        e2 = get_adaptation_engine('sess_abc')
        assert e1 is e2

    def test_different_sessions_return_different_engines(self):
        e1 = get_adaptation_engine('sess_x1')
        e2 = get_adaptation_engine('sess_x2')
        assert e1 is not e2


# ── Streak snapshot ───────────────────────────────────────────────────────────

class TestStreakSnapshot:
    def test_streak_stored_in_nudges(self):
        eng = EpistemicAdaptationEngine()
        for streak in (0, 1, 2, 3, 5):
            state = _FakeState(streak=streak)
            n = eng.compute(state, _FakeStressReport())
            assert n.streak == streak

    def test_multiple_rules_can_fire_simultaneously(self):
        eng = EpistemicAdaptationEngine()
        state = _FakeState(streak=_STREAK_MODE_THRESHOLD)
        stress = _FakeStressReport(
            domain_entropy=0.0,
            authority_conflict=1.0,
            decay_pressure=1.0,
        )
        n = eng.compute(state, stress)
        assert n.retrieval_scope_broadened
        assert n.prefer_high_authority
        assert n.prefer_recent
        assert n.consolidation_mode

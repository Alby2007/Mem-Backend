"""
knowledge/epistemic_adaptation.py — Epistemic Adaptation Engine (Phase 3)

When epistemic_stress stays high for N consecutive turns, the system shifts
behavior structurally — not cosmetically. This module computes AdaptationNudges
from SystemState + EpistemicStressReport and feeds them back into the arbiter
and retrieval pipeline as additive modulations.

Design invariants:
  - Never overrides arbiter sovereignty — only provides nudges
  - All nudges are additive to existing arbiter parameters
  - No LLM calls, no hardcoded domain logic
  - Scheduled actions are log-only (append to queue tables, no execution)
  - Zero-LLM, pure Python, <2ms per call

Adaptation rules (evaluated in order, all additive):
  1. Retrieval scope broadening  — low domain_entropy + streak ≥ 2
  2. Authority filtering         — high authority_conflict + streak ≥ 2
  3. Recency bias                — high decay_pressure + streak ≥ 2
  4. Consolidation mode          — streak ≥ 3 (mode switch)
  5. Scheduled actions           — sustained stress + streak ≥ 3 (log-only)
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    from knowledge.kb_insufficiency_classifier import (
        classify_insufficiency,
        InsufficiencyDiagnosis,
    )
    _HAS_CLASSIFIER = True
except ImportError:
    _HAS_CLASSIFIER = False
    InsufficiencyDiagnosis = None

try:
    from knowledge.kb_repair_proposals import (
        generate_repair_proposals,
        persist_proposals,
        ensure_repair_proposals_table,
    )
    _HAS_REPAIR_PROPOSALS = True
except ImportError:
    _HAS_REPAIR_PROPOSALS = False


# ── Constants ──────────────────────────────────────────────────────────────────

_STRESS_STREAK_THRESHOLD = 0.65   # composite_stress above this increments streak
_STREAK_ADAPT_THRESHOLD  = 2      # streak ≥ 2 → retrieval adaptations activate
_STREAK_MODE_THRESHOLD   = 3      # streak ≥ 3 → consolidation mode activates

# Retrieval adaptation thresholds
_LOW_ENTROPY_THRESHOLD       = 0.35   # domain_entropy below this → broaden scope
_HIGH_AUTHORITY_CONFLICT     = 0.55   # authority_conflict above this → prefer high-authority
_HIGH_DECAY_PRESSURE         = 0.50   # decay_pressure above this → prefer recent

# Consolidation mode nudges (additive — never replace arbiter values)
_CONSOLIDATION_ESC_DELTA     = -0.08  # lower escalation threshold (synthesis, not reaction)
_CONSOLIDATION_CONF_FLOOR    = +0.15  # raise confidence floor (more cautious assertion)
_CONSOLIDATION_TOOL_CAP      = 1      # suppress non-essential tools

# Scheduled action thresholds
_REFRESH_DECAY_THRESHOLD     = 0.60   # decay_pressure above this → queue refresh
_SYNTHESIS_CONFLICT_THRESHOLD = 0.50  # conflict_cluster above this → queue synthesis

# Authority filter cutoff (prefer_high_authority mode)
AUTHORITY_FILTER_CUTOFF = 0.60

# KB insufficiency detection
# If consolidation mode fires ≥ this many times for the same topic within the window,
# the problem is structural (KB representation is insufficient), not transient.
_KB_INSUFFICIENT_CONSOLIDATION_COUNT = 5   # fires in N distinct sessions/windows
_KB_INSUFFICIENT_WINDOW_DAYS         = 7   # look-back window


# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_REFRESH_QUEUE = """
CREATE TABLE IF NOT EXISTS domain_refresh_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT,
    reason      TEXT,
    queued_at   TEXT NOT NULL,
    processed   INTEGER DEFAULT 0
)
"""

_CREATE_SYNTHESIS_QUEUE = """
CREATE TABLE IF NOT EXISTS synthesis_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT,
    key_terms   TEXT,   -- JSON array
    reason      TEXT,
    queued_at   TEXT NOT NULL,
    processed   INTEGER DEFAULT 0
)
"""

# KB insufficiency event log.
# Each row = one consolidation_mode activation for a topic.
# When count(topic, window) >= threshold, KB representation is structurally insufficient.
_CREATE_CONSOLIDATION_LOG = """
CREATE TABLE IF NOT EXISTS consolidation_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT,
    session_id  TEXT,
    stress      REAL,
    streak      INTEGER,
    logged_at   TEXT NOT NULL
)
"""

_CREATE_KB_INSUFFICIENT_LOG = """
CREATE TABLE IF NOT EXISTS kb_insufficient_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    topic               TEXT,
    consolidation_count INTEGER,
    window_days         INTEGER,
    insufficiency_types TEXT,   -- JSON array of InsufficiencyType values
    detected_at         TEXT NOT NULL
)
"""

# Migration: add insufficiency_types column to existing tables (idempotent)
_ALTER_KB_INSUFFICIENT_LOG = """
ALTER TABLE kb_insufficient_log ADD COLUMN insufficiency_types TEXT
"""


def ensure_adaptation_tables(conn: sqlite3.Connection) -> None:
    """Idempotent migration. Safe to call on every startup."""
    conn.execute(_CREATE_REFRESH_QUEUE)
    conn.execute(_CREATE_SYNTHESIS_QUEUE)
    conn.execute(_CREATE_CONSOLIDATION_LOG)
    conn.execute(_CREATE_KB_INSUFFICIENT_LOG)
    # Add insufficiency_types column to existing tables (safe no-op if already present)
    try:
        conn.execute(_ALTER_KB_INSUFFICIENT_LOG)
    except Exception:
        pass  # column already exists
    conn.commit()


# ── AdaptationNudges dataclass ─────────────────────────────────────────────────

@dataclass
class AdaptationNudges:
    """
    Structural behavioral shifts produced by EpistemicAdaptationEngine.

    All fields are additive modulations — they never replace arbiter decisions.
    The arbiter applies these as deltas on top of its own computed values.

    Retrieval flags are consumed by _retrieve_knowledge() before the arbiter runs.
    Arbiter nudges are consumed by PolicyArbiter.resolve() as additive deltas.
    """
    # ── Retrieval strategy flags ───────────────────────────────────────────────
    retrieval_scope_broadened: bool = False   # low entropy → include github/ast sources
    prefer_high_authority: bool = False        # authority_conflict → filter to top-authority
    prefer_recent: bool = False                # decay_pressure → sort by timestamp DESC

    # ── Arbiter nudges (additive) ──────────────────────────────────────────────
    escalation_threshold_delta: float = 0.0   # consolidation → lower esc threshold
    confidence_floor_delta: float = 0.0        # sustained stress → raise floor
    tool_budget_cap: Optional[int] = None      # consolidation → suppress non-essential tools

    # ── Scheduled actions (log-only, no immediate execution) ──────────────────
    refresh_domain_queued: bool = False        # decay sustained → queue re-ingest
    conflict_synthesis_queued: bool = False    # conflict sustained → queue synthesis
    kb_insufficient: bool = False              # structural: KB representation is insufficient
    kb_insufficiency_diagnosis: Optional[object] = None  # InsufficiencyDiagnosis | None
    repair_proposals: List[object] = field(default_factory=list)  # List[RepairProposal]

    # ── Mode flag ─────────────────────────────────────────────────────────────
    consolidation_mode: bool = False           # streak ≥ 3

    # ── Streak snapshot (for debug) ───────────────────────────────────────────
    streak: int = 0

    def is_active(self) -> bool:
        """True if any adaptation is in effect."""
        return (
            self.retrieval_scope_broadened
            or self.prefer_high_authority
            or self.prefer_recent
            or self.consolidation_mode
            or self.escalation_threshold_delta != 0.0
            or self.confidence_floor_delta != 0.0
            or self.tool_budget_cap is not None
        )

    def debug_str(self) -> str:
        mode = "CONSOLIDATION" if self.consolidation_mode else (
            "ADAPTING" if self.is_active() else "NOMINAL"
        )
        parts = [f"EpistemicAdaptation[mode={mode} streak={self.streak}"]
        if self.retrieval_scope_broadened:
            parts.append("broaden=True")
        if self.prefer_high_authority:
            parts.append("auth_filter=True")
        if self.prefer_recent:
            parts.append("recency=True")
        if self.escalation_threshold_delta:
            parts.append(f"esc_delta={self.escalation_threshold_delta:+.2f}")
        if self.confidence_floor_delta:
            parts.append(f"conf_floor_delta={self.confidence_floor_delta:+.2f}")
        if self.tool_budget_cap is not None:
            parts.append(f"tool_cap={self.tool_budget_cap}")
        if self.refresh_domain_queued:
            parts.append("refresh_queued=True")
        if self.conflict_synthesis_queued:
            parts.append("synth_queued=True")
        if self.kb_insufficient:
            parts.append("KB_INSUFFICIENT=True")
        if self.kb_insufficiency_diagnosis is not None:
            try:
                primary = self.kb_insufficiency_diagnosis.primary_type().value
                parts.append(f"type={primary}")
            except Exception:
                pass
        if self.repair_proposals:
            try:
                primary_p = next((p for p in self.repair_proposals if p.is_primary), self.repair_proposals[0])
                parts.append(f"strategy={primary_p.strategy.value}")
                parts.append(f"proposal_id={primary_p.proposal_id[:8]}...")
            except Exception:
                pass
        return ' '.join(parts) + ']'


# ── Adaptation Engine ──────────────────────────────────────────────────────────

class EpistemicAdaptationEngine:
    """
    Computes AdaptationNudges from SystemState + EpistemicStressReport.

    Per-session singleton. Reads streak from SystemState (maintained there via EMA).
    Produces nudges that feed into retrieval and arbiter — never decides directly.

    Also tracks per-topic consolidation frequency. When consolidation fires
    ≥ _KB_INSUFFICIENT_CONSOLIDATION_COUNT times for the same topic within
    _KB_INSUFFICIENT_WINDOW_DAYS, it logs a KB_INSUFFICIENT event and sets
    nudges.kb_insufficient = True. This is a structural signal: the KB
    representation itself is insufficient, not just transiently stressed.
    """

    def __init__(self, db_path: Optional[str] = None):
        self._db_path = db_path
        self._session_id: Optional[str] = None

    def compute(
        self,
        state,                    # SystemState — duck-typed to avoid circular import
        stress_report,            # EpistemicStressReport
        topic: Optional[str] = None,
        key_terms: Optional[List[str]] = None,
    ) -> AdaptationNudges:
        """
        Evaluate all 5 adaptation rules and return AdaptationNudges.

        Args:
            state:        SystemState (must have epistemic_stress_streak field)
            stress_report: EpistemicStressReport from compute_stress()
            topic:        current active topic label (for queue logging)
            key_terms:    current message key terms (for synthesis queue)
        """
        streak = getattr(state, 'epistemic_stress_streak', 0)
        nudges = AdaptationNudges(streak=streak)

        if streak < _STREAK_ADAPT_THRESHOLD:
            return nudges  # no adaptation below threshold

        # Bind session_id for consolidation logging (set lazily)
        if self._session_id is None and hasattr(state, '_session_id'):
            self._session_id = state._session_id

        # ── Rule 1: Retrieval scope broadening (low entropy) ──────────────────
        if (stress_report.domain_entropy < _LOW_ENTROPY_THRESHOLD
                and streak >= _STREAK_ADAPT_THRESHOLD):
            nudges.retrieval_scope_broadened = True

        # ── Rule 2: Authority filtering (high authority conflict) ─────────────
        if (stress_report.authority_conflict > _HIGH_AUTHORITY_CONFLICT
                and streak >= _STREAK_ADAPT_THRESHOLD):
            nudges.prefer_high_authority = True

        # ── Rule 3: Recency bias (high decay pressure) ────────────────────────
        if (stress_report.decay_pressure > _HIGH_DECAY_PRESSURE
                and streak >= _STREAK_ADAPT_THRESHOLD):
            nudges.prefer_recent = True

        # ── Rule 4: Consolidation mode (sustained high composite stress) ──────
        if streak >= _STREAK_MODE_THRESHOLD:
            nudges.consolidation_mode = True
            nudges.escalation_threshold_delta = _CONSOLIDATION_ESC_DELTA
            nudges.confidence_floor_delta = _CONSOLIDATION_CONF_FLOOR
            nudges.tool_budget_cap = _CONSOLIDATION_TOOL_CAP

        # ── Rule 5: Scheduled actions (log-only) ──────────────────────────────
        if streak >= _STREAK_MODE_THRESHOLD:
            if (stress_report.decay_pressure > _REFRESH_DECAY_THRESHOLD
                    and self._db_path):
                self._queue_refresh(topic)
                nudges.refresh_domain_queued = True

            if (stress_report.conflict_cluster > _SYNTHESIS_CONFLICT_THRESHOLD
                    and self._db_path):
                self._queue_synthesis(topic, key_terms or [])
                nudges.conflict_synthesis_queued = True

            # ── Rule 6: KB insufficiency detection ────────────────────────────
            # Log this consolidation activation. Then check if the same topic
            # has fired consolidation ≥ N times in the last W days.
            # If so: this is structural, not transient.
            if self._db_path:
                self._log_consolidation(topic, stress_report.composite_stress, streak)
                count = self._consolidation_count(topic)
                if count >= _KB_INSUFFICIENT_CONSOLIDATION_COUNT:
                    diagnosis, diagnosis_db_id = self._classify_and_log(topic, stress_report, count)
                    nudges.kb_insufficient = True
                    nudges.kb_insufficiency_diagnosis = diagnosis
                    print(
                        f"[EpistemicAdaptation] KB_INSUFFICIENT: topic='{topic}' "
                        f"consolidation_count={count} in {_KB_INSUFFICIENT_WINDOW_DAYS}d "
                        f"— KB representation is structurally insufficient"
                    )
                    # Generate repair proposals for all matched types (capped at 3)
                    if _HAS_REPAIR_PROPOSALS and diagnosis is not None:
                        try:
                            conn = sqlite3.connect(self._db_path, check_same_thread=False)
                            proposals = generate_repair_proposals(
                                diagnosis, conn, diagnosis_id=str(diagnosis_db_id)
                            )
                            persist_proposals(proposals, conn)
                            conn.close()
                            nudges.repair_proposals = proposals
                        except Exception as e:
                            print(f"[EpistemicAdaptation] repair proposals error: {e}")

        return nudges

    def _queue_refresh(self, topic: Optional[str]) -> None:
        """Append a domain refresh request to domain_refresh_queue. Log-only."""
        try:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            ensure_adaptation_tables(conn)
            conn.execute(
                "INSERT INTO domain_refresh_queue (topic, reason, queued_at) VALUES (?, ?, ?)",
                (
                    topic or 'unknown',
                    'decay_pressure_sustained',
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[EpistemicAdaptation] refresh queue error: {e}")

    def _queue_synthesis(self, topic: Optional[str], key_terms: List[str]) -> None:
        """Append a conflict synthesis request to synthesis_queue. Log-only."""
        try:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            ensure_adaptation_tables(conn)
            conn.execute(
                "INSERT INTO synthesis_queue (topic, key_terms, reason, queued_at) VALUES (?, ?, ?, ?)",
                (
                    topic or 'unknown',
                    json.dumps(key_terms[:10]),
                    'conflict_cluster_sustained',
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[EpistemicAdaptation] synthesis queue error: {e}")

    def _log_consolidation(self, topic: Optional[str], stress: float, streak: int) -> None:
        """Log one consolidation_mode activation to consolidation_log."""
        try:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            ensure_adaptation_tables(conn)
            conn.execute(
                "INSERT INTO consolidation_log (topic, session_id, stress, streak, logged_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    topic or 'unknown',
                    self._session_id or 'unknown',
                    round(stress, 4),
                    streak,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[EpistemicAdaptation] consolidation log error: {e}")

    def _consolidation_count(self, topic: Optional[str]) -> int:
        """
        Count how many times consolidation has fired for this topic
        within the last _KB_INSUFFICIENT_WINDOW_DAYS days.

        Counts distinct calendar days to avoid inflating from a single
        long session — 20 fires in one day is one bad session, not
        structural insufficiency.
        """
        if not self._db_path or not topic:
            return 0
        try:
            from datetime import timedelta
            cutoff = (
                datetime.now(timezone.utc)
                - timedelta(days=_KB_INSUFFICIENT_WINDOW_DAYS)
            ).isoformat()
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            cursor = conn.cursor()
            # Count distinct days, not raw rows — prevents single-session inflation
            cursor.execute(
                """
                SELECT COUNT(DISTINCT DATE(logged_at))
                FROM consolidation_log
                WHERE topic = ? AND logged_at >= ?
                """,
                (topic, cutoff),
            )
            count = cursor.fetchone()[0] or 0
            conn.close()
            return int(count)
        except Exception:
            return 0

    def _classify_and_log(
        self, topic: Optional[str], stress_report, count: int
    ) -> tuple:  # (InsufficiencyDiagnosis | None, int | None)
        """
        Run the KB insufficiency classifier, log the result, and return
        (diagnosis, diagnosis_db_id) so proposals can be linked by diagnosis_id.
        """
        diagnosis = None
        types_json = '[]'

        if _HAS_CLASSIFIER:
            try:
                conn = sqlite3.connect(self._db_path, check_same_thread=False)
                diagnosis = classify_insufficiency(
                    topic or 'unknown',
                    stress_report,
                    conn,
                )
                conn.close()
                types_json = json.dumps([t.value for t in diagnosis.types])
            except Exception as e:
                print(f"[EpistemicAdaptation] classifier error: {e}")

        diagnosis_db_id = None
        try:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            ensure_adaptation_tables(conn)
            cursor = conn.execute(
                "INSERT INTO kb_insufficient_log "
                "(topic, consolidation_count, window_days, insufficiency_types, detected_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    topic or 'unknown',
                    count,
                    _KB_INSUFFICIENT_WINDOW_DAYS,
                    types_json,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            diagnosis_db_id = cursor.lastrowid
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[EpistemicAdaptation] kb_insufficient log error: {e}")

        return diagnosis, diagnosis_db_id


# ── Session Registry ───────────────────────────────────────────────────────────

_engines: Dict[str, EpistemicAdaptationEngine] = {}


def get_adaptation_engine(session_id: str, db_path: Optional[str] = None) -> EpistemicAdaptationEngine:
    """Return (and lazily create) a per-session EpistemicAdaptationEngine."""
    if session_id not in _engines:
        _engines[session_id] = EpistemicAdaptationEngine(db_path=db_path)
    return _engines[session_id]

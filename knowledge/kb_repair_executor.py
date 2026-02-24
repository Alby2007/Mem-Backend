"""
knowledge/kb_repair_executor.py — KB Repair Executor (Phase 6)

Turns repair proposals into measured, human-gated structural interventions.

Four capabilities:
  1. execute_repair(proposal_id, db_path, dry_run=False) — atomic execution
  2. rollback_repair(proposal_id, db_path) — restore original atom states
  3. _snapshot_signals(topic, db_conn) — pre/post structural signal measurement
  4. repair_impact_score(strategy, db_path) — aggregate calibration metrics

Design invariants:
  - Human-gated: never auto-executes
  - Atomic: all mutations inside BEGIN IMMEDIATE / COMMIT; ROLLBACK on any error
  - Soft-delete only: no DELETE statements; mutations zero confidence_effective
  - Structurally-scoped: snapshots use compute_structural_stress(), not retrieval context
  - atom_count counts active atoms only (confidence_effective > 0)
  - Auto-rollback: fires if repair worsens stress or causes entropy collapse
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    from knowledge.epistemic_stress import compute_structural_stress
    _HAS_STRESS = True
except ImportError:
    _HAS_STRESS = False

try:
    from knowledge.kb_repair_proposals import ensure_repair_proposals_table
    _HAS_PROPOSALS = True
except ImportError:
    _HAS_PROPOSALS = False

try:
    from knowledge.kb_validation import validate_all, governance_verdict
    _HAS_VALIDATION = True
except ImportError:
    _HAS_VALIDATION = False


# ── Constants ──────────────────────────────────────────────────────────────────

_AUTO_ROLLBACK_STRESS_INCREASE  = 0.05
_AUTO_ROLLBACK_ENTROPY_COLLAPSE = 0.20
_REWEIGHT_CLAMP_MIN = 0.01
_REWEIGHT_CLAMP_MAX = 1.00
_STABLE_ROLLBACK_RATE_MAX  = 0.10
_STABLE_VARIANCE_MAX       = 0.02
_CHAOTIC_ROLLBACK_RATE_MIN = 0.30
_CHAOTIC_SIDE_EFFECT_MIN   = 0.20


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class SignalSnapshot:
    composite_stress: float = 0.0
    conflict_cluster: float = 0.0
    authority_conflict: float = 0.0
    domain_entropy: float = 1.0
    predicate_diversity: float = 0.0
    atom_count: int = 0
    captured_at: str = ''

    def to_dict(self) -> dict:
        return {
            'composite_stress': round(self.composite_stress, 4),
            'conflict_cluster': round(self.conflict_cluster, 4),
            'authority_conflict': round(self.authority_conflict, 4),
            'domain_entropy': round(self.domain_entropy, 4),
            'predicate_diversity': round(self.predicate_diversity, 4),
            'atom_count': self.atom_count,
            'captured_at': self.captured_at,
        }


@dataclass
class DivergenceReport:
    stress_divergence: float = 0.0
    conflict_divergence: float = 0.0
    authority_divergence: float = 0.0
    entropy_divergence: float = 0.0
    atom_count_divergence: int = 0
    mean_abs_divergence: float = 0.0
    direction_correct: bool = True

    def to_dict(self) -> dict:
        return {
            'stress_divergence': round(self.stress_divergence, 4),
            'conflict_divergence': round(self.conflict_divergence, 4),
            'authority_divergence': round(self.authority_divergence, 4),
            'entropy_divergence': round(self.entropy_divergence, 4),
            'atom_count_divergence': self.atom_count_divergence,
            'mean_abs_divergence': round(self.mean_abs_divergence, 4),
            'direction_correct': self.direction_correct,
        }


@dataclass
class ExecutionResult:
    proposal_id: str
    strategy: str
    dry_run: bool
    success: bool
    auto_rolled_back: bool = False
    rollback_reason: str = ''
    signals_before: Optional[SignalSnapshot] = None
    signals_after: Optional[SignalSnapshot] = None
    divergence: Optional[DivergenceReport] = None
    mutations_applied: int = 0
    executed_at: str = ''
    error: str = ''

    def debug_str(self) -> str:
        if not self.success:
            return f"[RepairExecutor] FAILED id={self.proposal_id[:8]} error={self.error}"
        tag = 'DRY_RUN ' if self.dry_run else ''
        rb = ' AUTO-ROLLED-BACK' if self.auto_rolled_back else ''
        lines = [
            f"[RepairExecutor] {tag}SUCCESS{rb} id={self.proposal_id[:8]}... "
            f"strategy={self.strategy} mutations={self.mutations_applied}",
        ]
        if self.signals_before and self.signals_after:
            b, a = self.signals_before, self.signals_after
            lines.append(f"  before: stress={b.composite_stress:.2f} conflict={b.conflict_cluster:.2f} "
                         f"entropy={b.domain_entropy:.2f} atoms={b.atom_count}")
            lines.append(f"  after:  stress={a.composite_stress:.2f} conflict={a.conflict_cluster:.2f} "
                         f"entropy={a.domain_entropy:.2f} atoms={a.atom_count}")
        if self.divergence:
            lines.append(f"  divergence: mean_abs={self.divergence.mean_abs_divergence:.3f} "
                         f"direction_correct={self.divergence.direction_correct}")
        if self.auto_rolled_back:
            lines.append(f"  rollback_reason: {self.rollback_reason}")
        return '\n'.join(lines)


@dataclass
class RollbackResult:
    proposal_id: str
    success: bool
    atoms_restored: int = 0
    signals_after: Optional[SignalSnapshot] = None
    error: str = ''


@dataclass
class ImpactScore:
    strategy: str
    n_executions: int = 0
    n_rollbacks: int = 0
    n_auto_rollbacks: int = 0
    mean_stress_delta: float = 0.0
    stress_delta_variance: float = 0.0
    mean_abs_divergence: float = 0.0
    rollback_rate: float = 0.0
    side_effect_rate: float = 0.0
    reliability: str = 'unknown'


# ── Schema ─────────────────────────────────────────────────────────────────────

def ensure_executor_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS repair_execution_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id         TEXT NOT NULL,
            action              TEXT NOT NULL,
            strategy            TEXT,
            topic               TEXT,
            signals_before_json TEXT,
            signals_after_json  TEXT,
            simulation_json     TEXT,
            divergence_json     TEXT,
            mutations_applied   INTEGER DEFAULT 0,
            auto_rolled_back    INTEGER DEFAULT 0,
            rollback_reason     TEXT,
            success             INTEGER DEFAULT 0,
            error               TEXT,
            executed_at         TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS repair_rollback_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id     TEXT NOT NULL UNIQUE,
            snapshot_json   TEXT NOT NULL,
            created_at      TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predicate_vocabulary (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            predicate   TEXT NOT NULL UNIQUE,
            proposed_by TEXT,
            status      TEXT DEFAULT 'proposed',
            proposed_at TEXT NOT NULL
        )
    """)
    # Add governance_json column if not present (idempotent)
    try:
        conn.execute("ALTER TABLE repair_execution_log ADD COLUMN governance_json TEXT")
    except Exception:
        pass
    conn.commit()


# ── Signal snapshot ────────────────────────────────────────────────────────────

def _snapshot_signals(topic: str, conn: sqlite3.Connection) -> SignalSnapshot:
    snap = SignalSnapshot(captured_at=datetime.now(timezone.utc).isoformat())
    if _HAS_STRESS:
        try:
            report = compute_structural_stress(topic, conn)
            snap.composite_stress   = report.composite_stress
            snap.conflict_cluster   = report.conflict_cluster
            snap.authority_conflict = report.authority_conflict
            snap.domain_entropy     = report.domain_entropy
        except Exception:
            pass
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM facts "
            "WHERE (subject LIKE ? OR object LIKE ?) "
            "AND (confidence_effective IS NULL OR confidence_effective > 0)",
            (f'%{topic}%', f'%{topic}%'),
        )
        snap.atom_count = cursor.fetchone()[0] or 0
        cursor.execute(
            "SELECT COUNT(DISTINCT predicate), COUNT(*) FROM facts "
            "WHERE (subject LIKE ? OR object LIKE ?) "
            "AND (confidence_effective IS NULL OR confidence_effective > 0)",
            (f'%{topic}%', f'%{topic}%'),
        )
        row = cursor.fetchone()
        snap.predicate_diversity = round((row[0] or 0) / max(1, row[1] or 1), 4)
    except Exception:
        pass
    return snap


# ── Divergence ────────────────────────────────────────────────────────────────

def _compute_divergence(simulation: dict, before: SignalSnapshot, after: SignalSnapshot) -> DivergenceReport:
    actual_stress   = after.composite_stress   - before.composite_stress
    actual_conflict = after.conflict_cluster   - before.conflict_cluster
    actual_auth     = after.authority_conflict - before.authority_conflict
    actual_entropy  = after.domain_entropy     - before.domain_entropy
    actual_atoms    = after.atom_count         - before.atom_count

    est_stress   = simulation.get('estimated_stress_delta', 0.0)
    est_conflict = simulation.get('estimated_conflict_delta', 0.0)
    est_auth     = simulation.get('estimated_authority_delta', 0.0)
    est_entropy  = simulation.get('estimated_entropy_delta', 0.0)
    est_atoms    = simulation.get('estimated_atom_count_delta', 0)

    div_s = actual_stress   - est_stress
    div_c = actual_conflict - est_conflict
    div_a = actual_auth     - est_auth
    div_e = actual_entropy  - est_entropy

    mean_abs = (abs(div_s) + abs(div_c) + abs(div_a) + abs(div_e)) / 4.0
    direction_correct = not (est_stress < 0 and actual_stress >= 0) and not (est_stress > 0 and actual_stress <= 0)

    return DivergenceReport(
        stress_divergence=round(div_s, 4), conflict_divergence=round(div_c, 4),
        authority_divergence=round(div_a, 4), entropy_divergence=round(div_e, 4),
        atom_count_divergence=actual_atoms - est_atoms,
        mean_abs_divergence=round(mean_abs, 4), direction_correct=direction_correct,
    )


# ── Rollback snapshot ─────────────────────────────────────────────────────────

def _store_rollback_snapshot(proposal_id: str, atom_ids: List[int], conn: sqlite3.Connection) -> None:
    if not atom_ids:
        return
    ph = ','.join('?' * len(atom_ids))
    cursor = conn.cursor()
    cursor.execute(f"SELECT id, confidence_effective FROM facts WHERE id IN ({ph})", atom_ids)
    originals = [{'id': r[0], 'confidence_effective': r[1]} for r in cursor.fetchall()]
    now = datetime.now(timezone.utc).isoformat()
    snap = {'proposal_id': proposal_id, 'atoms_original': originals, 'created_at': now}
    conn.execute(
        "INSERT OR REPLACE INTO repair_rollback_log (proposal_id, snapshot_json, created_at) VALUES (?,?,?)",
        (proposal_id, json.dumps(snap), now),
    )


# ── Per-strategy mutation functions ──────────────────────────────────────────

def _apply_zero_ids(ids: List[int], conn: sqlite3.Connection) -> int:
    if not ids:
        return 0
    ph = ','.join('?' * len(ids))
    conn.execute(f"UPDATE facts SET confidence_effective = 0.0 WHERE id IN ({ph})", ids)
    return len(ids)


def _apply_reweight_sources(preview: dict, topic: str, conn: sqlite3.Connection) -> int:
    reweight = preview.get('sources_to_reweight', {})
    touched = 0
    for source, multiplier in reweight.items():
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, confidence_effective FROM facts "
            "WHERE source = ? AND (subject LIKE ? OR object LIKE ?) "
            "AND (confidence_effective IS NULL OR confidence_effective > 0)",
            (source, f'%{topic}%', f'%{topic}%'),
        )
        rows = cursor.fetchall()
        for atom_id, ce in rows:
            ce = ce if ce is not None else 1.0
            new_ce = max(_REWEIGHT_CLAMP_MIN, min(_REWEIGHT_CLAMP_MAX, ce * multiplier))
            conn.execute("UPDATE facts SET confidence_effective = ? WHERE id = ?", (new_ce, atom_id))
        touched += len(rows)
    return touched


# Entropy floor guard: abort the entire merge batch if cumulative domain_entropy
# drop would exceed this threshold. Uses the same source-prefix Shannon entropy
# metric as the drift logs (domain_entropy in [0,1], high=healthy).
# Observed cycle-1 drop: 0.4164 → 0.4079 = −0.0085. Floor set at 0.006 to catch
# any merge batch that causes a comparable structural collapse.
_MERGE_ENTROPY_FLOOR_DELTA = 0.006  # max cumulative domain_entropy drop per execute_repair call


def _topic_domain_entropy(topic: str, conn: sqlite3.Connection) -> float:
    """Source-prefix Shannon entropy for a topic via compute_structural_stress.
    Same metric and scope as the drift logs. Returns 1.0 (healthy) on failure."""
    if _HAS_STRESS:
        try:
            return compute_structural_stress(topic, conn).domain_entropy
        except Exception:
            pass
    return 1.0


def _apply_merge_atoms(preview: dict, conn: sqlite3.Connection) -> int:
    clusters = preview.get('atoms_to_merge', [])
    topic = preview.get('topic', 'jarvis')
    touched = 0
    entropy_baseline = _topic_domain_entropy(topic, conn)
    for cluster in clusters:
        if len(cluster) < 2:
            continue
        ph = ','.join('?' * len(cluster))
        cursor = conn.cursor()
        cursor.execute(f"SELECT id, object FROM facts WHERE id IN ({ph})", cluster)
        rows = cursor.fetchall()
        if not rows:
            continue
        keep_id = max(rows, key=lambda r: len(r[1] or ''))[0]
        to_zero = [r[0] for r in rows if r[0] != keep_id]

        # Entropy floor guard: apply cluster, then check cumulative drop.
        # If total drop since start of this merge batch exceeds the floor, undo
        # this cluster and all subsequent ones.
        _apply_zero_ids(to_zero, conn)
        entropy_now = _topic_domain_entropy(topic, conn)
        cumulative_drop = entropy_baseline - entropy_now
        if cumulative_drop > _MERGE_ENTROPY_FLOOR_DELTA:
            # Undo this cluster — restore confidence_effective
            conn.execute(
                f"UPDATE facts SET confidence_effective = 1.0 WHERE id IN ({ph})",
                to_zero
            )
            print(f"[MergeGuard] cluster {cluster} aborted: cumulative entropy drop "
                  f"{cumulative_drop:.4f} > floor {_MERGE_ENTROPY_FLOOR_DELTA} "
                  f"(baseline={entropy_baseline:.4f} now={entropy_now:.4f})")
            break  # stop processing further clusters in this batch
        else:
            touched += len(to_zero)
    return touched


def _apply_introduce_predicates(preview: dict, proposal_id: str, conn: sqlite3.Connection) -> int:
    predicates = preview.get('new_predicates', [])
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for pred in predicates:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO predicate_vocabulary (predicate, proposed_by, status, proposed_at) VALUES (?,?,'proposed',?)",
                (pred, proposal_id, now),
            )
            inserted += 1
        except Exception:
            pass
    return inserted


# ── restore_atoms strategy ───────────────────────────────────────────────────

# Entropy floor for restore_atoms trigger. If domain_entropy drops below
# (baseline - _RESTORE_ENTROPY_TRIGGER), reinstate soft-deleted atoms.
# Set just above the residual damage level (0.0003) to catch any merge-induced drop.
_RESTORE_ENTROPY_TRIGGER = 0.002   # trigger if entropy dropped > 0.002 from pre-repair baseline
_RESTORE_MAX_ATOMS       = 50      # max atoms to reinstate per restore_atoms call (safety cap)
_RESTORE_COOLDOWN_CYCLES = 3       # min repair cycles between restore_atoms executions (per topic)

# In-process cooldown tracker: topic -> last restore cycle count (reset on process restart)
_restore_cooldown: dict = {}


def _apply_restore_atoms(topic: str, conn: sqlite3.Connection,
                         entropy_before: float, entropy_after: float) -> int:
    """
    Reinstate soft-deleted atoms (confidence_effective=0) for a topic when
    domain_entropy has dropped materially. Selects atoms most likely to restore
    entropy: those from source prefixes that are now under-represented.

    Returns count of atoms reinstated.
    """
    drop = entropy_before - entropy_after
    if drop <= _RESTORE_ENTROPY_TRIGGER:
        return 0

    # Cooldown check
    import time
    now_ts = time.monotonic()
    last = _restore_cooldown.get(topic, 0.0)
    if (now_ts - last) < _RESTORE_COOLDOWN_CYCLES:
        print(f"[RestoreAtoms] cooldown active for topic='{topic}', skipping")
        return 0

    try:
        # Find soft-deleted atoms for this topic, ordered by recency (highest id first)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, source FROM facts
            WHERE (subject LIKE ? OR object LIKE ?)
              AND confidence_effective = 0.0
            ORDER BY id DESC
            LIMIT ?
            """,
            (f'%{topic}%', f'%{topic}%', _RESTORE_MAX_ATOMS * 3),
        )
        candidates = cursor.fetchall()
        if not candidates:
            return 0

        # Compute current source-prefix distribution among active atoms
        cursor.execute(
            "SELECT source FROM facts WHERE (subject LIKE ? OR object LIKE ?) "
            "AND (confidence_effective IS NULL OR confidence_effective > 0)",
            (f'%{topic}%', f'%{topic}%'),
        )
        active_sources = [r[0] or '' for r in cursor.fetchall()]
        prefix_counts: dict = {}
        for src in active_sources:
            p = src.split('_')[0] if '_' in src else src or 'unknown'
            prefix_counts[p] = prefix_counts.get(p, 0) + 1
        total_active = max(1, len(active_sources))

        # Score candidates: prefer atoms from under-represented source prefixes
        def _score(src: str) -> float:
            p = src.split('_')[0] if '_' in src else src or 'unknown'
            return 1.0 - (prefix_counts.get(p, 0) / total_active)

        scored = sorted(candidates, key=lambda r: _score(r[1] or ''), reverse=True)
        to_restore = [r[0] for r in scored[:_RESTORE_MAX_ATOMS]]

        if not to_restore:
            return 0

        ph = ','.join('?' * len(to_restore))
        conn.execute(
            f"UPDATE facts SET confidence_effective = 1.0 WHERE id IN ({ph})",
            to_restore,
        )
        _restore_cooldown[topic] = now_ts
        print(f"[RestoreAtoms] reinstated {len(to_restore)} atoms for topic='{topic}' "
              f"(entropy drop={drop:.4f} > trigger={_RESTORE_ENTROPY_TRIGGER})")
        return len(to_restore)
    except Exception as e:
        print(f"[RestoreAtoms] error: {e}")
        return 0


# ── Execution log ─────────────────────────────────────────────────────────────

def _write_execution_log(conn, proposal_id, action, strategy, topic,
                         before, after, simulation, divergence,
                         mutations, auto_rolled_back, rollback_reason, success, error,
                         governance_json=None):
    conn.execute(
        "INSERT INTO repair_execution_log "
        "(proposal_id, action, strategy, topic, signals_before_json, signals_after_json, "
        "simulation_json, divergence_json, mutations_applied, auto_rolled_back, "
        "rollback_reason, success, error, executed_at, governance_json) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            proposal_id, action, strategy, topic,
            json.dumps(before.to_dict()) if before else None,
            json.dumps(after.to_dict()) if after else None,
            json.dumps(simulation),
            json.dumps(divergence.to_dict()) if divergence else None,
            mutations, 1 if auto_rolled_back else 0,
            rollback_reason or None, 1 if success else 0,
            error or None, datetime.now(timezone.utc).isoformat(),
            governance_json,
        ),
    )


# ── Main execution function ───────────────────────────────────────────────────

def execute_repair(proposal_id: str, db_path: str, dry_run: bool = False) -> ExecutionResult:
    """
    Execute a repair proposal atomically. Must be status='pending'.
    dry_run=True previews mutations without committing.
    """
    result = ExecutionResult(
        proposal_id=proposal_id, strategy='unknown',
        dry_run=dry_run, success=False,
        executed_at=datetime.now(timezone.utc).isoformat(),
    )
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        ensure_executor_tables(conn)

        cursor = conn.cursor()
        cursor.execute(
            "SELECT strategy, topic, preview_json, simulation_json, status FROM repair_proposals WHERE id = ?",
            (proposal_id,),
        )
        row = cursor.fetchone()
        if not row:
            result.error = f"proposal_id={proposal_id} not found"
            return result
        strategy, topic, preview_json, simulation_json, status = row
        if status != 'pending':
            result.error = f"proposal status='{status}' — only 'pending' proposals can be executed"
            return result

        result.strategy = strategy
        preview = json.loads(preview_json or '{}')
        simulation = json.loads(simulation_json or '{}')

        print(f"[RepairExecutor] execute id={proposal_id[:8]}... strategy={strategy} dry_run={dry_run}")

        before = _snapshot_signals(topic, conn)
        result.signals_before = before
        print(f"[RepairExecutor] signals_before: stress={before.composite_stress:.2f} "
              f"conflict={before.conflict_cluster:.2f} entropy={before.domain_entropy:.2f} atoms={before.atom_count}")

        # ── Governance gate (Hook 1) ──────────────────────────────────────────
        governance_json_str = None
        if _HAS_VALIDATION:
            try:
                verdict = governance_verdict(validate_all(topic, conn), conn=conn)
                governance_json_str = json.dumps(verdict.to_dict())
                if not verdict.allow_execution:
                    result.error = f"governance block: {verdict.verdict_reason}"
                    _write_execution_log(
                        conn, proposal_id, 'governance_block', strategy, topic,
                        before, None, simulation, None, 0, False,
                        verdict.verdict_reason, False, result.error,
                        governance_json=governance_json_str,
                    )
                    conn.commit()
                    return result
                if strategy in verdict.suppressed_strategies:
                    result.error = (
                        f"governance suppressed strategy='{strategy}': "
                        f"{verdict.verdict_reason}"
                    )
                    _write_execution_log(
                        conn, proposal_id, 'governance_suppress', strategy, topic,
                        before, None, simulation, None, 0, False,
                        verdict.verdict_reason, False, result.error,
                        governance_json=governance_json_str,
                    )
                    conn.commit()
                    return result
            except Exception as _gov_err:
                print(f"[RepairExecutor] governance check error (non-fatal): {_gov_err}")

        # Collect all atom IDs that will be touched for rollback snapshot
        all_touched: List[int] = (
            [int(i) for i in preview.get('atoms_to_remove', [])] +
            [int(i) for cluster in preview.get('atoms_to_merge', []) for i in cluster]
        )

        conn.execute("BEGIN IMMEDIATE")
        try:
            if all_touched:
                _store_rollback_snapshot(proposal_id, all_touched, conn)

            if strategy in ('resolve_conflicts', 'deduplicate'):
                mutations = _apply_zero_ids([int(i) for i in preview.get('atoms_to_remove', [])], conn)
            elif strategy == 'reweight_sources':
                mutations = _apply_reweight_sources(preview, topic, conn)
            elif strategy == 'merge_atoms':
                mutations = _apply_merge_atoms(preview, conn)
            elif strategy == 'introduce_predicates':
                mutations = _apply_introduce_predicates(preview, proposal_id, conn)
            elif strategy == 'restore_atoms':
                entropy_floor_before = preview.get('entropy_before', before.domain_entropy)
                mutations = _apply_restore_atoms(topic, conn, entropy_floor_before, before.domain_entropy)
            else:
                mutations = 0  # split_domain, ingest_missing, manual_review — no facts mutation

            result.mutations_applied = mutations

            if dry_run:
                conn.execute("ROLLBACK")
                result.success = True
                print(f"[RepairExecutor] DRY_RUN complete — {mutations} mutations previewed, not applied")
                return result

            conn.execute("COMMIT")

        except Exception as e:
            conn.execute("ROLLBACK")
            result.error = f"transaction error: {e}"
            _write_execution_log(conn, proposal_id, 'execute', strategy, topic,
                                 before, None, simulation, None, 0, False, '', False, str(e))
            conn.commit()
            return result

        after = _snapshot_signals(topic, conn)
        result.signals_after = after
        print(f"[RepairExecutor] signals_after:  stress={after.composite_stress:.2f} "
              f"conflict={after.conflict_cluster:.2f} entropy={after.domain_entropy:.2f} atoms={after.atom_count}")

        divergence = _compute_divergence(simulation, before, after)
        result.divergence = divergence
        print(f"[RepairExecutor] divergence: mean_abs={divergence.mean_abs_divergence:.3f} "
              f"direction_correct={divergence.direction_correct}")

        # Auto-rollback triggers
        stress_increase  = after.composite_stress - before.composite_stress
        entropy_collapse = before.domain_entropy  - after.domain_entropy
        auto_rolled_back = False
        rollback_reason  = ''

        if stress_increase > _AUTO_ROLLBACK_STRESS_INCREASE:
            rollback_reason = (f"composite_stress increased by {stress_increase:+.3f} "
                               f"(threshold={_AUTO_ROLLBACK_STRESS_INCREASE})")
            auto_rolled_back = True
        elif entropy_collapse > _AUTO_ROLLBACK_ENTROPY_COLLAPSE:
            rollback_reason = (f"domain_entropy collapsed by {entropy_collapse:.3f} "
                               f"(threshold={_AUTO_ROLLBACK_ENTROPY_COLLAPSE})")
            auto_rolled_back = True

        if auto_rolled_back:
            print(f"[RepairExecutor] AUTO-ROLLBACK: {rollback_reason}")
            _do_rollback(proposal_id, conn)
            conn.execute("UPDATE repair_proposals SET status = 'rolled_back' WHERE id = ?", (proposal_id,))
            result.auto_rolled_back = True
            result.rollback_reason = rollback_reason
        else:
            conn.execute("UPDATE repair_proposals SET status = 'executed' WHERE id = ?", (proposal_id,))

        result.success = True
        action = 'auto_rollback' if auto_rolled_back else 'execute'
        _write_execution_log(conn, proposal_id, action, strategy, topic,
                             before, after, simulation, divergence,
                             mutations, auto_rolled_back, rollback_reason, True, '',
                             governance_json=governance_json_str)
        conn.commit()
        print(result.debug_str())
        return result

    except Exception as e:
        result.error = str(e)
        try:
            _write_execution_log(conn, proposal_id, 'execute', result.strategy, '',
                                 result.signals_before, None, {}, None, 0, False, '', False, str(e))
            conn.commit()
        except Exception:
            pass
        return result

    finally:
        conn.close()


# ── Rollback ──────────────────────────────────────────────────────────────────

def _do_rollback(proposal_id: str, conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute("SELECT snapshot_json FROM repair_rollback_log WHERE proposal_id = ?", (proposal_id,))
    row = cursor.fetchone()
    if not row:
        return 0
    snap = json.loads(row[0])
    restored = 0
    for atom in snap.get('atoms_original', []):
        conn.execute("UPDATE facts SET confidence_effective = ? WHERE id = ?",
                     (atom['confidence_effective'], atom['id']))
        restored += 1
    return restored


def rollback_repair(proposal_id: str, db_path: str) -> RollbackResult:
    """Restore atom states from rollback snapshot. Updates proposal status to 'rolled_back'."""
    result = RollbackResult(proposal_id=proposal_id, success=False)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        ensure_executor_tables(conn)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT strategy, topic, simulation_json, status FROM repair_proposals WHERE id = ?",
            (proposal_id,),
        )
        row = cursor.fetchone()
        if not row:
            result.error = f"proposal_id={proposal_id} not found"
            return result
        strategy, topic, simulation_json, status = row
        if status not in ('executed',):
            result.error = f"proposal status='{status}' — only 'executed' proposals can be rolled back"
            return result

        restored = _do_rollback(proposal_id, conn)
        conn.execute("UPDATE repair_proposals SET status = 'rolled_back' WHERE id = ?", (proposal_id,))

        after = _snapshot_signals(topic, conn)
        result.signals_after = after
        result.atoms_restored = restored
        result.success = True

        _write_execution_log(conn, proposal_id, 'rollback', strategy, topic,
                             None, after, json.loads(simulation_json or '{}'), None,
                             0, False, 'manual rollback', True, '')
        conn.commit()
        print(f"[RepairExecutor] ROLLBACK complete id={proposal_id[:8]}... "
              f"atoms_restored={restored} stress_after={after.composite_stress:.2f}")
        return result

    except Exception as e:
        result.error = str(e)
        return result
    finally:
        conn.close()


# ── Impact score ──────────────────────────────────────────────────────────────

def repair_impact_score(strategy: str, db_path: str) -> ImpactScore:
    """
    Aggregate calibration metrics for a strategy across all executed repairs.
    Phase 7 training signal when n_executions >= 30.
    """
    score = ImpactScore(strategy=strategy)
    conn = sqlite3.connect(db_path, check_same_thread=False)

    try:
        ensure_executor_tables(conn)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT signals_before_json, signals_after_json, divergence_json, "
            "auto_rolled_back, action FROM repair_execution_log "
            "WHERE strategy = ? AND success = 1 AND action IN ('execute', 'auto_rollback')",
            (strategy,),
        )
        rows = cursor.fetchall()
        if not rows:
            return score

        score.n_executions = len(rows)
        stress_deltas = []
        divergences = []

        for before_json, after_json, div_json, auto_rb, action in rows:
            if auto_rb:
                score.n_auto_rollbacks += 1
            try:
                before = json.loads(before_json or '{}')
                after  = json.loads(after_json  or '{}')
                delta = (after.get('composite_stress', 0) - before.get('composite_stress', 0))
                stress_deltas.append(delta)
            except Exception:
                pass
            try:
                div = json.loads(div_json or '{}')
                divergences.append(div.get('mean_abs_divergence', 0.0))
            except Exception:
                pass

        # Manual rollbacks (action='rollback')
        cursor.execute(
            "SELECT COUNT(*) FROM repair_execution_log "
            "WHERE strategy = ? AND action = 'rollback'",
            (strategy,),
        )
        score.n_rollbacks = cursor.fetchone()[0] or 0

        if stress_deltas:
            mean = sum(stress_deltas) / len(stress_deltas)
            score.mean_stress_delta = round(mean, 4)
            variance = sum((d - mean) ** 2 for d in stress_deltas) / len(stress_deltas)
            score.stress_delta_variance = round(variance, 4)

        if divergences:
            score.mean_abs_divergence = round(sum(divergences) / len(divergences), 4)

        total = score.n_executions
        score.rollback_rate   = round((score.n_rollbacks + score.n_auto_rollbacks) / max(1, total), 4)
        score.side_effect_rate = round(score.n_auto_rollbacks / max(1, total), 4)

        # Reliability classification
        if (score.rollback_rate < _STABLE_ROLLBACK_RATE_MAX
                and score.stress_delta_variance < _STABLE_VARIANCE_MAX):
            score.reliability = 'stable'
        elif (score.rollback_rate > _CHAOTIC_ROLLBACK_RATE_MIN
              or score.side_effect_rate > _CHAOTIC_SIDE_EFFECT_MIN):
            score.reliability = 'chaotic'
        else:
            score.reliability = 'variable'

        print(f"[RepairImpact] strategy={strategy} n={total} "
              f"mean_stress_delta={score.mean_stress_delta:+.3f} "
              f"rollback_rate={score.rollback_rate:.2f} "
              f"reliability={score.reliability}")
        return score

    except Exception as e:
        print(f"[RepairImpact] error: {e}")
        return score
    finally:
        conn.close()

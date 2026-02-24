"""
knowledge/kb_validation.py — KB Epistemic Validation & Governance (Phase 7)

Three-layer validation that acts as a governance authority over the repair pipeline.
Validation findings actively suppress and downgrade repair strategies — not passive reporting.

Layers:
  1. Schema    — required predicates present? (wraps kb_domain_schemas)
  2. Semantic  — values plausible and internally consistent?
  3. Cross-topic — related entities agree about shared facts?

Governance output:
  governance_verdict(reports, conn=None) -> GovernanceVerdict
    .suppressed_strategies  — blocked from execution
    .confidence_penalty     — subtracted from diagnosis.confidence before gating
    .allow_execution        — hard block when True is False
    .adaptive_threshold     — computed threshold (or static if cold-start)
    .adaptive_would_suppress — whether adaptive gate would have fired

Adaptive governance (Phase 7 — Dynamic Thresholding):
  When conn is provided, governance_verdict() writes the current severity snapshot
  to the governance_metrics table and computes an adaptive suppression threshold:

    adaptive_threshold = max(STATIC_MIN, rolling_mean + 3 * rolling_std)

  The adaptive verdict is computed in SHADOW MODE: static verdict is used for
  actual suppression; adaptive verdict is logged only. Divergence cases are printed
  so they can be studied in PS13 before adaptive mode is activated.

  Cold-start guard: < 5 observations → fall back to static threshold entirely.

Design invariants:
  - Zero-LLM: all checks are constraint/agreement/keyword-based
  - Read-only: never writes to facts table (governance_metrics is a separate table)
  - Bounded cost: all loops capped (max 200 atoms, 20 cross-topic checks)
  - Governance is deterministic: suppression rules are hard-coded thresholds
  - GovernanceVerdict.to_dict() produces a frozen snapshot — written once, never recomputed
"""

from __future__ import annotations

import math
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    from knowledge.kb_domain_schemas import (
        DOMAIN_PREDICATE_SCHEMAS,
        PREDICATE_VALUE_CONSTRAINTS,
        _ANTONYM_SIGNALS,
        detect_topic_domain,
        missing_schema_predicates,
        schema_completeness,
    )
    _HAS_SCHEMAS = True
except ImportError:
    _HAS_SCHEMAS = False

try:
    from knowledge.authority import get_authority
    _HAS_AUTHORITY = True
except ImportError:
    def get_authority(source: str) -> float:
        return 0.5
    _HAS_AUTHORITY = False


# ── Constants ──────────────────────────────────────────────────────────────────

_MAX_ATOMS = 200
_MAX_CROSS_TOPIC_CHECKS = 20
_MIN_SIBLINGS_FOR_ORG_CHECK = 3

# Governance suppression thresholds (static)
_SEMANTIC_SUPPRESS_THRESHOLD   = 0.65  # suppress merge_atoms, deduplicate (PS10: 8x wider margin, zero FP)
_SEMANTIC_DOWNGRADE_THRESHOLD  = 0.5   # downgrade introduce_predicates
_CROSS_SUPPRESS_THRESHOLD      = 0.6   # suppress split_domain
_CROSS_PENALTY_THRESHOLD       = 0.4   # apply confidence_penalty
_HARD_BLOCK_SCHEMA_SEVERITY    = 1.0   # schema_severity == 1.0 (all missing)
_HARD_BLOCK_SEMANTIC_THRESHOLD = 0.5   # + semantic_severity > 0.5 → hard block

# Adaptive governance constants (Phase 7)
_ADAPTIVE_STATIC_MIN      = 0.60   # floor: adaptive threshold can never go below this
_ADAPTIVE_K_SIGMA         = 3.0    # multiplier: mean + K * std
_ADAPTIVE_WINDOW          = 50     # rolling window size (rows kept in governance_metrics)
_ADAPTIVE_MIN_OBS         = 5      # cold-start guard: need at least this many observations

# Severity aggregation function (PS27 experiment)
# Options: 'mean' (default) | 'max' | 'weighted_mean' | 'boost'
#   mean          — arithmetic mean of issue severities (original, 0.65 ceiling)
#   max           — maximum issue severity (activates suppression at sev=0.70)
#   weighted_mean — sum(sev²)/sum(sev), upweights high-severity issues
#   boost         — mean + 1.0*std, penalises co-occurring high-severity issues
_SEVERITY_AGGREGATION = 'mean'

# Source agreement
_LOW_AUTHORITY_CUTOFF = 0.5

# Symmetric predicates for cross-topic check
_SYMMETRIC_PREDICATES = {'integrates_with', 'related_to', 'compared_to', 'depends_on'}


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class ValidationIssue:
    predicate: str
    atom_id: Optional[int]
    description: str
    severity: float
    issue_type: str  # 'missing' | 'constraint_violation' | 'low_authority_singleton'
                     # | 'source_conflict' | 'semantic_contradiction' | 'cross_topic_asymmetry'
                     # | 'org_outlier'


@dataclass
class ValidationReport:
    topic: str
    layer: str          # 'schema' | 'semantic' | 'cross_topic'
    issues: List[ValidationIssue] = field(default_factory=list)
    severity: float = 0.0
    passed: bool = True
    signals: Dict[str, float] = field(default_factory=dict)

    def _compute_severity(self) -> None:
        if not self.issues:
            self.severity = 0.0
            self.passed = True
        else:
            sevs = [i.severity for i in self.issues]
            n = len(sevs)
            agg = _SEVERITY_AGGREGATION
            if agg == 'max':
                raw = max(sevs)
            elif agg == 'weighted_mean':
                # sum(sev²) / sum(sev) — upweights high-severity issues
                raw = sum(s * s for s in sevs) / max(1e-9, sum(sevs))
            elif agg == 'boost':
                # mean + 1.0 * std — penalises co-occurring high-severity issues
                mean_ = sum(sevs) / n
                std_  = (sum((s - mean_) ** 2 for s in sevs) / n) ** 0.5
                raw = mean_ + 1.0 * std_
            else:
                # 'mean' — arithmetic mean (original, PS10 baseline)
                raw = sum(sevs) / n
            self.severity = round(min(1.0, raw), 4)
            self.passed = self.severity < 0.3


@dataclass
class GovernanceVerdict:
    topic: str
    schema_severity: float
    semantic_severity: float
    cross_topic_severity: float
    suppressed_strategies: List[str]
    downgraded_strategies: Dict[str, str]   # strategy -> reason
    confidence_penalty: float
    allow_execution: bool
    verdict_reason: str
    adaptive_threshold: float = _SEMANTIC_SUPPRESS_THRESHOLD   # effective threshold used
    adaptive_would_suppress: bool = False                       # shadow: would adaptive have fired?

    def to_dict(self) -> dict:
        return {
            'schema_severity':         round(self.schema_severity, 4),
            'semantic_severity':       round(self.semantic_severity, 4),
            'cross_topic_severity':    round(self.cross_topic_severity, 4),
            'confidence_penalty':      round(self.confidence_penalty, 4),
            'blocked_strategies':      list(self.suppressed_strategies),
            'downgraded_strategies':   dict(self.downgraded_strategies),
            'allowed':                 self.allow_execution,
            'verdict_reason':          self.verdict_reason,
            'adaptive_threshold':      round(self.adaptive_threshold, 4),
            'adaptive_would_suppress': self.adaptive_would_suppress,
            'captured_at':             datetime.now(timezone.utc).isoformat(),
        }


# ── Atom fetcher ───────────────────────────────────────────────────────────────

def _fetch_atoms(topic: str, conn: sqlite3.Connection) -> List[dict]:
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, subject, predicate, object, source, confidence, confidence_effective "
            "FROM facts WHERE (subject LIKE ? OR object LIKE ?) "
            "AND (confidence_effective IS NULL OR confidence_effective > 0) "
            "ORDER BY id ASC LIMIT ?",
            (f'%{topic}%', f'%{topic}%', _MAX_ATOMS),
        )
        cols = ['id', 'subject', 'predicate', 'object', 'source', 'confidence', 'confidence_effective']
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception:
        return []


# ── Layer 1: Schema validation ─────────────────────────────────────────────────

def validate_schema(topic: str, conn: sqlite3.Connection) -> ValidationReport:
    """
    Layer 1: Are required domain predicates present?
    Wraps kb_domain_schemas — no new logic, just structured output.
    """
    report = ValidationReport(topic=topic, layer='schema')

    if not _HAS_SCHEMAS:
        report.signals['error'] = 1.0
        report._compute_severity()
        return report

    atoms = _fetch_atoms(topic, conn)
    domain = detect_topic_domain(topic, atoms)
    existing_preds = {(a['predicate'] or '').lower() for a in atoms if a['predicate']}

    report.signals['domain_detected'] = 1.0 if domain else 0.0

    if not domain:
        report.signals['schema_completeness'] = 1.0
        report._compute_severity()
        return report

    missing = missing_schema_predicates(domain, existing_preds)
    completeness = schema_completeness(domain, existing_preds)
    report.signals['schema_completeness'] = round(completeness, 4)
    report.signals['missing_count'] = float(len(missing))

    schema_len = len(DOMAIN_PREDICATE_SCHEMAS.get(domain, []))
    missing_frac = len(missing) / max(1, schema_len)

    for pred in missing:
        report.issues.append(ValidationIssue(
            predicate=pred,
            atom_id=None,
            description=f"Required predicate '{pred}' absent for domain '{domain}'",
            severity=round(missing_frac, 4),
            issue_type='missing',
        ))

    report._compute_severity()
    return report


# ── Layer 2: Semantic validation ───────────────────────────────────────────────

def validate_semantics(topic: str, conn: sqlite3.Connection) -> ValidationReport:
    """
    Layer 2: Are predicate values plausible and internally consistent?
    Sub-checks: value constraints, source agreement, intra-topic contradiction.
    """
    report = ValidationReport(topic=topic, layer='semantic')

    atoms = _fetch_atoms(topic, conn)
    if not atoms:
        report._compute_severity()
        return report

    constraints = {}
    antonym_signals = {}
    if _HAS_SCHEMAS:
        try:
            constraints = PREDICATE_VALUE_CONSTRAINTS
        except Exception:
            pass
        try:
            antonym_signals = _ANTONYM_SIGNALS
        except Exception:
            pass

    # Index atoms by predicate for efficient lookup
    by_pred: Dict[str, List[dict]] = {}
    for a in atoms:
        pred = (a['predicate'] or '').lower()
        if pred:
            by_pred.setdefault(pred, []).append(a)

    # 2A — Value constraint checking
    for pred, rules in constraints.items():
        for atom in by_pred.get(pred, []):
            obj = atom['object'] or ''
            obj_len = len(obj.strip())
            min_len = rules.get('min_length', 0)
            max_len = rules.get('max_length', 10000)
            if obj_len < min_len:
                violation = (min_len - obj_len) / max(1, min_len)
                report.issues.append(ValidationIssue(
                    predicate=pred,
                    atom_id=atom['id'],
                    description=f"'{pred}' value too short ({obj_len} < {min_len} chars): '{obj[:60]}'",
                    severity=round(min(0.8, 0.3 + violation * 0.5), 4),
                    issue_type='constraint_violation',
                ))
            elif obj_len > max_len:
                violation = (obj_len - max_len) / max(1, max_len)
                report.issues.append(ValidationIssue(
                    predicate=pred,
                    atom_id=atom['id'],
                    description=f"'{pred}' value too long ({obj_len} > {max_len} chars)",
                    severity=round(min(0.5, 0.2 + violation * 0.3), 4),
                    issue_type='constraint_violation',
                ))

    # 2B — Source agreement check
    evaluative_preds = set(constraints.keys()) if constraints else {
        'has_purpose', 'target_user', 'architectural_pattern',
        'known_limitation', 'has_weakness', 'has_strength',
    }
    for pred in evaluative_preds:
        group = by_pred.get(pred, [])
        if not group:
            continue
        sources = [a['source'] for a in group if a['source']]
        unique_sources = set(sources)

        if len(unique_sources) == 1:
            auth = get_authority(sources[0])
            if auth < _LOW_AUTHORITY_CUTOFF:
                report.issues.append(ValidationIssue(
                    predicate=pred,
                    atom_id=group[0]['id'],
                    description=(
                        f"'{pred}' provided by single low-authority source "
                        f"'{sources[0]}' (authority={auth:.2f})"
                    ),
                    severity=0.4,
                    issue_type='low_authority_singleton',
                ))
        elif len(unique_sources) > 1:
            # Check if object values diverge significantly across sources
            obj_token_sets = [
                set((a['object'] or '').lower().split())
                for a in group if a['object']
            ]
            if len(obj_token_sets) >= 2:
                # Pairwise Jaccard — if any pair is very dissimilar, flag conflict
                ts_a, ts_b = obj_token_sets[0], obj_token_sets[1]
                union = ts_a | ts_b
                jaccard = len(ts_a & ts_b) / max(1, len(union))
                if jaccard < 0.15 and len(ts_a) > 3 and len(ts_b) > 3:
                    report.issues.append(ValidationIssue(
                        predicate=pred,
                        atom_id=None,
                        description=(
                            f"'{pred}' has conflicting values across {len(unique_sources)} sources "
                            f"(Jaccard={jaccard:.2f})"
                        ),
                        severity=0.6,
                        issue_type='source_conflict',
                    ))

    # 2C — Intra-topic contradiction detection
    for (pred_a, pred_b), antonym_pairs in antonym_signals.items():
        atoms_a = by_pred.get(pred_a, [])
        atoms_b = by_pred.get(pred_b, [])
        if not atoms_a or not atoms_b:
            continue
        text_a = ' '.join((a['object'] or '').lower() for a in atoms_a)
        text_b = ' '.join((a['object'] or '').lower() for a in atoms_b)
        tokens_a = set(text_a.split())
        tokens_b = set(text_b.split())
        for (kw_set_a, kw_set_b) in antonym_pairs:
            hit_a = bool(tokens_a & kw_set_a)
            hit_b = bool(tokens_b & kw_set_b)
            hit_a_rev = bool(tokens_a & kw_set_b)
            hit_b_rev = bool(tokens_b & kw_set_a)
            if (hit_a and hit_b) or (hit_a_rev and hit_b_rev):
                report.issues.append(ValidationIssue(
                    predicate=f'{pred_a}+{pred_b}',
                    atom_id=None,
                    description=(
                        f"Semantic contradiction between '{pred_a}' and '{pred_b}': "
                        f"opposing keyword signals detected"
                    ),
                    severity=0.7,
                    issue_type='semantic_contradiction',
                ))
                break  # one contradiction per pair is enough

    report.signals['constraint_violations'] = float(
        sum(1 for i in report.issues if i.issue_type == 'constraint_violation')
    )
    report.signals['source_conflicts'] = float(
        sum(1 for i in report.issues if i.issue_type == 'source_conflict')
    )
    report.signals['contradictions'] = float(
        sum(1 for i in report.issues if i.issue_type == 'semantic_contradiction')
    )
    report._compute_severity()
    return report


# ── Layer 3: Cross-topic validation ───────────────────────────────────────────

def validate_cross_topic(topic: str, conn: sqlite3.Connection) -> ValidationReport:
    """
    Layer 3: Do related entities agree about shared facts?
    Sub-checks: relational symmetry, org-level consistency.
    """
    report = ValidationReport(topic=topic, layer='cross_topic')

    try:
        cursor = conn.cursor()

        # 3A — Relational symmetry check
        asymmetries = 0
        checked = 0
        cursor.execute(
            "SELECT id, predicate, object FROM facts "
            "WHERE subject LIKE ? AND predicate IN ({}) "
            "AND (confidence_effective IS NULL OR confidence_effective > 0) LIMIT ?".format(
                ','.join('?' * len(_SYMMETRIC_PREDICATES))
            ),
            [f'%{topic}%'] + list(_SYMMETRIC_PREDICATES) + [_MAX_CROSS_TOPIC_CHECKS],
        )
        sym_rows = cursor.fetchall()

        for atom_id, pred, obj in sym_rows:
            if not obj:
                continue
            checked += 1
            # Check if the reciprocal exists: (obj, pred, topic)
            cursor.execute(
                "SELECT COUNT(*) FROM facts "
                "WHERE subject LIKE ? AND predicate = ? AND object LIKE ? "
                "AND (confidence_effective IS NULL OR confidence_effective > 0)",
                (f'%{obj}%', pred, f'%{topic}%'),
            )
            count = cursor.fetchone()[0] or 0
            if count == 0:
                asymmetries += 1
                report.issues.append(ValidationIssue(
                    predicate=pred,
                    atom_id=atom_id,
                    description=(
                        f"Asymmetric relation: ({topic!r}, {pred!r}, {obj!r}) exists "
                        f"but reciprocal does not"
                    ),
                    severity=0.4,
                    issue_type='cross_topic_asymmetry',
                ))

        report.signals['symmetric_checks'] = float(checked)
        report.signals['asymmetries'] = float(asymmetries)

        # 3B — Org-level consistency check (org:repo pattern)
        import re
        org_match = re.match(r'^([\w\-\.]+)[:/]([\w\-\.]+)$', topic)
        if org_match and _HAS_SCHEMAS:
            org = org_match.group(1)
            # Find sibling topics in same org
            cursor.execute(
                "SELECT DISTINCT subject FROM facts "
                "WHERE subject LIKE ? AND subject != ? "
                "AND (confidence_effective IS NULL OR confidence_effective > 0) LIMIT 30",
                (f'{org}%', topic),
            )
            siblings = [r[0] for r in cursor.fetchall()
                        if re.match(r'^[\w\-\.]+[:/][\w\-\.]+$', r[0])]

            report.signals['org_siblings'] = float(len(siblings))

            if len(siblings) >= _MIN_SIBLINGS_FOR_ORG_CHECK:
                # Compare architectural_pattern across siblings
                cursor.execute(
                    "SELECT object FROM facts "
                    "WHERE subject LIKE ? AND predicate = 'architectural_pattern' "
                    "AND (confidence_effective IS NULL OR confidence_effective > 0) LIMIT 5",
                    (f'%{topic}%',),
                )
                topic_arch_rows = cursor.fetchall()
                topic_arch_tokens = set()
                for (obj,) in topic_arch_rows:
                    topic_arch_tokens |= set((obj or '').lower().split())

                if topic_arch_tokens:
                    sibling_arch_tokens: set = set()
                    for sib in siblings[:10]:
                        cursor.execute(
                            "SELECT object FROM facts "
                            "WHERE subject LIKE ? AND predicate = 'architectural_pattern' "
                            "AND (confidence_effective IS NULL OR confidence_effective > 0) LIMIT 3",
                            (f'%{sib}%',),
                        )
                        for (obj,) in cursor.fetchall():
                            sibling_arch_tokens |= set((obj or '').lower().split())

                    if sibling_arch_tokens:
                        overlap = topic_arch_tokens & sibling_arch_tokens
                        if not overlap:
                            report.issues.append(ValidationIssue(
                                predicate='architectural_pattern',
                                atom_id=None,
                                description=(
                                    f"Org outlier: '{topic}' architectural_pattern has no token "
                                    f"overlap with {len(siblings)} org siblings"
                                ),
                                severity=0.3,
                                issue_type='org_outlier',
                            ))

    except Exception as e:
        report.signals['error'] = 1.0
        report.signals['error_msg'] = 0.0  # can't store string in float signals

    report._compute_severity()
    return report


# ── validate_all ───────────────────────────────────────────────────────────────

def validate_all(topic: str, conn: sqlite3.Connection) -> List[ValidationReport]:
    """Run all three validation layers and return reports."""
    return [
        validate_schema(topic, conn),
        validate_semantics(topic, conn),
        validate_cross_topic(topic, conn),
    ]


# ── Adaptive governance helpers ────────────────────────────────────────────────

def _ensure_governance_metrics_table(conn: sqlite3.Connection) -> None:
    """Create governance_metrics table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS governance_metrics (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            topic                TEXT NOT NULL,
            semantic_severity    REAL,
            schema_severity      REAL,
            cross_topic_severity REAL,
            domain_entropy       REAL,
            merge_density        REAL,
            captured_at          TEXT NOT NULL,
            is_baseline          INTEGER NOT NULL DEFAULT 0
        )
    """)
    # Migrate existing tables that lack is_baseline
    try:
        conn.execute("ALTER TABLE governance_metrics ADD COLUMN is_baseline INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass  # column already exists


def _record_governance_metrics(
    conn: sqlite3.Connection,
    topic: str,
    semantic_severity: float,
    schema_severity: float,
    cross_topic_severity: float,
    is_baseline: bool = False,
) -> None:
    """
    Append a severity snapshot to governance_metrics and prune to the
    last _ADAPTIVE_WINDOW rows (globally, not per-topic).

    is_baseline=True marks rows written during the seed/baseline phase.
    Damage/recovery observations are written with is_baseline=False (default)
    so that _compute_adaptive_threshold(baseline_only=True) can ignore them.
    """
    try:
        _ensure_governance_metrics_table(conn)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO governance_metrics
                (topic, semantic_severity, schema_severity, cross_topic_severity,
                 captured_at, is_baseline)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (topic, semantic_severity, schema_severity, cross_topic_severity,
             now, 1 if is_baseline else 0),
        )
        # Prune: keep all baseline rows; prune non-baseline rows to _ADAPTIVE_WINDOW.
        # This prevents stressed damage/recovery observations from evicting the
        # baseline window and inflating the adaptive threshold (PS22 fix).
        conn.execute(
            """
            DELETE FROM governance_metrics
            WHERE is_baseline = 0
            AND id NOT IN (
                SELECT id FROM governance_metrics
                WHERE is_baseline = 0
                ORDER BY id DESC LIMIT ?
            )
            """,
            (_ADAPTIVE_WINDOW,),
        )
        conn.commit()
    except Exception as e:
        print(f"[AdaptiveGov] metrics write error: {e}")


def _compute_adaptive_threshold(
    conn: sqlite3.Connection,
    baseline_only: bool = False,
) -> tuple[float, dict]:
    """
    Compute adaptive suppression threshold from rolling history.

    Returns (threshold, stats_dict) where stats_dict contains:
      n, mean, std, cold_start

    Formula: max(_ADAPTIVE_STATIC_MIN, mean + _ADAPTIVE_K_SIGMA * std)
    Cold-start guard: if < _ADAPTIVE_MIN_OBS rows, return static threshold.

    baseline_only=True: use only rows where is_baseline=1 (seed/pre-damage
    observations). This prevents stressed recovery observations from inflating
    the threshold and disabling the adaptive gate (threshold drift bug).
    """
    try:
        _ensure_governance_metrics_table(conn)
        if baseline_only:
            rows = conn.execute(
                "SELECT semantic_severity FROM governance_metrics "
                "WHERE semantic_severity IS NOT NULL AND is_baseline = 1 "
                "ORDER BY id DESC LIMIT ?",
                (_ADAPTIVE_WINDOW,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT semantic_severity FROM governance_metrics "
                "WHERE semantic_severity IS NOT NULL "
                "ORDER BY id DESC LIMIT ?",
                (_ADAPTIVE_WINDOW,),
            ).fetchall()
        values = [r[0] for r in rows if r[0] is not None]
        n = len(values)

        if n < _ADAPTIVE_MIN_OBS:
            return _SEMANTIC_SUPPRESS_THRESHOLD, {
                'n': n, 'mean': None, 'std': None, 'cold_start': True
            }

        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        std = math.sqrt(variance)

        threshold = max(_ADAPTIVE_STATIC_MIN, mean + _ADAPTIVE_K_SIGMA * std)
        return round(threshold, 4), {
            'n': n,
            'mean': round(mean, 4),
            'std': round(std, 4),
            'cold_start': False,
            'baseline_only': baseline_only,
        }
    except Exception as e:
        print(f"[AdaptiveGov] threshold compute error: {e}")
        return _SEMANTIC_SUPPRESS_THRESHOLD, {
            'n': 0, 'mean': None, 'std': None, 'cold_start': True
        }


# ── Governance verdict ─────────────────────────────────────────────────────────

def governance_verdict(
    reports: List[ValidationReport],
    conn: Optional[sqlite3.Connection] = None,
) -> GovernanceVerdict:
    """
    Derive a GovernanceVerdict from validation reports.

    Suppression rules (deterministic, hard-coded):
      semantic_severity > _SEMANTIC_SUPPRESS_THRESHOLD (0.65)
                        → suppress merge_atoms, deduplicate, reweight_sources
      semantic_severity > _SEMANTIC_DOWNGRADE_THRESHOLD (0.5)
                        → downgrade introduce_predicates
      cross_topic_severity > _CROSS_SUPPRESS_THRESHOLD (0.6)
                        → suppress split_domain
      cross_topic_severity > _CROSS_PENALTY_THRESHOLD (0.4)
                        → confidence_penalty = cross_topic_severity * 0.2
      schema_severity == 1.0 AND semantic_severity > 0.5
                        → hard block (allow_execution=False)

    Adaptive governance (shadow mode — Phase 7):
      When conn is provided:
        1. Write current severities to governance_metrics table.
        2. Compute adaptive_threshold = max(0.60, rolling_mean + 3*rolling_std).
        3. Evaluate whether adaptive gate would suppress (shadow only — not enforced).
        4. Log divergence when static and adaptive verdicts differ.
      Static verdict is always used for actual suppression.
    """
    schema_sev     = 0.0
    semantic_sev   = 0.0
    cross_sev      = 0.0
    topic          = ''

    for r in reports:
        topic = r.topic
        if r.layer == 'schema':
            schema_sev = r.severity
        elif r.layer == 'semantic':
            semantic_sev = r.severity
        elif r.layer == 'cross_topic':
            cross_sev = r.severity

    # ── Adaptive threshold ────────────────────────────────────────────────────
    governance_mode = os.environ.get('JARVIS_GOVERNANCE_MODE', 'static')
    adaptive_threshold = _SEMANTIC_SUPPRESS_THRESHOLD
    adaptive_stats: dict = {'cold_start': True}
    if conn is not None:
        try:
            # Compute threshold from prior history BEFORE recording current observation.
            # This ensures the gate reflects the rolling baseline, not the current cycle.
            adaptive_threshold, adaptive_stats = _compute_adaptive_threshold(conn)
            _record_governance_metrics(
                conn, topic, semantic_sev, schema_sev, cross_sev
            )
        except Exception as _ae:
            print(f"[AdaptiveGov] error during metrics/threshold: {_ae}")

    adaptive_would_suppress = semantic_sev > adaptive_threshold

    suppressed: List[str] = []
    downgraded: Dict[str, str] = {}
    confidence_penalty = 0.0
    allow_execution = True
    reasons: List[str] = []

    # Hard block: schema completely empty AND semantic values bad
    if schema_sev >= _HARD_BLOCK_SCHEMA_SEVERITY and semantic_sev > _HARD_BLOCK_SEMANTIC_THRESHOLD:
        allow_execution = False
        reasons.append(
            f"hard block: schema_severity={schema_sev:.2f} (all predicates missing) "
            f"AND semantic_severity={semantic_sev:.2f} > {_HARD_BLOCK_SEMANTIC_THRESHOLD}"
        )

    # Suppress merge_atoms, deduplicate, and reweight_sources when semantic instability is high
    static_would_suppress = semantic_sev > _SEMANTIC_SUPPRESS_THRESHOLD
    # In adaptive mode, use the computed adaptive_threshold for enforcement
    effective_threshold = (
        adaptive_threshold
        if governance_mode == 'adaptive' and not adaptive_stats.get('cold_start', True)
        else _SEMANTIC_SUPPRESS_THRESHOLD
    )
    if semantic_sev > effective_threshold:
        suppressed.extend(['merge_atoms', 'deduplicate', 'reweight_sources'])
        reasons.append(
            f"suppress merge_atoms+deduplicate+reweight_sources: semantic_severity={semantic_sev:.2f} "
            f"> {effective_threshold} (mode={governance_mode})"
        )

    # Downgrade introduce_predicates when semantic values are questionable
    if semantic_sev > _SEMANTIC_DOWNGRADE_THRESHOLD:
        penalty_pct = round(semantic_sev * 0.3, 4)
        downgraded['introduce_predicates'] = (
            f"semantic_severity={semantic_sev:.2f} > {_SEMANTIC_DOWNGRADE_THRESHOLD}; "
            f"confidence penalized by {penalty_pct:.2f}"
        )
        reasons.append(f"downgrade introduce_predicates by {penalty_pct:.2f}")

    # Suppress split_domain when cross-topic coherence is broken
    if cross_sev > _CROSS_SUPPRESS_THRESHOLD:
        suppressed.append('split_domain')
        reasons.append(
            f"suppress split_domain: cross_topic_severity={cross_sev:.2f} "
            f"> {_CROSS_SUPPRESS_THRESHOLD}"
        )

    # Apply confidence penalty for cross-topic drift
    if cross_sev > _CROSS_PENALTY_THRESHOLD:
        confidence_penalty = round(cross_sev * 0.2, 4)
        reasons.append(
            f"confidence_penalty={confidence_penalty:.2f} from "
            f"cross_topic_severity={cross_sev:.2f}"
        )

    verdict_reason = '; '.join(reasons) if reasons else 'all layers passed'

    verdict = GovernanceVerdict(
        topic=topic,
        schema_severity=round(schema_sev, 4),
        semantic_severity=round(semantic_sev, 4),
        cross_topic_severity=round(cross_sev, 4),
        suppressed_strategies=suppressed,
        downgraded_strategies=downgraded,
        confidence_penalty=round(confidence_penalty, 4),
        allow_execution=allow_execution,
        verdict_reason=verdict_reason,
        adaptive_threshold=round(adaptive_threshold, 4),
        adaptive_would_suppress=adaptive_would_suppress,
    )

    if not allow_execution or suppressed or confidence_penalty > 0:
        print(
            f"[Governance] topic={topic!r} "
            f"schema={schema_sev:.2f} semantic={semantic_sev:.2f} cross={cross_sev:.2f} "
            f"allowed={allow_execution} suppressed={suppressed} penalty={confidence_penalty:.2f}"
        )

    # Shadow mode divergence logging
    if conn is not None and not adaptive_stats.get('cold_start', True):
        if static_would_suppress != adaptive_would_suppress:
            direction = (
                'static=SUPPRESS adaptive=PASS'
                if static_would_suppress and not adaptive_would_suppress
                else 'static=PASS adaptive=SUPPRESS'
            )
            print(
                f"[AdaptiveGov] SHADOW divergence topic={topic!r} "
                f"{direction} "
                f"severity={semantic_sev:.4f} "
                f"static_threshold={_SEMANTIC_SUPPRESS_THRESHOLD} "
                f"adaptive_threshold={adaptive_threshold:.4f} "
                f"(mean={adaptive_stats['mean']:.4f} "
                f"std={adaptive_stats['std']:.4f} "
                f"n={adaptive_stats['n']})"
            )

    return verdict

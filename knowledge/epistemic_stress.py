"""
knowledge/epistemic_stress.py — Epistemic Stress Signaling

Derives five stress signals from the KB governance layer (Phase 1).
These signals make atoms into sensors — feeding the executive arbiter
with knowledge-quality information so it can modulate decisions accordingly.

Signals:
  supersession_density  — domain instability (atoms being actively revised)
  decay_pressure        — staleness of retrieved atoms
  authority_conflict    — source disagreement (std dev of authority weights)
  conflict_cluster      — known contradiction hotspot for this topic
  domain_entropy        — reasoning breadth (low = narrow sourcing bias)

All signals: float in [0, 1]. Higher = more stress (except domain_entropy
where LOW is the stress condition — low entropy = narrow sourcing).

Zero-LLM, pure Python, <2ms per call.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

try:
    from knowledge.authority import get_authority
    _HAS_AUTHORITY = True
except ImportError:
    _HAS_AUTHORITY = False
    def get_authority(source: str) -> float:
        return 0.5


# ── Constants ──────────────────────────────────────────────────────────────────

_SUPERSESSION_WINDOW_DAYS = 7    # look back this many days for recent conflicts
_COMPOSITE_WEIGHTS = {
    'supersession': 0.30,
    'decay':        0.25,
    'authority':    0.20,
    'conflict':     0.15,
    'entropy':      0.10,   # weight on (1 - domain_entropy) — low entropy = stress
}


# ── Report dataclass ───────────────────────────────────────────────────────────

@dataclass
class EpistemicStressReport:
    """
    Five epistemic stress signals + composite score.
    All values in [0, 1]. Higher = more stress.

    Exception: domain_entropy is in [0, 1] where LOW = stress (narrow sourcing).
    The composite_stress uses (1 - domain_entropy) as the stress contribution.
    """
    supersession_density: float = 0.0   # domain instability
    decay_pressure:       float = 0.0   # staleness of retrieved atoms
    authority_conflict:   float = 0.0   # source disagreement
    conflict_cluster:     float = 0.0   # known contradiction hotspot
    domain_entropy:       float = 1.0   # reasoning breadth (1.0 = healthy/diverse)
    composite_stress:     float = 0.0   # weighted aggregate

    def debug_str(self) -> str:
        return (
            f"EpistemicStress[composite={self.composite_stress:.2f} "
            f"supersession={self.supersession_density:.2f} "
            f"decay={self.decay_pressure:.2f} "
            f"auth_conflict={self.authority_conflict:.2f} "
            f"conflict_cluster={self.conflict_cluster:.2f} "
            f"entropy={self.domain_entropy:.2f}]"
        )


# ── Signal computations ────────────────────────────────────────────────────────

def _compute_supersession_density(
    conn: sqlite3.Connection,
    key_terms: List[str],
    window_days: int = _SUPERSESSION_WINDOW_DAYS,
) -> float:
    """
    Fraction of recent conflicts (last N days) whose winner/loser objects
    contain any key term from the current message.

    Normalised by key term count to avoid inflation on long messages.
    """
    if not key_terms or conn is None:
        return 0.0

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM fact_conflicts WHERE detected_at >= ?",
            (cutoff,),
        )
        total_recent = cursor.fetchone()[0]
        if total_recent == 0:
            return 0.0

        # Count conflicts touching any key term
        hits = 0
        for term in key_terms[:8]:  # cap to avoid N+1 explosion
            cursor.execute(
                """
                SELECT COUNT(*) FROM fact_conflicts
                WHERE detected_at >= ?
                AND (LOWER(winner_obj) LIKE ? OR LOWER(loser_obj) LIKE ?)
                """,
                (cutoff, f'%{term}%', f'%{term}%'),
            )
            hits += cursor.fetchone()[0]

        # Normalise: hits per term, capped at 1.0
        density = min(1.0, hits / (len(key_terms) * max(1, total_recent / 10)))
        return float(density)
    except Exception:
        return 0.0


def _compute_decay_pressure(atoms: List[dict]) -> float:
    """
    Average decay ratio across retrieved atoms.

    decay_pressure = 1.0 - mean(confidence_effective / confidence)

    High → atoms have decayed significantly → stale assumptions.
    Returns 0.0 if no atoms or no confidence data.
    """
    if not atoms:
        return 0.0

    ratios = []
    for atom in atoms:
        base = float(atom.get('confidence') or 0.5)
        effective = atom.get('confidence_effective')
        if effective is not None and base > 0:
            ratios.append(float(effective) / base)
        else:
            ratios.append(1.0)  # no decay data → assume fresh

    if not ratios:
        return 0.0

    mean_ratio = sum(ratios) / len(ratios)
    return max(0.0, min(1.0, 1.0 - mean_ratio))


def _compute_authority_conflict(atoms: List[dict]) -> float:
    """
    Standard deviation of authority weights among retrieved atoms.

    High std dev → atoms from sources with very different authority levels
    → mixed-quality input → less reliable reasoning.

    Normalised: max possible std dev for [0,1] values is 0.5, so divide by 0.5.
    """
    if not atoms or not _HAS_AUTHORITY:
        return 0.0

    weights = [get_authority(a.get('source', '')) for a in atoms]
    if len(weights) < 2:
        return 0.0

    mean = sum(weights) / len(weights)
    variance = sum((w - mean) ** 2 for w in weights) / len(weights)
    std_dev = math.sqrt(variance)

    # Normalise by max possible std dev (0.5 for uniform [0,1] distribution)
    return min(1.0, std_dev / 0.5)


def _compute_conflict_cluster(
    conn: sqlite3.Connection,
    key_terms: List[str],
    window_days: int = _SUPERSESSION_WINDOW_DAYS,
) -> float:
    """
    Whether the current topic is a known conflict hotspot.

    Counts conflicts in the last N days where winner_obj or loser_obj
    contains any key term, normalised by key term count.
    """
    if not key_terms or conn is None:
        return 0.0

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
        cursor = conn.cursor()

        total_hits = 0
        for term in key_terms[:8]:
            cursor.execute(
                """
                SELECT COUNT(*) FROM fact_conflicts
                WHERE detected_at >= ?
                AND (LOWER(winner_obj) LIKE ? OR LOWER(loser_obj) LIKE ?)
                """,
                (cutoff, f'%{term}%', f'%{term}%'),
            )
            total_hits += cursor.fetchone()[0]

        # Normalise: 1 hit per term = 0.5, 2+ hits per term = 1.0
        density = min(1.0, total_hits / (len(key_terms) * 2))
        return float(density)
    except Exception:
        return 0.0


def _compute_domain_entropy(atoms: List[dict]) -> float:
    """
    Shannon entropy of source prefixes among retrieved atoms.
    Normalised to [0, 1] by dividing by log2(N).

    HIGH entropy (1.0) = diverse sources = healthy broad grounding.
    LOW entropy (0.0) = all atoms from one source = narrow reasoning bias.

    Note: LOW is the stress condition. The composite uses (1 - entropy).
    """
    if not atoms:
        return 1.0  # no atoms → assume healthy (no stress from this signal)

    # Bucket by source prefix (first segment before '_')
    prefix_counts: dict[str, int] = {}
    for atom in atoms:
        src = atom.get('source', '') or ''
        # Use first 2 segments as prefix bucket (e.g. 'github_lltm' → 'github')
        prefix = src.split('_')[0] if '_' in src else src or 'unknown'
        prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1

    n = len(atoms)
    if n == 0:
        return 1.0

    # Shannon entropy
    entropy = 0.0
    for count in prefix_counts.values():
        p = count / n
        if p > 0:
            entropy -= p * math.log2(p)

    # Normalise by log2(number of unique prefixes) — max possible entropy
    max_entropy = math.log2(len(prefix_counts)) if len(prefix_counts) > 1 else 1.0
    normalised = entropy / max_entropy if max_entropy > 0 else 1.0

    return max(0.0, min(1.0, normalised))


# ── Structural entry point (executor-scoped) ───────────────────────────────────

def compute_structural_stress(
    topic: str,
    db_conn: sqlite3.Connection,
) -> 'EpistemicStressReport':
    """
    Compute epistemic stress over the full active atom population for a topic.

    Structurally-scoped: operates on ALL active atoms (confidence_effective > 0)
    for the topic, not a retrieval slice. No message key terms, no pipeline context.

    Used by the repair executor for pre/post signal snapshots so that calibration
    data is consistent across CLI, admin, and batch execution contexts.

    Pipeline uses: compute_stress(retrieved_atoms, key_terms, db_conn)
    Executor uses: compute_structural_stress(topic, db_conn)

    Same signal philosophy. Different scope. No coupling.
    """
    atoms: List[dict] = []
    try:
        cursor = db_conn.cursor()
        cursor.execute(
            """
            SELECT id, subject, predicate, object, source,
                   confidence, confidence_effective
            FROM facts
            WHERE (subject LIKE ? OR object LIKE ?)
              AND (confidence_effective IS NULL OR confidence_effective > 0)
            ORDER BY id ASC
            """,
            (f'%{topic}%', f'%{topic}%'),
        )
        cols = ['id', 'subject', 'predicate', 'object', 'source',
                'confidence', 'confidence_effective']
        atoms = [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception:
        pass

    # Use topic words as key_terms for conflict/supersession signals
    key_terms = [w for w in topic.lower().split() if len(w) > 3][:8]

    report = EpistemicStressReport()
    report.supersession_density = _compute_supersession_density(db_conn, key_terms)
    report.decay_pressure       = _compute_decay_pressure(atoms)
    report.authority_conflict   = _compute_authority_conflict(atoms)
    report.conflict_cluster     = _compute_conflict_cluster(db_conn, key_terms)
    report.domain_entropy       = _compute_domain_entropy(atoms)

    w = _COMPOSITE_WEIGHTS
    report.composite_stress = (
        w['supersession'] * report.supersession_density
        + w['decay']      * report.decay_pressure
        + w['authority']  * report.authority_conflict
        + w['conflict']   * report.conflict_cluster
        + w['entropy']    * (1.0 - report.domain_entropy)
    )
    report.composite_stress = max(0.0, min(1.0, report.composite_stress))
    return report


# ── Main entry point ───────────────────────────────────────────────────────────

def compute_stress(
    retrieved_atoms: List[dict],
    message_key_terms: List[str],
    db_conn: Optional[sqlite3.Connection] = None,
) -> EpistemicStressReport:
    """
    Compute all five epistemic stress signals and return an EpistemicStressReport.

    Args:
        retrieved_atoms:    list of fact dicts from _retrieve_knowledge()
                            (must have 'confidence', 'source', optionally 'confidence_effective')
        message_key_terms:  key terms extracted from the user message
        db_conn:            sqlite3 connection to jarvis_knowledge.db
                            (needed for supersession_density and conflict_cluster)
    """
    report = EpistemicStressReport()

    report.supersession_density = _compute_supersession_density(
        db_conn, message_key_terms
    )
    report.decay_pressure = _compute_decay_pressure(retrieved_atoms)
    report.authority_conflict = _compute_authority_conflict(retrieved_atoms)
    report.conflict_cluster = _compute_conflict_cluster(
        db_conn, message_key_terms
    )
    report.domain_entropy = _compute_domain_entropy(retrieved_atoms)

    # Composite: weighted sum. Low entropy contributes as (1 - entropy).
    w = _COMPOSITE_WEIGHTS
    report.composite_stress = (
        w['supersession'] * report.supersession_density
        + w['decay']      * report.decay_pressure
        + w['authority']  * report.authority_conflict
        + w['conflict']   * report.conflict_cluster
        + w['entropy']    * (1.0 - report.domain_entropy)
    )
    report.composite_stress = max(0.0, min(1.0, report.composite_stress))

    return report

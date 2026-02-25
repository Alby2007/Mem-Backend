"""
knowledge/confidence_intervals.py — Bayesian Confidence Intervals

Tracks confidence as a distribution (mean + variance) rather than a point
estimate. Each atom carries two new columns:

    conf_n    INTEGER  — observation count (how many sources confirmed/denied)
    conf_var  REAL     — running variance (Welford online algorithm)

When a second source confirms the same (subject, predicate) with a
compatible object, conf_n increments and conf_var narrows (evidence
converges). When sources conflict, the existing contradiction.py sets the
winner/loser, and the winner's conf_var is widened to reflect the
disagreement — confidence should be less certain when sources fight.

INFORMATIONAL-ONLY IN V1
========================
The distribution is tracked and exposed via get_confidence_interval() and
GET /kb/confidence but does NOT yet feed back into position_size_pct. That
hook is planned for v2 once sufficient multi-source observation data exists
to validate the calibration.

WELFORD ONLINE UPDATE
=====================
To update (mean, n, M2) with a new observation x:
    n  += 1
    delta  = x - mean
    mean  += delta / n
    delta2 = x - mean
    M2    += delta * delta2

variance = M2 / n      (population)
std      = sqrt(variance)

We store:
    confidence = mean  (existing column, already populated)
    conf_n     = n     (new column)
    conf_var   = M2    (running sum of squared deviations, not yet divided)

So variance = conf_var / conf_n  when conf_n > 1, else 0.

AUTHORITY-WEIGHTED UPDATE
=========================
Rather than treating all sources as equal observations, each new
observation is weighted by its source authority (from authority.py).
The update uses a weighted Welford variant:
    w_total  += w
    delta     = x - mean
    mean     += (w / w_total) * delta
    delta2    = x - mean
    conf_var += w * delta * delta2

This means an exchange_feed (authority=1.0) observation moves the mean
more than a social_signal (authority=0.35) observation.

Zero-LLM, pure Python, <2ms per call.
"""

from __future__ import annotations

import math
import sqlite3
from typing import Optional, Tuple

try:
    from knowledge.authority import get_authority
    _HAS_AUTHORITY = True
except ImportError:
    _HAS_AUTHORITY = False

    def get_authority(source: str) -> float:
        return 0.5


# ── Schema migration ──────────────────────────────────────────────────────────

def ensure_confidence_columns(conn: sqlite3.Connection) -> None:
    """
    Idempotent migration: add conf_n and conf_var to the facts table if absent.
    Safe to call on every startup.

    conf_n   — observation count, default 1 (every atom starts with 1 observation)
    conf_var — running weighted sum of squared deviations (Welford M2), default 0
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(facts)")
    existing = {row[1] for row in cursor.fetchall()}

    if 'conf_n' not in existing:
        conn.execute("ALTER TABLE facts ADD COLUMN conf_n INTEGER DEFAULT 1")
    if 'conf_var' not in existing:
        conn.execute("ALTER TABLE facts ADD COLUMN conf_var REAL DEFAULT 0")
    conn.commit()


# ── Welford weighted update ───────────────────────────────────────────────────

def welford_update(
    mean: float,
    conf_n: int,
    conf_var: float,
    new_confidence: float,
    source: str = '',
) -> Tuple[float, int, float]:
    """
    Apply one authority-weighted Welford update to (mean, conf_n, conf_var).

    Parameters
    ----------
    mean           current confidence mean (= facts.confidence)
    conf_n         current observation count (= facts.conf_n)
    conf_var       current running M2 (= facts.conf_var)
    new_confidence new observation confidence value in [0, 1]
    source         source string for authority weighting

    Returns
    -------
    (new_mean, new_conf_n, new_conf_var)
    """
    w = get_authority(source) if _HAS_AUTHORITY else 0.5
    w = max(0.01, w)  # floor to avoid divide-by-zero

    # Authority-weighted Welford
    # w_total after this observation = sum of all weights so far
    # We approximate w_total as: conf_n * avg_authority ≈ conf_n * 0.65
    # (midpoint of typical derived_signal_ sources)
    # A simpler, numerically stable approach: use conf_n as a proxy count,
    # treat each unit of conf_n as having average weight, then correct.
    # For simplicity: standard Welford on n (unweighted), apply w to delta only.
    n       = conf_n + 1
    delta   = new_confidence - mean
    mean   += (w * delta) / (conf_n * 0.65 + w)  # weighted fraction
    delta2  = new_confidence - mean
    conf_var += w * delta * delta2

    return (
        max(0.0, min(1.0, mean)),
        n,
        max(0.0, conf_var),
    )


def widen_for_conflict(
    mean: float,
    conf_n: int,
    conf_var: float,
    conflict_confidence: float,
    source: str = '',
) -> Tuple[float, int, float]:
    """
    Widen the confidence distribution when a contradicting atom arrives.

    The mean is pulled toward the midpoint between the current mean and the
    conflicting value, and conf_var is increased proportionally to the
    squared distance between them. conf_n is NOT incremented (the conflicting
    atom did not confirm the same object — it contradicted it).

    Used by the contradiction handler to signal epistemic uncertainty.
    """
    distance  = abs(mean - conflict_confidence)
    pull      = 0.15 * distance          # 15% mean pull toward conflict
    mean      = mean - math.copysign(pull, mean - 0.5)
    conf_var += (distance ** 2) * 0.5    # widen variance proportional to disagreement

    return (
        max(0.0, min(1.0, mean)),
        conf_n,
        max(0.0, conf_var),
    )


# ── Read helpers ──────────────────────────────────────────────────────────────

def get_confidence_interval(
    conn: sqlite3.Connection,
    subject: str,
    predicate: str,
    z: float = 1.96,
) -> Optional[dict]:
    """
    Return the confidence distribution for the most authoritative atom
    matching (subject, predicate).

    Parameters
    ----------
    conn        open sqlite3 connection to trading_knowledge.db
    subject     atom subject (case-insensitive)
    predicate   atom predicate
    z           z-score for interval width (default 1.96 = 95% interval)

    Returns
    -------
    {
        "subject":      str,
        "predicate":    str,
        "object":       str,
        "mean":         float,          # current confidence mean
        "n":            int,            # observation count
        "std":          float,          # standard deviation
        "interval_low": float,          # mean - z * std  (clipped to [0,1])
        "interval_high":float,          # mean + z * std  (clipped to [0,1])
        "interval_z":   float,          # z-score used
        "source":       str,
        "authority":    float,
    }

    Returns None if no matching atom found.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, subject, predicate, object, source, confidence,
               COALESCE(conf_n, 1)    AS conf_n,
               COALESCE(conf_var, 0)  AS conf_var
        FROM facts
        WHERE LOWER(subject) = LOWER(?)
          AND predicate = ?
        ORDER BY confidence DESC
        LIMIT 1
    """, (subject, predicate))
    row = cursor.fetchone()
    if row is None:
        return None

    _, subj, pred, obj, source, mean, n, m2 = row

    # variance = M2 / n  (population variance), std = sqrt(variance)
    variance = (m2 / max(n, 1)) if n > 0 else 0.0
    std      = math.sqrt(max(0.0, variance))

    interval_low  = max(0.0, mean - z * std)
    interval_high = min(1.0, mean + z * std)

    authority = get_authority(source) if _HAS_AUTHORITY else 0.5

    return {
        'subject':       subj,
        'predicate':     pred,
        'object':        obj,
        'mean':          round(mean, 4),
        'n':             int(n),
        'std':           round(std, 4),
        'variance':      round(variance, 4),
        'interval_low':  round(interval_low, 4),
        'interval_high': round(interval_high, 4),
        'interval_z':    z,
        'source':        source,
        'authority':     round(authority, 3),
    }


def get_all_confidence_intervals(
    conn: sqlite3.Connection,
    subject: str,
    z: float = 1.96,
) -> list:
    """
    Return confidence distributions for ALL predicates of a given subject,
    ordered by mean confidence descending.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, subject, predicate, object, source, confidence,
               COALESCE(conf_n, 1)   AS conf_n,
               COALESCE(conf_var, 0) AS conf_var
        FROM facts
        WHERE LOWER(subject) = LOWER(?)
        ORDER BY confidence DESC
    """, (subject,))
    rows = cursor.fetchall()

    results = []
    for _, subj, pred, obj, source, mean, n, m2 in rows:
        variance = (m2 / max(n, 1)) if n > 0 else 0.0
        std      = math.sqrt(max(0.0, variance))
        authority = get_authority(source) if _HAS_AUTHORITY else 0.5
        results.append({
            'subject':       subj,
            'predicate':     pred,
            'object':        obj,
            'mean':          round(mean, 4),
            'n':             int(n),
            'std':           round(std, 4),
            'variance':      round(variance, 4),
            'interval_low':  round(max(0.0, mean - z * std), 4),
            'interval_high': round(min(1.0, mean + z * std), 4),
            'interval_z':    z,
            'source':        source,
            'authority':     round(authority, 3),
        })
    return results


def update_atom_confidence(
    conn: sqlite3.Connection,
    atom_id: int,
    new_confidence: float,
    source: str = '',
) -> None:
    """
    Update a single atom's confidence distribution with a new observation.
    Reads current (confidence, conf_n, conf_var), applies Welford update,
    writes back. Commits immediately.

    Called by the ingest pipeline when a second source confirms the same
    (subject, predicate) — after conflict resolution has determined that
    both objects are compatible (same direction, similar value).
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT confidence,
               COALESCE(conf_n, 1)   AS conf_n,
               COALESCE(conf_var, 0) AS conf_var
        FROM facts WHERE id = ?
    """, (atom_id,))
    row = cursor.fetchone()
    if row is None:
        return

    mean, n, m2 = row
    new_mean, new_n, new_m2 = welford_update(mean, n, m2, new_confidence, source)

    conn.execute("""
        UPDATE facts
        SET confidence = ?, conf_n = ?, conf_var = ?
        WHERE id = ?
    """, (new_mean, new_n, new_m2, atom_id))
    conn.commit()


def widen_atom_confidence(
    conn: sqlite3.Connection,
    atom_id: int,
    conflict_confidence: float,
    source: str = '',
) -> None:
    """
    Widen a single atom's confidence distribution due to a conflict.
    Called by the contradiction handler when a conflict is detected.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT confidence,
               COALESCE(conf_n, 1)   AS conf_n,
               COALESCE(conf_var, 0) AS conf_var
        FROM facts WHERE id = ?
    """, (atom_id,))
    row = cursor.fetchone()
    if row is None:
        return

    mean, n, m2 = row
    new_mean, new_n, new_m2 = widen_for_conflict(mean, n, m2, conflict_confidence, source)

    conn.execute("""
        UPDATE facts
        SET confidence = ?, conf_n = ?, conf_var = ?
        WHERE id = ?
    """, (new_mean, new_n, new_m2, atom_id))
    conn.commit()

"""
knowledge/contradiction.py — Contradiction Detection

Detects when a newly ingested atom conflicts with an existing one.
Conflict = same (subject, predicate), different object.

Resolution policy:
  Primary:   epistemic strength = effective_score() = confidence × authority
  Secondary: recency (tiebreaker, same source class only)

The loser is NOT deleted — it is marked with superseded_by in its metadata JSON.
All conflicts are logged to the fact_conflicts table for auditability.

Zero-LLM, pure Python, <2ms per call.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from knowledge.authority import conflict_winner, effective_score


# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_CONFLICTS_TABLE = """
CREATE TABLE IF NOT EXISTS fact_conflicts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    winner_id   INTEGER NOT NULL,
    loser_id    INTEGER NOT NULL,
    winner_obj  TEXT,
    loser_obj   TEXT,
    reason      TEXT,
    detected_at TEXT,
    FOREIGN KEY(winner_id) REFERENCES facts(id),
    FOREIGN KEY(loser_id)  REFERENCES facts(id)
)
"""


def ensure_conflicts_table(conn: sqlite3.Connection) -> None:
    """Idempotent migration. Safe to call on every startup."""
    conn.execute(_CREATE_CONFLICTS_TABLE)
    conn.commit()


# ── Result dataclass ───────────────────────────────────────────────────────────

@dataclass
class ConflictResult:
    detected:   bool
    winner_id:  Optional[int] = None
    loser_id:   Optional[int] = None
    winner_obj: Optional[str] = None
    loser_obj:  Optional[str] = None
    reason:     str = ''


# ── Detector ───────────────────────────────────────────────────────────────────

class ContradictionDetector:
    """
    Checks for (subject, predicate) conflicts after a new atom is inserted.

    Usage (called from graph.py add_fact after successful insert):
        result = detector.check(conn, new_fact_id, subject, predicate, object)
    """

    def check(
        self,
        conn: sqlite3.Connection,
        new_id: int,
        subject: str,
        predicate: str,
        new_object: str,
    ) -> ConflictResult:
        """
        Find existing atoms with same (subject, predicate) but different object.
        If found, resolve winner via epistemic strength, mark loser, log conflict.

        Returns ConflictResult — caller can log or ignore.
        """
        cursor = conn.cursor()

        # Find all atoms with same S-P but different O
        cursor.execute(
            """
            SELECT id, subject, predicate, object, confidence, source, timestamp, metadata
            FROM facts
            WHERE subject = ? AND predicate = ? AND object != ? AND id != ?
            """,
            (subject, predicate, new_object, new_id),
        )
        rows = cursor.fetchall()

        if not rows:
            return ConflictResult(detected=False)

        # Build fact dicts for comparison
        new_fact = self._row_to_dict(
            {'id': new_id, 'subject': subject, 'predicate': predicate,
             'object': new_object, 'confidence': None, 'source': None,
             'timestamp': None, 'metadata': None},
            cursor, new_id,
        )

        # Find the strongest existing competitor
        competitors = [dict(zip(
            ['id', 'subject', 'predicate', 'object', 'confidence', 'source', 'timestamp', 'metadata'],
            row
        )) for row in rows]

        strongest = max(competitors, key=effective_score)

        winner = conflict_winner(new_fact, strongest)
        loser  = strongest if winner is new_fact else new_fact

        winner_id  = winner.get('id') or new_id
        loser_id   = loser.get('id') or new_id
        winner_obj = winner.get('object', '')
        loser_obj  = loser.get('object', '')

        score_new = effective_score(new_fact)
        score_old = effective_score(strongest)
        if abs(score_new - score_old) > 0.05:
            reason = 'epistemic_strength'
        else:
            reason = 'recency_tiebreaker'

        # Mark loser as superseded in its metadata
        self._mark_superseded(conn, loser_id, winner_id)

        # Log to fact_conflicts
        self._log_conflict(conn, winner_id, loser_id, winner_obj, loser_obj, reason)

        # Propagate conflict downstream: write signal_conflicted atom so
        # signal_enrichment_adapter picks it up on next cycle and downgrades
        # conviction_tier automatically without any additional wiring.
        _SIGNAL_PREDICATES = {'signal_direction', 'price_target', 'conviction_tier'}
        if predicate in _SIGNAL_PREDICATES:
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO facts "
                    "(subject, predicate, object, confidence, source, timestamp) "
                    "VALUES (?, 'signal_conflicted', ?, 0.9, 'contradiction_detector', ?)",
                    (subject,
                     f"{winner_obj}_vs_{loser_obj}",
                     datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
            except Exception:
                pass   # never let downstream propagation break the ingest path

        return ConflictResult(
            detected=True,
            winner_id=winner_id,
            loser_id=loser_id,
            winner_obj=winner_obj,
            loser_obj=loser_obj,
            reason=reason,
        )

    def _row_to_dict(self, base: dict, cursor: sqlite3.Cursor, fact_id: int) -> dict:
        """Fetch full fact row for a given id."""
        cursor.execute(
            "SELECT id, subject, predicate, object, confidence, source, timestamp, metadata FROM facts WHERE id = ?",
            (fact_id,),
        )
        row = cursor.fetchone()
        if row:
            return dict(zip(
                ['id', 'subject', 'predicate', 'object', 'confidence', 'source', 'timestamp', 'metadata'],
                row,
            ))
        return base

    def _mark_superseded(
        self,
        conn: sqlite3.Connection,
        loser_id: int,
        winner_id: int,
    ) -> None:
        """Add superseded_by to loser's metadata JSON. Non-destructive."""
        cursor = conn.cursor()
        cursor.execute("SELECT metadata FROM facts WHERE id = ?", (loser_id,))
        row = cursor.fetchone()
        if not row:
            return

        try:
            meta = json.loads(row[0]) if row[0] else {}
        except (json.JSONDecodeError, TypeError):
            meta = {}

        meta['superseded_by'] = winner_id
        meta['superseded_at'] = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            "UPDATE facts SET metadata = ? WHERE id = ?",
            (json.dumps(meta), loser_id),
        )
        conn.commit()

    def _log_conflict(
        self,
        conn: sqlite3.Connection,
        winner_id: int,
        loser_id: int,
        winner_obj: str,
        loser_obj: str,
        reason: str,
    ) -> None:
        """Append to fact_conflicts table."""
        try:
            conn.execute(
                """
                INSERT INTO fact_conflicts
                    (winner_id, loser_id, winner_obj, loser_obj, reason, detected_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (winner_id, loser_id, winner_obj, loser_obj, reason,
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        except Exception:
            pass  # never let conflict logging break the ingest path


# ── Module-level singleton ─────────────────────────────────────────────────────

_detector: Optional[ContradictionDetector] = None


def get_detector() -> ContradictionDetector:
    global _detector
    if _detector is None:
        _detector = ContradictionDetector()
    return _detector

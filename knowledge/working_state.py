"""
knowledge/working_state.py — Cross-Session Working State Persistence

Persists the user's current working context (goal, phase, topic, open threads)
to a SQLite table every _PERSIST_INTERVAL turns. On the first turn of a new
session, the last saved state is injected as context so JARVIS remembers
where the user left off — even after a restart.

Design decisions:
  - Persists every 5 turns (not every response — avoids I/O churn)
  - Survives crashes (mid-session state is not lost)
  - Stored in jarvis_knowledge.db alongside atoms (no extra DB file)
  - Ephemeral session objects (WorkingGoal, ActiveTopic) remain the primary
    continuity mechanism; this module just makes them persistent

Zero-LLM, pure Python, <2ms per call.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

_logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────────────────────

_PERSIST_INTERVAL = 5   # persist every N turns
# Crash loss window: up to (_PERSIST_INTERVAL - 1) = 4 turns of state can be lost.
# Mitigated by force=True on turn 1 of every session (immediate anchor write).
_MAX_STORED_STATES = 10  # keep last N states per session (older pruned)
_RECENT_STATES_LIMIT = 3  # how many prior states to surface on session start


# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_WORKING_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS working_state (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id   TEXT NOT NULL,
    goal         TEXT,
    phase        TEXT,
    topic        TEXT,
    open_threads TEXT,   -- JSON array of strings
    last_intent  TEXT,
    turn_count   INTEGER DEFAULT 0,
    saved_at     TEXT NOT NULL
)
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_working_state_session
ON working_state(session_id, saved_at DESC)
"""


def ensure_working_state_table(conn: sqlite3.Connection) -> None:
    """Idempotent migration. Safe to call on every startup."""
    conn.execute(_CREATE_WORKING_STATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.commit()


# ── Dataclass ──────────────────────────────────────────────────────────────────

@dataclass
class WorkingStateSnapshot:
    session_id:   str
    goal:         Optional[str]
    phase:        Optional[str]
    topic:        Optional[str]
    open_threads: List[str]
    last_intent:  Optional[str]
    turn_count:   int
    saved_at:     str

    def format_for_context(self) -> str:
        """
        Compact string for injection into the system prompt on session start.
        Tells JARVIS where the user left off without overwhelming the context.
        """
        parts = []
        if self.goal:
            parts.append(f"Goal: {self.goal}")
        if self.topic:
            parts.append(f"Topic: {self.topic}")
        if self.phase:
            parts.append(f"Phase: {self.phase}")
        if self.open_threads:
            threads = ', '.join(self.open_threads[:3])
            parts.append(f"Open threads: {threads}")
        if self.saved_at:
            # Human-readable age
            try:
                ts = datetime.fromisoformat(self.saved_at)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_sec = (datetime.now(timezone.utc) - ts).total_seconds()
                if age_sec < 3600:
                    age = f"{int(age_sec / 60)}m ago"
                elif age_sec < 86400:
                    age = f"{int(age_sec / 3600)}h ago"
                else:
                    age = f"{int(age_sec / 86400)}d ago"
                parts.append(f"Last active: {age}")
            except Exception:
                pass

        if not parts:
            return ''

        return '=== PRIOR SESSION STATE ===\n' + '\n'.join(parts)


# ── Store ──────────────────────────────────────────────────────────────────────

class WorkingStateStore:
    """
    Persists and retrieves working state snapshots from jarvis_knowledge.db.

    Typical usage:
        store = WorkingStateStore(db_path)

        # Every 5 turns — called from stream_response()
        store.maybe_persist(session_id, turn_count, goal=..., phase=..., topic=..., ...)

        # First turn of new session — called from assemble_context()
        snippet = store.format_prior_context(session_id)
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            ensure_working_state_table(self._conn)
        return self._conn

    def maybe_persist(
        self,
        session_id: str,
        turn_count: int,
        *,
        goal: Optional[str] = None,
        phase: Optional[str] = None,
        topic: Optional[str] = None,
        open_threads: Optional[List[str]] = None,
        last_intent: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        """
        Persist working state if turn_count is a multiple of _PERSIST_INTERVAL,
        OR if force=True (used for turn 1 of every session as a crash-safe anchor).

        Crash loss window without force: up to 4 turns.
        With force=True on turn 1: worst case is 4 turns lost mid-session,
        but the session's initial intent is always captured.

        Returns True if a write occurred.
        """
        if not force and (turn_count == 0 or turn_count % _PERSIST_INTERVAL != 0):
            return False

        return self._write(
            session_id=session_id,
            goal=goal,
            phase=phase,
            topic=topic,
            open_threads=open_threads or [],
            last_intent=last_intent,
            turn_count=turn_count,
        )

    def _write(
        self,
        session_id: str,
        goal: Optional[str],
        phase: Optional[str],
        topic: Optional[str],
        open_threads: List[str],
        last_intent: Optional[str],
        turn_count: int,
    ) -> bool:
        try:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT INTO working_state
                    (session_id, goal, phase, topic, open_threads, last_intent, turn_count, saved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    goal,
                    phase,
                    topic,
                    json.dumps(open_threads),
                    last_intent,
                    turn_count,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
            self._prune(conn, session_id)
            return True
        except Exception as e:
            _logger.error('[WorkingState] persist error: %s', e)
            return False

    def _prune(self, conn: sqlite3.Connection, session_id: str) -> None:
        """Keep only the last _MAX_STORED_STATES rows per session."""
        conn.execute(
            """
            DELETE FROM working_state
            WHERE session_id = ? AND id NOT IN (
                SELECT id FROM working_state
                WHERE session_id = ?
                ORDER BY saved_at DESC
                LIMIT ?
            )
            """,
            (session_id, session_id, _MAX_STORED_STATES),
        )
        conn.commit()

    def get_recent(
        self,
        session_id: str,
        limit: int = _RECENT_STATES_LIMIT,
        exclude_session: bool = True,
    ) -> List[WorkingStateSnapshot]:
        """
        Return the most recent working state snapshots.

        Args:
            session_id:      current session — used to optionally exclude same-session rows
            limit:           max snapshots to return
            exclude_session: if True, only return states from *other* sessions
                             (useful for cross-session continuity on first turn)
        """
        try:
            conn = self._get_conn()
            if exclude_session:
                cursor = conn.execute(
                    """
                    SELECT session_id, goal, phase, topic, open_threads, last_intent, turn_count, saved_at
                    FROM working_state
                    WHERE session_id != ?
                    ORDER BY saved_at DESC
                    LIMIT ?
                    """,
                    (session_id, limit),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT session_id, goal, phase, topic, open_threads, last_intent, turn_count, saved_at
                    FROM working_state
                    ORDER BY saved_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                )

            rows = cursor.fetchall()
            results = []
            for row in rows:
                try:
                    threads = json.loads(row['open_threads']) if row['open_threads'] else []
                except (json.JSONDecodeError, TypeError):
                    threads = []
                results.append(WorkingStateSnapshot(
                    session_id=row['session_id'],
                    goal=row['goal'],
                    phase=row['phase'],
                    topic=row['topic'],
                    open_threads=threads,
                    last_intent=row['last_intent'],
                    turn_count=row['turn_count'],
                    saved_at=row['saved_at'],
                ))
            return results
        except Exception as e:
            _logger.error('[WorkingState] get_recent error: %s', e)
            return []

    def format_prior_context(self, session_id: str) -> str:
        """
        Return a formatted string for the most recent prior session state.
        Returns empty string if no prior state exists.
        Called on the first turn of a new session.
        """
        states = self.get_recent(session_id, limit=1, exclude_session=True)
        if not states:
            return ''
        return states[0].format_for_context()


# ── Module-level singleton ─────────────────────────────────────────────────────

_store: Optional[WorkingStateStore] = None


def get_working_state_store(db_path: str) -> WorkingStateStore:
    """Return (and lazily create) the module-level WorkingStateStore singleton."""
    global _store
    if _store is None:
        _store = WorkingStateStore(db_path)
    return _store

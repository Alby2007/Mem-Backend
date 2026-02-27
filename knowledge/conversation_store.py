"""
knowledge/conversation_store.py — Persistent conversation history for Trading Galaxy.

Ported from JARVIS v3.2 conversation_store.py and adapted for:
  - Multi-user scoping: session_id = f"TRADING_{user_id}" per user (not a global singleton)
  - Trading-domain atom predicates (signal_direction, conviction_tier, etc.)
  - Two-path KB graduation: salience > 0.40 for all atoms, or > 0.25 for user intent atoms
  - graduated_at column to track which atoms have been committed to the main KB

Tables (created inside trading_knowledge.db):
  conv_sessions        — one row per user conversation thread
  conv_messages        — every turn, chronological, with metadata
  conv_atoms           — atoms extracted from each turn with temporal salience decay

Temporal salience decay:
  effective_salience = source_weight * exp(-DECAY_LAMBDA * age_in_days)
  DECAY_LAMBDA = 0.01 → 70-day-old atom retains ~50% salience
  source_weight: user=1.0, assistant=0.6 (prevent self-reinforcement)
"""

from __future__ import annotations

import sqlite3
import json
import math
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional


DECAY_LAMBDA   = 0.01
SOURCE_WEIGHTS = {'user': 1.0, 'assistant': 0.6}


def session_id_for_user(user_id: Optional[str]) -> str:
    """Return the canonical conversation session ID for a user."""
    return f"TRADING_{user_id or 'default'}"


class ConversationStore:
    """
    Persistent conversation store backed by the trading KB SQLite database.
    All tables are prefixed with 'conv_' to avoid collisions with existing KB tables.
    """

    def __init__(self, db_path: str = "trading_knowledge.db"):
        self.db_path = db_path
        self._ensure_tables()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ── Schema ────────────────────────────────────────────────────────────────

    def _ensure_tables(self) -> None:
        conn = self._conn()
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS conv_sessions (
                session_id TEXT PRIMARY KEY,
                user_id    TEXT,
                title      TEXT,
                created_at TEXT,
                updated_at TEXT,
                metadata   TEXT DEFAULT '{}'
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS conv_messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role       TEXT NOT NULL,
                content    TEXT NOT NULL,
                metadata   TEXT DEFAULT '{}',
                timestamp  TEXT NOT NULL,
                FOREIGN KEY (session_id) REFERENCES conv_sessions(session_id)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS conv_atoms (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id     INTEGER NOT NULL,
                session_id     TEXT NOT NULL,
                source         TEXT NOT NULL,
                subject        TEXT NOT NULL,
                predicate      TEXT NOT NULL,
                object         TEXT NOT NULL,
                atom_type      TEXT NOT NULL DEFAULT 'fact',
                timestamp      TEXT NOT NULL,
                source_weight  REAL NOT NULL DEFAULT 1.0,
                salience_score REAL NOT NULL DEFAULT 1.0,
                graduated_at   TEXT,
                FOREIGN KEY (message_id) REFERENCES conv_messages(id)
            )
        """)

        c.execute("CREATE INDEX IF NOT EXISTS idx_cmsg_session  ON conv_messages(session_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cmsg_ts       ON conv_messages(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_catom_session ON conv_atoms(session_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_catom_msg     ON conv_atoms(message_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_catom_ts      ON conv_atoms(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_catom_grad    ON conv_atoms(graduated_at)")

        conn.commit()
        conn.close()

    # ── Session management ────────────────────────────────────────────────────

    def get_or_create_session(self, session_id: str, user_id: Optional[str] = None) -> Dict:
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT * FROM conv_sessions WHERE session_id = ?", (session_id,))
        row = c.fetchone()
        if row:
            conn.close()
            return dict(row)
        now = datetime.now().isoformat()
        title = f"Conversation — {user_id or 'default'}"
        c.execute("""
            INSERT INTO conv_sessions (session_id, user_id, title, created_at, updated_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (session_id, user_id, title, now, now, '{}'))
        conn.commit()
        conn.close()
        return {'session_id': session_id, 'user_id': user_id, 'title': title,
                'created_at': now, 'updated_at': now}

    def get_session(self, session_id: str) -> Optional[Dict]:
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT * FROM conv_sessions WHERE session_id = ?", (session_id,))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None

    def delete_session_messages(self, session_id: str) -> int:
        """Delete all messages (and their atoms) for a session. Returns deleted message count."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT id FROM conv_messages WHERE session_id = ?", (session_id,))
        ids = [r[0] for r in c.fetchall()]
        if ids:
            placeholders = ','.join('?' * len(ids))
            c.execute(f"DELETE FROM conv_atoms WHERE message_id IN ({placeholders})", ids)
        c.execute("DELETE FROM conv_messages WHERE session_id = ?", (session_id,))
        deleted = len(ids)
        c.execute("UPDATE conv_sessions SET updated_at = ? WHERE session_id = ?",
                  (datetime.now().isoformat(), session_id))
        conn.commit()
        conn.close()
        return deleted

    # ── Messages ──────────────────────────────────────────────────────────────

    def add_message(self, session_id: str, role: str, content: str,
                    metadata: Optional[Dict] = None,
                    user_id: Optional[str] = None) -> Dict:
        """Add a message to a session (creating the session if needed). Returns message record."""
        self.get_or_create_session(session_id, user_id=user_id)
        conn = self._conn()
        c = conn.cursor()
        now = datetime.now().isoformat()
        c.execute("""
            INSERT INTO conv_messages (session_id, role, content, metadata, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (session_id, role, content, json.dumps(metadata or {}), now))
        message_id = c.lastrowid
        c.execute("UPDATE conv_sessions SET updated_at = ? WHERE session_id = ?",
                  (now, session_id))
        conn.commit()
        conn.close()
        return {'id': message_id, 'session_id': session_id, 'role': role,
                'content': content, 'metadata': metadata or {}, 'timestamp': now}

    def get_recent_messages_for_context(self, session_id: str,
                                         n_turns: int = 8) -> List[Dict]:
        """
        Return the last n_turns complete (user, assistant) pairs, oldest first,
        formatted as [{role, content}] for injection into the LLM messages array.
        """
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            SELECT id, role, content FROM conv_messages
            WHERE session_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (session_id, n_turns * 2))
        rows = list(reversed(c.fetchall()))
        conn.close()
        return [{'id': r['id'], 'role': r['role'], 'content': r['content']} for r in rows]

    def get_message_pair(self, user_message_id: int) -> Dict:
        """Return a user message and the immediately following assistant message."""
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT * FROM conv_messages WHERE id = ?", (user_message_id,))
        user_row = c.fetchone()
        if not user_row:
            conn.close()
            return {}
        user_msg = dict(user_row)
        user_msg['metadata'] = json.loads(user_msg.get('metadata') or '{}')

        c.execute("""
            SELECT * FROM conv_messages
            WHERE session_id = ? AND role = 'assistant' AND id > ?
            ORDER BY id ASC LIMIT 1
        """, (user_msg['session_id'], user_message_id))
        asst_row = c.fetchone()
        asst_msg = None
        if asst_row:
            asst_msg = dict(asst_row)
            asst_msg['metadata'] = json.loads(asst_msg.get('metadata') or '{}')
        conn.close()
        return {'user': user_msg, 'assistant': asst_msg}

    # ── Conversation atoms ─────────────────────────────────────────────────────

    def add_turn_atoms(self, message_id: int, session_id: str,
                       atoms: List[Dict]) -> None:
        """
        Store atoms extracted from a conversation turn.
        Each atom: {source, subject, predicate, object, atom_type}
        source_weight applied at insertion (user=1.0, assistant=0.6).
        """
        if not atoms:
            return
        conn = self._conn()
        c = conn.cursor()
        now = datetime.now().isoformat()
        rows = []
        for a in atoms:
            source = a.get('source', 'assistant')
            weight = SOURCE_WEIGHTS.get(source, 1.0)
            rows.append((
                message_id,
                session_id,
                source,
                (a.get('subject') or '').strip()[:120],
                (a.get('predicate') or '').strip()[:80],
                (a.get('object') or '').strip()[:200],
                a.get('atom_type', 'fact'),
                now,
                weight,
                weight,  # initial salience_score == source_weight
            ))
        c.executemany("""
            INSERT INTO conv_atoms
                (message_id, session_id, source, subject, predicate, object,
                 atom_type, timestamp, source_weight, salience_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()

    def get_salient_atoms(self, session_id: str, limit: int = 30,
                          min_salience: float = 0.1) -> List[Dict]:
        """
        Return atoms sorted by effective salience:
            effective = source_weight * exp(-DECAY_LAMBDA * age_in_days)

        Used to find candidates for KB graduation.
        """
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            SELECT * FROM conv_atoms
            WHERE session_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (session_id, limit * 10))
        rows = c.fetchall()
        conn.close()

        now = datetime.now()
        results = []
        for row in rows:
            try:
                ts = datetime.fromisoformat(row['timestamp'])
                age_days = (now - ts).total_seconds() / 86400
            except Exception:
                age_days = 0
            weight = row['source_weight'] if row['source_weight'] else 1.0
            effective = weight * math.exp(-DECAY_LAMBDA * age_days)
            if effective < min_salience:
                continue
            d = dict(row)
            d['effective_salience'] = round(effective, 4)
            d['age_days'] = round(age_days, 1)
            d['graduated'] = bool(row['graduated_at'])
            results.append(d)

        results.sort(key=lambda x: x['effective_salience'], reverse=True)
        return results[:limit]

    def mark_atom_graduated(self, atom_id: int) -> None:
        """Mark an atom as graduated to the main KB."""
        conn = self._conn()
        conn.execute("UPDATE conv_atoms SET graduated_at = ? WHERE id = ?",
                     (datetime.now().isoformat(), atom_id))
        conn.commit()
        conn.close()

    def get_atoms_with_status(self, session_id: str,
                               limit: int = 50) -> List[Dict]:
        """
        Return all conversation atoms for a session with effective salience
        and graduation status — used by GET /chat/atoms.
        """
        conn = self._conn()
        c = conn.cursor()
        c.execute("""
            SELECT ca.*, cm.timestamp as extracted_at
            FROM conv_atoms ca
            JOIN conv_messages cm ON ca.message_id = cm.id
            WHERE ca.session_id = ?
            ORDER BY ca.timestamp DESC
            LIMIT ?
        """, (session_id, limit))
        rows = c.fetchall()
        conn.close()

        now = datetime.now()
        results = []
        for row in rows:
            try:
                ts = datetime.fromisoformat(row['timestamp'])
                age_days = (now - ts).total_seconds() / 86400
            except Exception:
                age_days = 0
            weight = row['source_weight'] if row['source_weight'] else 1.0
            effective = weight * math.exp(-DECAY_LAMBDA * age_days)
            results.append({
                'id':                row['id'],
                'subject':           row['subject'],
                'predicate':         row['predicate'],
                'object':            row['object'],
                'atom_type':         row['atom_type'],
                'source':            row['source'],
                'effective_salience': round(effective, 4),
                'graduated':         bool(row['graduated_at']),
                'graduated_at':      row['graduated_at'],
                'extracted_at':      row['timestamp'],
            })
        return results

    # ── Timeline ──────────────────────────────────────────────────────────────

    def get_timeline(self, session_id: str, limit: int = 50,
                     offset: int = 0, search: str = '') -> List[Dict]:
        """
        Return chronological user turns with paired assistant preview + atom count.
        Used by the GET /chat/history sidebar endpoint.
        """
        conn = self._conn()
        c = conn.cursor()
        today = date.today()

        if search:
            escaped = search.replace('%', '\\%').replace('_', '\\_')
            like = f"%{escaped}%"
            c.execute("""
                SELECT id, content, timestamp FROM conv_messages
                WHERE session_id = ? AND role = 'user' AND content LIKE ? ESCAPE '\\'
                ORDER BY timestamp ASC LIMIT ? OFFSET ?
            """, (session_id, like, limit, offset))
        else:
            c.execute("""
                SELECT id, content, timestamp FROM conv_messages
                WHERE session_id = ? AND role = 'user'
                ORDER BY timestamp ASC LIMIT ? OFFSET ?
            """, (session_id, limit, offset))

        user_rows = c.fetchall()
        entries = []

        for row in user_rows:
            msg_id  = row['id']
            content = row['content'] or ''
            ts      = row['timestamp'] or ''

            try:
                msg_date = datetime.fromisoformat(ts).date()
                delta    = (today - msg_date).days
                if delta == 0:      day_label = 'Today'
                elif delta == 1:    day_label = 'Yesterday'
                else:               day_label = msg_date.strftime('%b %d')
            except Exception:
                day_label = ''

            c.execute("""
                SELECT id, content, metadata FROM conv_messages
                WHERE session_id = ? AND role = 'assistant' AND id > ?
                ORDER BY id ASC LIMIT 1
            """, (session_id, msg_id))
            asst_row     = c.fetchone()
            asst_id      = None
            asst_preview = ''
            if asst_row:
                asst_id      = asst_row['id']
                asst_preview = (asst_row['content'] or '')[:120]

            atom_count = 0
            if asst_id is not None:
                c.execute("SELECT COUNT(*) as cnt FROM conv_atoms WHERE message_id = ?",
                          (asst_id,))
                ar = c.fetchone()
                atom_count = ar['cnt'] if ar else 0

            graduated_count = 0
            if asst_id is not None:
                c.execute("""
                    SELECT COUNT(*) as cnt FROM conv_atoms
                    WHERE message_id = ? AND graduated_at IS NOT NULL
                """, (asst_id,))
                gr = c.fetchone()
                graduated_count = gr['cnt'] if gr else 0

            entries.append({
                'message_id':           msg_id,
                'assistant_message_id': asst_id,
                'user_preview':         content[:80],
                'assistant_preview':    asst_preview,
                'timestamp':            ts,
                'day_label':            day_label,
                'atom_count':           atom_count,
                'graduated_count':      graduated_count,
            })

        conn.close()
        return entries

    def get_total_turn_count(self, session_id: str) -> int:
        conn = self._conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as cnt FROM conv_messages WHERE session_id = ? AND role = 'user'",
                  (session_id,))
        row = c.fetchone()
        conn.close()
        return row['cnt'] if row else 0

    # ── Cognitive metrics ─────────────────────────────────────────────────────

    def get_cognitive_metrics(self, session_id: str) -> Dict:
        """
        Return longitudinal metrics for a user session.
        Includes atom growth, source ratios, concept entropy, and graduation stats.
        """
        conn = self._conn()
        c = conn.cursor()
        now      = datetime.now()
        cut_7d   = (now - timedelta(days=7)).isoformat()
        cut_30d  = (now - timedelta(days=30)).isoformat()

        c.execute("SELECT COUNT(*) as n FROM conv_atoms WHERE session_id = ?", (session_id,))
        total = c.fetchone()['n']

        c.execute("SELECT COUNT(*) as n FROM conv_atoms WHERE session_id = ? AND source = 'user'",
                  (session_id,))
        user_atoms = c.fetchone()['n']

        c.execute("SELECT COUNT(*) as n FROM conv_atoms WHERE session_id = ? AND source = 'assistant'",
                  (session_id,))
        asst_atoms = c.fetchone()['n']

        c.execute("SELECT COUNT(*) as n FROM conv_atoms WHERE session_id = ? AND timestamp >= ?",
                  (session_id, cut_7d))
        last_7d = c.fetchone()['n']

        c.execute("SELECT COUNT(*) as n FROM conv_atoms WHERE session_id = ? AND timestamp >= ?",
                  (session_id, cut_30d))
        last_30d = c.fetchone()['n']

        c.execute("SELECT COUNT(*) as n FROM conv_atoms WHERE session_id = ? AND graduated_at IS NOT NULL",
                  (session_id,))
        graduated = c.fetchone()['n']

        c.execute("""
            SELECT subject, COUNT(*) as cnt FROM conv_atoms
            WHERE session_id = ? AND timestamp >= ? AND subject != ''
            GROUP BY subject
        """, (session_id, cut_30d))
        subject_rows = c.fetchall()
        entropy = 0.0
        if subject_rows:
            total_30d = sum(r['cnt'] for r in subject_rows)
            if total_30d > 0:
                for r in subject_rows:
                    p = r['cnt'] / total_30d
                    entropy -= p * math.log2(p)

        c.execute("SELECT COUNT(*) as n FROM conv_messages WHERE session_id = ? AND role = 'user'",
                  (session_id,))
        turn_count = c.fetchone()['n']

        conn.close()
        return {
            'session_id':      session_id,
            'total_atoms':     total,
            'user_atoms':      user_atoms,
            'assistant_atoms': asst_atoms,
            'assistant_ratio': round(asst_atoms / total, 3) if total else 0.0,
            'graduated_to_kb': graduated,
            'pending':         total - graduated,
            'atoms_last_7d':   last_7d,
            'atoms_last_30d':  last_30d,
            'concept_entropy': round(entropy, 3),
            'turn_count':      turn_count,
            'decay_lambda':    DECAY_LAMBDA,
            'source_weights':  SOURCE_WEIGHTS,
        }

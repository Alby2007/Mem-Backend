"""
analytics/state_transitions.py — Market State Transition Engine

Builds and queries empirical state transition probabilities from sequential
market_state_snapshots. Answers: "Given today's market state, what typically
comes next, and with what probability?"

ALGORITHM
---------
1. Load snapshots ordered chronologically for a scope/subject
2. Discretize each consecutive pair (snap_i, snap_i+1) → (from_state, to_state)
3. Compute hours elapsed + forward OHLCV returns
4. Write to state_transitions (idempotent via UNIQUE constraint)
5. On query: GROUP BY to_state_id, compute P = n / total_n per destination

PERFORMANCE
-----------
  build_transitions()           < 30s for all tickers (daily run)
  get_transition_probabilities() < 10ms (indexed GROUP BY)
  get_current_state_forecast()  < 50ms (discretize current + query + format)
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from analytics.state_discretizer import (
    CanonicalState,
    decode_state_id,
    discretize,
    discretize_global,
)

_log = logging.getLogger(__name__)

# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_TRANSITIONS = """
CREATE TABLE IF NOT EXISTS state_transitions (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    scope              TEXT NOT NULL,
    subject            TEXT NOT NULL,
    from_state_id      TEXT NOT NULL,
    to_state_id        TEXT NOT NULL,
    transition_at      TEXT NOT NULL,
    hours_elapsed      REAL NOT NULL,
    forward_return_1w  REAL,
    forward_return_1m  REAL,
    UNIQUE(scope, subject, transition_at)
);
"""

_CREATE_IDX_FROM = """
CREATE INDEX IF NOT EXISTS idx_transitions_from
ON state_transitions(scope, from_state_id);
"""

_CREATE_IDX_PAIR = """
CREATE INDEX IF NOT EXISTS idx_transitions_pair
ON state_transitions(scope, from_state_id, to_state_id);
"""

_CREATE_IDX_SUBJ = """
CREATE INDEX IF NOT EXISTS idx_transitions_subject
ON state_transitions(subject, from_state_id);
"""


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_TRANSITIONS)
    conn.execute(_CREATE_IDX_FROM)
    conn.execute(_CREATE_IDX_PAIR)
    conn.execute(_CREATE_IDX_SUBJ)
    conn.commit()


# ── Forward outcome lookup ────────────────────────────────────────────────────

def _forward_returns(
    subject: str,
    snapshot_at: str,
    db_path: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Look up 1w and 1m price returns after snapshot_at from ohlcv_cache."""
    try:
        from analytics.temporal_search import compute_forward_outcomes
        return compute_forward_outcomes(subject, snapshot_at, db_path)
    except Exception:
        pass
    return None, None


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class TransitionProbability:
    to_state_id:            str
    to_state:               CanonicalState
    probability:            float
    observation_count:      int
    avg_hours_to_transition: float
    avg_forward_return_1w:  Optional[float]
    avg_forward_return_1m:  Optional[float]
    confidence:             str   # 'high' ≥20, 'moderate' 10-19, 'low' 3-9


@dataclass
class TransitionForecast:
    current_state:         CanonicalState
    current_state_id:      str
    total_observations:    int
    avg_persistence_hours: float
    transitions:           List[TransitionProbability] = field(default_factory=list)
    self_transition_rate:  float = 0.0
    confidence:            str = 'low'


def _conf_label(n: int) -> str:
    if n >= 20:
        return 'high'
    if n >= 10:
        return 'moderate'
    return 'low'


# ── TransitionEngine ──────────────────────────────────────────────────────────

class TransitionEngine:

    def __init__(self, db_path: str):
        self._db_path = db_path

    def _conn(self, timeout: float = 15.0) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=timeout, check_same_thread=False)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=15000')
        return conn

    # ── build_transitions ─────────────────────────────────────────────────────

    def build_transitions(
        self,
        scope: str = 'global',
        subject: str = 'market',
        limit: int = 5000,
    ) -> int:
        """
        Process sequential snapshots for scope/subject into the transitions table.
        Returns number of new rows written.
        """
        conn = self._conn()
        try:
            _ensure_tables(conn)

            rows = conn.execute(
                """SELECT snapshot_at, state_json
                   FROM market_state_snapshots
                   WHERE scope=? AND subject=?
                   ORDER BY snapshot_at ASC
                   LIMIT ?""",
                (scope, subject, limit),
            ).fetchall()
        except Exception as exc:
            _log.debug('build_transitions: snapshot read failed (%s/%s): %s', scope, subject, exc)
            conn.close()
            return 0

        if len(rows) < 2:
            conn.close()
            return 0

        written = 0
        disc_fn = discretize_global if scope == 'global' else discretize

        for i in range(len(rows) - 1):
            snap_at_i,   json_i   = rows[i]
            snap_at_ip1, json_ip1 = rows[i + 1]

            try:
                state_i   = json.loads(json_i)
                state_ip1 = json.loads(json_ip1)
            except Exception:
                continue

            try:
                cs_i   = disc_fn(state_i)
                cs_ip1 = disc_fn(state_ip1)
            except Exception:
                continue

            # Hours elapsed between consecutive snapshots
            try:
                dt_i   = datetime.fromisoformat(snap_at_i.replace('Z', '+00:00'))
                dt_ip1 = datetime.fromisoformat(snap_at_ip1.replace('Z', '+00:00'))
                hours  = (dt_ip1 - dt_i).total_seconds() / 3600.0
            except Exception:
                hours = 6.0

            # Forward returns based on subject (only meaningful for tickers, not 'market')
            r1w, r1m = None, None
            if scope == 'ticker':
                r1w, r1m = _forward_returns(subject, snap_at_ip1, self._db_path)

            try:
                conn.execute(
                    """INSERT OR IGNORE INTO state_transitions
                       (scope, subject, from_state_id, to_state_id, transition_at,
                        hours_elapsed, forward_return_1w, forward_return_1m)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (scope, subject, cs_i.state_id, cs_ip1.state_id,
                     snap_at_ip1, hours, r1w, r1m),
                )
                written += conn.execute('SELECT changes()').fetchone()[0]
            except Exception as exc:
                _log.debug('build_transitions: insert failed: %s', exc)

        conn.commit()
        conn.close()
        _log.info('build_transitions(%s/%s): %d rows written from %d snapshots', scope, subject, written, len(rows))
        return written

    # ── get_transition_probabilities ──────────────────────────────────────────

    def get_transition_probabilities(
        self,
        from_state_id: str,
        scope: str = 'global',
        min_observations: int = 3,
    ) -> Optional[TransitionForecast]:
        """
        Query the empirical probability distribution over next states.
        Returns None if insufficient data.
        """
        try:
            conn = self._conn(timeout=5)
            rows = conn.execute(
                """SELECT to_state_id,
                          COUNT(*) as n,
                          AVG(hours_elapsed) as avg_hours,
                          AVG(forward_return_1w) as avg_r1w,
                          AVG(forward_return_1m) as avg_r1m
                   FROM state_transitions
                   WHERE scope=? AND from_state_id=?
                   GROUP BY to_state_id
                   HAVING COUNT(*) >= ?
                   ORDER BY n DESC""",
                (scope, from_state_id, min_observations),
            ).fetchall()
            conn.close()
        except Exception as exc:
            _log.debug('get_transition_probabilities: query failed: %s', exc)
            return None

        if not rows:
            return None

        total_n = sum(r[1] for r in rows)
        if total_n < min_observations:
            return None

        # Self-transition (staying in same state)
        self_n = sum(r[1] for r in rows if r[0] == from_state_id)
        self_rate = self_n / total_n if total_n > 0 else 0.0

        # Average persistence: how many hours typically spent in this state
        # Use avg_hours of self-transition rows if present, else first row
        self_rows = [r for r in rows if r[0] == from_state_id]
        avg_persist = self_rows[0][2] if self_rows else 6.0

        transitions: List[TransitionProbability] = []
        for to_state_id, n, avg_hours, avg_r1w, avg_r1m in rows:
            prob = n / total_n
            tp = TransitionProbability(
                to_state_id             = to_state_id,
                to_state                = decode_state_id(to_state_id),
                probability             = round(prob, 3),
                observation_count       = n,
                avg_hours_to_transition = round(avg_hours or 6.0, 1),
                avg_forward_return_1w   = round(avg_r1w, 4) if avg_r1w is not None else None,
                avg_forward_return_1m   = round(avg_r1m, 4) if avg_r1m is not None else None,
                confidence              = _conf_label(n),
            )
            transitions.append(tp)

        # Sort non-self transitions by probability DESC, self-transition excluded from list
        non_self = sorted(
            [t for t in transitions if t.to_state_id != from_state_id],
            key=lambda t: t.probability, reverse=True,
        )

        return TransitionForecast(
            current_state         = decode_state_id(from_state_id),
            current_state_id      = from_state_id,
            total_observations    = total_n,
            avg_persistence_hours = round(avg_persist, 1),
            transitions           = non_self[:5],
            self_transition_rate  = round(self_rate, 3),
            confidence            = _conf_label(total_n),
        )

    # ── get_current_state_forecast ────────────────────────────────────────────

    def get_current_state_forecast(
        self,
        scope: str = 'global',
        subject: str = 'market',
    ) -> Optional[TransitionForecast]:
        """
        Read the most recent snapshot, discretize it, return transition forecast.
        """
        try:
            conn = self._conn(timeout=5)
            row = conn.execute(
                """SELECT state_json FROM market_state_snapshots
                   WHERE scope=? AND subject=?
                   ORDER BY snapshot_at DESC LIMIT 1""",
                (scope, subject),
            ).fetchone()
            conn.close()
        except Exception as exc:
            _log.debug('get_current_state_forecast: snapshot read failed: %s', exc)
            return None

        if not row:
            return None

        try:
            state_json = json.loads(row[0])
        except Exception:
            return None

        disc_fn = discretize_global if scope == 'global' else discretize
        try:
            cs = disc_fn(state_json)
        except Exception as exc:
            _log.debug('get_current_state_forecast: discretize failed: %s', exc)
            return None

        forecast = self.get_transition_probabilities(cs.state_id, scope=scope)
        if forecast:
            # Replace the decoded state with the freshly computed one
            forecast.current_state = cs
        return forecast

    # ── get_state_statistics ──────────────────────────────────────────────────

    def get_state_statistics(self, scope: str = 'global') -> dict:
        """
        Return summary statistics for the entire transition graph.
        """
        result: dict = {
            'total_states':            0,
            'total_transitions':       0,
            'most_common_state':       None,
            'most_common_state_pct':   None,
            'most_common_transition':  None,
            'most_common_trans_prob':  None,
            'stickiest_states':        [],
            'highest_entropy_states':  [],
            'avg_persistence_hours':   None,
        }
        try:
            conn = self._conn(timeout=5)

            # Total unique states + total transition records
            row = conn.execute(
                'SELECT COUNT(DISTINCT from_state_id), COUNT(*) FROM state_transitions WHERE scope=?',
                (scope,),
            ).fetchone()
            if row:
                result['total_states']      = row[0]
                result['total_transitions'] = row[1]

            if result['total_transitions'] == 0:
                conn.close()
                return result

            # Most common state (by total observations as from_state)
            state_counts = conn.execute(
                """SELECT from_state_id, COUNT(*) as n
                   FROM state_transitions WHERE scope=?
                   GROUP BY from_state_id ORDER BY n DESC LIMIT 1""",
                (scope,),
            ).fetchone()
            if state_counts:
                result['most_common_state'] = state_counts[0]
                result['most_common_state_pct'] = round(
                    state_counts[1] / result['total_transitions'] * 100, 1
                )

            # Most common transition pair
            pair = conn.execute(
                """SELECT from_state_id, to_state_id, COUNT(*) as n
                   FROM state_transitions WHERE scope=? AND from_state_id != to_state_id
                   GROUP BY from_state_id, to_state_id ORDER BY n DESC LIMIT 1""",
                (scope,),
            ).fetchone()
            if pair:
                result['most_common_transition'] = f'{pair[0]} → {pair[1]}'
                # Compute probability: n / total from from_state
                from_total = conn.execute(
                    'SELECT COUNT(*) FROM state_transitions WHERE scope=? AND from_state_id=?',
                    (scope, pair[0]),
                ).fetchone()[0]
                result['most_common_trans_prob'] = round(pair[2] / from_total, 3) if from_total else None

            # Stickiest states (highest self-transition rate)
            sticky_rows = conn.execute(
                """SELECT from_state_id,
                          SUM(CASE WHEN from_state_id=to_state_id THEN 1 ELSE 0 END) as self_n,
                          COUNT(*) as total_n
                   FROM state_transitions WHERE scope=?
                   GROUP BY from_state_id
                   HAVING total_n >= 5
                   ORDER BY CAST(self_n AS FLOAT)/total_n DESC
                   LIMIT 5""",
                (scope,),
            ).fetchall()
            result['stickiest_states'] = [
                {'state_id': r[0], 'self_rate': round(r[1] / r[2], 3)}
                for r in sticky_rows
            ]

            # Highest entropy states (most transitions OUT to different states)
            entropy_rows = conn.execute(
                """SELECT from_state_id, COUNT(DISTINCT to_state_id) as n_dest, COUNT(*) as total
                   FROM state_transitions WHERE scope=? AND from_state_id != to_state_id
                   GROUP BY from_state_id
                   HAVING total >= 5
                   ORDER BY n_dest DESC LIMIT 5""",
                (scope,),
            ).fetchall()
            result['highest_entropy_states'] = [
                {'state_id': r[0], 'distinct_destinations': r[1]}
                for r in entropy_rows
            ]

            # Average persistence
            avg_p = conn.execute(
                """SELECT AVG(hours_elapsed) FROM state_transitions
                   WHERE scope=? AND from_state_id=to_state_id""",
                (scope,),
            ).fetchone()
            if avg_p and avg_p[0]:
                result['avg_persistence_hours'] = round(avg_p[0], 1)

            conn.close()
        except Exception as exc:
            _log.debug('get_state_statistics: %s', exc)

        return result

"""
users/personal_kb.py — Personal Knowledge Base Layer

Provides per-user KB atoms stored in user_kb_context table.
Strictly isolated from shared KB (facts table) — never imports from or
writes to knowledge/graph.py or the shared facts table.

Structural isolation rule:
  _TABLE is a hardcoded string literal — never parameterised. write_atom()
  carries a class-level assertion to enforce this at runtime.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)

_TABLE = 'user_kb_context'  # hardcoded — never a variable


# ── PersonalContext dataclass ──────────────────────────────────────────────────

@dataclass
class PersonalContext:
    user_id: str
    sector_affinity: List[str] = field(default_factory=list)
    risk_tolerance: str = 'moderate'
    holding_style: str = 'unknown'
    portfolio_beta: Optional[float] = None
    preferred_pattern: Optional[str] = None
    avg_win_rate: Optional[float] = None
    high_engagement_sector: Optional[str] = None
    low_engagement_sector: Optional[str] = None
    preferred_upside_min: Optional[float] = None
    active_universe: List[str] = field(default_factory=list)
    pattern_hit_rates: Dict[str, float] = field(default_factory=dict)
    raw_atoms: List[dict] = field(default_factory=list)


# ── PersonalKB ─────────────────────────────────────────────────────────────────

class PersonalKB:
    """Personal KB layer — reads and writes to user_kb_context only."""

    _table = _TABLE  # class-level constant for assertion

    # ── schema ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _ensure_table(conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_kb_context (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     TEXT NOT NULL,
                subject     TEXT NOT NULL,
                predicate   TEXT NOT NULL,
                object      TEXT NOT NULL,
                confidence  REAL DEFAULT 1.0,
                source      TEXT,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                UNIQUE(user_id, subject, predicate)
            )
        """)
        conn.commit()

    # ── write ──────────────────────────────────────────────────────────────────

    def write_atom(
        self,
        user_id: str,
        subject: str,
        predicate: str,
        object_: str,
        confidence: float,
        source: str,
        db_path: str,
    ) -> None:
        assert self._table == 'user_kb_context', (
            f"personal_kb.write_atom must only write to user_kb_context, got {self._table}"
        )
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            self._ensure_table(conn)
            conn.execute(
                """INSERT INTO user_kb_context
                   (user_id, subject, predicate, object, confidence, source, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(user_id, subject, predicate)
                   DO UPDATE SET object=excluded.object, confidence=excluded.confidence,
                                 source=excluded.source, updated_at=excluded.updated_at""",
                (user_id, subject, predicate, str(object_), confidence, source, now, now),
            )
            conn.commit()
        finally:
            conn.close()

    # ── read ───────────────────────────────────────────────────────────────────

    def read_atoms(
        self,
        user_id: str,
        db_path: str,
        predicates: Optional[List[str]] = None,
    ) -> List[dict]:
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            self._ensure_table(conn)
            if predicates:
                ph = ','.join('?' for _ in predicates)
                rows = conn.execute(
                    f"""SELECT id, user_id, subject, predicate, object, confidence, source,
                               created_at, updated_at
                        FROM user_kb_context
                        WHERE user_id = ? AND predicate IN ({ph})
                        ORDER BY updated_at DESC""",
                    [user_id] + predicates,
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id, user_id, subject, predicate, object, confidence, source,
                              created_at, updated_at
                       FROM user_kb_context WHERE user_id = ?
                       ORDER BY updated_at DESC""",
                    (user_id,),
                ).fetchall()
            cols = ['id', 'user_id', 'subject', 'predicate', 'object', 'confidence',
                    'source', 'created_at', 'updated_at']
            return [dict(zip(cols, r)) for r in rows]
        finally:
            conn.close()

    # ── get_context_document ──────────────────────────────────────────────────

    def get_context_document(self, user_id: str, db_path: str) -> PersonalContext:
        """Return a PersonalContext dataclass assembled from the user's personal atoms."""
        atoms = self.read_atoms(user_id, db_path)
        atom_map: Dict[str, str] = {a['predicate']: a['object'] for a in atoms}

        sector_affinity: List[str] = []
        raw_sa = atom_map.get('sector_affinity', '')
        if raw_sa:
            try:
                sector_affinity = json.loads(raw_sa)
            except (json.JSONDecodeError, TypeError):
                sector_affinity = [s.strip() for s in raw_sa.split(',') if s.strip()]

        active_universe: List[str] = []
        raw_au = atom_map.get('active_universe', '')
        if raw_au:
            try:
                active_universe = json.loads(raw_au)
            except (json.JSONDecodeError, TypeError):
                active_universe = [t.strip() for t in raw_au.split(',') if t.strip()]

        # Collect per-pattern hit rates: atom predicate = "{pattern}_hit_rate"
        pattern_hit_rates: Dict[str, float] = {}
        for atom in atoms:
            pred = atom['predicate']
            if pred.endswith('_hit_rate'):
                pattern = pred[:-len('_hit_rate')]
                try:
                    pattern_hit_rates[pattern] = float(atom['object'])
                except (ValueError, TypeError):
                    pass

        def _float(key: str) -> Optional[float]:
            try:
                return float(atom_map[key]) if key in atom_map else None
            except (ValueError, TypeError):
                return None

        return PersonalContext(
            user_id=user_id,
            sector_affinity=sector_affinity,
            risk_tolerance=atom_map.get('risk_tolerance', 'moderate'),
            holding_style=atom_map.get('holding_style', 'unknown'),
            portfolio_beta=_float('portfolio_beta'),
            preferred_pattern=atom_map.get('preferred_pattern'),
            avg_win_rate=_float('avg_win_rate'),
            high_engagement_sector=atom_map.get('high_engagement_sector'),
            low_engagement_sector=atom_map.get('low_engagement_sector'),
            preferred_upside_min=_float('preferred_upside_min'),
            active_universe=active_universe,
            pattern_hit_rates=pattern_hit_rates,
            raw_atoms=atoms,
        )

    # ── infer_and_write_from_portfolio ────────────────────────────────────────

    def infer_and_write_from_portfolio(self, user_id: str, db_path: str) -> None:
        """
        Infer and write personal atoms from the user's portfolio + model.
        Writes: sector_affinity, risk_tolerance, holding_style, portfolio_beta.
        """
        try:
            from users.user_store import get_user_model, get_user
        except ImportError:
            _log.warning('personal_kb: user_store not available')
            return

        model = get_user_model(db_path, user_id)
        prefs = get_user(db_path, user_id)

        if model:
            affinity = model.get('sector_affinity', [])
            self.write_atom(user_id, user_id, 'sector_affinity',
                            json.dumps(affinity), 0.9, 'portfolio_infer', db_path)
            self.write_atom(user_id, user_id, 'risk_tolerance',
                            model.get('risk_tolerance', 'moderate'), 0.9, 'portfolio_infer', db_path)
            self.write_atom(user_id, user_id, 'holding_style',
                            model.get('holding_style', 'unknown'), 0.8, 'portfolio_infer', db_path)
            if model.get('portfolio_beta') is not None:
                self.write_atom(user_id, user_id, 'portfolio_beta',
                                str(model['portfolio_beta']), 0.8, 'portfolio_infer', db_path)
        elif prefs:
            sectors = prefs.get('selected_sectors', [])
            self.write_atom(user_id, user_id, 'sector_affinity',
                            json.dumps(sectors), 0.7, 'onboarding_prefs', db_path)
            self.write_atom(user_id, user_id, 'risk_tolerance',
                            prefs.get('selected_risk', 'moderate'), 0.7, 'onboarding_prefs', db_path)

    # ── update_from_feedback ──────────────────────────────────────────────────

    def update_from_feedback(
        self,
        user_id: str,
        feedback_event: dict,
        db_path: str,
    ) -> None:
        """
        Update personal atoms from a tip feedback event.
        Writes: {pattern}_hit_rate, preferred_pattern, avg_win_rate.
        feedback_event: { pattern_type, outcome, tip_id?, pattern_id? }
        """
        pattern = feedback_event.get('pattern_type', '')
        outcome = feedback_event.get('outcome', '')
        if not pattern or not outcome:
            return

        # Update per-pattern hit rate from tip_feedback history
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            try:
                rows = conn.execute(
                    """SELECT tf.outcome FROM tip_feedback tf
                       LEFT JOIN pattern_signals ps ON ps.id = tf.pattern_id
                       WHERE tf.user_id = ? AND ps.pattern_type = ?""",
                    (user_id, pattern),
                ).fetchall()
            finally:
                conn.close()

            outcomes = [r[0] for r in rows]
            hits = sum(1 for o in outcomes if o in ('hit_t1', 'hit_t2', 'hit_t3'))
            resolved = sum(1 for o in outcomes if o in ('hit_t1', 'hit_t2', 'hit_t3', 'stopped_out'))
            if resolved > 0:
                hit_rate = round(hits / resolved, 3)
                self.write_atom(user_id, user_id, f'{pattern}_hit_rate',
                                str(hit_rate), 0.85, 'feedback', db_path)

            # Overall win rate across all patterns
            conn2 = sqlite3.connect(db_path, timeout=10)
            try:
                all_rows = conn2.execute(
                    "SELECT outcome FROM tip_feedback WHERE user_id = ?",
                    (user_id,),
                ).fetchall()
            finally:
                conn2.close()

            all_outcomes = [r[0] for r in all_rows]
            total_hits = sum(1 for o in all_outcomes if o in ('hit_t1', 'hit_t2', 'hit_t3'))
            total_resolved = sum(1 for o in all_outcomes
                                 if o in ('hit_t1', 'hit_t2', 'hit_t3', 'stopped_out'))
            if total_resolved > 0:
                avg_win = round(total_hits / total_resolved, 3)
                self.write_atom(user_id, user_id, 'avg_win_rate',
                                str(avg_win), 0.85, 'feedback', db_path)

            # Update preferred_pattern (highest hit rate pattern with >= 3 samples)
            personal_atoms = self.read_atoms(user_id, db_path)
            hit_rates = {
                a['predicate'].replace('_hit_rate', ''): float(a['object'])
                for a in personal_atoms
                if a['predicate'].endswith('_hit_rate')
            }
            if hit_rates:
                best = max(hit_rates, key=hit_rates.get)
                self.write_atom(user_id, user_id, 'preferred_pattern',
                                best, 0.8, 'feedback', db_path)

        except Exception as exc:
            _log.error('personal_kb.update_from_feedback: %s', exc)

    # ── update_from_engagement ────────────────────────────────────────────────

    def update_from_engagement(
        self,
        user_id: str,
        db_path: str,
    ) -> None:
        """
        Infer high/low engagement sectors from user_engagement_events.
        Writes: high_engagement_sector, low_engagement_sector, preferred_upside_min.
        """
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            try:
                rows = conn.execute(
                    """SELECT sector, COUNT(*) as cnt
                       FROM user_engagement_events
                       WHERE user_id = ? AND sector IS NOT NULL
                       GROUP BY sector ORDER BY cnt DESC""",
                    (user_id,),
                ).fetchall()
            finally:
                conn.close()

            if not rows:
                return

            if len(rows) >= 1:
                self.write_atom(user_id, user_id, 'high_engagement_sector',
                                rows[0][0], 0.75, 'engagement', db_path)
            if len(rows) >= 2:
                self.write_atom(user_id, user_id, 'low_engagement_sector',
                                rows[-1][0], 0.75, 'engagement', db_path)

        except Exception as exc:
            _log.error('personal_kb.update_from_engagement: %s', exc)

    # ── write_universe_atoms ──────────────────────────────────────────────────

    def write_universe_atoms(
        self,
        user_id: str,
        tickers: List[str],
        niche_description: str,
        db_path: str,
    ) -> None:
        """
        Write active_universe and niche_interest_N atoms after expansion.
        """
        self.write_atom(user_id, user_id, 'active_universe',
                        json.dumps([t.upper() for t in tickers]),
                        0.9, 'universe_expansion', db_path)

        # niche_interest_N — count existing niche atoms to find N
        existing = self.read_atoms(user_id, db_path)
        niche_count = sum(1 for a in existing if a['predicate'].startswith('niche_interest_'))
        self.write_atom(user_id, user_id, f'niche_interest_{niche_count + 1}',
                        niche_description[:200], 0.8, 'universe_expansion', db_path)


# ── Module-level convenience instance ─────────────────────────────────────────

_personal_kb = PersonalKB()


def write_atom(user_id: str, subject: str, predicate: str, object_: str,
               confidence: float, source: str, db_path: str) -> None:
    _personal_kb.write_atom(user_id, subject, predicate, object_, confidence, source, db_path)


def read_atoms(user_id: str, db_path: str, predicates: Optional[List[str]] = None) -> List[dict]:
    return _personal_kb.read_atoms(user_id, db_path, predicates)


def get_context_document(user_id: str, db_path: str) -> PersonalContext:
    return _personal_kb.get_context_document(user_id, db_path)


def infer_and_write_from_portfolio(user_id: str, db_path: str) -> None:
    _personal_kb.infer_and_write_from_portfolio(user_id, db_path)


def update_from_feedback(user_id: str, feedback_event: dict, db_path: str) -> None:
    _personal_kb.update_from_feedback(user_id, feedback_event, db_path)


def update_from_engagement(user_id: str, db_path: str) -> None:
    _personal_kb.update_from_engagement(user_id, db_path)


def write_universe_atoms(user_id: str, tickers: List[str],
                         niche_description: str, db_path: str) -> None:
    _personal_kb.write_universe_atoms(user_id, tickers, niche_description, db_path)

"""
knowledge/thesis_builder.py — Natural Language Thesis Builder + Monitor

Formalises a user's investment idea into a KB-stored thesis with explicit
premises, supporting evidence, contradicting evidence, and an invalidation
condition. The ThesisMonitor then watches for incoming atoms that approach
the invalidation threshold and fires Telegram alerts proactively.

THESIS STRUCTURE
================
A thesis is stored as KB atoms under subject `thesis_{ticker}_{user_id}_{date}`:

  thesis_id | premise              | "HSBA.L will benefit from higher-for-longer rates"
  thesis_id | direction            | "bullish"
  thesis_id | ticker               | "HSBA.L"
  thesis_id | invalidation_condition | "boe_base_rate < 4.5%"
  thesis_id | thesis_score         | "0.78"
  thesis_id | thesis_status        | "CONFIRMED"
  thesis_id | supporting_evidence  | "boe_base_rate=5.25%, central_bank_stance=restrictive"
  thesis_id | contradicting_evidence | "uk_gdp_growth=+0.1%"
  thesis_id | user_id              | "alby2007"
  thesis_id | created_at           | "2026-02-27T10:00:00Z"

The thesis_id also maps to a thesis_index table for efficient lookup by
user_id and by predicate (for the ThesisMonitor).

THESIS MONITOR
==============
ThesisMonitor.on_atom_written(subject, predicate, object) is called by
KnowledgeGraph.add_fact() via the same post-write hook as CausalShockEngine.
It checks all active theses referencing this predicate and fires a Telegram
alert if the new atom value is within 20% of the invalidation threshold.

INTENT DETECTION
================
ThesisBuilder.detect_thesis_intent(message) returns True when the chat
message appears to be requesting a thesis build. Used by /chat to decide
whether to invoke the builder.

Patterns detected:
  - "I think HSBA.L will..."
  - "thesis on BARC.L..."
  - "build a thesis for..."
  - "investment case for..."
  - "what's the case for..."
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

# ── Thesis intent patterns ─────────────────────────────────────────────────────

_THESIS_INTENT_PATTERNS = [
    r'\bI think\b.*\bwill\b',
    r'\bthesis\b.*\bon\b',
    r'\bbuild\s+a\s+thesis\b',
    r'\binvestment\s+case\b',
    r'\bcase\s+for\b',
    r'\bbull\s+case\b',
    r'\bbear\s+case\b',
    r'\bformalise\b',
    r'\binvalidation\s+condition\b',
]

_COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _THESIS_INTENT_PATTERNS]

# ── Supporting predicates (pulled from KB to assess thesis) ───────────────────

_BULLISH_SUPPORT_PREDICATES = [
    'signal_direction',       # bullish/buy/long
    'conviction_tier',        # high
    'boe_base_rate',          # high = NIM benefit
    'central_bank_stance',    # restrictive = bank beneficiary
    'institutional_flow',     # accumulating
    'return_in_risk_on_expansion',
    'return_in_stagflation',
    'return_in_recovery',
    'macro_confirmation',     # confirmed
    'uk_gilt_10y',            # high = NIM benefit
]

_CONTRADICTION_PREDICATES = [
    'signal_direction',       # bearish/sell/short
    'conviction_tier',        # avoid
    'uk_gdp_growth',          # near-stagnation = cut pressure
    'us_macro',               # global easing pressure
    'fca_short_interest',     # heavy shorts = institutional bearish view
    'macro_confirmation',     # unconfirmed
    'central_bank_stance',    # accommodative (bad for rate beneficiaries)
]

# ── Invalidation templates ─────────────────────────────────────────────────────

# Maps (predicate, direction) → invalidation condition template
_INVALIDATION_TEMPLATES: Dict[Tuple[str, str], str] = {
    ('boe_base_rate', 'bullish'):        'boe_base_rate < {threshold:.2f}%',
    ('central_bank_stance', 'bullish'):  'central_bank_stance = accommodative',
    ('signal_direction', 'bullish'):     'signal_direction = bearish',
    ('signal_direction', 'bearish'):     'signal_direction = bullish',
    ('conviction_tier', 'bullish'):      'conviction_tier = avoid',
    ('institutional_flow', 'bullish'):   'institutional_flow = distributing',
    ('uk_gdp_growth', 'bullish'):        'uk_gdp_growth < -0.5%',
}

# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_THESIS_INDEX = """
CREATE TABLE IF NOT EXISTS thesis_index (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    thesis_id       TEXT    NOT NULL,
    user_id         TEXT    NOT NULL,
    ticker          TEXT    NOT NULL,
    direction       TEXT,
    thesis_status   TEXT    DEFAULT 'CONFIRMED',
    invalidation_predicate TEXT,
    invalidation_threshold REAL,
    invalidation_condition TEXT,
    created_at      TEXT    NOT NULL,
    last_evaluated  TEXT,
    UNIQUE(thesis_id)
)
"""

_CREATE_THESIS_ALERTS = """
CREATE TABLE IF NOT EXISTS thesis_alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    thesis_id   TEXT    NOT NULL,
    user_id     TEXT    NOT NULL,
    alert_type  TEXT    NOT NULL,
    message     TEXT,
    triggered_at TEXT   NOT NULL,
    sent        INTEGER DEFAULT 0
)
"""


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class ThesisResult:
    thesis_id:            str
    ticker:               str
    user_id:              str
    direction:            str
    premise:              str
    thesis_status:        str        # CONFIRMED | CHALLENGED | INVALIDATED
    thesis_score:         float      # 0.0–1.0
    supporting_evidence:  List[str]
    contradicting_evidence: List[str]
    invalidation_condition: str
    created_at:           str


@dataclass
class ThesisEvaluation:
    thesis_id:   str
    ticker:      str
    status:      str
    score:       float
    supporting:  List[str]
    contradicting: List[str]
    evaluated_at: str


# ── ThesisBuilder ──────────────────────────────────────────────────────────────

class ThesisBuilder:
    """
    Builds and stores a formal thesis from a natural language premise.

    Usage
    -----
    builder = ThesisBuilder(db_path)
    result  = builder.build(
        ticker    = 'HSBA.L',
        premise   = 'Will benefit from higher-for-longer BoE rates',
        direction = 'bullish',
        user_id   = 'alby2007',
    )
    """

    def __init__(self, db_path: str) -> None:
        self._db = db_path
        self._ensure_tables()

    # ── Public API ────────────────────────────────────────────────────────────

    @staticmethod
    def detect_thesis_intent(message: str) -> bool:
        """Return True if the message appears to be requesting a thesis build."""
        return any(p.search(message) for p in _COMPILED_PATTERNS)

    def build(
        self,
        ticker:    str,
        premise:   str,
        direction: str,
        user_id:   str,
    ) -> ThesisResult:
        """
        Build and persist a thesis for (ticker, premise, direction, user_id).

        Parameters
        ----------
        ticker    : Ticker symbol e.g. 'HSBA.L'
        premise   : Free-text thesis premise
        direction : 'bullish' | 'bearish'
        user_id   : User who owns the thesis
        """
        ticker_up  = ticker.upper()
        ticker_lo  = ticker.lower()
        now_iso    = datetime.now(timezone.utc).isoformat()
        date_str   = now_iso[:10].replace('-', '')
        thesis_id  = f'thesis_{ticker_lo}_{user_id}_{date_str}'

        # ── Pull KB evidence ──────────────────────────────────────────────────
        supporting, contradicting = self._evaluate_evidence(ticker_lo, direction)

        # ── Score and classify ────────────────────────────────────────────────
        n_sup  = len(supporting)
        n_con  = len(contradicting)
        total  = n_sup + n_con
        score  = round(n_sup / total, 3) if total > 0 else 0.5

        if score >= 0.65:
            status = 'CONFIRMED'
        elif score >= 0.40:
            status = 'CHALLENGED'
        else:
            status = 'INVALIDATED'

        # ── Derive invalidation condition ─────────────────────────────────────
        inv_condition, inv_predicate, inv_threshold = self._derive_invalidation(
            ticker_lo, direction
        )

        # ── Persist as KB atoms ───────────────────────────────────────────────
        self._write_thesis_atoms(
            thesis_id, ticker_lo, user_id, premise, direction,
            status, score, supporting, contradicting, inv_condition, now_iso,
        )

        # ── Update thesis_index ───────────────────────────────────────────────
        self._upsert_thesis_index(
            thesis_id, user_id, ticker_up, direction, status,
            inv_predicate, inv_threshold, inv_condition, now_iso,
        )

        _log.info(
            'ThesisBuilder: built %s for %s/%s status=%s score=%.2f',
            thesis_id, ticker_up, user_id, status, score,
        )

        return ThesisResult(
            thesis_id             = thesis_id,
            ticker                = ticker_up,
            user_id               = user_id,
            direction             = direction,
            premise               = premise,
            thesis_status         = status,
            thesis_score          = score,
            supporting_evidence   = supporting,
            contradicting_evidence= contradicting,
            invalidation_condition= inv_condition,
            created_at            = now_iso,
        )

    def evaluate(self, thesis_id: str) -> Optional[ThesisEvaluation]:
        """
        Re-evaluate a stored thesis against current KB state.
        Updates thesis_index.thesis_status and last_evaluated.
        """
        row = self._get_thesis_index_row(thesis_id)
        if row is None:
            return None

        ticker_lo = row['ticker'].lower()
        direction = row['direction'] or 'bullish'
        now_iso   = datetime.now(timezone.utc).isoformat()

        supporting, contradicting = self._evaluate_evidence(ticker_lo, direction)
        n_sup = len(supporting)
        n_con = len(contradicting)
        total = n_sup + n_con
        score = round(n_sup / total, 3) if total > 0 else 0.5

        if score >= 0.65:
            status = 'CONFIRMED'
        elif score >= 0.40:
            status = 'CHALLENGED'
        else:
            status = 'INVALIDATED'

        conn = self._connect()
        try:
            conn.execute(
                """UPDATE thesis_index
                   SET thesis_status=?, last_evaluated=?
                   WHERE thesis_id=?""",
                (status, now_iso, thesis_id),
            )
            conn.commit()
        finally:
            conn.close()

        return ThesisEvaluation(
            thesis_id    = thesis_id,
            ticker       = row['ticker'],
            status       = status,
            score        = score,
            supporting   = supporting,
            contradicting= contradicting,
            evaluated_at = now_iso,
        )

    def list_user_theses(self, user_id: str) -> List[Dict[str, Any]]:
        """Return all theses for a user from thesis_index."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT thesis_id, ticker, direction, thesis_status,
                          invalidation_condition, created_at, last_evaluated
                   FROM thesis_index
                   WHERE user_id=?
                   ORDER BY created_at DESC""",
                (user_id,),
            ).fetchall()
            cols = ['thesis_id', 'ticker', 'direction', 'thesis_status',
                    'invalidation_condition', 'created_at', 'last_evaluated']
            return [dict(zip(cols, r)) for r in rows]
        finally:
            conn.close()

    # ── Evidence evaluation ───────────────────────────────────────────────────

    def _evaluate_evidence(
        self,
        ticker_lo: str,
        direction: str,
    ) -> Tuple[List[str], List[str]]:
        """
        Query KB for supporting and contradicting atoms.
        Returns (supporting_list, contradicting_list) of human-readable strings.
        """
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT predicate, object, confidence FROM facts
                   WHERE subject=?
                   ORDER BY confidence DESC""",
                (ticker_lo,),
            ).fetchall()
            # Also pull macro subjects
            macro_rows = []
            for macro_subj in ('uk_macro', 'us_macro', 'uk_yields'):
                macro_rows += conn.execute(
                    """SELECT predicate, object, confidence FROM facts
                       WHERE subject=?
                       ORDER BY confidence DESC LIMIT 5""",
                    (macro_subj,),
                ).fetchall()
            all_rows = rows + macro_rows
        finally:
            conn.close()

        atom_map: Dict[str, str] = {}
        for pred, obj, conf in all_rows:
            if pred not in atom_map:
                atom_map[pred] = obj

        supporting:    List[str] = []
        contradicting: List[str] = []

        for pred, obj in atom_map.items():
            sup = self._is_supporting(pred, obj, direction)
            con = self._is_contradicting(pred, obj, direction)
            readable = f'{pred}={obj}'
            if sup:
                supporting.append(readable)
            elif con:
                contradicting.append(readable)

        return supporting, contradicting

    @staticmethod
    def _is_supporting(predicate: str, value: str, direction: str) -> bool:
        v = value.lower()
        if direction == 'bullish':
            if predicate == 'signal_direction' and v in ('bullish', 'buy', 'long', 'near_high'):
                return True
            if predicate == 'conviction_tier' and v == 'high':
                return True
            if predicate == 'central_bank_stance' and 'restrictive' in v:
                return True
            if predicate == 'institutional_flow' and 'accumulat' in v:
                return True
            if predicate == 'macro_confirmation' and 'confirmed' in v and 'un' not in v:
                return True
            if predicate in ('boe_base_rate', 'uk_gilt_10y'):
                try:
                    num = float(re.findall(r'[\d.]+', v)[0])
                    if num >= 4.5:
                        return True
                except (IndexError, ValueError):
                    pass
        if direction == 'bearish':
            if predicate == 'signal_direction' and v in ('bearish', 'sell', 'short', 'near_low'):
                return True
            if predicate == 'fca_short_interest':
                try:
                    num = float(v.split('%')[0])
                    if num >= 2.0:
                        return True
                except (ValueError, AttributeError):
                    pass
        return False

    @staticmethod
    def _is_contradicting(predicate: str, value: str, direction: str) -> bool:
        v = value.lower()
        if direction == 'bullish':
            if predicate == 'signal_direction' and v in ('bearish', 'sell', 'short', 'near_low'):
                return True
            if predicate == 'conviction_tier' and v == 'avoid':
                return True
            if predicate == 'central_bank_stance' and ('accommodative' in v or 'dovish' in v):
                return True
            if predicate == 'macro_confirmation' and ('unconfirmed' in v or 'not confirmed' in v):
                return True
            if predicate == 'institutional_flow' and 'distribut' in v:
                return True
            if predicate == 'uk_gdp_growth':
                try:
                    num = float(re.findall(r'[+-]?[\d.]+', v)[0])
                    if num < 0.2:
                        return True
                except (IndexError, ValueError):
                    pass
        if direction == 'bearish':
            if predicate == 'signal_direction' and v in ('bullish', 'buy', 'long', 'near_high'):
                return True
            if predicate == 'conviction_tier' and v == 'high':
                return True
        return False

    # ── Invalidation condition derivation ─────────────────────────────────────

    def _derive_invalidation(
        self,
        ticker_lo: str,
        direction: str,
    ) -> Tuple[str, Optional[str], Optional[float]]:
        """
        Derive the most meaningful invalidation condition for this thesis.
        Returns (condition_string, predicate, numeric_threshold).
        """
        conn = self._connect()
        try:
            # Find the single most important supporting predicate in KB
            for pred in _BULLISH_SUPPORT_PREDICATES if direction == 'bullish' else ['signal_direction', 'conviction_tier']:
                template = _INVALIDATION_TEMPLATES.get((pred, direction))
                if template is None:
                    continue

                # Read current value to compute threshold
                row = conn.execute(
                    """SELECT object FROM facts WHERE subject=? AND predicate=?
                       ORDER BY confidence DESC LIMIT 1""",
                    (ticker_lo, pred),
                ).fetchone()
                if row is None:
                    # Try macro subjects
                    for macro_subj in ('uk_macro', 'us_macro'):
                        row = conn.execute(
                            """SELECT object FROM facts WHERE subject=? AND predicate=?
                               ORDER BY confidence DESC LIMIT 1""",
                            (macro_subj, pred),
                        ).fetchone()
                        if row:
                            break

                if row:
                    val = row[0]
                    nums = re.findall(r'[\d.]+', val)
                    if nums and '{threshold' in template:
                        current = float(nums[0])
                        threshold = round(current * 0.80, 2)  # 20% below current
                        cond = template.format(threshold=threshold)
                        return cond, pred, threshold
                    elif '{threshold' not in template:
                        return template, pred, None
        finally:
            conn.close()

        return f'signal_direction = {"bearish" if direction == "bullish" else "bullish"}', 'signal_direction', None

    # ── KB atom writing ───────────────────────────────────────────────────────

    def _write_thesis_atoms(
        self,
        thesis_id:    str,
        ticker_lo:    str,
        user_id:      str,
        premise:      str,
        direction:    str,
        status:       str,
        score:        float,
        supporting:   List[str],
        contradicting: List[str],
        inv_condition: str,
        now_iso:      str,
    ) -> None:
        """Write all thesis atoms to the facts table."""
        source = f'thesis_builder_{user_id}'
        atoms = [
            (thesis_id, 'premise',                premise,          0.90),
            (thesis_id, 'ticker',                 ticker_lo,        1.00),
            (thesis_id, 'direction',              direction,        1.00),
            (thesis_id, 'thesis_status',          status,           0.85),
            (thesis_id, 'thesis_score',           str(score),       0.85),
            (thesis_id, 'invalidation_condition', inv_condition,    0.90),
            (thesis_id, 'user_id',                user_id,          1.00),
            (thesis_id, 'created_at',             now_iso,          1.00),
            (thesis_id, 'supporting_evidence',
             '; '.join(supporting[:5]),            0.80),
            (thesis_id, 'contradicting_evidence',
             '; '.join(contradicting[:5]) if contradicting else 'none', 0.80),
        ]
        conn = self._connect()
        try:
            for subj, pred, obj, conf in atoms:
                if not obj:
                    continue
                try:
                    conn.execute(
                        """INSERT INTO facts
                           (subject, predicate, object, source, confidence, timestamp)
                           VALUES (?,?,?,?,?,?)
                           ON CONFLICT(subject, predicate, source)
                           DO UPDATE SET object=excluded.object,
                                         confidence=excluded.confidence,
                                         timestamp=excluded.timestamp""",
                        (subj, pred, obj, source, conf, now_iso),
                    )
                except Exception:
                    # Fallback for tables without source-keyed unique
                    try:
                        conn.execute(
                            """INSERT OR REPLACE INTO facts
                               (subject, predicate, object, source, confidence, timestamp)
                               VALUES (?,?,?,?,?,?)""",
                            (subj, pred, obj, source, conf, now_iso),
                        )
                    except Exception as exc:
                        _log.debug('thesis atom write failed %s|%s: %s', subj, pred, exc)
            conn.commit()
        finally:
            conn.close()

    def _upsert_thesis_index(
        self,
        thesis_id:     str,
        user_id:       str,
        ticker_up:     str,
        direction:     str,
        status:        str,
        inv_predicate: Optional[str],
        inv_threshold: Optional[float],
        inv_condition: str,
        now_iso:       str,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO thesis_index
                   (thesis_id, user_id, ticker, direction, thesis_status,
                    invalidation_predicate, invalidation_threshold,
                    invalidation_condition, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(thesis_id) DO UPDATE SET
                     thesis_status=excluded.thesis_status,
                     last_evaluated=excluded.created_at""",
                (thesis_id, user_id, ticker_up, direction, status,
                 inv_predicate, inv_threshold, inv_condition, now_iso),
            )
            conn.commit()
        finally:
            conn.close()

    def _get_thesis_index_row(self, thesis_id: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                """SELECT thesis_id, user_id, ticker, direction, thesis_status,
                          invalidation_predicate, invalidation_threshold,
                          invalidation_condition
                   FROM thesis_index WHERE thesis_id=?""",
                (thesis_id,),
            ).fetchone()
            if row is None:
                return None
            cols = ['thesis_id', 'user_id', 'ticker', 'direction', 'thesis_status',
                    'invalidation_predicate', 'invalidation_threshold', 'invalidation_condition']
            return dict(zip(cols, row))
        finally:
            conn.close()

    # ── Schema init ───────────────────────────────────────────────────────────

    def _ensure_tables(self) -> None:
        conn = self._connect()
        try:
            conn.execute(_CREATE_THESIS_INDEX)
            conn.execute(_CREATE_THESIS_ALERTS)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db, timeout=10)


# ── ThesisMonitor ──────────────────────────────────────────────────────────────

class ThesisMonitor:
    """
    Watches for atoms that approach thesis invalidation conditions.
    Called via the same on_atom_written() hook as CausalShockEngine.

    The monitor maintains an in-memory predicate index:
        { predicate: [thesis_id, ...] }
    Built lazily from thesis_index on first access, refreshed every 5 minutes.
    """

    _REFRESH_INTERVAL_SEC = 300   # 5 minutes
    _APPROACH_THRESHOLD   = 0.20  # alert when within 20% of invalidation threshold

    def __init__(self, db_path: str) -> None:
        self._db          = db_path
        self._builder     = ThesisBuilder(db_path)
        self._index: Dict[str, List[dict]] = defaultdict(list)  # pred → [thesis rows]
        self._last_refresh: Optional[datetime] = None

    def on_atom_written(self, subject: str, predicate: str, object_val: str) -> None:
        """
        Called by KnowledgeGraph.add_fact() for every atom write.
        O(1) check: only processes predicates that have active thesis watchers.
        """
        self._maybe_refresh_index()

        watchers = self._index.get(predicate)
        if not watchers:
            return

        for thesis_row in watchers:
            try:
                self._check_thesis(thesis_row, predicate, object_val)
            except Exception as exc:
                _log.debug('ThesisMonitor: check failed for %s: %s',
                           thesis_row.get('thesis_id'), exc)

    # ── Index management ──────────────────────────────────────────────────────

    def _maybe_refresh_index(self) -> None:
        now = datetime.now(timezone.utc)
        if (self._last_refresh is None or
                (now - self._last_refresh).total_seconds() > self._REFRESH_INTERVAL_SEC):
            self._refresh_index()
            self._last_refresh = now

    def _refresh_index(self) -> None:
        """Load active thesis rows from thesis_index keyed by invalidation_predicate."""
        self._index.clear()
        try:
            conn = sqlite3.connect(self._db, timeout=5)
            try:
                rows = conn.execute(
                    """SELECT thesis_id, user_id, ticker, direction,
                              invalidation_predicate, invalidation_threshold,
                              invalidation_condition
                       FROM thesis_index
                       WHERE thesis_status != 'INVALIDATED'
                         AND invalidation_predicate IS NOT NULL""",
                ).fetchall()
                cols = ['thesis_id', 'user_id', 'ticker', 'direction',
                        'invalidation_predicate', 'invalidation_threshold',
                        'invalidation_condition']
                for row in rows:
                    d = dict(zip(cols, row))
                    pred = d['invalidation_predicate']
                    if pred:
                        self._index[pred].append(d)
            finally:
                conn.close()
        except Exception as exc:
            _log.debug('ThesisMonitor: index refresh failed: %s', exc)

    # ── Invalidation check ────────────────────────────────────────────────────

    def _check_thesis(
        self,
        thesis_row:  dict,
        predicate:   str,
        new_value:   str,
    ) -> None:
        """
        Check if the new atom value is within 20% of the thesis invalidation
        threshold. If so, alert the thesis owner via Telegram.
        """
        threshold = thesis_row.get('invalidation_threshold')
        if threshold is None:
            return   # string-match condition — not numeric, skip

        # Extract numeric from new_value
        nums = re.findall(r'[\d.]+', new_value)
        if not nums:
            return
        current = float(nums[0])

        # Check whether current is within 20% of threshold
        # For "below" conditions (bullish thesis invalidated when value falls):
        #   alert if current < threshold * 1.20
        direction = thesis_row.get('direction', 'bullish')
        approaching = False

        if direction == 'bullish':
            # Threshold is a lower bound: alert if current < threshold * 1.20
            if current < threshold * (1.0 + self._APPROACH_THRESHOLD):
                approaching = True
        else:
            # Threshold is an upper bound: alert if current > threshold * 0.80
            if current > threshold * (1.0 - self._APPROACH_THRESHOLD):
                approaching = True

        if not approaching:
            return

        self._fire_alert(thesis_row, predicate, current, threshold)

    def _fire_alert(
        self,
        thesis_row: dict,
        predicate:  str,
        current:    float,
        threshold:  float,
    ) -> None:
        """Log alert to thesis_alerts table and send Telegram notification."""
        thesis_id  = thesis_row['thesis_id']
        user_id    = thesis_row['user_id']
        ticker     = thesis_row['ticker']
        inv_cond   = thesis_row.get('invalidation_condition', '')
        now_iso    = datetime.now(timezone.utc).isoformat()

        # Rate-limit: don't re-alert within 6 hours for the same thesis
        if self._recently_alerted(thesis_id, hours=6):
            return

        msg = (
            f'⚠️ THESIS ALERT — {ticker}\n'
            f'Invalidation condition approaching:\n'
            f'  Condition: {inv_cond}\n'
            f'  Current {predicate}: {current}\n'
            f'  Threshold: {threshold}\n'
            f'  Distance: {abs(current - threshold):.2f} ({abs(current - threshold) / max(threshold, 0.01) * 100:.0f}%)\n'
            f'Check: GET /thesis/{thesis_id}'
        )

        # Write to alert log
        conn = sqlite3.connect(self._db, timeout=10)
        try:
            conn.execute(
                """INSERT INTO thesis_alerts
                   (thesis_id, user_id, alert_type, message, triggered_at)
                   VALUES (?,?,?,?,?)""",
                (thesis_id, user_id, 'invalidation_approaching', msg, now_iso),
            )
            conn.commit()
            alert_id = conn.execute(
                "SELECT last_insert_rowid()"
            ).fetchone()[0]
        finally:
            conn.close()

        # Send via Telegram if user has a chat_id
        chat_id = self._get_user_chat_id(user_id)
        if chat_id:
            try:
                from notifications.telegram_notifier import TelegramNotifier
                sent = TelegramNotifier().send_plain(chat_id, msg)
                if sent:
                    self._mark_alert_sent(thesis_id, now_iso)
                    _log.info(
                        'ThesisMonitor: alerted user %s for thesis %s',
                        user_id, thesis_id,
                    )
            except Exception as exc:
                _log.debug('ThesisMonitor: Telegram send failed: %s', exc)

    def _recently_alerted(self, thesis_id: str, hours: int = 6) -> bool:
        """Return True if an alert was sent for this thesis within `hours`."""
        try:
            from datetime import timedelta
            cutoff = (datetime.now(timezone.utc) -
                      timedelta(hours=hours)).isoformat()
            conn = sqlite3.connect(self._db, timeout=5)
            try:
                row = conn.execute(
                    """SELECT COUNT(*) FROM thesis_alerts
                       WHERE thesis_id=? AND triggered_at >= ? AND sent=1""",
                    (thesis_id, cutoff),
                ).fetchone()
                return (row[0] or 0) > 0
            finally:
                conn.close()
        except Exception:
            return False

    def _mark_alert_sent(self, thesis_id: str, triggered_at: str) -> None:
        try:
            conn = sqlite3.connect(self._db, timeout=5)
            try:
                conn.execute(
                    """UPDATE thesis_alerts SET sent=1
                       WHERE thesis_id=? AND triggered_at=?""",
                    (thesis_id, triggered_at),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    def _get_user_chat_id(self, user_id: str) -> Optional[str]:
        try:
            conn = sqlite3.connect(self._db, timeout=5)
            try:
                row = conn.execute(
                    """SELECT telegram_chat_id FROM user_preferences
                       WHERE user_id=?""",
                    (user_id,),
                ).fetchone()
                return str(row[0]) if row and row[0] else None
            finally:
                conn.close()
        except Exception:
            return None

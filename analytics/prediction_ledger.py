"""
analytics/prediction_ledger.py — Prediction Ledger

Records structured predictions at the moment a position is entered, alongside
the KB state commitment (Merkle root) that proves what the KB contained when
the decision was made.

PREDICTION_LEDGER TABLE
=======================
    prediction_ledger (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT NOT NULL,
        pattern_type    TEXT NOT NULL,
        timeframe       TEXT,
        entry_price     REAL,
        target_1        REAL,
        target_2        REAL,
        stop_loss       REAL,
        p_hit_t1        REAL,          -- predicted probability of hitting target 1
        p_hit_t2        REAL,          -- predicted probability of hitting target 2
        p_stopped_out   REAL,          -- predicted probability of stop-out
        market_regime   TEXT,
        conviction_tier TEXT,
        issued_at       TEXT NOT NULL,  -- ISO-8601 UTC
        expires_at      TEXT,
        source          TEXT DEFAULT 'system',
        outcome         TEXT,           -- hit_t1 | hit_t2 | stopped_out | expired | pending
        resolved_at     TEXT,
        kb_root         TEXT,           -- 64-char hex SHA-256 Merkle root
        kb_fact_ids     TEXT            -- JSON array of fact IDs in snapshot
    )

USAGE
=====
    from analytics.prediction_ledger import PredictionLedger

    ledger = PredictionLedger(db_path)
    ledger.record_prediction(
        ticker='NVDA', pattern_type='fvg', entry_price=890.0,
        target_1=920.0, stop_loss=875.0,
        kb_root='ab12cd...', kb_fact_ids='[1,2,3]',
    )

    # Query:
    rows = ledger.get_predictions(ticker='NVDA', pending_only=True)
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

_logger = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_PREDICTION_LEDGER = """
CREATE TABLE IF NOT EXISTS prediction_ledger (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    pattern_type    TEXT NOT NULL,
    timeframe       TEXT,
    entry_price     REAL,
    target_1        REAL,
    target_2        REAL,
    stop_loss       REAL,
    p_hit_t1        REAL,
    p_hit_t2        REAL,
    p_stopped_out   REAL,
    market_regime   TEXT,
    conviction_tier TEXT,
    issued_at       TEXT NOT NULL,
    expires_at      TEXT,
    source          TEXT DEFAULT 'system',
    outcome         TEXT DEFAULT 'pending',
    resolved_at     TEXT,
    kb_root         TEXT,
    kb_fact_ids     TEXT
)
"""

_IDX_TICKER   = "CREATE INDEX IF NOT EXISTS idx_pl_ticker ON prediction_ledger(ticker)"
_IDX_ISSUED   = "CREATE INDEX IF NOT EXISTS idx_pl_issued ON prediction_ledger(issued_at)"
_IDX_OUTCOME  = "CREATE INDEX IF NOT EXISTS idx_pl_outcome ON prediction_ledger(outcome)"

# Columns that may be added via migration on older DBs
_MIGRATION_COLS = [
    ('kb_root',     'TEXT'),
    ('kb_fact_ids', 'TEXT'),
]


def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create the prediction_ledger table and indexes if absent.
    Idempotent column additions for KB provenance on existing DBs."""
    conn.execute(_CREATE_PREDICTION_LEDGER)
    conn.execute(_IDX_TICKER)
    conn.execute(_IDX_ISSUED)
    conn.execute(_IDX_OUTCOME)

    # Idempotent migration — add kb_root / kb_fact_ids if missing
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(prediction_ledger)").fetchall()
    }
    for col, defn in _MIGRATION_COLS:
        if col not in existing:
            conn.execute(f"ALTER TABLE prediction_ledger ADD COLUMN {col} {defn}")

    conn.commit()


# ── Ledger class ──────────────────────────────────────────────────────────────

class PredictionLedger:
    """Records and queries structured predictions with KB provenance."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        conn = sqlite3.connect(db_path, timeout=10)
        try:
            _ensure_table(conn)
        finally:
            conn.close()

    def record_prediction(
        self,
        ticker:          str,
        pattern_type:    str,
        timeframe:       Optional[str] = None,
        entry_price:     Optional[float] = None,
        target_1:        Optional[float] = None,
        target_2:        Optional[float] = None,
        stop_loss:       Optional[float] = None,
        p_hit_t1:        Optional[float] = None,
        p_hit_t2:        Optional[float] = None,
        p_stopped_out:   Optional[float] = None,
        market_regime:   Optional[str] = None,
        conviction_tier: Optional[str] = None,
        issued_at:       Optional[str] = None,
        expires_at:      Optional[str] = None,
        source:          str = 'system',
        kb_root:         Optional[str] = None,
        kb_fact_ids:     Optional[str] = None,
        conn:            Optional[sqlite3.Connection] = None,
    ) -> bool:
        """
        Insert a prediction row. Returns True on success, False on duplicate
        or error.
        """
        if issued_at is None:
            issued_at = datetime.now(timezone.utc).isoformat()

        _own = conn is None
        _conn = sqlite3.connect(self.db_path, timeout=10) if _own else conn

        try:
            _conn.execute(
                """INSERT OR IGNORE INTO prediction_ledger
                   (ticker, pattern_type, timeframe, entry_price,
                    target_1, target_2, stop_loss,
                    p_hit_t1, p_hit_t2, p_stopped_out,
                    market_regime, conviction_tier,
                    issued_at, expires_at, source,
                    kb_root, kb_fact_ids)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ticker.upper(), pattern_type, timeframe, entry_price,
                    target_1, target_2, stop_loss,
                    p_hit_t1, p_hit_t2, p_stopped_out,
                    market_regime, conviction_tier,
                    issued_at, expires_at, source,
                    kb_root, kb_fact_ids,
                ),
            )
            if _own:
                _conn.commit()
            return True
        except Exception as e:
            _logger.error('record_prediction failed: %s', e)
            return False
        finally:
            if _own:
                _conn.close()

    def resolve_prediction(
        self,
        prediction_id: int,
        outcome: str,
        conn: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Mark a prediction as resolved with an outcome."""
        _VALID = {'hit_t1', 'hit_t2', 'stopped_out', 'expired', 'pending'}
        if outcome not in _VALID:
            _logger.warning('invalid outcome %s, must be one of %s', outcome, _VALID)
            return False

        resolved_at = datetime.now(timezone.utc).isoformat()
        _own = conn is None
        _conn = sqlite3.connect(self.db_path, timeout=10) if _own else conn

        try:
            _conn.execute(
                "UPDATE prediction_ledger SET outcome = ?, resolved_at = ? WHERE id = ?",
                (outcome, resolved_at, prediction_id),
            )
            if _own:
                _conn.commit()
            return True
        except Exception as e:
            _logger.error('resolve_prediction failed: %s', e)
            return False
        finally:
            if _own:
                _conn.close()

    def get_predictions(
        self,
        ticker: Optional[str] = None,
        pending_only: bool = False,
        limit: int = 100,
    ) -> List[Dict]:
        """Query prediction rows, optionally filtered by ticker and/or pending status."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            clauses = []
            params: list = []
            if ticker:
                clauses.append("ticker = ?")
                params.append(ticker.upper())
            if pending_only:
                clauses.append("outcome = 'pending'")

            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = conn.execute(
                f"SELECT * FROM prediction_ledger {where} ORDER BY issued_at DESC LIMIT ?",
                params + [limit],
            ).fetchall()

            cols = [d[0] for d in conn.execute(
                "SELECT * FROM prediction_ledger LIMIT 0"
            ).description]

            return [dict(zip(cols, r)) for r in rows]
        finally:
            conn.close()

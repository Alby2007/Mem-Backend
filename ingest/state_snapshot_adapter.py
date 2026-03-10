"""
ingest/state_snapshot_adapter.py — Full Market State Snapshot Adapter

Captures a complete state vector for every watchlist ticker + one global
state blob every 6 hours. Snapshots are written to market_state_snapshots
and form the searchable history used by analytics/temporal_search.py.

Registered in api_v2.py:
    scheduler.register(StateSnapshotAdapter(db_path=db_path), interval_sec=21600)

Schema
------
    market_state_snapshots(id, snapshot_at, scope, subject, state_json)
    UNIQUE(snapshot_at, scope, subject) — idempotent on rerun.

Performance
-----------
    ~167 tickers × 1 query each + 1 global query ≈ < 5s per cycle.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ingest.base import BaseIngestAdapter, RawAtom, db_connect

_logger = logging.getLogger(__name__)

# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS market_state_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_at TEXT NOT NULL,
    scope       TEXT NOT NULL,
    subject     TEXT NOT NULL,
    state_json  TEXT NOT NULL,
    UNIQUE(snapshot_at, scope, subject)
);
"""

_CREATE_IDX_SUBJECT = """
CREATE INDEX IF NOT EXISTS idx_snapshots_subject
ON market_state_snapshots(subject, snapshot_at);
"""

_CREATE_IDX_TIME = """
CREATE INDEX IF NOT EXISTS idx_snapshots_time
ON market_state_snapshots(snapshot_at);
"""

# ── GDELT tension bucketing ─────────────────────────────────────────────────────

def _bucket_gdelt(val: Optional[str]) -> str:
    if not val:
        return 'unknown'
    try:
        v = float(val)
        if v < 15:
            return 'low'
        if v < 30:
            return 'medium'
        return 'high'
    except Exception:
        return str(val).lower()


def _bucket_gpr(val: Optional[str]) -> str:
    if not val:
        return 'unknown'
    try:
        v = float(val)
        if v < 80:
            return 'low'
        if v < 150:
            return 'moderate'
        return 'elevated'
    except Exception:
        return str(val).lower()


def ensure_snapshot_table(conn: sqlite3.Connection) -> None:
    """Create market_state_snapshots table and indexes if they don't exist."""
    conn.execute(_CREATE_SNAPSHOTS)
    conn.execute(_CREATE_IDX_SUBJECT)
    conn.execute(_CREATE_IDX_TIME)
    conn.commit()


# ── State readers ───────────────────────────────────────────────────────────────

def _read_ticker_state(conn: sqlite3.Connection, ticker: str) -> dict:
    """
    Read the current KB state for a single ticker.
    Returns a dict of the state vector. Empty dict = no meaningful data.
    """
    rows = conn.execute(
        """SELECT predicate, object, confidence
           FROM facts
           WHERE subject = ? AND confidence > 0.3
           ORDER BY confidence DESC, timestamp DESC""",
        (ticker,),
    ).fetchall()

    state: dict = {}
    # Track which predicates we've captured (first = highest confidence)
    seen: set = set()

    _WANTED = {
        'signal_direction', 'conviction_tier', 'price_regime',
        'volatility_regime', 'sector', 'last_price', 'price_target',
        'upside_pct', 'macro_confirmation', 'pattern_type',
        'calibration_boost', 'signal_quality', 'market_regime',
    }

    pattern_types: List[str] = []
    for predicate, obj, _conf in rows:
        pred = predicate.lower().strip()
        val  = (obj or '').strip()
        if not val:
            continue
        if pred == 'pattern_type':
            if val not in pattern_types:
                pattern_types.append(val)
            continue
        if pred not in seen and pred in _WANTED:
            seen.add(pred)
            # Normalise numeric fields
            if pred in ('last_price', 'price_target', 'upside_pct', 'calibration_boost'):
                try:
                    state[pred] = round(float(val), 4)
                except Exception:
                    state[pred] = val
            else:
                state[pred] = val.lower()

    if pattern_types:
        state['pattern_types_active'] = pattern_types[:5]

    return state


def _read_global_state(conn: sqlite3.Connection) -> dict:
    """
    Read the current global market state from KB atoms.
    Covers regime, central bank, GDELT, GPR, Polymarket probabilities.
    """
    state: dict = {}

    # Regime label
    _REGIME_SUBJECTS = ['market', 'global_macro_regime', 'us_macro']
    _REGIME_PREDS    = ['regime_label', 'market_regime', 'price_regime']
    for subj in _REGIME_SUBJECTS:
        for pred in _REGIME_PREDS:
            row = conn.execute(
                "SELECT object FROM facts WHERE subject=? AND predicate=? ORDER BY timestamp DESC LIMIT 1",
                (subj, pred),
            ).fetchone()
            if row:
                state['regime_label'] = row[0].lower().strip()
                break
        if 'regime_label' in state:
            break

    # Central bank stance
    for subj in ['fed', 'ecb', 'boe', 'us_macro', 'global_macro_regime']:
        row = conn.execute(
            "SELECT object FROM facts WHERE subject=? AND predicate='central_bank_stance' ORDER BY timestamp DESC LIMIT 1",
            (subj,),
        ).fetchone()
        if row:
            state['central_bank_stance'] = row[0].lower().strip()
            break

    # GDELT tension
    row = conn.execute(
        "SELECT object FROM facts WHERE predicate IN ('gdelt_tension_level','gdelt_tension') ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    if row:
        state['gdelt_tension_level'] = _bucket_gdelt(row[0])

    # GPR
    row = conn.execute(
        "SELECT object FROM facts WHERE predicate IN ('gpr_level','gpr_index','geopolitical_risk_index') ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    if row:
        state['gpr_level'] = _bucket_gpr(row[0])
        try:
            state['gpr_index'] = round(float(row[0]), 1)
        except Exception:
            pass

    # Polymarket probabilities
    for pred, key in [
        ('fed_rate_cut_prob',   'fed_rate_cut_prob'),
        ('us_recession_prob',   'us_recession_prob'),
        ('rate_cut_probability', 'fed_rate_cut_prob'),
    ]:
        if key not in state:
            row = conn.execute(
                "SELECT object FROM facts WHERE predicate=? ORDER BY timestamp DESC LIMIT 1",
                (pred,),
            ).fetchone()
            if row:
                try:
                    state[key] = round(float(row[0]), 4)
                except Exception:
                    pass

    # KB stress + counts
    try:
        state['kb_facts_count'] = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    except Exception:
        pass

    try:
        state['active_patterns_count'] = conn.execute(
            "SELECT COUNT(*) FROM facts WHERE predicate='pattern_type'"
        ).fetchone()[0]
    except Exception:
        pass

    # Volatility regime (global)
    row = conn.execute(
        "SELECT object FROM facts WHERE predicate IN ('volatility_regime','market_volatility','vix_regime') ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    if row:
        state['volatility_regime'] = row[0].lower().strip()

    # Top bullish / bearish sectors
    try:
        bull_rows = conn.execute(
            """SELECT sector, COUNT(*) as c FROM (
                SELECT object as sector FROM facts WHERE predicate='sector' AND subject IN (
                    SELECT subject FROM facts WHERE predicate='signal_direction' AND object IN ('bullish','long')
                )
            ) GROUP BY sector ORDER BY c DESC LIMIT 3"""
        ).fetchall()
        state['top_sectors_bullish'] = [r[0].lower() for r in bull_rows if r[0]]
    except Exception:
        pass

    try:
        bear_rows = conn.execute(
            """SELECT sector, COUNT(*) as c FROM (
                SELECT object as sector FROM facts WHERE predicate='sector' AND subject IN (
                    SELECT subject FROM facts WHERE predicate='signal_direction' AND object IN ('bearish','short')
                )
            ) GROUP BY sector ORDER BY c DESC LIMIT 3"""
        ).fetchall()
        state['top_sectors_bearish'] = [r[0].lower() for r in bear_rows if r[0]]
    except Exception:
        pass

    return state


def _write_snapshots(
    conn: sqlite3.Connection,
    snapshot_at: str,
    ticker_states: Dict[str, dict],
    global_state: dict,
) -> int:
    """
    Bulk-insert ticker + global snapshots.
    Returns number of rows written (INSERT OR IGNORE — skips duplicates).
    """
    rows = []
    for ticker, state in ticker_states.items():
        if state:
            rows.append((snapshot_at, 'ticker', ticker, json.dumps(state, separators=(',', ':'))))
    if global_state:
        rows.append((snapshot_at, 'global', 'market', json.dumps(global_state, separators=(',', ':'))))

    conn.executemany(
        "INSERT OR IGNORE INTO market_state_snapshots(snapshot_at, scope, subject, state_json) VALUES(?,?,?,?)",
        rows,
    )
    conn.commit()
    return len(rows)


# ── Adapter ─────────────────────────────────────────────────────────────────────

class StateSnapshotAdapter(BaseIngestAdapter):
    """
    Captures full market state vectors for all watchlist tickers + global state.
    Runs every 6 hours (21600s). Writes to market_state_snapshots table.

    Produces a single summary RawAtom for the scheduler's tracking purposes.
    """

    name = 'state_snapshot'

    def __init__(self, db_path: str):
        super().__init__(name=self.name)
        self._db_path = db_path

    def _get_watchlist(self, conn: sqlite3.Connection) -> List[str]:
        """Return the active watchlist tickers."""
        try:
            from ingest.dynamic_watchlist import DynamicWatchlistManager
            tickers = DynamicWatchlistManager.get_pattern_tickers(self._db_path)
            if tickers:
                return tickers
        except Exception:
            pass
        # Fallback: distinct subjects with signal_direction atoms
        rows = conn.execute(
            """SELECT DISTINCT subject FROM facts
               WHERE predicate='signal_direction' AND subject NOT LIKE '%:%'
               AND LENGTH(subject) <= 10
               ORDER BY subject"""
        ).fetchall()
        return [r[0] for r in rows]

    def fetch(self) -> List[RawAtom]:
        conn = db_connect(self._db_path)
        try:
            ensure_snapshot_table(conn)

            snapshot_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:00:00+00:00')

            # Check if this hour's snapshot already exists
            existing = conn.execute(
                "SELECT COUNT(*) FROM market_state_snapshots WHERE snapshot_at=? AND scope='global'",
                (snapshot_at,),
            ).fetchone()[0]
            if existing:
                _logger.info('StateSnapshotAdapter: snapshot for %s already exists — skipping', snapshot_at)
                return []

            watchlist = self._get_watchlist(conn)
            _logger.info('StateSnapshotAdapter: snapshotting %d tickers at %s', len(watchlist), snapshot_at)

            ticker_states: Dict[str, dict] = {}
            for ticker in watchlist:
                state = _read_ticker_state(conn, ticker)
                if state:
                    ticker_states[ticker] = state

            global_state = _read_global_state(conn)

            written = _write_snapshots(conn, snapshot_at, ticker_states, global_state)
            _logger.info(
                'StateSnapshotAdapter: wrote %d snapshot rows (%d tickers, 1 global)',
                written, len(ticker_states),
            )

            return [
                RawAtom(
                    subject   = 'system',
                    predicate = 'state_snapshot_last_run',
                    object    = snapshot_at,
                    confidence= 1.0,
                    source    = 'state_snapshot_adapter',
                    metadata  = {
                        'tickers_snapped': str(len(ticker_states)),
                        'rows_written':    str(written),
                    },
                    upsert = True,
                )
            ]

        except Exception as exc:
            _logger.error('StateSnapshotAdapter.fetch() failed: %s', exc)
            return []
        finally:
            conn.close()

    def transform(self, atoms: List[RawAtom]) -> List[RawAtom]:
        return atoms

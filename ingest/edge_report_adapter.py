"""
ingest/edge_report_adapter.py — Daily Edge Report Job

Runs daily (86400s interval). Computes and stores:
1. Z-scores for all pattern+direction combinations (n>=5 unique patterns)
2. New calibration edges (HR>=0.60, samples>=2000)
3. Performance delta since last report (WR trending up/down per pattern_type)

Writes results as KB facts readable by Oracle bot, tip scheduler, FORGE screen.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import datetime, timezone
from typing import List

_log = logging.getLogger(__name__)

_DDL_PATTERN_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS pattern_performance (
    pattern_id      TEXT PRIMARY KEY,
    ticker          TEXT,
    pattern_type    TEXT,
    direction       TEXT,
    timeframe       TEXT,
    sector          TEXT,
    kb_conviction   TEXT,
    market_regime   TEXT,
    quality_score   REAL,
    first_entry_at  TEXT,
    outcome         TEXT,
    avg_pnl_r       REAL,
    bot_count       INTEGER DEFAULT 1,
    hold_hours      REAL,
    updated_at      TEXT
)
"""
_DDL_PP_IDX1 = "CREATE INDEX IF NOT EXISTS idx_pp_pattern ON pattern_performance(pattern_type, direction)"
_DDL_PP_IDX2 = "CREATE INDEX IF NOT EXISTS idx_pp_sector  ON pattern_performance(sector, pattern_type)"


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_PATTERN_PERFORMANCE)
    conn.execute(_DDL_PP_IDX1)
    conn.execute(_DDL_PP_IDX2)
    conn.commit()


def _compute_z_score(wins: int, n: int, prior: float = 0.50) -> float:
    """One-sample proportion z-score vs prior WR."""
    if n < 1:
        return 0.0
    p = wins / n
    se = math.sqrt(prior * (1 - prior) / n)
    if se == 0:
        return 0.0
    return round((p - prior) / se, 3)


def _sync_pattern_performance(conn: sqlite3.Connection) -> int:
    """
    Upsert closed paper_positions into pattern_performance.
    One row per unique pattern_signals.id (pattern_id).
    Returns number of rows upserted.
    """
    rows = conn.execute(
        """SELECT
               p.pattern_id,
               p.ticker,
               pp.pattern_type,
               pp.direction,
               pp.timeframe,
               pp.kb_conviction,
               pp.kb_regime       AS market_regime,
               pp.quality_score,
               MIN(p.opened_at)   AS first_entry_at,
               p.status           AS outcome,
               AVG(p.pnl_r)       AS avg_pnl_r,
               COUNT(*)           AS bot_count,
               AVG(
                   CASE WHEN p.closed_at IS NOT NULL AND p.opened_at IS NOT NULL
                        THEN (julianday(p.closed_at) - julianday(p.opened_at)) * 24
                        ELSE NULL END
               ) AS hold_hours
           FROM paper_positions p
           LEFT JOIN pattern_signals pp ON pp.id = CAST(p.pattern_id AS INTEGER)
           WHERE p.status IN ('t1_hit','t2_hit','stopped_out','closed')
             AND p.pattern_id IS NOT NULL
             AND p.pnl_r IS NOT NULL
           GROUP BY p.pattern_id""",
    ).fetchall()

    upserted = 0
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        (pattern_id, ticker, pattern_type, direction, timeframe,
         kb_conviction, market_regime, quality_score,
         first_entry_at, outcome, avg_pnl_r, bot_count, hold_hours) = row

        # Resolve sector from facts table if available
        sector = None
        if ticker:
            sec_row = conn.execute(
                "SELECT object FROM facts WHERE LOWER(subject)=LOWER(?) AND predicate='sector' LIMIT 1",
                (ticker,),
            ).fetchone()
            if sec_row:
                sector = sec_row[0]

        conn.execute(
            """INSERT INTO pattern_performance
               (pattern_id, ticker, pattern_type, direction, timeframe, sector,
                kb_conviction, market_regime, quality_score,
                first_entry_at, outcome, avg_pnl_r, bot_count, hold_hours, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(pattern_id) DO UPDATE SET
                   outcome=excluded.outcome,
                   avg_pnl_r=excluded.avg_pnl_r,
                   bot_count=excluded.bot_count,
                   hold_hours=excluded.hold_hours,
                   updated_at=excluded.updated_at""",
            (pattern_id, ticker, pattern_type, direction, timeframe, sector,
             kb_conviction, market_regime, quality_score,
             first_entry_at, outcome, avg_pnl_r, bot_count, hold_hours, now),
        )
        upserted += 1

    conn.commit()
    return upserted


def _compute_edge_report(conn: sqlite3.Connection) -> List[dict]:
    """
    Compute z-scores and WR for all (pattern_type, direction) slices with n>=5.
    Returns list of dicts to be written as KB facts.
    """
    rows = conn.execute(
        """SELECT
               pattern_type,
               direction,
               COUNT(*) AS n,
               SUM(CASE WHEN avg_pnl_r > 0 THEN 1 ELSE 0 END) AS wins,
               AVG(avg_pnl_r) AS avg_r
           FROM pattern_performance
           WHERE outcome IN ('t1_hit','t2_hit','stopped_out','closed')
           GROUP BY pattern_type, direction
           HAVING n >= 5
           ORDER BY n DESC""",
    ).fetchall()

    results = []
    for (pattern_type, direction, n, wins, avg_r) in rows:
        wr = round(wins / n, 4) if n else 0.0
        z  = _compute_z_score(wins, n, prior=0.50)
        slug = f"{pattern_type}_{direction}"
        results.append({
            'slug':         slug,
            'pattern_type': pattern_type,
            'direction':    direction,
            'n':            n,
            'win_rate':     wr,
            'avg_r':        round(float(avg_r or 0), 3),
            'z_score':      z,
        })
    return results


def _write_kb_facts(conn: sqlite3.Connection, report: List[dict]) -> None:
    """Write edge report as KB facts in the facts table."""
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # System-level report date
    conn.execute(
        "INSERT OR REPLACE INTO facts (subject, predicate, object, confidence, source, timestamp) "
        "VALUES ('system','edge_report_date',?,1.0,'edge_report_adapter',?)",
        (today, now),
    )

    for r in report:
        slug = r['slug']
        meta = json.dumps({'n': r['n'], 'avg_r': r['avg_r'], 'direction': r['direction'],
                           'pattern_type': r['pattern_type']})
        for predicate, value in [
            ('live_z_score',  str(r['z_score'])),
            ('live_wr',       str(r['win_rate'])),
            ('live_n_unique', str(r['n'])),
        ]:
            conn.execute(
                """INSERT OR REPLACE INTO facts
                   (subject, predicate, object, confidence, source, timestamp, metadata)
                   VALUES (?,?,?,?,?,?,?)""",
                (slug, predicate, value, 1.0, 'edge_report_adapter', now, meta),
            )

    conn.commit()


class EdgeReportAdapter:
    name = 'edge_report_adapter'

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def fetch(self):
        """Run the full edge report pipeline. Called by the ingest scheduler."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            _ensure_tables(conn)

            synced = _sync_pattern_performance(conn)
            _log.info('EdgeReportAdapter: synced %d pattern_performance rows', synced)

            report = _compute_edge_report(conn)
            _log.info('EdgeReportAdapter: computed %d edge slices', len(report))

            _write_kb_facts(conn, report)
            conn.close()

            _log.info('EdgeReportAdapter: done — %d z-scores written to KB', len(report))
        except Exception as e:
            _log.error('EdgeReportAdapter: failed: %s', e)
        return []  # IngestScheduler expects list[RawAtom] or []

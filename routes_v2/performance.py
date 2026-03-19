"""
routes_v2/performance.py — Pattern performance analytics endpoints.

Endpoints:
    GET  /users/{user_id}/performance/patterns  — deduplicated trade outcomes
    GET  /mcp/tools/edge_miner/scan             — ranked calibration edge candidates
    GET  /mcp/tools/performance/edge_report     — z-scores + WR per pattern+direction
"""

from __future__ import annotations

import math
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

import extensions as ext
from middleware.fastapi_auth import get_current_user, user_path_auth

router = APIRouter()

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


def _ensure_pp_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_PATTERN_PERFORMANCE)
    conn.execute(_DDL_PP_IDX1)
    conn.execute(_DDL_PP_IDX2)


def compute_z_score(
    pattern_type: str,
    direction: str,
    sector: Optional[str] = None,
    timeframe: Optional[str] = None,
    db_path: str = None,
    prior: float = 0.50,
) -> dict:
    """
    Compute current z-score for any slice of pattern_performance.
    Returns dict with n, win_rate, z_score, avg_r.
    """
    db_path = db_path or ext.DB_PATH
    conn = sqlite3.connect(db_path, timeout=10)
    _ensure_pp_table(conn)

    clauses = [
        "outcome IN ('t1_hit','t2_hit','stopped_out','closed')",
        "LOWER(pattern_type) = LOWER(?)",
        "LOWER(direction) = LOWER(?)",
    ]
    params = [pattern_type, direction]
    if sector:
        clauses.append("LOWER(sector) = LOWER(?)")
        params.append(sector)
    if timeframe:
        clauses.append("timeframe = ?")
        params.append(timeframe)

    where = " AND ".join(clauses)
    row = conn.execute(
        f"""SELECT COUNT(*) AS n,
               SUM(CASE WHEN avg_pnl_r > 0 THEN 1 ELSE 0 END) AS wins,
               AVG(avg_pnl_r) AS avg_r
           FROM pattern_performance WHERE {where}""",
        params,
    ).fetchone()
    conn.close()

    n    = row[0] or 0
    wins = row[1] or 0
    avg_r = round(float(row[2] or 0), 3)
    wr   = round(wins / n, 4) if n > 0 else 0.0
    se   = math.sqrt(prior * (1 - prior) / n) if n > 0 else 0.0
    z    = round((wr - prior) / se, 3) if se > 0 else 0.0

    return {'n': n, 'win_rate': wr, 'z_score': z, 'avg_r': avg_r}


# ── GET /users/{user_id}/performance/patterns ──────────────────────────────────

@router.get("/users/{user_id}/performance/patterns")
async def get_pattern_performance(
    user_id: str,
    pattern_type: Optional[str] = Query(None),
    direction:    Optional[str] = Query(None),
    sector:       Optional[str] = Query(None),
    timeframe:    Optional[str] = Query(None),
    conviction:   Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: str = Depends(user_path_auth),
):
    """
    Return deduplicated trade outcomes from pattern_performance.
    One row per unique pattern_id — honest z-score foundation.
    Supports filtering by pattern_type, direction, sector, timeframe, conviction.
    """
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    _ensure_pp_table(conn)

    clauses = []
    params: list = []

    if pattern_type:
        clauses.append("LOWER(pattern_type) = LOWER(?)")
        params.append(pattern_type)
    if direction:
        clauses.append("LOWER(direction) = LOWER(?)")
        params.append(direction)
    if sector:
        clauses.append("LOWER(sector) = LOWER(?)")
        params.append(sector)
    if timeframe:
        clauses.append("timeframe = ?")
        params.append(timeframe)
    if conviction:
        clauses.append("LOWER(kb_conviction) = LOWER(?)")
        params.append(conviction)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    try:
        rows = conn.execute(
            f"""SELECT pattern_id, ticker, pattern_type, direction, timeframe,
                       sector, kb_conviction, market_regime, quality_score,
                       first_entry_at, outcome, avg_pnl_r, bot_count, hold_hours, updated_at
                FROM pattern_performance
                {where}
                ORDER BY first_entry_at DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

        total = conn.execute(
            f"SELECT COUNT(*) FROM pattern_performance {where}", params
        ).fetchone()[0]

        # Aggregate z-scores per slice
        agg_rows = conn.execute(
            f"""SELECT pattern_type, direction,
                       COUNT(*) AS n,
                       SUM(CASE WHEN avg_pnl_r > 0 THEN 1 ELSE 0 END) AS wins,
                       AVG(avg_pnl_r) AS avg_r
                FROM pattern_performance
                {where}
                WHERE outcome IN ('t1_hit','t2_hit','stopped_out','closed')
                GROUP BY pattern_type, direction
                HAVING n >= 5""",
            params,
        ).fetchall()
        conn.close()
    except Exception as e:
        conn.close()
        raise HTTPException(500, detail=str(e))

    def _z(wins, n, prior=0.50):
        if n < 1: return 0.0
        wr = wins / n
        se = math.sqrt(prior * (1 - prior) / n)
        return round((wr - prior) / se, 3) if se > 0 else 0.0

    aggregates = [
        {
            'pattern_type': r[0],
            'direction':    r[1],
            'n':            r[2],
            'win_rate':     round(r[3] / r[2], 4) if r[2] else 0.0,
            'avg_r':        round(float(r[4] or 0), 3),
            'z_score':      _z(r[3], r[2]),
        }
        for r in agg_rows
    ]

    return {
        'total':      total,
        'limit':      limit,
        'offset':     offset,
        'aggregates': aggregates,
        'rows': [
            {
                'pattern_id':    r[0],
                'ticker':        r[1],
                'pattern_type':  r[2],
                'direction':     r[3],
                'timeframe':     r[4],
                'sector':        r[5],
                'kb_conviction': r[6],
                'market_regime': r[7],
                'quality_score': r[8],
                'first_entry_at': r[9],
                'outcome':       r[10],
                'avg_pnl_r':     r[11],
                'bot_count':     r[12],
                'hold_hours':    r[13],
                'updated_at':    r[14],
            }
            for r in rows
        ],
    }


# ── GET /mcp/tools/edge_miner/scan ────────────────────────────────────────────

@router.get("/mcp/tools/edge_miner/scan")
async def mcp_edge_miner_scan(
    min_hr: float = Query(0.60, ge=0.50, le=1.0),
    min_samples: int = Query(2000, ge=100),
    min_tickers: int = Query(2, ge=1),
    _: str = Depends(get_current_user),
):
    """
    Return ranked calibration edge candidates.
    Each candidate includes a ready-to-use bot genome.
    already_covered=true means an active bot already targets this cell.
    """
    try:
        from analytics.edge_miner import scan_calibration_edges
        candidates = scan_calibration_edges(
            min_hr=min_hr,
            min_samples=min_samples,
            min_tickers=min_tickers,
            db_path=ext.DB_PATH,
        )
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    return {
        'total':           len(candidates),
        'uncovered_count': sum(1 for c in candidates if not c.already_covered),
        'candidates': [
            {
                'sector':          c.sector,
                'pattern_type':    c.pattern_type,
                'timeframe':       c.timeframe,
                'avg_hr':          c.avg_hr,
                'samples':         c.samples,
                'tickers':         c.tickers,
                'avg_stop_rate':   c.avg_stop_rate,
                'already_covered': c.already_covered,
                'genome':          c.genome,
            }
            for c in candidates
        ],
    }


# ── GET /mcp/tools/edge_miner/exchange_scan ───────────────────────────────────

@router.get("/mcp/tools/edge_miner/exchange_scan")
async def mcp_exchange_scan(
    min_gap: float = Query(0.22, ge=0.05, le=1.0),
    min_samples: int = Query(5000, ge=100),
    min_tickers: int = Query(5, ge=1),
    _: str = Depends(get_current_user),
):
    """
    Return ranked exchange×pattern×timeframe edges by edge_gap (HR - stop_rate).
    already_covered=true means an active bot already targets this exchange+pattern+tf.
    """
    try:
        from analytics.edge_miner import scan_exchange_edges
        candidates = scan_exchange_edges(
            min_gap=min_gap,
            min_samples=min_samples,
            min_tickers=min_tickers,
            db_path=ext.DB_PATH,
        )
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    return {
        'total':           len(candidates),
        'uncovered_count': sum(1 for c in candidates if not c.already_covered),
        'candidates': [
            {
                'exchange_suffix': c.exchange_suffix,
                'pattern_type':   c.pattern_type,
                'timeframe':      c.timeframe,
                'avg_hr':         c.avg_hr,
                'avg_stop':       c.avg_stop,
                'edge_gap':       c.edge_gap,
                'samples':        c.samples,
                'tickers':        c.tickers,
                'already_covered': c.already_covered,
            }
            for c in candidates
        ],
    }


# ── GET /mcp/tools/performance/edge_report ────────────────────────────────────

@router.get("/mcp/tools/performance/edge_report")
async def mcp_edge_report(_: str = Depends(get_current_user)):
    """
    Return z-scores, win rates, and avg_R for all pattern+direction slices with n>=5.
    Called by the Observatory, tip scheduler, and FORGE screen.
    """
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    _ensure_pp_table(conn)

    try:
        rows = conn.execute(
            """SELECT
                   pattern_type,
                   direction,
                   COUNT(*) AS n,
                   SUM(CASE WHEN avg_pnl_r > 0 THEN 1 ELSE 0 END) AS wins,
                   AVG(avg_pnl_r) AS avg_r,
                   AVG(quality_score) AS avg_quality
               FROM pattern_performance
               WHERE outcome IN ('t1_hit','t2_hit','stopped_out','closed')
               GROUP BY pattern_type, direction
               HAVING n >= 5
               ORDER BY n DESC""",
        ).fetchall()
        conn.close()
    except Exception as e:
        conn.close()
        raise HTTPException(500, detail=str(e))

    def _z(wins, n, prior=0.50):
        if n < 1: return 0.0
        wr = wins / n
        se = math.sqrt(prior * (1 - prior) / n)
        return round((wr - prior) / se, 3) if se > 0 else 0.0

    edges = []
    for (pt, direction, n, wins, avg_r, avg_q) in rows:
        wr = round(wins / n, 4) if n else 0.0
        z  = _z(wins, n)
        edges.append({
            'pattern_type': pt,
            'direction':    direction,
            'n':            n,
            'win_rate':     wr,
            'avg_r':        round(float(avg_r or 0), 3),
            'avg_quality':  round(float(avg_q or 0), 3),
            'z_score':      z,
            'edge_confirmed': z >= 1.65 and n >= 10,
        })

    confirmed = [e for e in edges if e['edge_confirmed']]
    return {
        'slices':             edges,
        'confirmed_edges':    confirmed,
        'confirmed_count':    len(confirmed),
        'total_slices':       len(edges),
    }

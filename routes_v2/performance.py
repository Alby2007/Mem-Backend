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

@router.get("/mcp/tools/edge_miner/regime_split_scan")
async def regime_split_scan(
    min_null_hr: float = 0.65,
    min_null_n:  int   = 200,
    min_hr_lift: float = 0.15,
    _: str = Depends(get_current_user),
):
    """
    Find edges hidden by regime mixing in aggregate calibration.
    Returns (ticker, pattern, tf) cells where NULL-regime HR >> aggregate HR.
    These are edges that EdgeMiner misses because bad-regime cells drag the average.
    """
    from analytics.edge_miner import scan_regime_split_edges
    candidates = scan_regime_split_edges(
        min_null_hr=min_null_hr,
        min_null_n=min_null_n,
        min_hr_lift=min_hr_lift,
        db_path=ext.DB_PATH,
    )
    return {
        "count": len(candidates),
        "params": {"min_null_hr": min_null_hr, "min_null_n": min_null_n, "min_hr_lift": min_hr_lift},
        "candidates": [
            {
                "ticker":          c.ticker,
                "pattern_type":    c.pattern_type,
                "timeframe":       c.timeframe,
                "sector":          c.sector,
                "null_regime_hr":  c.null_regime_hr,
                "null_regime_n":   c.null_regime_n,
                "aggregate_hr":    c.aggregate_hr,
                "hr_lift":         c.hr_lift,
                "already_covered": c.already_covered,
            }
            for c in candidates
        ],
    }

# ── GET /mcp/tools/performance/direction_split ────────────────────────────────

@router.get("/mcp/tools/performance/direction_split")
async def mcp_direction_split(
    pattern_type: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    min_n: int = Query(10, ge=1),
    _: str = Depends(get_current_user),
):
    """
    Cross-join paper_positions direction data with aggregate calibration HR to surface
    directional calibration splits.

    Surfaces findings like: 'liq_void 15m bullish: live WR=25% despite 56% calibration'
    which reveals the aggregate is masking a bearish-only edge.

    verdict values:
    - LIVE_BEATS_CAL: live WR significantly exceeds calibration
    - CAL_BEATS_LIVE: calibration HR >> live WR (haven't traded enough yet)
    - BOTH_STRONG:    both above 60%
    - BOTH_WEAK:      both below 45%
    - DIRECTIONAL_MISMATCH: calibration undifferentiated but live data shows direction matters
    """
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        where_clauses = []
        params: list = []
        if pattern_type:
            where_clauses.append("LOWER(ps.pattern_type) = LOWER(?)")
            params.append(pattern_type)
        if timeframe:
            where_clauses.append("ps.timeframe = ?")
            params.append(timeframe)
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Group paper_positions by (pattern_type, direction, timeframe) via pattern_signals join
        pos_rows = conn.execute(
            f"""SELECT
                   LOWER(ps.pattern_type) AS pattern_type,
                   LOWER(pp.direction)    AS direction,
                   ps.timeframe,
                   COUNT(*)               AS live_n,
                   SUM(CASE WHEN pp.status IN ('t1_hit','t2_hit') THEN 1
                            WHEN pp.pnl_r > 0 THEN 1 ELSE 0 END) AS wins,
                   AVG(pp.pnl_r)          AS avg_r
               FROM paper_positions pp
               JOIN pattern_signals ps ON pp.pattern_id = ps.id
               {where_sql}
               GROUP BY LOWER(ps.pattern_type), LOWER(pp.direction), ps.timeframe
               HAVING live_n >= ?
               ORDER BY live_n DESC""",
            params + [min_n],
        ).fetchall()

        # Pull aggregate calibration HRs for these pattern+tf combos
        cal_rows = conn.execute(
            """SELECT LOWER(pattern_type), timeframe,
                      SUM(hit_rate_t1 * sample_size) / SUM(sample_size) AS wtd_hr
               FROM signal_calibration
               WHERE hit_rate_t1 IS NOT NULL AND direction IS NULL
               GROUP BY LOWER(pattern_type), timeframe"""
        ).fetchall()
        conn.close()
    except Exception as e:
        conn.close()
        raise HTTPException(500, detail=str(e))

    cal_lookup: dict[tuple[str, str], float] = {
        (pt, tf): float(hr) for (pt, tf, hr) in cal_rows
    }

    def _z(wins: int, n: int, prior: float = 0.50) -> float:
        if n < 1:
            return 0.0
        wr = wins / n
        se = math.sqrt(prior * (1 - prior) / n)
        return round((wr - prior) / se, 3) if se > 0 else 0.0

    def _verdict(live_wr: float, cal_hr: Optional[float]) -> str:
        if cal_hr is None:
            return 'NO_CALIBRATION'
        diff = live_wr - cal_hr
        if live_wr >= 0.60 and cal_hr >= 0.60:
            return 'BOTH_STRONG'
        if live_wr < 0.45 and cal_hr < 0.45:
            return 'BOTH_WEAK'
        if diff >= 0.10:
            return 'LIVE_BEATS_CAL'
        if diff <= -0.10:
            return 'CAL_BEATS_LIVE'
        return 'DIRECTIONAL_MISMATCH'

    groups = []
    for (pt, direction, tf, live_n, wins, avg_r) in pos_rows:
        wins = wins or 0
        live_wr = round(wins / live_n, 4) if live_n else 0.0
        z = _z(wins, live_n)
        cal_hr = cal_lookup.get((pt, tf or ''))
        divergence = round(live_wr - cal_hr, 4) if cal_hr is not None else None
        groups.append({
            'pattern_type':   pt,
            'direction':      direction,
            'timeframe':      tf,
            'live_n':         live_n,
            'live_wr':        live_wr,
            'live_avg_r':     round(float(avg_r or 0), 3),
            'z_score':        z,
            'calibration_hr': round(cal_hr, 4) if cal_hr is not None else None,
            'divergence':     divergence,
            'verdict':        _verdict(live_wr, cal_hr),
            'edge_confirmed': z >= 1.65 and live_n >= 10,
        })

    groups.sort(key=lambda g: g['live_wr'], reverse=True)
    return {
        'groups':          groups,
        'total_groups':    len(groups),
        'confirmed_edges': [g for g in groups if g['edge_confirmed']],
    }

# ── POST /mcp/tools/calibration/backfill_direction ───────────────────────────

@router.post("/mcp/tools/calibration/backfill_direction")
async def mcp_backfill_direction(
    min_n: int = Query(5, ge=1),
    _: str = Depends(get_current_user),
):
    """
    Backfill directional calibration cells from paper_positions outcome data.

    For each (ticker, pattern_type, timeframe) group with n >= min_n real trades
    split by direction, inserts NEW directional rows alongside existing NULL-direction
    rows using the partial unique index (direction IS NOT NULL).

    Run once after direction column is deployed. Will immediately populate directional
    cells for IFVG bearish (57 trades at 87.7% WR), revealing the true directional
    calibration the aggregate was hiding.
    """
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        inserted = _backfill_direction_cells(conn, min_n=min_n)
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    return {'status': 'ok', 'rows_inserted_or_updated': inserted}

def _backfill_direction_cells(conn: sqlite3.Connection, min_n: int = 5) -> int:
    """
    For each (ticker, pattern_type, timeframe, direction) group with n >= min_n trades
    in paper_positions (joined to pattern_signals), insert/update directional calibration
    cells using observed WR as hit_rate_t1 proxy.

    Creates NEW directional rows alongside existing NULL-direction rows via the
    partial unique index. Does not modify historical NULL cells.
    """
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()

    rows = conn.execute(
        """SELECT
               UPPER(pp.ticker)       AS ticker,
               LOWER(ps.pattern_type) AS pattern_type,
               ps.timeframe,
               LOWER(pp.direction)    AS direction,
               COUNT(*)               AS n,
               SUM(CASE WHEN pp.status IN ('t1_hit','t2_hit') THEN 1
                        WHEN pp.pnl_r > 0 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN pp.status = 'stopped_out' THEN 1
                        WHEN pp.pnl_r < 0 THEN 1 ELSE 0 END) AS stops,
               AVG(pp.pnl_r)                                  AS avg_r
           FROM paper_positions pp
           JOIN pattern_signals ps ON pp.pattern_id = ps.id
           WHERE pp.status NOT IN ('open', 'pending')
             AND pp.direction IS NOT NULL
             AND ps.pattern_type IS NOT NULL
           GROUP BY UPPER(pp.ticker), LOWER(ps.pattern_type), ps.timeframe, LOWER(pp.direction)
           HAVING n >= ?
           ORDER BY n DESC""",
        (min_n,),
    ).fetchall()

    inserted = 0
    for (ticker, pt, tf, direction, n, wins, stops, avg_r) in rows:
        wins  = wins  or 0
        stops = stops or 0
        hr1   = round(wins / n, 4)
        sor   = round(stops / n, 4)
        conf  = min(1.0, round(n / 100.0, 4))

        try:
            conn.execute(
                """INSERT INTO signal_calibration
                   (ticker, pattern_type, timeframe, market_regime, direction,
                    sample_size, hit_rate_t1, stopped_out_rate,
                    calibration_confidence, last_updated)
                   VALUES (?,?,?,NULL,?,?,?,?,?,?)
                   ON CONFLICT(ticker, pattern_type, timeframe, market_regime, direction)
                   WHERE direction IS NOT NULL
                   DO UPDATE SET
                     sample_size=excluded.sample_size,
                     hit_rate_t1=excluded.hit_rate_t1,
                     stopped_out_rate=excluded.stopped_out_rate,
                     calibration_confidence=excluded.calibration_confidence,
                     last_updated=excluded.last_updated""",
                (ticker, pt, tf, direction, n, hr1, sor, conf, now_iso),
            )
            inserted += 1
        except Exception:
            pass

    return inserted

# ── GET /mcp/tools/edge_miner/full_scan ───────────────────────────────────────

@router.get("/mcp/tools/edge_miner/full_scan")
async def mcp_full_scan(
    min_hr: float = Query(0.55, ge=0.30, le=1.0),
    min_samples: int = Query(2000, ge=100),
    min_tickers: int = Query(2, ge=1),
    include_regime_split: bool = Query(True),
    _: str = Depends(get_current_user),
):
    """
    Combined EdgeMiner scan: sector-aggregate edges + regime-split hidden edges.
    Unified ranking by effective_hr descending.
    This is the canonical endpoint for Observatory, FORGE screen, and tip scheduler.
    """
    try:
        from analytics.edge_miner import scan_all_edges
        result = scan_all_edges(
            min_hr=min_hr,
            min_samples=min_samples,
            min_tickers=min_tickers,
            include_regime_split=include_regime_split,
            db_path=ext.DB_PATH,
        )
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    return result


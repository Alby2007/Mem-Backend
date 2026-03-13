"""routes_v2/discovery.py — Internal discovery fleet API routes.

These routes are NOT user-facing. They are called by the companion MCP server
using a shared secret header (X-Internal-Secret).
"""

from __future__ import annotations

import os
import sqlite3

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

from middleware.fastapi_auth import user_path_auth

import extensions as ext
from services.discovery_fleet import (
    ensure_discovery_user,
    seed_discovery_fleet,
    get_discovery_report,
    get_discovery_status,
    DISCOVERY_USER_ID,
)

router = APIRouter()

INTERNAL_SECRET = os.environ.get('DISCOVERY_SECRET', 'dev-secret-change-me')


def _internal_auth(x_internal_secret: str = Header(None)):
    if x_internal_secret != INTERNAL_SECRET:
        raise HTTPException(403, detail='forbidden')


def _get_runner():
    runner = getattr(ext, 'bot_runner', None)
    if runner is None:
        from services.bot_runner import BotRunner
        runner = BotRunner(ext.DB_PATH)
    return runner


@router.post('/internal/discovery/seed')
async def discovery_seed(x_internal_secret: str = Header(None)):
    _internal_auth(x_internal_secret)
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        ensure_discovery_user(conn)
        conn.close()
        runner = _get_runner()
        bot_ids = seed_discovery_fleet(runner)
        return {'seeded': len(bot_ids), 'bot_ids': bot_ids, 'status': 'ok'}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get('/internal/discovery/status')
async def discovery_status(x_internal_secret: str = Header(None)):
    _internal_auth(x_internal_secret)
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        result = get_discovery_status(conn)
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get('/internal/discovery/report')
async def discovery_report(
    min_observations: int = Query(5, ge=1),
    limit: int = Query(50, ge=1, le=500),
    x_internal_secret: str = Header(None),
):
    _internal_auth(x_internal_secret)
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        report = get_discovery_report(conn, min_observations=min_observations, limit=limit)
        conn.close()
        return {'report': report}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post('/internal/discovery/reset')
async def discovery_reset(x_internal_secret: str = Header(None)):
    _internal_auth(x_internal_secret)
    try:
        runner = _get_runner()

        # Stop all discovery bot threads
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        bot_ids = [r[0] for r in conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE user_id=?", (DISCOVERY_USER_ID,)
        ).fetchall()]
        conn.close()

        for bid in bot_ids:
            try:
                runner.stop_bot(bid)
            except Exception:
                pass

        # Delete positions and bot configs (NOT calibration_observations)
        conn2 = sqlite3.connect(ext.DB_PATH, timeout=15)
        try:
            conn2.execute(
                "DELETE FROM paper_positions WHERE bot_id IN "
                "(SELECT bot_id FROM paper_bot_configs WHERE user_id=?)",
                (DISCOVERY_USER_ID,),
            )
            for bid in bot_ids:
                conn2.execute("DELETE FROM paper_bot_equity WHERE bot_id=?", (bid,))
            conn2.execute(
                "DELETE FROM paper_bot_configs WHERE user_id=?", (DISCOVERY_USER_ID,)
            )
            conn2.commit()
        finally:
            conn2.close()

        return {'deleted_bots': len(bot_ids), 'status': 'reset'}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Dev-only proxy routes (user-authenticated, is_dev gated) ─────────────────

def _dev_gate(user_id: str) -> None:
    conn = sqlite3.connect(ext.DB_PATH, timeout=5)
    row = conn.execute(
        'SELECT is_dev FROM user_preferences WHERE user_id=?', (user_id,)
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        raise HTTPException(403, detail='dev only')


@router.get('/users/{user_id}/private-fleet/status')
async def private_fleet_status(user_id: str, _: str = Depends(user_path_auth)):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    result = get_discovery_status(conn)
    conn.close()
    return result


@router.get('/users/{user_id}/private-fleet/report')
async def private_fleet_report(
    user_id: str,
    min_observations: int = Query(3, ge=1),
    limit: int = Query(50, ge=1, le=500),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    result = get_discovery_report(conn, min_observations=min_observations, limit=limit)
    conn.close()
    return {'report': result}


@router.get('/users/{user_id}/private-fleet/closed-positions')
async def pf_closed_positions(
    user_id: str,
    limit: int = Query(200, ge=1, le=1000),
    status: str = Query(None),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        q = """
            SELECT
                pp.id, pp.ticker, pp.direction, pp.pattern_id,
                pp.entry_price, pp.exit_price, pp.stop, pp.t1, pp.t2,
                pp.pnl_r, pp.status, pp.opened_at, pp.closed_at,
                pp.bot_id, pp.note,
                pbc.strategy_name, pbc.pattern_types, pbc.sectors
            FROM paper_positions pp
            LEFT JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
            WHERE pp.status != 'open'
              AND (pp.bot_id LIKE 'disc_%' OR pp.user_id = 'system_discovery')
        """
        params: list = []
        if status:
            q += " AND pp.status = ?"
            params.append(status)
        q += " ORDER BY pp.closed_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(q, params).fetchall()
        cols = ['id', 'ticker', 'direction', 'pattern_id', 'entry_price', 'exit_price',
                'stop', 't1', 't2', 'pnl_r', 'status', 'opened_at', 'closed_at',
                'bot_id', 'note', 'strategy_name', 'pattern_types', 'sectors']
        positions = [dict(zip(cols, r)) for r in rows]

        wins = [p for p in positions if (p.get('pnl_r') or 0) > 0]
        losses = [p for p in positions if (p.get('pnl_r') or 0) < 0]
        gross_profit = sum(p['pnl_r'] for p in wins if p['pnl_r'])
        gross_loss   = abs(sum(p['pnl_r'] for p in losses if p['pnl_r']))
        avg_r = (sum(p['pnl_r'] for p in positions if p.get('pnl_r') is not None) / len(positions)) if positions else 0

        outcome_counts: dict = {}
        for p in positions:
            s = p.get('status') or 'closed'
            outcome_counts[s] = outcome_counts.get(s, 0) + 1

        stats = {
            'win_rate': round(len(wins) / len(positions) * 100, 1) if positions else 0,
            'avg_r':    round(avg_r, 2),
            'gross_profit': round(gross_profit, 2),
            'gross_loss':   round(gross_loss, 2),
            't2_hits':    outcome_counts.get('t2_hit', 0),
            't1_hits':    outcome_counts.get('t1_hit', 0),
            'stopped':    outcome_counts.get('stopped_out', 0),
            'closed':     outcome_counts.get('closed', 0),
        }
        return {'total': len(positions), 'stats': stats, 'positions': positions}
    finally:
        conn.close()


@router.get('/users/{user_id}/private-fleet/bots')
async def pf_bots(
    user_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    pattern: str = Query(None),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        where = "WHERE pbc.user_id=?"
        params: list = [DISCOVERY_USER_ID]
        if pattern:
            where += " AND pbc.pattern_types LIKE ?"
            params.append(f'%{pattern}%')

        total = conn.execute(
            f"SELECT COUNT(*) FROM paper_bot_configs pbc {where}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""SELECT pbc.bot_id, pbc.strategy_name, pbc.pattern_types, pbc.sectors,
                       pbc.direction_bias, pbc.active, pbc.created_at,
                       COUNT(CASE WHEN pp.status='open' THEN 1 END) as open_pos,
                       COUNT(CASE WHEN pp.status!='open' THEN 1 END) as closed_pos,
                       SUM(CASE WHEN pp.pnl_r > 0 AND pp.status!='open' THEN 1 ELSE 0 END) as wins,
                       AVG(CASE WHEN pp.status!='open' THEN pp.pnl_r END) as avg_r
                FROM paper_bot_configs pbc
                LEFT JOIN paper_positions pp ON pp.bot_id = pbc.bot_id
                {where}
                GROUP BY pbc.bot_id
                ORDER BY closed_pos DESC, pbc.created_at DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

        cols = ['bot_id', 'strategy_name', 'pattern_types', 'sectors', 'direction_bias',
                'active', 'created_at', 'open_pos', 'closed_pos', 'wins', 'avg_r']
        bots = []
        for r in rows:
            b = dict(zip(cols, r))
            closed = b['closed_pos'] or 0
            wins   = b['wins'] or 0
            b['win_rate'] = round(wins / closed * 100, 1) if closed > 0 else None
            b['avg_r']    = round(b['avg_r'], 2) if b['avg_r'] is not None else None
            bots.append(b)

        return {'total': total, 'limit': limit, 'offset': offset, 'bots': bots}
    finally:
        conn.close()


@router.get('/users/{user_id}/private-fleet/calibration-obs')
async def pf_calibration_obs(
    user_id: str,
    limit: int = Query(200, ge=1, le=2000),
    _: str = Depends(user_path_auth),
):
    _dev_gate(user_id)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        rows = conn.execute("""
            SELECT co.id, co.ticker, co.pattern_type, co.timeframe,
                   co.market_regime, co.outcome, co.source, co.bot_id,
                   co.pnl_r, co.observed_at
            FROM calibration_observations co
            WHERE co.ticker != 'TEST.L'
            ORDER BY co.observed_at DESC
            LIMIT ?
        """, (limit,)).fetchall()

        cols = ['id', 'ticker', 'pattern_type', 'timeframe', 'market_regime',
                'outcome', 'source', 'bot_id', 'pnl_r', 'observed_at']
        observations = [dict(zip(cols, r)) for r in rows]

        outcome_breakdown: dict = {}
        pattern_breakdown: dict = {}
        for o in observations:
            oc = o.get('outcome') or 'unknown'
            outcome_breakdown[oc] = outcome_breakdown.get(oc, 0) + 1
            pt = o.get('pattern_type') or 'unknown'
            pattern_breakdown[pt] = pattern_breakdown.get(pt, 0) + 1

        try:
            signal_cells = conn.execute(
                "SELECT COUNT(*) FROM calibration_matrix"
            ).fetchone()[0]
        except Exception:
            signal_cells = 0

        cells_with_obs = len(set(
            (o.get('pattern_type', ''), o.get('timeframe', ''), o.get('market_regime', ''))
            for o in observations
        ))

        return {
            'total': len(observations),
            'signal_cells': signal_cells,
            'cells_with_obs': cells_with_obs,
            'outcome_breakdown': outcome_breakdown,
            'pattern_breakdown': pattern_breakdown,
            'observations': observations,
        }
    finally:
        conn.close()

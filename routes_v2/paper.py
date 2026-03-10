"""routes_v2/paper.py — Phase 5: paper trading endpoints."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

import extensions as ext
from middleware.fastapi_auth import get_current_user, user_path_auth
from services import paper_trading as svc

router = APIRouter()


def _tier_gate(user_id: str) -> None:
    tier, err_msg = svc.paper_tier_check(user_id)
    if err_msg:
        raise HTTPException(403, detail={"error": err_msg, "tier": tier})


class UpdateAccountRequest(BaseModel):
    virtual_balance: Optional[float] = None
    mark_set: Optional[bool] = True


class OpenPositionRequest(BaseModel):
    ticker: str
    direction: str
    entry_price: Optional[float] = None
    quantity: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None


class ClosePositionRequest(BaseModel):
    exit_price: Optional[float] = None


@router.get("/users/{user_id}/paper/account")
async def paper_account_get(user_id: str, _: str = Depends(user_path_auth)):
    try:
        tier, err = svc.paper_tier_check(user_id)
        acct = svc.get_account(user_id)
        acct['tier'] = tier
        acct['account_size_set'] = acct.get('account_size_set', False)
        if err:
            acct['requires_upgrade'] = True
        return acct
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.patch("/users/{user_id}/paper/account")
async def paper_account_update(user_id: str, data: UpdateAccountRequest, _: str = Depends(user_path_auth)):
    result = svc.update_account_size(user_id, data.virtual_balance, data.mark_set)
    if 'error' in result:
        raise HTTPException(400, detail=result['error'])
    return result


@router.get("/users/{user_id}/paper/equity")
async def paper_equity_log(user_id: str, days: int = 90, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        return {'equity': svc.get_equity_log(user_id, days)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/paper/positions")
async def paper_positions_list(
    user_id: str,
    status: str = "all",
    _: str = Depends(user_path_auth),
):
    _tier_gate(user_id)
    try:
        return svc.list_positions(user_id, status)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/paper/positions", status_code=201)
async def paper_position_open(
    user_id: str,
    data: OpenPositionRequest,
    _: str = Depends(user_path_auth),
):
    _tier_gate(user_id)
    try:
        result, status = svc.open_position(user_id, data.model_dump())
        if status != 201:
            raise HTTPException(status, detail=result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/paper/positions/{pos_id}/close")
async def paper_position_close(
    user_id: str,
    pos_id: int,
    data: ClosePositionRequest = ClosePositionRequest(),
    _: str = Depends(user_path_auth),
):
    _tier_gate(user_id)
    try:
        result, status = svc.close_position(user_id, pos_id, data.exit_price)
        if status != 200:
            raise HTTPException(status, detail=result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/paper/monitor")
async def paper_monitor(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        return svc.monitor_positions(user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/paper/stats")
async def paper_stats(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        return svc.get_stats(user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/paper/agent/log")
async def paper_agent_log_get(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        return {"log": svc.get_agent_log(user_id)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/paper/agent/run")
async def paper_agent_run_once(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        result = svc.ai_run(user_id)
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/paper/agent/start")
async def paper_agent_start(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    status, message = svc.start_scanner(user_id)
    return {"status": status, "message": message}


@router.post("/users/{user_id}/paper/agent/stop")
async def paper_agent_stop(user_id: str, _: str = Depends(user_path_auth)):
    status, message = svc.stop_scanner(user_id)
    return {"status": status, "message": message}


@router.get("/users/{user_id}/paper/agent/status")
async def paper_agent_status(user_id: str, _: str = Depends(user_path_auth)):
    return {"running": svc.scanner_running(user_id)}


@router.delete("/users/{user_id}/paper/reset", status_code=200)
async def paper_reset(user_id: str, _: str = Depends(user_path_auth)):
    try:
        return svc.reset_paper_trader(user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/paper/public-performance")
async def public_paper_performance():
    """Aggregate paper trading performance across all users. No auth required.
    Used by the landing page and dashboard to show social proof / track record.
    """
    import sqlite3 as _sq
    conn = _sq.connect(ext.DB_PATH, timeout=5)
    try:
        svc.ensure_paper_tables(conn)
        total = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE status != 'open'"
        ).fetchone()[0]
        wins = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE pnl_r > 0 AND status != 'open'"
        ).fetchone()[0]
        avg_r_row = conn.execute(
            "SELECT AVG(pnl_r) FROM paper_positions WHERE pnl_r IS NOT NULL AND status != 'open'"
        ).fetchone()
        avg_r = avg_r_row[0] if avg_r_row else None
        equity_rows = conn.execute(
            """SELECT user_id, equity_value FROM paper_equity_log
               WHERE id IN (SELECT MAX(id) FROM paper_equity_log GROUP BY user_id)"""
        ).fetchall()
        active = conn.execute(
            "SELECT COUNT(*) FROM paper_account WHERE agent_running=1"
        ).fetchone()[0]
    except Exception as _e:
        conn.close()
        return {"total_trades": 0, "error": str(_e)}
    conn.close()
    win_rate = round(wins / total * 100, 1) if total > 0 else None
    return {
        "total_trades": total,
        "win_rate_pct": win_rate,
        "avg_r": round(avg_r, 2) if avg_r is not None else None,
        "active_agents": active,
        "total_agents": len(equity_rows),
    }


@router.get("/users/{user_id}/paper/stress-sim")
async def paper_stress_sim(user_id: str, _: str = Depends(user_path_auth)):
    """
    Probability-weighted portfolio stress simulation.
    Combines transition engine + regime-conditional returns for open positions.
    """
    _tier_gate(user_id)
    try:
        import extensions as _ext
        from analytics.portfolio_stress_simulator import PortfolioStressSimulator
        sim = PortfolioStressSimulator(_ext.DB_PATH)
        return sim.run(user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ── Bot / Fleet endpoints ─────────────────────────────────────────────────────

class CreateBotRequest(BaseModel):
    pattern_types:  Optional[str] = None
    sectors:        Optional[str] = None
    exchanges:      Optional[str] = None
    volatility:     Optional[str] = None
    regimes:        Optional[str] = None
    timeframes:     Optional[str] = None
    direction_bias: Optional[str] = None
    risk_pct:       float = 1.0
    max_positions:  int   = 4
    min_quality:    float = 0.65
    strategy_name:  Optional[str] = None
    virtual_balance: float = 5000.0


class UpdateBotRequest(BaseModel):
    risk_pct:      Optional[float] = None
    max_positions: Optional[int]   = None
    min_quality:   Optional[float] = None


def _get_runner():
    """Return the shared BotRunner from ext, or create a transient one."""
    if hasattr(ext, 'bot_runner') and ext.bot_runner:
        return ext.bot_runner
    from services.bot_runner import BotRunner
    return BotRunner(ext.DB_PATH)


@router.get("/users/{user_id}/bots")
async def list_bots(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        runner = _get_runner()
        return {'bots': runner.list_bots(user_id)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/bots", status_code=201)
async def create_bot(user_id: str, data: CreateBotRequest, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        runner = _get_runner()
        genome = data.model_dump(exclude={'virtual_balance'})
        bot_id = runner.create_manual_bot(user_id, genome, data.virtual_balance)
        return {'bot_id': bot_id, 'status': 'started'}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.patch("/users/{user_id}/bots/{bot_id}")
async def update_bot(user_id: str, bot_id: str, data: UpdateBotRequest, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        import sqlite3 as _sq
        conn = _sq.connect(ext.DB_PATH, timeout=10)
        # Verify ownership
        row = conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE bot_id=? AND user_id=?", (bot_id, user_id)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, detail='bot not found')
        updates = {k: v for k, v in data.model_dump().items() if v is not None}
        if not updates:
            conn.close()
            return {'status': 'no changes'}
        sets = ', '.join(f'{k}=?' for k in updates)
        conn.execute(f"UPDATE paper_bot_configs SET {sets} WHERE bot_id=?",
                     list(updates.values()) + [bot_id])
        conn.commit()
        conn.close()
        return {'bot_id': bot_id, 'updated': list(updates.keys())}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.delete("/users/{user_id}/bots/{bot_id}")
async def delete_bot(user_id: str, bot_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        runner = _get_runner()
        import sqlite3 as _sq
        conn = _sq.connect(ext.DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE bot_id=? AND user_id=?", (bot_id, user_id)
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, detail='bot not found')
        runner.kill_bot(bot_id, reason='user_deleted')
        return {'status': 'killed', 'bot_id': bot_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/bots/{bot_id}/start")
async def start_bot(user_id: str, bot_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        runner = _get_runner()
        import sqlite3 as _sq
        conn = _sq.connect(ext.DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE bot_id=? AND user_id=? AND active=1",
            (bot_id, user_id)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, detail='bot not found or inactive')
        # Clear paused_at so restore logic works
        conn.execute("UPDATE paper_bot_configs SET paused_at=NULL WHERE bot_id=?", (bot_id,))
        conn.commit()
        conn.close()
        runner.start_bot(bot_id)
        return {'status': 'started', 'bot_id': bot_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/bots/{bot_id}/stop")
async def stop_bot(user_id: str, bot_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        runner = _get_runner()
        import sqlite3 as _sq
        conn = _sq.connect(ext.DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT bot_id FROM paper_bot_configs WHERE bot_id=? AND user_id=?", (bot_id, user_id)
        ).fetchone()
        conn.close()
        if not row:
            raise HTTPException(404, detail='bot not found')
        runner.stop_bot(bot_id)
        return {'status': 'paused', 'bot_id': bot_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/users/{user_id}/bots/evolve-now")
async def evolve_now(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        from analytics.strategy_evolution import StrategyEvolution
        engine = StrategyEvolution(ext.DB_PATH)
        result = engine.evaluate(user_id)
        return result
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/bots/{bot_id}/equity")
async def bot_equity(user_id: str, bot_id: str, days: int = 90, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        import sqlite3 as _sq
        from datetime import timedelta
        conn = _sq.connect(ext.DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute(
            "SELECT equity_value, cash_balance, open_positions, logged_at "
            "FROM paper_bot_equity WHERE bot_id=? AND logged_at >= ? ORDER BY logged_at ASC",
            (bot_id, since)
        ).fetchall()
        conn.close()
        return {'bot_id': bot_id, 'equity': [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/bots/{bot_id}/positions")
async def bot_positions(user_id: str, bot_id: str, status: str = 'all', _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        import sqlite3 as _sq
        conn = _sq.connect(ext.DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        if status == 'open':
            rows = conn.execute(
                "SELECT * FROM paper_positions WHERE bot_id=? AND status='open' ORDER BY opened_at DESC",
                (bot_id,)
            ).fetchall()
        elif status == 'closed':
            rows = conn.execute(
                "SELECT * FROM paper_positions WHERE bot_id=? AND status!='open' ORDER BY closed_at DESC LIMIT 100",
                (bot_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM paper_positions WHERE bot_id=? ORDER BY opened_at DESC LIMIT 100",
                (bot_id,)
            ).fetchall()
        conn.close()
        return {'bot_id': bot_id, 'positions': [dict(r) for r in rows], 'count': len(rows)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/bots/{bot_id}/log")
async def bot_log(user_id: str, bot_id: str, limit: int = 50, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        import sqlite3 as _sq
        conn = _sq.connect(ext.DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        rows = conn.execute(
            "SELECT * FROM paper_agent_log WHERE bot_id=? ORDER BY created_at DESC LIMIT ?",
            (bot_id, limit)
        ).fetchall()
        conn.close()
        return {'bot_id': bot_id, 'log': [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/bots/fleet-performance")
async def fleet_performance(user_id: str, _: str = Depends(user_path_auth)):
    tier, err = svc.paper_tier_check(user_id)
    if err:
        raise HTTPException(403, detail={"error": err, "tier": tier})
    try:
        from analytics.strategy_evolution import StrategyEvolution
        engine = StrategyEvolution(ext.DB_PATH)
        return engine.get_fleet_performance(user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/bots/evolution-history")
async def evolution_history(user_id: str, _: str = Depends(user_path_auth)):
    tier, err = svc.paper_tier_check(user_id)
    if err:
        raise HTTPException(403, detail={"error": err, "tier": tier})
    try:
        from analytics.strategy_evolution import StrategyEvolution
        engine = StrategyEvolution(ext.DB_PATH)
        return {'events': engine.get_evolution_history(user_id)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/bots/discoveries")
async def bot_discoveries(user_id: str, _: str = Depends(user_path_auth)):
    tier, err = svc.paper_tier_check(user_id)
    if err:
        raise HTTPException(403, detail={"error": err, "tier": tier})
    try:
        from analytics.strategy_evolution import StrategyEvolution
        engine = StrategyEvolution(ext.DB_PATH)
        return engine.get_discoveries(user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/users/{user_id}/paper/agent/log/export")
async def paper_agent_log_export(user_id: str, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
    try:
        csv_bytes = svc.export_log_csv(user_id)
        fname = f"paper_trade_log_{user_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(
            iter([csv_bytes]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
    except Exception as e:
        raise HTTPException(500, detail=str(e))

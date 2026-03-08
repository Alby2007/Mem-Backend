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
    _tier_gate(user_id)
    try:
        return svc.get_account(user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.patch("/users/{user_id}/paper/account")
async def paper_account_update(user_id: str, data: UpdateAccountRequest, _: str = Depends(user_path_auth)):
    _tier_gate(user_id)
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

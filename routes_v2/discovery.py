"""routes_v2/discovery.py — Internal discovery fleet API routes.

These routes are NOT user-facing. They are called by the companion MCP server
using a shared secret header (X-Internal-Secret).
"""

from __future__ import annotations

import os
import sqlite3

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel

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

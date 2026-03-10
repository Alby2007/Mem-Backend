"""routes_v2/chat.py — Phase 3: chat endpoint.

Gate: smoke test 7/7 pass against :8001.
chat_pipeline.run() already has zero Flask dependency.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

import extensions as ext
from middleware.fastapi_auth import get_current_user_optional
from middleware.fastapi_rate_limiter import RATE_LIMITS, limiter

router = APIRouter()


class ChatRequest(BaseModel):
    message:      str            = Field(..., max_length=2000)
    session_id:   Optional[str]  = Field("default", max_length=128)
    tickers:      Optional[list] = None
    portfolio:    Optional[list] = None
    mode:         Optional[str]  = Field(None, max_length=32)
    explain_mode: Optional[bool] = False


@router.get("/chat/history")
async def chat_history(
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=80, le=200),
    offset: int = Query(default=0, ge=0),
    search: str = Query(default=""),
    auth_user: Optional[str] = Depends(get_current_user_optional),
):
    uid = auth_user or user_id
    if not uid:
        raise HTTPException(401, detail="authentication required")
    from knowledge.conversation_store import ConversationStore, session_id_for_user
    store = ConversationStore(ext.DB_PATH)
    session_id = session_id_for_user(uid)
    entries = store.get_timeline(session_id, limit=limit, offset=offset, search=search)
    total = store.get_total_turn_count(session_id)
    return {"entries": entries, "total": total, "user_id": uid}


@router.get("/chat/history/{message_id}")
async def chat_history_turn(
    message_id: int,
    user_id: Optional[str] = Query(default=None),
    auth_user: Optional[str] = Depends(get_current_user_optional),
):
    uid = auth_user or user_id
    if not uid:
        raise HTTPException(401, detail="authentication required")
    from knowledge.conversation_store import session_id_for_user
    session_id = session_id_for_user(uid)
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        user_row = conn.execute(
            "SELECT id, content, timestamp FROM conv_messages WHERE id=? AND session_id=? AND role='user'",
            (message_id, session_id)
        ).fetchone()
        if not user_row:
            raise HTTPException(404, detail="turn not found")
        asst_row = conn.execute(
            "SELECT id, content, metadata FROM conv_messages WHERE session_id=? AND role='assistant' AND id>? ORDER BY id ASC LIMIT 1",
            (session_id, message_id)
        ).fetchone()
    finally:
        conn.close()
    result = {
        "user": {"id": user_row["id"], "content": user_row["content"], "timestamp": user_row["timestamp"]},
        "assistant": None,
    }
    if asst_row:
        try:
            import json as _json
            meta = _json.loads(asst_row["metadata"]) if asst_row["metadata"] else {}
        except Exception:
            meta = {}
        result["assistant"] = {"id": asst_row["id"], "content": asst_row["content"], "metadata": meta}
    return result


_CHAT_DAILY_LIMITS: dict[str, int] = {
    'free':    5,
    'basic':   20,
    'pro':     200,
    'premium': 200,
}


def _check_chat_quota(user_id: str) -> None:
    """Raise 429 if user has exceeded their daily chat query limit."""
    if not user_id:
        return
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT tier FROM user_preferences WHERE user_id=?", (user_id,)
        ).fetchone()
        tier = (row[0] if row else 'free') or 'free'
        limit = _CHAT_DAILY_LIMITS.get(tier, 5)
        today = __import__('datetime').date.today().isoformat()
        count_row = conn.execute(
            "SELECT COUNT(*) FROM conv_messages "
            "WHERE session_id IN (SELECT session_id FROM conv_sessions WHERE user_id=?) "
            "AND role='user' AND date(timestamp)=?",
            (user_id, today),
        ).fetchone()
        conn.close()
        used = count_row[0] if count_row else 0
        if used >= limit:
            raise HTTPException(
                429,
                detail={"error": "chat_quota_exceeded", "tier": tier,
                        "limit": limit, "used": used},
            )
    except HTTPException:
        raise
    except Exception:
        pass


@router.post("/chat")
@limiter.limit(RATE_LIMITS["chat"])
async def chat_endpoint(
    request: Request,
    data: ChatRequest,
    user_id: Optional[str] = Depends(get_current_user_optional),
):
    _check_chat_quota(user_id)
    from services import chat_pipeline

    response, status = chat_pipeline.run(
        message=data.message,
        session_id=data.session_id or "default",
        user_id=user_id,
        overlay_mode=(data.mode == "overlay") if data.mode else False,
        explain_mode=bool(data.explain_mode),
    )
    if status != 200:
        raise HTTPException(status_code=status, detail=response)
    return response

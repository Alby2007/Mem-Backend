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


@router.get("/chat/atoms")
async def get_chat_atoms(
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
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
        total_row = conn.execute(
            "SELECT COUNT(*) FROM conv_atoms WHERE session_id=?", (session_id,)
        ).fetchone()
        graduated_row = conn.execute(
            "SELECT COUNT(*) FROM conv_atoms WHERE session_id=? AND graduated_at IS NOT NULL",
            (session_id,),
        ).fetchone()
        rows = conn.execute(
            """SELECT subject, predicate, object, atom_type, source,
                      salience_score, source_weight, graduated_at
               FROM conv_atoms
               WHERE session_id=?
               ORDER BY salience_score * source_weight DESC
               LIMIT ?""",
            (session_id, limit),
        ).fetchall()
    finally:
        conn.close()
    total     = total_row[0] if total_row else 0
    graduated = graduated_row[0] if graduated_row else 0
    atoms = [
        {
            "subject":            r["subject"],
            "predicate":          r["predicate"],
            "object":             r["object"],
            "atom_type":          r["atom_type"],
            "source":             r["source"],
            "effective_salience": round((r["salience_score"] or 0) * (r["source_weight"] or 1), 4),
            "graduated":          r["graduated_at"] is not None,
        }
        for r in rows
    ]
    return {
        "total_atoms":     total,
        "graduated_to_kb": graduated,
        "pending":         total - graduated,
        "atoms":           atoms,
    }


@router.get("/chat/stats")
async def get_chat_stats(
    user_id: Optional[str] = Query(default=None),
    auth_user: Optional[str] = Depends(get_current_user_optional),
):
    uid = auth_user or user_id
    if not uid:
        raise HTTPException(401, detail="authentication required")
    from knowledge.conversation_store import ConversationStore, session_id_for_user
    store      = ConversationStore(ext.DB_PATH)
    session_id = session_id_for_user(uid)
    metrics    = store.get_cognitive_metrics(session_id)
    turns      = store.get_total_turn_count(session_id)
    # top_subjects not in get_cognitive_metrics — query separately
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        subj_rows = conn.execute(
            """SELECT subject, COUNT(*) as cnt FROM conv_atoms
               WHERE session_id=? AND subject != ''
               GROUP BY subject ORDER BY cnt DESC LIMIT 10""",
            (session_id,),
        ).fetchall()
        top_subjects = [{"subject": r["subject"], "count": r["cnt"]} for r in subj_rows]
    finally:
        conn.close()
    return {
        "total_turns":   turns,
        "total_atoms":   metrics.get("total_atoms", 0),
        "user_atoms":    metrics.get("user_atoms", 0),
        "asst_atoms":    metrics.get("assistant_atoms", 0),
        "last_7d":       metrics.get("atoms_last_7d", 0),
        "last_30d":      metrics.get("atoms_last_30d", 0),
        "graduated":     metrics.get("graduated_to_kb", 0),
        "pending":       metrics.get("pending", 0),
        "top_subjects":  top_subjects,
    }


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


@router.get("/alerts/pending")
async def alerts_pending_count(
    user_id: Optional[str] = Depends(get_current_user_optional),
):
    """Return count of unsurfaced chat alerts for the badge. Lightweight — no body."""
    if not user_id:
        raise HTTPException(401, detail="authentication required")
    try:
        from analytics.position_monitor import get_pending_alerts
        alerts = get_pending_alerts(ext.DB_PATH, user_id)
        critical = sum(1 for a in alerts if a.get('priority') == 'CRITICAL')
        high     = sum(1 for a in alerts if a.get('priority') == 'HIGH')
        return {"count": len(alerts), "critical": critical, "high": high}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.delete("/chat/workflow")
async def cancel_workflow_endpoint(
    user_id: Optional[str] = Depends(get_current_user_optional),
):
    """Cancel any active workflow for the current user. Called on page/chat load."""
    if not user_id:
        return {"ok": True}
    try:
        from services.workflow_engine import cancel_workflow
        cancel_workflow(ext.DB_PATH, user_id)
    except Exception:
        pass
    return {"ok": True}


@router.post("/chat")
@limiter.limit(RATE_LIMITS["chat"])
async def chat_endpoint(
    request: Request,
    data: ChatRequest,
    user_id: Optional[str] = Depends(get_current_user_optional),
):
    if data.mode != 'debate':
        _check_chat_quota(user_id)

    # ── Workflow intercept ────────────────────────────────────────────────────
    if user_id and data.mode != 'debate':
        try:
            from services.workflow_engine import (
                detect_workflow_trigger, get_active_workflow,
                start_workflow, start_workflow_prefilled, advance_workflow,
                cancel_workflow, detect_nl_setup,
            )
            user_msg = data.message.strip()

            # 1. Cancel command — highest priority
            if user_msg.lower() in ('cancel', '/cancel'):
                active = get_active_workflow(ext.DB_PATH, user_id)
                if active:
                    cancel_workflow(ext.DB_PATH, user_id)
                    return {"answer": "Workflow cancelled. Ask me anything.", "workflow": None}

            # 2. New workflow trigger (/setup, /log)
            trigger = detect_workflow_trigger(user_msg)
            if trigger:
                active = get_active_workflow(ext.DB_PATH, user_id)
                if active:
                    return {
                        "answer": (
                            f"You're already in a workflow — type **cancel** to start over, "
                            f"or answer: {active['current_prompt']}"
                        ),
                        "workflow": {"active": active["workflow"], "step": active["step"]},
                    }
                prompt = start_workflow(ext.DB_PATH, user_id, trigger)
                return {"answer": prompt, "workflow": {"active": trigger, "step": 0}}

            # 3. Active workflow step — route to engine instead of LLM
            active = get_active_workflow(ext.DB_PATH, user_id)
            if active:
                result = advance_workflow(ext.DB_PATH, user_id, user_msg)
                return {
                    "answer": result.answer,
                    "workflow": (
                        None if result.done
                        else {
                            "active": active["workflow"],
                            "step": result.next_step,
                            "field": result.workflow_field,
                        }
                    ),
                    "ticker_context": result.ticker_context or None,
                }

            # 4. Natural language setup detection — only when no workflow active
            nl_fields = detect_nl_setup(user_msg)
            if nl_fields:
                result = start_workflow_prefilled(ext.DB_PATH, user_id, 'setup_trade', nl_fields)
                return {
                    "answer": result.answer,
                    "workflow": (
                        None if result.done
                        else {
                            "active": "setup_trade",
                            "step": result.next_step,
                            "field": result.workflow_field,
                        }
                    ),
                }
        except Exception as _wf_exc:
            import logging as _wf_log
            _wf_log.getLogger(__name__).warning("workflow intercept error: %s", _wf_exc)
            # Fall through to normal pipeline on unexpected error

    # ── Normal chat pipeline ──────────────────────────────────────────────────
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

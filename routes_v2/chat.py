"""routes_v2/chat.py — Phase 3: chat endpoint.

Gate: smoke test 7/7 pass against :8001.
chat_pipeline.run() already has zero Flask dependency.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user_optional
from middleware.fastapi_rate_limiter import RATE_LIMITS, limiter

router = APIRouter()


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = "default"
    tickers: Optional[list] = None
    portfolio: Optional[list] = None
    mode: Optional[str] = None


@router.post("/chat")
@limiter.limit(RATE_LIMITS["chat"])
async def chat_endpoint(
    request: Request,
    data: ChatRequest,
    user_id: Optional[str] = Depends(get_current_user_optional),
):
    from services import chat_pipeline

    response, status = chat_pipeline.run(
        message=data.message,
        session_id=data.session_id or "default",
        user_id=user_id,
        overlay_mode=(data.mode == "overlay") if data.mode else False,
    )
    if status != 200:
        raise HTTPException(status_code=status, detail=response)
    return response

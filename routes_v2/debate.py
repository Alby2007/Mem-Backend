"""routes_v2/debate.py — Dedicated debate agent endpoint.

Bypasses the full chat pipeline (no workflow, no KB retrieval, no quota).
Takes a system_prompt (persona) + user_message (trade context) and calls
the LLM directly. Used by the Oracle Ledger debate panel in Meridian.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

import extensions as ext
from middleware.fastapi_auth import get_current_user_optional

router = APIRouter()


class DebateAgentRequest(BaseModel):
    system_prompt: str = Field(..., max_length=4000)
    user_message:  str = Field(..., max_length=2000)
    agent_id:      Optional[str] = Field(None, max_length=32)


@router.post("/debate/agent")
async def debate_agent(
    data: DebateAgentRequest,
    user_id: Optional[str] = Depends(get_current_user_optional),
):
    """
    Direct LLM call for a debate agent persona.
    No workflow intercept, no quota, no KB pipeline overhead.
    The system_prompt sets the agent's role/persona.
    The user_message provides the trade context to reason over.
    """
    if not ext.HAS_LLM and not ext.HAS_GROQ:
        return {"content": "LLM unavailable.", "error": True}

    messages = [
        {"role": "system", "content": data.system_prompt},
        {"role": "user",   "content": data.user_message},
    ]

    try:
        answer = ext.llm_chat(messages)
        if not answer:
            return {"content": "No response from LLM.", "error": True}
        return {"content": answer, "agent_id": data.agent_id}
    except Exception as e:
        return {"content": f"Error: {e}", "error": True}

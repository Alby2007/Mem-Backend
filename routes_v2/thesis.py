"""routes_v2/thesis.py — Phase 6: thesis builder endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user

router = APIRouter()


class ThesisBuildRequest(BaseModel):
    ticker: str
    premise: str
    direction: str = "bullish"


@router.get("/thesis")
async def thesis_list(user_id: str = Depends(get_current_user)):
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder = ThesisBuilder(ext.DB_PATH)
        theses  = builder.list_user_theses(user_id)
        return {"theses": theses, "count": len(theses)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/thesis/build")
async def thesis_build(data: ThesisBuildRequest, user_id: str = Depends(get_current_user)):
    if not data.ticker.strip() or not data.premise.strip():
        raise HTTPException(400, detail="ticker and premise are required")
    if data.direction not in ("bullish", "bearish"):
        raise HTTPException(400, detail="direction must be bullish or bearish")
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder = ThesisBuilder(ext.DB_PATH)
        result  = builder.build(
            ticker=data.ticker.strip(),
            premise=data.premise.strip(),
            direction=data.direction,
            user_id=user_id,
        )
        return {
            "thesis_id":              result.thesis_id,
            "ticker":                 result.ticker,
            "direction":              result.direction,
            "thesis_status":          result.thesis_status,
            "thesis_score":           result.thesis_score,
            "supporting_evidence":    result.supporting_evidence,
            "contradicting_evidence": result.contradicting_evidence,
            "invalidation_condition": result.invalidation_condition,
            "created_at":             result.created_at,
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/thesis/{thesis_id}")
async def thesis_get(thesis_id: str, _: str = Depends(get_current_user)):
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder    = ThesisBuilder(ext.DB_PATH)
        evaluation = builder.evaluate(thesis_id)
        if evaluation is None:
            raise HTTPException(404, detail="thesis not found")
        return {
            "thesis_id":    evaluation.thesis_id,
            "ticker":       evaluation.ticker,
            "status":       evaluation.status,
            "score":        evaluation.score,
            "supporting":   evaluation.supporting,
            "contradicting": evaluation.contradicting,
            "evaluated_at": evaluation.evaluated_at,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/thesis/{thesis_id}/status")
async def thesis_status_narrative(thesis_id: str, user_id: str = Depends(get_current_user)):
    """
    Re-evaluate a thesis and return a 2–3 sentence human narrative
    summarising its current validity alongside structured data.
    Falls back gracefully if LLM is unavailable (narrative=null).

    Auth check (401) is performed before thesis lookup (404) so that
    unauthenticated requests cannot probe which thesis IDs exist.
    """
    # user_id is guaranteed non-None here — get_current_user raises 401 if absent.
    # Now look up the thesis and enforce ownership before evaluating.
    try:
        import sqlite3 as _sq
        _c = _sq.connect(ext.DB_PATH, timeout=5)
        _owner_row = _c.execute(
            "SELECT user_id FROM thesis_index WHERE thesis_id=?",
            (thesis_id,),
        ).fetchone()
        _c.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    if _owner_row is None:
        raise HTTPException(404, detail="thesis not found")
    if _owner_row[0] != user_id:
        raise HTTPException(403, detail="not your thesis")

    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder    = ThesisBuilder(ext.DB_PATH)
        evaluation = builder.evaluate(thesis_id)
        if evaluation is None:
            raise HTTPException(404, detail="thesis not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    # Build a compact context string for the LLM
    sup_str  = '; '.join(evaluation.supporting[:3])  or 'none'
    con_str  = '; '.join(evaluation.contradicting[:2]) or 'none'
    inv_row  = None
    try:
        import sqlite3 as _sq
        _c = _sq.connect(ext.DB_PATH, timeout=5)
        _r = _c.execute(
            "SELECT invalidation_condition FROM thesis_index WHERE thesis_id=?",
            (thesis_id,),
        ).fetchone()
        _c.close()
        if _r:
            inv_row = _r[0]
    except Exception:
        pass

    thesis_snippet = (
        f"THESIS STATUS:\n"
        f"thesis_id: {evaluation.thesis_id}\n"
        f"ticker: {evaluation.ticker} | status: {evaluation.status} "
        f"| score: {evaluation.score:.2f}\n"
        f"supporting: {sup_str}\n"
        f"contradicting: {con_str}\n"
        f"invalidation_condition: {inv_row or 'see thesis atoms'}\n"
    )
    narrative: Optional[str] = None
    if ext.HAS_LLM:
        try:
            messages = ext.build_prompt(
                user_message=(
                    f"Summarise the current validity of the {evaluation.ticker} "
                    f"{evaluation.status} thesis in 2-3 sentences."
                ),
                snippet=thesis_snippet,
                briefing_mode='thesis_status',
                telegram_mode=False,
            )
            narrative = ext.llm_chat(messages)
        except Exception:
            pass

    return {
        "thesis_id":             evaluation.thesis_id,
        "ticker":                evaluation.ticker,
        "status":                evaluation.status,
        "score":                 evaluation.score,
        "narrative":             narrative,
        "supporting":            evaluation.supporting,
        "contradicting":         evaluation.contradicting,
        "invalidation_condition": inv_row,
        "evaluated_at":          evaluation.evaluated_at,
    }


@router.patch("/tip/followup/{followup_id}/link-thesis")
async def link_followup_thesis_route(
    followup_id: int,
    thesis_id: str,
    user_id: str = Depends(get_current_user),
):
    """Confirm thesis linkage for a followup after user confirmation prompt."""
    try:
        from users.user_store import link_followup_thesis
        link_followup_thesis(ext.DB_PATH, followup_id, thesis_id)
        return {"followup_id": followup_id, "thesis_id": thesis_id, "linked": True}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/thesis/{thesis_id}/check")
async def thesis_check(thesis_id: str, _: str = Depends(get_current_user)):
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder    = ThesisBuilder(ext.DB_PATH)
        evaluation = builder.evaluate(thesis_id)
        if evaluation is None:
            raise HTTPException(404, detail="thesis not found")
        return {
            "thesis_id":    evaluation.thesis_id,
            "ticker":       evaluation.ticker,
            "status":       evaluation.status,
            "score":        evaluation.score,
            "supporting":   evaluation.supporting,
            "contradicting": evaluation.contradicting,
            "evaluated_at": evaluation.evaluated_at,
            "note":         "thesis re-evaluated against current KB state",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))

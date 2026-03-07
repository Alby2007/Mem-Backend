"""routes_v2/thesis.py — Phase 6: thesis builder endpoints."""

from __future__ import annotations

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

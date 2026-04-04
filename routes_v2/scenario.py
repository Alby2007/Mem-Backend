"""routes_v2/scenario.py — Scenario testing endpoints."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

import extensions as ext
from middleware.fastapi_auth import get_current_user
from middleware.fastapi_rate_limiter import RATE_LIMITS, limiter
from services.scenario_engine import (
    SEED_CATEGORIES,
    _SHOCK_ALIASES,
    run_scenario,
)

router = APIRouter()
_logger = logging.getLogger(__name__)


class ScenarioRequest(BaseModel):
    shock:          str   = Field(..., max_length=500)
    max_depth:      int   = Field(4, ge=1, le=8)
    min_confidence: float = Field(0.5, ge=0.0, le=1.0)
    narrative:      bool  = False


# ── GET /scenario/seeds ───────────────────────────────────────────────────────

@router.get("/scenario/seeds")
async def scenario_seeds(_: str = Depends(get_current_user)):
    """
    Return valid seed concepts grouped by category plus the alias mapping.
    Use for autocomplete chips and friendly labels in the frontend.
    """
    return {
        "categories": SEED_CATEGORIES,
        "aliases":    _SHOCK_ALIASES,
    }


# ── POST /scenario/run ────────────────────────────────────────────────────────

@router.post("/scenario/run")
@limiter.limit(RATE_LIMITS["scenario"])
async def scenario_run(
    request: Request,
    body: ScenarioRequest,
    user_id: str = Depends(get_current_user),
):
    """
    Run a read-only causal scenario from a free-text or concept-name shock.

    - narrative=false (default): returns chain + affected tickers in <50ms
    - narrative=true: same response + 2-3 sentence LLM narrative (~1-2s Groq)

    If resolved=false the chain is empty and unresolved_message suggests
    the closest-matching seed concepts via edit distance.
    """
    try:
        # Fetch user's open positions for portfolio_impact enrichment
        portfolio_tickers: list[str] = []
        if user_id and ext.HAS_PRODUCT_LAYER:
            try:
                from users.user_store import get_user_open_positions
                positions = get_user_open_positions(ext.DB_PATH, user_id)
                portfolio_tickers = [p['ticker'] for p in positions if p.get('ticker')]
            except Exception:
                portfolio_tickers = []

        # Trader level for narrative tone
        trader_level = 'developing'
        if user_id and ext.HAS_PRODUCT_LAYER:
            try:
                from users.user_store import get_user_profile
                profile = get_user_profile(ext.DB_PATH, user_id)
                trader_level = profile.get('trader_level', 'developing') if profile else 'developing'
            except Exception:
                pass

        result = run_scenario(
            shock=body.shock,
            db_path=ext.DB_PATH,
            max_depth=body.max_depth,
            min_confidence=body.min_confidence,
            portfolio_tickers=portfolio_tickers,
            narrative=body.narrative,
            trader_level=trader_level,
        )
    except Exception as e:
        _logger.exception("scenario_run error: %s", e)
        raise HTTPException(500, detail=str(e))

    response: dict = {
        "shock_input":       result.shock_input,
        "seed_concept":      result.seed_concept,
        "resolved":          result.resolved,
        "chain":             result.chain,
        "concepts_reached":  result.concepts_reached,
        "affected_tickers":  result.affected_tickers,
        "portfolio_impact":  result.portfolio_impact,
        "chain_confidence":  result.chain_confidence,
        "narrative":         result.narrative,
        "elapsed_ms":        result.elapsed_ms,
    }
    if result.unresolved_message:
        response["unresolved_message"] = result.unresolved_message

    return response

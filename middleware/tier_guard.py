"""middleware/tier_guard.py — Tier-based access control for FastAPI routes.

Usage:
    from middleware.tier_guard import require_tier

    @router.get("/premium-feature")
    async def feature(user_id: str = Depends(require_tier("premium"))):
        ...

Tiers (ascending):
    free < premium < enterprise

A user without a preferences row is treated as free.
is_dev=1 bypasses all tier checks (eval harness + internal testing).
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import Depends, HTTPException

import extensions as ext
from middleware.fastapi_auth import get_current_user

_TIER_ORDER: dict[str, int] = {
    "free":       0,
    "premium":    1,
    "enterprise": 2,
}


def _get_user_tier(user_id: str) -> tuple[str, bool]:
    """Return (tier, is_dev) for a user. Falls back to ('free', False)."""
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT tier, is_dev FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        conn.close()
        if row:
            return (row[0] or "free", bool(row[1]))
    except Exception:
        pass
    return ("free", False)


def require_tier(min_tier: str):
    """
    FastAPI dependency factory. Enforces minimum tier on a route.

    Example:
        @router.post("/chat")
        async def chat(user_id: str = Depends(require_tier("premium"))):
            ...

    Returns user_id if the check passes. Raises 403 otherwise.
    is_dev users bypass all tier checks.
    """
    async def _check(current_user: str = Depends(get_current_user)) -> str:
        tier, is_dev = _get_user_tier(current_user)
        if is_dev:
            return current_user
        required_level = _TIER_ORDER.get(min_tier, 0)
        user_level     = _TIER_ORDER.get(tier, 0)
        if user_level < required_level:
            raise HTTPException(
                status_code=403,
                detail={
                    "error":        "tier_required",
                    "required_tier": min_tier,
                    "your_tier":    tier,
                    "message":      f"This feature requires a {min_tier} subscription.",
                },
            )
        return current_user

    return _check


def check_tier(user_id: str, min_tier: str) -> bool:
    """
    Synchronous helper for non-route code (e.g. scheduler, bot runner).
    Returns True if the user meets the minimum tier requirement.
    """
    tier, is_dev = _get_user_tier(user_id)
    if is_dev:
        return True
    return _TIER_ORDER.get(tier, 0) >= _TIER_ORDER.get(min_tier, 0)

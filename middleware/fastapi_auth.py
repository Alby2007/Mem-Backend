"""middleware/fastapi_auth.py — FastAPI auth dependencies.

Replaces @require_auth + g.user_id across all v2 route files.
"""

from __future__ import annotations

from typing import Optional

from fastapi import Cookie, Depends, HTTPException, Request

from middleware.auth import _decode_token


async def get_current_user(
    request: Request,
    tg_access: Optional[str] = Cookie(default=None),
) -> str:
    """Require a valid JWT. Returns user_id or raises 401."""
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token and tg_access:
        token = tg_access
    if not token:
        raise HTTPException(status_code=401, detail="missing token")
    try:
        payload = _decode_token(token)
        return payload["user_id"]
    except Exception as exc:
        name = type(exc).__name__
        if "Expired" in name:
            raise HTTPException(status_code=401, detail="token_expired")
        raise HTTPException(status_code=401, detail="invalid_token")


async def get_current_user_optional(
    request: Request,
    tg_access: Optional[str] = Cookie(default=None),
) -> Optional[str]:
    """Return user_id if a valid token is present, else None (no 401)."""
    try:
        return await get_current_user(request, tg_access)
    except HTTPException:
        return None


async def user_path_auth(
    user_id: str,
    current_user: str = Depends(get_current_user),
) -> str:
    """Combine auth + assert_self for /users/{user_id}/... routes."""
    if current_user != user_id:
        raise HTTPException(status_code=403, detail="forbidden")
    return current_user

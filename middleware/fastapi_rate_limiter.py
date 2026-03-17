"""middleware/fastapi_rate_limiter.py — slowapi rate limiter for FastAPI.

Key function uses authenticated user_id when a valid JWT is present so each
user gets their own bucket. Falls back to remote IP for unauthenticated requests.
Behind Cloudflare all users would otherwise share a single IP bucket.
"""

from __future__ import annotations

import os
import uuid

from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address

RATE_LIMITS: dict[str, str] = {
    "auth":      "10/minute",
    "chat":      "60/hour",
    "scenario":  "20/hour",
    "snapshot":  "5/hour",
    "patterns":  "60/hour",
    "portfolio": "60/hour",
    "write":     "60/hour",
    "default":   "200/day",
}

_EXEMPT_IPS = {"127.0.0.1", "::1"}
_JWT_SECRET  = os.environ.get("JWT_SECRET_KEY", "")


def _rate_limit_key(request: Request) -> str:
    """
    Rate-limit key priority:
      1. EVAL_MODE=1 → unique key per request (unlimited — test harness)
      2. Localhost IP → unique key per request (unlimited — dev)
      3. Valid JWT → user:{user_id}  (per-user bucket, works behind Cloudflare)
      4. Fallback → remote IP
    """
    if os.environ.get("EVAL_MODE") == "1":
        return f"eval-{uuid.uuid4().hex}"

    ip = get_remote_address(request)
    if ip in _EXEMPT_IPS:
        return f"exempt-{uuid.uuid4().hex}"

    # Try to extract user_id from JWT without a full DB round-trip.
    # verify_exp=False because we only need identity here — auth dep
    # already validates expiry on authenticated routes.
    if _JWT_SECRET:
        try:
            import jwt as _jwt
            token = (
                request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
                or request.cookies.get("tg_access", "").strip()
            )
            if token:
                payload = _jwt.decode(
                    token, _JWT_SECRET, algorithms=["HS256"],
                    options={"verify_exp": False},
                )
                uid = payload.get("user_id")
                if uid:
                    return f"user:{uid}"
        except Exception:
            pass

    return ip or "unknown"


limiter = Limiter(
    key_func=_rate_limit_key,
    default_limits=[RATE_LIMITS["default"]],
)

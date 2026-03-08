"""middleware/fastapi_rate_limiter.py — slowapi rate limiter for FastAPI."""

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


def _rate_limit_key(request: Request) -> str:
    if os.environ.get("EVAL_MODE") == "1":
        return f"eval-{uuid.uuid4().hex}"
    ip = get_remote_address(request)
    if ip in _EXEMPT_IPS:
        # Unique key per request → never shares a bucket → effectively unlimited
        return f"exempt-{uuid.uuid4().hex}"
    return ip


limiter = Limiter(
    key_func=_rate_limit_key,
    default_limits=[RATE_LIMITS["default"]],
)

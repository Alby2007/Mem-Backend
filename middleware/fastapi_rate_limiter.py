"""middleware/fastapi_rate_limiter.py — slowapi rate limiter for FastAPI."""

from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address

RATE_LIMITS: dict[str, str] = {
    "auth":      "10/minute",
    "chat":      "60/hour",
    "snapshot":  "5/hour",
    "patterns":  "60/hour",
    "portfolio": "60/hour",
    "write":     "60/hour",
    "default":   "200/day",
}

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[RATE_LIMITS["default"]],
)

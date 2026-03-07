"""middleware/fastapi_rate_limiter.py — slowapi rate limiter for FastAPI.

Drop-in equivalent of middleware/rate_limiter.py for Flask.
Uses the same SQLite bucket so Flask and FastAPI workers share limits
during the dual-run migration period.
"""

from __future__ import annotations

import os

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

_db_path = os.environ.get("TRADING_KB_DB", "trading_knowledge.db")

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[RATE_LIMITS["default"]],
    storage_uri=f"sqlite:///{_db_path}",
)

"""api_v2.py — FastAPI application factory.

Runs alongside Flask on :8001 during migration.
Cutover: point gunicorn at this file and drop api.py once all phases pass eval.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

import extensions as ext

_logger = logging.getLogger(__name__)


def create_fastapi_app() -> FastAPI:
    app = FastAPI(title="Trading Galaxy API", version="2.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://trading-galaxy.uk",
            "https://www.trading-galaxy.uk",
            "https://app.trading-galaxy.uk",
            "http://localhost:3000",
            "http://localhost:5173",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from middleware.fastapi_rate_limiter import limiter
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    # ── Routers (all 15 blueprints) ────────────────────────────────────────────
    from routes_v2 import (
        health, auth, chat, billing, paper,
        markets, analytics_, patterns, network, waitlist, thesis,
        ingest_routes, kb, users, telegram,
    )
    for _router in [
        health.router, auth.router, chat.router, billing.router, paper.router,
        markets.router, analytics_.router, patterns.router, network.router,
        waitlist.router, thesis.router, ingest_routes.router, kb.router,
        users.router, telegram.router,
    ]:
        app.include_router(_router)

    return app


app = create_fastapi_app()

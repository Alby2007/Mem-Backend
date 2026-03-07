"""api_v2.py — FastAPI application factory.

Runs alongside Flask on :8001 during migration.
Cutover: point gunicorn at this file and drop api.py once all phases pass eval.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

import extensions as ext

_logger = logging.getLogger(__name__)

_INGEST_INTERVAL = int(os.environ.get('INGEST_INTERVAL_SECONDS', '300'))   # LLM extraction cadence
_INGEST_BATCH    = int(os.environ.get('INGEST_BATCH_SIZE', '15'))          # items per LLM run


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # ── Paper agent: re-launch threads that were running before restart ────────
    try:
        from services.paper_trading import restore_scanners
        restore_scanners()
    except Exception as _e:
        _logger.warning('restore_scanners on startup failed: %s', _e)

    # ── Ingest scheduler: continuous background data ingestion ─────────────────
    scheduler = None
    try:
        from ingest.scheduler import IngestScheduler
        from ingest.rss_adapter import RSSAdapter
        from ingest.llm_extraction_adapter import LLMExtractionAdapter
        from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
        from ingest.yfinance_adapter import YFinanceAdapter
        from ingest.sector_rotation_adapter import SectorRotationAdapter
        from ingest.edgar_realtime_adapter import EDGARRealtimeAdapter

        db_path = ext.DB_PATH
        scheduler = IngestScheduler(ext.kg)

        # LLM extractor — drains the extraction_queue; batch size tunable via env
        os.environ.setdefault('INGEST_BATCH_SIZE', str(_INGEST_BATCH))
        scheduler.register(LLMExtractionAdapter(db_path=db_path), interval_sec=_INGEST_INTERVAL)

        # RSS news — replenishes the extraction_queue
        scheduler.register(RSSAdapter(db_path=db_path), interval_sec=900)

        # Price feed — keeps last_price atoms fresh for all tracked tickers
        scheduler.register(YFinanceAdapter(db_path=db_path), interval_sec=1800)

        # Derived signals — sector rotation, enrichment (no external API needed)
        scheduler.register(SectorRotationAdapter(db_path=db_path), interval_sec=3600)
        scheduler.register(SignalEnrichmentAdapter(db_path=db_path), interval_sec=3600)

        # SEC real-time filings — 8-K atom feed
        scheduler.register(EDGARRealtimeAdapter(db_path=db_path), interval_sec=1800)

        app.state.scheduler = scheduler
        scheduler.start(startup_delay_sec=15)
        _logger.info('Ingest scheduler started (%d adapters)', 6)

    except Exception as _e:
        _logger.warning('Ingest scheduler failed to start: %s', _e)
        app.state.scheduler = None

    yield

    # ── Shutdown ───────────────────────────────────────────────────────────────
    if scheduler is not None:
        try:
            scheduler.stop()
        except Exception:
            pass


def create_fastapi_app() -> FastAPI:
    app = FastAPI(title="Trading Galaxy API", version="2.0", lifespan=_lifespan)

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

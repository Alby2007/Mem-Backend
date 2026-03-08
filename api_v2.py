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
        from ingest.insider_adapter import InsiderAdapter
        from ingest.boe_adapter import BoEAdapter
        from ingest.gdelt_adapter import GDELTAdapter
        from ingest.usgs_adapter import USGSAdapter
        from ingest.earnings_calendar_adapter import EarningsCalendarAdapter
        from ingest.economic_calendar_adapter import EconomicCalendarAdapter
        from ingest.finra_short_interest_adapter import FINRAShortInterestAdapter
        from ingest.yield_curve_adapter import YieldCurveAdapter
        from ingest.fred_adapter import FREDAdapter
        from ingest.lse_flow_adapter import LSEFlowAdapter
        from ingest.acled_adapter import ACLEDAdapter
        from ingest.eia_adapter import EIAAdapter
        from ingest.pattern_adapter import PatternAdapter
        from analytics.position_monitor import PositionMonitor

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

        # SEC Form 4 insider transactions — directional signal, no key needed
        scheduler.register(InsiderAdapter(db_path=db_path), interval_sec=3600)

        # UK macro — BoE rate decisions, gilt yields; critical for FTSE coverage
        scheduler.register(BoEAdapter(), interval_sec=86400)

        # Geopolitical tension scores — GDELT event counts, no key needed
        scheduler.register(GDELTAdapter(), interval_sec=3600)

        # Commodity region seismic risk — no key needed
        scheduler.register(USGSAdapter(), interval_sec=3600)

        # Earnings risk flags — reads next_earnings_date atoms from KB
        scheduler.register(EarningsCalendarAdapter(db_path=db_path), interval_sec=3600)

        # FOMC/CPI/NFP countdown — macro event risk atoms
        scheduler.register(EconomicCalendarAdapter(db_path=db_path), interval_sec=86400)

        # FINRA biweekly short interest — REST API per symbol, no key needed
        scheduler.register(FINRAShortInterestAdapter(db_path=db_path), interval_sec=86400)

        # Yield curve regime — TLT/IEF/SHY via Polygon; skips gracefully if no key
        scheduler.register(YieldCurveAdapter(), interval_sec=86400)

        # FRED macro indicators — fed funds, CPI, GDP, unemployment, yield curve
        scheduler.register(FREDAdapter(), interval_sec=86400)

        # LSE institutional order flow — block volume ratio, accumulation/distribution
        scheduler.register(LSEFlowAdapter(db_path=db_path), interval_sec=7200)

        # ACLED geopolitical conflict/protest events — skips gracefully if no API key
        scheduler.register(ACLEDAdapter(), interval_sec=86400)

        # EIA oil/gas prices, production, inventories, Henry Hub
        scheduler.register(EIAAdapter(), interval_sec=86400)

        app.state.scheduler = scheduler
        scheduler.start(startup_delay_sec=15)
        _logger.info('Ingest scheduler started (%d adapters)', 18)

        # PatternAdapter — not a BaseIngestAdapter; runs in its own daemon thread
        # Pass explicit tickers so it doesn't fall back to reading all facts subjects
        # (which includes thousands of LLM-extracted entity names that aren't valid tickers)
        try:
            from ingest.dynamic_watchlist import DynamicWatchlistManager as _DWM
            _pattern_tickers = _DWM.get_active_tickers(db_path)
        except Exception:
            _pattern_tickers = None
        _pattern = PatternAdapter(db_path=db_path, tickers=_pattern_tickers)
        import threading as _threading
        def _pattern_loop(adapter: PatternAdapter, stop: _threading.Event) -> None:
            import time as _time
            while not stop.is_set():
                try:
                    adapter.run()
                except Exception as _pe:
                    _logger.error('PatternAdapter cycle error: %s', _pe)
                stop.wait(adapter.interval_sec)
        _pattern_stop = _threading.Event()
        _pt = _threading.Thread(target=_pattern_loop, args=(_pattern, _pattern_stop),
                                name='pattern-adapter', daemon=True)
        _pt.start()
        app.state.pattern_stop = _pattern_stop
        _logger.info('PatternAdapter thread started (interval=%ds)', _pattern.interval_sec)

        # PositionMonitor — background thread watching open positions
        _pos_monitor = PositionMonitor(db_path=db_path, interval_sec=300)
        _pos_monitor.start()
        app.state.position_monitor = _pos_monitor

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
    try:
        if getattr(app.state, 'pattern_stop', None):
            app.state.pattern_stop.set()
    except Exception:
        pass
    try:
        if getattr(app.state, 'position_monitor', None):
            app.state.position_monitor.stop()
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

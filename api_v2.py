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
        from ingest.gpr_adapter import GPRAdapter
        from ingest.alpha_vantage_adapter import AlphaVantageAdapter
        from ingest.polymarket_adapter import PolymarketAdapter
        from ingest.pattern_adapter import PatternAdapter

        db_path = ext.DB_PATH
        # S4: wire db_path into retrieval.py so Strategy 6 (Historical State Match) works
        try:
            import retrieval as _ret_mod
            _ret_mod.set_db_path(db_path)
        except Exception:
            pass
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

        # ACLED geopolitical conflict/protest events — requires commercial license for production use.
        # Set ACLED_COMMERCIAL_LICENSE=1 in env to enable (free tier = non-commercial research only).
        if os.environ.get('ACLED_COMMERCIAL_LICENSE') == '1':
            scheduler.register(ACLEDAdapter(), interval_sec=86400)
        else:
            _logger.warning('ACLED adapter disabled: set ACLED_COMMERCIAL_LICENSE=1 to enable (commercial license required for paying users)')

        # EIA oil/gas prices, production, inventories, Henry Hub
        scheduler.register(EIAAdapter(), interval_sec=86400)

        # GPR Index — Caldara-Iacoviello Fed geopolitical risk index; no key needed
        scheduler.register(GPRAdapter(db_path=db_path), interval_sec=86400)

        # Historical signal calibration — nightly back-population of hit rates
        # 20h recency guard prevents double-runs; first manual run via POST /calibrate/historical
        from ingest.historical_calibration_adapter import HistoricalCalibrationAdapter
        scheduler.register(HistoricalCalibrationAdapter(db_path=db_path), interval_sec=86400)

        # State snapshots — full market state vectors every 6h for temporal search
        from ingest.state_snapshot_adapter import StateSnapshotAdapter
        scheduler.register(StateSnapshotAdapter(db_path=db_path), interval_sec=21600)

        # Transition builder — daily, processes snapshots into state_transitions table
        from ingest.transition_builder_adapter import TransitionBuilderAdapter
        scheduler.register(TransitionBuilderAdapter(db_path=db_path), interval_sec=86400)

        # Alpha Vantage news sentiment — per-ticker AI sentiment; skips if no key
        scheduler.register(AlphaVantageAdapter(db_path=db_path), interval_sec=86400)

        # Polymarket prediction markets — macro/geo odds; no key needed
        scheduler.register(PolymarketAdapter(), interval_sec=3600)

        app.state.scheduler = scheduler
        # Stagger startup: each adapter gets a unique delay so they don't all
        # hammer SQLite simultaneously. Heavy writers are spread 20s apart.
        import threading as _sched_threading
        def _delayed_register(adapter, interval_sec, delay_sec):
            def _run():
                import time as _t
                _t.sleep(delay_sec)
                scheduler._schedule(adapter=adapter, interval_sec=interval_sec, immediate=True)
            t = _sched_threading.Thread(target=_run, daemon=True, name=f'ingest-start-{adapter.name}')
            t.start()

        scheduler._running = True
        for i, (adapter, interval_sec) in enumerate(scheduler._adapters):
            _delayed_register(adapter, interval_sec, delay_sec=15 + i * 20)

        _logger.info('Ingest scheduler started (%d adapters, staggered 20s apart)', len(scheduler._adapters))

        # PatternAdapter — not a BaseIngestAdapter; runs in its own daemon thread
        # Pass explicit tickers so it doesn't fall back to reading all facts subjects
        # (which includes thousands of LLM-extracted entity names that aren't valid tickers)
        try:
            from ingest.dynamic_watchlist import DynamicWatchlistManager as _DWM
            _pattern_tickers = _DWM.get_pattern_tickers(db_path)
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

    except Exception as _e:
        _logger.warning('Ingest scheduler failed to start: %s', _e)
        app.state.scheduler = None

    # ── Causal graph seed edges — must run before ShockEngine + Scenario engine ──
    try:
        import sqlite3 as _sq_cg
        from knowledge.causal_graph import ensure_causal_edges_table
        _cg_conn = _sq_cg.connect(ext.DB_PATH, timeout=10)
        ensure_causal_edges_table(_cg_conn)
        _cg_conn.close()
        _logger.info('Causal graph seed edges ensured')
    except Exception as _cg_e:
        _logger.warning('Causal graph seeding failed: %s', _cg_e)

    # ── CausalShockEngine — propagates macro shocks through causal graph ──────
    try:
        from analytics.causal_shock_engine import CausalShockEngine
        _shock_engine = CausalShockEngine(ext.DB_PATH)
        ext.kg.set_shock_engine(_shock_engine)
        ext.shock_engine = _shock_engine
        _logger.info('CausalShockEngine wired into KnowledgeGraph')
    except Exception as _se_e:
        _logger.warning('CausalShockEngine failed to start: %s', _se_e)

    # ── ThesisMonitor — proactive thesis invalidation alerts ─────────────────
    try:
        from knowledge.thesis_builder import ThesisMonitor
        _thesis_monitor = ThesisMonitor(ext.DB_PATH)
        ext.kg.set_thesis_monitor(_thesis_monitor)
        ext.thesis_monitor = _thesis_monitor
        _logger.info('ThesisMonitor wired into KnowledgeGraph')
    except Exception as _tm_e:
        _logger.warning('ThesisMonitor failed to start: %s', _tm_e)

    # ── PredictionLedger — Brier-scored prediction tracking ──────────────────
    try:
        from analytics.prediction_ledger import PredictionLedger
        _ledger = PredictionLedger(ext.DB_PATH)
        ext.kg.set_ledger(_ledger)
        ext.prediction_ledger = _ledger
        _logger.info('PredictionLedger wired into KnowledgeGraph (intraday resolution active)')
    except Exception as _pl_e:
        _logger.warning('PredictionLedger failed to start: %s', _pl_e)

    # PositionMonitor — independent of ingest; must start even when ingest fails
    try:
        from analytics.position_monitor import PositionMonitor
        _pos_monitor = PositionMonitor(db_path=ext.DB_PATH, interval_sec=300)
        _pos_monitor.start()
        app.state.position_monitor = _pos_monitor
        ext.position_monitor = _pos_monitor
    except Exception as _pm_e:
        _logger.warning('PositionMonitor failed to start: %s', _pm_e)

    # ── Notification schedulers ────────────────────────────────────────────────
    # Guard: skip entirely if bot token is absent — no point burning CPU on
    # curate_snapshot() for every user when sends will silently fail anyway.
    _tg_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not _tg_token:
        _logger.warning(
            'TELEGRAM_BOT_TOKEN not set — TipScheduler and DeliveryScheduler will NOT start. '
            'Set the token in .env and restart to enable briefings.'
        )
    else:
        try:
            from notifications.delivery_scheduler import DeliveryScheduler
            ext.delivery_scheduler = DeliveryScheduler(db_path=ext.DB_PATH)
            ext.delivery_scheduler.start()
        except Exception as _de:
            _logger.warning('DeliveryScheduler failed to start: %s', _de)

        try:
            from notifications.tip_scheduler import TipScheduler
            ext.tip_scheduler = TipScheduler(db_path=ext.DB_PATH)
            ext.tip_scheduler.start()
        except Exception as _te:
            _logger.warning('TipScheduler failed to start: %s', _te)

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
        if ext.position_monitor:
            ext.position_monitor.stop()
    except Exception:
        pass
    try:
        if ext.delivery_scheduler:
            ext.delivery_scheduler.stop()
    except Exception:
        pass
    try:
        if ext.tip_scheduler:
            ext.tip_scheduler.stop()
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
        ingest_routes, kb, users, telegram, scenario,
    )
    for _router in [
        health.router, auth.router, chat.router, billing.router, paper.router,
        markets.router, analytics_.router, patterns.router, network.router,
        waitlist.router, thesis.router, ingest_routes.router, kb.router,
        users.router, telegram.router, scenario.router,
    ]:
        app.include_router(_router)

    return app


app = create_fastapi_app()

"""
api.py — Trading KB REST API

Thin Flask wrapper exposing the Trading Knowledge Graph over HTTP.
Designed for the ingest team to push atoms and for the copilot layer
to pull context.

Endpoints:
  POST /ingest          — add one or more atoms to the KB
  GET  /query           — retrieve atoms matching subject/predicate/object
  POST /retrieve        — smart multi-strategy retrieval for a query string
  GET  /stress          — compute epistemic stress for a topic
  GET  /stats           — KB statistics
  GET  /health          — liveness check
"""

from __future__ import annotations

import os
import pathlib
import re

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
from datetime import datetime, timezone
from typing import List, Optional

from flask import Flask, g, request, jsonify

try:
    from llm.ollama_client import chat as ollama_chat, list_models, is_available, warmup, DEFAULT_MODEL
    from llm.prompt_builder import build as build_prompt
    HAS_LLM = True
except ImportError:
    HAS_LLM = False

try:
    from llm.groq_client import chat as groq_chat, is_available as groq_available
    HAS_GROQ = True
except ImportError:
    HAS_GROQ = False

_llm_logger = __import__('logging').getLogger('llm.token_count')

def _llm_chat(messages, model=None, **kwargs):
    """Unified LLM chat: prefer Groq (fast, free API) over local Ollama."""
    # Log approximate token count so we can monitor context window usage.
    # llama-3.3-70b-versatile on Groq has a 128k token context window.
    # ~4 chars/token is a safe estimate for English+JSON mixed content.
    try:
        _total_chars = sum(len(m.get('content', '')) for m in messages)
        _est_tokens = _total_chars // 4
        _llm_logger.info(
            'prompt: %d messages, ~%d chars, ~%d tokens est (128k limit)',
            len(messages), _total_chars, _est_tokens,
        )
        if _est_tokens > 100_000:
            _llm_logger.warning('CONTEXT NEAR LIMIT: ~%d tokens est', _est_tokens)
    except Exception:
        pass
    if HAS_GROQ and groq_available():
        return groq_chat(messages)
    if HAS_LLM:
        return ollama_chat(messages, model=model or DEFAULT_MODEL, **kwargs)
    return None

try:
    from knowledge.working_memory import WorkingMemory, kb_has_atoms, MAX_ON_DEMAND_TICKERS
    _working_memory = WorkingMemory()
    HAS_WORKING_MEMORY = True
except ImportError:
    HAS_WORKING_MEMORY = False
    _working_memory = None  # type: ignore

from knowledge import KnowledgeGraph
from knowledge.decay import get_decay_worker
from retrieval import retrieve

try:
    from ingest.scheduler import IngestScheduler
    from ingest.yfinance_adapter import YFinanceAdapter
    from ingest.fred_adapter import FREDAdapter
    from ingest.edgar_adapter import EDGARAdapter
    from ingest.rss_adapter import RSSAdapter
    from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
    from ingest.historical_adapter import HistoricalBackfillAdapter
    from ingest.llm_extraction_adapter import LLMExtractionAdapter
    from ingest.edgar_realtime_adapter import EDGARRealtimeAdapter
    from ingest.options_adapter import OptionsAdapter
    from ingest.polygon_options_adapter import PolygonOptionsAdapter
    from ingest.yield_curve_adapter import YieldCurveAdapter
    from ingest.finra_short_interest_adapter import FINRAShortInterestAdapter
    from ingest.boe_adapter import BoEAdapter
    from ingest.earnings_calendar_adapter import EarningsCalendarAdapter
    from ingest.fca_short_interest_adapter import FCAShortInterestAdapter
    from ingest.lse_flow_adapter import LSEFlowAdapter
    from ingest.insider_adapter import InsiderAdapter
    from ingest.short_interest_adapter import ShortInterestAdapter
    from ingest.sector_rotation_adapter import SectorRotationAdapter
    from ingest.economic_calendar_adapter import EconomicCalendarAdapter
    from ingest.discovery_pipeline import DiscoveryPipeline
    from ingest.eia_adapter import EIAAdapter
    from ingest.gdelt_adapter import GDELTAdapter
    from ingest.ucdp_adapter import UCDPAdapter
    from ingest.acled_adapter import ACLEDAdapter
    from ingest.usgs_adapter import USGSAdapter
    HAS_INGEST = True
except ImportError:
    HAS_INGEST = False

try:
    from analytics.backtest import (
        run_backtest, take_snapshot, list_snapshots,
        run_regime_backtest, list_snapshot_regimes,
    )
    from analytics.portfolio import build_portfolio_summary
    from analytics.alerts import AlertMonitor, get_alerts, mark_alerts_seen
    from analytics.adversarial_stress import run_stress_test, _SCENARIOS as _STRESS_SCENARIOS
    from analytics.counterfactual import run_counterfactual
    HAS_ANALYTICS = True
except ImportError:
    HAS_ANALYTICS = False

try:
    from users.user_store import (
        ensure_user_tables, create_user, get_user, update_preferences,
        upsert_portfolio, get_portfolio, get_user_model,
        get_delivery_history,
    )
    from analytics.user_modeller import build_user_model
    from analytics.snapshot_curator import curate_snapshot
    from notifications.snapshot_formatter import format_snapshot, snapshot_to_dict
    from notifications.telegram_notifier import TelegramNotifier
    from notifications.delivery_scheduler import DeliveryScheduler
    HAS_PRODUCT_LAYER = True
except ImportError:
    HAS_PRODUCT_LAYER = False

try:
    from users.user_store import (
        get_open_patterns, upsert_pattern_signal,
        get_tip_history, update_tip_config, get_user_tier,
        already_tipped_today, log_tip_feedback, get_tip_performance,
        get_user_watchlist_tickers, ensure_tip_feedback_table,
        get_today_chat_count,
    )
    from analytics.pattern_detector import detect_all_patterns, OHLCV
    from analytics.position_calculator import calculate_position
    from notifications.tip_formatter import format_tip, tip_to_dict, TIER_LIMITS
    from notifications.tip_scheduler import TipScheduler
    HAS_PATTERN_LAYER = True
except ImportError:
    HAS_PATTERN_LAYER = False

try:
    from llm.overlay_builder import extract_tickers as _extract_overlay_tickers
    from llm.overlay_builder import build_overlay_cards
    HAS_OVERLAY = True
except ImportError:
    HAS_OVERLAY = False

try:
    from knowledge.epistemic_stress import compute_stress
    HAS_STRESS = True
except ImportError:
    HAS_STRESS = False

try:
    from knowledge.confidence_intervals import (
        ensure_confidence_columns,
        get_confidence_interval,
        get_all_confidence_intervals,
    )
    HAS_CONF_INTERVALS = True
except ImportError:
    HAS_CONF_INTERVALS = False

try:
    from knowledge.causal_graph import (
        ensure_causal_edges_table,
        traverse_causal,
        add_causal_edge,
        list_causal_edges,
    )
    HAS_CAUSAL_GRAPH = True
except ImportError:
    HAS_CAUSAL_GRAPH = False

try:
    from knowledge.epistemic_adaptation import get_adaptation_engine
    HAS_ADAPTATION = True
except ImportError:
    HAS_ADAPTATION = False

try:
    from knowledge.working_state import get_working_state_store
    HAS_WORKING_STATE = True
except ImportError:
    HAS_WORKING_STATE = False

try:
    from knowledge.conversation_store import ConversationStore as _ConvStore, session_id_for_user as _sid_for_user
    HAS_CONV_STORE = True
except ImportError:
    HAS_CONV_STORE = False

try:
    from knowledge.kb_insufficiency_classifier import classify_insufficiency
    HAS_CLASSIFIER = True
except ImportError:
    HAS_CLASSIFIER = False

try:
    from knowledge.kb_repair_proposals import generate_repair_proposals, ensure_repair_proposals_table
    HAS_PROPOSALS = True
except ImportError:
    HAS_PROPOSALS = False

try:
    from knowledge.kb_repair_executor import execute_repair, rollback_repair, repair_impact_score
    HAS_EXECUTOR = True
except ImportError:
    HAS_EXECUTOR = False

try:
    from knowledge.kb_validation import validate_all, governance_verdict
    HAS_VALIDATION = True
except ImportError:
    HAS_VALIDATION = False

try:
    from knowledge.graph_retrieval import build_graph_context, what_do_i_know_about
    HAS_GRAPH_RETRIEVAL = True
except ImportError:
    HAS_GRAPH_RETRIEVAL = False

try:
    from ingest.dynamic_watchlist import DynamicWatchlistManager
    from analytics.universe_expander import (
        resolve_interest, validate_tickers, seed_causal_edges,
        bootstrap_ticker_async, estimate_bootstrap_seconds,
    )
    from analytics.signal_calibration import get_calibration, update_calibration
    from analytics.network_effect_engine import (
        compute_coverage_tier, promote_to_shared_kb, update_refresh_schedule,
        detect_cohort_consensus, compute_trending_markets, compute_network_health,
    )
    from users.personal_kb import (
        get_context_document, infer_and_write_from_portfolio,
        update_from_feedback, update_from_engagement, write_universe_atoms,
    )
    from users.user_store import (
        get_universe_tickers, get_staged_tickers,
        log_engagement_event, get_engagement_events,
        ensure_hybrid_tables,
    )
    HAS_HYBRID = True
except ImportError:
    HAS_HYBRID = False


# ── Middleware imports ────────────────────────────────────────────────────────

try:
    from middleware.auth import (
        require_auth, assert_self, register_user, authenticate_user,
        ensure_user_auth_table, issue_refresh_token, rotate_refresh_token,
    )
    HAS_AUTH = True
except ImportError:
    HAS_AUTH = False
    def require_auth(f):      # type: ignore
        return f
    def assert_self(uid):     # type: ignore  # noqa: E306
        return None

try:
    from middleware.validators import (
        validate_portfolio_submission, validate_onboarding, validate_tip_config,
        validate_ingest_atom, validate_feedback, validate_register,
    )
    HAS_VALIDATORS = True
except ImportError:
    HAS_VALIDATORS = False

if os.environ.get('EVAL_MODE') == '1':
    HAS_LIMITER = False
    def rate_limit(cls):      # type: ignore
        def decorator(f):
            return f
        return decorator
    class _NoOpLimiter:
        def exempt(self, f): return f
        def init_app(self, app): pass
        def limit(self, *a, **kw):
            def decorator(f): return f
            return decorator
    limiter = _NoOpLimiter()  # type: ignore
else:
    try:
        from middleware.rate_limiter import limiter, rate_limit
        HAS_LIMITER = True
    except ImportError:
        HAS_LIMITER = False
        def rate_limit(cls):      # type: ignore
            def decorator(f):
                return f
            return decorator
        class _NoOpLimiter:
            def exempt(self, f): return f
            def init_app(self, app): pass
        limiter = _NoOpLimiter()  # type: ignore

try:
    from middleware.audit import log_audit_event, get_audit_log, ensure_audit_table
    HAS_AUDIT = True
except ImportError:
    HAS_AUDIT = False
    def log_audit_event(*a, **kw): pass  # type: ignore


try:
    from core.tiers import get_tier as _get_tier, check_feature as _check_feature, _next_tier as _next_tier_name
    HAS_TIERS = True
except ImportError:
    HAS_TIERS = False


# ── Tier feature guard ────────────────────────────────────────────────────────

def _get_user_tier_for_request(user_id: str) -> str:
    """Fetch the tier for user_id from the DB. Defaults to 'basic'."""
    try:
        if HAS_PATTERN_LAYER:
            return get_user_tier(_DB_PATH, user_id)
    except Exception:
        pass
    return 'basic'


def require_feature(feature: str):
    """
    Decorator: gate an endpoint by tier feature.
    Must be applied AFTER @require_auth so g.user_id is set.
    Returns 403 with upgrade_required payload when feature is not available.
    """
    from functools import wraps as _wraps
    def decorator(f):
        @_wraps(f)
        def wrapper(*args, **kwargs):
            if not HAS_TIERS:
                return f(*args, **kwargs)
            uid  = getattr(g, 'user_id', None)
            tier = _get_user_tier_for_request(uid) if uid else 'basic'
            if not _check_feature(tier, feature):
                next_t = _next_tier_name(tier)
                return jsonify({
                    'error':        'upgrade_required',
                    'feature':      feature,
                    'current_tier': tier,
                    'upgrade_to':   next_t,
                    'message':      f'This feature requires {next_t} or above',
                }), 403
            return f(*args, **kwargs)
        return wrapper
    return decorator


# ── App setup ─────────────────────────────────────────────────────────────────

app = Flask(__name__)

# ── CORS (must be before rate limiter init) ───────────────────────────────────
try:
    from flask_cors import CORS as _CORS
    _CORS(app, resources={r"/*": {
        "origins": [
            "https://trading-galaxy.uk",
            "https://www.trading-galaxy.uk",
            "https://app.trading-galaxy.uk",
            "https://*.pages.dev",
            "http://localhost:3000",
            "http://localhost:5050",
        ],
        "methods": ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        "allow_headers": ["Authorization", "Content-Type"],
        "supports_credentials": True,
        "max_age": 3600,
    }})
except ImportError:
    pass

# Attach rate limiter
if HAS_LIMITER:
    limiter.init_app(app)

_DB_PATH = os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')
_kg = KnowledgeGraph(db_path=_DB_PATH)

# Start background decay worker (runs every 24h)
_decay_worker = get_decay_worker(_DB_PATH)

# Ensure Bayesian confidence interval columns exist on the facts table
if HAS_CONF_INTERVALS:
    try:
        import sqlite3 as _sqlite3
        _ci_conn = _sqlite3.connect(_DB_PATH)
        ensure_confidence_columns(_ci_conn)
        _ci_conn.close()
    except Exception:
        pass

# Seed causal graph edges table (idempotent — INSERT OR IGNORE)
if HAS_CAUSAL_GRAPH:
    try:
        import sqlite3 as _sqlite3
        _cg_conn = _sqlite3.connect(_DB_PATH)
        ensure_causal_edges_table(_cg_conn)
        _cg_conn.close()
    except Exception:
        pass

# Ensure user management tables exist
if HAS_PRODUCT_LAYER:
    try:
        import sqlite3 as _sqlite3
        _user_conn = _sqlite3.connect(_DB_PATH)
        ensure_user_tables(_user_conn)
        _user_conn.close()
    except Exception:
        pass

# Ensure auth + audit tables exist
try:
    import sqlite3 as _sqlite3
    _auth_conn = _sqlite3.connect(_DB_PATH)
    if HAS_AUTH:
        ensure_user_auth_table(_auth_conn)
    if HAS_AUDIT:
        ensure_audit_table(_auth_conn)
    _auth_conn.commit()
    _auth_conn.close()
except Exception:
    pass

# Ensure hybrid build tables exist (Phase 1)
if HAS_HYBRID:
    try:
        import sqlite3 as _sqlite3
        _hybrid_conn = _sqlite3.connect(_DB_PATH)
        ensure_hybrid_tables(_hybrid_conn)
        _hybrid_conn.close()
    except Exception:
        pass

# ── Intelligence layer: CausalShockEngine + PredictionLedger ──────────────────
# Both injected into _kg so add_fact() can fire hooks on every atom write.

_shock_engine = None
try:
    from analytics.causal_shock_engine import CausalShockEngine as _CSE
    _shock_engine = _CSE(_DB_PATH)
    _kg.set_shock_engine(_shock_engine)
except Exception as _e:
    import logging as _logging
    _logging.getLogger(__name__).warning('CausalShockEngine init failed: %s', _e)

_prediction_ledger = None
try:
    from analytics.prediction_ledger import PredictionLedger as _PL
    _prediction_ledger = _PL(_DB_PATH)
    _kg.set_ledger(_prediction_ledger)
except Exception as _e:
    import logging as _logging
    _logging.getLogger(__name__).warning('PredictionLedger init failed: %s', _e)

_thesis_monitor = None
try:
    from knowledge.thesis_builder import ThesisMonitor as _TM
    _thesis_monitor = _TM(_DB_PATH)
    _kg.set_thesis_monitor(_thesis_monitor)
except Exception as _e:
    import logging as _logging
    _logging.getLogger(__name__).warning('ThesisMonitor init failed: %s', _e)

# ── ConversationStore — persistent chat history + KB atom graduation ──────────
_conv_store = None
if HAS_CONV_STORE:
    try:
        _conv_store = _ConvStore(_DB_PATH)
    except Exception as _e:
        import logging as _logging
        _logging.getLogger(__name__).warning('ConversationStore init failed: %s', _e)

# Auto-seed on first boot: if the DB is empty and the seed file exists, load it.
# This means `docker-compose up` gives collaborators a populated KB immediately —
# no manual load step required.
try:
    import sqlite3 as _sqlite3
    import pathlib as _pathlib
    _SEED_PATH = _pathlib.Path(__file__).parent / 'tests' / 'fixtures' / 'kb_seed.sql'
    if _SEED_PATH.exists():
        _seed_check = _sqlite3.connect(_DB_PATH, timeout=5)
        try:
            _fact_count = _seed_check.execute(
                "SELECT COUNT(*) FROM facts"
            ).fetchone()[0]
        except Exception:
            _fact_count = 0
        finally:
            _seed_check.close()
        if _fact_count < 100:
            import logging as _logging
            _seed_log = _logging.getLogger('api.autoseed')
            _seed_log.info('Auto-seeding KB from %s (%d facts found) …', _SEED_PATH, _fact_count)
            _seed_conn = _sqlite3.connect(_DB_PATH, timeout=15)
            try:
                _seed_conn.executescript(_SEED_PATH.read_text(encoding='utf-8'))
                _after = _seed_conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
                _seed_log.info('Auto-seed complete — %d facts loaded.', _after)
            except Exception as _seed_err:
                _seed_log.warning('Auto-seed failed: %s', _seed_err)
            finally:
                _seed_conn.close()
except Exception:
    pass

# Start seed sync client — polls GitHub Releases hourly, applies newer seeds automatically
_seed_sync = None
try:
    from ingest.seed_sync import SeedSyncClient
    _seed_sync = SeedSyncClient(db_path=_DB_PATH)
    _seed_sync.start()
except Exception:
    pass

# Start delivery scheduler (checks every 60s for due briefings)
_delivery_scheduler = None
if HAS_PRODUCT_LAYER:
    try:
        _delivery_scheduler = DeliveryScheduler(_DB_PATH, check_interval_sec=60)
        _delivery_scheduler.start()
    except Exception:
        pass

# Start tip scheduler (checks every 60s for due pattern tips)
_tip_scheduler = None
if HAS_PATTERN_LAYER:
    try:
        _tip_scheduler = TipScheduler(_DB_PATH, interval_sec=60)
        _tip_scheduler.start()
    except Exception:
        pass

# Start position monitor (checks every 300s for tip-originated position triggers)
_position_monitor = None
try:
    from analytics.position_monitor import PositionMonitor
    _position_monitor = PositionMonitor(_DB_PATH, interval_sec=300)
    _position_monitor.start()
except Exception:
    _position_monitor = None

# Per-session streak store for epistemic adaptation
# { session_id: { 'streak': int, 'last_stress': float } }
_session_streaks: dict = {}

# Per-session last-seen tickers for follow-up carry-forward
# { session_id: [list of canonical ticker strings] }
_session_tickers: dict = {}

# Per-session portfolio tickers — set when user submits/views portfolio,
# persists across turns so "how does X tie in to my portfolio" always
# retrieves all portfolio atoms even after a single-ticker follow-up overwrites _session_tickers
_session_portfolio_tickers: dict = {}

# Start ingest scheduler (adapters run on their own intervals)
_ingest_scheduler = None
if HAS_INGEST:
    try:
        _ingest_scheduler = IngestScheduler(_kg)
        _ingest_scheduler.register(YFinanceAdapter(),                         interval_sec=300)    # 5 min
        _ingest_scheduler.register(SignalEnrichmentAdapter(db_path=_DB_PATH), interval_sec=300)   # 5 min, after yfinance
        _ingest_scheduler.register(RSSAdapter(db_path=_DB_PATH),             interval_sec=900)    # 15 min
        _ingest_scheduler.register(LLMExtractionAdapter(db_path=_DB_PATH),   interval_sec=300)    # 5 min, drains queue
        _ingest_scheduler.register(EDGARAdapter(db_path=_DB_PATH),           interval_sec=21600)  # 6 hours
        _ingest_scheduler.register(EDGARRealtimeAdapter(db_path=_DB_PATH),   interval_sec=180)    # 3 min real-time 8-K
        _ingest_scheduler.register(OptionsAdapter(),                          interval_sec=1800)   # 30 min options chain
        if os.environ.get('POLYGON_API_KEY'):
            _ingest_scheduler.register(PolygonOptionsAdapter(),               interval_sec=1800)   # 30 min Polygon Greeks
            _ingest_scheduler.register(YieldCurveAdapter(),                   interval_sec=86400)  # 24 hours yield curve
        _ingest_scheduler.register(FINRAShortInterestAdapter(db_path=_DB_PATH), interval_sec=86400) # 24 hours FINRA short interest
        _ingest_scheduler.register(FREDAdapter(),                             interval_sec=86400)  # 24 hours
        _ingest_scheduler.register(BoEAdapter(),                              interval_sec=86400)  # 24 hours UK macro
        _ingest_scheduler.register(EarningsCalendarAdapter(db_path=_DB_PATH), interval_sec=3600)   # 1 hour earnings calendar
        _ingest_scheduler.register(FCAShortInterestAdapter(db_path=_DB_PATH), interval_sec=86400)  # 24 hours FCA short interest
        _ingest_scheduler.register(LSEFlowAdapter(db_path=_DB_PATH),          interval_sec=3600)   # 1 hour LSE order flow
        _ingest_scheduler.register(InsiderAdapter(db_path=_DB_PATH),          interval_sec=3600)   # 1 hour Form 4 insider transactions
        _ingest_scheduler.register(ShortInterestAdapter(db_path=_DB_PATH),    interval_sec=86400)  # 24 hours FINRA short interest
        _ingest_scheduler.register(SectorRotationAdapter(db_path=_DB_PATH),   interval_sec=3600)   # 1 hour sector rotation
        _ingest_scheduler.register(EconomicCalendarAdapter(db_path=_DB_PATH), interval_sec=86400)  # 24 hours economic calendar
        _ingest_scheduler.register(EIAAdapter(),                               interval_sec=86400)  # 24 hours EIA oil/energy
        _ingest_scheduler.register(GDELTAdapter(),                             interval_sec=3600)   # 1 hour GDELT bilateral tension
        _ingest_scheduler.register(UCDPAdapter(),                              interval_sec=86400)  # 24 hours UCDP conflict baseline
        _ingest_scheduler.register(ACLEDAdapter(),                             interval_sec=21600)  # 6 hours ACLED protest/unrest
        _ingest_scheduler.register(USGSAdapter(),                              interval_sec=3600)   # 1 hour USGS earthquakes
        _ingest_scheduler.start()
    except Exception as _e:
        import logging as _logging
        _logging.getLogger(__name__).error('Failed to start ingest scheduler: %s', _e)
        _ingest_scheduler = None

# Initialise discovery pipeline (after KG is ready)
_discovery_pipeline = None
if HAS_INGEST:
    try:
        _discovery_pipeline = DiscoveryPipeline(kg=_kg, db_path=_DB_PATH)
    except Exception as _e:
        import logging as _logging
        _logging.getLogger(__name__).error('Failed to init discovery pipeline: %s', _e)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/markets/chart', methods=['GET'])
@limiter.exempt
def markets_chart():
    """
    Serve a standalone TradingView chart page for a given symbol.
    Used as the iframe src= so it gets its own CSP header (not inherited
    from the parent SPA page which blocks external scripts via srcdoc).
    """
    from flask import request as _req, make_response as _make_response
    import json as _json
    sym = _req.args.get('sym', 'FOREXCOM:SPXUSD')
    cfg = _json.dumps({
        'autosize': True, 'symbol': sym, 'interval': 'D',
        'timezone': 'Europe/London', 'theme': 'dark', 'style': '1',
        'locale': 'en', 'toolbar_bg': '#111111',
        'backgroundColor': '#0a0a0a', 'gridColor': '#1a1a1a',
        'hide_side_toolbar': False, 'allow_symbol_change': True,
        'enable_publishing': False, 'save_image': False,
        'support_host': 'https://www.tradingview.com',
    })
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>*{{margin:0;padding:0;box-sizing:border-box;}}html,body{{width:100%;height:100%;overflow:hidden;background:#0a0a0a;}}</style>
</head><body>
<div class="tradingview-widget-container" style="width:100%;height:100%;">
  <div class="tradingview-widget-container__widget" style="width:100%;height:100%;"></div>
  <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js">
  {cfg}
  </script>
</div>
</body></html>"""
    resp = _make_response(html)
    resp.headers['Content-Type'] = 'text/html; charset=utf-8'
    resp.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'unsafe-inline' https://s3.tradingview.com https://*.tradingview.com; "
        "style-src 'unsafe-inline' https://*.tradingview.com; "
        "connect-src https://*.tradingview.com wss://*.tradingview.com; "
        "img-src data: blob: https://*.tradingview.com; "
        "font-src https://*.tradingview.com; "
        "frame-src https://*.tradingview.com; "
        "frame-ancestors *"
    )
    resp.headers['X-Frame-Options'] = 'ALLOWALL'
    return resp


@app.route('/health', methods=['GET'])
@limiter.exempt
def health():
    return jsonify({'status': 'ok', 'db': _DB_PATH})


@app.route('/ingest/status', methods=['GET'])
@limiter.exempt
def ingest_status():
    """
    Health check for the ingest scheduler.

    Returns per-adapter status: last run time, atom count, errors.
    Use this to detect silent failures (e.g. missing FRED_API_KEY,
    yfinance rate limits, network errors).
    """
    if not _ingest_scheduler:
        return jsonify({
            'scheduler': 'not_running',
            'reason': 'ingest dependencies not installed or scheduler failed to start',
            'adapters': {},
        })

    adapter_status = _ingest_scheduler.get_status()
    # Enrich each adapter entry with live KB atom count by source prefix
    try:
        import sqlite3 as _sqlite3
        _sc = _sqlite3.connect(_DB_PATH, timeout=5)
        try:
            # Map adapter name → source prefix pattern
            _src_patterns = {
                'yfinance':              ['exchange_feed_yahoo%', 'yfinance%'],
                'signal_enrichment':     ['derived_signal%', 'signal_enrichment%'],
                'rss_news':              ['news_wire%', 'rss_%'],
                'llm_extraction':        ['llm_extract%'],
                'fred':                  ['macro_data_fred%', 'fred%'],
                'edgar':                 ['regulatory_filing_sec%', 'edgar%'],
                'bne':                   ['bne%'],
                'options':               ['options%'],
                'earnings_calendar':     ['earnings%'],
                'lse_flow':              ['lse%', 'uk_%', 'alt_data_lse%'],
                'fca_short_interest':    ['fca%', 'alt_data_fca%'],
                'edgar_realtime':        ['edgar_realtime%'],
                'insider_transactions':  ['regulatory_filing_sec_form4%'],
                'short_interest':        ['alt_data_finra%'],
                'sector_rotation':       ['derived_signal_sector_rotation%'],
                'economic_calendar_macro': ['macro_data_calendar%'],
            }
            for name, entry in adapter_status.items():
                patterns = _src_patterns.get(name, [])
                total = 0
                for pat in patterns:
                    row = _sc.execute(
                        "SELECT COUNT(*) FROM facts WHERE source LIKE ?", (pat,)
                    ).fetchone()
                    total += row[0] if row else 0
                entry['kb_atoms'] = total
        finally:
            _sc.close()
    except Exception:
        pass
    return jsonify({
        'scheduler': 'running',
        'adapters': adapter_status,
    })


@app.route('/ingest/run-all', methods=['POST'])
def ingest_run_all():
    """
    Trigger an immediate out-of-schedule run of ALL registered ingest adapters.

    Useful for:
      - Seeding a fresh DB quickly
      - Forcing a refresh after a long downtime
      - CI/CD pipeline warm-up

    Body (optional):
      { "adapters": ["yfinance", "rss_news"] }   — limit to named adapters
      {}                                          — run all adapters

    Returns immediately; runs are dispatched to background threads.
    """
    if not _ingest_scheduler:
        return jsonify({'error': 'scheduler not running'}), 503

    data = request.get_json(force=True, silent=True) or {}
    requested = data.get('adapters')  # None = all

    status = _ingest_scheduler.get_status()
    dispatched = []
    skipped    = []

    for name in status:
        if requested and name not in requested:
            continue
        ok = _ingest_scheduler.run_now(name)
        if ok:
            dispatched.append(name)
        else:
            skipped.append(name)

    return jsonify({
        'dispatched': dispatched,
        'skipped':    skipped,
        'note':       'runs are async — poll /ingest/status to track progress',
    })


@app.route('/ingest/historical', methods=['POST'])
def ingest_historical():
    """
    Trigger a one-shot historical summary backfill for the watchlist.

    Downloads 1 year of daily OHLCV via yf.download() and writes
    interpretable summary atoms (returns, vol, drawdown, 52w levels,
    relative performance vs SPY) into the KB.  All atoms are upsert=True
    so repeated calls are safe and idempotent.

    Body (optional):
      { "tickers": ["NVDA", "META"] }  — backfill a subset
      {}                               — backfill full default watchlist

    This is synchronous (runs in the request thread) because the download
    is a single bulk call and typically completes in 5-15 seconds.
    """
    if not HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    data    = request.get_json(force=True, silent=True) or {}
    tickers = data.get('tickers')  # None = full watchlist

    try:
        adapter = HistoricalBackfillAdapter(tickers=tickers)
        result  = adapter.run_and_push(_kg)
        return jsonify({
            'ingested': result.get('ingested', 0),
            'skipped':  result.get('skipped',  0),
            'tickers':  len(adapter.tickers),
        })
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('historical backfill failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/calibrate/historical', methods=['POST'])
def calibrate_historical():
    """
    POST /calibrate/historical

    Back-populate signal_calibration with historical pattern outcome statistics.
    Slides a 100-candle detection window through N years of daily OHLCV,
    checks outcomes against the following 20 candles, and writes aggregated
    hit rates to signal_calibration by (ticker, pattern_type, regime).

    Body (optional):
      { "tickers": ["HSBA.L", "NVDA"],   -- subset (default: full watchlist)
        "lookback_years": 3 }             -- history depth (default: 3)

    Returns:
      { "tickers_calibrated": 12,
        "total_patterns_detected": 4821,
        "total_rows_written": 87,
        "per_ticker": { "HSBA.L": {"patterns_detected": 412, "rows_written": 8}, ... } }

    This runs synchronously. Expect ~3-5 minutes for a full watchlist.
    Run once at launch, then re-run monthly to incorporate new history.
    """
    if not HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    try:
        from analytics.historical_calibration import HistoricalCalibrator
    except ImportError as e:
        return jsonify({'error': f'historical_calibration not available: {e}'}), 503

    data           = request.get_json(force=True, silent=True) or {}
    tickers        = data.get('tickers') or None
    lookback_years = int(data.get('lookback_years', 3))
    lookback_years = max(1, min(lookback_years, 10))

    try:
        cal     = HistoricalCalibrator(db_path=_DB_PATH)
        results = cal.calibrate_watchlist(tickers=tickers, lookback_years=lookback_years)

        total_patterns = sum(r.get('patterns_detected', 0) for r in results.values())
        total_rows     = sum(r.get('calibration_rows_written', 0) for r in results.values())

        return jsonify({
            'tickers_calibrated':      len(results),
            'total_patterns_detected': total_patterns,
            'total_rows_written':      total_rows,
            'lookback_years':          lookback_years,
            'per_ticker': {
                t: {
                    'patterns_detected':       r.get('patterns_detected', 0),
                    'calibration_rows_written': r.get('calibration_rows_written', 0),
                    'error':                   r.get('error'),
                }
                for t, r in results.items()
            },
        })
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('historical calibration failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/calibrate/regime-history', methods=['POST'])
def calibrate_regime_history():
    """
    POST /calibrate/regime-history

    Classify each historical month into a macro regime (risk_on_expansion,
    risk_off_contraction, stagflation, recovery) using cross-asset proxy data,
    then write regime-conditional performance atoms to the KB for each ticker:

      global_macro_regime | regime_history_2022_06 | risk_off_contraction
      HSBA.L              | return_in_risk_on_expansion  | "+3.2"
      HSBA.L              | regime_hit_rate_risk_off_contraction | "38.5"
      HSBA.L              | best_regime  | risk_on_expansion (+3.2%/mo)
      HSBA.L              | worst_regime | risk_off_contraction (-2.1%/mo)

    Body (optional):
      { "lookback_years": 5,
        "tickers": ["HSBA.L", "BP.L"] }  -- default: full watchlist

    Runs synchronously (~1–2 minutes for full watchlist).
    Run once at launch; re-run quarterly to incorporate new history.
    """
    if not HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    try:
        from analytics.regime_history import RegimeHistoryClassifier
    except ImportError as e:
        return jsonify({'error': f'regime_history not available: {e}'}), 503

    data           = request.get_json(force=True, silent=True) or {}
    lookback_years = int(data.get('lookback_years', 5))
    lookback_years = max(1, min(lookback_years, 10))
    tickers        = data.get('tickers') or None

    try:
        clf    = RegimeHistoryClassifier(db_path=_DB_PATH)
        result = clf.run(tickers=tickers, lookback_years=lookback_years)
        return jsonify({**result, 'lookback_years': lookback_years})
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('regime history failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/ingest/patterns', methods=['POST'])
def ingest_patterns():
    """
    Trigger pattern detection across all KB tickers that have last_price atoms.

    Fetches 6 months of daily OHLCV via yfinance, runs detect_all_patterns(),
    and inserts new signals into pattern_signals. Skips duplicates.

    Body (optional):
      { "tickers": ["NVDA", "META"] }  — run on a subset only
      {}                               — run on all KB tickers with last_price
    """
    if not HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    import sqlite3 as _sq
    try:
        from analytics.pattern_detector import detect_all_patterns, OHLCV as _OHLCV
        import yfinance as _yf
    except ImportError as e:
        return jsonify({'error': f'pattern detection not available: {e}'}), 503

    _YF_MAP = {
        'xauusd': 'GC=F',  'xagusd': 'SI=F',  'xptusd': 'PL=F',
        'cl': 'CL=F',      'bz': 'BZ=F',       'ng': 'NG=F',
        'gbpusd': 'GBPUSD=X', 'eurusd': 'EURUSD=X', 'usdjpy': 'JPY=X',
        'dxy': 'DX-Y.NYB',
        'spx': '^GSPC',    'ndx': '^NDX',       'dji': '^DJI',
        'ftse': '^FTSE',   'dax': '^GDAXI',     'vix': '^VIX',
    }

    data    = request.get_json(force=True, silent=True) or {}
    filter_tickers = [t.lower() for t in data.get('tickers', [])]

    conn = _sq.connect(_DB_PATH, timeout=15)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, pattern_type TEXT NOT NULL,
            direction TEXT NOT NULL, zone_high REAL NOT NULL,
            zone_low REAL NOT NULL, zone_size_pct REAL,
            timeframe TEXT NOT NULL, formed_at TEXT,
            status TEXT NOT NULL DEFAULT 'open', filled_at TEXT,
            quality_score REAL, kb_conviction TEXT DEFAULT '',
            kb_regime TEXT DEFAULT '', kb_signal_dir TEXT DEFAULT '',
            alerted_users TEXT DEFAULT '[]', detected_at TEXT
        )
    """)
    conn.commit()

    # Load KB tickers
    rows = conn.execute(
        "SELECT DISTINCT subject FROM facts WHERE predicate = 'last_price'"
    ).fetchall()
    tickers = [r[0] for r in rows]
    if filter_tickers:
        tickers = [t for t in tickers if t.lower() in filter_tickers]

    now_iso = datetime.now(timezone.utc).isoformat()
    total_inserted = 0
    total_tickers  = 0

    for ticker in tickers:
        yf_sym = _YF_MAP.get(ticker.lower(), ticker.upper())
        atoms_rows = conn.execute(
            "SELECT predicate, object FROM facts WHERE subject = ?", (ticker,)
        ).fetchall()
        atoms_map = {r[0]: r[1] for r in atoms_rows}

        try:
            hist = _yf.Ticker(yf_sym).history(period='6mo', interval='1d', auto_adjust=True)
            if hist.empty or len(hist) < 10:
                continue
            candles = [
                _OHLCV(
                    timestamp=ts.isoformat(),
                    open=float(row['Open']), high=float(row['High']),
                    low=float(row['Low']),   close=float(row['Close']),
                    volume=float(row.get('Volume', 0) or 0),
                )
                for ts, row in hist.iterrows()
            ]
            signals = detect_all_patterns(
                candles, ticker=ticker.upper(), timeframe='1d',
                kb_conviction=atoms_map.get('conviction_tier', ''),
                kb_regime=atoms_map.get('price_regime', ''),
                kb_signal_dir=atoms_map.get('signal_direction', ''),
            )
            inserted = 0
            for sig in signals:
                exists = conn.execute(
                    """SELECT 1 FROM pattern_signals
                       WHERE ticker=? AND pattern_type=? AND formed_at=? AND timeframe=?
                       LIMIT 1""",
                    (sig.ticker, sig.pattern_type, sig.formed_at, sig.timeframe),
                ).fetchone()
                if exists:
                    continue
                conn.execute(
                    """INSERT INTO pattern_signals
                       (ticker, pattern_type, direction, zone_high, zone_low,
                        zone_size_pct, timeframe, formed_at, status,
                        quality_score, kb_conviction, kb_regime, kb_signal_dir,
                        alerted_users, detected_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'[]',?)""",
                    (sig.ticker, sig.pattern_type, sig.direction,
                     sig.zone_high, sig.zone_low, sig.zone_size_pct,
                     sig.timeframe, sig.formed_at, sig.status,
                     sig.quality_score, sig.kb_conviction,
                     sig.kb_regime, sig.kb_signal_dir, now_iso),
                )
                inserted += 1
            conn.commit()
            total_inserted += inserted
            total_tickers  += 1
        except Exception as _e:
            import logging as _logging
            _logging.getLogger(__name__).warning('pattern detection failed for %s: %s', ticker, _e)

    conn.close()
    total_now = _sq.connect(_DB_PATH).execute(
        "SELECT COUNT(*) FROM pattern_signals"
    ).fetchone()[0]
    return jsonify({
        'tickers_processed': total_tickers,
        'patterns_inserted': total_inserted,
        'pattern_signals_total': total_now,
    })


@app.route('/stats', methods=['GET'])
@limiter.exempt
def stats():
    base = _kg.get_stats()
    conn = _kg.thread_local_conn()
    c = conn.cursor()
    extras = {}

    # Conflict audit count
    try:
        c.execute("SELECT COUNT(*) FROM fact_conflicts")
        extras['total_conflicts_detected'] = c.fetchone()[0]
    except Exception:
        extras['total_conflicts_detected'] = 0

    # Top 5 most-retrieved atoms (hit_count)
    try:
        c.execute("""
            SELECT subject, predicate, SUM(hit_count) as hits
            FROM facts
            WHERE hit_count > 0
            GROUP BY subject, predicate
            ORDER BY hits DESC
            LIMIT 5
        """)
        extras['top_retrieved_atoms'] = [
            {'subject': r[0], 'predicate': r[1], 'hits': r[2]}
            for r in c.fetchall()
        ]
    except Exception:
        extras['top_retrieved_atoms'] = []

    # Pending repair proposals
    try:
        c.execute("SELECT COUNT(*) FROM repair_proposals WHERE status = 'pending'")
        extras['pending_repair_proposals'] = c.fetchone()[0]
    except Exception:
        extras['pending_repair_proposals'] = 0

    # Unprocessed domain refresh queue entries
    try:
        c.execute("SELECT COUNT(*) FROM domain_refresh_queue WHERE processed = 0")
        extras['domain_refresh_queue_depth'] = c.fetchone()[0]
    except Exception:
        extras['domain_refresh_queue_depth'] = 0

    # Active adaptation sessions (streak > 0)
    extras['adaptation_sessions_active'] = sum(
        1 for s in _session_streaks.values() if s.get('streak', 0) > 0
    )
    extras['adaptation_sessions_total'] = len(_session_streaks)

    # KB insufficient events in last 7 days
    try:
        from datetime import timedelta as _td
        cutoff = (datetime.now(timezone.utc) - _td(days=7)).isoformat()
        c.execute(
            "SELECT COUNT(*) FROM kb_insufficient_log WHERE detected_at >= ?",
            (cutoff,)
        )
        extras['kb_insufficient_events_7d'] = c.fetchone()[0]
    except Exception:
        extras['kb_insufficient_events_7d'] = 0

    # Current market regime — query facts table directly
    # signal_enrichment_adapter writes: subject='market', predicate='market_regime'
    try:
        row = c.execute("""
            SELECT object FROM facts
            WHERE subject = 'market' AND predicate = 'market_regime'
            ORDER BY timestamp DESC LIMIT 1
        """).fetchone()
        if not row:
            row = c.execute("""
                SELECT object FROM facts
                WHERE predicate IN ('market_regime', 'regime_label', 'current_regime')
                ORDER BY timestamp DESC LIMIT 1
            """).fetchone()
        extras['market_regime'] = row[0] if row else None
    except Exception:
        extras['market_regime'] = None

    # Regime detail fields — volatility, sector lead, KB confidence
    try:
        _vrow = c.execute("""
            SELECT object FROM facts
            WHERE predicate IN ('volatility_regime','market_volatility','vix_regime')
            ORDER BY timestamp DESC LIMIT 1
        """).fetchone()
        extras['regime_volatility'] = _vrow[0] if _vrow else None
    except Exception:
        extras['regime_volatility'] = None

    try:
        # Leading sector = the sector with the highest count of bullish conviction_tier facts
        _srow = c.execute("""
            SELECT subject, COUNT(*) as cnt FROM facts
            WHERE predicate = 'sector'
            GROUP BY object
            ORDER BY cnt DESC LIMIT 1
        """).fetchone()
        # Better: look for explicit sector_rotation or leading_sector fact
        _srrow = c.execute("""
            SELECT object FROM facts
            WHERE predicate IN ('leading_sector','sector_rotation','top_sector')
            ORDER BY timestamp DESC LIMIT 1
        """).fetchone()
        extras['regime_sector_lead'] = _srrow[0] if _srrow else None
    except Exception:
        extras['regime_sector_lead'] = None

    try:
        # KB confidence = fraction of facts with confidence > 0.7
        _total_row = c.execute("SELECT COUNT(*) FROM facts").fetchone()
        _high_row  = c.execute("SELECT COUNT(*) FROM facts WHERE confidence >= 0.7").fetchone()
        _total = _total_row[0] if _total_row else 0
        _high  = _high_row[0]  if _high_row  else 0
        extras['regime_kb_confidence'] = round(_high / _total * 100, 1) if _total > 0 else None
    except Exception:
        extras['regime_kb_confidence'] = None

    # Open patterns count — use min_quality=0.0 to match Patterns screen
    try:
        if HAS_PRODUCT_LAYER:
            from users.user_store import get_open_patterns as _gop
            pats = _gop(_DB_PATH, min_quality=0.0, limit=500)
            extras['open_patterns'] = len(pats)
    except Exception:
        pass

    return jsonify({**base, **extras})


@app.route('/market/snapshot', methods=['GET'])
@limiter.exempt
def market_snapshot():
    """
    GET /market/snapshot

    Returns last known price and 1-month return for a fixed set of market
    benchmark symbols, read directly from KB facts (written by yfinance adapter).
    No live network call — cached KB data only.

    Response: { "symbols": { "^GSPC": {"price": 5123.4, "return_1m": 2.1}, ... }, "as_of": "..." }
    """
    _SNAPSHOT_SYMS = ['^GSPC', '^NDX', '^FTSE', 'GLD', 'DX-Y.NYB']
    import sqlite3 as _sq
    result = {}
    try:
        conn = _sq.connect(_DB_PATH, timeout=5)
        for sym in _SNAPSHOT_SYMS:
            sym_upper = sym.upper()
            row_price = conn.execute(
                """SELECT object, timestamp FROM facts
                   WHERE UPPER(subject) = ? AND predicate IN ('last_price','price','close')
                   ORDER BY timestamp DESC LIMIT 1""",
                (sym_upper,)
            ).fetchone()
            row_ret = conn.execute(
                """SELECT object FROM facts
                   WHERE UPPER(subject) = ? AND predicate IN ('return_1m','change_pct_1m','pct_change_1m')
                   ORDER BY timestamp DESC LIMIT 1""",
                (sym_upper,)
            ).fetchone()
            entry = {}
            if row_price:
                try:
                    entry['price'] = float(row_price[0])
                    entry['as_of'] = row_price[1]
                except (TypeError, ValueError):
                    pass
            if row_ret:
                try:
                    entry['return_1m'] = float(row_ret[0])
                except (TypeError, ValueError):
                    pass
            result[sym] = entry
        conn.close()
    except Exception as e:
        import logging as _log
        _log.getLogger(__name__).warning('market/snapshot failed: %s', e)

    return jsonify({
        'symbols': result,
        'as_of': datetime.now(timezone.utc).isoformat(),
    })


@app.route('/opportunities', methods=['POST'])
@require_auth
@require_feature('opportunity_scan')
def opportunities_endpoint():
    """
    POST /opportunities

    Run an on-demand opportunity scan against the KB and return structured results.
    No LLM call — pure KB scan, fast (<100ms).

    Body (optional):
      {
        "query":  "make me a daytime trading strategy",  // free-text OR
        "modes":  ["intraday", "momentum"],               // explicit mode list
        "limit":  6                                       // max results per mode
      }

    Modes: broad_screen | intraday | momentum | gap_fill | sector_rotation |
           squeeze | macro_gap | mean_reversion

    Returns:
      {
        "mode":           "intraday+momentum",
        "generated_at":   "2026-02-27T16:00:00Z",
        "market_regime":  "risk_off_contraction",
        "market_context": "Market regime: risk off contraction | ...",
        "results": [
          {
            "ticker":          "NVDA",
            "mode":            "momentum",
            "score":           4.2,
            "conviction_tier": "high",
            "signal_direction":"long",
            "signal_quality":  "confirmed",
            "upside_pct":      "18.5",
            "position_size_pct": "3.2",
            "thesis":          "Price 875.0 (mid range) | signal long | ...",
            "rationale":       "bullish signal | confirmed signal | sector tailwind",
            "pattern":         "FVG bullish 1d",
            "extra":           { "sector_tailwind": "positive", ... }
          }, ...
        ],
        "scan_notes": []
      }
    """
    try:
        from analytics.opportunity_engine import (
            run_opportunity_scan, format_scan_as_context,
        )
        from dataclasses import asdict
    except ImportError:
        return jsonify({'error': 'opportunity engine not available'}), 503

    data   = request.get_json(force=True, silent=True) or {}
    query  = data.get('query', '')
    modes  = data.get('modes') or None
    limit  = int(data.get('limit', 6))

    if not query and not modes:
        query = 'broad screen'

    try:
        scan = run_opportunity_scan(
            query=query,
            db_path=_DB_PATH,
            modes=modes,
            limit_per_mode=limit,
        )
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('opportunities scan failed: %s', e)
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'mode':           scan.mode,
        'generated_at':   scan.generated_at,
        'market_regime':  scan.market_regime,
        'market_context': scan.market_context,
        'results': [
            {
                'ticker':            r.ticker,
                'mode':              r.mode,
                'score':             round(r.score, 3),
                'conviction_tier':   r.conviction_tier,
                'signal_direction':  r.signal_direction,
                'signal_quality':    r.signal_quality,
                'upside_pct':        r.upside_pct,
                'position_size_pct': r.position_size_pct,
                'thesis':            r.thesis,
                'rationale':         r.rationale,
                'pattern':           r.pattern,
                'extra':             r.extra,
            }
            for r in scan.results
        ],
        'scan_notes': scan.scan_notes,
    })


@app.route('/discover/<ticker>', methods=['POST'])
def discover_ticker(ticker: str):
    """
    POST /discover/<ticker>

    Trigger the universal discovery pipeline for a single ticker.
    Runs all enrichment stages that are missing or stale in the KB,
    commits atoms back to the shared KB, and returns a summary of
    what stages ran and how many atoms were written.

    Body (optional):
      { "force": true }   — ignore staleness thresholds, run all stages

    Returns:
      {
        "ticker":         "RR.L",
        "status":         "enriched",      // fresh | enriched | partial | failed
        "stages_run":     ["price", "historical", "patterns"],
        "stages_skipped": ["options"],
        "atoms_written":  24,
        "duration_ms":    3450,
        "staleness":      { "last_price": 95.3, "signal_direction": 182.1 }
      }

    Use this endpoint to:
      - Manually refresh a ticker before a presentation or briefing
      - Verify the discovery pipeline works for a new ticker
      - Trigger enrichment without waiting for the next scheduled adapter run
      - Debug why a ticker has thin KB coverage
    """
    if _discovery_pipeline is None:
        return jsonify({'error': 'discovery pipeline not available'}), 503

    ticker = ticker.upper().strip()
    if not ticker:
        return jsonify({'error': 'ticker is required'}), 400

    data  = request.get_json(force=True, silent=True) or {}
    force = bool(data.get('force', False))

    # Assess staleness first (always returned for transparency)
    staleness = _discovery_pipeline.assess_staleness(ticker)

    if not staleness and not force:
        return jsonify({
            'ticker':         ticker,
            'status':         'fresh',
            'stages_run':     [],
            'stages_skipped': [],
            'atoms_written':  0,
            'duration_ms':    0,
            'staleness':      {},
            'message':        'All atoms are fresh — use {"force": true} to re-run anyway',
        })

    # If forced, temporarily mark all thresholds as 0 so everything runs
    if force and not staleness:
        from ingest.discovery_pipeline import STALENESS_THRESHOLDS
        staleness = {p: float('inf') for p in STALENESS_THRESHOLDS}

    try:
        user_id = getattr(g, 'user_id', None)
        result  = _discovery_pipeline.discover(ticker, trigger='manual', user_id=user_id)
        return jsonify({
            'ticker':         result.ticker,
            'status':         result.status,
            'stages_run':     result.stages_run,
            'stages_skipped': result.stages_skipped,
            'atoms_written':  result.atoms_written,
            'duration_ms':    result.duration_ms,
            'staleness':      {k: round(v, 1) for k, v in staleness.items()
                               if v != float('inf')},
        })
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('discovery failed for %s: %s', ticker, e)
        return jsonify({'error': str(e), 'ticker': ticker}), 500


@app.route('/ingest', methods=['POST'])
def ingest():
    """
    Ingest one or more atoms into the KB.

    Body (JSON):
      Single atom:
        {
          "subject":    "AAPL",
          "predicate":  "signal_direction",
          "object":     "long",
          "confidence": 0.85,
          "source":     "model_signal_momentum_v1"
        }

      Batch:
        { "atoms": [ {...}, {...} ] }

    Returns:
        { "ingested": N, "skipped": M }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'error': 'invalid JSON'}), 400

    atoms: list = data.get('atoms') or [data]

    ingested = 0
    skipped = 0
    is_single = 'atoms' not in data
    for atom in atoms:
        subject   = atom.get('subject')
        predicate = atom.get('predicate')
        obj       = atom.get('object')
        if not (subject and predicate and obj):
            skipped += 1
            # Single-atom call with missing required field → 400
            if is_single:
                return jsonify({'error': 'subject, predicate and object are all required'}), 400
            continue
        ok = _kg.add_fact(
            subject=subject,
            predicate=predicate,
            object=obj,
            confidence=float(atom.get('confidence', 0.5)),
            source=atom.get('source', 'unverified_api'),
            metadata=atom.get('metadata'),
        )
        if ok:
            ingested += 1
        else:
            skipped += 1

    return jsonify({'ingested': ingested, 'skipped': skipped})


@app.route('/query', methods=['GET'])
def query():
    """
    Direct triple-store query with optional filters.

    Query params: subject, predicate, object, limit (default 50)
    """
    subject   = request.args.get('subject')
    predicate = request.args.get('predicate')
    obj       = request.args.get('object')
    limit     = int(request.args.get('limit', 50))

    results = _kg.query(subject=subject, predicate=predicate, object=obj, limit=limit)
    return jsonify({'results': results, 'count': len(results)})


@app.route('/retrieve', methods=['POST'])
def retrieve_endpoint():
    """
    Smart multi-strategy retrieval for a natural-language or structured query.

    Body (JSON):
      {
        "message": "What is the current signal on AAPL?",
        "session_id": "optional-session-id"
      }

    Returns:
      {
        "snippet": "=== TRADING KNOWLEDGE CONTEXT ===\\n...",
        "atoms":   [ { subject, predicate, object, source, confidence }, ... ],
        "stress":  { composite_stress, decay_pressure, ... }   // if available
      }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'error': 'invalid JSON'}), 400

    message    = data.get('message', '')
    session_id = data.get('session_id', 'default')
    goal       = data.get('goal')
    topic      = data.get('topic')
    turn_count = int(data.get('turn_count', 1))
    limit      = int(data.get('limit', 30))

    conn = _kg.thread_local_conn()

    prior_context = None
    if HAS_WORKING_STATE:
        try:
            ws = get_working_state_store(_DB_PATH)
            if turn_count == 0:
                prior_context = ws.format_prior_context(session_id) or None
            ws.maybe_persist(
                session_id, turn_count,
                goal=goal, topic=topic,
                force=(turn_count == 1),
            )
        except Exception:
            pass

    # ── Compute adaptation nudges from prior stress streak ──────────────────
    nudges = None
    if HAS_ADAPTATION and HAS_STRESS:
        try:
            from knowledge.epistemic_adaptation import ensure_adaptation_tables
            ensure_adaptation_tables(conn)
            engine = get_adaptation_engine(session_id, db_path=_DB_PATH)
            engine._session_id = session_id
            # Build a minimal SystemState with streak
            sess = _session_streaks.setdefault(session_id, {'streak': 0, 'last_stress': 0.0})

            class _StateStub:
                pass
            state_stub = _StateStub()
            state_stub.epistemic_stress_streak = sess['streak']
            state_stub._session_id = session_id

            # We need stress_report to compute nudges — compute it now from prior atoms
            # This is a zero-cost re-use: nudges are based on the PREVIOUS turn's stress.
            # The current turn's stress is computed after retrieve() and updates streak.
            class _StressStub:
                composite_stress = sess['last_stress']
                decay_pressure   = 0.0
                authority_conflict = 0.0
                supersession_density = 0.0
                conflict_cluster = 0.0
                domain_entropy   = 1.0
            nudges = engine.compute(state_stub, _StressStub(), topic=topic, key_terms=[])
        except Exception:
            nudges = None

    snippet, atoms = retrieve(message, conn, limit=limit, nudges=nudges)

    response: dict = {
        'snippet': snippet,
        'atoms':   atoms,
    }
    if prior_context:
        response['prior_context'] = prior_context

    # Attach epistemic stress if available
    stress_report = None
    if HAS_STRESS and atoms:
        try:
            words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)
            key_terms = list({w.lower() for w in words if len(w) > 2})[:10]
            stress_report = compute_stress(atoms, key_terms, conn)
            response['stress'] = {
                'composite_stress':    stress_report.composite_stress,
                'decay_pressure':      stress_report.decay_pressure,
                'authority_conflict':  stress_report.authority_conflict,
                'supersession_density': stress_report.supersession_density,
                'conflict_cluster':    stress_report.conflict_cluster,
                'domain_entropy':      stress_report.domain_entropy,
            }
        except Exception:
            pass

    # ── Update session streak from this turn's stress ────────────────────────
    if HAS_ADAPTATION and stress_report:
        try:
            from knowledge.epistemic_adaptation import _STRESS_STREAK_THRESHOLD
            sess = _session_streaks.setdefault(session_id, {'streak': 0, 'last_stress': 0.0})
            if stress_report.composite_stress >= _STRESS_STREAK_THRESHOLD:
                sess['streak'] = sess.get('streak', 0) + 1
            else:
                sess['streak'] = max(0, sess.get('streak', 0) - 1)
            sess['last_stress'] = stress_report.composite_stress
        except Exception:
            pass

    # ── Attach adaptation nudges to response ──────────────────────────────────
    if nudges is not None and nudges.is_active():
        response['adaptation'] = {
            'streak':                   nudges.streak,
            'consolidation_mode':       nudges.consolidation_mode,
            'retrieval_scope_broadened': nudges.retrieval_scope_broadened,
            'prefer_high_authority':    nudges.prefer_high_authority,
            'prefer_recent':            nudges.prefer_recent,
            'refresh_domain_queued':    nudges.refresh_domain_queued,
            'conflict_synthesis_queued': nudges.conflict_synthesis_queued,
            'kb_insufficient':          nudges.kb_insufficient,
        }
        # Dispatch domain_refresh_queue entries to ingest scheduler
        if nudges.refresh_domain_queued and _ingest_scheduler and topic:
            try:
                _ingest_scheduler.run_now('yfinance')
            except Exception:
                pass

    # KB insufficiency classification — fires when stress is elevated or coverage thin
    if HAS_CLASSIFIER and stress_report and atoms:
        try:
            import re as _re
            _tickers = [t for t in _re.findall(r'\b[A-Z]{2,5}\b', message)
                        if t not in {'THE','IS','AT','ON','AN','AND','OR','FOR','IN','OF',
                                     'TO','THAT','THIS','WITH','FROM','BY','ARE','WAS','BE',
                                     'HAS','HAVE','HAD','ITS','DO','DID','WHAT','HOW','WHY',
                                     'WHEN','WHERE','WHO','CAN','WILL','NOT','BUT','ALL'}]
            _terms = [w.lower() for w in _re.findall(r'\b[a-zA-Z][a-zA-Z0-9]{2,}\b', message)]
            composite = getattr(stress_report, 'composite_stress', 0.0)
            atom_count = len(atoms)
            # Classify when stress elevated (>0.35) or very few atoms returned (<8)
            if composite > 0.35 or atom_count < 8:
                topic_hint = (topic or
                              (_tickers[0] if _tickers else None) or
                              (_terms[0] if _terms else None) or
                              message[:40])
                diagnosis = classify_insufficiency(topic_hint, stress_report, conn)
                response['kb_diagnosis'] = {
                    'topic':         diagnosis.topic,
                    'types':         [t.value for t in diagnosis.types],
                    'primary_type':  diagnosis.primary_type().value,
                    'confidence':    diagnosis.confidence,
                    'matched_rules': diagnosis.matched_rules,
                    'signals':       diagnosis.signals,
                }
        except Exception:
            pass

    return jsonify(response)


@app.route('/search', methods=['GET'])
def search():
    """
    Full-text search over the KB.

    Query params: q (required), category (optional), limit (default 20)
    """
    q        = request.args.get('q', '')
    category = request.args.get('category')
    limit    = int(request.args.get('limit', 20))

    if not q:
        return jsonify({'error': 'q is required'}), 400

    results = _kg.search(q, limit=limit, category=category)
    return jsonify({'results': results, 'count': len(results)})


@app.route('/context/<entity>', methods=['GET'])
def context(entity: str):
    """
    Get all facts connected to a specific entity (ticker, concept, thesis ID).

    Path param: entity — e.g. 'AAPL', 'fed_rate_thesis_2024'
    """
    facts = _kg.get_context(entity)
    return jsonify({'entity': entity, 'facts': facts, 'count': len(facts)})


# ── Governance / Repair endpoints ─────────────────────────────────────────────

@app.route('/repair/diagnose', methods=['POST'])
def repair_diagnose():
    """
    Run KB insufficiency classification for a topic.

    Body: { "topic": "NVDA" }

    Returns an InsufficiencyDiagnosis: types, signals, confidence.
    Does NOT modify any data.
    """
    if not HAS_CLASSIFIER:
        return jsonify({'error': 'kb_insufficiency_classifier not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    topic = data.get('topic', '').strip()
    if not topic:
        return jsonify({'error': 'topic is required'}), 400

    conn = _kg.thread_local_conn()

    # Build a minimal stress_report stub for the classifier
    class _StressStub:
        conflict_cluster      = 0.0
        supersession_density  = 0.0
        authority_conflict    = 0.0
        domain_entropy        = 1.0

    # Use real stress if available
    stress_stub = _StressStub()
    if HAS_STRESS:
        try:
            from retrieval import retrieve as _retrieve
            _, atoms = _retrieve(topic, conn, limit=50)
            if atoms:
                from knowledge.epistemic_stress import compute_stress
                stress_stub = compute_stress(atoms, [topic], conn)
        except Exception:
            pass

    try:
        diagnosis = classify_insufficiency(topic, stress_stub, conn)
        return jsonify({
            'topic':         diagnosis.topic,
            'types':         [t.value for t in diagnosis.types],
            'primary_type':  diagnosis.primary_type().value,
            'confidence':    diagnosis.confidence,
            'matched_rules': diagnosis.matched_rules,
            'total_rules':   diagnosis.total_rules,
            'signals':       diagnosis.signals,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/repair/proposals', methods=['POST'])
def repair_proposals():
    """
    Generate repair proposals for a topic.

    Body: { "topic": "NVDA" }

    Returns a list of repair proposals (never executed here).
    Requires kb_repair_proposals to be available.
    """
    if not HAS_PROPOSALS:
        return jsonify({'error': 'kb_repair_proposals not available'}), 503
    if not HAS_CLASSIFIER:
        return jsonify({'error': 'kb_insufficiency_classifier not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    topic = data.get('topic', '').strip()
    if not topic:
        return jsonify({'error': 'topic is required'}), 400

    conn = _kg.thread_local_conn()

    class _StressStub:
        conflict_cluster      = 0.0
        supersession_density  = 0.0
        authority_conflict    = 0.0
        domain_entropy        = 1.0

    stress_stub = _StressStub()
    if HAS_STRESS:
        try:
            from retrieval import retrieve as _retrieve
            _, atoms = _retrieve(topic, conn, limit=50)
            if atoms:
                from knowledge.epistemic_stress import compute_stress
                stress_stub = compute_stress(atoms, [topic], conn)
        except Exception:
            pass

    try:
        diagnosis = classify_insufficiency(topic, stress_stub, conn)
        proposals = generate_repair_proposals(diagnosis, conn)
        return jsonify({
            'topic':     topic,
            'diagnosis': {
                'types':      [t.value for t in diagnosis.types],
                'confidence': diagnosis.confidence,
            },
            'proposals': [
                {
                    'id':          p.proposal_id,
                    'strategy':    p.strategy.value if hasattr(p.strategy, 'value') else str(p.strategy),
                    'description': p.description,
                    'is_primary':  p.is_primary,
                    'preview':     p.preview.to_dict() if hasattr(p.preview, 'to_dict') else {},
                    'simulation':  p.simulation.to_dict() if hasattr(p.simulation, 'to_dict') else {},
                    'validation':  p.validation.to_dict() if hasattr(p.validation, 'to_dict') else {},
                }
                for p in (proposals if isinstance(proposals, list) else [])
            ],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/repair/execute', methods=['POST'])
def repair_execute():
    """
    Execute a repair proposal by ID.

    Body: { "proposal_id": "<uuid>", "dry_run": true }

    dry_run=true (default) returns what would change without modifying data.
    Set dry_run=false to apply the repair — irreversible until rollback.

    This endpoint is human-gated. Always inspect proposals via /repair/proposals first.
    """
    if not HAS_EXECUTOR:
        return jsonify({'error': 'kb_repair_executor not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    proposal_id = data.get('proposal_id', '').strip()
    dry_run     = bool(data.get('dry_run', True))

    if not proposal_id:
        return jsonify({'error': 'proposal_id is required'}), 400

    try:
        import dataclasses as _dc
        result = execute_repair(proposal_id, _DB_PATH, dry_run=dry_run)
        return jsonify(_dc.asdict(result) if _dc.is_dataclass(result) else result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/repair/rollback', methods=['POST'])
def repair_rollback():
    """
    Roll back a previously executed repair.

    Body: { "proposal_id": "<uuid>" }
    """
    if not HAS_EXECUTOR:
        return jsonify({'error': 'kb_repair_executor not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    proposal_id = data.get('proposal_id', '').strip()
    if not proposal_id:
        return jsonify({'error': 'proposal_id is required'}), 400

    try:
        import dataclasses as _dc
        result = rollback_repair(proposal_id, _DB_PATH)
        return jsonify(_dc.asdict(result) if _dc.is_dataclass(result) else result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/repair/impact', methods=['GET'])
def repair_impact():
    """
    Aggregate repair calibration metrics across all executed repairs.

    Query params: strategy (optional filter)
    """
    if not HAS_EXECUTOR:
        return jsonify({'error': 'kb_repair_executor not available'}), 503

    strategy = request.args.get('strategy')
    try:
        import dataclasses as _dc
        result = repair_impact_score(strategy, _DB_PATH)
        return jsonify(_dc.asdict(result) if _dc.is_dataclass(result) else result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/adapt/status', methods=['GET'])
def adapt_status():
    """
    Return epistemic adaptation state for all active sessions.

    Query params: session_id (optional — if omitted, returns all sessions)

    Each entry:
      streak         — consecutive high-stress turns
      last_stress    — composite_stress from most recent turn
      adaptation_active — True if streak has triggered any nudges
    """
    session_id = request.args.get('session_id')
    if session_id:
        sess = _session_streaks.get(session_id, {'streak': 0, 'last_stress': 0.0})
        return jsonify({
            'session_id': session_id,
            'streak': sess['streak'],
            'last_stress': sess['last_stress'],
        })
    return jsonify({
        sid: {'streak': s['streak'], 'last_stress': s['last_stress']}
        for sid, s in _session_streaks.items()
    })


@app.route('/adapt/reset', methods=['POST'])
def adapt_reset():
    """
    Reset the epistemic stress streak for a session.

    Body: { "session_id": "default" }

    Use when a topic shift or new session should clear accumulated stress history.
    """
    data = request.get_json(force=True, silent=True) or {}
    session_id = data.get('session_id', 'default')
    if session_id in _session_streaks:
        _session_streaks[session_id] = {'streak': 0, 'last_stress': 0.0}
    # Also clear ticker carry-forward so new topic starts fresh
    _session_tickers.pop(session_id, None)
    _session_portfolio_tickers.pop(session_id, None)
    return jsonify({'session_id': session_id, 'reset': True})


@app.route('/kb/graph', methods=['POST'])
def kb_graph():
    """
    Graph-structured context for a topic or query.

    Body: { "message": "How does Fed policy affect tech stocks?" }

    Returns PageRank centrality, concept clusters, BFS paths between query concepts,
    and key relationships — the relational layer above flat atom retrieval.
    """
    if not HAS_GRAPH_RETRIEVAL:
        return jsonify({'error': 'graph_retrieval not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    message = data.get('message', '').strip()
    if not message:
        return jsonify({'error': 'message is required'}), 400

    conn = _kg.thread_local_conn()
    try:
        from retrieval import retrieve as _retrieve
        _, atoms = _retrieve(message, conn, limit=100)
        if not atoms:
            return jsonify({'graph_context': '', 'atom_count': 0})

        graph_ctx = build_graph_context(atoms, message, max_nodes_in_context=150)
        return jsonify({
            'graph_context': graph_ctx,
            'atom_count':    len(atoms),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/kb/traverse', methods=['POST'])
def kb_traverse():
    """
    Relational traversal — what does the KB know about a topic?

    Body: { "topic": "fed_policy" }

    Returns BFS-expanded connected concepts + direct facts from the knowledge graph.
    Surfaces relational importance rather than keyword matches.
    """
    if not HAS_GRAPH_RETRIEVAL:
        return jsonify({'error': 'graph_retrieval not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    topic = data.get('topic', '').strip()
    if not topic:
        return jsonify({'error': 'topic is required'}), 400

    conn = _kg.thread_local_conn()
    try:
        # Broad fetch for the topic
        c = conn.cursor()
        c.execute("""
            SELECT subject, predicate, object, source, confidence
            FROM facts
            WHERE (LOWER(subject) LIKE ? OR LOWER(object) LIKE ?)
            AND predicate NOT IN ('source_code','has_title','has_section','has_content')
            ORDER BY confidence DESC LIMIT 200
        """, (f'%{topic.lower()}%', f'%{topic.lower()}%'))
        rows = c.fetchall()
        atoms = [
            {'subject': str(r[0]), 'predicate': str(r[1]),
             'object': str(r[2])[:200], 'source': str(r[3] or ''),
             'confidence': float(r[4] or 0.5)}
            for r in rows
        ]
        traversal = what_do_i_know_about(topic, atoms)
        return jsonify({
            'topic':     topic,
            'traversal': traversal,
            'atom_count': len(atoms),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/kb/conflicts', methods=['GET'])
def kb_conflicts():
    """
    Return the fact_conflicts audit log — atoms that were detected as contradicting
    existing atoms and superseded.

    Query params: limit (default 50), subject (optional filter)
    """
    limit   = int(request.args.get('limit', 50))
    subject = request.args.get('subject', '').strip()

    conn = _kg.thread_local_conn()
    c = conn.cursor()
    try:
        if subject:
            c.execute("""
                SELECT fc.id, fc.winner_id, fc.loser_id, fc.winner_obj, fc.loser_obj,
                       fc.reason, fc.detected_at,
                       fw.subject, fw.predicate
                FROM fact_conflicts fc
                LEFT JOIN facts fw ON fc.winner_id = fw.id
                WHERE fw.subject LIKE ?
                ORDER BY fc.detected_at DESC LIMIT ?
            """, (f'%{subject.lower()}%', limit))
        else:
            c.execute("""
                SELECT fc.id, fc.winner_id, fc.loser_id, fc.winner_obj, fc.loser_obj,
                       fc.reason, fc.detected_at,
                       fw.subject, fw.predicate
                FROM fact_conflicts fc
                LEFT JOIN facts fw ON fc.winner_id = fw.id
                ORDER BY fc.detected_at DESC LIMIT ?
            """, (limit,))
        rows = c.fetchall()
        return jsonify({
            'count': len(rows),
            'conflicts': [
                {
                    'id':          r[0],
                    'winner_id':   r[1],
                    'loser_id':    r[2],
                    'winner_obj':  r[3],
                    'loser_obj':   r[4],
                    'reason':      r[5],
                    'detected_at': r[6],
                    'subject':     r[7],
                    'predicate':   r[8],
                }
                for r in rows
            ]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/kb/confidence', methods=['GET'])
def kb_confidence():
    """
    Return the Bayesian confidence distribution for KB atoms.

    Query params:
      subject    — required; atom subject (e.g. 'aapl', 'market')
      predicate  — optional; if omitted, returns all predicates for subject
      z          — z-score for interval width (default 1.96 = 95%)

    Returns (single predicate):
      {
        "subject": "AAPL", "predicate": "conviction_tier", "object": "high",
        "mean": 0.82, "n": 4, "std": 0.06, "variance": 0.004,
        "interval_low": 0.70, "interval_high": 0.94, "interval_z": 1.96,
        "source": "derived_signal_quality_aapl", "authority": 0.65
      }

    Returns (all predicates):
      { "subject": "aapl", "count": N, "atoms": [...] }

    Returns 404 if no atom found, 503 if module unavailable.
    """
    if not HAS_CONF_INTERVALS:
        return jsonify({'error': 'confidence_intervals module not available'}), 503

    subject   = request.args.get('subject', '').strip()
    predicate = request.args.get('predicate', '').strip()
    try:
        z = float(request.args.get('z', 1.96))
    except ValueError:
        return jsonify({'error': 'z must be a float'}), 400

    if not subject:
        return jsonify({'error': 'subject parameter is required'}), 400

    conn = _kg.thread_local_conn()
    try:
        if predicate:
            result = get_confidence_interval(conn, subject, predicate, z=z)
            if result is None:
                return jsonify({'error': f'no atom found for {subject!r} / {predicate!r}'}), 404
            return jsonify(result)
        else:
            atoms = get_all_confidence_intervals(conn, subject, z=z)
            if not atoms:
                return jsonify({'error': f'no atoms found for subject {subject!r}'}), 404
            return jsonify({'subject': subject, 'count': len(atoms), 'atoms': atoms})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/kb/causal-chain', methods=['POST'])
def kb_causal_chain():
    """
    Traverse the causal graph from a seed concept.

    Body:
      {
        "seed":           "fed_rate_hike",   -- required
        "depth":          3,                 -- optional, default 4, max 6
        "min_confidence": 0.6               -- optional, default 0.5
      }

    Returns:
      {
        "seed": "fed_rate_hike",
        "chain": [
          { "step": 1, "depth": 1, "cause": "fed_rate_hike",
            "effect": "credit_cost_rises", "mechanism": "debt_service_transmission",
            "confidence": 0.90 },
          ...
        ],
        "concepts_reached": [...],
        "affected_tickers": { "equity_multiples_compress": ["MSFT", "GOOGL", ...] },
        "chain_confidence": 0.72,
        "paths": 8
      }

    Returns 400 if seed is missing, 503 if module unavailable.
    """
    if not HAS_CAUSAL_GRAPH:
        return jsonify({'error': 'causal_graph module not available'}), 503

    body = request.get_json(force=True) or {}
    seed = (body.get('seed') or '').strip()
    if not seed:
        return jsonify({'error': 'seed is required'}), 400

    depth          = min(int(body.get('depth', 4)), 6)
    min_confidence = float(body.get('min_confidence', 0.5))

    conn = _kg.thread_local_conn()
    try:
        ensure_causal_edges_table(conn)
        result = traverse_causal(conn, seed, max_depth=depth,
                                 min_confidence=min_confidence)
        return jsonify(result)
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('causal chain traversal failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/kb/causal-edge', methods=['POST'])
def kb_causal_edge_add():
    """
    Add a new causal edge to the graph.

    Body:
      {
        "cause":      "inflation_rises",         -- required
        "effect":     "tech_capex_freezes",      -- required
        "mechanism":  "cost_of_capital_squeeze", -- required
        "confidence": 0.7,                       -- optional, default 0.7
        "source":     "user_defined"             -- optional
      }

    Returns:
      { "inserted": true, "id": 42, "cause": "...", "effect": "...",
        "mechanism": "...", "confidence": 0.7, "message": "created" }

    Duplicate (cause, effect, mechanism) returns inserted=false.
    """
    if not HAS_CAUSAL_GRAPH:
        return jsonify({'error': 'causal_graph module not available'}), 503

    body = request.get_json(force=True) or {}
    cause     = (body.get('cause')     or '').strip()
    effect    = (body.get('effect')    or '').strip()
    mechanism = (body.get('mechanism') or '').strip()

    if not cause or not effect or not mechanism:
        return jsonify({'error': 'cause, effect, and mechanism are required'}), 400

    confidence = float(body.get('confidence', 0.7))
    source     = (body.get('source') or 'user_defined').strip()

    conn = _kg.thread_local_conn()
    try:
        ensure_causal_edges_table(conn)
        result = add_causal_edge(conn, cause, effect, mechanism,
                                 confidence=confidence, source=source)
        status = 201 if result['inserted'] else 200
        return jsonify(result), status
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/kb/causal-edges', methods=['GET'])
def kb_causal_edges_list():
    """
    List all causal edges in the graph.

    Query params:
      cause  — optional filter substring on cause concept
      limit  — max rows (default 200)

    Returns:
      { "count": N, "edges": [ { id, cause, effect, mechanism, confidence, source, created_at } ] }
    """
    if not HAS_CAUSAL_GRAPH:
        return jsonify({'error': 'causal_graph module not available'}), 503

    cause_filter = request.args.get('cause', '').strip() or None
    limit        = int(request.args.get('limit', 200))

    conn = _kg.thread_local_conn()
    try:
        ensure_causal_edges_table(conn)
        edges = list_causal_edges(conn, cause_filter=cause_filter, limit=limit)
        return jsonify({'count': len(edges), 'edges': edges})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/kb/refresh-queue', methods=['GET'])
def kb_refresh_queue():
    """
    Inspect the domain_refresh_queue and synthesis_queue.
    These are populated by the EpistemicAdaptationEngine when decay_pressure
    or conflict_cluster remains elevated across multiple turns.

    Query params: processed (0=pending only, 1=all, default 0)
    """
    processed = int(request.args.get('processed', 0))
    conn = _kg.thread_local_conn()
    c = conn.cursor()
    result = {}

    try:
        if processed:
            c.execute("SELECT * FROM domain_refresh_queue ORDER BY queued_at DESC LIMIT 50")
        else:
            c.execute("SELECT * FROM domain_refresh_queue WHERE processed = 0 ORDER BY queued_at DESC LIMIT 50")
        rows = c.fetchall()
        result['domain_refresh_queue'] = [
            {'id': r[0], 'topic': r[1], 'reason': r[2], 'queued_at': r[3], 'processed': r[4]}
            for r in rows
        ]
    except Exception:
        result['domain_refresh_queue'] = []

    try:
        if processed:
            c.execute("SELECT * FROM synthesis_queue ORDER BY queued_at DESC LIMIT 50")
        else:
            c.execute("SELECT * FROM synthesis_queue WHERE processed = 0 ORDER BY queued_at DESC LIMIT 50")
        rows = c.fetchall()
        result['synthesis_queue'] = [
            {'id': r[0], 'topic': r[1], 'key_terms': r[2], 'reason': r[3],
             'queued_at': r[4], 'processed': r[5]}
            for r in rows
        ]
    except Exception:
        result['synthesis_queue'] = []

    try:
        c.execute("SELECT * FROM kb_insufficient_log ORDER BY detected_at DESC LIMIT 20")
        rows = c.fetchall()
        result['kb_insufficient_log'] = [
            {'id': r[0], 'topic': r[1], 'consolidation_count': r[2],
             'window_days': r[3], 'insufficiency_types': r[4], 'detected_at': r[5]}
            for r in rows
        ]
    except Exception:
        result['kb_insufficient_log'] = []

    return jsonify(result)


# ── LLM / Chat endpoints ─────────────────────────────────────────────────────

_LIVE_PRICE_KEYWORDS = (
    'current', 'currently', 'right now', 'right-now', 'today',
    'trading at', 'trading now', 'priced at', 'price now', 'price today',
    'what is', "what's", 'whats', 'how much', 'worth', 'value',
    'rate', 'rates', 'level', 'levels', 'spot', 'live', 'latest',
    'at the moment', 'at this moment', 'as of now',
)

def _query_wants_live(message: str) -> bool:
    """
    Returns True if the user's question is explicitly asking for a current /
    live price, rate, or level — triggering Pass 1 even when the KB has atoms.
    """
    m = message.lower()
    return any(kw in m for kw in _LIVE_PRICE_KEYWORDS)


@app.route('/chat', methods=['POST'])
@rate_limit('chat')
def chat_endpoint():
    """
    KB-grounded chat. Retrieves structured context, builds a KB-aware prompt,
    and calls the local Ollama model to produce an answer.

    Body:
      {
        "message":        "What is the current signal on NVDA?",
        "session_id":     "optional",
        "model":          "llama3.2",   // optional — defaults to OLLAMA_MODEL env or llama3.2
        "stream":         false,
        "screen_context": "NVDA Daily Chart — Price $192 — RSI 67",  // optional
        "screen_entities": ["NVDA"],   // optional — explicit tickers to include
        "overlay_mode":   true         // optional — adds overlay_cards to response
      }

    Returns:
      {
        "answer":        "...",         // null if Ollama unavailable
        "model":         "llama3.2",
        "stress":        { ... },
        "atoms_used":    14,
        "snippet":       "=== TRADING KNOWLEDGE CONTEXT ===...",
        "kb_diagnosis":  { ... },       // only if fired
        "adaptation":    { ... },       // only if active
        "overlay_cards": [ ... ]        // only when overlay_mode=true
      }

    overlay_cards contains typed cards: signal_summary (per ticker),
    causal_context (regime BFS chain), stress_flag (composite stress threshold).

    If Ollama is unavailable, returns HTTP 503 with the KB context still populated
    so callers can render it even without an LLM answer.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'error': 'invalid JSON'}), 400

    message    = data.get('message', '').strip()
    if not message:
        return jsonify({'error': 'message is required'}), 400

    session_id      = data.get('session_id', 'default')
    model           = data.get('model', DEFAULT_MODEL if HAS_LLM else 'llama3.2')
    goal            = data.get('goal')
    topic           = data.get('topic')
    turn_count      = int(data.get('turn_count', 1))
    limit           = int(data.get('limit', 30))
    screen_context  = data.get('screen_context', '')
    screen_entities = data.get('screen_entities') or []
    overlay_mode    = bool(data.get('overlay_mode', False))
    # Trust the authenticated token identity over the request body to prevent spoofing
    chat_user_id    = getattr(g, 'user_id', None) or data.get('user_id') or None

    # ── Trader level — controls communication style in the LLM prompt ──────
    _chat_trader_level = 'developing'
    if chat_user_id and HAS_PRODUCT_LAYER:
        try:
            _tl_row = get_user(_DB_PATH, chat_user_id)
            if _tl_row:
                _chat_trader_level = _tl_row.get('trader_level') or 'developing'
        except Exception:
            pass

    # ── Chat quota enforcement (basic tier: 10 queries/day) ────────────────
    if chat_user_id and HAS_TIERS and HAS_PATTERN_LAYER:
        try:
            _chat_tier = _get_user_tier_for_request(chat_user_id)
            _quota = _get_tier(_chat_tier).get('chat_queries_per_day')
            # quota=None means unlimited (pro/premium); quota=0 means no chat access (free tier)
            if _quota is not None and _quota == 0:
                return jsonify({
                    'error':        'upgrade_required',
                    'feature':      'chat_queries_per_day',
                    'current_tier': _chat_tier,
                    'upgrade_to':   _next_tier_name(_chat_tier),
                    'queries_used': 0,
                    'queries_limit': 0,
                    'message':      'Chat is not available on the free plan. Subscribe to unlock.',
                }), 403
            if _quota is not None and _quota > 0:
                _used = get_today_chat_count(_DB_PATH, chat_user_id)
                if _used >= _quota:
                    return jsonify({
                        'error':        'upgrade_required',
                        'feature':      'chat_queries_per_day',
                        'current_tier': _chat_tier,
                        'upgrade_to':   _next_tier_name(_chat_tier),
                        'queries_used': _used,
                        'queries_limit': _quota,
                        'message':      f'Daily chat limit of {_quota} reached. Upgrade to Pro for unlimited queries.',
                    }), 403
        except Exception:
            pass

    # ── Portfolio-intent detection ─────────────────────────────────────────
    # Keywords that signal the user is explicitly asking about their own holdings.
    # ONLY when these are present should we inject portfolio context or augment
    # KB retrieval with portfolio tickers.
    _PORTFOLIO_INTENT_KWS = (
        'my portfolio', 'my holdings', 'my positions', 'my stocks', 'my shares',
        'my book', 'my p&l', 'my pnl', 'my exposure', 'my allocation',
        'discuss my', 'analyse my', 'analyze my', 'review my',
        'affect my', 'impact my', 'affect portfolio', 'impact portfolio',
        'portfolio', 'holdings', 'positions',
    )
    _msg_lower_port = message.lower()
    _wants_portfolio = any(kw in _msg_lower_port for kw in _PORTFOLIO_INTENT_KWS)

    # Auto-boost limit for portfolio-wide queries so all holdings get KB atoms
    if chat_user_id and _wants_portfolio:
        limit = max(limit, 80)

    conn = _kg.thread_local_conn()

    # ── Prior session context (working_state) ──────────────────────────────
    prior_context = None
    if HAS_WORKING_STATE:
        try:
            ws = get_working_state_store(_DB_PATH)
            if turn_count == 0:
                prior_context = ws.format_prior_context(session_id) or None
            ws.maybe_persist(session_id, turn_count, goal=goal, topic=topic,
                             force=(turn_count == 1))
        except Exception:
            pass

    # ── Adaptation nudges (from prior streak) ──────────────────────────────
    nudges = None
    if HAS_ADAPTATION and HAS_STRESS:
        try:
            from knowledge.epistemic_adaptation import ensure_adaptation_tables
            ensure_adaptation_tables(conn)
            engine = get_adaptation_engine(session_id, db_path=_DB_PATH)
            engine._session_id = session_id
            sess = _session_streaks.setdefault(session_id, {'streak': 0, 'last_stress': 0.0})

            class _StateStub:
                pass
            state_stub = _StateStub()
            state_stub.epistemic_stress_streak = sess['streak']
            state_stub._session_id = session_id

            class _StressStub:
                composite_stress     = sess['last_stress']
                decay_pressure       = 0.0
                authority_conflict   = 0.0
                supersession_density = 0.0
                conflict_cluster     = 0.0
                domain_entropy       = 1.0
            nudges = engine.compute(state_stub, _StressStub(), topic=topic, key_terms=[])
        except Exception:
            nudges = None

    # ── Ticker carry-forward for follow-up queries ──────────────────────────
    # If the current message has no explicit tickers, inject the previous
    # session's tickers into the retrieval query so follow-ups like
    # "give me more info on it" or "what's the signal?" resolve correctly.
    try:
        from retrieval import _extract_tickers as _et
        _cur_tickers = _et(message)
    except Exception:
        _cur_tickers = []

    # Populate portfolio tickers on first portfolio query this session
    if not _session_portfolio_tickers.get(session_id) and chat_user_id and HAS_PRODUCT_LAYER:
        try:
            _ph = get_portfolio(_DB_PATH, chat_user_id)
            _pticks = [h['ticker'] for h in (_ph or []) if h.get('ticker')]
            if _pticks:
                _session_portfolio_tickers[session_id] = _pticks
        except Exception:
            pass

    _retrieve_message = message
    _aug_tickers: list = []
    if not _cur_tickers and session_id in _session_tickers:
        _aug_tickers = list(_session_tickers[session_id])
    # Only merge portfolio tickers when the user is explicitly asking about their portfolio.
    # Merging unconditionally caused every unrelated query to pull portfolio KB atoms,
    # which made the LLM anchor every response back to the user's holdings.
    if _wants_portfolio:
        _port_ticks = _session_portfolio_tickers.get(session_id, [])
        for _pt in _port_ticks:
            if _pt not in _aug_tickers and _pt not in _cur_tickers:
                _aug_tickers.append(_pt)
    if _aug_tickers:
        _retrieve_message = message + ' ' + ' '.join(_aug_tickers)

    # ── Retrieve KB context ────────────────────────────────────────────
    snippet, atoms = retrieve(_retrieve_message, conn, limit=limit, nudges=nudges)

    # Save tickers seen this turn so next follow-up can carry them forward
    if _cur_tickers:
        _session_tickers[session_id] = _cur_tickers
    elif session_id not in _session_tickers and atoms:
        # Derive from atom subjects as fallback
        _seen = list({a['subject'].upper() for a in atoms if 'subject' in a})[:4]
        if _seen:
            _session_tickers[session_id] = _seen

    # ── Working memory: on-demand fetch for tickers absent from KB ────────
    live_context  = ''
    live_fetched  = []
    wm_session_id = f'wm_{session_id}'
    if HAS_WORKING_MEMORY and _working_memory is not None:
        try:
            from retrieval import _extract_tickers
            from knowledge.working_memory import _YF_TICKER_MAP
            tickers_in_query = _extract_tickers(message)
            # Filter out junk words that _extract_tickers pulls from pattern card text
            # (e.g. BEARISH, BULLISH, ORDER, BREAKER, CONFIRMED, ZONE, QUALITY, etc.)
            # A valid ticker must: <=6 chars OR contain digits/dots/hyphens/^ OR be in the YF map.
            _JUNK_WORDS = {
                'BEARISH','BULLISH','ORDER','BREAKER','CONFIRMED','UNCONFIRMED','PARTIAL',
                'SIGNAL','QUALITY','ZONE','PATTERN','LONG','SHORT','NEUTRAL','AVOID',
                'HIGH','LOW','MID','RANGE','NEAR','MEDIUM','STRONG','WEAK','MACRO',
                'CONVICTION','TIER','PRICE','TARGET','REGIME','DIRECTION','RETURN',
                'YEAR','MONTH','WEEK','DAY','LIVE','DATA','CONTEXT','DETECTED',
                'TIMEFRAME','SCORE','PERIOD','OPEN','CLOSE','VOLUME','MARKET',
                'SECTOR','FACTOR','RISK','CATALYST','THESIS','BASIS','COST',
                'HOLDINGS','HOLDING','PORTFOLIO','ANALYSIS','ANALYSIS','PLEASE',
                'DISCUSS','PROVIDE','CONTEXT','INSIGHT','GIVEN','BASED','NOTE',
            }
            tickers_in_query = [
                t for t in tickers_in_query
                if t not in _JUNK_WORDS
                and (
                    len(t) <= 5               # most real tickers are ≤5 chars
                    or any(c in t for c in ('.', '-', '=', '^', '/'))  # commodity/forex/index format
                    or t in _YF_TICKER_MAP    # explicitly mapped
                    or any(c.isdigit() for c in t)  # e.g. BRK-B kept by '-' rule above
                )
            ]
            # If message has no explicit tickers (e.g. "analyse my portfolio"),
            # fall back to: (1) DB portfolio holdings, (2) session carry-forward,
            # (3) KB atom subjects — so on-demand fetch fires for every missing holding.
            if not tickers_in_query:
                # Priority 1: user's DB portfolio (most reliable for "analyse my portfolio")
                if chat_user_id and HAS_PRODUCT_LAYER:
                    try:
                        _ph = get_portfolio(_DB_PATH, chat_user_id)
                        tickers_in_query = [h['ticker'] for h in (_ph or []) if h.get('ticker')]
                    except Exception:
                        pass
                # Priority 2: session carry-forward from previous turns
                if not tickers_in_query:
                    tickers_in_query = list(_session_tickers.get(session_id, []))
                # Priority 3: subjects from KB atoms returned for this query
                if not tickers_in_query and atoms:
                    tickers_in_query = list({
                        a['subject'].upper() for a in atoms if 'subject' in a
                    })
            # Tickers completely absent from KB — always fetch
            missing_from_kb = [
                t for t in tickers_in_query[:MAX_ON_DEMAND_TICKERS]
                if not kb_has_atoms(t, _DB_PATH)
            ]
            # Commodity/forex/index/crypto tickers — always fetch live regardless
            # of KB atoms because seeded prices go stale within hours.
            # Catches both: raw KB tickers (XAUUSD in _YF_TICKER_MAP) and
            # already-resolved yfinance symbols (BTC-USD, GBPUSD=X, GC=F, ^GSPC)
            _yf_values = set(_YF_TICKER_MAP.values())
            def _is_live_asset(t: str) -> bool:
                tu = t.upper()
                if tu in _YF_TICKER_MAP:
                    return True
                if t in _yf_values:
                    return True
                # yfinance symbol patterns: BTC-USD, GBPUSD=X, GC=F, ^GSPC, DX-Y.NYB
                if (t.endswith('-USD') or t.endswith('=X') or t.endswith('=F')
                        or t.startswith('^') or t.endswith('.NYB')):
                    return True
                return False
            live_always = [
                t for t in tickers_in_query[:MAX_ON_DEMAND_TICKERS]
                if _is_live_asset(t) and t not in missing_from_kb
            ]
            to_fetch = missing_from_kb + live_always
            if missing_from_kb or live_always:
                _working_memory.open_session(wm_session_id)
                for ticker in to_fetch[:MAX_ON_DEMAND_TICKERS]:
                    _working_memory.fetch_on_demand(ticker, wm_session_id, _DB_PATH)
                live_context = _working_memory.get_session_snippet(wm_session_id)
                live_fetched = _working_memory.get_fetched_tickers(wm_session_id)
        except Exception:
            pass

    # ── Async discovery — fire-and-forget, never blocks response ──────────
    # Triggers background enrichment for stale or missing tickers.
    # Next request for the same ticker gets a richer KB-grounded answer.
    if _discovery_pipeline is not None:
        try:
            from retrieval import _extract_tickers as _et_disc
            _disc_tickers = _et_disc(message)
            for _dt in _disc_tickers[:3]:
                _stale = _discovery_pipeline.assess_staleness(_dt)
                if _stale:
                    import threading as _threading
                    _threading.Thread(
                        target=_discovery_pipeline.discover,
                        args=(_dt, 'user_query', chat_user_id),
                        daemon=True,
                    ).start()
        except Exception:
            pass

    # ── Epistemic stress ───────────────────────────────────────────────────
    stress_report = None
    stress_dict   = None
    if HAS_STRESS and atoms:
        try:
            words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)
            key_terms = list({w.lower() for w in words if len(w) > 2})[:10]
            stress_report = compute_stress(atoms, key_terms, conn)
            stress_dict = {
                'composite_stress':     stress_report.composite_stress,
                'decay_pressure':       stress_report.decay_pressure,
                'authority_conflict':   stress_report.authority_conflict,
                'supersession_density': stress_report.supersession_density,
                'conflict_cluster':     stress_report.conflict_cluster,
                'domain_entropy':       stress_report.domain_entropy,
            }
        except Exception:
            pass

    # ── Update session streak ──────────────────────────────────────────────
    if HAS_ADAPTATION and stress_report:
        try:
            from knowledge.epistemic_adaptation import _STRESS_STREAK_THRESHOLD
            sess = _session_streaks.setdefault(session_id, {'streak': 0, 'last_stress': 0.0})
            if stress_report.composite_stress >= _STRESS_STREAK_THRESHOLD:
                sess['streak'] = sess.get('streak', 0) + 1
            else:
                sess['streak'] = max(0, sess.get('streak', 0) - 1)
            sess['last_stress'] = stress_report.composite_stress
        except Exception:
            pass

    # ── KB insufficiency diagnosis ─────────────────────────────────────────
    kb_diagnosis = None
    if HAS_CLASSIFIER and stress_report and atoms:
        try:
            import re as _re
            _tickers = [t for t in _re.findall(r'\b[A-Z]{2,5}\b', message)
                        if t not in {'THE','IS','AT','ON','AN','AND','OR','FOR','IN','OF',
                                     'TO','THAT','THIS','WITH','FROM','BY','ARE','WAS','BE',
                                     'HAS','HAVE','HAD','ITS','DO','DID','WHAT','HOW','WHY',
                                     'WHEN','WHERE','WHO','CAN','WILL','NOT','BUT','ALL'}]
            _terms = [w.lower() for w in _re.findall(r'\b[a-zA-Z][a-zA-Z0-9]{2,}\b', message)]
            composite  = getattr(stress_report, 'composite_stress', 0.0)
            atom_count = len(atoms)
            if composite > 0.35 or atom_count < 8:
                topic_hint = (topic or
                              (_tickers[0] if _tickers else None) or
                              (_terms[0] if _terms else None) or
                              message[:40])
                diag = classify_insufficiency(topic_hint, stress_report, conn)
                kb_diagnosis = {
                    'topic':         diag.topic,
                    'types':         [t.value for t in diag.types],
                    'primary_type':  diag.primary_type().value,
                    'confidence':    diag.confidence,
                    'matched_rules': diag.matched_rules,
                    'signals':       diag.signals,
                }
        except Exception:
            pass

    # ── Overlay cards (active copilot mode) ──────────────────────────────────
    overlay_cards = None
    if overlay_mode and HAS_OVERLAY:
        try:
            overlay_tickers = _extract_overlay_tickers(
                screen_context, conn, screen_entities
            )
            # If entities extracted from screen but not in message, append to message
            # for retrieval scoping (non-destructive — message is already used above)
            overlay_cards = build_overlay_cards(overlay_tickers, conn, stress_dict)
        except Exception:
            overlay_cards = []

    # ── Build response skeleton (always returned) ──────────────────────────
    response: dict = {
        'answer':     None,
        'model':      model,
        'atoms_used': len(atoms),
        'snippet':    snippet,
    }
    if overlay_cards is not None:
        response['overlay_cards'] = overlay_cards
    if stress_dict:
        response['stress'] = stress_dict
    if kb_diagnosis:
        response['kb_diagnosis'] = kb_diagnosis
    if nudges is not None and nudges.is_active():
        response['adaptation'] = {
            'streak':                    nudges.streak,
            'consolidation_mode':        nudges.consolidation_mode,
            'retrieval_scope_broadened': nudges.retrieval_scope_broadened,
            'prefer_high_authority':     nudges.prefer_high_authority,
            'prefer_recent':             nudges.prefer_recent,
        }

    # ── Call Ollama ────────────────────────────────────────────────────────
    if not HAS_LLM:
        response['error'] = 'llm package not available'
        return jsonify(response), 503

    if not is_available():
        response['error'] = 'Ollama not reachable — KB context returned without LLM answer'
        return jsonify(response), 503

    # ── Portfolio context (personalisation) ──────────────────────────────
    # Only inject when the user explicitly asked about their portfolio/holdings/positions.
    # Injecting unconditionally caused every response to reference the user's book.
    portfolio_context = None
    if chat_user_id and HAS_PRODUCT_LAYER and _wants_portfolio:
        try:
            _holdings = get_portfolio(_DB_PATH, chat_user_id)
            _model    = get_user_model(_DB_PATH, chat_user_id)
            if _holdings:
                _h_parts = [f"{h['ticker']} ×{int(h['quantity'])}"
                            for h in _holdings[:20]]
                # Compute portfolio value metrics for sizing education
                _pos_values = [
                    h['quantity'] * h['avg_cost']
                    for h in _holdings if h.get('quantity') and h.get('avg_cost')
                ]
                _total_cost = sum(_pos_values)
                _largest_pct = (
                    round(max(_pos_values) / _total_cost * 100)
                    if _total_cost > 0 and _pos_values else None
                )
                _lines = ["=== USER PORTFOLIO ===",
                          f"Holdings: {', '.join(_h_parts)}"]
                if _total_cost > 0:
                    _lines.append(f"Total invested (cost basis): £{_total_cost:,.0f}")
                if _largest_pct is not None:
                    _largest_ticker = max(
                        (h for h in _holdings if h.get('quantity') and h.get('avg_cost')),
                        key=lambda h: h['quantity'] * h['avg_cost']
                    )['ticker']
                    _lines.append(f"Largest single position: {_largest_pct}% ({_largest_ticker})")
                if _model:
                    _risk    = _model.get('risk_tolerance', '')
                    _style   = _model.get('holding_style', '')
                    _sectors = ', '.join(_model.get('sector_affinity') or [])
                    _profile = ' · '.join(p for p in [_risk, _style, _sectors] if p)
                    if _profile:
                        _lines.append(f"Risk profile: {_profile}")
                # ── Per-ticker KB signals injected into portfolio block ──────
                _holding_tickers = [h['ticker'] for h in _holdings]
                _ticker_atoms: dict = {}
                for _ht in _holding_tickers:
                    try:
                        _ht_rows = conn.execute(
                            """SELECT predicate, object FROM facts
                               WHERE subject=? AND predicate IN
                               ('last_price','currency','price_regime','signal_direction',
                                'signal_quality','return_1m','return_3m','return_1y',
                                'upside_pct','conviction_tier','macro_confirmation')
                               ORDER BY predicate""",
                            (_ht.lower(),)
                        ).fetchall()
                        if _ht_rows:
                            _ticker_atoms[_ht] = _ht_rows
                    except Exception:
                        pass
                _lines.append("\nPer-holding KB signals:")
                for _ht in _holding_tickers:
                    _rows = _ticker_atoms.get(_ht, [])
                    if not _rows:
                        _lines.append(f"  {_ht}: No KB signals available — discuss based on general knowledge of this ticker.")
                        continue
                    _d = {p: v for p, v in _rows}
                    _price    = _d.get('last_price', '?')
                    _regime   = _d.get('price_regime', '?').replace('_', ' ')
                    _dir      = _d.get('signal_direction', '?')
                    _qual     = _d.get('signal_quality', '?')
                    _macro    = _d.get('macro_confirmation', '?')
                    _conv     = _d.get('conviction_tier', '?')
                    _upside   = _d.get('upside_pct', '?')
                    _ret1m    = _d.get('return_1m', '')
                    _ret1y    = _d.get('return_1y', '')
                    _target   = _d.get('price_target', '')
                    # Compute implied move narrative from price_target vs last_price
                    _implied = ''
                    try:
                        if _target and _price and _price != '?' and _target != '?':
                            _move = float(_target) - float(_price)
                            _move_dir = 'up to' if _move >= 0 else 'down to'
                            _implied = (f" The KB price target is {_target}, implying a move "
                                        f"{_move_dir} {_target} ({_upside}% from current price).")
                    except Exception:
                        pass
                    _sent = (
                        f"  {_ht}: Current price {_price} ({_regime} regime). "
                        f"KB signal direction is {_dir}.{_implied} "
                        f"Signal quality: {_qual}. Macro confirmation: {_macro}. "
                        f"Conviction tier: {_conv}."
                    )
                    if _ret1m:
                        _sent += f" 1-month return: {_ret1m}%."
                    if _ret1y:
                        _sent += f" 1-year return: {_ret1y}%."
                    _lines.append(_sent)

                portfolio_context = '\n'.join(_lines)

                # ── Geo-risk context injection ─────────────────────────────
                # Append geopolitical_risk_exposure and energy_shock_risk atoms
                # for any holding that has elevated/moderate geo risk in the KB.
                try:
                    import sqlite3 as _sq
                    _gc = _sq.connect(_DB_PATH, timeout=5)
                    _geo_lines = []
                    for _ht in _holding_tickers:
                        _geo_row = _gc.execute(
                            """SELECT object FROM facts
                               WHERE subject=? AND predicate='geopolitical_risk_exposure'
                               ORDER BY confidence DESC LIMIT 1""",
                            (_ht.lower(),),
                        ).fetchone()
                        if _geo_row and _geo_row[0] in ('elevated', 'moderate'):
                            _geo_lines.append(
                                f"  {_ht}: geopolitical_risk_exposure={_geo_row[0]}"
                            )
                    # Energy shock risk macro atom
                    _shock_row = _gc.execute(
                        """SELECT object FROM facts
                           WHERE subject='macro_regime' AND predicate='energy_shock_risk'
                           ORDER BY confidence DESC LIMIT 1"""
                    ).fetchone()
                    _gc.close()
                    if _geo_lines or (_shock_row and _shock_row[0] in ('elevated', 'moderate')):
                        portfolio_context += '\n=== GEOPOLITICAL RISK FLAGS ==='
                        if _shock_row and _shock_row[0] in ('elevated', 'moderate'):
                            portfolio_context += f'\n  Energy shock risk: {_shock_row[0]} (WTI/Middle East tension)'
                        if _geo_lines:
                            portfolio_context += '\n' + '\n'.join(_geo_lines)
                except Exception:
                    pass

        except Exception:
            portfolio_context = None

    # ── Pass 1: LLM-initiated data request (only when KB is thin) ────────
    # If we already fetched live data (live_fetched), skip pass 1 — we have
    # what we need.  If KB had plenty of atoms, skip pass 1 too.
    llm_requested_tickers: list = []
    web_searched: str | None = None
    if (HAS_WORKING_MEMORY and _working_memory is not None
            and not live_fetched
            and (len(atoms) < 8 or _query_wants_live(message))):
        try:
            from knowledge.working_memory import (
                DATA_REQUEST_SYSTEM_PROMPT, parse_llm_response
            )
            # Inject portfolio "no KB signals" context into pass-1 so the LLM
            # can decide between DATA_REQUEST (prices) and SEARCH_REQUEST (news)
            _p1_ctx = snippet or '(No KB context)'
            if portfolio_context:
                _p1_ctx = portfolio_context + '\n\n' + _p1_ctx
            _p1_messages = [
                {'role': 'system', 'content': DATA_REQUEST_SYSTEM_PROMPT},
                {'role': 'user',   'content':
                    f"{_p1_ctx}\n\nQuestion: {message}"},
            ]
            _p1_raw = _llm_chat(_p1_messages, model=model)
            if _p1_raw:
                _mode, _payload = parse_llm_response(_p1_raw)
                if _mode == 'data_request' and _payload:
                    llm_requested_tickers = _payload
                    _working_memory.open_session(wm_session_id)
                    for _t in llm_requested_tickers:
                        _working_memory.fetch_on_demand(_t, wm_session_id, _DB_PATH)
                    live_context  = _working_memory.get_session_snippet(wm_session_id)
                    live_fetched  = _working_memory.get_fetched_tickers(wm_session_id)
                elif _mode == 'search_request' and _payload:
                    _search_query = _payload[0]
                    _working_memory.open_session(wm_session_id)
                    _search_atoms = _working_memory.web_search_on_demand(
                        _search_query, wm_session_id
                    )
                    if _search_atoms:
                        web_searched  = _search_query
                        live_context  = _working_memory.get_session_snippet(wm_session_id)
                        live_fetched  = _working_memory.get_fetched_tickers(wm_session_id)
        except Exception:
            pass

    # ── Extract resolved aliases from snippet for system prompt injection ─────
    _resolved_aliases: dict = {}
    if snippet and 'is an alias' in snippet:
        import re as _re2
        for _m in _re2.finditer(
            r"INSTRUCTION: '(\S+)' is an alias\. The KB data below \(subject='(\S+)'\)",
            snippet
        ):
            _resolved_aliases[_m.group(1)] = _m.group(2).upper()

    # Detect whether prior conversation turns will be spliced in
    _has_prior_turns = False
    if _conv_store is not None:
        try:
            _conv_session_id_check = _sid_for_user(chat_user_id) if HAS_CONV_STORE else session_id
            _check_hist = _conv_store.get_recent_messages_for_context(_conv_session_id_check, n_turns=2)
            _has_prior_turns = len(_check_hist) > 1
        except Exception:
            pass

    # ── On-demand tip intent detection ────────────────────────────────────
    # "give me a tip" / "what should I trade today" / "any setups worth looking at"
    # Routes to tip pipeline and injects a structured tip card into response.
    _TIP_INTENT_PHRASES = (
        'give me a tip', 'give me tip', 'daily tip', 'today\'s tip',
        'what should i trade', 'what should i buy', 'what should i sell',
        'any setups worth', 'best opportunity right now', 'what\'s looking good',
        "what's looking good", 'best setup today', 'top trade today',
        'trade of the day', 'tip of the day', 'recommend a trade',
        'show me a trade', 'suggest a trade',
    )
    _msg_lower_tip = message.lower()
    _is_tip_request = any(ph in _msg_lower_tip for ph in _TIP_INTENT_PHRASES)

    if _is_tip_request and HAS_PATTERN_LAYER and chat_user_id:
        try:
            from notifications.tip_scheduler import _pick_best_pattern, _get_local_now
            from notifications.tip_formatter import format_tip, tip_to_dict, TIER_LIMITS
            from analytics.pattern_detector import PatternSignal
            from analytics.position_calculator import calculate_position
            import sqlite3 as _sq2

            _prefs_row = None
            _c_tip = _sq2.connect(_DB_PATH, timeout=5)
            try:
                _prefs_row = _c_tip.execute(
                    """SELECT tier, tip_timeframes, tip_pattern_types,
                              account_size, max_risk_per_trade_pct, account_currency
                       FROM user_preferences WHERE user_id=?""", (chat_user_id,)
                ).fetchone()
            finally:
                _c_tip.close()

            _tier = 'basic'
            _tip_prefs: dict = {}
            if _prefs_row:
                import json as _json
                _tier = _prefs_row[0] or 'basic'
                def _j(v): 
                    try: return _json.loads(v) if v else None
                    except Exception: return None
                _limits = TIER_LIMITS.get(_tier, TIER_LIMITS['basic'])
                _tip_prefs = {
                    'account_size': _prefs_row[3] or 10000,
                    'max_risk_per_trade_pct': _prefs_row[4] or 1.0,
                    'account_currency': _prefs_row[5] or 'GBP',
                    'tier': _tier,
                    'tip_timeframes': _j(_prefs_row[1]) or _limits['timeframes'],
                    'tip_pattern_types': _j(_prefs_row[2]),
                }

            _pat_row = _pick_best_pattern(
                _DB_PATH, chat_user_id, _tier,
                _tip_prefs.get('tip_timeframes', ['1h']),
                _tip_prefs.get('tip_pattern_types'),
            )
            if _pat_row:
                _sig = PatternSignal(
                    pattern_type  = _pat_row['pattern_type'],
                    ticker        = _pat_row['ticker'],
                    direction     = _pat_row['direction'],
                    zone_high     = _pat_row['zone_high'],
                    zone_low      = _pat_row['zone_low'],
                    zone_size_pct = _pat_row.get('zone_size_pct', 0.0),
                    timeframe     = _pat_row['timeframe'],
                    formed_at     = _pat_row.get('formed_at', ''),
                    quality_score = _pat_row.get('quality_score') or 0.0,
                    status        = _pat_row['status'],
                    kb_conviction = _pat_row.get('kb_conviction', ''),
                    kb_regime     = _pat_row.get('kb_regime', ''),
                    kb_signal_dir = _pat_row.get('kb_signal_dir', ''),
                )
                _tip_pos = calculate_position(_sig, _tip_prefs) if _tip_prefs else None
                _tip_dict = tip_to_dict(_sig, _tip_pos, tier=_tier)
                response['tip_card'] = {
                    **_tip_dict,
                    'tip_id': None,
                    'pattern_id': _pat_row.get('id'),
                    'feedback_actions': ['taking_it', 'tell_me_more', 'not_for_me'],
                }
        except Exception as _tip_err:
            import logging as _logging
            _logging.getLogger(__name__).warning('on-demand tip failed: %s', _tip_err)

    # ── Opportunity generation scan ────────────────────────────────────────
    # Detects open-ended generation queries ("make me a daytime strategy",
    # "where are gaps", "find momentum plays") and injects a structured
    # KB scan block into the prompt so the LLM can build a concrete strategy.
    _opportunity_scan_context: Optional[str] = None
    try:
        from analytics.opportunity_engine import (
            classify_intent as _classify_intent,
            run_opportunity_scan as _run_opportunity_scan,
            format_scan_as_context as _format_scan_as_context,
        )
        _gen_modes = _classify_intent(message)
        # Only fire for genuine generation queries — exclude pure portfolio / single-ticker queries
        _GEN_SKIP_KEYWORDS = (
            'what is', 'what\'s', 'tell me about', 'explain', 'why is', 'how is',
            'price of', 'signal for', 'analyse my portfolio', 'analyze my portfolio',
            'portfolio', 'my holdings',
        )
        _is_gen_query = not any(kw in message.lower() for kw in _GEN_SKIP_KEYWORDS)
        # Also require the message to have some generation-flavoured phrasing
        _GEN_TRIGGER_WORDS = (
            'strategy', 'strateg', 'trade', 'trading', 'opportunity', 'opportunit',
            'setup', 'setups', 'find me', 'show me', 'make me', 'give me',
            'where are', 'what sectors', 'momentum', 'squeeze', 'gap', 'intraday',
            'daytime', 'ideas', 'idea', 'rotation', 'reversal', 'breakout',
            'best trade', 'top trade', 'mean reversion', 'play',
        )
        _has_gen_trigger = any(kw in message.lower() for kw in _GEN_TRIGGER_WORDS)
        if _is_gen_query and _has_gen_trigger and _gen_modes:
            _scan = _run_opportunity_scan(
                query=message,
                db_path=_DB_PATH,
                modes=_gen_modes,
                limit_per_mode=6,
            )
            _opportunity_scan_context = _format_scan_as_context(_scan)
            response['opportunity_scan'] = {
                'mode':    _scan.mode,
                'results': len(_scan.results),
                'regime':  _scan.market_regime,
            }
    except Exception as _opp_err:
        import logging as _logging
        _logging.getLogger(__name__).warning('opportunity scan failed: %s', _opp_err)

    # ── Build full prompt (pass 2 or single-pass if no data request) ──────
    messages = build_prompt(
        user_message=message,
        snippet=snippet,
        stress=stress_dict,
        kb_diagnosis=kb_diagnosis,
        prior_context=prior_context,
        portfolio_context=portfolio_context,
        atom_count=len(atoms),
        live_context=live_context or None,
        resolved_aliases=_resolved_aliases or None,
        web_searched=web_searched or None,
        has_history=_has_prior_turns,
        opportunity_scan_context=_opportunity_scan_context,
        trader_level=_chat_trader_level,
    )

    # ── Persist user turn + inject DB-backed conversation history ──────────
    # ConversationStore is server-authoritative: the client only sends the
    # current message. Prior turns are loaded from SQLite and spliced in.
    _conv_session_id = _sid_for_user(chat_user_id) if HAS_CONV_STORE else session_id
    _user_msg_record = None
    if _conv_store is not None:
        try:
            _user_msg_record = _conv_store.add_message(
                _conv_session_id, 'user', message, user_id=chat_user_id
            )
            _db_history = _conv_store.get_recent_messages_for_context(
                _conv_session_id, n_turns=8
            )
            # Exclude the message we just inserted so it isn't doubled
            _just_id = _user_msg_record.get('id') if _user_msg_record else None
            _db_hist_msgs = [
                {'role': m['role'], 'content': m['content']}
                for m in _db_history
                if m.get('id') != _just_id
            ]
            # Skip history injection if this is a retry of the same question.
            # A retry must get a fresh response from the current KB context —
            # splicing in a stale assistant turn would anchor the LLM to the
            # old (pre-fix) answer regardless of what the new KB snippet contains.
            _last_user_msg = next(
                (m['content'] for m in reversed(_db_hist_msgs) if m['role'] == 'user'),
                None
            )
            _is_retry = (
                _last_user_msg is not None
                and message.strip().lower() == _last_user_msg.strip().lower()
            )
            if _db_hist_msgs and len(messages) >= 2 and not _is_retry:
                messages = [messages[0]] + _db_hist_msgs + [messages[-1]]
        except Exception:
            pass

    answer = _llm_chat(messages, model=model)
    if answer is None:
        if HAS_WORKING_MEMORY and _working_memory:
            _working_memory.close_without_commit(wm_session_id)
        response['error'] = 'Ollama returned no response'
        return jsonify(response), 503

    response['answer'] = answer
    if llm_requested_tickers:
        response['llm_requested_tickers'] = llm_requested_tickers
    if web_searched:
        response['web_searched'] = web_searched

    # ── Persist assistant turn + async atom extraction → KB graduation ────
    if _conv_store is not None:
        try:
            _stress_val = stress_dict.get('composite_stress') if stress_dict else None
            _asst_meta  = {
                'tickers':  _session_tickers.get(session_id, []),
                'stress':   _stress_val,
                'atoms':    len(atoms),
            }
            _asst_msg_record = _conv_store.add_message(
                _conv_session_id, 'assistant', answer,
                metadata=_asst_meta, user_id=chat_user_id
            )
        except Exception:
            _asst_msg_record = None

        # Async thread: extract atoms → graduate to KB → force working_state persist
        import threading as _threading
        _atom_msg_id  = _asst_msg_record.get('id') if _asst_msg_record else None
        _atom_user_q  = message
        _atom_answer  = answer
        _atom_cs_id   = _conv_session_id
        _atom_sess_id = session_id
        _atom_turn    = turn_count
        _atom_goal    = goal

        def _extract_and_graduate():
            try:
                if _atom_msg_id is None:
                    return
                # ── Step 1: atom extraction via local Ollama ────────────────
                from llm.ollama_client import chat as _oc
                _atom_prompt = [
                    {'role': 'system', 'content': (
                        'You are a knowledge extractor for a trading intelligence system. '
                        'Extract exactly 3-6 knowledge atoms from the conversation turn. '
                        'Prefer these predicates where applicable: '
                        'signal_direction, conviction_tier, price_target, risk_factor, '
                        'catalyst, thesis_premise, invalidation_condition, sector_bias, '
                        'user_interest, pattern_preference, regime_view. '
                        'Each atom must be a JSON object with keys: '
                        'subject (ticker or concept), predicate (from vocabulary or freeform), '
                        'object (value), atom_type (fact|intent|topic|signal), source (user|assistant). '
                        'Respond with ONLY a JSON array. No preamble, no explanation.'
                    )},
                    {'role': 'user', 'content': (
                        f'User said: "{_atom_user_q[:300]}"\n'
                        f'Assistant replied: "{_atom_answer[:400]}"'
                    )},
                ]
                _raw = _oc(_atom_prompt, model='llama3.2')
                if not _raw:
                    return
                import json as _json
                _s = _raw.find('[')
                _e = _raw.rfind(']') + 1
                if _s == -1 or _e <= 0:
                    return
                _atoms = _json.loads(_raw[_s:_e])
                if not isinstance(_atoms, list):
                    return
                _conv_store.add_turn_atoms(_atom_msg_id, _atom_cs_id, _atoms)

                # ── Step 2: KB graduation ────────────────────────────────────
                import math as _math
                _salient = _conv_store.get_salient_atoms(_atom_cs_id, limit=30, min_salience=0.1)
                _graduated = []
                for _at in _salient:
                    if _at.get('graduated'):
                        continue
                    _is_user_intent = (
                        _at.get('source') == 'user' and
                        _at.get('atom_type') == 'intent'
                    )
                    _threshold = 0.25 if _is_user_intent else 0.40
                    # Never commit price/financial data from conversation —
                    # LLM-generated figures must not become KB ground truth.
                    _PRICE_PREDICATES = {
                        'last_price', 'price', 'price_target', 'price_range',
                        'invalidation_price', 'nav_price', 'close_price',
                        'open_price', 'high_price', 'low_price',
                        'high_52w', 'low_52w', 'pe_ratio', 'eps', 'revenue',
                        'market_cap', 'market_cap_tier', 'return_1m', 'return_1y',
                        'return_1w', 'return_3m', 'return_6m', 'drawdown_from_52w_high',
                        'upside_pct', 'volatility_30d', 'volatility_90d',
                    }
                    if _at.get('predicate') in _PRICE_PREDICATES:
                        continue
                    if _at['effective_salience'] >= _threshold:
                        try:
                            _kg.add_fact(
                                _at['subject'], _at['predicate'], _at['object'],
                                source='conversation',
                                confidence=round(_at['effective_salience'], 3),
                            )
                            _conv_store.mark_atom_graduated(_at['id'])
                            _graduated.append(_at)
                        except Exception:
                            pass

                # ── Step 3: working_state persist with richer topic ──────────
                if HAS_WORKING_STATE and _graduated:
                    try:
                        _ws2 = get_working_state_store(_DB_PATH)
                        _top_subj = list(dict.fromkeys(
                            a['subject'] for a in _graduated
                        ))[:3]
                        _ws2.maybe_persist(
                            _atom_sess_id, _atom_turn,
                            goal=_atom_goal,
                            topic=', '.join(_top_subj),
                            last_intent=_atom_user_q[:120],
                            force=True,
                        )
                    except Exception:
                        pass
            except Exception:
                pass

        _threading.Thread(target=_extract_and_graduate, daemon=True).start()

    # ── Commit working memory atoms back to KB ────────────────────────────
    if HAS_WORKING_MEMORY and _working_memory and live_fetched:
        try:
            commit_result = _working_memory.commit_session(wm_session_id, _kg)
            response['kb_enriched']     = commit_result.committed > 0
            response['live_fetched']    = live_fetched
            response['atoms_committed'] = commit_result.committed
        except Exception:
            _working_memory.close_without_commit(wm_session_id)

    return jsonify(response)


@app.route('/chat/clear', methods=['POST'])
def chat_clear():
    """
    POST /chat/clear
    Body: { "session_id": "...", "user_id": "...", "purge": false }

    Clear conversation history for a session.
    purge=true also deletes DB messages; purge=false (default) only resets
    in-memory ticker state (DB history is preserved for the timeline).
    """
    data = request.get_json(force=True, silent=True) or {}
    user_id    = data.get('user_id') or getattr(g, 'user_id', None)
    purge      = bool(data.get('purge', False))
    conv_sid   = _sid_for_user(user_id) if HAS_CONV_STORE else data.get('session_id', 'default')
    _session_tickers.pop(data.get('session_id', 'default'), None)
    _session_portfolio_tickers.pop(data.get('session_id', 'default'), None)
    deleted = 0
    if purge and _conv_store is not None:
        try:
            deleted = _conv_store.delete_session_messages(conv_sid)
        except Exception:
            pass
    return jsonify({
        'session_id':    conv_sid,
        'turns_deleted': deleted,
        'purge':         purge,
        'cleared':       True,
    })


@app.route('/chat/history', methods=['GET'])
def chat_history():
    """
    GET /chat/history?limit=50&offset=0&search=

    Read-only conversation timeline for the authenticated user.
    Returns chronological user turns with paired assistant previews,
    day labels, atom counts, and graduated atom counts.
    """
    if _conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    limit    = min(int(request.args.get('limit', 50)), 200)
    offset   = int(request.args.get('offset', 0))
    search   = request.args.get('search', '').strip()
    try:
        entries = _conv_store.get_timeline(conv_sid, limit=limit, offset=offset, search=search)
        total   = _conv_store.get_total_turn_count(conv_sid)
        return jsonify({'session_id': conv_sid, 'entries': entries,
                        'total': total, 'offset': offset})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/chat/history/<int:message_id>', methods=['GET'])
def chat_history_turn(message_id):
    """
    GET /chat/history/<message_id>

    Return the full text of a user turn and its paired assistant response.
    """
    if _conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    try:
        pair = _conv_store.get_message_pair(message_id)
        if not pair:
            return jsonify({'error': 'Message not found'}), 404
        return jsonify(pair)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/chat/atoms', methods=['GET'])
def chat_atoms():
    """
    GET /chat/atoms?limit=50

    Return conversation atoms extracted from the user's session with
    effective salience scores and KB graduation status.
    Useful for interns to verify the atom extraction pipeline is working.
    """
    if _conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    limit    = min(int(request.args.get('limit', 50)), 200)
    try:
        atoms     = _conv_store.get_atoms_with_status(conv_sid, limit=limit)
        total     = len(atoms)
        graduated = sum(1 for a in atoms if a.get('graduated'))
        return jsonify({
            'session_id':     conv_sid,
            'total_atoms':    total,
            'graduated_to_kb': graduated,
            'pending':        total - graduated,
            'atoms':          atoms,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/chat/metrics', methods=['GET'])
def chat_metrics():
    """
    GET /chat/metrics

    Longitudinal cognitive metrics for the user's conversation session:
    atom growth rate, source ratios, concept entropy, graduation stats.
    """
    if _conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    try:
        return jsonify(_conv_store.get_cognitive_metrics(conv_sid))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/chat/models', methods=['GET'])
def chat_models():
    """
    List locally available Ollama models.
    Returns { "models": ["llama3.2", "mistral", ...], "default": "llama3.2" }
    Returns { "models": [], "available": false } if Ollama is unreachable.
    """
    if not HAS_LLM:
        return jsonify({'models': [], 'available': False,
                        'error': 'llm package not available'}), 503
    models = list_models()
    return jsonify({
        'models':    models,
        'default':   DEFAULT_MODEL,
        'available': bool(models),
    })


# ── Analytics endpoints ──────────────────────────────────────────────────────

@app.route('/alerts', methods=['GET'])
def alerts_list():
    """
    List alerts.

    Query params:
      all    — if 'true', return all alerts (default: unseen only)
      since  — ISO-8601 datetime, only return alerts triggered after this
      limit  — max rows (default 200)

    Returns:
      { "alerts": [...], "count": N }
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        unseen_only = request.args.get('all', '').lower() != 'true'
        since_iso   = request.args.get('since') or None
        limit       = int(request.args.get('limit', 200))
        rows = get_alerts(_DB_PATH, unseen_only=unseen_only,
                          since_iso=since_iso, limit=limit)
        return jsonify({'alerts': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/alerts/mark-seen', methods=['POST'])
def alerts_mark_seen():
    """
    Mark alerts as seen.

    Body: { "ids": [1, 2, 3] }

    Returns:
      { "updated": N }
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        body = request.get_json(force=True) or {}
        ids  = [int(i) for i in body.get('ids', [])]
        updated = mark_alerts_seen(_DB_PATH, ids)
        return jsonify({'updated': updated})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/analytics/snapshot', methods=['POST'])
def analytics_snapshot():
    """
    Capture the current KB conviction state into the signal_snapshots table.

    One row per ticker with today's UTC date as snapshot_date. Idempotent
    within a calendar day (repeated calls on the same day are no-ops).

    Returns:
      { "inserted": N, "skipped": M, "snapshot_date": "YYYY-MM-DD",
        "snapshot_count": K, "snapshots": ["YYYY-MM-DD", ...] }

    Run this today. Run it again in 4 weeks. After two snapshots exist,
    GET /analytics/backtest switches from backward-looking to forward-looking.
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        result   = take_snapshot(_DB_PATH)
        snaps    = list_snapshots(_DB_PATH)
        result['snapshot_count'] = len(snaps)
        result['snapshots']      = snaps
        return jsonify(result)
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('snapshot failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/analytics/snapshot', methods=['GET'])
def analytics_snapshot_list():
    """
    List all recorded signal snapshot dates.

    Returns:
      { "snapshot_count": K, "snapshots": ["YYYY-MM-DD", ...] }
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        snaps = list_snapshots(_DB_PATH)
        return jsonify({'snapshot_count': len(snaps), 'snapshots': snaps})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/analytics/backtest', methods=['GET'])
def analytics_backtest():
    """
    Cross-sectional KB backtest — measures whether high-conviction names
    show better trailing returns than low-conviction names.

    Query params:
      window  — return window: '1w', '1m' (default), '3m'

    Returns cohort statistics, portfolio-weighted return, and an alpha_signal
    boolean computed against a pre-committed threshold of:
      high cohort mean_return > low cohort mean_return + 1.0 percentage points

    IMPORTANT — methodology: 'point_in_time_snapshot'
      This is NOT a walk-forward backtest. It measures coherence between
      current signal state and recent trailing returns. See methodology_note
      in the response for the full disclaimer.
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    window = request.args.get('window', '1m')
    if window not in ('1w', '1m', '3m'):
        return jsonify({'error': "window must be '1w', '1m', or '3m'"}), 400

    try:
        result = run_backtest(_DB_PATH, window=window)
        return jsonify(result)
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('backtest failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/analytics/backtest/regime', methods=['GET'])
def analytics_backtest_regime():
    """
    Regime-conditional cross-sectional KB backtest.

    Partitions forward-looking backtest results by the market_regime that
    was active at the time each signal snapshot was recorded. Answers the
    question: "does high-conviction outperform in risk_on_expansion but
    not in risk_off_contraction?"

    Requires >= 2 signal snapshots (same as GET /analytics/backtest).

    Returns:
      {
        "snapshot_count": N,
        "snapshot_start": "YYYY-MM-DD",
        "snapshot_end":   "YYYY-MM-DD",
        "regimes_observed": ["risk_on_expansion", ...],
        "by_regime": {
          "risk_on_expansion": {
            "n_tickers": N,
            "alpha_signal": true,
            "alpha_explanation": "...",
            "portfolio_return": 3.2,
            "cohorts": {...},
            "ticker_detail": [...]
          },
          ...
        },
        "unconditional_cohorts": {...},
        "unconditional_alpha": bool
      }

    When snapshot_count < 2:
      { "snapshot_count": 1, "warning": "insufficient_snapshots ...", ... }
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        result = run_regime_backtest(_DB_PATH)
        return jsonify(result)
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('regime backtest failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/analytics/stress-test', methods=['POST'])
def analytics_stress_test():
    """
    Adversarial signal stress test.

    Injects pre-committed contradictory atoms into an in-memory KB copy,
    re-runs signal enrichment, and measures conviction tier degradation.

    Body (all optional):
      {
        "scenarios": ["bear_analyst", "risk_off_regime"]
        -- if omitted, all 6 scenarios run
      }

    Valid scenarios:
      bear_analyst, risk_off_regime, earnings_miss,
      macro_flip, guidance_lowered, credit_downgrade

    Returns:
      {
        "as_of": "...",
        "baseline_tickers": N,
        "scenarios_run": [...],
        "results": {
          "bear_analyst": {
            "n_tickers_tested": N, "n_degraded": M, "n_robust": K,
            "fragility_score": 0.42,
            "ticker_results": [
              { "ticker": "AAPL", "tier_before": "high",
                "tier_after": "avoid", "delta": 3, "robust": false },
              ...
            ]
          },
          ...
        },
        "portfolio_fragility": {
          "most_fragile_ticker": "AAPL",
          "mean_fragility": 0.38,
          "scenario_fragility": { "bear_analyst": 0.42, ... }
        }
      }
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    body      = request.get_json(force=True) or {}
    scenarios = body.get('scenarios') or None

    try:
        result = run_stress_test(_DB_PATH, scenarios=scenarios)
        return jsonify(result)
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('stress test failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/analytics/counterfactual', methods=['POST'])
def analytics_counterfactual():
    """
    Counterfactual reasoning — "what if X changed?"

    Applies a simulated macro/signal shift to the current KB state, re-runs
    conviction tier classification, and returns the delta across all tickers.

    Body:
      {
        "scenario": {
          "spy_signal":      "near_high",          -- direct macro override
          "hyg_signal":      "near_high",
          "tlt_signal":      "near_low",
          "market_regime":   "risk_on_expansion",
          "fed_funds_rate":  -0.25,                -- scalar: -0.25 = cut 25bps
          "credit_spreads_bps": 50,                -- scalar: +50bps = spread widening
          "tickers": {                             -- per-ticker atom overrides
            "GS": { "thesis_risk_level": "wide" }
          }
        }
      }

    Returns:
      {
        "as_of": "...",
        "scenario_applied": { ... },
        "causal_propagation": [ { seed, concept, propagated_to, mechanism } ],
        "baseline_tickers": N,
        "tier_changes": [
          { "ticker": "GS", "from": "medium", "to": "high",
            "delta": 1, "direction": "upgrade" },
          ...
        ],
        "upgrades": N,
        "downgrades": N,
        "unchanged": N,
        "regime_change": { "from": "risk_off_contraction",
                           "to":   "risk_on_expansion" },
        "methodology": "direct_override" | "causal_graph_propagation"
      }
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    body     = request.get_json(force=True) or {}
    scenario = body.get('scenario') or {}

    if not scenario:
        return jsonify({'error': 'scenario is required and must not be empty'}), 400

    try:
        result = run_counterfactual(_DB_PATH, scenario=scenario)
        return jsonify(result)
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('counterfactual failed: %s', e)
        return jsonify({'error': str(e)}), 500


@app.route('/portfolio/summary', methods=['GET'])
@limiter.exempt
def portfolio_summary():
    """
    Aggregated portfolio view from current KB signal atoms.

    Returns:
      long_book       — tickers with conviction_tier != avoid, with totals
      avoid_book      — names the KB recommends avoiding
      sector_weights  — position-size-weighted sector allocation
      macro_alignment — macro_confirmation distribution across long book
      top_conviction  — top 20 names sorted by tier then upside
      all_tickers     — full ranked list

    NOTE on total_position_pct:
      Exceeds 100% by design. See the 'total_position_pct_note' field
      in the long_book object for interpretation guidance.
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        result = build_portfolio_summary(_DB_PATH)
        return jsonify(result)
    except Exception as e:
        import logging as _logging
        _logging.getLogger(__name__).error('portfolio summary failed: %s', e)
        return jsonify({'error': str(e)}), 500


# ── Product layer — User management + Daily Briefing endpoints ───────────────

@app.route('/users/<user_id>/portfolio', methods=['POST'])
@require_auth
@rate_limit('portfolio')
def user_portfolio_submit(user_id):
    """
    Submit or replace a user's portfolio holdings.

    Body: { "holdings": [{"ticker": "AAPL", "quantity": 10, "avg_cost": 150.0, "sector": "Technology"}, ...] }
    Returns: { "user_id": "...", "count": N, "submitted_at": "...", "model": {...} }
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    holdings = data.get('holdings', [])
    if HAS_VALIDATORS:
        vr = validate_portfolio_submission(holdings)
        if not vr.valid:
            return jsonify({'error': 'validation_failed', 'details': vr.errors}), 400
    if not isinstance(holdings, list):
        return jsonify({'error': 'holdings must be a list'}), 400
    try:
        result = upsert_portfolio(_DB_PATH, user_id, holdings)
        model  = build_user_model(user_id, _DB_PATH)
        result['model'] = model
        log_audit_event(_DB_PATH, action='portfolio_submit', user_id=user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success', detail={'count': len(holdings)})
        # Wire personal KB inference from portfolio (Phase 3) — non-fatal
        if HAS_HYBRID:
            try:
                infer_and_write_from_portfolio(user_id, _DB_PATH)
            except Exception:
                pass
        return jsonify(result), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/portfolio', methods=['GET'])
@require_auth
def user_portfolio_get(user_id):
    """
    Get current portfolio holdings for a user.
    Returns: { "user_id": "...", "holdings": [...], "count": N }
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        holdings = get_portfolio(_DB_PATH, user_id)
        # Append cash as a virtual holding if set
        try:
            from users.user_store import get_available_cash as _get_cash
            _cash_data = _get_cash(_DB_PATH, user_id)
            if _cash_data['available_cash'] is not None:
                _ccy = _cash_data['cash_currency'] or 'GBP'
                holdings = list(holdings) + [{
                    'ticker':    f'CASH:{_ccy}',
                    'quantity':  1,
                    'avg_cost':  _cash_data['available_cash'],
                    'currency':  _ccy,
                    'is_cash':   True,
                    'sector':    'Cash',
                    'value':     _cash_data['available_cash'],
                }]
        except Exception:
            pass
        return jsonify({'user_id': user_id, 'holdings': holdings, 'count': len(holdings)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/portfolio/generate-sim', methods=['POST'])
@require_auth
def user_portfolio_generate_sim(user_id):
    """
    POST /users/<user_id>/portfolio/generate-sim

    Generate a seeded, realistic UK test portfolio for an intern/tester account.
    Selects one of 5 archetypes deterministically from user_id (reproducible).

    Returns:
      { archetype, description, tips_alignment, holdings, model, simulated: true }
    """
    err = assert_self(user_id)
    if err:
        return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    import hashlib as _hashlib
    import random  as _random

    _ARCHETYPES = [
        {
            'key':           'conservative_income',
            'title':         'Conservative Income Trader',
            'description':   'Focuses on FTSE defensive names and dividend payers. Low turnover, avoids high-beta cyclicals.',
            'tips_alignment': 'Tips favour low-risk setups: mitigation blocks and IFVG patterns. Expect smaller position sizes (1–2%) and tight stop levels.',
            'risk_tolerance': 'conservative',
            'holding_style':  'value',
            'sectors':       ['utilities', 'consumer_staples', 'healthcare', 'financials'],
            'holdings': [
                {'ticker': 'ULVR.L', 'quantity': 120, 'avg_cost': 3820.0,  'sector': 'consumer_staples'},
                {'ticker': 'NG.L',   'quantity': 400, 'avg_cost': 1042.0,  'sector': 'utilities'},
                {'ticker': 'TSCO.L', 'quantity': 350, 'avg_cost': 295.0,   'sector': 'consumer_staples'},
                {'ticker': 'GSK.L',  'quantity': 180, 'avg_cost': 1685.0,  'sector': 'healthcare'},
                {'ticker': 'BATS.L', 'quantity': 160, 'avg_cost': 2460.0,  'sector': 'consumer_staples'},
                {'ticker': 'NWG.L',  'quantity': 900, 'avg_cost': 285.0,   'sector': 'financials'},
            ],
        },
        {
            'key':           'ftse_momentum',
            'title':         'FTSE Momentum Trader',
            'description':   'Chases high-conviction breakouts in FTSE growth names. Sector-diverse with a bias toward industrials and tech.',
            'tips_alignment': 'Tips favour momentum breakouts: FVG and order block patterns on growth names. Expect moderate position sizes (2–3%) with wider targets.',
            'risk_tolerance': 'moderate',
            'holding_style':  'momentum',
            'sectors':       ['technology', 'industrials', 'healthcare', 'financials'],
            'holdings': [
                {'ticker': 'AZN.L',  'quantity':  80, 'avg_cost': 11200.0, 'sector': 'healthcare'},
                {'ticker': 'LSEG.L', 'quantity': 100, 'avg_cost': 9850.0,  'sector': 'financials'},
                {'ticker': 'RR.L',   'quantity': 600, 'avg_cost': 415.0,   'sector': 'industrials'},
                {'ticker': 'BA.L',   'quantity': 250, 'avg_cost': 1295.0,  'sector': 'industrials'},
                {'ticker': 'AUTO.L', 'quantity': 200, 'avg_cost': 630.0,   'sector': 'technology'},
                {'ticker': 'SAGE.L', 'quantity': 220, 'avg_cost': 1105.0,  'sector': 'technology'},
            ],
        },
        {
            'key':           'energy_commodities',
            'title':         'Commodities & Energy Trader',
            'description':   'Concentrated in FTSE energy and mining. Cyclical, regime-sensitive, comfortable with higher volatility.',
            'tips_alignment': 'Tips favour commodity cycle plays. Patterns will be regime-sensitive — expect elevated vol signals and wider stops. Stress flags fire more often for this profile.',
            'risk_tolerance': 'aggressive',
            'holding_style':  'mixed',
            'sectors':       ['energy', 'materials', 'mining'],
            'holdings': [
                {'ticker': 'SHEL.L', 'quantity': 200, 'avg_cost': 2680.0,  'sector': 'energy'},
                {'ticker': 'BP.L',   'quantity': 500, 'avg_cost': 445.0,   'sector': 'energy'},
                {'ticker': 'RIO.L',  'quantity': 120, 'avg_cost': 4950.0,  'sector': 'materials'},
                {'ticker': 'GLEN.L', 'quantity': 800, 'avg_cost': 420.0,   'sector': 'materials'},
                {'ticker': 'AAL.L',  'quantity': 450, 'avg_cost': 225.0,   'sector': 'materials'},
                {'ticker': 'BHP.L',  'quantity': 150, 'avg_cost': 2150.0,  'sector': 'materials'},
            ],
        },
        {
            'key':           'financials_heavy',
            'title':         'Financials-Concentrated Trader',
            'description':   'Heavily weighted to UK banks and financial infrastructure. High concentration risk — sensitive to rate and credit cycle.',
            'tips_alignment': 'Tips lean financials-sector. Concentration risk is flagged — watch sector stress alerts. Position sizes moderate (2–3%) with value-oriented setups.',
            'risk_tolerance': 'moderate',
            'holding_style':  'value',
            'sectors':       ['financials'],
            'holdings': [
                {'ticker': 'HSBA.L', 'quantity': 700, 'avg_cost': 680.0,   'sector': 'financials'},
                {'ticker': 'BARC.L', 'quantity': 900, 'avg_cost': 205.0,   'sector': 'financials'},
                {'ticker': 'LLOY.L', 'quantity': 2500,'avg_cost': 52.0,    'sector': 'financials'},
                {'ticker': 'STAN.L', 'quantity': 400, 'avg_cost': 690.0,   'sector': 'financials'},
                {'ticker': 'NWG.L',  'quantity': 800, 'avg_cost': 285.0,   'sector': 'financials'},
                {'ticker': 'LSEG.L', 'quantity':  90, 'avg_cost': 9850.0,  'sector': 'financials'},
            ],
        },
        {
            'key':           'high_conviction_growth',
            'title':         'High-Conviction Growth Trader',
            'description':   'Concentrated in FTSE tech and pharma growth names. Aggressive, targets T2/T3 on breakouts, tolerates wide drawdowns.',
            'tips_alignment': 'Tips favour aggressive growth plays: high quality-score patterns, T2/T3 targets. Expect larger position sizes (3–5%) and higher-conviction FVG/OB setups.',
            'risk_tolerance': 'aggressive',
            'holding_style':  'momentum',
            'sectors':       ['technology', 'healthcare'],
            'holdings': [
                {'ticker': 'AZN.L',  'quantity': 100, 'avg_cost': 11200.0, 'sector': 'healthcare'},
                {'ticker': 'SAGE.L', 'quantity': 300, 'avg_cost': 1105.0,  'sector': 'technology'},
                {'ticker': 'LSEG.L', 'quantity': 120, 'avg_cost': 9850.0,  'sector': 'technology'},
                {'ticker': 'RR.L',   'quantity': 500, 'avg_cost': 415.0,   'sector': 'industrials'},
                {'ticker': 'AUTO.L', 'quantity': 280, 'avg_cost': 630.0,   'sector': 'technology'},
                {'ticker': 'HIK.L',  'quantity': 200, 'avg_cost': 1820.0,  'sector': 'healthcare'},
            ],
        },
    ]

    # Seed selection deterministically from user_id (same archetype every call)
    seed_int = int(_hashlib.md5(user_id.encode()).hexdigest(), 16)
    archetype = _ARCHETYPES[seed_int % len(_ARCHETYPES)]

    # Apply ±10% random variation to avg_cost (seeded so repeatable)
    rng = _random.Random(seed_int)
    holdings = []
    for h in archetype['holdings']:
        jitter = 1.0 + rng.uniform(-0.10, 0.10)
        holdings.append({
            'ticker':   h['ticker'],
            'quantity': h['quantity'],
            'avg_cost': round(h['avg_cost'] * jitter, 2),
            'sector':   h['sector'],
        })

    try:
        result = upsert_portfolio(_DB_PATH, user_id, holdings)
        model  = build_user_model(user_id, _DB_PATH)
        result['model'] = model
        if HAS_HYBRID:
            try:
                infer_and_write_from_portfolio(user_id, _DB_PATH)
            except Exception:
                pass
        return jsonify({
            'simulated':      True,
            'archetype':      archetype['key'],
            'title':          archetype['title'],
            'description':    archetype['description'],
            'tips_alignment': archetype['tips_alignment'],
            'risk_tolerance': archetype['risk_tolerance'],
            'holding_style':  archetype['holding_style'],
            'sectors':        archetype['sectors'],
            'holdings':       holdings,
            'count':          len(holdings),
            'model':          model,
        }), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Paper Trading endpoints ───────────────────────────────────────────────────

_PAPER_AGENT_SYSTEM = """You are an autonomous paper trading agent. You have no emotions and no bias.
Your only goal is to evaluate KB signals and decide ENTER or SKIP.

Rules:
- ENTER only if: quality >= 0.80, conviction = HIGH or CONFIRMED, no conflicting signals
- SKIP if: put_call_ratio > 1.3 on bullish, long_end_stress = true, bear_steepen regime
- SKIP if: ticker already has an open paper position
- Entry = midpoint of zone_low/zone_high
- Stop = zone_low minus 0.5% buffer (bullish) or zone_high plus 0.5% (bearish)
- T1 = entry + (entry - stop) * 2
- T2 = entry + (entry - stop) * 3

In the reasoning field you MUST cite actual signal data — pattern type, direction, conviction tier, regime, signal direction, and any warning atoms present.
Example good reasoning: "FVG bullish 4H, quality=0.91 HIGH conviction, risk_on regime, signal_dir=bullish, PCR=0.72 — clean entry"
Example bad reasoning: "Quality and conviction meet the ENTER criteria" (DO NOT do this)

Respond ONLY with valid JSON. No explanation outside the JSON.
{"action": "ENTER"|"SKIP", "entry": float, "stop": float, "t1": float, "t2": float, "reasoning": "cite signal data, max 120 chars"}"""


def _ensure_paper_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_account (
            user_id TEXT PRIMARY KEY,
            virtual_balance REAL NOT NULL DEFAULT 500000.0,
            currency TEXT NOT NULL DEFAULT 'GBP',
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            pattern_id INTEGER,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop REAL NOT NULL,
            t1 REAL NOT NULL,
            t2 REAL,
            quantity REAL NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'open',
            partial_closed INTEGER NOT NULL DEFAULT 0,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            exit_price REAL,
            pnl_r REAL,
            note TEXT,
            ai_reasoning TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_agent_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            ticker TEXT,
            detail TEXT,
            created_at TEXT NOT NULL
        )
    """)
    # Migrate: add ai_reasoning to paper_positions if it doesn't exist yet
    try:
        conn.execute("ALTER TABLE paper_positions ADD COLUMN ai_reasoning TEXT")
    except Exception:
        pass
    conn.commit()


def _paper_tier_check(user_id):
    """Return (tier, error_response) — error_response is None if tier is pro/premium."""
    import sqlite3 as _sq3
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT tier FROM user_preferences WHERE user_id=?", (user_id,)
        ).fetchone()
        conn.close()
        tier = (row[0] if row else 'basic') or 'basic'
    except Exception:
        tier = 'basic'
    if tier not in ('pro', 'premium'):
        from flask import jsonify as _jfy
        return tier, (_jfy({'error': 'paper_trading_requires_pro', 'tier': tier}), 403)
    return tier, None


@app.route('/users/<user_id>/paper/account', methods=['GET'])
@require_auth
def paper_account_get(user_id):
    """GET /users/<user_id>/paper/account — virtual balance + summary stats."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,500000.0,'GBP',?)",
            (user_id, now_iso)
        )
        conn.commit()
        row = conn.execute(
            "SELECT virtual_balance, currency, created_at FROM paper_account WHERE user_id=?",
            (user_id,)
        ).fetchone()
        total = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE user_id=?", (user_id,)
        ).fetchone()[0]
        open_c = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE user_id=? AND status='open'", (user_id,)
        ).fetchone()[0]
        closed_rows = conn.execute(
            "SELECT pnl_r, status FROM paper_positions WHERE user_id=? AND status IN ('t1_hit','t2_hit','stopped_out','closed') AND pnl_r IS NOT NULL",
            (user_id,)
        ).fetchall()
        wins = sum(1 for r in closed_rows if r[0] > 0)
        total_closed = len(closed_rows)
        win_rate = round(wins / total_closed * 100, 1) if total_closed else None
        avg_r = round(sum(r[0] for r in closed_rows) / total_closed, 2) if total_closed else None
        # Compute unrealised P&L on open positions for account equity
        open_pos = conn.execute(
            "SELECT ticker, direction, entry_price, stop, quantity FROM paper_positions WHERE user_id=? AND status='open'",
            (user_id,)
        ).fetchall()
        conn.close()
        open_tickers2 = list({r[0] for r in open_pos})
        live2 = {}
        if open_tickers2:
            try:
                import yfinance as _yf2
                batch2 = _yf2.download(
                    open_tickers2, period='1d', interval='1m',
                    progress=False, auto_adjust=True, threads=False
                )
                for tk in open_tickers2:
                    try:
                        if len(open_tickers2) == 1:
                            live2[tk] = float(batch2['Close'].dropna().iloc[-1])
                        else:
                            live2[tk] = float(batch2['Close'][tk].dropna().iloc[-1])
                    except Exception:
                        pass
            except Exception:
                pass
        unrealised_cash = 0.0
        for r in open_pos:
            tk, direction, entry, stop_, qty = r
            cp = live2.get(tk)
            if cp is not None and entry and stop_:
                risk = abs(entry - stop_)
                if risk > 0:
                    if direction == 'bullish':
                        pnl_r = (cp - entry) / risk
                    else:
                        pnl_r = (entry - cp) / risk
                    unrealised_cash += pnl_r * risk * qty
        account_value = round(row[0] + unrealised_cash, 2)
        return jsonify({
            'user_id': user_id,
            'virtual_balance': row[0],
            'account_value': account_value,
            'unrealised_pnl': round(unrealised_cash, 2),
            'currency': row[1],
            'created_at': row[2],
            'total_trades': total,
            'open_positions': open_c,
            'closed_trades': total_closed,
            'win_rate_pct': win_rate,
            'avg_r': avg_r,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/paper/positions', methods=['GET'])
@require_auth
def paper_positions_list(user_id):
    """GET /users/<user_id>/paper/positions?status=open|closed|all"""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3
    status_filter = request.args.get('status', 'all')
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.row_factory = _sq3.Row
        if status_filter == 'open':
            rows = conn.execute(
                "SELECT * FROM paper_positions WHERE user_id=? AND status='open' ORDER BY opened_at DESC",
                (user_id,)
            ).fetchall()
        elif status_filter == 'closed':
            rows = conn.execute(
                "SELECT * FROM paper_positions WHERE user_id=? AND status NOT IN ('open') ORDER BY closed_at DESC",
                (user_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM paper_positions WHERE user_id=? ORDER BY opened_at DESC",
                (user_id,)
            ).fetchall()
        positions = [dict(r) for r in rows]
        conn.close()
        # Enrich open positions with live price + unrealised P&L
        open_tickers = list({p['ticker'] for p in positions if p['status'] == 'open'})
        live_prices = {}
        if open_tickers:
            try:
                import yfinance as _yf
                data_batch = _yf.download(
                    open_tickers, period='1d', interval='1m',
                    progress=False, auto_adjust=True, threads=False
                )
                for tk in open_tickers:
                    try:
                        if len(open_tickers) == 1:
                            price = float(data_batch['Close'].dropna().iloc[-1])
                        else:
                            price = float(data_batch['Close'][tk].dropna().iloc[-1])
                        live_prices[tk] = round(price, 4)
                    except Exception:
                        pass
            except Exception:
                pass
        for p in positions:
            if p['status'] == 'open' and p['ticker'] in live_prices:
                cp = live_prices[p['ticker']]
                p['current_price'] = cp
                risk = abs(p['entry_price'] - p['stop'])
                if risk > 0:
                    if p['direction'] == 'bullish':
                        p['unrealised_pnl_r'] = round((cp - p['entry_price']) / risk, 2)
                    else:
                        p['unrealised_pnl_r'] = round((p['entry_price'] - cp) / risk, 2)
                else:
                    p['unrealised_pnl_r'] = None
            else:
                p['current_price'] = None
                p['unrealised_pnl_r'] = None
        return jsonify({'positions': positions, 'count': len(positions)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/paper/positions', methods=['POST'])
@require_auth
def paper_position_open(user_id):
    """POST /users/<user_id>/paper/positions — open a new paper position."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3
    data = request.get_json(force=True, silent=True) or {}
    ticker    = (data.get('ticker') or '').strip().upper()
    direction = (data.get('direction') or '').strip().lower()
    try:
        entry = float(data['entry_price'])
        stop  = float(data['stop'])
        t1    = float(data['t1'])
        t2    = float(data['t2']) if data.get('t2') is not None else None
        qty   = float(data.get('quantity', 1))
    except (KeyError, ValueError, TypeError) as exc:
        return jsonify({'error': f'missing or invalid field: {exc}'}), 400
    if not ticker:
        return jsonify({'error': 'ticker is required'}), 400
    if direction not in ('bullish', 'bearish'):
        return jsonify({'error': 'direction must be bullish or bearish'}), 400
    pattern_id = data.get('pattern_id')
    note       = data.get('note', '')
    now_iso    = datetime.now(timezone.utc).isoformat()
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.execute(
            "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,500000.0,'GBP',?)",
            (user_id, now_iso)
        )
        cur = conn.execute(
            """INSERT INTO paper_positions
               (user_id, pattern_id, ticker, direction, entry_price, stop, t1, t2,
                quantity, status, partial_closed, opened_at, note)
               VALUES (?,?,?,?,?,?,?,?,?,'open',0,?,?)""",
            (user_id, pattern_id, ticker, direction, entry, stop, t1, t2, qty, now_iso, note)
        )
        pos_id = cur.lastrowid
        conn.commit()
        conn.close()
        return jsonify({'id': pos_id, 'ticker': ticker, 'direction': direction,
                        'entry_price': entry, 'stop': stop, 't1': t1, 't2': t2,
                        'quantity': qty, 'status': 'open', 'opened_at': now_iso}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/paper/positions/<int:pos_id>/close', methods=['POST'])
@require_auth
def paper_position_close(user_id, pos_id):
    """POST /users/<user_id>/paper/positions/<id>/close — manually close a position."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3
    data       = request.get_json(force=True, silent=True) or {}
    exit_price = data.get('exit_price')
    now_iso    = datetime.now(timezone.utc).isoformat()
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.row_factory = _sq3.Row
        pos = conn.execute(
            "SELECT * FROM paper_positions WHERE id=? AND user_id=?", (pos_id, user_id)
        ).fetchone()
        if not pos:
            conn.close()
            return jsonify({'error': 'position not found'}), 404
        if pos['status'] != 'open':
            conn.close()
            return jsonify({'error': 'position already closed'}), 400
        ep = float(exit_price) if exit_price is not None else pos['entry_price']
        risk = abs(pos['entry_price'] - pos['stop'])
        if risk > 0:
            if pos['direction'] == 'bullish':
                pnl_r = round((ep - pos['entry_price']) / risk, 2)
            else:
                pnl_r = round((pos['entry_price'] - ep) / risk, 2)
        else:
            pnl_r = 0.0
        conn.execute(
            "UPDATE paper_positions SET status='closed', exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
            (ep, pnl_r, now_iso, pos_id)
        )
        conn.commit()
        conn.close()
        return jsonify({'id': pos_id, 'status': 'closed', 'exit_price': ep, 'pnl_r': pnl_r, 'closed_at': now_iso})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/paper/monitor', methods=['POST'])
@require_auth
def paper_monitor(user_id):
    """POST /users/<user_id>/paper/monitor — check open positions vs live prices."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        conn = _sq3.connect(_DB_PATH, timeout=15)
        _ensure_paper_tables(conn)
        conn.row_factory = _sq3.Row
        open_pos = conn.execute(
            "SELECT * FROM paper_positions WHERE user_id=? AND status='open'", (user_id,)
        ).fetchall()
        updates = []
        for pos in open_pos:
            ticker  = pos['ticker']
            yf_sym  = _YF_MAP.get(ticker.lower(), ticker)
            try:
                info  = _yf.Ticker(yf_sym).fast_info
                price = float(info.get('last_price') or info.get('regularMarketPrice') or 0)
            except Exception:
                try:
                    hist  = _yf.Ticker(yf_sym).history(period='1d', interval='1m')
                    price = float(hist['Close'].iloc[-1]) if not hist.empty else 0
                except Exception:
                    price = 0
            if price <= 0:
                continue
            entry = pos['entry_price']
            stop  = pos['stop']
            t1    = pos['t1']
            t2    = pos['t2']
            risk  = abs(entry - stop) if abs(entry - stop) > 0 else 1
            direction = pos['direction']
            new_status = None
            exit_p = None
            if direction == 'bullish':
                if price <= stop:
                    new_status = 'stopped_out'
                    exit_p = price
                elif t2 is not None and price >= t2:
                    new_status = 't2_hit'
                    exit_p = price
                elif not pos['partial_closed'] and price >= t1:
                    conn.execute(
                        "UPDATE paper_positions SET partial_closed=1 WHERE id=?", (pos['id'],)
                    )
                    updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price})
            else:
                if price >= stop:
                    new_status = 'stopped_out'
                    exit_p = price
                elif t2 is not None and price <= t2:
                    new_status = 't2_hit'
                    exit_p = price
                elif not pos['partial_closed'] and price <= t1:
                    conn.execute(
                        "UPDATE paper_positions SET partial_closed=1 WHERE id=?", (pos['id'],)
                    )
                    updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price})
            if new_status and exit_p is not None:
                if direction == 'bullish':
                    pnl_r = round((exit_p - entry) / risk, 2)
                else:
                    pnl_r = round((entry - exit_p) / risk, 2)
                conn.execute(
                    "UPDATE paper_positions SET status=?, exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
                    (new_status, exit_p, pnl_r, now_iso, pos['id'])
                )
                updates.append({'id': pos['id'], 'ticker': ticker, 'event': new_status, 'price': price, 'pnl_r': pnl_r})
        conn.commit()
        conn.close()
        return jsonify({'checked': len(open_pos), 'updates': updates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/paper/stats', methods=['GET'])
@require_auth
def paper_stats(user_id):
    """GET /users/<user_id>/paper/stats — performance breakdown."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.row_factory = _sq3.Row
        closed = conn.execute(
            """SELECT p.*, ps.kb_conviction, ps.pattern_type
               FROM paper_positions p
               LEFT JOIN pattern_signals ps ON p.pattern_id = ps.id
               WHERE p.user_id=? AND p.status NOT IN ('open') AND p.pnl_r IS NOT NULL
               ORDER BY p.closed_at DESC""",
            (user_id,)
        ).fetchall()
        conn.close()
        rows = [dict(r) for r in closed]
        def _group_stats(items, key):
            groups = {}
            for r in items:
                k = r.get(key) or 'unknown'
                groups.setdefault(k, []).append(r['pnl_r'])
            result = []
            for k, pnls in groups.items():
                wins = sum(1 for p in pnls if p > 0)
                result.append({
                    'label': k,
                    'trades': len(pnls),
                    'wins': wins,
                    'win_rate_pct': round(wins / len(pnls) * 100, 1),
                    'avg_r': round(sum(pnls) / len(pnls), 2),
                })
            return sorted(result, key=lambda x: -x['trades'])
        best  = max(rows, key=lambda r: r['pnl_r'], default=None)
        worst = min(rows, key=lambda r: r['pnl_r'], default=None)
        return jsonify({
            'total_closed': len(rows),
            'by_conviction': _group_stats(rows, 'kb_conviction'),
            'by_pattern_type': _group_stats(rows, 'pattern_type'),
            'best_trade': best,
            'worst_trade': worst,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


_PAPER_MAX_OPEN_POSITIONS  = 12  # never hold more than this many concurrent positions
_PAPER_MAX_NEW_PER_SCAN    = 3   # max new entries opened in a single scan run


def _paper_kb_chat(ticker: str, question: str, kg_conn) -> tuple[str, int] | tuple[None, int]:
    """
    Route a paper-agent decision through the same KB-aware pipeline as /chat:
      retrieve(ticker query) → build_prompt(system_override) → _llm_chat
    Returns (raw_llm_text, atom_count), or (None, 0) on failure.
    atom_count is the number of KB atoms retrieved — used to compute kb_depth.
    """
    if not HAS_LLM:
        return None, 0
    try:
        snippet, atoms = retrieve(question, kg_conn, limit=30)
        atom_count = len(atoms)
        messages = build_prompt(
            user_message=question,
            snippet=snippet,
            atom_count=atom_count,
            trader_level='developing',
        )
        # Replace the system message with the paper-agent system prompt so the
        # LLM responds with JSON and cites KB signal data in its reasoning.
        if messages and messages[0]['role'] == 'system':
            messages[0]['content'] = (
                _PAPER_AGENT_SYSTEM + '\n\n'
                + messages[0]['content']
            )
        return _llm_chat(messages), atom_count
    except Exception as _e:
        import logging as _lg
        _lg.getLogger('paper_agent').warning('_paper_kb_chat error for %s: %s', ticker, _e)
        return None, 0


def _paper_ai_run(user_id: str) -> dict:
    """
    Core autonomous paper trading agent for one user.

    1. Logs scan_start
    2. Fetches top open patterns (quality>=0.75, HIGH/CONFIRMED conviction)
    3. Skips tickers already in open positions
    4. For each candidate: calls LLM (or rule-based fallback) → ENTER or SKIP
    5. Runs existing price monitor on open positions
    6. Returns summary dict
    """
    import sqlite3 as _sq3
    import json as _json
    import logging as _logging

    _log = _logging.getLogger('paper_agent')
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        conn = _sq3.connect(_DB_PATH, timeout=15)
        _ensure_paper_tables(conn)
        conn.row_factory = _sq3.Row

        # Ensure account row exists
        conn.execute(
            "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,500000.0,'GBP',?)",
            (user_id, now_iso)
        )

        # --- Get tickers already in open positions (skip duplicates) ---
        open_rows = conn.execute(
            "SELECT ticker FROM paper_positions WHERE user_id=? AND status='open'",
            (user_id,)
        ).fetchall()
        open_tickers = {r['ticker'] for r in open_rows}

        # --- 24h cooldown: skip tickers stopped out in the last 24 hours ---
        from datetime import timedelta as _timedelta
        _cooldown_cutoff = (datetime.now(timezone.utc) - _timedelta(hours=24)).isoformat()
        _cooldown_rows = conn.execute(
            """SELECT DISTINCT ticker FROM paper_positions
               WHERE user_id=? AND status='stopped_out' AND closed_at > ?""",
            (user_id, _cooldown_cutoff)
        ).fetchall()
        cooled_tickers = {r['ticker'] for r in _cooldown_rows}

        # Hard cap: do not open any new positions if already at max
        if len(open_tickers) >= _PAPER_MAX_OPEN_POSITIONS:
            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                (user_id, 'scan_start', None,
                 f'Scan skipped — already at max {_PAPER_MAX_OPEN_POSITIONS} open positions', now_iso)
            )
            conn.commit()
            conn.close()
            return {'entries': 0, 'skips': 0, 'monitor_updates': []}

        # Fetch current balance for position sizing
        acct_row = conn.execute(
            "SELECT virtual_balance FROM paper_account WHERE user_id=?", (user_id,)
        ).fetchone()
        balance = float(acct_row['virtual_balance']) if acct_row else 500000.0

        # Pull risk % from user preferences (default 1% if not set)
        _pref_row = None
        try:
            _pref_row = conn.execute(
                "SELECT max_risk_per_trade_pct FROM user_preferences WHERE user_id=?",
                (user_id,)
            ).fetchone()
        except Exception:
            pass
        risk_pct = float((_pref_row[0] if _pref_row and _pref_row[0] else None) or 1.0)
        risk_pct = min(risk_pct, 2.0)  # safety cap: never risk more than 2% per trade regardless of prefs
        risk_per_trade = balance * risk_pct / 100.0
        max_position_value = balance * 0.10  # hard cap: no single position > 10% of account
        remaining_cash = balance  # track available cash this run

        entries = 0
        skips   = 0

        # Log scan start
        conn.execute(
            "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
            (user_id, 'scan_start', None,
             f'Scanning open patterns for {user_id} ({len(open_tickers)}/{_PAPER_MAX_OPEN_POSITIONS} slots used, {len(cooled_tickers)} on 24h cooldown)', now_iso)
        )
        conn.commit()

        # --- Fetch candidate patterns ---
        # Best pattern per unique ticker via GROUP BY (fast, avoids correlated subquery on 16k rows)
        candidate_rows = conn.execute(
            """SELECT p.id, p.ticker, p.pattern_type, p.direction, p.zone_high, p.zone_low,
                      p.quality_score, p.kb_conviction, p.kb_regime, p.kb_signal_dir
               FROM pattern_signals p
               INNER JOIN (
                   SELECT ticker, MAX(quality_score) AS best_q
                   FROM pattern_signals
                   WHERE status NOT IN ('filled','broken')
                     AND quality_score >= 0.70
                     AND LOWER(kb_conviction) IN ('high','confirmed','strong')
                   GROUP BY ticker
               ) best ON best.ticker = p.ticker AND best.best_q = p.quality_score
               WHERE p.status NOT IN ('filled','broken')
                 AND p.quality_score >= 0.70
                 AND LOWER(p.kb_conviction) IN ('high','confirmed','strong')
               ORDER BY RANDOM()
               LIMIT 100"""
        ).fetchall()
        import random as _random
        # Sort into quality bands then shuffle within each band for diversity
        all_cands = [dict(r) for r in candidate_rows]
        high_band  = [c for c in all_cands if c['quality_score'] >= 0.85]
        mid_band   = [c for c in all_cands if 0.75 <= c['quality_score'] < 0.85]
        low_band   = [c for c in all_cands if c['quality_score'] < 0.75]
        _random.shuffle(high_band)
        _random.shuffle(mid_band)
        _random.shuffle(low_band)
        candidates = (high_band + mid_band + low_band)[:50]

        # --- Fetch warning atoms for SKIP conditions (PCR, long_end_stress, regime) ---
        _warning_atoms = {}
        try:
            atom_rows = conn.execute(
                """SELECT subject, predicate, object FROM knowledge_atoms
                   WHERE predicate IN ('put_call_ratio','long_end_stress','price_regime','bear_steepener')
                   ORDER BY confidence DESC LIMIT 30"""
            ).fetchall()
            for ar in atom_rows:
                _warning_atoms.setdefault(ar[0], {})[ar[1]] = ar[2]
        except Exception:
            pass

        conn.row_factory = None

        scanned = len(candidates)
        evaluated = []

        for c in candidates:
            ticker    = c['ticker']
            direction = c['direction']
            quality   = c.get('quality_score') or 0
            conviction = (c.get('kb_conviction') or '').upper()
            zone_low  = float(c.get('zone_low') or 0)
            zone_high = float(c.get('zone_high') or 0)
            regime    = (c.get('kb_regime') or '').lower()
            pattern_id = c['id']

            # Hard rule: skip already-open tickers
            if ticker in open_tickers:
                skips += 1
                continue

            # 24h cooldown: skip tickers stopped out in the last 24 hours
            if ticker in cooled_tickers:
                skips += 1
                continue

            # Pre-LLM rule-based SKIP checks
            skip_reason = None
            _ticker_atoms = _warning_atoms.get(ticker, {})
            try:
                pcr = float(_ticker_atoms.get('put_call_ratio', 0) or 0)
                if direction == 'bullish' and pcr > 1.3:
                    skip_reason = f'PCR={pcr:.2f} > 1.3 on bullish'
            except Exception:
                pass
            if not skip_reason:
                if _ticker_atoms.get('long_end_stress', '').lower() in ('true', '1', 'yes'):
                    skip_reason = 'long_end_stress=true'
            if not skip_reason:
                if 'bear_steepen' in regime or 'risk_off' in regime:
                    skip_reason = f'regime={regime} unfavourable'

            # Rule-based price calculation
            midpoint = (zone_low + zone_high) / 2.0 if zone_low and zone_high else None
            if midpoint and midpoint > 0:
                if direction == 'bullish':
                    entry_p = midpoint
                    stop_p  = round(zone_low * 0.995, 6)  # -0.5% buffer
                    risk    = entry_p - stop_p
                    t1_p    = round(entry_p + risk * 2, 6)
                    t2_p    = round(entry_p + risk * 3, 6)
                else:
                    entry_p = midpoint
                    stop_p  = round(zone_high * 1.005, 6)  # +0.5% buffer
                    risk    = stop_p - entry_p
                    t1_p    = round(entry_p - risk * 2, 6)
                    t2_p    = round(entry_p - risk * 3, 6)
            else:
                skips += 1
                continue

            if risk <= 0:
                skips += 1
                continue

            action    = 'SKIP' if skip_reason else 'ENTER'
            pattern_type = c.get('pattern_type', '?')
            kb_signal_dir = (c.get('kb_signal_dir') or '').lower() or '?'
            reasoning = skip_reason or (
                f'{pattern_type} {direction} | q={quality:.2f} {conviction} '
                f'regime={regime or "?"} signal_dir={kb_signal_dir}'
            )

            # ── KB-aware LLM decision (routes through retrieve + build_prompt) ──
            if not skip_reason:
                # Build a natural-language question so retrieve() pulls the right KB atoms
                kb_question = (
                    f"Paper trading decision for {ticker}: should I enter a {direction} position? "
                    f"Pattern type: {c.get('pattern_type','?')}. "
                    f"Quality score: {quality:.2f}. Conviction: {conviction}. "
                    f"Regime: {regime or '?'}. Signal direction: {c.get('kb_signal_dir','?')}. "
                    f"Zone: {zone_low}–{zone_high}. "
                    f"Rule-based levels — entry: {entry_p:.4f}, stop: {stop_p:.4f}, "
                    f"t1: {t1_p:.4f}, t2: {t2_p:.4f}. "
                    f"Warning atoms present: {_ticker_atoms or 'none'}. "
                    f"Reply with JSON only: {{\"action\":\"ENTER\"|\"SKIP\", "
                    f"\"entry\":float, \"stop\":float, \"t1\":float, \"t2\":float, "
                    f"\"reasoning\":\"cite specific KB signals, max 150 chars\"}}"
                )
                try:
                    kg_conn = _kg.thread_local_conn()
                    raw, atom_count = _paper_kb_chat(ticker, kb_question, kg_conn)
                    kb_depth = 'deep' if atom_count >= 15 else 'shallow' if atom_count >= 5 else 'thin'
                    if raw:
                        raw = raw.strip()
                        # Extract JSON even if LLM wraps it in prose
                        start = raw.find('{')
                        end   = raw.rfind('}') + 1
                        if start >= 0 and end > start:
                            parsed = _json.loads(raw[start:end])
                            action = parsed.get('action', 'SKIP').upper()
                            llm_reasoning = parsed.get('reasoning', reasoning)[:200]
                            reasoning = f'{llm_reasoning} | kb_depth={kb_depth} ({atom_count} atoms)'
                            if action == 'ENTER':
                                entry_p = float(parsed.get('entry', entry_p))
                                stop_p  = float(parsed.get('stop',  stop_p))
                                t1_p    = float(parsed.get('t1',    t1_p))
                                t2_p    = float(parsed.get('t2',    t2_p))
                                risk    = abs(entry_p - stop_p)
                    else:
                        # LLM unavailable — append kb_depth to rule-based reasoning
                        reasoning = f'{reasoning} | kb_depth={kb_depth} ({atom_count} atoms)'
                except Exception as llm_err:
                    _log.warning('KB-chat paper agent error for %s: %s', ticker, llm_err)
                    # Fall through with rule-based result

            evaluated.append({'ticker': ticker, 'action': action, 'reasoning': reasoning})

            # Per-scan entry cap — prefer highest quality, don't bulk-enter everything
            if action == 'ENTER' and entries >= _PAPER_MAX_NEW_PER_SCAN:
                skips += 1
                continue

            if action == 'ENTER' and risk > 0:
                # Size quantity: risk_per_trade / risk_per_unit gives shares/units
                qty = round(risk_per_trade / risk, 4) if risk > 0 else 1.0
                qty = max(qty, 0.0001)  # floor
                position_value = round(entry_p * qty, 2)
                # Cap: never allocate more than 15% of account to one position
                if position_value > max_position_value:
                    qty = round(max_position_value / entry_p, 4)
                    position_value = round(entry_p * qty, 2)
                # ── Cash constraint: skip if we can't afford it ──────────
                if position_value > remaining_cash:
                    skips += 1
                    conn.row_factory = None
                    conn.execute(
                        "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                        (user_id, 'skip', ticker,
                         f'Insufficient cash: need £{position_value:,.2f}, have £{remaining_cash:,.2f}',
                         now_iso)
                    )
                    continue
                # Deduct from balance
                remaining_cash -= position_value
                conn.row_factory = None
                conn.execute(
                    "UPDATE paper_account SET virtual_balance = virtual_balance - ? WHERE user_id=?",
                    (position_value, user_id)
                )
                # Insert position
                conn.execute(
                    """INSERT INTO paper_positions
                       (user_id, pattern_id, ticker, direction, entry_price, stop, t1, t2,
                        quantity, status, partial_closed, opened_at, note, ai_reasoning)
                       VALUES (?,?,?,?,?,?,?,?,?,'open',0,?,?,?)""",
                    (user_id, pattern_id, ticker, direction,
                     entry_p, stop_p, t1_p, t2_p, qty,
                     now_iso, f'AI agent: {c.get("pattern_type","")}', reasoning)
                )
                conn.execute(
                    "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                    (user_id, 'entry', ticker,
                     f'{direction} entry={entry_p:.4f} stop={stop_p:.4f} t1={t1_p:.4f} qty={qty:.4f} value=£{position_value:,.2f} cash_remaining=£{remaining_cash:,.2f} | {reasoning}',
                     now_iso)
                )
                open_tickers.add(ticker)
                entries += 1
            else:
                skips += 1

        # Summarise skips in one log entry (not per-skip)
        if skips > 0:
            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                (user_id, 'skip', None,
                 f'Scanned {scanned} patterns — {entries} entr{"y" if entries==1 else "ies"}, {skips} skipped',
                 now_iso)
            )

        conn.commit()

        # ── Price monitor on all open positions ───────────────────────
        monitor_updates = []
        try:
            conn.row_factory = _sq3.Row
            open_pos = conn.execute(
                "SELECT * FROM paper_positions WHERE user_id=? AND status='open'",
                (user_id,)
            ).fetchall()
            import yfinance as _yf
            for pos in open_pos:
                ticker    = pos['ticker']
                direction = pos['direction']
                entry     = pos['entry_price']
                stop      = pos['stop']
                t1        = pos['t1']
                t2        = pos['t2']
                risk      = abs(entry - stop)
                if risk == 0:
                    continue
                try:
                    hist = _yf.Ticker(ticker).history(period='1d', interval='1m')
                    price = float(hist['Close'].iloc[-1]) if not hist.empty else None
                except Exception:
                    price = None
                if price is None:
                    continue
                new_status = None
                exit_p     = None
                if direction == 'bullish':
                    if price <= stop:
                        new_status, exit_p = 'stopped_out', price
                    elif t2 and price >= t2:
                        new_status, exit_p = 't2_hit', price
                    elif not pos['partial_closed'] and price >= t1:
                        conn.execute("UPDATE paper_positions SET partial_closed=1 WHERE id=?", (pos['id'],))
                        monitor_updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price})
                else:
                    if price >= stop:
                        new_status, exit_p = 'stopped_out', price
                    elif t2 and price <= t2:
                        new_status, exit_p = 't2_hit', price
                    elif not pos['partial_closed'] and price <= t1:
                        conn.execute("UPDATE paper_positions SET partial_closed=1 WHERE id=?", (pos['id'],))
                        monitor_updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price})
                if new_status and exit_p is not None:
                    if direction == 'bullish':
                        pnl_r = round((exit_p - entry) / risk, 2)
                    else:
                        pnl_r = round((entry - exit_p) / risk, 2)
                    conn.execute(
                        "UPDATE paper_positions SET status=?, exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
                        (new_status, exit_p, pnl_r, now_iso, pos['id'])
                    )
                    # Refund exit value to balance so agent can redeploy cash
                    qty = pos['quantity'] or 1
                    exit_value = round(exit_p * qty, 2)
                    conn.execute(
                        "UPDATE paper_account SET virtual_balance = virtual_balance + ? WHERE user_id=?",
                        (exit_value, user_id)
                    )
                    conn.execute(
                        "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                        (user_id, new_status, ticker,
                         f'exit={exit_p:.4f} P&L={pnl_r:+.2f}R refund=£{exit_value:,.2f}', now_iso)
                    )
                    monitor_updates.append({'id': pos['id'], 'ticker': ticker, 'event': new_status, 'pnl_r': pnl_r})
            conn.commit()
        except Exception as mon_err:
            _log.warning('Paper monitor error for %s: %s', user_id, mon_err)

        if monitor_updates:
            conn.row_factory = None
            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                (user_id, 'monitor_run', None,
                 f'Monitor: {len(monitor_updates)} update(s)', now_iso)
            )
            conn.commit()

        conn.close()
        return {'entries': entries, 'skips': skips, 'monitor_updates': monitor_updates}

    except Exception as e:
        import logging as _log2
        _log2.getLogger('paper_agent').error('_paper_ai_run error for %s: %s', user_id, e)
        return {'error': str(e)}


def _paper_ai_global_run():
    """Called by PaperAgentAdapter scheduler — runs agent for every pro/premium user."""
    try:
        from users.user_store import get_pro_premium_users
        users = get_pro_premium_users(_DB_PATH)
    except Exception:
        users = []
    for uid in users:
        try:
            _paper_ai_run(uid)
        except Exception:
            pass


class PaperAgentAdapter:
    """Ingest-scheduler-compatible adapter that runs the autonomous paper trading agent."""
    name = 'paper_agent'

    def run(self) -> None:
        _paper_ai_global_run()


# Register paper agent adapter now that the class is defined
if _ingest_scheduler is not None:
    try:
        _ingest_scheduler.register(PaperAgentAdapter(), interval_sec=1800)  # 30 min autonomous paper trading
    except Exception as _pae:
        import logging as _logging
        _logging.getLogger(__name__).error('Failed to register PaperAgentAdapter: %s', _pae)


@app.route('/users/<user_id>/paper/agent/log', methods=['GET'])
@require_auth
def paper_agent_log_get(user_id):
    """GET /users/<user_id>/paper/agent/log — last 100 agent activity entries."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.row_factory = _sq3.Row
        rows = conn.execute(
            """SELECT id, user_id, event_type, ticker, detail, created_at
               FROM paper_agent_log
               WHERE user_id=?
               ORDER BY created_at DESC LIMIT 100""",
            (user_id,)
        ).fetchall()
        conn.close()
        return jsonify({'log': [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Continuous scanner state ──────────────────────────────────────────────────
_paper_scanner_threads = {}   # user_id -> threading.Event (stop signal)

def _paper_continuous_scan(user_id, stop_event, interval_sec=120):
    """Loop: scan every interval_sec until stop_event is set."""
    import logging as _lg
    _lg.getLogger('paper_agent').info('Continuous scanner started for %s', user_id)
    while not stop_event.is_set():
        try:
            _paper_ai_run(user_id)
        except Exception as _e:
            _lg.getLogger('paper_agent').error('Scanner error for %s: %s', user_id, _e)
        stop_event.wait(interval_sec)
    _lg.getLogger('paper_agent').info('Continuous scanner stopped for %s', user_id)


@app.route('/users/<user_id>/paper/agent/run', methods=['POST'])
@require_auth
def paper_agent_run_once(user_id):
    """POST /users/<user_id>/paper/agent/run — one-shot scan, returns result synchronously."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    try:
        result = _paper_ai_run(user_id)
        return jsonify({'status': 'ok', 'result': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/paper/agent/start', methods=['POST'])
@require_auth
def paper_agent_start(user_id):
    """POST /users/<user_id>/paper/agent/start — start continuous scanner."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import threading as _threading
    if user_id in _paper_scanner_threads and _paper_scanner_threads[user_id].is_set() is False:
        return jsonify({'status': 'already_running', 'message': 'Scanner already running'})
    stop_ev = _threading.Event()
    _paper_scanner_threads[user_id] = stop_ev
    t = _threading.Thread(target=_paper_continuous_scan, args=(user_id, stop_ev, 1800), daemon=True)
    t.start()
    return jsonify({'status': 'started', 'message': 'Continuous scanner started — scans every 30 min'})


@app.route('/users/<user_id>/paper/agent/stop', methods=['POST'])
@require_auth
def paper_agent_stop(user_id):
    """POST /users/<user_id>/paper/agent/stop — stop continuous scanner."""
    err = assert_self(user_id)
    if err: return err
    ev = _paper_scanner_threads.get(user_id)
    if ev:
        ev.set()
        del _paper_scanner_threads[user_id]
        return jsonify({'status': 'stopped', 'message': 'Scanner stopped'})
    return jsonify({'status': 'not_running', 'message': 'Scanner was not running'})


@app.route('/users/<user_id>/paper/agent/status', methods=['GET'])
@require_auth
def paper_agent_status(user_id):
    """GET /users/<user_id>/paper/agent/status — is scanner running?"""
    err = assert_self(user_id)
    if err: return err
    running = user_id in _paper_scanner_threads and not _paper_scanner_threads[user_id].is_set()
    return jsonify({'running': running})


@app.route('/users/<user_id>/paper/agent/log/export', methods=['GET'])
@require_auth
def paper_agent_log_export(user_id):
    """GET /users/<user_id>/paper/agent/log/export — full audit log as CSV."""
    err = assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    import sqlite3 as _sq3, csv as _csv, io as _io
    try:
        conn = _sq3.connect(_DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.row_factory = _sq3.Row
        # Agent activity log
        log_rows = conn.execute(
            "SELECT id, event_type, ticker, detail, created_at FROM paper_agent_log WHERE user_id=? ORDER BY created_at ASC",
            (user_id,)
        ).fetchall()
        # All positions
        pos_rows = conn.execute(
            """SELECT id, ticker, direction, entry_price, stop, t1, t2, quantity,
                      status, partial_closed, opened_at, closed_at, exit_price, pnl_r, ai_reasoning
               FROM paper_positions WHERE user_id=? ORDER BY opened_at ASC""",
            (user_id,)
        ).fetchall()
        conn.close()
        buf = _io.StringIO()
        w = _csv.writer(buf)
        # Section 1: positions
        w.writerow(['=== POSITIONS ==='])
        w.writerow(['id','ticker','direction','entry_price','stop','t1','t2','quantity',
                    'status','partial_closed','opened_at','closed_at','exit_price','pnl_r','ai_reasoning'])
        for r in pos_rows:
            w.writerow(list(r))
        w.writerow([])
        # Section 2: agent log
        w.writerow(['=== AGENT LOG ==='])
        w.writerow(['id','event_type','ticker','detail','created_at'])
        for r in log_rows:
            w.writerow(list(r))
        from flask import Response as _Resp
        csv_bytes = buf.getvalue().encode('utf-8')
        fname = f'paper_trade_log_{user_id}_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}.csv'
        return _Resp(
            csv_bytes,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{fname}"'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/history/screenshot', methods=['POST'])
@require_auth
def user_portfolio_screenshot(user_id: str):
    """
    POST /users/<user_id>/history/screenshot — extract holdings from broker screenshot.

    Accepts multipart/form-data with a 'file' field (image/png or image/jpeg).
    Sends the image to the local Ollama vision model (llava) and returns extracted
    holdings as a JSON list.

    Returns:
      { "holdings": [{"ticker": "SHEL.L", "quantity": 10, "avg_cost": 27.50}, ...],
        "vision_available": true }
    or on model unavailable:
      { "holdings": [], "vision_available": false, "reason": "vision_model_unavailable" }
    """
    err = assert_self(user_id)
    if err: return err

    import base64 as _b64
    from llm.ollama_client import chat_vision, list_models, VISION_MODEL

    # Check vision model is available
    available_models = list_models()
    vision_available = any(VISION_MODEL.split(':')[0] in m for m in available_models)
    if not vision_available:
        return jsonify({
            'holdings': [],
            'vision_available': False,
            'reason': 'vision_model_unavailable',
            'available_models': available_models,
        }), 200

    if 'file' not in request.files:
        return jsonify({'error': 'file field required'}), 400

    f = request.files['file']
    if not f.content_type or not f.content_type.startswith('image/'):
        return jsonify({'error': 'file must be an image (image/png or image/jpeg)'}), 400

    image_bytes = f.read()
    if len(image_bytes) > 10 * 1024 * 1024:
        return jsonify({'error': 'image too large (max 10 MB)'}), 400

    image_b64 = _b64.b64encode(image_bytes).decode('utf-8')

    prompt = (
        "This is a screenshot of a stock brokerage portfolio page. "
        "Extract all stock holdings visible in the image. "
        "For each holding, identify: the ticker symbol, the quantity held, and the average cost/price per share if visible. "
        "LSE-listed UK stocks use a .L suffix (e.g. SHEL.L, BARC.L). "
        "Respond with ONLY valid JSON — no markdown, no explanation. "
        "Format: [{\"ticker\": \"SHEL.L\", \"quantity\": 10, \"avg_cost\": 27.50}, ...] "
        "If avg_cost is not visible, set it to null. "
        "If no holdings are visible, return []."
    )

    try:
        raw = chat_vision(image_b64, prompt, timeout=90)
        if not raw:
            return jsonify({'holdings': [], 'vision_available': True, 'reason': 'model_returned_empty'}), 200

        # Strip markdown fences if present
        raw = raw.strip()
        if raw.startswith('```'):
            raw = '\n'.join(l for l in raw.split('\n') if not l.startswith('```'))

        import json as _json
        holdings = _json.loads(raw)
        if not isinstance(holdings, list):
            holdings = []

        # Normalise: ensure required keys, coerce types
        clean = []
        for h in holdings:
            ticker = str(h.get('ticker') or '').strip().upper()
            if not ticker:
                continue
            try:
                qty = float(h.get('quantity') or 0)
            except (TypeError, ValueError):
                qty = 0.0
            avg_cost = h.get('avg_cost')
            try:
                avg_cost = float(avg_cost) if avg_cost is not None else None
            except (TypeError, ValueError):
                avg_cost = None
            clean.append({'ticker': ticker, 'quantity': qty, 'avg_cost': avg_cost})

        return jsonify({'holdings': clean, 'vision_available': True, 'count': len(clean)})

    except Exception as e:
        return jsonify({'error': str(e), 'holdings': [], 'vision_available': True}), 500


@app.route('/users/<user_id>/model', methods=['GET'])
@require_auth
def user_model_get(user_id):
    """
    Get the derived user model for a user.
    Returns the user_models row or 404 if no model exists yet.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        model = get_user_model(_DB_PATH, user_id)
        if model is None:
            return jsonify({'error': 'no model found — submit portfolio first'}), 404
        return jsonify(model)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/onboarding', methods=['POST'])
@require_auth
def user_onboarding(user_id):
    """
    Set onboarding preferences (fallback path — no portfolio required).

    Body:
      {
        "selected_sectors": ["technology", "financials"],
        "risk_tolerance": "moderate",
        "delivery_time": "08:00",
        "timezone": "Europe/London",
        "telegram_chat_id": "123456789"
      }
    Returns: updated user_preferences row.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    if HAS_VALIDATORS:
        vr = validate_onboarding(data)
        if not vr.valid:
            return jsonify({'error': 'validation_failed', 'details': vr.errors}), 400
    try:
        # Check if this is a first-time Telegram link (no existing chat_id)
        new_chat_id = data.get('telegram_chat_id')
        existing_chat_id = None
        if new_chat_id:
            import sqlite3 as _sq
            _c = _sq.connect(_DB_PATH, timeout=5)
            try:
                _row = _c.execute(
                    "SELECT telegram_chat_id FROM user_preferences WHERE user_id=?",
                    (user_id,)
                ).fetchone()
                existing_chat_id = (_row[0] or '').strip() if _row else ''
            finally:
                _c.close()

        prefs = update_preferences(
            _DB_PATH, user_id,
            selected_sectors=data.get('selected_sectors'),
            selected_risk=data.get('risk_tolerance'),
            telegram_chat_id=new_chat_id,
            delivery_time=data.get('delivery_time'),
            timezone_str=data.get('timezone'),
            onboarding_complete=1,
        )
        log_audit_event(_DB_PATH, action='onboarding_update', user_id=user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')

        # Send welcome message on first-time Telegram link
        if new_chat_id and not existing_chat_id:
            try:
                from notifications.telegram_notifier import TelegramNotifier as _TGN
                _notifier = _TGN()
                _welcome = (
                    "👋 *Welcome to Trading Galaxy\\!*\n\n"
                    "You're now connected\\. Here's what happens from here:\n\n"
                    "📅 *Monday briefing* — Your week ahead: open positions, new pattern setups sized to your account, "
                    "and anything that closed or expired last week\\.\n\n"
                    "📍 *Wednesday update* — A compound update on all your open positions: "
                    "what's changed in the knowledge base since Monday, any KB signal shifts, regime changes\\.\n\n"
                    "⚡ *Real\\-time alerts* — If a position you're tracking hits its target zone, "
                    "stop zone, or the KB confidence deteriorates, you'll get an immediate alert here "
                    "with a recommended action\\.\n\n"
                    "💰 *Profit alerts* — When you're sitting on a gain and KB signals are weakening, "
                    "we'll flag it before the move reverses\\. T1 hit with strong KB \\= hold\\. "
                    "T1 hit with deteriorating KB \\= exit signal\\.\n\n"
                    "🧠 *AI Chat* — Ask anything about your positions, signals, or market conditions "
                    "at [app\\.trading\\-galaxy\\.uk](https://app.trading-galaxy.uk)\\.\n\n"
                    "Use the *Tips* tab to configure your delivery time, timeframes, and pattern types\\. "
                    "Hit *Taking it* on any setup to activate full position tracking\\.\n\n"
                    "_Trading Galaxy — epistemic signals, not noise\\._"
                )
                _notifier.send(new_chat_id, _welcome, parse_mode='MarkdownV2')
            except Exception:
                pass  # Non-fatal — don't fail the onboarding if welcome message fails

        return jsonify(prefs)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/snapshot/preview', methods=['GET'])
@require_auth
def user_snapshot_preview(user_id):
    """
    Generate a personalised snapshot as JSON without sending it to Telegram.
    Used for UI preview — shows users what their briefing will look like.

    Returns: full CuratedSnapshot as a dict.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        snapshot = curate_snapshot(user_id, _DB_PATH)
        return jsonify(snapshot_to_dict(snapshot))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/snapshot/send-now', methods=['POST'])
@require_auth
@rate_limit('snapshot')
def user_snapshot_send_now(user_id):
    """
    Trigger an immediate snapshot delivery to the user's Telegram chat ID.
    Used for testing and on-demand delivery.

    Returns: { "sent": true/false, "opportunities": N, "regime": "...", "error": "..." }
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        from users.user_store import get_user, log_delivery
        user = get_user(_DB_PATH, user_id)
        chat_id = (user or {}).get('telegram_chat_id')
        if not chat_id:
            return jsonify({'error': 'no telegram_chat_id — complete onboarding first'}), 400

        snapshot = curate_snapshot(user_id, _DB_PATH)
        message  = format_snapshot(snapshot)
        notifier = TelegramNotifier()
        sent     = notifier.send(chat_id, message)
        # Use UTC date as local_date for on-demand sends (no timezone context here)
        from datetime import datetime as _dt, timezone as _tz
        local_date = _dt.now(_tz.utc).strftime('%Y-%m-%d')
        log_delivery(
            _DB_PATH, user_id,
            success=sent,
            message_length=len(message),
            regime_at_delivery=snapshot.market_regime,
            opportunities_count=len(snapshot.top_opportunities),
            local_date=local_date,
        )
        return jsonify({
            'sent':          sent,
            'opportunities': len(snapshot.top_opportunities),
            'regime':        snapshot.market_regime,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/delivery-history', methods=['GET'])
@require_auth
def user_delivery_history(user_id):
    """
    Get past delivery log entries for a user.
    Query param: limit (default 30)
    Returns: { "user_id": "...", "history": [...], "count": N }
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    limit = int(request.args.get('limit', 30))
    try:
        history = get_delivery_history(_DB_PATH, user_id, limit=limit)
        return jsonify({'user_id': user_id, 'history': history, 'count': len(history)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/notify/test', methods=['POST'])
def notify_test():
    """
    Send a test Telegram message to verify bot connection.

    Body: { "chat_id": "123456789" }
    Returns: { "sent": true/false }
    """
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    chat_id = str(data.get('chat_id', '')).strip()
    if not chat_id:
        return jsonify({'error': 'chat_id is required'}), 400
    try:
        notifier = TelegramNotifier()
        sent = notifier.send_test(chat_id)
        return jsonify({'sent': sent})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Pattern Tip Pipeline endpoints ────────────────────────────────────────────

@app.route('/patterns/live', methods=['GET'])
def patterns_live():
    """
    GET /patterns/live?ticker=NVDA&pattern_type=fvg&direction=bullish
                      &timeframe=1h&min_quality=0.5&limit=20

    Returns open/partially-filled pattern signals, sorted by quality desc.
    Requires HAS_PATTERN_LAYER.
    """
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503
    ticker       = request.args.get('ticker')
    pattern_type = request.args.get('pattern_type')
    direction    = request.args.get('direction')
    timeframe    = request.args.get('timeframe')
    try:
        min_quality = float(request.args.get('min_quality', 0.0))
        limit       = int(request.args.get('limit', 50))
    except (ValueError, TypeError):
        return jsonify({'error': 'min_quality and limit must be numeric'}), 400
    try:
        patterns = get_open_patterns(
            _DB_PATH,
            ticker       = ticker or None,
            pattern_type = pattern_type or None,
            direction    = direction or None,
            timeframe    = timeframe or None,
            min_quality  = min_quality,
            limit        = limit,
        )
        return jsonify({'patterns': patterns, 'count': len(patterns)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/tip/preview', methods=['GET'])
@require_auth
def tip_preview(user_id: str):
    """
    GET /users/<user_id>/tip/preview

    Returns the tip that would be sent right now for this user
    (highest eligible pattern + position sizing), without actually sending.
    Respects tier gating.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503
    try:
        import sqlite3 as _sqlite3
        conn = _sqlite3.connect(_DB_PATH, timeout=10)
        row = conn.execute(
            """SELECT tier, tip_timeframes, tip_pattern_types, tip_markets,
                      account_size, max_risk_per_trade_pct, account_currency
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if row is None:
        return jsonify({'error': 'user not found'}), 404

    import json as _json
    cols     = ['tier', 'tip_timeframes', 'tip_pattern_types', 'tip_markets',
                'account_size', 'max_risk_per_trade_pct', 'account_currency']
    prefs    = dict(zip(cols, row))
    tier     = prefs.get('tier') or 'basic'

    for jcol in ('tip_timeframes', 'tip_pattern_types', 'tip_markets'):
        try:
            prefs[jcol] = _json.loads(prefs[jcol]) if prefs[jcol] else None
        except Exception:
            prefs[jcol] = None

    from core.tiers import TIER_CONFIG as TIER_LIMITS
    limits          = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    tip_timeframes  = prefs.get('tip_timeframes') or limits['timeframes']
    tip_pattern_tys = prefs.get('tip_pattern_types')
    tip_markets     = prefs.get('tip_markets')  # None = all tickers
    delivery_days   = limits.get('delivery_days', 'daily')
    is_weekly       = delivery_days != 'daily'

    from analytics.pattern_detector import PatternSignal

    if is_weekly:
        from notifications.tip_scheduler import _pick_batch
        batch_size = limits.get('batch_size', 3)
        batch, tip_source = _pick_batch(
            _DB_PATH, user_id, tier, tip_timeframes, tip_pattern_tys, tip_markets, batch_size
        )
        if not batch:
            return jsonify({'tip': None, 'tips': [], 'reason': 'no eligible patterns',
                            'cadence': 'weekly', 'tip_source': None}), 200

        tips = []
        for row in batch:
            sig = PatternSignal(
                pattern_type  = row['pattern_type'],
                ticker        = row['ticker'],
                direction     = row['direction'],
                zone_high     = row['zone_high'],
                zone_low      = row['zone_low'],
                zone_size_pct = row['zone_size_pct'],
                timeframe     = row['timeframe'],
                formed_at     = row['formed_at'],
                quality_score = row['quality_score'] or 0.0,
                status        = row['status'],
                kb_conviction = row.get('kb_conviction', ''),
                kb_regime     = row.get('kb_regime', ''),
                kb_signal_dir = row.get('kb_signal_dir', ''),
            )
            pos = calculate_position(sig, prefs)
            tips.append(tip_to_dict(sig, pos, tier=tier))
        return jsonify({
            'tip':        tips[0] if tips else None,
            'tips':       tips,
            'tip_source': tip_source,
            'cadence':    'weekly',
            'delivery_days': delivery_days,
        })

    # Premium — single daily tip
    from notifications.tip_scheduler import _pick_best_pattern
    pattern_row = _pick_best_pattern(_DB_PATH, user_id, tier, tip_timeframes, tip_pattern_tys, tip_markets)
    if pattern_row is None:
        return jsonify({'tip': None, 'tips': [], 'reason': 'no eligible patterns',
                        'cadence': 'daily', 'tip_source': None}), 200

    sig = PatternSignal(
        pattern_type  = pattern_row['pattern_type'],
        ticker        = pattern_row['ticker'],
        direction     = pattern_row['direction'],
        zone_high     = pattern_row['zone_high'],
        zone_low      = pattern_row['zone_low'],
        zone_size_pct = pattern_row['zone_size_pct'],
        timeframe     = pattern_row['timeframe'],
        formed_at     = pattern_row['formed_at'],
        quality_score = pattern_row['quality_score'] or 0.0,
        status        = pattern_row['status'],
        kb_conviction = pattern_row.get('kb_conviction', ''),
        kb_regime     = pattern_row.get('kb_regime', ''),
        kb_signal_dir = pattern_row.get('kb_signal_dir', ''),
    )
    position = calculate_position(sig, prefs)
    tip_dict = tip_to_dict(sig, position, tier=tier)
    tip_source = pattern_row.get('tip_source')
    return jsonify({'tip': tip_dict, 'tips': [tip_dict], 'tip_source': tip_source, 'cadence': 'daily'})


def _build_prefs_confirmation(new: dict, old: dict) -> str:
    """
    Build a MarkdownV2 Telegram confirmation message describing what changed
    in the user's tip preferences.  Returns empty string if nothing notable changed.
    """
    from notifications.tip_formatter import _escape_mdv2, _PATTERN_LABELS, _TF_LABELS

    lines = []

    # ── Markets ───────────────────────────────────────────────────────────────
    new_markets = new.get('tip_markets')   # list or None
    old_markets = old.get('tip_markets')   # list or None
    markets_changed = new_markets != old_markets and 'tip_markets' in new

    if markets_changed:
        if not new_markets:
            lines.append('🌐 You\'ll now receive tips from *all available markets*\\.')
        else:
            tickers_str = ', '.join(_escape_mdv2(t) for t in new_markets[:10])
            suffix = f' \\+{len(new_markets) - 10} more' if len(new_markets) > 10 else ''
            if old_markets:
                added   = [t for t in new_markets if t not in old_markets]
                removed = [t for t in old_markets if t not in new_markets]
                if added and removed:
                    a_str = ', '.join(_escape_mdv2(t) for t in added[:5])
                    r_str = ', '.join(_escape_mdv2(t) for t in removed[:5])
                    lines.append(
                        f'📊 You\'ll now see *more {a_str}* tips and *fewer {r_str}* tips\\.'
                    )
                elif added:
                    a_str = ', '.join(_escape_mdv2(t) for t in added[:5])
                    lines.append(f'📊 Added to your watchlist: *{a_str}*\\.')
                elif removed:
                    r_str = ', '.join(_escape_mdv2(t) for t in removed[:5])
                    lines.append(f'📊 Removed from your watchlist: *{r_str}*\\.')
            else:
                lines.append(
                    f'🎯 Your tips will now focus on: *{tickers_str}{suffix}*\\.'
                )

    # ── Pattern types ─────────────────────────────────────────────────────────
    new_patterns = new.get('tip_pattern_types')
    old_patterns = old.get('tip_pattern_types')
    if new_patterns != old_patterns and 'tip_pattern_types' in new:
        if not new_patterns:
            lines.append('📐 Pattern filter cleared — you\'ll see *all pattern types*\\.')
        else:
            added_p   = [p for p in new_patterns if not old_patterns or p not in old_patterns]
            removed_p = [p for p in (old_patterns or []) if p not in new_patterns]
            def _plabel(p):
                return _escape_mdv2(_PATTERN_LABELS.get(p, p.replace('_', ' ').title()))
            if added_p and removed_p:
                lines.append(
                    f'📐 You\'ll now see more *{", ".join(_plabel(p) for p in added_p)}* '
                    f'and fewer *{", ".join(_plabel(p) for p in removed_p)}* patterns\\.'
                )
            elif added_p:
                lines.append(
                    f'📐 Added pattern types: *{", ".join(_plabel(p) for p in added_p)}*\\.'
                )
            elif removed_p:
                lines.append(
                    f'📐 Removed pattern types: *{", ".join(_plabel(p) for p in removed_p)}*\\.'
                )

    # ── Timeframes ────────────────────────────────────────────────────────────
    new_tfs = new.get('tip_timeframes')
    old_tfs = old.get('tip_timeframes')
    if new_tfs != old_tfs and 'tip_timeframes' in new:
        if not new_tfs:
            lines.append('⏱ Timeframe filter cleared — you\'ll see *all timeframes*\\.')
        else:
            def _tflabel(tf):
                return _escape_mdv2(_TF_LABELS.get(tf, tf.upper()))
            added_tf   = [tf for tf in new_tfs if not old_tfs or tf not in old_tfs]
            removed_tf = [tf for tf in (old_tfs or []) if tf not in new_tfs]
            if added_tf and removed_tf:
                lines.append(
                    f'⏱ More *{", ".join(_tflabel(t) for t in added_tf)}* tips, '
                    f'fewer *{", ".join(_tflabel(t) for t in removed_tf)}* tips\\.'
                )
            elif added_tf:
                lines.append(f'⏱ Added timeframes: *{", ".join(_tflabel(t) for t in added_tf)}*\\.')
            elif removed_tf:
                lines.append(f'⏱ Removed timeframes: *{", ".join(_tflabel(t) for t in removed_tf)}*\\.')

    # ── Delivery time / timezone ──────────────────────────────────────────────
    new_time = new.get('tip_delivery_time')
    new_tz   = new.get('tip_delivery_timezone')
    old_time = old.get('tip_delivery_time')
    old_tz   = old.get('tip_delivery_timezone')
    time_changed = (new_time and new_time != old_time) or (new_tz and new_tz != old_tz)
    if time_changed:
        t = _escape_mdv2(new_time or old_time or '?')
        tz = _escape_mdv2(new_tz or old_tz or 'UTC')
        lines.append(f'🕐 Tips will now arrive at *{t}* \\({tz}\\)\\.')

    # ── Tier ─────────────────────────────────────────────────────────────────
    new_tier = new.get('tier')
    old_tier = old.get('tier')
    if new_tier and new_tier != old_tier:
        _TIER_DISPLAY = {
            'basic':   'Basic \\(Mon weekly batch\\)',
            'pro':     'Pro \\(Mon \\+ Wed batch\\)',
            'premium': 'Premium \\(daily tips\\)',
        }
        tier_label = _TIER_DISPLAY.get(new_tier, _escape_mdv2(new_tier.title()))
        lines.append(f'⭐ Tier updated to *{tier_label}*\\.')

    if not lines:
        return ''

    header = '✅ *Tip preferences updated\\!*\n'
    footer = '\n_Changes take effect from your next scheduled tip\\._'
    return header + '\n'.join(lines) + footer


@app.route('/users/<user_id>/tip-config', methods=['GET', 'POST'])
@require_auth
def tip_config(user_id: str):
    """
    GET  /users/<user_id>/tip-config          — Return current tip configuration.
    POST /users/<user_id>/tip-config          — Update tip configuration.

    POST body (all optional):
      tip_delivery_time      "HH:MM"
      tip_delivery_timezone  IANA timezone string
      tip_markets            ["equities"]
      tip_timeframes         ["1h","4h"]
      tip_pattern_types      ["fvg","order_block"]
      account_size           12000.0
      max_risk_per_trade_pct 1.5
      account_currency       "GBP"
      tier                   "basic" | "pro"
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    if request.method == 'GET':
        try:
            import sqlite3 as _sqlite3, json as _json
            conn = _sqlite3.connect(_DB_PATH, timeout=10)
            row  = conn.execute(
                """SELECT user_id, tier, tip_delivery_time, tip_delivery_timezone,
                          tip_markets, tip_timeframes, tip_pattern_types,
                          account_size, max_risk_per_trade_pct, account_currency,
                          available_cash
                   FROM user_preferences WHERE user_id = ?""",
                (user_id,),
            ).fetchone()
            conn.close()
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        if row is None:
            return jsonify({'error': 'user not found'}), 404
        cols = ['user_id', 'tier', 'tip_delivery_time', 'tip_delivery_timezone',
                'tip_markets', 'tip_timeframes', 'tip_pattern_types',
                'account_size', 'max_risk_per_trade_pct', 'account_currency',
                'available_cash']
        d = dict(zip(cols, row))
        for jcol in ('tip_markets', 'tip_timeframes', 'tip_pattern_types'):
            try:
                d[jcol] = _json.loads(d[jcol]) if d[jcol] else None
            except Exception:
                d[jcol] = None
        # Fetch cash_currency separately (newer column)
        try:
            from users.user_store import get_available_cash as _gc
            _cd = _gc(_DB_PATH, user_id)
            d['cash_currency'] = _cd.get('cash_currency', 'GBP')
        except Exception:
            d['cash_currency'] = 'GBP'
        return jsonify(d)

    # POST
    data = request.get_json(force=True, silent=True) or {}
    if HAS_VALIDATORS:
        vr = validate_tip_config(data)
        if not vr.valid:
            return jsonify({'error': 'validation_failed', 'details': vr.errors}), 400
    try:
        # Snapshot old prefs before update (for diff message)
        import sqlite3 as _sq2, json as _json2
        _old_prefs: dict = {}
        try:
            _oc = _sq2.connect(_DB_PATH, timeout=5)
            _or = _oc.execute(
                """SELECT telegram_chat_id, tip_markets, tip_timeframes, tip_pattern_types,
                          tip_delivery_time, tip_delivery_timezone, tier
                   FROM user_preferences WHERE user_id=?""", (user_id,)
            ).fetchone()
            _oc.close()
            if _or:
                _cols = ['telegram_chat_id', 'tip_markets', 'tip_timeframes',
                         'tip_pattern_types', 'tip_delivery_time', 'tip_delivery_timezone', 'tier']
                _old_prefs = dict(zip(_cols, _or))
                for _jc in ('tip_markets', 'tip_timeframes', 'tip_pattern_types'):
                    try:
                        _old_prefs[_jc] = _json2.loads(_old_prefs[_jc]) if _old_prefs[_jc] else None
                    except Exception:
                        _old_prefs[_jc] = None
        except Exception:
            pass

        updated = update_tip_config(
            _DB_PATH,
            user_id,
            tip_delivery_time      = data.get('tip_delivery_time'),
            tip_delivery_timezone  = data.get('tip_delivery_timezone'),
            tip_markets            = data.get('tip_markets'),
            tip_timeframes         = data.get('tip_timeframes'),
            tip_pattern_types      = data.get('tip_pattern_types'),
            account_size           = data.get('account_size'),
            max_risk_per_trade_pct = data.get('max_risk_per_trade_pct'),
            account_currency       = data.get('account_currency'),
            tier                   = data.get('tier'),
        )

        # ── Send preference confirmation via Telegram (non-fatal) ────────────
        try:
            _chat_id = (_old_prefs.get('telegram_chat_id') or '').strip()
            if _chat_id:
                _msg = _build_prefs_confirmation(data, _old_prefs)
                if _msg:
                    from notifications.telegram_notifier import TelegramNotifier as _TGN
                    _TGN().send(_chat_id, _msg, parse_mode='MarkdownV2')
        except Exception:
            pass  # Non-fatal

        return jsonify(updated)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/cash', methods=['GET', 'POST'])
@require_auth
def user_cash(user_id: str):
    """
    GET  /users/<user_id>/cash  — Return current available_cash + currency.
    POST /users/<user_id>/cash  — Set available_cash. Body: { "available_cash": 5000.0 }
    """
    err = assert_self(user_id)
    if err: return err

    if request.method == 'GET':
        try:
            from users.user_store import get_available_cash
            result = get_available_cash(_DB_PATH, user_id)
            return jsonify(result)
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # POST
    data = request.get_json(force=True, silent=True) or {}
    if 'available_cash' not in data:
        return jsonify({'error': 'available_cash is required'}), 400
    raw = data.get('available_cash')
    cash_currency = str(data.get('cash_currency', 'GBP')).upper().strip() or 'GBP'
    # Allow null to clear the balance
    if raw is None:
        try:
            import sqlite3 as _sq3c
            _cc = _sq3c.connect(_DB_PATH, timeout=5)
            _cc.execute("UPDATE user_preferences SET available_cash = NULL WHERE user_id = ?", (user_id,))
            _cc.commit()
            _cc.close()
            return jsonify({'user_id': user_id, 'available_cash': None, 'cash_currency': cash_currency})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    try:
        amount = float(raw)
    except (TypeError, ValueError):
        return jsonify({'error': 'available_cash must be a number'}), 400
    try:
        from users.user_store import update_available_cash
        result = update_available_cash(_DB_PATH, user_id, amount, cash_currency=cash_currency)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/positions/open', methods=['GET'])
@require_auth
def user_positions_open(user_id: str):
    """GET /users/<user_id>/positions/open — open (watching + active) followups."""
    err = assert_self(user_id)
    if err: return err
    try:
        from users.user_store import get_user_open_positions
        positions = get_user_open_positions(_DB_PATH, user_id)
        return jsonify({'user_id': user_id, 'positions': positions, 'count': len(positions)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/positions/closed', methods=['GET'])
@require_auth
def user_positions_closed(user_id: str):
    """GET /users/<user_id>/positions/closed?since=YYYY-MM-DD — recently closed followups."""
    err = assert_self(user_id)
    if err: return err
    try:
        from users.user_store import get_recently_closed_positions
        since = request.args.get('since', '')
        if not since:
            from datetime import datetime, timedelta, timezone as _tz
            since = (datetime.now(_tz.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        positions = get_recently_closed_positions(_DB_PATH, user_id, since)
        return jsonify({'user_id': user_id, 'positions': positions, 'count': len(positions), 'since': since})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/tip/history', methods=['GET'])
@require_auth
def tip_history(user_id: str):
    """
    GET /users/<user_id>/tip/history?limit=30

    Returns recent tip_delivery_log rows for this user, newest first.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503
    try:
        limit = int(request.args.get('limit', 30))
    except (ValueError, TypeError):
        limit = 30
    try:
        history = get_tip_history(_DB_PATH, user_id, limit=limit)
        return jsonify({'history': history, 'count': len(history)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Security headers ─────────────────────────────────────────────────────────

@app.after_request
def security_headers(response):
    from flask import request as _req
    # /markets/chart sets its own permissive CSP and X-Frame-Options — don't overwrite
    if _req.path == '/markets/chart':
        return response
    response.headers['X-Content-Type-Options']       = 'nosniff'
    response.headers['X-Frame-Options']              = 'DENY'
    response.headers['X-XSS-Protection']             = '1; mode=block'
    response.headers['Referrer-Policy']              = 'strict-origin-when-cross-origin'
    response.headers['Strict-Transport-Security']    = 'max-age=31536000'
    # SPA frontend needs inline styles/scripts and Google Fonts CDN
    if response.content_type and 'text/html' in response.content_type:
        response.headers['Content-Security-Policy'] = (
            "default-src 'self'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "script-src 'self' 'unsafe-inline'; "
            "connect-src 'self'; "
            "img-src 'self' data: blob:; "
            "frame-src 'self'; "
            "frame-ancestors 'none'"
        )
    else:
        response.headers['Content-Security-Policy'] = "default-src 'none'"
    return response


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.route('/auth/register', methods=['POST'])
@rate_limit('auth')
def auth_register():
    """
    POST /auth/register

    Body: { "user_id": "alice", "email": "alice@example.com", "password": "..." }
    Returns: { "user_id": "...", "email": "...", "created_at": "..." }

    Also creates the user_preferences row so all product-layer endpoints
    are immediately usable after registration.
    """
    if not HAS_AUTH:
        return jsonify({'error': 'auth not available — install PyJWT and bcrypt'}), 503

    data = request.get_json(force=True, silent=True) or {}

    # Beta access gate — checked before any other validation
    _beta_secret = os.environ.get('BETA_PASSWORD', '')
    _beta_given  = str(data.get('beta_password', ''))
    if not _beta_secret or _beta_given != _beta_secret:
        log_audit_event(_DB_PATH, action='register',
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='failure', detail={'reason': 'invalid beta password'})
        return jsonify({'error': 'Invalid beta access password.'}), 403

    if HAS_VALIDATORS:
        result = validate_register(data)
        if not result.valid:
            return jsonify({'error': 'validation_failed', 'details': result.errors}), 400

    user_id = str(data.get('user_id', '')).strip()
    email   = str(data.get('email', '')).strip()
    password = str(data.get('password', ''))

    try:
        row = register_user(_DB_PATH, user_id, email, password)
    except ValueError as e:
        log_audit_event(_DB_PATH, action='register',
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='failure', detail={'reason': str(e)})
        # 409 for duplicate email, 400 for validation errors (weak password etc.)
        status_code = 409 if 'already registered' in str(e) else 400
        return jsonify({'error': str(e)}), status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Create user_preferences row if product layer available
    if HAS_PRODUCT_LAYER:
        try:
            create_user(_DB_PATH, user_id)
        except Exception:
            pass

    log_audit_event(_DB_PATH, action='register', user_id=user_id,
                    ip_address=request.remote_addr,
                    user_agent=request.user_agent.string,
                    outcome='success')
    return jsonify(row), 201


# ── Cookie helpers ────────────────────────────────────────────────────────────
_IS_PROD = os.environ.get('FLASK_ENV', 'production') != 'development'

def _set_auth_cookies(resp, access_token: str, refresh_token: str) -> None:
    """Set HttpOnly, Secure, SameSite=None auth cookies on a response.
    SameSite=None is required because the frontend (trading-galaxy.uk) and
    API (api.trading-galaxy.uk) are cross-origin; Strict would block the cookie.
    Secure=True is mandatory with SameSite=None.
    """
    resp.set_cookie(
        'tg_access',
        value=access_token,
        httponly=True,
        secure=True,            # mandatory with SameSite=None
        samesite='None',
        path='/',
        max_age=86400,          # 24 h — matches JWT_EXPIRY_HOURS default
    )
    if refresh_token:
        resp.set_cookie(
            'tg_refresh',
            value=refresh_token,
            httponly=True,
            secure=True,
            samesite='None',
            path='/auth/refresh',   # scoped — only sent to refresh endpoint
            max_age=2592000,        # 30 d — matches JWT_REFRESH_EXPIRY_DAYS default
        )

def _clear_auth_cookies(resp) -> None:
    """Expire both auth cookies immediately."""
    resp.set_cookie('tg_access',  value='', httponly=True, secure=True,
                    samesite='None', path='/', max_age=0)
    resp.set_cookie('tg_refresh', value='', httponly=True, secure=True,
                    samesite='None', path='/auth/refresh', max_age=0)


@app.route('/auth/token', methods=['POST'])
@rate_limit('auth')
def auth_token():
    """
    POST /auth/token

    Body: { "email": "alice@example.com", "password": "..." }
    Returns: { "access_token": "eyJ...", "token_type": "Bearer", "expires_in": 86400 }
    """
    if not HAS_AUTH:
        return jsonify({'error': 'auth not available — install PyJWT and bcrypt'}), 503

    data = request.get_json(force=True, silent=True) or {}
    email    = str(data.get('email', '')).strip()
    password = str(data.get('password', ''))

    if not email or not password:
        return jsonify({'error': 'email and password are required'}), 400

    try:
        token_data = authenticate_user(_DB_PATH, email, password)
    except ValueError as e:
        log_audit_event(_DB_PATH, action='login_failure',
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='failure', detail={'email': email, 'reason': str(e)})
        return jsonify({'error': str(e)}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    log_audit_event(_DB_PATH, action='login_success', user_id=token_data['user_id'],
                    ip_address=request.remote_addr,
                    user_agent=request.user_agent.string,
                    outcome='success')
    try:
        refresh_data = issue_refresh_token(_DB_PATH, token_data['user_id'])
        token_data['refresh_token']         = refresh_data['refresh_token']
        token_data['refresh_token_expires'] = refresh_data['expires_at']
    except Exception:
        pass
    resp = jsonify(token_data)
    _set_auth_cookies(resp, token_data['access_token'],
                      token_data.get('refresh_token', ''))
    return resp


@app.route('/auth/refresh', methods=['POST'])
@rate_limit('auth')
def auth_refresh():
    """
    POST /auth/refresh

    Exchange a valid refresh token for a new access token + refresh token pair.
    The old refresh token is revoked immediately (rotation — single-use).

    Body: { "refresh_token": "<opaque token string>" }
    Returns: { access_token, refresh_token, token_type, expires_in, user_id }

    Error responses:
      400  — missing refresh_token field
      401  — token invalid, expired, or already revoked
    """
    if not HAS_AUTH:
        return jsonify({'error': 'auth not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    refresh_token = data.get('refresh_token', '').strip()
    # Fall back to HttpOnly cookie if not in body
    if not refresh_token:
        refresh_token = request.cookies.get('tg_refresh', '').strip()
    if not refresh_token:
        return jsonify({'error': 'refresh_token is required'}), 400
    try:
        result = rotate_refresh_token(_DB_PATH, refresh_token)
        log_audit_event(_DB_PATH, action='token_refresh', user_id=result['user_id'],
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
        resp = jsonify(result)
        _set_auth_cookies(resp, result['access_token'], result['refresh_token'])
        return resp
    except ValueError as e:
        return jsonify({'error': 'token_expired' if 'expired' in str(e) else 'invalid_token',
                        'detail': str(e)}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/auth/logout', methods=['POST'])
@require_auth
def auth_logout():
    """
    POST /auth/logout

    Revoke the refresh token supplied in the body.  The access token cannot
    be server-side revoked (it expires naturally per JWT_EXPIRY_HOURS).
    Frontend must discard the access token from storage on receipt of 200.

    Body: { "refresh_token": "<opaque token string>" }  (optional)
    Returns: { "logged_out": true }
    """
    if not HAS_AUTH:
        return jsonify({'error': 'auth not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    refresh_token = data.get('refresh_token', '').strip()
    if refresh_token:
        try:
            import sqlite3 as _sqlite3
            conn = _sqlite3.connect(_DB_PATH, timeout=10)
            conn.execute(
                "UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ? AND user_id = ?",
                (refresh_token, g.user_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
    log_audit_event(_DB_PATH, action='logout', user_id=g.user_id,
                    ip_address=request.remote_addr,
                    user_agent=request.user_agent.string,
                    outcome='success')
    resp = jsonify({'logged_out': True})
    _clear_auth_cookies(resp)
    return resp


_TG_LOGIN_CODES: dict = {}  # code -> {chat_id, user_data, expires}

@app.route('/auth/telegram/code', methods=['POST'])
@limiter.exempt
def auth_telegram_code():
    """
    POST /auth/telegram/code
    Generate a one-time login code. Frontend opens t.me/bot?start=<code>.
    Returns: { "code": "ABC12345" }
    """
    import secrets, time as _time
    code = secrets.token_hex(4).upper()  # e.g. "A3F9C12E"
    _TG_LOGIN_CODES[code] = {'chat_id': None, 'user_data': None, 'expires': _time.time() + 300}
    # Prune expired codes
    expired = [k for k, v in _TG_LOGIN_CODES.items() if v['expires'] < _time.time()]
    for k in expired:
        del _TG_LOGIN_CODES[k]
    return jsonify({'code': code})


@app.route('/auth/telegram/verify', methods=['POST'])
@limiter.exempt
def auth_telegram_verify():
    """
    POST /auth/telegram/verify
    Body: { "code": "ABC12345" }
    Returns: { "access_token": "...", "user_id": "tg_123", "first_name": "..." }
    """
    import time as _time, base64 as _b64, json as _json
    data = request.get_json(force=True, silent=True) or {}
    code = (data.get('code') or '').strip().upper()
    entry = _TG_LOGIN_CODES.get(code)
    if not entry:
        return jsonify({'error': 'Invalid code'}), 400
    if _time.time() > entry['expires']:
        del _TG_LOGIN_CODES[code]
        return jsonify({'error': 'Code expired'}), 400
    if not entry.get('chat_id'):
        return jsonify({'error': 'Code not yet confirmed — send it to the bot first'}), 202
    # Consume the code
    tg_data = entry.get('user_data') or {}
    chat_id = entry['chat_id']
    del _TG_LOGIN_CODES[code]
    user_id = f"tg_{chat_id}"
    # Upsert user
    try:
        import sqlite3 as _sq
        conn = _sq.connect(_DB_PATH, timeout=10)
        try:
            from users.user_store import ensure_user_tables
            ensure_user_tables(conn)
            conn.execute(
                "INSERT OR IGNORE INTO user_preferences (user_id, onboarding_complete, tier) VALUES (?, 0, 'free')",
                (user_id,),
            )
            conn.execute(
                "UPDATE user_preferences SET telegram_chat_id=? WHERE user_id=?",
                (str(chat_id), user_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass
    # Issue token
    if HAS_AUTH:
        try:
            from middleware.auth import _make_token
            access_token = _make_token(user_id, f"{user_id}@telegram.local")
        except Exception:
            access_token = _b64.urlsafe_b64encode(_json.dumps(
                {'user_id': user_id, 'sub': user_id, 'exp': int(_time.time()) + 86400 * 30}
            ).encode()).decode()
    else:
        access_token = _b64.urlsafe_b64encode(_json.dumps(
            {'user_id': user_id, 'sub': user_id, 'exp': int(_time.time()) + 86400 * 30}
        ).encode()).decode()
    resp = jsonify({
        'access_token': access_token,
        'user_id':      user_id,
        'first_name':   tg_data.get('first_name', ''),
        'username':     tg_data.get('username', ''),
        'tg_data':      tg_data,
    })
    if HAS_AUTH and access_token and access_token.startswith('eyJ'):
        try:
            _set_auth_cookies(resp, access_token, '')
        except Exception:
            pass
    return resp


@app.route('/telegram/bot', methods=['POST'])
@limiter.exempt
def telegram_bot_webhook():
    """
    POST /telegram/bot  — Telegram bot webhook
    Handles:
      - /start <code>  — confirms login codes
      - /help          — usage info
      - General text   — routes through KB /chat pipeline, replies with KB-grounded answer
    """
    import time as _time, json as _json, sqlite3 as _sq3
    update = request.get_json(force=True, silent=True) or {}
    msg = update.get('message', {})
    text = (msg.get('text') or '').strip()
    chat = msg.get('chat', {})
    chat_id = chat.get('id')
    from_user = msg.get('from', {})

    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')

    def _bot_send(cid, text_msg):
        if not bot_token:
            return
        try:
            import urllib.request as _ur
            payload = _json.dumps({'chat_id': cid, 'text': text_msg}).encode()
            req = _ur.Request(
                f'https://api.telegram.org/bot{bot_token}/sendMessage',
                data=payload, headers={'Content-Type': 'application/json'}
            )
            _ur.urlopen(req, timeout=10)
        except Exception:
            pass

    if not text:
        return jsonify({'ok': True})

    # ── /start <code> — login code flow ───────────────────────────────────────
    if text.startswith('/start'):
        parts = text.split(maxsplit=1)
        code = parts[1].strip().upper() if len(parts) > 1 else ''
        if code and code in _TG_LOGIN_CODES:
            entry = _TG_LOGIN_CODES[code]
            if _time.time() < entry['expires']:
                entry['chat_id'] = chat_id
                entry['user_data'] = {
                    'id':         chat_id,
                    'first_name': from_user.get('first_name', ''),
                    'last_name':  from_user.get('last_name', ''),
                    'username':   from_user.get('username', ''),
                }
                _bot_send(chat_id, "✅ Logged in! Return to the Trading Galaxy dashboard.")
            else:
                _bot_send(chat_id, "⚠️ That login code has expired. Please request a new one.")
        else:
            _bot_send(chat_id, "👋 Welcome to Trading Galaxy!\n\nSend me any question about markets, tickers, or your portfolio and I'll answer from the live knowledge base.\n\nTo link your account, use the Sign In button on the dashboard.")
        return jsonify({'ok': True})

    # ── /help ─────────────────────────────────────────────────────────────────
    if text.startswith('/help'):
        _bot_send(chat_id,
            "Trading Galaxy Bot\n\n"
            "Ask me anything about markets, tickers, signals, or your portfolio.\n\n"
            "Examples:\n"
            "• Tell me about NVDA\n"
            "• What's the current market regime?\n"
            "• What's the signal on gold?\n"
            "• What market are we in?\n\n"
            "Your chat is linked to your Trading Galaxy account if you've signed in via the dashboard."
        )
        return jsonify({'ok': True})

    # ── Ignore other bot commands ─────────────────────────────────────────────
    if text.startswith('/'):
        return jsonify({'ok': True})

    # ── General chat — route through KB pipeline ──────────────────────────────
    if not HAS_LLM or not is_available():
        _bot_send(chat_id, "⚠️ The knowledge engine is temporarily unavailable. Please try again shortly.")
        return jsonify({'ok': True})

    # Look up the user_id linked to this telegram_chat_id so KB is personalised
    tg_user_id = None
    try:
        _tc = _sq3.connect(_DB_PATH, timeout=5)
        _tr = _tc.execute(
            "SELECT user_id FROM user_preferences WHERE telegram_chat_id=? LIMIT 1",
            (str(chat_id),)
        ).fetchone()
        _tc.close()
        if _tr:
            tg_user_id = _tr[0]
    except Exception:
        pass

    try:
        # Reuse the KB retrieval + LLM pipeline directly
        conn = _kg.thread_local_conn()
        snippet, atoms = retrieve(text, conn, limit=25)

        # Trader level for prompt formatting
        _tg_trader_level = 'developing'
        if tg_user_id and HAS_PRODUCT_LAYER:
            try:
                _tl = get_user(_DB_PATH, tg_user_id)
                if _tl:
                    _tg_trader_level = _tl.get('trader_level') or 'developing'
            except Exception:
                pass

        stress_dict = None
        if HAS_STRESS and atoms:
            try:
                import re as _re
                _words = _re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', text)
                _key_terms = list({w.lower() for w in _words if len(w) > 2})[:10]
                _sr = compute_stress(atoms, _key_terms, conn)
                stress_dict = {
                    'composite_stress':     _sr.composite_stress,
                    'decay_pressure':       _sr.decay_pressure,
                    'authority_conflict':   _sr.authority_conflict,
                    'supersession_density': _sr.supersession_density,
                    'conflict_cluster':     _sr.conflict_cluster,
                    'domain_entropy':       _sr.domain_entropy,
                }
            except Exception:
                pass

        messages = build_prompt(
            user_message=text,
            snippet=snippet,
            stress=stress_dict,
            atom_count=len(atoms),
            trader_level=_tg_trader_level,
        )
        answer = _llm_chat(messages, model=DEFAULT_MODEL if HAS_LLM else 'llama3.2')

        if answer:
            # Telegram plain-text: strip markdown bold/italic for readability
            import re as _re2
            plain = _re2.sub(r'\*\*(.+?)\*\*', r'\1', answer)
            plain = _re2.sub(r'\*(.+?)\*', r'\1', plain)
            plain = plain[:4000]  # Telegram message limit
            _bot_send(chat_id, plain)
        else:
            _bot_send(chat_id, "⚠️ Couldn't generate a response right now. Please try again.")
    except Exception as _exc:
        _log.error('telegram_bot_webhook chat error: %s', _exc)
        _bot_send(chat_id, "⚠️ Something went wrong. Please try again in a moment.")

    return jsonify({'ok': True})


@app.route('/auth/telegram', methods=['POST'])
@limiter.exempt
def auth_telegram():
    """
    POST /auth/telegram

    Exchange Telegram Login Widget auth data for an app access token.
    The Telegram data hash is verified against TELEGRAM_BOT_TOKEN.

    Body (from Telegram widget):
      { "id": 123456, "first_name": "Alice", "username": "alice",
        "photo_url": "...", "auth_date": 1700000000, "hash": "abc..." }

    Returns: { "access_token": "...", "user_id": "tg_123456" }
    """
    import hashlib
    import hmac
    import time

    data = request.get_json(force=True, silent=True) or {}
    tg_id = data.get('id')
    if not tg_id:
        return jsonify({'error': 'Telegram auth data missing id'}), 400

    # Verify hash against bot token (if token is configured)
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if bot_token:
        try:
            check_hash = data.get('hash', '')
            data_check = {k: v for k, v in data.items() if k != 'hash'}
            data_check_str = '\n'.join(f'{k}={v}' for k, v in sorted(data_check.items()))
            secret = hashlib.sha256(bot_token.encode()).digest()
            computed = hmac.new(secret, data_check_str.encode(), hashlib.sha256).hexdigest()  # noqa: E501
            if not hmac.compare_digest(computed, check_hash):
                return jsonify({'error': 'Telegram auth hash invalid'}), 401
            auth_date = int(data.get('auth_date', 0))
            if time.time() - auth_date > 86400:
                return jsonify({'error': 'Telegram auth data expired'}), 401
        except Exception as e:
            return jsonify({'error': f'Hash verification error: {e}'}), 400

    # Build a stable user_id from Telegram ID
    user_id = f"tg_{tg_id}"

    if not HAS_AUTH:
        # Auth layer absent — issue a minimal token so the UI can proceed
        import base64 as _b64, json as _json
        minimal = _b64.urlsafe_b64encode(_json.dumps({
            'user_id': user_id, 'sub': user_id, 'exp': int(time.time()) + 86400 * 30,
        }).encode()).decode()
        return jsonify({'access_token': minimal, 'user_id': user_id, 'token_type': 'Bearer'})

    try:
        from middleware.auth import _make_token
        import sqlite3 as _sq

        # Upsert user in user_preferences (onboarding = 0 if new)
        conn = _sq.connect(_DB_PATH, timeout=10)
        try:
            from users.user_store import ensure_user_tables
            ensure_user_tables(conn)
            conn.execute(
                """INSERT OR IGNORE INTO user_preferences
                   (user_id, onboarding_complete, tier) VALUES (?, 0, 'free')""",
                (user_id,),
            )
            # Store telegram_chat_id so position monitor alerts can reach them
            conn.execute(
                "UPDATE user_preferences SET telegram_chat_id=? WHERE user_id=?",
                (str(tg_id), user_id),
            )
            # Upsert into user_auth (email = tg_<id>@telegram.local, no password)
            try:
                conn.execute("CREATE TABLE IF NOT EXISTS user_auth (user_id TEXT PRIMARY KEY, email TEXT UNIQUE, password_hash TEXT, created_at TEXT)")
            except Exception:
                pass
            conn.execute(
                """INSERT OR IGNORE INTO user_auth
                   (user_id, email, password_hash, created_at)
                   VALUES (?, ?, '', datetime('now'))""",
                (user_id, f"{user_id}@telegram.local"),
            )
            conn.commit()
        finally:
            conn.close()

        access_token = _make_token(user_id, f"{user_id}@telegram.local")
        resp = jsonify({
            'access_token': access_token,
            'user_id':      user_id,
            'token_type':   'Bearer',
            'first_name':   data.get('first_name', ''),
            'username':     data.get('username', ''),
        })
        _set_auth_cookies(resp, access_token, '')
        return resp
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/auth/me', methods=['GET'])
@require_auth
def auth_me():
    """
    GET /auth/me

    Returns the authenticated user's profile from user_preferences.
    Requires Authorization: Bearer {token}.
    """
    from flask import g as _g
    user_id = _g.user_id
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT email, first_name, last_name, phone FROM user_auth WHERE user_id=?",
            (user_id,)
        ).fetchone()
        conn.close()
    except Exception:
        row = None
    base = {'user_id': user_id, 'email': g.user_email}
    if row:
        base['email']      = row[0] or g.user_email
        base['first_name'] = row[1] or ''
        base['last_name']  = row[2] or ''
        base['phone']      = row[3] or ''
    if not HAS_PRODUCT_LAYER:
        return jsonify(base)
    try:
        user = get_user(_DB_PATH, user_id)
        if user:
            user.update(base)
            return jsonify(user)
        return jsonify(base)
    except Exception as e:
        return jsonify(base)


@app.route('/admin/users/<target_user_id>/set-dev', methods=['POST'])
@require_auth
def admin_set_dev(target_user_id):
    """
    POST /admin/users/<target_user_id>/set-dev

    Toggle the is_dev flag on any user account.
    Restricted to user IDs listed in the ADMIN_USER_IDS env var
    (comma-separated). JWT auth is also required.

    Body: { "is_dev": true | false }
    Returns: { "ok": true, "user_id": "...", "is_dev": true|false }
    """
    import os as _os
    _admin_ids = {
        uid.strip()
        for uid in _os.environ.get('ADMIN_USER_IDS', '').split(',')
        if uid.strip()
    }
    if not _admin_ids or g.user_id not in _admin_ids:
        return jsonify({'error': 'forbidden'}), 403

    data    = request.get_json(force=True, silent=True) or {}
    is_dev  = bool(data.get('is_dev', False))

    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        from users.user_store import set_user_dev
        set_user_dev(_DB_PATH, target_user_id, is_dev)
        return jsonify({'ok': True, 'user_id': target_user_id, 'is_dev': is_dev})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Dev / test helpers ────────────────────────────────────────────────────
# TEMPORARY: lets any logged-in user self-upgrade to premium for testing.
# Remove this endpoint before going fully live.

@app.route('/dev/upgrade-premium', methods=['POST'])
def dev_upgrade_premium():
    """
    POST /dev/upgrade-premium
    Instantly sets the calling user's tier to 'premium' — no payment required.
    Accepts user_id from cookie JWT (if present) or from the request body.
    TEMPORARY testing endpoint. Remove before production launch.
    """
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    # Resolve user_id: prefer JWT cookie/header, fall back to body
    user_id = getattr(g, 'user_id', None)
    if not user_id:
        # Try to decode cookie manually
        if HAS_AUTH:
            try:
                from middleware.auth import _decode_token
                _tok = request.cookies.get('tg_access', '') or \
                       request.headers.get('Authorization', '').removeprefix('Bearer ').strip()
                if _tok:
                    user_id = _decode_token(_tok).get('user_id')
            except Exception:
                pass
    if not user_id:
        body = request.get_json(force=True, silent=True) or {}
        user_id = body.get('user_id')
    if not user_id:
        return jsonify({'error': 'user_id required'}), 400
    try:
        from users.user_store import set_user_tier as _set_tier
        _set_tier(_DB_PATH, user_id, 'premium')
        return jsonify({'ok': True, 'tier': 'premium', 'user_id': user_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Stripe ─────────────────────────────────────────────────────────────────

@app.route('/stripe/checkout', methods=['POST'])
@require_auth
def stripe_checkout():
    """
    POST /stripe/checkout
    Body: { "tier": "basic"|"pro"|"premium", "annual": false }
    Returns: { "url": "https://checkout.stripe.com/..." }
    """
    from flask import g as _g, request as _req
    from middleware.stripe_billing import create_checkout_session
    from users.user_store import get_user as _get_user
    import os as _os

    data     = _req.get_json(silent=True) or {}
    tier     = data.get('tier', '').lower()
    annual   = bool(data.get('annual', False))

    if tier not in ('basic', 'pro', 'premium'):
        return jsonify({'error': 'invalid tier'}), 400

    # Fetch user email for Stripe pre-fill
    try:
        user_row = _get_user(_DB_PATH, _g.user_id)
        email    = user_row.get('email') if user_row else None
    except Exception:
        email = None

    base_url    = _req.host_url.rstrip('/')
    success_url = f'{base_url}/subscription?success=1'
    cancel_url  = f'{base_url}/subscription?cancelled=1'

    try:
        url = create_checkout_session(
            user_id=_g.user_id,
            user_email=email or '',
            tier=tier,
            annual=annual,
            success_url=success_url,
            cancel_url=cancel_url,
        )
        return jsonify({'url': url})
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/stripe/portal', methods=['POST'])
@require_auth
def stripe_portal():
    """
    POST /stripe/portal
    Returns: { "url": "https://billing.stripe.com/..." }
    """
    from flask import g as _g, request as _req
    from middleware.stripe_billing import create_portal_session

    base_url   = _req.host_url.rstrip('/')
    return_url = f'{base_url}/subscription'

    try:
        url = create_portal_session(user_id=_g.user_id, return_url=return_url)
        return jsonify({'url': url})
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/stripe/webhook', methods=['POST'])
def stripe_webhook():
    """
    POST /stripe/webhook
    Stripe sends signed events here. Verifies signature, updates user tier.
    """
    from flask import request as _req
    from middleware.stripe_billing import handle_webhook

    payload    = _req.get_data()
    sig_header = _req.headers.get('Stripe-Signature', '')

    try:
        result = handle_webhook(payload, sig_header, _DB_PATH)
        return jsonify(result)
    except ValueError:
        return jsonify({'error': 'invalid signature'}), 400
    except RuntimeError as e:
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/profile', methods=['PATCH'])
@require_auth
def update_user_profile(user_id):
    """
    PATCH /users/<user_id>/profile

    Update first_name, last_name, phone for the authenticated user.
    Adds columns to user_auth if they don't exist yet (safe migration).
    Body: { "first_name": "...", "last_name": "...", "phone": "..." }
    Returns: { "ok": true, "first_name": ..., "last_name": ..., "phone": ... }
    """
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    data = request.get_json(force=True, silent=True) or {}
    first_name = str(data.get('first_name', '') or '').strip()[:100]
    last_name  = str(data.get('last_name',  '') or '').strip()[:100]
    phone      = str(data.get('phone',      '') or '').strip()[:30]
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        for col in ('first_name', 'last_name', 'phone'):
            try:
                conn.execute(f"ALTER TABLE user_auth ADD COLUMN {col} TEXT DEFAULT ''")
            except Exception:
                pass
        conn.execute(
            "UPDATE user_auth SET first_name=?, last_name=?, phone=? WHERE user_id=?",
            (first_name, last_name, phone, user_id),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True, 'first_name': first_name, 'last_name': last_name, 'phone': phone})


@app.route('/auth/change-password', methods=['POST'])
@require_auth
def change_password():
    """
    POST /auth/change-password

    Body: { "current_password": "...", "new_password": "..." }
    Returns: { "ok": true } or 400/401 on failure.
    """
    if not HAS_AUTH:
        return jsonify({'error': 'auth not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    current_pw = str(data.get('current_password', ''))
    new_pw     = str(data.get('new_password', ''))
    if not current_pw or not new_pw:
        return jsonify({'error': 'current_password and new_password are required'}), 400
    if len(new_pw) < 8:
        return jsonify({'error': 'new password must be at least 8 characters'}), 400
    try:
        import bcrypt as _bcrypt
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT password_hash FROM user_auth WHERE user_id=?", (g.user_id,)
        ).fetchone()
        if not row:
            conn.close()
            return jsonify({'error': 'user not found'}), 404
        if not _bcrypt.checkpw(current_pw.encode(), row[0].encode()):
            conn.close()
            return jsonify({'error': 'current password is incorrect'}), 401
        new_hash = _bcrypt.hashpw(new_pw.encode(), _bcrypt.gensalt(rounds=12)).decode()
        conn.execute(
            "UPDATE user_auth SET password_hash=? WHERE user_id=?", (new_hash, g.user_id)
        )
        conn.commit()
        conn.close()
        log_audit_event(_DB_PATH, action='password_change', user_id=g.user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True})


@app.route('/users/<user_id>/notification-prefs', methods=['PATCH'])
@require_auth
def update_notification_prefs(user_id):
    """
    PATCH /users/<user_id>/notification-prefs

    Save notification toggle states. Pro+ features are rejected if the user's
    tier is 'basic'.
    Body: { "monday_briefing": true, "wednesday_update": true,
            "zone_alerts": true, "thesis_alerts": true,
            "profit_lock_alerts": false, "trailing_alerts": false }
    Returns: { "ok": true, "prefs": {...} }
    """
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}

    # Determine tier
    try:
        user = get_user(_DB_PATH, user_id)
        tier = (user.get('tier') or 'basic').lower() if user else 'basic'
    except Exception:
        tier = 'basic'

    PRO_ONLY = {'profit_lock_alerts', 'trailing_alerts'}
    ALLOWED = {'monday_briefing', 'wednesday_update', 'zone_alerts',
                'thesis_alerts', 'profit_lock_alerts', 'trailing_alerts'}

    prefs = {}
    for key in ALLOWED:
        if key not in data:
            continue
        if key in PRO_ONLY and tier == 'basic':
            return jsonify({'error': f'{key} requires Pro or Premium tier'}), 403
        prefs[key] = bool(data[key])

    try:
        import sqlite3 as _sq, json as _json
        conn = _sq.connect(_DB_PATH, timeout=10)
        try:
            conn.execute(
                "ALTER TABLE user_preferences ADD COLUMN notification_prefs TEXT DEFAULT '{}'"
            )
        except Exception:
            pass
        row = conn.execute(
            "SELECT notification_prefs FROM user_preferences WHERE user_id=?",
            (user_id,)
        ).fetchone()
        existing = {}
        if row and row[0]:
            try:
                existing = _json.loads(row[0])
            except Exception:
                pass
        existing.update(prefs)
        conn.execute(
            "UPDATE user_preferences SET notification_prefs=? WHERE user_id=?",
            (_json.dumps(existing), user_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'ok': True, 'prefs': existing})


@app.route('/users/<user_id>/trading-prefs', methods=['PATCH'])
@require_auth
def update_trading_prefs(user_id):
    """
    PATCH /users/<user_id>/trading-prefs

    Save trading preference fields: max_risk_per_trade_pct, preferred_broker,
    experience_level, trading_bio.
    Returns: { "ok": true }
    """
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}

    risk_pct  = data.get('max_risk_per_trade_pct')
    broker    = str(data.get('preferred_broker',  '') or '')[:100]
    exp_level = str(data.get('experience_level',  '') or '')[:100]
    bio       = str(data.get('trading_bio',       '') or '')[:1000]

    if risk_pct is not None:
        try:
            risk_pct = float(risk_pct)
            if not (0 < risk_pct <= 100):
                return jsonify({'error': 'max_risk_per_trade_pct must be between 0 and 100'}), 400
        except (TypeError, ValueError):
            return jsonify({'error': 'max_risk_per_trade_pct must be a number'}), 400

    try:
        import sqlite3 as _sq
        conn = _sq.connect(_DB_PATH, timeout=10)
        for col, default in [
            ('preferred_broker',  "TEXT DEFAULT ''"),
            ('experience_level',  "TEXT DEFAULT ''"),
            ('trading_bio',       "TEXT DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE user_preferences ADD COLUMN {col} {default}")
            except Exception:
                pass
        updates, params = [], []
        if risk_pct is not None:
            updates.append('max_risk_per_trade_pct=?'); params.append(risk_pct)
        if broker:
            updates.append('preferred_broker=?'); params.append(broker)
        if exp_level:
            updates.append('experience_level=?'); params.append(exp_level)
        if bio is not None:
            updates.append('trading_bio=?'); params.append(bio)
        if updates:
            params.append(user_id)
            conn.execute(
                f"UPDATE user_preferences SET {', '.join(updates)} WHERE user_id=?",
                params
            )
            conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'ok': True})


@app.route('/users/<user_id>', methods=['DELETE'])
@require_auth
def delete_account(user_id):
    """
    DELETE /users/<user_id>

    Permanently delete the authenticated user's account.
    Removes rows from user_auth and user_preferences.
    Returns: { "deleted": true }
    """
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        conn.execute("DELETE FROM user_auth WHERE user_id=?", (user_id,))
        try:
            conn.execute("DELETE FROM user_preferences WHERE user_id=?", (user_id,))
        except Exception:
            pass
        try:
            conn.execute("DELETE FROM refresh_tokens WHERE user_id=?", (user_id,))
        except Exception:
            pass
        conn.commit()
        conn.close()
        log_audit_event(_DB_PATH, action='account_deleted', user_id=user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    resp = jsonify({'deleted': True})
    _clear_auth_cookies(resp)
    return resp


# ── Seed distribution endpoints ───────────────────────────────────────────────

@app.route('/seed/status', methods=['GET'])
@limiter.exempt
def seed_status():
    """
    GET /seed/status

    Returns the current seed tag pushed to GitHub, local fact count, last push
    time, and next scheduled push times. Clients use this to check whether
    they need to pull a new seed without hitting the GitHub API directly.
    """
    import sqlite3 as _sqlite3
    from datetime import datetime as _dt, timezone as _tz

    # Read last push tag from .seed_tag file if present (written by push_seed.py)
    tag_file = pathlib.Path('.seed_tag')
    last_tag = tag_file.read_text().strip() if tag_file.exists() else None

    # Fact count from live DB
    try:
        _c = sqlite3.connect(_DB_PATH, timeout=5)
        total_facts = _c.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
        _c.close()
    except Exception:
        total_facts = None

    # Next push times: 09:00, 13:00, 17:00 UTC
    now_utc = _dt.now(_tz.utc)
    push_hours = [9, 13, 17]
    next_pushes = []
    for h in push_hours:
        candidate = now_utc.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate <= now_utc:
            from datetime import timedelta as _td
            candidate += _td(days=1)
        next_pushes.append(candidate.strftime('%Y-%m-%dT%H:%M:%SZ'))

    return jsonify({
        'last_tag':    last_tag,
        'total_facts': total_facts,
        'next_pushes': next_pushes,
        'db_path':     _DB_PATH,
        'server_time': now_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
    })


# ── Frontend-Ready endpoints ──────────────────────────────────────────────────

@app.route('/health/detailed', methods=['GET'])
def health_detailed():
    """
    GET /health/detailed

    Extended liveness check: KB stats, per-adapter ingest status, epistemic
    stress score, and scheduler states.  Always available (no feature guard).
    """
    import sqlite3 as _sqlite3
    result: dict = {'status': 'ok', 'db': _DB_PATH}

    try:
        conn = _sqlite3.connect(_DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT subject), COUNT(DISTINCT predicate) FROM facts"
        ).fetchone()
        conn.close()
        result['kb_stats'] = {
            'total_facts':        row[0],
            'unique_subjects':    row[1],
            'unique_predicates':  row[2],
        }
    except Exception:
        result['kb_stats'] = None

    if HAS_STRESS:
        try:
            conn2 = _sqlite3.connect(_DB_PATH, timeout=5)
            sample_atoms = conn2.execute(
                "SELECT subject, predicate, object, confidence, source, timestamp "
                "FROM facts ORDER BY confidence DESC LIMIT 50"
            ).fetchall()
            conn2.close()
            cols = ['subject', 'predicate', 'object', 'confidence', 'source', 'timestamp']
            atoms = [dict(zip(cols, r)) for r in sample_atoms]
            sr = compute_stress(atoms, [], None)
            result['kb_stress'] = sr.composite_stress
        except Exception:
            result['kb_stress'] = None

    if HAS_INGEST and _ingest_scheduler:
        try:
            result['adapters'] = _ingest_scheduler.get_status()
        except Exception:
            result['adapters'] = None

    result['tip_scheduler']      = 'running' if (_tip_scheduler and getattr(_tip_scheduler, '_thread', None) and _tip_scheduler._thread.is_alive()) else 'stopped'
    result['delivery_scheduler'] = 'running' if (_delivery_scheduler and getattr(_delivery_scheduler, '_thread', None) and _delivery_scheduler._thread.is_alive()) else 'stopped'
    result['position_monitor']   = 'running' if (_position_monitor and getattr(_position_monitor, '_thread', None) and _position_monitor._thread.is_alive()) else 'stopped'

    return jsonify(result)


@app.route('/markets/tickers', methods=['GET'])
def markets_tickers():
    """
    GET /markets/tickers

    Returns the full available ticker universe grouped by sector,
    for use in the Tips interested-markets picker.
    """
    _SECTORS = [
        {'group': 'Mega-cap Tech',   'tickers': ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','AVGO']},
        {'group': 'Financials',      'tickers': ['JPM','V','MA','BAC','GS','MS','BRK-B','AXP','BLK','SCHW']},
        {'group': 'Healthcare',      'tickers': ['UNH','JNJ','LLY','ABBV','PFE','CVS','MRK','BMY','GILD']},
        {'group': 'Energy',          'tickers': ['XOM','CVX','COP']},
        {'group': 'Consumer',        'tickers': ['WMT','PG','KO','MCD','COST']},
        {'group': 'Industrials',     'tickers': ['CAT','HON','RTX']},
        {'group': 'Comms / Media',   'tickers': ['DIS','NFLX','CMCSA']},
        {'group': 'Semis / Software','tickers': ['AMD','INTC','QCOM','MU','CRM','ADBE','NOW','SNOW']},
        {'group': 'Fintech',         'tickers': ['PYPL','COIN']},
        {'group': 'REITs',           'tickers': ['AMT','PLD','EQIX']},
        {'group': 'Utilities',       'tickers': ['NEE','DUK','SO']},
        {'group': 'ETFs — Broad',    'tickers': ['SPY','QQQ','IWM','DIA','VTI']},
        {'group': 'ETFs — Sector',   'tickers': ['XLF','XLE','XLK','XLV','XLI','XLC','XLY','XLP']},
        {'group': 'ETFs — Macro',    'tickers': ['GLD','SLV','TLT','HYG','LQD','UUP']},
    ]
    all_default = [t for s in _SECTORS for t in s['tickers']]
    extra = []
    try:
        import sqlite3 as _sq
        _c = _sq.connect(_DB_PATH, timeout=5)
        try:
            rows = _c.execute(
                "SELECT ticker FROM universe_tickers WHERE added_to_ingest=1"
            ).fetchall()
            for (t,) in rows:
                if t.upper() not in (x.upper() for x in all_default):
                    extra.append(t.upper())
        finally:
            _c.close()
    except Exception:
        pass
    result = list(_SECTORS)
    if extra:
        result.append({'group': 'User-added', 'tickers': extra})
    return jsonify({'sectors': result})


@app.route('/markets/overview', methods=['GET'])
def markets_overview():
    """
    GET /markets/overview

    Single-call market snapshot: regime, top 3 high-conviction tickers,
    macro summary, KB stress, and unread alert count.
    """
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    import sqlite3 as _sqlite3
    result: dict = {'as_of': datetime.now(timezone.utc).isoformat()}

    try:
        summary = build_portfolio_summary(_DB_PATH)
        top = [
            {
                'ticker':           t.get('ticker'),
                'conviction_tier':  t.get('conviction_tier'),
                'upside_pct':       t.get('upside_pct'),
                'position_size_pct': t.get('position_size_pct'),
            }
            for t in summary.get('top_conviction', [])[:3]
        ]
        result['top_conviction'] = top
        result['regime']         = summary.get('macro_regime')
        result['macro_summary']  = summary.get('macro_summary')
    except Exception as e:
        result['top_conviction'] = []
        result['regime']         = None
        result['macro_summary']  = None

    if HAS_STRESS:
        try:
            conn = _sqlite3.connect(_DB_PATH, timeout=5)
            sample = conn.execute(
                "SELECT subject,predicate,object,confidence,source,timestamp "
                "FROM facts ORDER BY confidence DESC LIMIT 50"
            ).fetchall()
            conn.close()
            cols = ['subject','predicate','object','confidence','source','timestamp']
            atoms = [dict(zip(cols, r)) for r in sample]
            sr = compute_stress(atoms, [], None)
            result['kb_stress'] = sr.composite_stress
        except Exception:
            result['kb_stress'] = None

    try:
        unread = get_alerts(_DB_PATH, unseen_only=True, limit=500)
        result['unread_alerts'] = len(unread)
    except Exception:
        result['unread_alerts'] = 0

    return jsonify(result)


@app.route('/tickers/<ticker>/summary', methods=['GET'])
def ticker_summary(ticker: str):
    """
    GET /tickers/<ticker>/summary

    Full KB signal profile for a single ticker: conviction, signal quality,
    upside, invalidation, position sizing, open patterns, recent alerts.
    """
    ticker = ticker.upper()
    import sqlite3 as _sqlite3

    conn = _sqlite3.connect(_DB_PATH, timeout=10)
    try:
        rows = conn.execute(
            """SELECT predicate, object, confidence, source, timestamp
               FROM facts
               WHERE UPPER(subject) = ?
               ORDER BY confidence DESC""",
            (ticker,),
        ).fetchall()
    finally:
        conn.close()

    atoms: dict = {}
    for pred, obj, conf, src, ts in rows:
        if pred not in atoms:
            atoms[pred] = obj

    signal_preds = [
        'conviction_tier', 'signal_quality', 'upside_pct',
        'invalidation_distance', 'position_size_pct', 'options_regime',
        'macro_confirmation', 'thesis_risk_level', 'signal_direction',
        'volatility_regime', 'price_target', 'last_price',
    ]
    profile = {p: atoms.get(p) for p in signal_preds}
    profile['ticker'] = ticker

    if HAS_STRESS:
        try:
            cols = ['subject','predicate','object','confidence','source','timestamp']
            ticker_atoms = [dict(zip(cols, r)) for r in
                _sqlite3.connect(_DB_PATH, timeout=5).execute(
                    "SELECT subject,predicate,object,confidence,source,timestamp "
                    "FROM facts WHERE UPPER(subject) = ? ORDER BY confidence DESC LIMIT 30",
                    (ticker,),
                ).fetchall()]
            sr = compute_stress(ticker_atoms, [ticker.lower()], None)
            profile['kb_stress'] = sr.composite_stress
        except Exception:
            profile['kb_stress'] = None

    if HAS_PATTERN_LAYER:
        try:
            patterns = get_open_patterns(_DB_PATH, ticker=ticker, limit=5)
            profile['open_patterns'] = [
                {k: p[k] for k in ('pattern_type','direction','quality_score','timeframe','status')}
                for p in patterns
            ]
        except Exception:
            profile['open_patterns'] = []
    else:
        profile['open_patterns'] = []

    if HAS_ANALYTICS:
        try:
            recent_alerts = get_alerts(_DB_PATH, unseen_only=False, limit=200)
            profile['recent_alerts'] = [a for a in recent_alerts if a.get('ticker') == ticker][:5]
        except Exception:
            profile['recent_alerts'] = []
    else:
        profile['recent_alerts'] = []

    profile['as_of'] = datetime.now(timezone.utc).isoformat()
    return jsonify(profile)


@app.route('/users/<user_id>/watchlist/signals', methods=['GET'])
@require_auth
def watchlist_signals(user_id: str):
    """
    GET /users/<user_id>/watchlist/signals

    Signal summary for every ticker in the user's portfolio — conviction tier,
    pattern count, last tip date — in one call.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    import sqlite3 as _sqlite3

    try:
        tickers = get_user_watchlist_tickers(_DB_PATH, user_id)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if not tickers:
        return jsonify({'signals': [], 'count': 0})

    conn = _sqlite3.connect(_DB_PATH, timeout=10)
    try:
        placeholders = ','.join('?' for _ in tickers)
        rows = conn.execute(
            f"""SELECT UPPER(subject) as ticker, predicate, object
                FROM facts
                WHERE UPPER(subject) IN ({placeholders})
                  AND predicate IN ('conviction_tier','signal_quality','upside_pct','position_size_pct')
                ORDER BY UPPER(subject), predicate""",
            tickers,
        ).fetchall()
    finally:
        conn.close()

    by_ticker: dict = {t: {} for t in tickers}
    for ticker, pred, obj in rows:
        if ticker in by_ticker and pred not in by_ticker[ticker]:
            by_ticker[ticker][pred] = obj

    if HAS_PATTERN_LAYER:
        for ticker in tickers:
            try:
                pats = get_open_patterns(_DB_PATH, ticker=ticker, limit=100)
                by_ticker[ticker]['pattern_count'] = len(pats)
            except Exception:
                by_ticker[ticker]['pattern_count'] = 0

    if HAS_ANALYTICS:
        try:
            tip_logs = {}
            import sqlite3 as _sq
            c = _sq.connect(_DB_PATH, timeout=5)
            log_rows = c.execute(
                """SELECT ps.ticker, MAX(t.delivered_at)
                   FROM tip_delivery_log t
                   JOIN pattern_signals ps ON ps.id = t.pattern_signal_id
                   WHERE t.user_id = ? AND t.success = 1
                   GROUP BY ps.ticker""",
                (user_id,),
            ).fetchall()
            c.close()
            for t, dt in log_rows:
                tip_logs[t.upper()] = dt
        except Exception:
            tip_logs = {}

        for ticker in tickers:
            by_ticker[ticker]['last_tip_date'] = tip_logs.get(ticker)

    signals = [{'ticker': t, **v} for t, v in by_ticker.items()]
    return jsonify({'signals': signals, 'count': len(signals)})


@app.route('/users/<user_id>/alerts/unread-count', methods=['GET'])
@require_auth
def user_alerts_unread_count(user_id: str):
    """
    GET /users/<user_id>/alerts/unread-count

    Lightweight polling endpoint. Returns {"count": N} — the number of unseen
    alerts for tickers in this user's portfolio.  If the user has no portfolio,
    returns the global unseen count.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        if HAS_PRODUCT_LAYER:
            tickers = get_user_watchlist_tickers(_DB_PATH, user_id)
        else:
            tickers = []

        unseen = get_alerts(_DB_PATH, unseen_only=True, limit=10000)
        if tickers:
            unseen = [a for a in unseen if a.get('ticker') in tickers]
        return jsonify({'count': len(unseen)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/onboarding-status', methods=['GET'])
@require_auth
def user_onboarding_status(user_id: str):
    """
    GET /users/<user_id>/onboarding-status

    Returns a structured object of which onboarding steps are complete,
    so the frontend can route users through the flow without inferring state
    from multiple responses.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    import sqlite3 as _sqlite3
    try:
        conn = _sqlite3.connect(_DB_PATH, timeout=10)
        row = conn.execute(
            """SELECT onboarding_complete, telegram_chat_id, tip_delivery_time,
                      tip_delivery_timezone, account_size, selected_sectors
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        portfolio_count = conn.execute(
            "SELECT COUNT(*) FROM user_portfolios WHERE user_id = ?",
            (user_id,),
        ).fetchone()[0]
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if row is None:
        return jsonify({'error': 'user not found'}), 404

    onboarding_complete, chat_id, tip_time, tip_tz, account_size, sectors = row

    telegram_connected = bool(chat_id and chat_id.strip())
    portfolio_submitted = portfolio_count > 0

    tip_config_set = bool(
        (tip_time and tip_time != '07:30') or
        (tip_tz and tip_tz != 'Europe/London')
    )
    account_size_set = account_size is not None and float(account_size or 0) > 0

    import json as _json
    try:
        sector_list = _json.loads(sectors or '[]')
    except Exception:
        sector_list = []
    preferences_set = len(sector_list) > 0

    all_complete = all([
        portfolio_submitted, telegram_connected,
        tip_config_set, account_size_set,
    ])

    return jsonify({
        'portfolio_submitted': portfolio_submitted,
        'telegram_connected':  telegram_connected,
        'tip_config_set':      tip_config_set,
        'account_size_set':    account_size_set,
        'preferences_set':     preferences_set,
        'complete':            all_complete,
    })


@app.route('/users/<user_id>/telegram/verify', methods=['POST'])
@require_auth
def user_telegram_verify(user_id: str):
    """
    POST /users/<user_id>/telegram/verify

    Sends a test message to the user's stored telegram_chat_id.
    Returns {"sent": true/false}.  No body required.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    try:
        user = get_user(_DB_PATH, user_id)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if user is None:
        return jsonify({'error': 'user not found'}), 404

    chat_id = (user.get('telegram_chat_id') or '').strip()
    if not chat_id:
        return jsonify({'error': 'no telegram_chat_id on record — use POST /users/{id}/onboarding to set it'}), 400

    try:
        notifier = TelegramNotifier()
        sent = notifier.send_test(chat_id)
        return jsonify({'sent': sent})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/trader-level', methods=['POST'])
@require_auth
def user_set_trader_level(user_id: str):
    """
    POST /users/<user_id>/trader-level

    Set the trader experience level for the authenticated user.
    Body: {"level": "beginner" | "developing" | "experienced" | "quant"}
    Returns {"trader_level": "<level>"}.
    """
    err = assert_self(user_id)
    if err: return err
    body = request.get_json(force=True, silent=True) or {}
    level = (body.get('level') or '').strip().lower()
    _valid = {'beginner', 'developing', 'experienced', 'quant'}
    if level not in _valid:
        return jsonify({'error': f"Invalid level '{level}'. Must be one of: {sorted(_valid)}"}), 400
    try:
        from users.user_store import set_trader_level as _set_level
        _set_level(_DB_PATH, user_id, level)
        return jsonify({'trader_level': level})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/telegram', methods=['DELETE'])
@require_auth
def user_telegram_delink(user_id: str):
    """
    DELETE /users/<user_id>/telegram

    Clears the stored telegram_chat_id, effectively de-linking the account.
    Returns {"delinked": true}.
    """
    err = assert_self(user_id)
    if err: return err
    import sqlite3 as _sq
    try:
        _c = _sq.connect(_DB_PATH, timeout=10)
        try:
            _c.execute(
                "UPDATE user_preferences SET telegram_chat_id = NULL WHERE user_id = ?",
                (user_id,)
            )
            _c.commit()
        finally:
            _c.close()
        log_audit_event(_DB_PATH, action='telegram_delink', user_id=user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
        return jsonify({'delinked': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    """
    POST /telegram/webhook

    Receives Telegram update payloads pushed by Telegram servers.
    Handles two update types:
      - message       → KB-grounded chat reply to the sending user
      - callback_query → inline keyboard action (position close/hold/more)

    Security: validates X-Telegram-Bot-Api-Secret-Token header against
    TELEGRAM_WEBHOOK_SECRET env var (skipped if env var not set — dev mode).

    Returns HTTP 200 always (Telegram retries on non-200).
    """
    import os as _os

    # ── Signature check ────────────────────────────────────────────────────
    _webhook_secret = _os.environ.get('TELEGRAM_WEBHOOK_SECRET', '')
    if _webhook_secret:
        _sent_token = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if _sent_token != _webhook_secret:
            app.logger.warning('telegram_webhook: invalid secret token from %s', request.remote_addr)
            return jsonify({'ok': False}), 403

    update = request.get_json(force=True, silent=True) or {}

    # ── Dispatch ───────────────────────────────────────────────────────────
    if 'callback_query' in update:
        _handle_tg_callback(update['callback_query'])
    elif 'message' in update:
        _handle_tg_message(update['message'])

    return jsonify({'ok': True})


def _tg_api(method: str, payload: dict) -> bool:
    """Call a Telegram Bot API method. Returns True on HTTP 200."""
    import os as _os, requests as _rq
    token = _os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not token:
        return False
    try:
        r = _rq.post(
            f'https://api.telegram.org/bot{token}/{method}',
            json=payload, timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False


def _handle_tg_message(msg: dict) -> None:
    """
    Process an inbound Telegram text message:
    1. Identify user by chat_id → user_id lookup
    2. Load conversation history from ConversationStore
    3. Run KB retrieve + prompt build + LLM
    4. Reply with MarkdownV2-escaped answer
    5. Persist both turns to ConversationStore
    """
    chat_id  = str(msg.get('chat', {}).get('id', ''))
    text     = (msg.get('text') or '').strip()
    if not chat_id or not text:
        return

    # Ignore bot commands like /start for now
    if text.startswith('/'):
        if text == '/start':
            _tg_api('sendMessage', {
                'chat_id':    chat_id,
                'text':       '👋 *Trading Galaxy Bot*\n\nYour account is linked\\. Ask me anything about your portfolio, market signals, or geopolitical risks\\.',
                'parse_mode': 'MarkdownV2',
            })
        return

    # ── User lookup ────────────────────────────────────────────────────────
    if not HAS_PRODUCT_LAYER:
        return
    try:
        from users.user_store import get_user_by_chat_id
        user_id = get_user_by_chat_id(_DB_PATH, chat_id)
    except Exception as _e:
        app.logger.error('telegram_webhook: user lookup failed: %s', _e)
        return

    if not user_id:
        _tg_api('sendMessage', {
            'chat_id':    chat_id,
            'text':       '⚠️ Your Telegram account is not linked\\. Visit [trading\\-galaxy\\.uk](https://trading-galaxy.uk) to connect your account\\.',
            'parse_mode': 'MarkdownV2',
        })
        return

    # ── Conversation history ───────────────────────────────────────────────
    session_id = f'TG_{chat_id}'
    history_messages = []
    try:
        from knowledge.conversation_store import ConversationStore
        _cs = ConversationStore(_DB_PATH)
        history_messages = _cs.get_recent_messages_for_context(session_id, n_turns=6)
    except Exception:
        _cs = None

    # ── Portfolio-intent detection (Telegram) ───────────────────────
    # Mirror the same keyword gate used in the /chat endpoint.
    # Portfolio context must only be injected when the user explicitly asks
    # about their own holdings — not for every general market question.
    _TG_PORTFOLIO_INTENT_KWS = (
        'my portfolio', 'my holdings', 'my positions', 'my stocks', 'my shares',
        'my book', 'my p&l', 'my pnl', 'my exposure', 'my allocation',
        'discuss my', 'analyse my', 'analyze my', 'review my',
        'affect my', 'impact my', 'affect portfolio', 'impact portfolio',
        'portfolio', 'holdings', 'positions',
    )
    _tg_wants_portfolio = any(kw in text.lower() for kw in _TG_PORTFOLIO_INTENT_KWS)

    # ── KB retrieval ───────────────────────────────────────────────────────
    conn = _kg.thread_local_conn()
    try:
        # Only augment query with portfolio tickers when explicitly asked.
        # Augmenting unconditionally caused every market question to pull
        # portfolio-specific KB atoms and anchor the LLM to the user's book.
        _retrieve_text = text
        if _tg_wants_portfolio and HAS_PRODUCT_LAYER:
            try:
                from users.user_store import get_portfolio as _gp
                from retrieval import _extract_tickers as _et_tg
                _tg_cur_tickers = _et_tg(text)
                if not _tg_cur_tickers:
                    _tg_holdings = _gp(_DB_PATH, user_id)
                    _tg_port_tickers = [h['ticker'] for h in (_tg_holdings or []) if h.get('ticker')]
                    if _tg_port_tickers:
                        _retrieve_text = text + ' ' + ' '.join(_tg_port_tickers)
            except Exception:
                pass
        _tg_limit = 80 if _tg_wants_portfolio else 50
        snippet, atoms = retrieve(_retrieve_text, conn, limit=_tg_limit)
    except Exception as _re:
        app.logger.error('telegram_webhook: retrieve failed: %s', _re)
        snippet, atoms = '', []

    # ── Portfolio context (per-ticker KB signals) ────────────────────
    # Only built and injected when the user explicitly asked about their portfolio.
    portfolio_context = None
    if _tg_wants_portfolio:
        try:
            from users.user_store import get_portfolio as _gp2, get_user_model as _gum2
            _holdings = _gp2(_DB_PATH, user_id)
            _model    = _gum2(_DB_PATH, user_id)
            if _holdings:
                _h_parts = [f"{h['ticker']} ×{int(h['quantity'])}" for h in _holdings[:20]]
                _pos_values = [
                    h['quantity'] * h['avg_cost']
                    for h in _holdings if h.get('quantity') and h.get('avg_cost')
                ]
                _total_cost = sum(_pos_values)
                _lines = ["=== USER PORTFOLIO ===",
                          f"Holdings: {', '.join(_h_parts)}"]
                if _total_cost > 0:
                    _lines.append(f"Total invested (cost basis): £{_total_cost:,.0f}")
                if _model:
                    _risk    = _model.get('risk_tolerance', '')
                    _style   = _model.get('holding_style', '')
                    _sectors = ', '.join(_model.get('sector_affinity') or [])
                    _profile = ' · '.join(p for p in [_risk, _style, _sectors] if p)
                    if _profile:
                        _lines.append(f"Risk profile: {_profile}")
                _holding_tickers = [h['ticker'] for h in _holdings]
                _lines.append("\nPer-holding KB signals:")
                for _ht in _holding_tickers:
                    try:
                        _ht_rows = conn.execute(
                            """SELECT predicate, object FROM facts
                               WHERE subject=? AND predicate IN
                               ('last_price','price_regime','signal_direction',
                                'signal_quality','return_1m','return_1y',
                                'upside_pct','conviction_tier','macro_confirmation',
                                'price_target')
                               ORDER BY predicate""",
                            (_ht.lower(),)
                        ).fetchall()
                        if _ht_rows:
                            _d = {p: v for p, v in _ht_rows}
                            _price  = _d.get('last_price', '?')
                            _regime = _d.get('price_regime', '?').replace('_', ' ')
                            _dir    = _d.get('signal_direction', '?')
                            _qual   = _d.get('signal_quality', '?')
                            _conv   = _d.get('conviction_tier', '?')
                            _up     = _d.get('upside_pct', '?')
                            _target = _d.get('price_target', '')
                            _ret1m  = _d.get('return_1m', '')
                            _ret1y  = _d.get('return_1y', '')
                            _implied = ''
                            try:
                                if _target and _price and _price != '?' and _target != '?':
                                    _move = float(_target) - float(_price)
                                    _move_dir = 'up to' if _move >= 0 else 'down to'
                                    _implied = f" Target: {_target} ({_move_dir}, {_up}% upside)."
                            except Exception:
                                pass
                            _sent = (
                                f"  {_ht}: price {_price} ({_regime}). "
                                f"Signal: {_dir}.{_implied} "
                                f"Quality: {_qual}. Conviction: {_conv}."
                            )
                            if _ret1m:
                                _sent += f" 1m return: {_ret1m}%."
                            if _ret1y:
                                _sent += f" 1y return: {_ret1y}%."
                            _lines.append(_sent)
                        else:
                            _lines.append(f"  {_ht}: No KB signals — answer from general knowledge.")
                    except Exception:
                        _lines.append(f"  {_ht}: No KB signals — answer from general knowledge.")
                portfolio_context = '\n'.join(_lines)
        except Exception:
            pass

    # ── Live data fetch for Telegram (on-demand prices) ─────────────────
    tg_live_context = ''
    if HAS_WORKING_MEMORY and _working_memory is not None:
        try:
            from retrieval import _extract_tickers as _et_tg2
            from knowledge.working_memory import _YF_TICKER_MAP, MAX_ON_DEMAND_TICKERS
            _tg_tickers = _et_tg2(text)
            if not _tg_tickers and _tg_wants_portfolio:
                try:
                    from users.user_store import get_portfolio as _gp3
                    _tg_ph = _gp3(_DB_PATH, user_id)
                    _tg_tickers = [h['ticker'] for h in (_tg_ph or []) if h.get('ticker')]
                except Exception:
                    pass
            _tg_wm_session = f'TG_WM_{chat_id}'
            _yf_vals = set(_YF_TICKER_MAP.values())
            _tg_missing = [
                t for t in _tg_tickers[:MAX_ON_DEMAND_TICKERS]
                if not kb_has_atoms(t, _DB_PATH)
                or t in _YF_TICKER_MAP
                or t in _yf_vals
            ]
            if _tg_missing:
                _working_memory.open_session(_tg_wm_session)
                for _tt in _tg_missing[:MAX_ON_DEMAND_TICKERS]:
                    _working_memory.fetch_on_demand(_tt, _tg_wm_session, _DB_PATH)
                tg_live_context = _working_memory.get_session_snippet(_tg_wm_session)
        except Exception as _tg_wm_err:
            app.logger.debug('telegram_webhook: live fetch failed: %s', _tg_wm_err)

    # ── Epistemic stress ─────────────────────────────────────────────────
    tg_stress_dict = None
    if HAS_STRESS and atoms:
        try:
            _tg_words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', text)
            _tg_key_terms = list({w.lower() for w in _tg_words if len(w) > 2})[:10]
            _tg_stress = compute_stress(atoms, _tg_key_terms, conn)
            tg_stress_dict = {
                'composite_stress':     _tg_stress.composite_stress,
                'decay_pressure':       _tg_stress.decay_pressure,
                'authority_conflict':   _tg_stress.authority_conflict,
                'supersession_density': _tg_stress.supersession_density,
                'conflict_cluster':     _tg_stress.conflict_cluster,
                'domain_entropy':       _tg_stress.domain_entropy,
            }
        except Exception:
            pass

    # ── Trader level — fresh DB fetch, no session state in Telegram ────────
    # Telegram messages carry no session — must query DB on every request so
    # a level change takes effect immediately without waiting for cache expiry.
    _tg_trader_level = 'developing'
    try:
        from users.user_store import get_user as _get_tg_user
        _tg_user_row = _get_tg_user(_DB_PATH, user_id)
        if _tg_user_row:
            _tg_trader_level = _tg_user_row.get('trader_level') or 'developing'
    except Exception:
        pass

    # ── Build prompt ───────────────────────────────────────────────────────
    try:
        from llm.prompt_builder import build as _build_prompt
        messages = _build_prompt(
            user_message=text,
            snippet=snippet,
            portfolio_context=portfolio_context,
            atom_count=len(atoms),
            live_context=tg_live_context or None,
            stress=tg_stress_dict,
            has_history=bool(history_messages),
            telegram_mode=True,
            trader_level=_tg_trader_level,
        )
        # Splice conversation history between system and user turns
        # Strip 'id' and any non-standard fields — Groq rejects them with 400
        if history_messages and len(messages) >= 2:
            _clean_history = [
                {'role': m['role'], 'content': m['content']}
                for m in history_messages
                if m.get('role') in ('user', 'assistant') and m.get('content')
            ]
            messages = [messages[0]] + _clean_history + [messages[-1]]
    except Exception as _pe:
        app.logger.error('telegram_webhook: prompt build failed: %s', _pe)
        return

    # ── LLM call ──────────────────────────────────────────────────────────
    try:
        answer = _llm_chat(messages)
    except Exception as _le:
        app.logger.error('telegram_webhook: LLM call failed: %s', _le)
        answer = None

    if not answer:
        _tg_api('sendMessage', {
            'chat_id': chat_id,
            'text':    '⚠️ The AI is temporarily unavailable. Please try again in a moment.',
        })
        return

    # ── Send reply (MarkdownV2-escaped) ────────────────────────────────────
    from notifications.telegram_notifier import escape_mdv2
    _tg_api('sendMessage', {
        'chat_id':    chat_id,
        'text':       escape_mdv2(answer),
        'parse_mode': 'MarkdownV2',
    })

    # ── Persist both turns ────────────────────────────────────────────────
    if _cs is not None:
        try:
            _cs.add_message(session_id, 'user',      text,   user_id=user_id)
            _cs.add_message(session_id, 'assistant', answer, user_id=user_id)
        except Exception:
            pass


def _handle_tg_callback(cb: dict) -> None:
    """
    Handle a Telegram inline keyboard callback_query.

    Callback data format: pos:{followup_id}:{action}
    Actions: closed | stopped | partial | hold_t2 | override | more

    Always calls answerCallbackQuery to dismiss the Telegram spinner.
    """
    callback_id = cb.get('id', '')
    chat_id     = str(cb.get('from', {}).get('id', ''))
    data        = (cb.get('data') or '').strip()

    # Acknowledge immediately so Telegram spinner dismisses
    _tg_api('answerCallbackQuery', {'callback_query_id': callback_id})

    if not data.startswith('pos:'):
        return

    parts = data.split(':')
    if len(parts) < 3:
        return

    try:
        followup_id = int(parts[1])
    except ValueError:
        return
    action = parts[2]

    if not HAS_PRODUCT_LAYER:
        return

    try:
        from users.user_store import update_followup_status, get_user_by_chat_id
        from datetime import datetime as _dt, timezone as _tz

        user_id = get_user_by_chat_id(_DB_PATH, chat_id)
        if not user_id:
            return

        now_iso = _dt.now(_tz.utc).isoformat()

        if action in ('closed', 'stopped', 'partial'):
            status = 'closed' if action != 'stopped' else 'stopped'
            update_followup_status(_DB_PATH, followup_id, status=status, closed_at=now_iso)
            _reply = '✅ Position marked as closed\\.' if action != 'stopped' else '🛑 Position marked as stopped out\\.'
            _tg_api('sendMessage', {'chat_id': chat_id, 'text': _reply, 'parse_mode': 'MarkdownV2'})

        elif action in ('hold_t2', 'override'):
            _tg_api('sendMessage', {
                'chat_id': chat_id,
                'text':    '👍 Noted — still watching this position\\.',
                'parse_mode': 'MarkdownV2',
            })

        elif action == 'more':
            # Fetch ticker from DB and run a quick KB signal summary
            import sqlite3 as _sq3
            _c3 = _sq3.connect(_DB_PATH, timeout=5)
            _row = _c3.execute(
                "SELECT ticker FROM tip_followups WHERE id=?", (followup_id,)
            ).fetchone()
            _c3.close()
            if _row:
                _ticker = _row[0]
                _conn = _kg.thread_local_conn()
                _snip, _atoms = retrieve(f'{_ticker} signal conviction outlook', _conn, limit=20)
                if _snip:
                    from notifications.telegram_notifier import escape_mdv2
                    _tg_api('sendMessage', {
                        'chat_id':    chat_id,
                        'text':       escape_mdv2(f'📊 KB signals for {_ticker}:\n\n{_snip[:800]}'),
                        'parse_mode': 'MarkdownV2',
                    })

    except Exception as _ce:
        app.logger.error('telegram_webhook: callback handling failed: %s', _ce)


@app.route('/telegram/webhook/register', methods=['POST'])
def telegram_webhook_register():
    """
    POST /telegram/webhook/register

    Convenience endpoint — calls Telegram's setWebhook API to point
    Telegram at this server's /telegram/webhook URL.

    Body (optional): { "base_url": "https://api.trading-galaxy.uk" }
    If base_url is omitted, uses the request's Host header.

    Requires TELEGRAM_BOT_TOKEN env var.
    Returns: { "ok": true/false, "telegram_response": {...} }
    """
    import os as _os, requests as _rq
    token = _os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not token:
        return jsonify({'error': 'TELEGRAM_BOT_TOKEN not configured'}), 503

    data = request.get_json(force=True, silent=True) or {}
    base_url = (data.get('base_url') or '').rstrip('/')
    if not base_url:
        scheme   = 'https' if request.is_secure else 'http'
        base_url = f"{scheme}://{request.host}"

    webhook_url = f'{base_url}/telegram/webhook'
    secret      = _os.environ.get('TELEGRAM_WEBHOOK_SECRET', '')

    payload: dict = {'url': webhook_url, 'allowed_updates': ['message', 'callback_query']}
    if secret:
        payload['secret_token'] = secret

    try:
        resp = _rq.post(
            f'https://api.telegram.org/bot{token}/setWebhook',
            json=payload, timeout=10,
        )
        tg_data = resp.json()
        return jsonify({'ok': resp.status_code == 200, 'telegram_response': tg_data,
                        'webhook_url': webhook_url})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/patterns/<int:pattern_id>', methods=['GET'])
def pattern_detail(pattern_id: int):
    """
    GET /patterns/<id>?user_id=<uid>

    Full detail for a single pattern signal.  If user_id query param is
    provided and the user has account_size set, also returns a position
    recommendation.
    """
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    import sqlite3 as _sqlite3
    try:
        conn = _sqlite3.connect(_DB_PATH, timeout=10)
        row = conn.execute(
            """SELECT id, ticker, pattern_type, direction, zone_high, zone_low,
                      zone_size_pct, timeframe, formed_at, status, filled_at,
                      quality_score, kb_conviction, kb_regime, kb_signal_dir,
                      alerted_users, detected_at
               FROM pattern_signals WHERE id = ?""",
            (pattern_id,),
        ).fetchone()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if row is None:
        return jsonify({'error': 'pattern not found'}), 404

    import json as _json
    cols = ['id','ticker','pattern_type','direction','zone_high','zone_low',
            'zone_size_pct','timeframe','formed_at','status','filled_at',
            'quality_score','kb_conviction','kb_regime','kb_signal_dir',
            'alerted_users','detected_at']
    pattern = dict(zip(cols, row))
    try:
        pattern['alerted_users'] = _json.loads(pattern['alerted_users'] or '[]')
    except Exception:
        pattern['alerted_users'] = []

    position = None
    user_id = request.args.get('user_id')
    if user_id:
        try:
            from analytics.pattern_detector import PatternSignal
            import sqlite3 as _sq
            c = _sq.connect(_DB_PATH, timeout=5)
            pref_row = c.execute(
                """SELECT account_size, max_risk_per_trade_pct, account_currency
                   FROM user_preferences WHERE user_id = ?""",
                (user_id,),
            ).fetchone()
            c.close()
            if pref_row:
                prefs = dict(zip(['account_size','max_risk_per_trade_pct','account_currency'], pref_row))
                sig = PatternSignal(
                    pattern_type  = pattern['pattern_type'],
                    ticker        = pattern['ticker'],
                    direction     = pattern['direction'],
                    zone_high     = pattern['zone_high'],
                    zone_low      = pattern['zone_low'],
                    zone_size_pct = pattern['zone_size_pct'],
                    timeframe     = pattern['timeframe'],
                    formed_at     = pattern['formed_at'],
                    quality_score = pattern['quality_score'] or 0.0,
                    status        = pattern['status'],
                    kb_conviction = pattern.get('kb_conviction', ''),
                    kb_regime     = pattern.get('kb_regime', ''),
                    kb_signal_dir = pattern.get('kb_signal_dir', ''),
                )
                pos = calculate_position(sig, prefs)
                if pos is not None:
                    from dataclasses import asdict
                    position = asdict(pos)
        except Exception:
            position = None

    return jsonify({'pattern': pattern, 'position': position})


@app.route('/users/<user_id>/performance', methods=['GET'])
@require_auth
def user_performance(user_id: str):
    """
    GET /users/<user_id>/performance

    Performance summary derived from tip_delivery_log + tip_feedback:
    tips sent, outcome breakdown, win rate, recent history.
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    try:
        perf = get_tip_performance(_DB_PATH, user_id)
        return jsonify(perf)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/feedback', methods=['POST'])
def submit_feedback():
    """
    POST /feedback

    Record a user-reported tip outcome.

    Body:
      {
        "user_id":    "alice",
        "tip_id":     4,          -- tip_delivery_log.id (optional)
        "pattern_id": 42,         -- pattern_signals.id (optional)
        "outcome":    "hit_t2"    -- hit_t1|hit_t2|hit_t3|stopped_out|pending|skipped
      }

    Returns: { "id": 7, "recorded": true }
    """
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    # Trust authenticated token identity over request body
    user_id = getattr(g, 'user_id', None) or str(data.get('user_id', '')).strip()
    outcome = str(data.get('outcome', '')).strip()

    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400

    _VALID_OUTCOMES = {'hit_t1','hit_t2','hit_t3','stopped_out','pending','skipped'}
    if outcome not in _VALID_OUTCOMES:
        return jsonify({'error': f'outcome must be one of: {", ".join(sorted(_VALID_OUTCOMES))}'}), 400

    try:
        tip_id     = int(data['tip_id'])     if data.get('tip_id')     is not None else None
        pattern_id = int(data['pattern_id']) if data.get('pattern_id') is not None else None
    except (TypeError, ValueError) as e:
        return jsonify({'error': f'tip_id and pattern_id must be integers: {e}'}), 400

    try:
        row = log_tip_feedback(_DB_PATH, user_id, outcome,
                               tip_id=tip_id, pattern_id=pattern_id)

        # Wire calibration update (Phase 4) — non-fatal if hybrid layer absent
        if HAS_HYBRID and pattern_id is not None:
            try:
                import sqlite3 as _sq
                _conn = _sq.connect(_DB_PATH, timeout=5)
                try:
                    prow = _conn.execute(
                        "SELECT ticker, pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                        (pattern_id,),
                    ).fetchone()
                finally:
                    _conn.close()
                if prow:
                    update_calibration(
                        ticker=prow[0], pattern_type=prow[1],
                        timeframe=prow[2], market_regime=prow[3] or None,
                        outcome=outcome, db_path=_DB_PATH,
                    )
                    update_from_feedback(user_id,
                        {'pattern_type': prow[1], 'outcome': outcome}, _DB_PATH)
            except Exception:
                pass

        return jsonify({'id': row['id'], 'recorded': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Tip feedback (3-path: taking_it / tell_me_more / not_for_me) ──────────────

@app.route('/tips/<int:tip_id>/feedback', methods=['POST'])
@limiter.exempt
def tip_feedback_action(tip_id: int):
    """
    POST /tips/<tip_id>/feedback

    Handle user response to a tip card. Three paths:
      taking_it   — add position to portfolio + create tip_followup for monitoring
      tell_me_more — pre-load tip context; returns extended tip detail for Q&A
      not_for_me  — store rejection reason, soft-update user model

    Body:
      { "user_id": "alice", "action": "taking_it"|"tell_me_more"|"not_for_me",
        "rejection_reason": "too_risky" (only for not_for_me),
        "pattern_id": 42 }
    """
    if not HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    data    = request.get_json(force=True, silent=True) or {}
    user_id = getattr(g, 'user_id', None) or str(data.get('user_id', '')).strip()
    action  = str(data.get('action', '')).strip()
    pattern_id = data.get('pattern_id')

    if not user_id:
        return jsonify({'error': 'user_id required'}), 400
    if action not in ('taking_it', 'tell_me_more', 'not_for_me'):
        return jsonify({'error': 'action must be taking_it|tell_me_more|not_for_me'}), 400

    try:
        import sqlite3 as _sq
        pattern_row = None
        if pattern_id:
            _c = _sq.connect(_DB_PATH, timeout=5)
            try:
                r = _c.execute(
                    """SELECT id, ticker, pattern_type, direction, timeframe,
                              zone_low, zone_high, quality_score, status,
                              kb_conviction, kb_regime, kb_signal_dir
                       FROM pattern_signals WHERE id=?""",
                    (int(pattern_id),),
                ).fetchone()
                if r:
                    cols = ['id','ticker','pattern_type','direction','timeframe',
                            'zone_low','zone_high','quality_score','status',
                            'kb_conviction','kb_regime','kb_signal_dir']
                    pattern_row = dict(zip(cols, r))
            finally:
                _c.close()

        # ── Path A: Taking it ────────────────────────────────────────────────
        if action == 'taking_it':
            from users.user_store import create_tip_followup, ensure_tip_feedback_table
            from analytics.pattern_detector import PatternSignal
            from analytics.position_calculator import calculate_position

            if not pattern_row:
                return jsonify({'error': 'pattern_id required for taking_it'}), 400

            # Load user prefs for position sizing
            _c2 = _sq.connect(_DB_PATH, timeout=5)
            try:
                prefs_row = _c2.execute(
                    """SELECT account_size, max_risk_per_trade_pct, account_currency, tier
                       FROM user_preferences WHERE user_id=?""", (user_id,)
                ).fetchone()
            finally:
                _c2.close()
            prefs = {}
            if prefs_row:
                prefs = {
                    'account_size': prefs_row[0] or 10000,
                    'max_risk_per_trade_pct': prefs_row[1] or 1.0,
                    'account_currency': prefs_row[2] or 'GBP',
                    'tier': prefs_row[3] or 'basic',
                }

            sig = PatternSignal(
                pattern_type  = pattern_row['pattern_type'],
                ticker        = pattern_row['ticker'],
                direction     = pattern_row['direction'],
                zone_high     = pattern_row['zone_high'],
                zone_low      = pattern_row['zone_low'],
                zone_size_pct = 0.0,
                timeframe     = pattern_row['timeframe'],
                formed_at     = '',
                quality_score = pattern_row['quality_score'] or 0.0,
                status        = pattern_row['status'],
                kb_conviction = pattern_row.get('kb_conviction',''),
                kb_regime     = pattern_row.get('kb_regime',''),
                kb_signal_dir = pattern_row.get('kb_signal_dir',''),
            )
            # Recalculate position using current KB last_price for freshness
            price_at_feedback = None
            price_at_generation = (pattern_row['zone_low'] + pattern_row['zone_high']) / 2.0
            try:
                import sqlite3 as _sqp
                _cp = _sqp.connect(_DB_PATH, timeout=5)
                _pr = _cp.execute(
                    """SELECT object FROM facts
                       WHERE LOWER(subject)=? AND predicate='last_price'
                       ORDER BY created_at DESC LIMIT 1""",
                    (pattern_row['ticker'].lower(),),
                ).fetchone()
                _cp.close()
                if _pr:
                    price_at_feedback = float(_pr[0])
                    # Rebuild sig with live price as zone midpoint for position sizing
                    _zone_half = (pattern_row['zone_high'] - pattern_row['zone_low']) / 2.0
                    sig = PatternSignal(
                        pattern_type  = pattern_row['pattern_type'],
                        ticker        = pattern_row['ticker'],
                        direction     = pattern_row['direction'],
                        zone_high     = price_at_feedback + _zone_half,
                        zone_low      = price_at_feedback - _zone_half,
                        zone_size_pct = 0.0,
                        timeframe     = pattern_row['timeframe'],
                        formed_at     = '',
                        quality_score = pattern_row['quality_score'] or 0.0,
                        status        = pattern_row['status'],
                        kb_conviction = pattern_row.get('kb_conviction',''),
                        kb_regime     = pattern_row.get('kb_regime',''),
                        kb_signal_dir = pattern_row.get('kb_signal_dir',''),
                    )
            except Exception:
                pass

            pos = calculate_position(sig, prefs) if prefs else None

            # Cash deduction (non-fatal, idempotent)
            cash_result = None
            try:
                from users.user_store import deduct_from_cash
                _pos_value = getattr(pos, 'position_value', None) or (
                    (pos.position_size_units * (price_at_feedback or price_at_generation))
                    if pos and pos.position_size_units else 0.0
                )
                if _pos_value:
                    cash_result = deduct_from_cash(_DB_PATH, user_id, _pos_value, tip_id=tip_id)
            except Exception:
                pass

            followup = create_tip_followup(
                _DB_PATH,
                user_id     = user_id,
                ticker      = pattern_row['ticker'],
                tip_id      = tip_id,
                pattern_id  = pattern_row['id'],
                direction   = pattern_row['direction'],
                entry_price = pos.suggested_entry if pos else pattern_row['zone_low'],
                stop_loss   = pos.stop_loss if pos else None,
                target_1    = pos.target_1 if pos else None,
                target_2    = pos.target_2 if pos else None,
                target_3    = pos.target_3 if pos else None,
                position_size       = pos.position_size_units if pos else None,
                regime_at_entry     = pattern_row.get('kb_regime'),
                conviction_at_entry = pattern_row.get('kb_conviction'),
                pattern_type = pattern_row.get('pattern_type'),
                timeframe    = pattern_row.get('timeframe'),
                zone_low     = pattern_row.get('zone_low'),
                zone_high    = pattern_row.get('zone_high'),
            )

            # Commit opening atom to personal KB
            if HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pattern_row['ticker'],
                               'user_action', 'opened_position', _DB_PATH)
                except Exception:
                    pass

            confirmation = {
                'action': 'taking_it',
                'followup_id': followup['id'],
                'ticker': pattern_row['ticker'],
                'entry_price': pos.suggested_entry if pos else None,
                'stop_loss':   pos.stop_loss if pos else None,
                'target_1':    pos.target_1 if pos else None,
                'target_2':    pos.target_2 if pos else None,
                'position_size': int(pos.position_size_units) if pos else None,
                'price_at_generation': round(price_at_generation, 4),
                'price_at_feedback':   round(price_at_feedback, 4) if price_at_feedback else None,
                'cash_after':      cash_result.get('new_balance') if cash_result else None,
                'cash_is_negative': cash_result.get('is_negative', False) if cash_result else False,
                'cash_deduction_skipped': cash_result.get('skipped', False) if cash_result else False,
                'message': (
                    f"{pattern_row['ticker']} added to monitoring — "
                    f"position monitor activated. "
                    f"You'll be alerted when action is needed."
                ),
            }
            return jsonify(confirmation)

        # ── Path B: Tell me more ─────────────────────────────────────────────
        if action == 'tell_me_more':
            detail = {
                'action': 'tell_me_more',
                'tip_id': tip_id,
                'pattern': pattern_row,
                'message': 'Tip context loaded. Ask me anything about this setup.',
                'suggested_questions': [
                    'What is the risk if it breaks below the zone?',
                    'How has this pattern performed in the current regime?',
                    'Does this conflict with my existing positions?',
                ],
            }
            return jsonify(detail)

        # ── Path C: Not for me ───────────────────────────────────────────────
        if action == 'not_for_me':
            reason = str(data.get('rejection_reason', 'no_reason')).strip()
            _VALID_REASONS = {'too_risky','wrong_setup','wrong_timing',
                              'dont_know_stock','prefer_uk','no_reason'}
            if reason not in _VALID_REASONS:
                reason = 'no_reason'

            log_tip_feedback(_DB_PATH, user_id, 'skipped',
                             tip_id=tip_id, pattern_id=pattern_id)

            # Soft update personal KB with rejection reason
            if HAS_HYBRID and pattern_row:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pattern_row['ticker'],
                               'user_rejection_reason', reason, _DB_PATH)
                    update_from_feedback(user_id,
                        {'pattern_type': pattern_row['pattern_type'],
                         'outcome': 'skipped', 'rejection_reason': reason}, _DB_PATH)
                except Exception:
                    pass

            return jsonify({
                'action': 'not_for_me',
                'rejection_reason': reason,
                'recorded': True,
                'message': 'Thanks — this helps improve future tips for you.',
            })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Position update response (closed / hold_t2 / partial / override) ──────────

@app.route('/tips/<int:followup_id>/position-update', methods=['POST'])
@limiter.exempt
def tip_position_update(followup_id: int):
    """
    POST /tips/<followup_id>/position-update

    Handle user response to a position monitor alert.
    Actions: closed | hold_t2 | partial | override

    Body:
      { "user_id": "alice", "action": "closed"|"hold_t2"|"partial"|"override",
        "exit_price": 923.40,    (closed/partial)
        "shares_closed": 6,      (partial)
        "close_method": "hit_t1" (closed: hit_t1|hit_t2|hit_t3|stopped_out|manual) }
    """
    from users.user_store import (
        get_user_followups, update_followup_status, ensure_tip_followups_table,
    )
    import sqlite3 as _sq

    data    = request.get_json(force=True, silent=True) or {}
    user_id = getattr(g, 'user_id', None) or str(data.get('user_id', '')).strip()
    action  = str(data.get('action', '')).strip()

    if not user_id:
        return jsonify({'error': 'user_id required'}), 400
    if action not in ('closed', 'hold_t2', 'partial', 'override'):
        return jsonify({'error': 'action must be closed|hold_t2|partial|override'}), 400

    # Load the followup row
    _c = _sq.connect(_DB_PATH, timeout=5)
    try:
        ensure_tip_followups_table(_c)
        row = _c.execute(
            """SELECT id, user_id, tip_id, pattern_id, ticker, direction,
                      entry_price, stop_loss, target_1, target_2, target_3,
                      position_size, tracking_target, status,
                      regime_at_entry, conviction_at_entry
               FROM tip_followups WHERE id=? AND user_id=?""",
            (followup_id, user_id),
        ).fetchone()
    finally:
        _c.close()

    if not row:
        return jsonify({'error': 'followup not found'}), 404
    cols = ['id','user_id','tip_id','pattern_id','ticker','direction',
            'entry_price','stop_loss','target_1','target_2','target_3',
            'position_size','tracking_target','status',
            'regime_at_entry','conviction_at_entry']
    pos = dict(zip(cols, row))

    try:
        if action == 'closed':
            exit_price   = float(data.get('exit_price', pos['entry_price'] or 0))
            close_method = str(data.get('close_method', 'manual'))
            entry        = pos['entry_price'] or exit_price
            position_size = pos['position_size'] or 1
            bullish       = pos['direction'] != 'bearish'
            pnl_raw       = (exit_price - entry) * position_size
            if not bullish:
                pnl_raw = -pnl_raw
            pnl_pct = ((exit_price - entry) / entry * 100) if entry else 0.0

            update_followup_status(_DB_PATH, followup_id, status='closed')

            # Resolve prediction ledger
            if HAS_PATTERN_LAYER and pos.get('pattern_id'):
                try:
                    from analytics.prediction_ledger import PredictionLedger
                    pl = PredictionLedger(_DB_PATH)
                    pl.on_price_written(pos['ticker'], exit_price)
                except Exception:
                    pass

            # Update signal calibration
            outcome_map = {
                'hit_t1': 'hit_t1', 'hit_t2': 'hit_t2', 'hit_t3': 'hit_t3',
                'stopped_out': 'stopped_out', 'manual': 'manual',
            }
            cal_outcome = outcome_map.get(close_method, 'manual')
            if HAS_HYBRID and pos.get('pattern_id'):
                try:
                    _c2 = _sq.connect(_DB_PATH, timeout=5)
                    prow = _c2.execute(
                        "SELECT ticker, pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                        (pos['pattern_id'],),
                    ).fetchone()
                    _c2.close()
                    if prow:
                        update_calibration(
                            ticker=prow[0], pattern_type=prow[1],
                            timeframe=prow[2], market_regime=prow[3] or None,
                            outcome=cal_outcome, db_path=_DB_PATH,
                        )
                        update_from_feedback(user_id,
                            {'pattern_type': prow[1], 'outcome': cal_outcome}, _DB_PATH)
                except Exception:
                    pass

            # Commit outcome atom to personal KB
            if HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos['ticker'], 'trade_outcome', cal_outcome, _DB_PATH)
                    write_atom(user_id, pos['ticker'], 'realised_pnl_pct',
                               f'{pnl_pct:+.1f}%', _DB_PATH)
                except Exception:
                    pass

            log_tip_feedback(_DB_PATH, user_id, cal_outcome,
                             tip_id=pos.get('tip_id'), pattern_id=pos.get('pattern_id'))

            return jsonify({
                'action': 'closed',
                'ticker': pos['ticker'],
                'exit_price': exit_price,
                'entry_price': entry,
                'pnl_gbp': round(pnl_raw, 2),
                'pnl_pct': round(pnl_pct, 2),
                'outcome': cal_outcome,
                'message': (
                    f"Trade closed — {pos['ticker']}: "
                    f"{'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%. "
                    f"Calibration updated."
                ),
            })

        elif action == 'hold_t2':
            new_stop = pos['entry_price']
            update_followup_status(_DB_PATH, followup_id,
                                   status='watching', tracking_target='T2',
                                   stop_loss=new_stop)
            if HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos['ticker'],
                               'user_position_intent', 'holding_for_t2', _DB_PATH)
                except Exception:
                    pass
            return jsonify({
                'action': 'hold_t2',
                'ticker': pos['ticker'],
                'tracking_target': 'T2',
                'new_stop': new_stop,
                'message': f"Stop moved to breakeven ({new_stop}) — risk-free position. Watching for T2.",
            })

        elif action == 'partial':
            shares_closed = float(data.get('shares_closed', 0))
            exit_price    = float(data.get('exit_price', pos['entry_price'] or 0))
            orig_size     = pos['position_size'] or 0
            remainder     = max(0, orig_size - shares_closed)
            entry         = pos['entry_price'] or exit_price
            partial_pnl   = (exit_price - entry) * shares_closed

            _c3 = _sq.connect(_DB_PATH, timeout=5)
            try:
                ensure_tip_followups_table(_c3)
                _c3.execute(
                    "UPDATE tip_followups SET position_size=?, status='partial', updated_at=? WHERE id=?",
                    (remainder, datetime.now(timezone.utc).isoformat(), followup_id),
                )
                _c3.commit()
            finally:
                _c3.close()

            return jsonify({
                'action': 'partial',
                'ticker': pos['ticker'],
                'shares_closed': shares_closed,
                'remainder': remainder,
                'partial_pnl': round(partial_pnl, 2),
                'exit_price': exit_price,
                'message': (
                    f"Partial exit recorded — {int(shares_closed)} shares closed at {exit_price}. "
                    f"{int(remainder)} shares remaining. Monitor continues."
                ),
            })

        elif action == 'override':
            # User is overriding the stop-zone recommendation
            update_followup_status(_DB_PATH, followup_id,
                                   status='watching', alert_level='OVERRIDE')
            if HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos['ticker'],
                               'user_override', 'held_past_stop_zone', _DB_PATH)
                except Exception:
                    pass
            return jsonify({
                'action': 'override',
                'ticker': pos['ticker'],
                'message': 'Override noted — monitoring every 15 minutes. If stop is breached a CRITICAL alert will fire.',
            })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Telegram callback webhook (inline keyboard button presses) ─────────────────

@app.route('/telegram/callback', methods=['POST'])
@limiter.exempt
def telegram_callback():
    """
    POST /telegram/callback

    Receives Telegram inline keyboard callback_query updates.
    Dispatches to tip_feedback_action or tip_position_update based on callback_data.

    callback_data formats:
      tip:<tip_id>:<action>          → tip feedback (taking_it|tell_me_more|not_for_me)
      pos:<followup_id>:<action>     → position update (closed|hold_t2|partial|override)
    """
    import os as _os
    data = request.get_json(force=True, silent=True) or {}

    callback_query = data.get('callback_query', {})
    if not callback_query:
        return jsonify({'ok': True})

    callback_data = callback_query.get('data', '')
    from_user     = callback_query.get('from', {})
    tg_user_id    = str(from_user.get('id', ''))
    query_id      = callback_query.get('id', '')

    # Acknowledge the callback immediately (Telegram requires <3s)
    try:
        import sqlite3 as _sq
        _c = _sq.connect(_DB_PATH, timeout=5)
        row = _c.execute(
            "SELECT user_id FROM user_preferences WHERE telegram_chat_id=?",
            (tg_user_id,),
        ).fetchone()
        _c.close()
        user_id = row[0] if row else None
    except Exception:
        user_id = None

    if not user_id:
        return jsonify({'ok': True})

    try:
        parts = callback_data.split(':')
        if len(parts) >= 3 and parts[0] == 'tip':
            tip_id = int(parts[1])
            action_map = {
                'taking': 'taking_it', 'more': 'tell_me_more', 'skip': 'not_for_me',
                'taking_it': 'taking_it', 'tell_me_more': 'tell_me_more', 'not_for_me': 'not_for_me',
            }
            action = action_map.get(parts[2], parts[2])
            with app.test_request_context(
                f'/tips/{tip_id}/feedback',
                method='POST',
                json={'user_id': user_id, 'action': action},
                content_type='application/json',
            ):
                from flask import g as _g
                _g.user_id = user_id
                resp = tip_feedback_action(tip_id)

        elif len(parts) >= 3 and parts[0] == 'pos':
            followup_id = int(parts[1])
            pos_action_map = {
                'closed': 'closed', 'hold_t2': 'hold_t2',
                'partial': 'partial', 'override': 'override', 'more': 'override',
            }
            action = pos_action_map.get(parts[2], parts[2])
            with app.test_request_context(
                f'/tips/{followup_id}/position-update',
                method='POST',
                json={'user_id': user_id, 'action': action},
                content_type='application/json',
            ):
                from flask import g as _g
                _g.user_id = user_id
                resp = tip_position_update(followup_id)

    except Exception as e:
        import logging as _log
        _log.getLogger(__name__).warning('telegram_callback error: %s', e)

    return jsonify({'ok': True})


@app.route('/users/<user_id>/alerts', methods=['GET'])
@require_auth
def user_alerts(user_id: str):
    """
    GET /users/<user_id>/alerts?all=false&limit=50

    Alerts scoped to tickers in the user's portfolio.  Falls back to the
    global alert list if the user has no portfolio holdings.

    Query params:
      all   — 'true' returns seen + unseen (default: unseen only)
      limit — max rows (default 50)
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        unseen_only = request.args.get('all', '').lower() != 'true'
        limit       = int(request.args.get('limit', 50))
    except (TypeError, ValueError):
        limit = 50

    try:
        if HAS_PRODUCT_LAYER:
            tickers = get_user_watchlist_tickers(_DB_PATH, user_id)
        else:
            tickers = []

        rows = get_alerts(_DB_PATH, unseen_only=unseen_only, limit=10000)
        if tickers:
            rows = [a for a in rows if a.get('ticker') in tickers]
        rows = rows[:limit]
        return jsonify({'alerts': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Hybrid Build Endpoints ────────────────────────────────────────────────────

@app.route('/users/<user_id>/expand-universe', methods=['POST'])
@require_auth
def expand_universe(user_id: str):
    """
    POST /users/<user_id>/expand-universe

    Resolve interest → validate tickers → register → bootstrap.

    Body: { "description": "uranium miners", "market_type": "equities" }
    Returns: { expansion_id, resolved_tickers, rejected_tickers,
               staging_tickers, causal_edges_seeded, estimated_bootstrap_seconds }
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    description = str(data.get('description', '')).strip()
    market_type = str(data.get('market_type', 'equities')).strip()

    if len(description) < 3:
        return jsonify({'error': 'description must be at least 3 characters'}), 400
    if not market_type:
        return jsonify({'error': 'market_type is required'}), 400

    # Tier limit check
    tier = 'basic'
    try:
        from users.user_store import get_user_tier
        tier = get_user_tier(_DB_PATH, user_id)
    except Exception:
        pass
    max_universe = 100 if tier == 'pro' else 20
    current_count = len(DynamicWatchlistManager.get_user_tickers(user_id, _DB_PATH))
    if current_count >= max_universe:
        return jsonify({'error': f'universe limit reached ({max_universe} tickers for {tier} tier)'}), 400

    try:
        # 1. Resolve via LLM
        expansion = resolve_interest(description, market_type, user_id, _DB_PATH)

        if expansion.error == 'llm_unavailable':
            return jsonify({'resolved_tickers': [], 'rejected_tickers': [],
                            'staging_tickers': [], 'causal_edges_seeded': 0,
                            'estimated_bootstrap_seconds': 0,
                            'error': 'llm_unavailable'}), 200

        # 2. Validate tickers
        all_candidates = expansion.tickers[:_MAX_TICKERS_PER_REQUEST]
        validation = validate_tickers(all_candidates, market_region=market_type)

        # 3. Write universe_expansions row
        import sqlite3 as _sqlite3
        import json as _json
        now = datetime.now(timezone.utc).isoformat()
        conn = _sqlite3.connect(_DB_PATH, timeout=10)
        try:
            ensure_hybrid_tables(conn)
            cur = conn.execute(
                """INSERT INTO user_universe_expansions
                   (user_id, description, sector_label, tickers, etfs, keywords,
                    causal_edges, status, requested_at, activated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (user_id, description, expansion.sector_label,
                 _json.dumps(validation.valid),
                 _json.dumps(expansion.etfs),
                 _json.dumps(expansion.keywords),
                 _json.dumps(expansion.causal_relationships),
                 'active', now, now),
            )
            expansion_id = cur.lastrowid
            conn.commit()
        finally:
            conn.close()

        # 4. Add tickers to watchlist
        result = DynamicWatchlistManager.add_tickers(
            validation.valid, user_id, _DB_PATH,
            sector_label=expansion.sector_label,
        )
        promoted = result['promoted']
        staged   = result['staged']

        # 5. Seed causal edges
        edges_seeded = seed_causal_edges(expansion.causal_relationships, _DB_PATH)

        # 6. Bootstrap newly promoted tickers async
        for t in promoted:
            bootstrap_ticker_async(t, _DB_PATH)

        # 7. Write personal KB atoms
        write_universe_atoms(user_id, validation.valid, description, _DB_PATH)

        # 8. Estimate bootstrap time
        est_seconds = estimate_bootstrap_seconds(len(promoted), _DB_PATH)

        return jsonify({
            'expansion_id':               expansion_id,
            'resolved_tickers':           validation.valid,
            'rejected_tickers':           validation.rejected,
            'staging_tickers':            staged,
            'causal_edges_seeded':        edges_seeded,
            'estimated_bootstrap_seconds': est_seconds,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


_MAX_TICKERS_PER_REQUEST = 20


@app.route('/users/<user_id>/universe', methods=['GET'])
@require_auth
def get_user_universe(user_id: str):
    """GET /users/<user_id>/universe — current expanded watchlist + coverage tiers."""
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        tickers = DynamicWatchlistManager.get_user_tickers(user_id, _DB_PATH)
        result = []
        for t in tickers:
            ct = compute_coverage_tier(t, _DB_PATH)
            result.append({
                'ticker':       t,
                'coverage_tier': ct.tier if ct else 'unknown',
                'coverage_count': ct.coverage_count if ct else 0,
            })
        return jsonify({'tickers': result, 'count': len(result)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/universe/<ticker>', methods=['DELETE'])
@require_auth
def remove_universe_ticker(user_id: str, ticker: str):
    """DELETE /users/<user_id>/universe/<ticker> — remove from personal universe."""
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        removed = DynamicWatchlistManager.remove_ticker(ticker, user_id, _DB_PATH)
        if not removed:
            return jsonify({'error': 'ticker not found or not owned by this user'}), 404
        return jsonify({'removed': ticker.upper(), 'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/universe/bootstrap-status', methods=['GET'])
@require_auth
def universe_bootstrap_status(user_id: str):
    """GET /users/<user_id>/universe/bootstrap-status — per-ticker bootstrap completion."""
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        status = DynamicWatchlistManager.get_bootstrap_status(user_id, _DB_PATH)
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/universe/staging', methods=['GET'])
@require_auth
def user_universe_staging(user_id: str):
    """GET /users/<user_id>/universe/staging — user's staged (not yet promoted) tickers."""
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        rows = get_staged_tickers(_DB_PATH, user_id=user_id)
        return jsonify({'staging': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/preferences/focus', methods=['POST'])
@require_auth
def set_user_focus(user_id: str):
    """
    POST /users/<user_id>/preferences/focus — explicit preference overrides.
    Body: { "preferred_upside_min": 15.0, "preferred_pattern": "fvg" }
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    try:
        from users.personal_kb import write_atom as pkb_write
        written = []
        if 'preferred_upside_min' in data:
            pkb_write(user_id, user_id, 'preferred_upside_min',
                      str(float(data['preferred_upside_min'])), 0.9, 'user_override', _DB_PATH)
            written.append('preferred_upside_min')
        if 'preferred_pattern' in data:
            pkb_write(user_id, user_id, 'preferred_pattern',
                      str(data['preferred_pattern']), 0.9, 'user_override', _DB_PATH)
            written.append('preferred_pattern')
        return jsonify({'updated': written, 'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/engagement', methods=['POST'])
@require_auth
def log_user_engagement(user_id: str):
    """
    POST /users/<user_id>/engagement — log an engagement event.
    Body: { "event_type": "tip_opened", "ticker": "NVDA", "pattern_type": "fvg", "sector": "technology" }
    """
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    event_type = str(data.get('event_type', '')).strip()
    if not event_type:
        return jsonify({'error': 'event_type is required'}), 400
    try:
        log_engagement_event(
            _DB_PATH, user_id, event_type,
            ticker=data.get('ticker'),
            pattern_type=data.get('pattern_type'),
            sector=data.get('sector'),
        )
        update_from_engagement(user_id, _DB_PATH)
        return jsonify({'logged': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/kb-context', methods=['GET'])
@require_auth
def user_kb_context(user_id: str):
    """GET /users/<user_id>/kb-context — personal KB atoms."""
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        from users.personal_kb import read_atoms as pkb_read
        atoms = pkb_read(user_id, _DB_PATH)
        return jsonify({'atoms': atoms, 'count': len(atoms)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/users/<user_id>/preferences/inferred', methods=['GET'])
@require_auth
def user_inferred_preferences(user_id: str):
    """GET /users/<user_id>/preferences/inferred — what system has inferred."""
    err = assert_self(user_id)
    if err: return err
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        ctx = get_context_document(user_id, _DB_PATH)
        return jsonify({
            'sector_affinity':         ctx.sector_affinity,
            'risk_tolerance':          ctx.risk_tolerance,
            'holding_style':           ctx.holding_style,
            'portfolio_beta':          ctx.portfolio_beta,
            'preferred_pattern':       ctx.preferred_pattern,
            'avg_win_rate':            ctx.avg_win_rate,
            'high_engagement_sector':  ctx.high_engagement_sector,
            'low_engagement_sector':   ctx.low_engagement_sector,
            'preferred_upside_min':    ctx.preferred_upside_min,
            'active_universe':         ctx.active_universe,
            'pattern_hit_rates':       ctx.pattern_hit_rates,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/universe/trending', methods=['GET'])
def universe_trending():
    """GET /universe/trending — fastest-growing coverage tickers (7d)."""
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        trending = compute_trending_markets(_DB_PATH)
        return jsonify({
            'trending': [
                {
                    'ticker':         t.ticker,
                    'coverage_count': t.coverage_count,
                    'coverage_7d_ago': t.coverage_7d_ago,
                    'growth_rate':    t.growth_rate,
                    'sector_label':   t.sector_label,
                }
                for t in trending
            ]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/universe/coverage', methods=['GET'])
def universe_coverage():
    """GET /universe/coverage — full coverage leaderboard."""
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        rows = get_universe_tickers(_DB_PATH)
        return jsonify({'tickers': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/universe/staging/global', methods=['GET'])
def universe_staging_global():
    """GET /universe/staging/global — all staged (not yet promoted) tickers."""
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        rows = get_staged_tickers(_DB_PATH)
        return jsonify({'staging': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/network/health', methods=['GET'])
def network_health():
    """GET /network/health — flywheel velocity, coverage distribution."""
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        report = compute_network_health(_DB_PATH)
        return jsonify({
            'total_tickers':          report.total_tickers,
            'total_users':            report.total_users,
            'tickers_by_tier':        report.tickers_by_tier,
            'coverage_distribution':  report.coverage_distribution,
            'flywheel_velocity':      report.flywheel_velocity,
            'cohort_signals_active':  report.cohort_signals_active,
            'generated_at':           report.generated_at,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/network/calibration/<ticker>', methods=['GET'])
def network_calibration(ticker: str):
    """GET /network/calibration/<ticker> — collective hit rates for ticker."""
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    pattern_type = request.args.get('pattern_type', 'fvg')
    timeframe    = request.args.get('timeframe', '1h')
    try:
        cal = get_calibration(ticker, pattern_type, timeframe, _DB_PATH)
        if cal is None:
            return jsonify({'calibration': None,
                            'reason': 'insufficient_samples (< 10)'}), 200
        return jsonify({
            'ticker':                   cal.ticker,
            'pattern_type':             cal.pattern_type,
            'timeframe':                cal.timeframe,
            'market_regime':            cal.market_regime,
            'sample_size':              cal.sample_size,
            'hit_rate_t1':              cal.hit_rate_t1,
            'hit_rate_t2':              cal.hit_rate_t2,
            'hit_rate_t3':              cal.hit_rate_t3,
            'stopped_out_rate':         cal.stopped_out_rate,
            'calibration_confidence':   cal.calibration_confidence,
            'confidence_label':         cal.confidence_label,
            'last_updated':             cal.last_updated,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/network/cohort/<ticker>', methods=['GET'])
def network_cohort(ticker: str):
    """GET /network/cohort/<ticker> — cohort consensus + stop cluster."""
    if not HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        signal = detect_cohort_consensus(ticker, _DB_PATH)
        if signal is None:
            return jsonify({'cohort_signal': None,
                            'reason': 'insufficient_cohort (< 10 users)'}), 200
        return jsonify({
            'ticker':               signal.ticker,
            'cohort_size':          signal.cohort_size,
            'consensus_direction':  signal.consensus_direction,
            'consensus_strength':   signal.consensus_strength,
            'stop_cluster':         signal.stop_cluster,
            'contrarian_flag':      signal.contrarian_flag,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Intelligence layer endpoints ──────────────────────────────────────────────

@app.route('/ledger/performance', methods=['GET'])
@limiter.exempt
def ledger_performance():
    """
    GET /ledger/performance

    Public endpoint (no auth required) returning the system's prediction
    accuracy record: Brier score, calibration curve, regime breakdown.
    This is the March 24th validation story told in a single API call.
    """
    if _prediction_ledger is None:
        return jsonify({'error': 'prediction_ledger_not_initialised'}), 503
    try:
        report = _prediction_ledger.get_performance_report()
        return jsonify(report)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/forecast/<ticker>/<pattern_type>', methods=['GET'])
@require_auth
def forecast_signal(ticker: str, pattern_type: str):
    """
    GET /forecast/<ticker>/<pattern_type>?timeframe=1d&account_size=10000&risk_pct=1.0

    On-demand probabilistic forecast for a (ticker, pattern_type) pair.
    Unseeded — natural Monte Carlo variance is acceptable here.
    Uses calibration data + current KB atoms (IV rank, macro, short interest).
    """
    try:
        from analytics.signal_forecaster import SignalForecaster
        timeframe    = request.args.get('timeframe', '1d')
        account_size = float(request.args.get('account_size', 10000))
        risk_pct     = float(request.args.get('risk_pct', 1.0))
        forecaster   = SignalForecaster(_DB_PATH)
        result       = forecaster.forecast(
            ticker       = ticker,
            pattern_type = pattern_type,
            timeframe    = timeframe,
            account_size = account_size,
            risk_pct     = risk_pct,
            seed         = None,  # unseeded — exploratory call
        )
        return jsonify({
            'ticker':                result.ticker,
            'pattern_type':          result.pattern_type,
            'timeframe':             result.timeframe,
            'market_regime':         result.market_regime,
            'p_hit_t1':              result.p_hit_t1,
            'p_hit_t2':              result.p_hit_t2,
            'p_stopped_out':         result.p_stopped_out,
            'p_expired':             result.p_expired,
            'expected_value_gbp':    result.expected_value_gbp,
            'ci_90_low':             result.ci_90_low,
            'ci_90_high':            result.ci_90_high,
            'days_to_target_median': result.days_to_target_median,
            'regime_adjustment_pct': result.regime_adjustment_pct,
            'iv_adjustment_pct':     result.iv_adjustment_pct,
            'macro_adjustment_pct':  result.macro_adjustment_pct,
            'short_adjustment_pct':  result.short_adjustment_pct,
            'calibration_samples':   result.calibration_samples,
            'used_prior':            result.used_prior,
            'generated_at':          result.generated_at,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/causal/shocks', methods=['GET'])
@require_auth
def causal_shocks():
    """
    GET /causal/shocks?n=50

    Returns the n most recent causal shock propagation events from the
    in-memory shock log. Shows what macro event triggered what, which tickers
    were affected, and how many atoms were written.
    """
    if _shock_engine is None:
        return jsonify({'shocks': [], 'note': 'shock_engine_not_initialised'})
    try:
        n      = min(int(request.args.get('n', 50)), 200)
        shocks = _shock_engine.get_recent_shocks(n=n)
        return jsonify({'shocks': shocks, 'count': len(shocks)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/signals/stress-test', methods=['POST'])
@require_auth
def signals_stress_test():
    """
    POST /signals/stress-test
    Body: { "ticker": "HSBA.L", "pattern_id": 42 }

    Runs signal-level adversarial testing on a specific open pattern signal.
    Returns survival_rate, invalidating_scenarios, robustness_label.
    """
    try:
        from analytics.adversarial_tester import AdversarialTester
        from users.user_store import get_open_patterns
        data       = request.get_json(silent=True) or {}
        ticker     = data.get('ticker', '')
        pattern_id = data.get('pattern_id')
        if not ticker:
            return jsonify({'error': 'ticker required'}), 400

        patterns = get_open_patterns(_DB_PATH, min_quality=0.0, limit=500)
        pattern  = None
        for p in patterns:
            if p['ticker'].upper() == ticker.upper():
                if pattern_id is None or p['id'] == pattern_id:
                    pattern = p
                    break
        if pattern is None:
            return jsonify({'error': 'no open pattern found for ticker'}), 404

        tester = AdversarialTester(_DB_PATH)
        result = tester.stress_test_signal(ticker, pattern)
        return jsonify({
            'ticker':                 ticker.upper(),
            'pattern_type':           pattern.get('pattern_type'),
            'survival_rate':          result.survival_rate,
            'robustness_label':       result.robustness_label,
            'invalidating_scenarios': result.invalidating_scenarios,
            'earnings_warning':       result.earnings_proximity_warning,
            'scenarios_tested':       result.scenarios_tested,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/network/convergence', methods=['GET'])
@require_auth
def network_convergence():
    """
    GET /network/convergence?lookback_hours=24

    Returns tickers where >= 3 independent users have queried organically
    (pre-tip lookback window — post-tip traffic excluded).
    """
    try:
        from analytics.network_effect_engine import NetworkEffectEngine
        lookback = int(request.args.get('lookback_hours', 24))
        engine   = NetworkEffectEngine(_DB_PATH)
        signals  = engine.detect_convergence(lookback_hours=lookback)
        return jsonify({
            'convergence_signals': [
                {
                    'ticker':          s.ticker,
                    'distinct_users':  s.distinct_users,
                    'lookback_hours':  s.lookback_hours,
                    'kb_signal':       s.kb_signal_direction,
                    'organic':         s.is_organic,
                    'detected_at':     s.detected_at,
                }
                for s in signals
            ],
            'count': len(signals),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/ledger/open', methods=['GET'])
@require_auth
def ledger_open():
    """
    GET /ledger/open

    Returns all open (unresolved) prediction ledger entries.
    Useful for monitoring live predictions.
    """
    if _prediction_ledger is None:
        return jsonify({'error': 'prediction_ledger_not_initialised'}), 503
    try:
        predictions = _prediction_ledger.get_open_predictions()
        return jsonify({'predictions': predictions, 'count': len(predictions)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/thesis', methods=['GET'])
@require_auth
def thesis_list():
    """
    GET /thesis

    List all theses stored for the authenticated user.
    """
    try:
        from knowledge.thesis_builder import ThesisBuilder
        user_id = g.get('user_id') or request.args.get('user_id', '')
        if not user_id:
            return jsonify({'error': 'user_id required'}), 400
        builder = ThesisBuilder(_DB_PATH)
        theses  = builder.list_user_theses(user_id)
        return jsonify({'theses': theses, 'count': len(theses)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/thesis/build', methods=['POST'])
@require_auth
def thesis_build():
    """
    POST /thesis/build
    Body: { "ticker": "HSBA.L", "premise": "...", "direction": "bullish" }

    Build a formal thesis from a natural language premise.
    Evaluates KB evidence, derives invalidation condition, stores as KB atoms.
    """
    try:
        from knowledge.thesis_builder import ThesisBuilder
        data      = request.get_json(silent=True) or {}
        ticker    = data.get('ticker', '').strip()
        premise   = data.get('premise', '').strip()
        direction = data.get('direction', 'bullish').strip().lower()
        user_id   = g.get('user_id') or data.get('user_id', '')

        if not ticker or not premise:
            return jsonify({'error': 'ticker and premise are required'}), 400
        if direction not in ('bullish', 'bearish'):
            return jsonify({'error': 'direction must be bullish or bearish'}), 400

        builder = ThesisBuilder(_DB_PATH)
        result  = builder.build(
            ticker    = ticker,
            premise   = premise,
            direction = direction,
            user_id   = user_id,
        )
        return jsonify({
            'thesis_id':              result.thesis_id,
            'ticker':                 result.ticker,
            'direction':              result.direction,
            'thesis_status':          result.thesis_status,
            'thesis_score':           result.thesis_score,
            'supporting_evidence':    result.supporting_evidence,
            'contradicting_evidence': result.contradicting_evidence,
            'invalidation_condition': result.invalidation_condition,
            'created_at':             result.created_at,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/thesis/<thesis_id>', methods=['GET'])
@require_auth
def thesis_get(thesis_id: str):
    """
    GET /thesis/<thesis_id>

    Retrieve a stored thesis with its current evidence evaluation.
    """
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder    = ThesisBuilder(_DB_PATH)
        evaluation = builder.evaluate(thesis_id)
        if evaluation is None:
            return jsonify({'error': 'thesis not found'}), 404
        return jsonify({
            'thesis_id':    evaluation.thesis_id,
            'ticker':       evaluation.ticker,
            'status':       evaluation.status,
            'score':        evaluation.score,
            'supporting':   evaluation.supporting,
            'contradicting':evaluation.contradicting,
            'evaluated_at': evaluation.evaluated_at,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/thesis/<thesis_id>/check', methods=['POST'])
@require_auth
def thesis_check(thesis_id: str):
    """
    POST /thesis/<thesis_id>/check

    Force re-evaluation of a stored thesis against current KB state.
    Updates thesis_status in thesis_index.
    """
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder    = ThesisBuilder(_DB_PATH)
        evaluation = builder.evaluate(thesis_id)
        if evaluation is None:
            return jsonify({'error': 'thesis not found'}), 404
        return jsonify({
            'thesis_id':    evaluation.thesis_id,
            'ticker':       evaluation.ticker,
            'status':       evaluation.status,
            'score':        evaluation.score,
            'supporting':   evaluation.supporting,
            'contradicting':evaluation.contradicting,
            'evaluated_at': evaluation.evaluated_at,
            'note':         'thesis re-evaluated against current KB state',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Frontend ──────────────────────────────────────────────────────────────────

@app.route('/')
def serve_frontend():
    from flask import send_from_directory, make_response
    import os as _os
    resp = make_response(send_from_directory(
        _os.path.join(_os.path.dirname(__file__), 'static'), 'index.html'
    ))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


# ── Entry point ───────────────────────────────────────────────────────────────

def _tg_poll_loop():
    """Background thread: long-poll Telegram getUpdates to handle /start <code> logins."""
    import time as _time, json as _json
    import urllib.request as _ur
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not bot_token:
        return
    offset = 0
    url_base = f'https://api.telegram.org/bot{bot_token}'

    def _send(chat_id, text):
        try:
            payload = _json.dumps({'chat_id': chat_id, 'text': text}).encode()
            req = _ur.Request(f'{url_base}/sendMessage', data=payload,
                              headers={'Content-Type': 'application/json'})
            _ur.urlopen(req, timeout=5)
        except Exception:
            pass

    while True:
        try:
            req = _ur.Request(f'{url_base}/getUpdates?timeout=30&offset={offset}')
            resp = _ur.urlopen(req, timeout=35)
            data = _json.loads(resp.read())
            for upd in data.get('result', []):
                offset = upd['update_id'] + 1
                msg = upd.get('message', {})
                text = (msg.get('text') or '').strip()
                chat_id = (msg.get('chat') or {}).get('id')
                from_user = msg.get('from', {})
                if not chat_id or not text.startswith('/start'):
                    continue
                parts = text.split(maxsplit=1)
                code = parts[1].strip().upper() if len(parts) > 1 else ''
                if code and code in _TG_LOGIN_CODES:
                    entry = _TG_LOGIN_CODES[code]
                    if _time.time() < entry['expires']:
                        entry['chat_id'] = chat_id
                        entry['user_data'] = {
                            'id':         chat_id,
                            'first_name': from_user.get('first_name', ''),
                            'last_name':  from_user.get('last_name', ''),
                            'username':   from_user.get('username', ''),
                        }
                        _send(chat_id, '✅ Logged in! Return to the Trading Galaxy dashboard and click Verify.')
                    else:
                        _send(chat_id, '⚠️ That login code has expired. Please request a new one.')
                else:
                    _send(chat_id, '👋 Welcome to Trading Galaxy! Use the Sign in button on the dashboard to get a login code.')
        except Exception:
            _time.sleep(5)


# ── Waitlist ───────────────────────────────────────────────────────────────────

def _ensure_waitlist_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS waitlist (
            email     TEXT PRIMARY KEY,
            joined_at TEXT NOT NULL DEFAULT (datetime('now')),
            source    TEXT DEFAULT 'landing'
        )
    """)
    conn.commit()


def _notify_waitlist_telegram(email: str) -> None:
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    chat_id   = os.environ.get('WAITLIST_TELEGRAM_CHAT_ID', '')
    if not bot_token or not chat_id:
        return
    try:
        import urllib.request as _ur2
        import json as _j2
        payload = _j2.dumps({
            'chat_id': chat_id,
            'text': f'🚀 New waitlist signup: {email}',
        }).encode()
        req = _ur2.Request(
            f'https://api.telegram.org/bot{bot_token}/sendMessage',
            data=payload,
            headers={'Content-Type': 'application/json'},
        )
        _ur2.urlopen(req, timeout=5)
    except Exception:
        pass


@app.route('/waitlist', methods=['POST'])
@limiter.limit('3 per hour')
def waitlist_join():
    """POST /waitlist — add an email to the beta waitlist."""
    try:
        data  = request.get_json(silent=True) or {}
        email = data.get('email', '').strip().lower()
        if not email or '@' not in email or len(email) > 254:
            return jsonify({'error': 'Invalid email'}), 400
        source = str(data.get('source', 'landing'))[:64]
        import sqlite3 as _sq_wl
        conn = _sq_wl.connect(_DB_PATH, timeout=5)
        _ensure_waitlist_table(conn)
        cur = conn.execute(
            "INSERT OR IGNORE INTO waitlist (email, joined_at, source) VALUES (?, datetime('now'), ?)",
            (email, source),
        )
        conn.commit()
        already = cur.rowcount == 0
        conn.close()
        if not already:
            _notify_waitlist_telegram(email)
        msg = "You're already on the list" if already else "You're on the list"
        return jsonify({'message': msg, 'already': already}), 200
    except Exception as e:
        _logger.error('waitlist_join error: %s', e)
        return jsonify({'error': 'Something went wrong'}), 500


@app.route('/waitlist/count', methods=['GET'])
def waitlist_count():
    """GET /waitlist/count — public signup count for landing page social proof."""
    try:
        import sqlite3 as _sq_wl
        conn = _sq_wl.connect(_DB_PATH, timeout=5)
        _ensure_waitlist_table(conn)
        row = conn.execute('SELECT COUNT(*) FROM waitlist').fetchone()
        conn.close()
        return jsonify({'count': row[0] if row else 0})
    except Exception:
        return jsonify({'count': 0})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    import threading
    if HAS_LLM:
        threading.Thread(target=warmup, daemon=True).start()
    threading.Thread(target=_tg_poll_loop, daemon=True, name='tg-poll').start()
    app.run(host='0.0.0.0', port=port, debug=False)

"""
extensions.py — Single source of truth for feature flags, shared objects, and imports.

All route Blueprint files import from here instead of re-doing try/except ImportError guards.
Evaluated once at startup.
"""

from __future__ import annotations

import logging
import os
import sqlite3

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import HTTPException
from knowledge import KnowledgeGraph
from knowledge.decay import get_decay_worker
from retrieval import retrieve

_logger = logging.getLogger(__name__)

# ── Database ──────────────────────────────────────────────────────────────────

DB_PATH = os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')
AUTH_DB_PATH = os.environ.get(
    'TRADING_AUTH_DB',
    os.path.join(os.path.dirname(DB_PATH), 'auth.db')
    if os.path.dirname(DB_PATH) else 'auth.db'
)

# Enable WAL mode for better concurrent read/write performance
# (paper agent + ingest adapters + live requests all write concurrently)
try:
    _wal_conn = sqlite3.connect(DB_PATH, timeout=5)
    _wal_conn.execute('PRAGMA journal_mode=WAL')
    _wal_conn.close()
except Exception as _e:
    _logger.warning('Failed to enable WAL mode: %s', _e)

# Force WAL checkpoint at startup to reclaim WAL file bloat
# Safe to run even with concurrent readers — uses PASSIVE mode
try:
    _ckpt_conn = sqlite3.connect(DB_PATH, timeout=5)
    _ckpt_conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
    _ckpt_conn.close()
except Exception:
    pass

# Initialise auth DB (separate file — bot threads can never block logins)
try:
    _auth_conn = sqlite3.connect(AUTH_DB_PATH, timeout=5)
    _auth_conn.execute('PRAGMA journal_mode=WAL')
    _auth_conn.execute('PRAGMA busy_timeout=30000')
    _auth_conn.close()
except Exception as _e:
    _logger.warning('Failed to initialise auth DB: %s', _e)

# ── PostgreSQL (hot tables) ────────────────────────────────────────────────────
PG_DSN = os.environ.get("PG_DSN", "")
HAS_POSTGRES = bool(PG_DSN)
if HAS_POSTGRES:
    try:
        from db import _init_pool
        _init_pool()
        _logger.info("PostgreSQL pool initialised")
    except Exception as _pg_e:
        _logger.warning("PostgreSQL pool failed: %s — falling back to SQLite", _pg_e)
        HAS_POSTGRES = False

kg = KnowledgeGraph(db_path=DB_PATH)
decay_worker = get_decay_worker(DB_PATH)

# ── LLM ───────────────────────────────────────────────────────────────────────

try:
    from llm.ollama_client import chat as ollama_chat, list_models, is_available, warmup, DEFAULT_MODEL
    from llm.prompt_builder import build as build_prompt
    HAS_LLM = True
except ImportError:
    HAS_LLM = False
    ollama_chat = None  # type: ignore
    list_models = None  # type: ignore
    is_available = None  # type: ignore
    warmup = None  # type: ignore
    DEFAULT_MODEL = None  # type: ignore
    build_prompt = None  # type: ignore

try:
    from llm.groq_client import chat as groq_chat, is_available as groq_available
    HAS_GROQ = True
except ImportError:
    HAS_GROQ = False
    groq_chat = None  # type: ignore
    groq_available = None  # type: ignore

_llm_logger = logging.getLogger('llm.token_count')

def llm_chat(messages, model=None, **kwargs):
    """Unified LLM chat: prefer Groq (fast, free API) over local Ollama."""
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

# ── Working Memory ────────────────────────────────────────────────────────────

try:
    from knowledge.working_memory import WorkingMemory, kb_has_atoms, MAX_ON_DEMAND_TICKERS
    working_memory = WorkingMemory()
    HAS_WORKING_MEMORY = True
except ImportError:
    HAS_WORKING_MEMORY = False
    working_memory = None  # type: ignore

# ── Ingest ────────────────────────────────────────────────────────────────────

try:
    from ingest.scheduler import IngestScheduler
    HAS_INGEST = True
except ImportError:
    HAS_INGEST = False

# ── Analytics ─────────────────────────────────────────────────────────────────

try:
    from analytics.backtest import (
        run_backtest, take_snapshot, list_snapshots,
        run_regime_backtest, list_snapshot_regimes,
    )
    from analytics.portfolio import build_portfolio_summary
    from analytics.alerts import AlertMonitor, get_alerts, mark_alerts_seen
    from analytics.adversarial_stress import run_stress_test, _SCENARIOS as STRESS_SCENARIOS
    from analytics.counterfactual import run_counterfactual
    HAS_ANALYTICS = True
except ImportError:
    HAS_ANALYTICS = False

# ── Product Layer (users, notifications, delivery) ────────────────────────────

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

# ── Pattern Layer (tips, patterns, position calc) ─────────────────────────────

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

# ── Overlay ───────────────────────────────────────────────────────────────────

try:
    from llm.overlay_builder import extract_tickers as extract_overlay_tickers
    from llm.overlay_builder import build_overlay_cards
    HAS_OVERLAY = True
except ImportError:
    HAS_OVERLAY = False

# ── Epistemic Stress ──────────────────────────────────────────────────────────

try:
    from knowledge.epistemic_stress import compute_stress
    HAS_STRESS = True
except ImportError:
    HAS_STRESS = False

# ── Confidence Intervals ──────────────────────────────────────────────────────

try:
    from knowledge.confidence_intervals import (
        ensure_confidence_columns,
        get_confidence_interval,
        get_all_confidence_intervals,
    )
    HAS_CONF_INTERVALS = True
except ImportError:
    HAS_CONF_INTERVALS = False

# ── Causal Graph ──────────────────────────────────────────────────────────────

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

# ── Epistemic Adaptation ──────────────────────────────────────────────────────

try:
    from knowledge.epistemic_adaptation import get_adaptation_engine
    HAS_ADAPTATION = True
except ImportError:
    HAS_ADAPTATION = False

# ── Working State ─────────────────────────────────────────────────────────────

try:
    from knowledge.working_state import get_working_state_store
    HAS_WORKING_STATE = True
except ImportError:
    HAS_WORKING_STATE = False

# ── Conversation Store ────────────────────────────────────────────────────────

try:
    from knowledge.conversation_store import ConversationStore, session_id_for_user
    HAS_CONV_STORE = True
except ImportError:
    HAS_CONV_STORE = False

# ── KB Insufficiency Classifier ───────────────────────────────────────────────

try:
    from knowledge.kb_insufficiency_classifier import classify_insufficiency
    HAS_CLASSIFIER = True
except ImportError:
    HAS_CLASSIFIER = False

# ── KB Repair ─────────────────────────────────────────────────────────────────

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

# ── KB Validation ─────────────────────────────────────────────────────────────

try:
    from knowledge.kb_validation import validate_all, governance_verdict
    HAS_VALIDATION = True
except ImportError:
    HAS_VALIDATION = False

# ── Graph Retrieval ───────────────────────────────────────────────────────────

try:
    from knowledge.graph_retrieval import build_graph_context, what_do_i_know_about
    HAS_GRAPH_RETRIEVAL = True
except ImportError:
    HAS_GRAPH_RETRIEVAL = False

# ── Hybrid Build (universe, personal KB, network effects) ────────────────────

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

# ── Middleware ────────────────────────────────────────────────────────────────

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
    from core.tiers import get_tier, check_feature, _next_tier as next_tier_name
    HAS_TIERS = True
except ImportError:
    HAS_TIERS = False

# ── Thesis Builder ────────────────────────────────────────────────────────────

try:
    from knowledge.thesis_builder import ThesisMonitor
    HAS_THESIS = True
except ImportError:
    HAS_THESIS = False

# ── Tier feature guard ────────────────────────────────────────────────────────

def get_user_tier_for_request(user_id: str) -> str:
    """Fetch the tier for user_id from the DB. Defaults to 'basic'."""
    try:
        if HAS_PATTERN_LAYER:
            return get_user_tier(DB_PATH, user_id)
    except Exception:
        pass
    return 'basic'


def require_feature(feature: str, user_id: str | None = None):
    """
    FastAPI dependency factory: gate an endpoint by tier feature.
    Usage: Depends(require_feature('feature_name')).
    Raises HTTP 403 with upgrade_required payload when feature not available.
    """
    def dependency(uid: str | None = user_id):
        if not HAS_TIERS:
            return
        tier = get_user_tier_for_request(uid) if uid else 'basic'
        if not check_feature(tier, feature):
            next_t = next_tier_name(tier)
            raise HTTPException(status_code=403, detail={
                'error':        'upgrade_required',
                'feature':      feature,
                'current_tier': tier,
                'upgrade_to':   next_t,
                'message':      f'This feature requires {next_t} or above',
            })
    return dependency


# ── Shared mutable state (initialised by create_app) ─────────────────────────
# These are set in api.py after all engines are initialised.

ingest_scheduler = None
discovery_pipeline = None
delivery_scheduler = None
tip_scheduler = None
position_monitor = None
shock_engine = None
try:
    from analytics.prediction_ledger import PredictionLedger as _PredictionLedger
    prediction_ledger = _PredictionLedger(DB_PATH)
    _logger.info('PredictionLedger initialized in extensions.py')
except Exception as _pl_init_e:
    prediction_ledger = None
    _logger.warning('PredictionLedger init failed in extensions.py: %s', _pl_init_e)
thesis_monitor = None
conv_store = None
seed_sync = None

# Per-session state — thread-safe SessionManager (Phase 2)
from services.session import SessionManager
sessions = SessionManager()

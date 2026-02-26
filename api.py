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
import re
from datetime import datetime, timezone
from typing import List, Optional

from flask import Flask, g, request, jsonify

try:
    from llm.ollama_client import chat as ollama_chat, list_models, is_available, DEFAULT_MODEL
    from llm.prompt_builder import build as build_prompt
    HAS_LLM = True
except ImportError:
    HAS_LLM = False

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


# ── App setup ─────────────────────────────────────────────────────────────────

app = Flask(__name__)

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

# Per-session streak store for epistemic adaptation
# { session_id: { 'streak': int, 'last_stress': float } }
_session_streaks: dict = {}

# Per-session last-seen tickers for follow-up carry-forward
# { session_id: [list of canonical ticker strings] }
_session_tickers: dict = {}

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
        _ingest_scheduler.register(FREDAdapter(),                             interval_sec=86400)  # 24 hours
        _ingest_scheduler.start()
    except Exception as _e:
        import logging as _logging
        _logging.getLogger(__name__).error('Failed to start ingest scheduler: %s', _e)
        _ingest_scheduler = None


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

    return jsonify({
        'scheduler': 'running',
        'adapters': _ingest_scheduler.get_status(),
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


@app.route('/stats', methods=['GET'])
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

    return jsonify({**base, **extras})


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

@app.route('/chat', methods=['POST'])
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
    chat_user_id    = data.get('user_id') or None

    # Auto-boost limit for portfolio-wide queries so all holdings get KB atoms
    _PORTFOLIO_KEYWORDS = ('portfolio', 'holdings', 'positions', 'my stocks',
                           'discuss my', 'analyse my', 'analyze my', 'review my')
    if chat_user_id and any(kw in message.lower() for kw in _PORTFOLIO_KEYWORDS):
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

    _retrieve_message = message
    if not _cur_tickers and session_id in _session_tickers:
        _prev = _session_tickers[session_id]
        if _prev:
            _retrieve_message = message + ' ' + ' '.join(_prev)

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
    if HAS_WORKING_MEMORY and _working_memory is not None and len(atoms) < 5:
        try:
            from retrieval import _extract_tickers
            tickers_in_query = _extract_tickers(message)
            missing = [
                t for t in tickers_in_query[:MAX_ON_DEMAND_TICKERS]
                if not kb_has_atoms(t, _DB_PATH)
            ]
            if missing:
                _working_memory.open_session(wm_session_id)
                for ticker in missing:
                    _working_memory.fetch_on_demand(ticker, wm_session_id, _DB_PATH)
                live_context = _working_memory.get_session_snippet(wm_session_id)
                live_fetched = _working_memory.get_fetched_tickers(wm_session_id)
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
    portfolio_context = None
    if chat_user_id and HAS_PRODUCT_LAYER:
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
        except Exception:
            portfolio_context = None

    # ── Pass 1: LLM-initiated data request (only when KB is thin) ────────
    # If we already fetched live data (live_fetched), skip pass 1 — we have
    # what we need.  If KB had plenty of atoms, skip pass 1 too.
    llm_requested_tickers: list = []
    web_searched: str | None = None
    if (HAS_WORKING_MEMORY and _working_memory is not None
            and not live_fetched and len(atoms) < 8):
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
            _p1_raw = ollama_chat(_p1_messages, model=model)
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
    )

    answer = ollama_chat(messages, model=model)
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
        prefs = update_preferences(
            _DB_PATH, user_id,
            selected_sectors=data.get('selected_sectors'),
            selected_risk=data.get('risk_tolerance'),
            telegram_chat_id=data.get('telegram_chat_id'),
            delivery_time=data.get('delivery_time'),
            timezone_str=data.get('timezone'),
            onboarding_complete=1,
        )
        log_audit_event(_DB_PATH, action='onboarding_update', user_id=user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
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
            """SELECT tier, tip_timeframes, tip_pattern_types,
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
    cols     = ['tier', 'tip_timeframes', 'tip_pattern_types',
                'account_size', 'max_risk_per_trade_pct', 'account_currency']
    prefs    = dict(zip(cols, row))
    tier     = prefs.get('tier') or 'basic'

    for jcol in ('tip_timeframes', 'tip_pattern_types'):
        try:
            prefs[jcol] = _json.loads(prefs[jcol]) if prefs[jcol] else None
        except Exception:
            prefs[jcol] = None

    from notifications.tip_formatter import TIER_LIMITS
    limits          = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    tip_timeframes  = prefs.get('tip_timeframes') or limits['timeframes']
    tip_pattern_tys = prefs.get('tip_pattern_types')

    from notifications.tip_scheduler import _pick_best_pattern
    pattern_row = _pick_best_pattern(_DB_PATH, user_id, tier, tip_timeframes, tip_pattern_tys)
    if pattern_row is None:
        return jsonify({'tip': None, 'reason': 'no eligible patterns'}), 200

    from analytics.pattern_detector import PatternSignal
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
    return jsonify({'tip': tip_dict})


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
                          account_size, max_risk_per_trade_pct, account_currency
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
                'account_size', 'max_risk_per_trade_pct', 'account_currency']
        d = dict(zip(cols, row))
        for jcol in ('tip_markets', 'tip_timeframes', 'tip_pattern_types'):
            try:
                d[jcol] = _json.loads(d[jcol]) if d[jcol] else None
            except Exception:
                d[jcol] = None
        return jsonify(d)

    # POST
    data = request.get_json(force=True, silent=True) or {}
    if HAS_VALIDATORS:
        vr = validate_tip_config(data)
        if not vr.valid:
            return jsonify({'error': 'validation_failed', 'details': vr.errors}), 400
    try:
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
        return jsonify(updated)
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
    return jsonify(token_data)


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
    if not refresh_token:
        return jsonify({'error': 'refresh_token is required'}), 400
    try:
        result = rotate_refresh_token(_DB_PATH, refresh_token)
        log_audit_event(_DB_PATH, action='token_refresh', user_id=result['user_id'],
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
        return jsonify(result)
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
    return jsonify({'logged_out': True})


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
    if not HAS_PRODUCT_LAYER:
        return jsonify({'user_id': user_id})
    try:
        user = get_user(_DB_PATH, user_id)
        return jsonify(user or {'user_id': user_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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

    return jsonify(result)


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
    user_id = str(data.get('user_id', '')).strip()
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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    app.run(host='0.0.0.0', port=port, debug=False)

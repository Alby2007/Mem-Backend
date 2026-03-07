"""
api.py — Trading KB REST API

Slim create_app() factory. All route handlers live in routes/*.py;
shared objects and feature flags live in extensions.py.
"""

from __future__ import annotations

import logging
import os
import pathlib
import sqlite3

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask

import extensions as ext
from routes import register_blueprints

_logger = logging.getLogger(__name__)


# ── Application factory ──────────────────────────────────────────────────────

def create_app() -> Flask:
    app = Flask(__name__)

    # ── CORS ──────────────────────────────────────────────────────────────
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

    # ── Rate limiter ──────────────────────────────────────────────────────
    if ext.HAS_LIMITER:
        ext.limiter.init_app(app)

    # ── DB table bootstrapping ────────────────────────────────────────────
    _ensure_tables()

    # ── Intelligence layer hooks ──────────────────────────────────────────
    _init_intelligence_hooks()

    # ── ConversationStore ─────────────────────────────────────────────────
    if ext.HAS_CONV_STORE:
        try:
            from knowledge.conversation_store import ConversationStore
            ext.conv_store = ConversationStore(ext.DB_PATH)
        except Exception as e:
            _logger.warning('ConversationStore init failed: %s', e)

    # ── Auto-seed on first boot ───────────────────────────────────────────
    _auto_seed()

    # ── Background workers ────────────────────────────────────────────────
    _start_workers()

    # ── Register all Blueprints ───────────────────────────────────────────
    register_blueprints(app)

    return app


# ── DB table bootstrapping ────────────────────────────────────────────────────

def _ensure_tables():
    """Ensure all required DB tables exist (idempotent)."""
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)

        if ext.HAS_CONF_INTERVALS:
            try:
                ext.ensure_confidence_columns(conn)
            except Exception:
                pass

        if ext.HAS_CAUSAL_GRAPH:
            try:
                ext.ensure_causal_edges_table(conn)
            except Exception:
                pass

        if ext.HAS_PRODUCT_LAYER:
            try:
                ext.ensure_user_tables(conn)
            except Exception:
                pass

        if ext.HAS_AUTH:
            try:
                ext.ensure_user_auth_table(conn)
            except Exception:
                pass

        if ext.HAS_AUDIT:
            try:
                ext.ensure_audit_table(conn)
            except Exception:
                pass

        if ext.HAS_HYBRID:
            try:
                ext.ensure_hybrid_tables(conn)
            except Exception:
                pass

        try:
            from services.paper_trading import ensure_paper_tables
            ensure_paper_tables(conn)
        except Exception:
            pass

        conn.commit()
        conn.close()
    except Exception as e:
        _logger.error('Table bootstrapping failed: %s', e)


# ── Intelligence layer hooks ──────────────────────────────────────────────────

def _init_intelligence_hooks():
    """Attach CausalShockEngine, PredictionLedger, ThesisMonitor to the KG."""
    try:
        from analytics.causal_shock_engine import CausalShockEngine
        ext.shock_engine = CausalShockEngine(ext.DB_PATH)
        ext.kg.set_shock_engine(ext.shock_engine)
    except Exception as e:
        _logger.warning('CausalShockEngine init failed: %s', e)

    try:
        from analytics.prediction_ledger import PredictionLedger
        ext.prediction_ledger = PredictionLedger(ext.DB_PATH)
        ext.kg.set_ledger(ext.prediction_ledger)
    except Exception as e:
        _logger.warning('PredictionLedger init failed: %s', e)

    try:
        from knowledge.thesis_builder import ThesisMonitor
        ext.thesis_monitor = ThesisMonitor(ext.DB_PATH)
        ext.kg.set_thesis_monitor(ext.thesis_monitor)
    except Exception as e:
        _logger.warning('ThesisMonitor init failed: %s', e)


# ── Auto-seed ─────────────────────────────────────────────────────────────────

def _auto_seed():
    """If the DB is nearly empty and the seed file exists, load it."""
    try:
        seed_path = pathlib.Path(__file__).parent / 'tests' / 'fixtures' / 'kb_seed.sql'
        if not seed_path.exists():
            return
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        try:
            fact_count = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        except Exception:
            fact_count = 0
        finally:
            conn.close()
        if fact_count < 100:
            seed_log = logging.getLogger('api.autoseed')
            seed_log.info('Auto-seeding KB from %s (%d facts) …', seed_path, fact_count)
            conn = sqlite3.connect(ext.DB_PATH, timeout=15)
            try:
                conn.executescript(seed_path.read_text(encoding='utf-8'))
                after = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
                seed_log.info('Auto-seed complete — %d facts loaded.', after)
            except Exception as e:
                seed_log.warning('Auto-seed failed: %s', e)
            finally:
                conn.close()
    except Exception:
        pass


# ── Background workers ────────────────────────────────────────────────────────

def _start_workers():
    """Start all background schedulers and workers."""

    # Seed sync (polls GitHub Releases hourly)
    try:
        from ingest.seed_sync import SeedSyncClient
        ext.seed_sync = SeedSyncClient(db_path=ext.DB_PATH)
        ext.seed_sync.start()
    except Exception:
        pass

    # Delivery scheduler (briefings every 60s)
    if ext.HAS_PRODUCT_LAYER:
        try:
            from notifications.delivery_scheduler import DeliveryScheduler
            ext.delivery_scheduler = DeliveryScheduler(ext.DB_PATH, check_interval_sec=60)
            ext.delivery_scheduler.start()
        except Exception:
            pass

    # Tip scheduler (pattern tips every 60s)
    if ext.HAS_PATTERN_LAYER:
        try:
            from notifications.tip_scheduler import TipScheduler
            ext.tip_scheduler = TipScheduler(ext.DB_PATH, interval_sec=60)
            ext.tip_scheduler.start()
        except Exception:
            pass

    # Position monitor (every 300s)
    try:
        from analytics.position_monitor import PositionMonitor
        ext.position_monitor = PositionMonitor(ext.DB_PATH, interval_sec=300)
        ext.position_monitor.start()
    except Exception:
        pass

    # Ingest scheduler (adapters loaded from registry table)
    if ext.HAS_INGEST:
        try:
            from importlib import import_module
            from ingest.scheduler import IngestScheduler

            # (module_path, class_name, needs_db, interval_sec, env_gate)
            _ADAPTER_REGISTRY = [
                ('ingest.yfinance_adapter',            'YFinanceAdapter',            False,  300,   None),
                ('ingest.signal_enrichment_adapter',   'SignalEnrichmentAdapter',    True,   300,   None),
                ('ingest.rss_adapter',                 'RSSAdapter',                 True,   900,   None),
                ('ingest.llm_extraction_adapter',      'LLMExtractionAdapter',       True,   300,   None),
                ('ingest.edgar_adapter',               'EDGARAdapter',               True,   21600, None),
                ('ingest.edgar_realtime_adapter',      'EDGARRealtimeAdapter',       True,   180,   None),
                ('ingest.options_adapter',             'OptionsAdapter',             False,  1800,  None),
                ('ingest.polygon_options_adapter',     'PolygonOptionsAdapter',      False,  1800,  'POLYGON_API_KEY'),
                ('ingest.yield_curve_adapter',         'YieldCurveAdapter',          False,  86400, 'POLYGON_API_KEY'),
                ('ingest.finra_short_interest_adapter','FINRAShortInterestAdapter',  True,   86400, None),
                ('ingest.fred_adapter',                'FREDAdapter',                False,  86400, None),
                ('ingest.boe_adapter',                 'BoEAdapter',                 False,  86400, None),
                ('ingest.earnings_calendar_adapter',   'EarningsCalendarAdapter',    True,   3600,  None),
                ('ingest.fca_short_interest_adapter',  'FCAShortInterestAdapter',    True,   86400, None),
                ('ingest.lse_flow_adapter',            'LSEFlowAdapter',             True,   3600,  None),
                ('ingest.insider_adapter',             'InsiderAdapter',             True,   3600,  None),
                ('ingest.short_interest_adapter',      'ShortInterestAdapter',       True,   86400, None),
                ('ingest.sector_rotation_adapter',     'SectorRotationAdapter',      True,   3600,  None),
                ('ingest.economic_calendar_adapter',   'EconomicCalendarAdapter',    True,   86400, None),
                ('ingest.eia_adapter',                 'EIAAdapter',                 False,  86400, None),
                ('ingest.gdelt_adapter',               'GDELTAdapter',               False,  3600,  None),
                ('ingest.ucdp_adapter',                'UCDPAdapter',                False,  86400, None),
                ('ingest.acled_adapter',               'ACLEDAdapter',               False,  21600, None),
                ('ingest.usgs_adapter',                'USGSAdapter',                False,  3600,  None),
            ]

            sched = IngestScheduler(ext.kg)
            for mod_path, cls_name, needs_db, interval, env_key in _ADAPTER_REGISTRY:
                if env_key and not os.environ.get(env_key):
                    continue
                try:
                    mod = import_module(mod_path)
                    cls = getattr(mod, cls_name)
                    adapter = cls(db_path=ext.DB_PATH) if needs_db else cls()
                    sched.register(adapter, interval_sec=interval)
                except Exception as _ae:
                    _logger.warning('Adapter %s.%s failed: %s', mod_path, cls_name, _ae)

            # Paper agent (autonomous trading every 30 min)
            try:
                from services.paper_trading import PaperAgentAdapter
                sched.register(PaperAgentAdapter(), interval_sec=1800)
            except Exception as _pae:
                _logger.warning('PaperAgentAdapter registration failed: %s', _pae)

            sched.start()
            ext.ingest_scheduler = sched
        except Exception as e:
            _logger.error('Failed to start ingest scheduler: %s', e)

    # Discovery pipeline
    if ext.HAS_INGEST:
        try:
            from ingest.discovery_pipeline import DiscoveryPipeline
            ext.discovery_pipeline = DiscoveryPipeline(kg=ext.kg, db_path=ext.DB_PATH)
        except Exception as e:
            _logger.error('Failed to init discovery pipeline: %s', e)


# ── Telegram poll loop (dev/fallback when webhook is not configured) ──────────

def _tg_poll_loop():
    """Background thread: long-poll Telegram getUpdates to handle /start <code> logins."""
    import json as _json
    import time as _time
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
                from routes.auth import _TG_LOGIN_CODES
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


# ── Module-level app for gunicorn / production WSGI servers ──────────────────
# gunicorn picks up `app` from `api:app`

app = create_app()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    import threading
    if ext.HAS_LLM:
        threading.Thread(target=ext.warmup, daemon=True).start()
    threading.Thread(target=_tg_poll_loop, daemon=True, name='tg-poll').start()
    app.run(host='0.0.0.0', port=port, debug=False)

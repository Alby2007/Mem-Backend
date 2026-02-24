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
from typing import List, Optional

from flask import Flask, request, jsonify

from knowledge import KnowledgeGraph
from knowledge.decay import get_decay_worker
from retrieval import retrieve

try:
    from ingest.scheduler import IngestScheduler
    from ingest.yfinance_adapter import YFinanceAdapter
    from ingest.fred_adapter import FREDAdapter
    from ingest.edgar_adapter import EDGARAdapter
    from ingest.rss_adapter import RSSAdapter
    HAS_INGEST = True
except ImportError:
    HAS_INGEST = False

try:
    from knowledge.epistemic_stress import compute_stress
    HAS_STRESS = True
except ImportError:
    HAS_STRESS = False

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


# ── App setup ─────────────────────────────────────────────────────────────────

app = Flask(__name__)

_DB_PATH = os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')
_kg = KnowledgeGraph(db_path=_DB_PATH)

# Start background decay worker (runs every 24h)
_decay_worker = get_decay_worker(_DB_PATH)

# Start ingest scheduler (adapters run on their own intervals)
_ingest_scheduler = None
if HAS_INGEST:
    try:
        _ingest_scheduler = IngestScheduler(_kg)
        _ingest_scheduler.register(YFinanceAdapter(),  interval_sec=900)    # 15 min
        _ingest_scheduler.register(RSSAdapter(),       interval_sec=1800)   # 30 min
        _ingest_scheduler.register(EDGARAdapter(),     interval_sec=21600)  # 6 hours
        _ingest_scheduler.register(FREDAdapter(),      interval_sec=86400)  # 24 hours
        _ingest_scheduler.start()
    except Exception as _e:
        import logging as _logging
        _logging.getLogger(__name__).error('Failed to start ingest scheduler: %s', _e)
        _ingest_scheduler = None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
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


@app.route('/stats', methods=['GET'])
def stats():
    return jsonify(_kg.get_stats())


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
    for atom in atoms:
        subject   = atom.get('subject')
        predicate = atom.get('predicate')
        obj       = atom.get('object')
        if not (subject and predicate and obj):
            skipped += 1
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

    snippet, atoms = retrieve(message, conn)

    response: dict = {
        'snippet': snippet,
        'atoms':   atoms,
    }
    if prior_context:
        response['prior_context'] = prior_context

    # Attach epistemic stress if available
    if HAS_STRESS and atoms:
        try:
            words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)
            key_terms = list({w.lower() for w in words if len(w) > 2})[:10]
            stress = compute_stress(atoms, key_terms, conn)
            response['stress'] = {
                'composite_stress':    stress.composite_stress,
                'decay_pressure':      stress.decay_pressure,
                'authority_conflict':  stress.authority_conflict,
                'supersession_density': stress.supersession_density,
                'conflict_cluster':    stress.conflict_cluster,
                'domain_entropy':      stress.domain_entropy,
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


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    app.run(host='0.0.0.0', port=port, debug=False)

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
        _ingest_scheduler.register(YFinanceAdapter(),  interval_sec=300)    # 5 min
        _ingest_scheduler.register(RSSAdapter(),       interval_sec=900)    # 15 min
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


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    app.run(host='0.0.0.0', port=port, debug=False)

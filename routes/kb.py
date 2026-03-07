"""routes/kb.py — Knowledge Base endpoints: ingest, query, retrieve, search, context, repair, adapt, graph."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('kb', __name__)


# ── Core CRUD ─────────────────────────────────────────────────────────────────

@bp.route('/ingest', methods=['POST'])
def ingest():
    """Ingest one or more atoms into the KB."""
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
            if is_single:
                return jsonify({'error': 'subject, predicate and object are all required'}), 400
            continue
        ok = ext.kg.add_fact(
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


@bp.route('/query', methods=['GET'])
def query():
    """Direct triple-store query with optional filters."""
    subject   = request.args.get('subject')
    predicate = request.args.get('predicate')
    obj       = request.args.get('object')
    limit     = int(request.args.get('limit', 50))

    results = ext.kg.query(subject=subject, predicate=predicate, object=obj, limit=limit)
    return jsonify({'results': results, 'count': len(results)})


@bp.route('/retrieve', methods=['POST'])
def retrieve_endpoint():
    """Smart multi-strategy retrieval for a natural-language or structured query."""
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'error': 'invalid JSON'}), 400

    message    = data.get('message', '')
    session_id = data.get('session_id', 'default')
    goal       = data.get('goal')
    topic      = data.get('topic')
    turn_count = int(data.get('turn_count', 1))
    limit      = int(data.get('limit', 30))

    conn = ext.kg.thread_local_conn()

    prior_context = None
    if ext.HAS_WORKING_STATE:
        try:
            ws = ext.get_working_state_store(ext.DB_PATH)
            if turn_count == 0:
                prior_context = ws.format_prior_context(session_id) or None
            ws.maybe_persist(
                session_id, turn_count,
                goal=goal, topic=topic,
                force=(turn_count == 1),
            )
        except Exception:
            pass

    # Compute adaptation nudges from prior stress streak
    nudges = None
    if ext.HAS_ADAPTATION and ext.HAS_STRESS:
        try:
            from knowledge.epistemic_adaptation import ensure_adaptation_tables
            ensure_adaptation_tables(conn)
            engine = ext.get_adaptation_engine(session_id, db_path=ext.DB_PATH)
            engine._session_id = session_id
            sess = ext.session_streaks.setdefault(session_id, {'streak': 0, 'last_stress': 0.0})

            class _StateStub:
                pass
            state_stub = _StateStub()
            state_stub.epistemic_stress_streak = sess['streak']
            state_stub._session_id = session_id

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

    snippet, atoms = ext.retrieve(message, conn, limit=limit, nudges=nudges)

    response: dict = {
        'snippet': snippet,
        'atoms':   atoms,
    }
    if prior_context:
        response['prior_context'] = prior_context

    # Attach epistemic stress
    stress_report = None
    if ext.HAS_STRESS and atoms:
        try:
            words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)
            key_terms = list({w.lower() for w in words if len(w) > 2})[:10]
            stress_report = ext.compute_stress(atoms, key_terms, conn)
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

    # Update session streak
    if ext.HAS_ADAPTATION and stress_report:
        try:
            from knowledge.epistemic_adaptation import _STRESS_STREAK_THRESHOLD
            sess = ext.session_streaks.setdefault(session_id, {'streak': 0, 'last_stress': 0.0})
            if stress_report.composite_stress >= _STRESS_STREAK_THRESHOLD:
                sess['streak'] = sess.get('streak', 0) + 1
            else:
                sess['streak'] = max(0, sess.get('streak', 0) - 1)
            sess['last_stress'] = stress_report.composite_stress
        except Exception:
            pass

    # Attach adaptation nudges
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
        if nudges.refresh_domain_queued and ext.ingest_scheduler and topic:
            try:
                ext.ingest_scheduler.run_now('yfinance')
            except Exception:
                pass

    # KB insufficiency classification
    if ext.HAS_CLASSIFIER and stress_report and atoms:
        try:
            _tickers = [t for t in re.findall(r'\b[A-Z]{2,5}\b', message)
                        if t not in {'THE','IS','AT','ON','AN','AND','OR','FOR','IN','OF',
                                     'TO','THAT','THIS','WITH','FROM','BY','ARE','WAS','BE',
                                     'HAS','HAVE','HAD','ITS','DO','DID','WHAT','HOW','WHY',
                                     'WHEN','WHERE','WHO','CAN','WILL','NOT','BUT','ALL'}]
            _terms = [w.lower() for w in re.findall(r'\b[a-zA-Z][a-zA-Z0-9]{2,}\b', message)]
            composite = getattr(stress_report, 'composite_stress', 0.0)
            atom_count = len(atoms)
            if composite > 0.35 or atom_count < 8:
                topic_hint = (topic or
                              (_tickers[0] if _tickers else None) or
                              (_terms[0] if _terms else None) or
                              message[:40])
                diagnosis = ext.classify_insufficiency(topic_hint, stress_report, conn)
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


@bp.route('/search', methods=['GET'])
def search():
    """Full-text search over the KB."""
    q        = request.args.get('q', '')
    category = request.args.get('category')
    limit    = int(request.args.get('limit', 20))

    if not q:
        return jsonify({'error': 'q is required'}), 400

    results = ext.kg.search(q, limit=limit, category=category)
    return jsonify({'results': results, 'count': len(results)})


@bp.route('/context/<entity>', methods=['GET'])
def context(entity: str):
    """Get all facts connected to a specific entity."""
    facts = ext.kg.get_context(entity)
    return jsonify({'entity': entity, 'facts': facts, 'count': len(facts)})


@bp.route('/stats', methods=['GET'])
@ext.limiter.exempt
def stats():
    """KB statistics: fact counts, conflicts, repair proposals, regime, patterns."""
    base = ext.kg.get_stats()
    conn = ext.kg.thread_local_conn()
    c = conn.cursor()
    extras = {}

    try:
        c.execute("SELECT COUNT(*) FROM fact_conflicts")
        extras['total_conflicts_detected'] = c.fetchone()[0]
    except Exception:
        extras['total_conflicts_detected'] = 0

    try:
        c.execute("""
            SELECT subject, predicate, SUM(hit_count) as hits
            FROM facts WHERE hit_count > 0
            GROUP BY subject, predicate ORDER BY hits DESC LIMIT 5
        """)
        extras['top_retrieved_atoms'] = [
            {'subject': r[0], 'predicate': r[1], 'hits': r[2]} for r in c.fetchall()
        ]
    except Exception:
        extras['top_retrieved_atoms'] = []

    try:
        c.execute("SELECT COUNT(*) FROM repair_proposals WHERE status = 'pending'")
        extras['pending_repair_proposals'] = c.fetchone()[0]
    except Exception:
        extras['pending_repair_proposals'] = 0

    try:
        c.execute("SELECT COUNT(*) FROM domain_refresh_queue WHERE processed = 0")
        extras['domain_refresh_queue_depth'] = c.fetchone()[0]
    except Exception:
        extras['domain_refresh_queue_depth'] = 0

    extras['adaptation_sessions_active'] = sum(
        1 for s in ext.session_streaks.values() if s.get('streak', 0) > 0
    )
    extras['adaptation_sessions_total'] = len(ext.session_streaks)

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
        _srrow = c.execute("""
            SELECT object FROM facts
            WHERE predicate IN ('leading_sector','sector_rotation','top_sector')
            ORDER BY timestamp DESC LIMIT 1
        """).fetchone()
        extras['regime_sector_lead'] = _srrow[0] if _srrow else None
    except Exception:
        extras['regime_sector_lead'] = None

    try:
        _total_row = c.execute("SELECT COUNT(*) FROM facts").fetchone()
        _high_row  = c.execute("SELECT COUNT(*) FROM facts WHERE confidence >= 0.7").fetchone()
        _total = _total_row[0] if _total_row else 0
        _high  = _high_row[0]  if _high_row  else 0
        extras['regime_kb_confidence'] = round(_high / _total * 100, 1) if _total > 0 else None
    except Exception:
        extras['regime_kb_confidence'] = None

    try:
        if ext.HAS_PATTERN_LAYER:
            pats = ext.get_open_patterns(ext.DB_PATH, min_quality=0.0, limit=500)
            extras['open_patterns'] = len(pats)
    except Exception:
        pass

    return jsonify({**base, **extras})


# ── Governance / Repair ───────────────────────────────────────────────────────

@bp.route('/repair/diagnose', methods=['POST'])
def repair_diagnose():
    """Run KB insufficiency classification for a topic."""
    if not ext.HAS_CLASSIFIER:
        return jsonify({'error': 'kb_insufficiency_classifier not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    topic = data.get('topic', '').strip()
    if not topic:
        return jsonify({'error': 'topic is required'}), 400

    conn = ext.kg.thread_local_conn()

    class _StressStub:
        conflict_cluster      = 0.0
        supersession_density  = 0.0
        authority_conflict    = 0.0
        domain_entropy        = 1.0

    stress_stub = _StressStub()
    if ext.HAS_STRESS:
        try:
            _, atoms = ext.retrieve(topic, conn, limit=50)
            if atoms:
                stress_stub = ext.compute_stress(atoms, [topic], conn)
        except Exception:
            pass

    try:
        diagnosis = ext.classify_insufficiency(topic, stress_stub, conn)
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


@bp.route('/repair/proposals', methods=['POST'])
def repair_proposals():
    """Generate repair proposals for a topic."""
    if not ext.HAS_PROPOSALS:
        return jsonify({'error': 'kb_repair_proposals not available'}), 503
    if not ext.HAS_CLASSIFIER:
        return jsonify({'error': 'kb_insufficiency_classifier not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    topic = data.get('topic', '').strip()
    if not topic:
        return jsonify({'error': 'topic is required'}), 400

    conn = ext.kg.thread_local_conn()

    class _StressStub:
        conflict_cluster      = 0.0
        supersession_density  = 0.0
        authority_conflict    = 0.0
        domain_entropy        = 1.0

    stress_stub = _StressStub()
    if ext.HAS_STRESS:
        try:
            _, atoms = ext.retrieve(topic, conn, limit=50)
            if atoms:
                stress_stub = ext.compute_stress(atoms, [topic], conn)
        except Exception:
            pass

    try:
        diagnosis = ext.classify_insufficiency(topic, stress_stub, conn)
        proposals = ext.generate_repair_proposals(diagnosis, conn)
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


@bp.route('/repair/execute', methods=['POST'])
def repair_execute():
    """Execute a repair proposal by ID."""
    if not ext.HAS_EXECUTOR:
        return jsonify({'error': 'kb_repair_executor not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    proposal_id = data.get('proposal_id', '').strip()
    dry_run     = bool(data.get('dry_run', True))

    if not proposal_id:
        return jsonify({'error': 'proposal_id is required'}), 400

    try:
        import dataclasses as _dc
        result = ext.execute_repair(proposal_id, ext.DB_PATH, dry_run=dry_run)
        return jsonify(_dc.asdict(result) if _dc.is_dataclass(result) else result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/repair/rollback', methods=['POST'])
def repair_rollback():
    """Roll back a previously executed repair."""
    if not ext.HAS_EXECUTOR:
        return jsonify({'error': 'kb_repair_executor not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    proposal_id = data.get('proposal_id', '').strip()
    if not proposal_id:
        return jsonify({'error': 'proposal_id is required'}), 400

    try:
        import dataclasses as _dc
        result = ext.rollback_repair(proposal_id, ext.DB_PATH)
        return jsonify(_dc.asdict(result) if _dc.is_dataclass(result) else result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/repair/impact', methods=['GET'])
def repair_impact():
    """Aggregate repair calibration metrics."""
    if not ext.HAS_EXECUTOR:
        return jsonify({'error': 'kb_repair_executor not available'}), 503

    strategy = request.args.get('strategy')
    try:
        import dataclasses as _dc
        result = ext.repair_impact_score(strategy, ext.DB_PATH)
        return jsonify(_dc.asdict(result) if _dc.is_dataclass(result) else result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Adaptation ────────────────────────────────────────────────────────────────

@bp.route('/adapt/status', methods=['GET'])
def adapt_status():
    """Return epistemic adaptation state for all active sessions."""
    session_id = request.args.get('session_id')
    if session_id:
        sess = ext.session_streaks.get(session_id, {'streak': 0, 'last_stress': 0.0})
        return jsonify({
            'session_id': session_id,
            'streak': sess['streak'],
            'last_stress': sess['last_stress'],
        })
    return jsonify({
        sid: {'streak': s['streak'], 'last_stress': s['last_stress']}
        for sid, s in ext.session_streaks.items()
    })


@bp.route('/adapt/reset', methods=['POST'])
def adapt_reset():
    """Reset the epistemic stress streak for a session."""
    data = request.get_json(force=True, silent=True) or {}
    session_id = data.get('session_id', 'default')
    if session_id in ext.session_streaks:
        ext.session_streaks[session_id] = {'streak': 0, 'last_stress': 0.0}
    ext.session_tickers.pop(session_id, None)
    ext.session_portfolio_tickers.pop(session_id, None)
    return jsonify({'session_id': session_id, 'reset': True})


# ── Graph ─────────────────────────────────────────────────────────────────────

@bp.route('/kb/graph', methods=['POST'])
def kb_graph():
    """Graph-structured context for a topic or query."""
    if not ext.HAS_GRAPH_RETRIEVAL:
        return jsonify({'error': 'graph_retrieval not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    message = data.get('message', '').strip()
    if not message:
        return jsonify({'error': 'message is required'}), 400

    conn = ext.kg.thread_local_conn()
    try:
        _, atoms = ext.retrieve(message, conn, limit=100)
        if not atoms:
            return jsonify({'graph_context': '', 'atom_count': 0})

        graph_ctx = ext.build_graph_context(atoms, message, max_nodes_in_context=150)
        return jsonify({
            'graph_context': graph_ctx,
            'atom_count':    len(atoms),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/kb/traverse', methods=['POST'])
def kb_traverse():
    """Relational traversal — what does the KB know about a topic?"""
    if not ext.HAS_GRAPH_RETRIEVAL:
        return jsonify({'error': 'graph_retrieval not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    topic = data.get('topic', '').strip()
    if not topic:
        return jsonify({'error': 'topic is required'}), 400

    conn = ext.kg.thread_local_conn()
    try:
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
        traversal = ext.what_do_i_know_about(topic, atoms)
        return jsonify({
            'topic':     topic,
            'traversal': traversal,
            'atom_count': len(atoms),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/kb/conflicts', methods=['GET'])
def kb_conflicts():
    """Return the fact_conflicts audit log."""
    limit   = int(request.args.get('limit', 50))
    subject = request.args.get('subject', '').strip()

    conn = ext.kg.thread_local_conn()
    c = conn.cursor()
    try:
        if subject:
            c.execute("""
                SELECT fc.id, fc.winner_id, fc.loser_id, fc.winner_obj, fc.loser_obj,
                       fc.reason, fc.detected_at, fw.subject, fw.predicate
                FROM fact_conflicts fc
                LEFT JOIN facts fw ON fc.winner_id = fw.id
                WHERE fw.subject LIKE ?
                ORDER BY fc.detected_at DESC LIMIT ?
            """, (f'%{subject.lower()}%', limit))
        else:
            c.execute("""
                SELECT fc.id, fc.winner_id, fc.loser_id, fc.winner_obj, fc.loser_obj,
                       fc.reason, fc.detected_at, fw.subject, fw.predicate
                FROM fact_conflicts fc
                LEFT JOIN facts fw ON fc.winner_id = fw.id
                ORDER BY fc.detected_at DESC LIMIT ?
            """, (limit,))
        rows = c.fetchall()
        return jsonify({
            'count': len(rows),
            'conflicts': [
                {
                    'id': r[0], 'winner_id': r[1], 'loser_id': r[2],
                    'winner_obj': r[3], 'loser_obj': r[4],
                    'reason': r[5], 'detected_at': r[6],
                    'subject': r[7], 'predicate': r[8],
                }
                for r in rows
            ]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/kb/confidence', methods=['GET'])
def kb_confidence():
    """Return the Bayesian confidence distribution for KB atoms."""
    if not ext.HAS_CONF_INTERVALS:
        return jsonify({'error': 'confidence_intervals module not available'}), 503

    subject   = request.args.get('subject', '').strip()
    predicate = request.args.get('predicate', '').strip()
    try:
        z = float(request.args.get('z', 1.96))
    except ValueError:
        return jsonify({'error': 'z must be a float'}), 400

    if not subject:
        return jsonify({'error': 'subject parameter is required'}), 400

    conn = ext.kg.thread_local_conn()
    try:
        if predicate:
            result = ext.get_confidence_interval(conn, subject, predicate, z=z)
            if result is None:
                return jsonify({'error': f'no atom found for {subject!r} / {predicate!r}'}), 404
            return jsonify(result)
        else:
            atoms = ext.get_all_confidence_intervals(conn, subject, z=z)
            if not atoms:
                return jsonify({'error': f'no atoms found for subject {subject!r}'}), 404
            return jsonify({'subject': subject, 'count': len(atoms), 'atoms': atoms})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/kb/causal-chain', methods=['POST'])
def kb_causal_chain():
    """Traverse the causal graph from a seed concept."""
    if not ext.HAS_CAUSAL_GRAPH:
        return jsonify({'error': 'causal_graph module not available'}), 503

    body = request.get_json(force=True) or {}
    seed = (body.get('seed') or '').strip()
    if not seed:
        return jsonify({'error': 'seed is required'}), 400

    depth          = min(int(body.get('depth', 4)), 6)
    min_confidence = float(body.get('min_confidence', 0.5))

    conn = ext.kg.thread_local_conn()
    try:
        ext.ensure_causal_edges_table(conn)
        result = ext.traverse_causal(conn, seed, max_depth=depth,
                                     min_confidence=min_confidence)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/kb/causal-edge', methods=['POST'])
def kb_causal_edge_add():
    """Add a new causal edge to the graph."""
    if not ext.HAS_CAUSAL_GRAPH:
        return jsonify({'error': 'causal_graph module not available'}), 503

    body = request.get_json(force=True) or {}
    cause     = (body.get('cause')     or '').strip()
    effect    = (body.get('effect')    or '').strip()
    mechanism = (body.get('mechanism') or '').strip()

    if not cause or not effect or not mechanism:
        return jsonify({'error': 'cause, effect, and mechanism are required'}), 400

    confidence = float(body.get('confidence', 0.7))
    source     = (body.get('source') or 'user_defined').strip()

    conn = ext.kg.thread_local_conn()
    try:
        ext.ensure_causal_edges_table(conn)
        result = ext.add_causal_edge(conn, cause, effect, mechanism,
                                     confidence=confidence, source=source)
        status = 201 if result['inserted'] else 200
        return jsonify(result), status
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/kb/causal-edges', methods=['GET'])
def kb_causal_edges_list():
    """List all causal edges in the graph."""
    if not ext.HAS_CAUSAL_GRAPH:
        return jsonify({'error': 'causal_graph module not available'}), 503

    cause_filter = request.args.get('cause', '').strip() or None
    limit        = int(request.args.get('limit', 200))

    conn = ext.kg.thread_local_conn()
    try:
        ext.ensure_causal_edges_table(conn)
        edges = ext.list_causal_edges(conn, cause_filter=cause_filter, limit=limit)
        return jsonify({'count': len(edges), 'edges': edges})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/kb/refresh-queue', methods=['GET'])
def kb_refresh_queue():
    """Inspect the domain_refresh_queue and synthesis_queue."""
    processed = int(request.args.get('processed', 0))
    conn = ext.kg.thread_local_conn()
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


# ── KB Validation ─────────────────────────────────────────────────────────────

@bp.route('/kb/validate', methods=['POST'])
def kb_validate():
    """Run all KB validation rules and return a governance verdict."""
    if not ext.HAS_VALIDATION:
        return jsonify({'error': 'kb_validation not available'}), 503
    try:
        results = ext.validate_all(ext.DB_PATH)
        verdict = ext.governance_verdict(results)
        return jsonify(verdict)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

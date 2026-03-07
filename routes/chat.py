"""routes/chat.py — Chat endpoints: main chat, clear, history, atoms, metrics, models, opportunities."""

from __future__ import annotations

from flask import Blueprint, g, jsonify, request

import extensions as ext
from services.chat_pipeline import sid_for_user as _sid_for_user
from services import chat_pipeline

bp = Blueprint('chat', __name__)


# ── Main chat endpoint ────────────────────────────────────────────────────────

@bp.route('/chat', methods=['POST'])
@ext.rate_limit('chat')
def chat_endpoint():
    """
    KB-grounded chat. Retrieves structured context, builds a KB-aware prompt,
    and calls the LLM to produce an answer.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'error': 'invalid JSON'}), 400

    message = data.get('message', '').strip()
    if not message:
        return jsonify({'error': 'message is required'}), 400

    response, status = chat_pipeline.run(
        message=message,
        session_id=data.get('session_id', 'default'),
        model=data.get('model'),
        goal=data.get('goal'),
        topic=data.get('topic'),
        turn_count=int(data.get('turn_count', 1)),
        limit=int(data.get('limit', 30)),
        screen_context=data.get('screen_context', ''),
        screen_entities=data.get('screen_entities') or [],
        overlay_mode=bool(data.get('overlay_mode', False)),
        user_id=getattr(g, 'user_id', None) or data.get('user_id') or None,
    )
    return jsonify(response), status


# ── Secondary chat endpoints ──────────────────────────────────────────────────

@bp.route('/chat/clear', methods=['POST'])
def chat_clear():
    """POST /chat/clear — clear conversation history for a session."""
    data = request.get_json(force=True, silent=True) or {}
    user_id = data.get('user_id') or getattr(g, 'user_id', None)
    purge   = bool(data.get('purge', False))
    conv_sid = _sid_for_user(user_id)
    _clear_sid = data.get('session_id', 'default')
    ext.sessions.pop_tickers(_clear_sid)
    ext.sessions.pop_portfolio_tickers(_clear_sid)
    deleted = 0
    if purge and ext.conv_store is not None:
        try:
            deleted = ext.conv_store.delete_session_messages(conv_sid)
        except Exception:
            pass
    return jsonify({
        'session_id': conv_sid, 'turns_deleted': deleted,
        'purge': purge, 'cleared': True,
    })


@bp.route('/chat/history', methods=['GET'])
def chat_history():
    """GET /chat/history — read-only conversation timeline."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    limit    = min(int(request.args.get('limit', 50)), 200)
    offset   = int(request.args.get('offset', 0))
    search   = request.args.get('search', '').strip()
    try:
        entries = ext.conv_store.get_timeline(conv_sid, limit=limit, offset=offset, search=search)
        total   = ext.conv_store.get_total_turn_count(conv_sid)
        return jsonify({'session_id': conv_sid, 'entries': entries,
                        'total': total, 'offset': offset})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/history/<int:message_id>', methods=['GET'])
def chat_history_turn(message_id):
    """GET /chat/history/<message_id> — full text of a user+assistant turn pair."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    try:
        pair = ext.conv_store.get_message_pair(message_id)
        if not pair:
            return jsonify({'error': 'Message not found'}), 404
        return jsonify(pair)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/atoms', methods=['GET'])
def chat_atoms():
    """GET /chat/atoms — conversation atoms with salience and graduation status."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    limit    = min(int(request.args.get('limit', 50)), 200)
    try:
        atoms     = ext.conv_store.get_atoms_with_status(conv_sid, limit=limit)
        total     = len(atoms)
        graduated = sum(1 for a in atoms if a.get('graduated'))
        return jsonify({
            'session_id': conv_sid, 'total_atoms': total,
            'graduated_to_kb': graduated, 'pending': total - graduated,
            'atoms': atoms,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/metrics', methods=['GET'])
def chat_metrics():
    """GET /chat/metrics — longitudinal cognitive metrics."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    try:
        return jsonify(ext.conv_store.get_cognitive_metrics(conv_sid))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/models', methods=['GET'])
def chat_models():
    """GET /chat/models — list locally available Ollama models."""
    if not ext.HAS_LLM:
        return jsonify({'models': [], 'available': False,
                        'error': 'llm package not available'}), 503
    models = ext.list_models()
    return jsonify({
        'models': models, 'default': ext.DEFAULT_MODEL,
        'available': bool(models),
    })


# ── Opportunities (POST — authenticated, tier-gated) ─────────────────────────

@bp.route('/opportunities', methods=['POST'])
@ext.require_auth
@ext.require_feature('opportunity_scan')
def opportunities_endpoint():
    """POST /opportunities — on-demand opportunity scan."""
    try:
        from analytics.opportunity_engine import run_opportunity_scan, format_scan_as_context
    except ImportError:
        return jsonify({'error': 'opportunity engine not available'}), 503

    data  = request.get_json(force=True, silent=True) or {}
    query = data.get('query', '')
    modes = data.get('modes') or None
    limit = int(data.get('limit', 6))

    if not query and not modes:
        query = 'broad screen'

    try:
        scan = run_opportunity_scan(
            query=query, db_path=ext.DB_PATH, modes=modes, limit_per_mode=limit,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'mode':          scan.mode,
        'generated_at':  scan.generated_at,
        'market_regime': scan.market_regime,
        'market_context': scan.market_context,
        'results': [
            {
                'ticker': r.ticker, 'mode': r.mode,
                'score': round(r.score, 3), 'conviction_tier': r.conviction_tier,
                'signal_direction': r.signal_direction, 'signal_quality': r.signal_quality,
                'upside_pct': r.upside_pct, 'position_size_pct': r.position_size_pct,
                'thesis': r.thesis, 'rationale': r.rationale,
                'pattern': r.pattern, 'extra': r.extra,
            }
            for r in scan.results
        ],
        'scan_notes': scan.scan_notes,
    })

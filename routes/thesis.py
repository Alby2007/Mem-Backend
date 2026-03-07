"""routes/thesis.py — Thesis builder endpoints."""

from __future__ import annotations

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('thesis', __name__)


@bp.route('/thesis', methods=['GET'])
@ext.require_auth
def thesis_list():
    """GET /thesis — list all theses for the authenticated user."""
    try:
        from knowledge.thesis_builder import ThesisBuilder
        user_id = g.get('user_id') or request.args.get('user_id', '')
        if not user_id:
            return jsonify({'error': 'user_id required'}), 400
        builder = ThesisBuilder(ext.DB_PATH)
        theses  = builder.list_user_theses(user_id)
        return jsonify({'theses': theses, 'count': len(theses)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/thesis/build', methods=['POST'])
@ext.require_auth
def thesis_build():
    """
    POST /thesis/build
    Body: { "ticker": "HSBA.L", "premise": "...", "direction": "bullish" }
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

        builder = ThesisBuilder(ext.DB_PATH)
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


@bp.route('/thesis/<thesis_id>', methods=['GET'])
@ext.require_auth
def thesis_get(thesis_id: str):
    """GET /thesis/<thesis_id> — retrieve a stored thesis with current evidence."""
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder    = ThesisBuilder(ext.DB_PATH)
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


@bp.route('/thesis/<thesis_id>/check', methods=['POST'])
@ext.require_auth
def thesis_check(thesis_id: str):
    """POST /thesis/<thesis_id>/check — force re-evaluation against current KB."""
    try:
        from knowledge.thesis_builder import ThesisBuilder
        builder    = ThesisBuilder(ext.DB_PATH)
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

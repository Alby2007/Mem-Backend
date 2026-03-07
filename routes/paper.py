"""routes/paper.py — Paper trading endpoints: account, positions, monitor, agent, stats."""

from __future__ import annotations

from datetime import datetime, timezone

from flask import Blueprint, Response, g, jsonify, request

import extensions as ext
from services import paper_trading as svc

bp = Blueprint('paper', __name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tier_gate(user_id):
    """Return error response tuple if user lacks pro/premium tier, else None."""
    tier, err_msg = svc.paper_tier_check(user_id)
    if err_msg:
        return jsonify({'error': err_msg, 'tier': tier}), 403
    return None


# ── Route endpoints ──────────────────────────────────────────────────────────

@bp.route('/users/<user_id>/paper/account', methods=['GET'])
@ext.require_auth
def paper_account_get(user_id):
    """GET /users/<user_id>/paper/account — virtual balance + summary stats."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    try:
        return jsonify(svc.get_account(user_id))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/positions', methods=['GET'])
@ext.require_auth
def paper_positions_list(user_id):
    """GET /users/<user_id>/paper/positions?status=open|closed|all"""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    try:
        return jsonify(svc.list_positions(user_id, request.args.get('status', 'all')))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/positions', methods=['POST'])
@ext.require_auth
def paper_position_open(user_id):
    """POST /users/<user_id>/paper/positions — open a new paper position."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    data = request.get_json(force=True, silent=True) or {}
    try:
        result, status = svc.open_position(user_id, data)
        return jsonify(result), status
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/positions/<int:pos_id>/close', methods=['POST'])
@ext.require_auth
def paper_position_close(user_id, pos_id):
    """POST /users/<user_id>/paper/positions/<id>/close — manually close a position."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    data = request.get_json(force=True, silent=True) or {}
    try:
        result, status = svc.close_position(user_id, pos_id, data.get('exit_price'))
        return jsonify(result), status
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/monitor', methods=['POST'])
@ext.require_auth
def paper_monitor(user_id):
    """POST /users/<user_id>/paper/monitor — check open positions vs live prices."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    try:
        return jsonify(svc.monitor_positions(user_id))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/stats', methods=['GET'])
@ext.require_auth
def paper_stats(user_id):
    """GET /users/<user_id>/paper/stats — performance breakdown."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    try:
        return jsonify(svc.get_stats(user_id))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Paper Agent endpoints ─────────────────────────────────────────────────────

@bp.route('/users/<user_id>/paper/agent/log', methods=['GET'])
@ext.require_auth
def paper_agent_log_get(user_id):
    """GET /users/<user_id>/paper/agent/log — last 100 agent activity entries."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    try:
        return jsonify({'log': svc.get_agent_log(user_id)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/agent/run', methods=['POST'])
@ext.require_auth
def paper_agent_run_once(user_id):
    """POST /users/<user_id>/paper/agent/run — one-shot scan."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    try:
        result = svc.ai_run(user_id)
        return jsonify({'status': 'ok', 'result': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/agent/start', methods=['POST'])
@ext.require_auth
def paper_agent_start(user_id):
    """POST /users/<user_id>/paper/agent/start — start continuous scanner."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    status, message = svc.start_scanner(user_id)
    return jsonify({'status': status, 'message': message})


@bp.route('/users/<user_id>/paper/agent/stop', methods=['POST'])
@ext.require_auth
def paper_agent_stop(user_id):
    """POST /users/<user_id>/paper/agent/stop — stop continuous scanner."""
    err = ext.assert_self(user_id)
    if err: return err
    status, message = svc.stop_scanner(user_id)
    return jsonify({'status': status, 'message': message})


@bp.route('/users/<user_id>/paper/agent/status', methods=['GET'])
@ext.require_auth
def paper_agent_status(user_id):
    """GET /users/<user_id>/paper/agent/status — is scanner running?"""
    err = ext.assert_self(user_id)
    if err: return err
    return jsonify({'running': svc.scanner_running(user_id)})


@bp.route('/users/<user_id>/paper/agent/log/export', methods=['GET'])
@ext.require_auth
def paper_agent_log_export(user_id):
    """GET /users/<user_id>/paper/agent/log/export — full audit log as CSV."""
    err = ext.assert_self(user_id)
    if err: return err
    terr = _tier_gate(user_id)
    if terr: return terr
    try:
        csv_bytes = svc.export_log_csv(user_id)
        fname = f'paper_trade_log_{user_id}_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}.csv'
        return Response(
            csv_bytes, mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{fname}"'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Re-export for backward compatibility (ingest scheduler imports from here) ─
PaperAgentAdapter = svc.PaperAgentAdapter

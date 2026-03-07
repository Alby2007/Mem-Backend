"""routes/health.py — Health, seed status, and detailed health endpoints."""

from __future__ import annotations

import pathlib
import sqlite3

from flask import Blueprint, jsonify, request

import extensions as ext

bp = Blueprint('health', __name__)


@bp.route('/health', methods=['GET'])
@ext.limiter.exempt
def health():
    return jsonify({'status': 'ok', 'db': ext.DB_PATH})


@bp.route('/seed/status', methods=['GET'])
@ext.limiter.exempt
def seed_status():
    """
    GET /seed/status

    Returns the current seed tag pushed to GitHub, local fact count, last push
    time, and next scheduled push times. Clients use this to check whether
    they need to pull a new seed without hitting the GitHub API directly.
    """
    from datetime import datetime as _dt, timezone as _tz

    # Read last push tag from .seed_tag file if present (written by push_seed.py)
    tag_file = pathlib.Path('.seed_tag')
    last_tag = tag_file.read_text().strip() if tag_file.exists() else None

    # Fact count from live DB
    try:
        _c = sqlite3.connect(ext.DB_PATH, timeout=5)
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
        'db_path':     ext.DB_PATH,
        'server_time': now_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
    })


@bp.route('/health/detailed', methods=['GET'])
def health_detailed():
    """
    GET /health/detailed

    Extended liveness check: KB stats, per-adapter ingest status, epistemic
    stress score, and scheduler states.  Always available (no feature guard).
    """
    result: dict = {'status': 'ok', 'db': ext.DB_PATH}

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
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

    if ext.HAS_STRESS:
        try:
            conn2 = sqlite3.connect(ext.DB_PATH, timeout=5)
            sample_atoms = conn2.execute(
                "SELECT subject, predicate, object, confidence, source, timestamp "
                "FROM facts ORDER BY confidence DESC LIMIT 50"
            ).fetchall()
            conn2.close()
            cols = ['subject', 'predicate', 'object', 'confidence', 'source', 'timestamp']
            atoms = [dict(zip(cols, r)) for r in sample_atoms]
            sr = ext.compute_stress(atoms, [], None)
            result['kb_stress'] = sr.composite_stress
        except Exception:
            result['kb_stress'] = None

    if ext.HAS_INGEST and ext.ingest_scheduler:
        try:
            result['adapters'] = ext.ingest_scheduler.get_status()
        except Exception:
            result['adapters'] = None

    result['tip_scheduler']      = 'running' if (ext.tip_scheduler and getattr(ext.tip_scheduler, '_thread', None) and ext.tip_scheduler._thread.is_alive()) else 'stopped'
    result['delivery_scheduler'] = 'running' if (ext.delivery_scheduler and getattr(ext.delivery_scheduler, '_thread', None) and ext.delivery_scheduler._thread.is_alive()) else 'stopped'
    result['position_monitor']   = 'running' if (ext.position_monitor and getattr(ext.position_monitor, '_thread', None) and ext.position_monitor._thread.is_alive()) else 'stopped'

    return jsonify(result)


@bp.route('/')
def serve_frontend():
    from flask import send_from_directory, make_response
    import os as _os
    resp = make_response(send_from_directory(
        _os.path.join(_os.path.dirname(__file__), '..', 'static'), 'index.html'
    ))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

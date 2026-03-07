"""routes/waitlist.py — Waitlist endpoints."""

from __future__ import annotations

import logging
import os
import sqlite3

from flask import Blueprint, jsonify, request

import extensions as ext

bp = Blueprint('waitlist', __name__)
_logger = logging.getLogger(__name__)


def _ensure_waitlist_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS waitlist (
            email     TEXT PRIMARY KEY,
            joined_at TEXT NOT NULL DEFAULT (datetime('now')),
            source    TEXT DEFAULT 'landing'
        )
    """)
    conn.commit()


def _notify_waitlist_telegram(email: str) -> None:
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    chat_id   = os.environ.get('WAITLIST_TELEGRAM_CHAT_ID', '')
    if not bot_token or not chat_id:
        return
    try:
        import urllib.request as _ur2
        import json as _j2
        payload = _j2.dumps({
            'chat_id': chat_id,
            'text': f'\U0001f680 New waitlist signup: {email}',
        }).encode()
        req = _ur2.Request(
            f'https://api.telegram.org/bot{bot_token}/sendMessage',
            data=payload,
            headers={'Content-Type': 'application/json'},
        )
        _ur2.urlopen(req, timeout=5)
    except Exception:
        pass


@bp.route('/waitlist', methods=['POST'])
@ext.limiter.limit('3 per hour')
def waitlist_join():
    """POST /waitlist — add an email to the beta waitlist."""
    try:
        data  = request.get_json(silent=True) or {}
        email = data.get('email', '').strip().lower()
        if not email or '@' not in email or len(email) > 254:
            return jsonify({'error': 'Invalid email'}), 400
        source = str(data.get('source', 'landing'))[:64]
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        _ensure_waitlist_table(conn)
        cur = conn.execute(
            "INSERT OR IGNORE INTO waitlist (email, joined_at, source) VALUES (?, datetime('now'), ?)",
            (email, source),
        )
        conn.commit()
        already = cur.rowcount == 0
        conn.close()
        if not already:
            _notify_waitlist_telegram(email)
        msg = "You're already on the list" if already else "You're on the list"
        return jsonify({'message': msg, 'already': already}), 200
    except Exception as e:
        _logger.error('waitlist_join error: %s', e)
        return jsonify({'error': 'Something went wrong'}), 500


@bp.route('/waitlist/count', methods=['GET'])
def waitlist_count():
    """GET /waitlist/count — public signup count for landing page social proof."""
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        _ensure_waitlist_table(conn)
        row = conn.execute('SELECT COUNT(*) FROM waitlist').fetchone()
        conn.close()
        return jsonify({'count': row[0] if row else 0})
    except Exception:
        return jsonify({'count': 0})

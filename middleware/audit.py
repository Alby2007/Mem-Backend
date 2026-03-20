"""
middleware/audit.py — Audit logging

Writes structured audit rows to the audit_log table.
Every write operation and auth event should call log_audit_event().

USAGE
=====
    from middleware.audit import log_audit_event

    log_audit_event(db_path, user_id='alice', action='portfolio_submit',
                    ip_address=request.remote_addr,
                    user_agent=request.user_agent.string,
                    outcome='success')
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from middleware.auth import _auth_conn

_log = logging.getLogger(__name__)

_DDL_AUDIT_LOG = """
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     TEXT,
    action      TEXT NOT NULL,
    ip_address  TEXT,
    user_agent  TEXT,
    outcome     TEXT,
    detail      TEXT,
    timestamp   TEXT NOT NULL
)
"""

_VALID_OUTCOMES = frozenset({'success', 'failure', 'blocked'})


def ensure_audit_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_AUDIT_LOG)
    conn.commit()


def log_audit_event(
    db_path: str,
    action: str,
    *,
    conn: Optional[sqlite3.Connection] = None,
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    outcome: str = 'success',
    detail: Optional[dict] = None,
) -> None:
    """
    Write one row to audit_log.  Never raises — failures are logged but swallowed
    so an audit error never breaks a real request.

    Pass an existing ``conn`` to avoid opening a new DB connection (caller retains
    ownership and must not close it before this call returns).
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        detail_str = json.dumps(detail) if detail else None
        _owned = conn is None
        if _owned:
            conn = _auth_conn(timeout=5)
        try:
            ensure_audit_table(conn)
            conn.execute(
                """INSERT INTO audit_log
                   (user_id, action, ip_address, user_agent, outcome, detail, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user_id, action, ip_address, user_agent, outcome, detail_str, now),
            )
            conn.commit()
        finally:
            if _owned:
                conn.close()
    except Exception as exc:
        _log.warning('audit log write failed: %s', exc)


def get_audit_log(
    db_path: str,
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = 100,
) -> list:
    """Return audit_log rows, optionally filtered by user_id and/or action."""
    conn = _auth_conn(timeout=10)
    try:
        ensure_audit_table(conn)
        clauses = []
        params: list = []
        if user_id:
            clauses.append('user_id = ?')
            params.append(user_id)
        if action:
            clauses.append('action = ?')
            params.append(action)
        where = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''
        params.append(limit)
        rows = conn.execute(
            f"""SELECT id, user_id, action, ip_address, user_agent, outcome, detail, timestamp
                FROM audit_log {where}
                ORDER BY timestamp DESC LIMIT ?""",
            params,
        ).fetchall()
        cols = ['id', 'user_id', 'action', 'ip_address', 'user_agent', 'outcome', 'detail', 'timestamp']
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            if d['detail']:
                try:
                    d['detail'] = json.loads(d['detail'])
                except Exception:
                    pass
            result.append(d)
        return result
    finally:
        conn.close()

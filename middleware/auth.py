"""
middleware/auth.py — JWT Authentication

Provides:
  require_auth        — decorator that validates Bearer token + sets g.user_id
  assert_self         — enforce g.user_id == user_id from URL (prevents horiz. escalation)
  register_user()     — create user_auth + user_preferences rows
  authenticate_user() — verify credentials, return JWT

Environment variables
---------------------
  JWT_SECRET_KEY            — required in production; defaults to an insecure dev value
  JWT_EXPIRY_HOURS          — access token lifetime in hours (default 24)
  JWT_REFRESH_EXPIRY_DAYS   — refresh token lifetime in days (default 30)

DB tables: user_auth, refresh_tokens (see DDL constants below)
"""

from __future__ import annotations

import logging
import os
import secrets as _secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Optional

from flask import g, jsonify, request

_log = logging.getLogger(__name__)

_SECRET_KEY           = os.environ.get('JWT_SECRET_KEY', 'dev-insecure-key-change-in-production')
_EXPIRY_HOURS         = int(os.environ.get('JWT_EXPIRY_HOURS', '24'))
_REFRESH_EXPIRY_DAYS  = int(os.environ.get('JWT_REFRESH_EXPIRY_DAYS', '30'))

_LOCKOUT_MINUTES  = 15
_MAX_FAILED_ATTEMPTS = 5

try:
    import jwt as _jwt
    HAS_JWT = True
except ImportError:
    _jwt = None     # type: ignore
    HAS_JWT = False

try:
    import bcrypt as _bcrypt
    HAS_BCRYPT = True
except ImportError:
    _bcrypt = None  # type: ignore
    HAS_BCRYPT = False


_DDL_REFRESH_TOKENS = """
CREATE TABLE IF NOT EXISTS refresh_tokens (
    token_id    TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    issued_at   TEXT NOT NULL,
    expires_at  TEXT NOT NULL,
    revoked     INTEGER DEFAULT 0
)
"""

_DDL_USER_AUTH = """
CREATE TABLE IF NOT EXISTS user_auth (
    user_id         TEXT PRIMARY KEY,
    email           TEXT UNIQUE NOT NULL,
    password_hash   TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    last_login      TEXT,
    failed_attempts INTEGER DEFAULT 0,
    locked_until    TEXT
)
"""


def ensure_user_auth_table(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_USER_AUTH)
    conn.execute(_DDL_REFRESH_TOKENS)
    conn.commit()


# ── Token helpers ──────────────────────────────────────────────────────────────

def _make_access_token(user_id: str, email: str) -> str:
    if not HAS_JWT:
        raise RuntimeError('PyJWT is not installed — run: pip install PyJWT')
    payload = {
        'sub':     user_id,
        'user_id': user_id,
        'email':   email,
        'type':    'access',
        'exp':     datetime.now(timezone.utc) + timedelta(hours=_EXPIRY_HOURS),
        'iat':     datetime.now(timezone.utc),
    }
    return _jwt.encode(payload, _SECRET_KEY, algorithm='HS256')


def _make_token(user_id: str, email: str) -> str:
    return _make_access_token(user_id, email)


def _decode_token(token: str) -> dict:
    if not HAS_JWT:
        raise RuntimeError('PyJWT is not installed')
    return _jwt.decode(token, _SECRET_KEY, algorithms=['HS256'])


# ── Refresh token helpers ──────────────────────────────────────────────────────


def issue_refresh_token(db_path: str, user_id: str) -> dict:
    """
    Generate a cryptographically random refresh token, persist it, and return
    { refresh_token, expires_at }.
    """
    token_id   = _secrets.token_urlsafe(48)
    now        = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=_REFRESH_EXPIRY_DAYS)
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_auth_table(conn)
        conn.execute(
            """INSERT INTO refresh_tokens (token_id, user_id, issued_at, expires_at)
               VALUES (?, ?, ?, ?)""",
            (token_id, user_id, now.isoformat(), expires_at.isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    return {'refresh_token': token_id, 'expires_at': expires_at.isoformat()}


def rotate_refresh_token(db_path: str, refresh_token: str) -> dict:
    """
    Validate an existing refresh token, revoke it, issue a new access token +
    refresh token pair (rotation).  Raises ValueError if invalid/expired/revoked.
    Returns { access_token, refresh_token, token_type, expires_in, user_id }.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_auth_table(conn)
        row = conn.execute(
            """SELECT user_id, expires_at, revoked
               FROM refresh_tokens WHERE token_id = ?""",
            (refresh_token,),
        ).fetchone()

        if row is None:
            raise ValueError('invalid refresh token')

        user_id, expires_at_str, revoked = row

        if revoked:
            raise ValueError('refresh token has been revoked')

        expires_at = datetime.fromisoformat(expires_at_str)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > expires_at:
            raise ValueError('refresh token has expired — please log in again')

        # Revoke the old token (rotation: one-time use)
        conn.execute(
            "UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ?",
            (refresh_token,),
        )
        conn.commit()

        # Fetch email for the new access token
        email_row = conn.execute(
            "SELECT email FROM user_auth WHERE user_id = ?", (user_id,)
        ).fetchone()
        email = email_row[0] if email_row else ''
    finally:
        conn.close()

    access_token  = _make_access_token(user_id, email)
    refresh_data  = issue_refresh_token(db_path, user_id)
    return {
        'access_token':  access_token,
        'refresh_token': refresh_data['refresh_token'],
        'token_type':    'Bearer',
        'expires_in':    _EXPIRY_HOURS * 3600,
        'user_id':       user_id,
    }


# ── Password helpers ───────────────────────────────────────────────────────────

def _hash_password(password: str) -> str:
    if not HAS_BCRYPT:
        raise RuntimeError('bcrypt is not installed — run: pip install bcrypt')
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def _check_password(password: str, hashed: str) -> bool:
    if not HAS_BCRYPT:
        return False
    return _bcrypt.checkpw(password.encode(), hashed.encode())


# ── Auth decorator ─────────────────────────────────────────────────────────────

def require_auth(f):
    """
    Decorator: validate Bearer JWT, set g.user_id.
    Returns 401 if token is missing, invalid, or expired.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not HAS_JWT:
            return jsonify({'error': 'auth not available — PyJWT not installed'}), 503
        header = request.headers.get('Authorization', '')
        token = header.removeprefix('Bearer ').strip()
        if not token:
            return jsonify({'error': 'unauthorized', 'detail': 'missing token'}), 401
        try:
            payload = _decode_token(token)
            g.user_id = payload['user_id']
            g.user_email = payload.get('email', '')
        except Exception as exc:
            name = type(exc).__name__
            if 'Expired' in name:
                return jsonify({'error': 'token_expired'}), 401
            return jsonify({'error': 'invalid_token'}), 401
        return f(*args, **kwargs)
    return decorated


def assert_self(user_id: str):
    """
    Call inside a route after @require_auth to enforce that the authenticated
    user can only access their own data.  Returns a 403 response tuple if the
    IDs don't match, or None if they match (caller continues normally).

    Usage:
        @require_auth
        def my_route(user_id):
            err = assert_self(user_id)
            if err: return err
            ...
    """
    if getattr(g, 'user_id', None) != user_id:
        return jsonify({'error': 'forbidden', 'detail': 'you can only access your own data'}), 403
    return None


# ── Registration and login ─────────────────────────────────────────────────────

def register_user(
    db_path: str,
    user_id: str,
    email: str,
    password: str,
) -> dict:
    """
    Create a user_auth row.
    Raises ValueError on duplicate email or weak password.
    """
    if not HAS_BCRYPT:
        raise RuntimeError('bcrypt not installed')
    if len(password) < 8:
        raise ValueError('password must be at least 8 characters')

    now = datetime.now(timezone.utc).isoformat()
    password_hash = _hash_password(password)

    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_auth_table(conn)
        try:
            conn.execute(
                """INSERT INTO user_auth (user_id, email, password_hash, created_at)
                   VALUES (?, ?, ?, ?)""",
                (user_id, email.lower().strip(), password_hash, now),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise ValueError(f'email already registered: {email}') from exc
    finally:
        conn.close()

    return {'user_id': user_id, 'email': email, 'created_at': now}


def authenticate_user(
    db_path: str,
    email: str,
    password: str,
) -> dict:
    """
    Verify credentials.  Returns JWT token dict on success.
    Raises ValueError on bad credentials or locked account.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        ensure_user_auth_table(conn)
        row = conn.execute(
            """SELECT user_id, password_hash, failed_attempts, locked_until
               FROM user_auth WHERE email = ?""",
            (email.lower().strip(),),
        ).fetchone()

        if row is None:
            raise ValueError('invalid email or password')

        user_id, password_hash, failed_attempts, locked_until = row

        # Check lockout
        if locked_until:
            lock_dt = datetime.fromisoformat(locked_until)
            if datetime.now(timezone.utc) < lock_dt:
                raise ValueError('account temporarily locked — try again later')
            else:
                conn.execute(
                    "UPDATE user_auth SET locked_until = NULL, failed_attempts = 0 WHERE user_id = ?",
                    (user_id,),
                )
                conn.commit()

        if not _check_password(password, password_hash):
            new_fails = (failed_attempts or 0) + 1
            if new_fails >= _MAX_FAILED_ATTEMPTS:
                lock_until = (
                    datetime.now(timezone.utc) + timedelta(minutes=_LOCKOUT_MINUTES)
                ).isoformat()
                conn.execute(
                    "UPDATE user_auth SET failed_attempts = ?, locked_until = ? WHERE user_id = ?",
                    (new_fails, lock_until, user_id),
                )
            else:
                conn.execute(
                    "UPDATE user_auth SET failed_attempts = ? WHERE user_id = ?",
                    (new_fails, user_id),
                )
            conn.commit()
            raise ValueError('invalid email or password')

        # Success — reset failures + record last_login
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE user_auth SET failed_attempts = 0, locked_until = NULL, last_login = ? WHERE user_id = ?",
            (now, user_id),
        )
        conn.commit()

        token = _make_token(user_id, email)
        return {
            'access_token': token,
            'token_type':   'Bearer',
            'expires_in':   _EXPIRY_HOURS * 3600,
            'user_id':      user_id,
        }
    finally:
        conn.close()

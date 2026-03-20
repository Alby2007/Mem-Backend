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

# TODO(FastAPI migration): replace g.user_id with dependency injection; require_auth becomes a Depends()
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

# Precomputed dummy hash used for constant-time blind when email is not found.
# Prevents user enumeration via timing (invalid email returns in ~0ms vs ~100ms).
_DUMMY_HASH: str = ''


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
    for ddl in [
        "ALTER TABLE user_auth ADD COLUMN oauth_provider TEXT",
        "ALTER TABLE user_auth ADD COLUMN oauth_sub TEXT",
    ]:
        try:
            conn.execute(ddl)
            conn.commit()
        except Exception:
            pass
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_user_auth_oauth
        ON user_auth (oauth_provider, oauth_sub)
        WHERE oauth_provider IS NOT NULL
    """)
    conn.commit()


# ── Token helpers ──────────────────────────────────────────────────────────────

def _make_access_token(user_id: str, email: str, token_version: int = 0) -> str:
    if not HAS_JWT:
        raise RuntimeError('PyJWT is not installed — run: pip install PyJWT')
    payload = {
        'sub':     user_id,
        'user_id': user_id,
        'email':   email,
        'type':    'access',
        'tv':      token_version,
        'exp':     datetime.now(timezone.utc) + timedelta(hours=_EXPIRY_HOURS),
        'iat':     datetime.now(timezone.utc),
    }
    return _jwt.encode(payload, _SECRET_KEY, algorithm='HS256')


def _make_token(user_id: str, email: str, db_path: str = '') -> str:
    """Convenience wrapper that reads token_version from DB when db_path is supplied."""
    tv = 0
    if db_path:
        try:
            _c = sqlite3.connect(db_path, timeout=3)
            _r = _c.execute(
                "SELECT token_version FROM user_auth WHERE user_id = ?", (user_id,)
            ).fetchone()
            _c.close()
            tv = int(_r[0]) if _r and _r[0] is not None else 0
        except Exception:
            pass
    return _make_access_token(user_id, email, token_version=tv)


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
    # Retry up to 3 times on DB lock — 27 bot threads can momentarily lock the DB
    _last_exc = None
    for _attempt in range(3):
        try:
            conn = sqlite3.connect(db_path, timeout=30)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=30000')
            break
        except Exception as _e:
            _last_exc = _e
            import time as _time; _time.sleep(0.5 * (_attempt + 1))
    else:
        raise _last_exc

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

    # Read current token_version so refreshed token carries correct version
    try:
        _tv_conn = sqlite3.connect(db_path, timeout=5)
        _tv_row  = _tv_conn.execute(
            "SELECT token_version FROM user_auth WHERE user_id = ?", (user_id,)
        ).fetchone()
        _tv_conn.close()
        tv = int(_tv_row[0]) if _tv_row and _tv_row[0] is not None else 0
    except Exception:
        tv = 0

    access_token  = _make_access_token(user_id, email, token_version=tv)
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


def _init_dummy_hash() -> None:
    global _DUMMY_HASH
    if HAS_BCRYPT and not _DUMMY_HASH:
        _DUMMY_HASH = _hash_password('__timing_blind_dummy__')


_init_dummy_hash()


def _check_password(password: str, hashed: str) -> bool:
    if not HAS_BCRYPT:
        return False
    return _bcrypt.checkpw(password.encode(), hashed.encode())


# ── Auth decorator ─────────────────────────────────────────────────────────────

def require_auth(f):
    """
    Decorator: validate JWT, set g.user_id.
    Accepts token from:
      1. Authorization: Bearer <token>  header (API clients, backwards compat)
      2. tg_access HttpOnly cookie       (browser SPA — preferred)
    Returns 401 if token is missing, invalid, or expired.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not HAS_JWT:
            return jsonify({'error': 'auth not available — PyJWT not installed'}), 503
        # Prefer Authorization header; fall back to HttpOnly cookie
        header = request.headers.get('Authorization', '')
        token = header.removeprefix('Bearer ').strip()
        if not token:
            token = request.cookies.get('tg_access', '').strip()
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


def revoke_user_tokens(db_path: str, user_id: str) -> None:
    """
    Increment token_version to immediately invalidate all existing access tokens
    for a user. Called on password change and forced logout.
    The next login will issue a token with the new version; old tokens will fail
    the version check in get_current_user.
    """
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute(
            "UPDATE user_auth SET token_version = COALESCE(token_version, 0) + 1 WHERE user_id = ?",
            (user_id,),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        _log.warning("revoke_user_tokens failed for %s: %s", user_id, exc)


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
            """SELECT user_id, password_hash, failed_attempts, locked_until, oauth_provider
               FROM user_auth WHERE email = ?""",
            (email.lower().strip(),),
        ).fetchone()

        if row is None:
            # Constant-time blind: run bcrypt so timing matches a wrong-password path
            if _DUMMY_HASH:
                _check_password(password, _DUMMY_HASH)
            raise ValueError('invalid email or password')

        user_id, password_hash, failed_attempts, locked_until, oauth_provider = row

        if oauth_provider:
            raise ValueError('google_account_use_oauth')

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

        # Read current token_version for embedding in JWT
        tv_row = conn.execute(
            "SELECT token_version FROM user_auth WHERE user_id = ?", (user_id,)
        ).fetchone()
        tv = int(tv_row[0]) if tv_row and tv_row[0] is not None else 0

        token = _make_access_token(user_id, email, token_version=tv)
        return {
            'access_token': token,
            'token_type':   'Bearer',
            'expires_in':   _EXPIRY_HOURS * 3600,
            'user_id':      user_id,
        }
    finally:
        conn.close()

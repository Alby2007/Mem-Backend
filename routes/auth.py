"""routes/auth.py — Authentication endpoints: register, token, refresh, logout, me, telegram auth."""

from __future__ import annotations

import os
import sqlite3

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('auth', __name__)

# ── Cookie helpers ────────────────────────────────────────────────────────────
_IS_PROD = os.environ.get('FLASK_ENV', 'production') != 'development'


def _set_auth_cookies(resp, access_token: str, refresh_token: str) -> None:
    """Set HttpOnly, Secure, SameSite=None auth cookies on a response."""
    resp.set_cookie(
        'tg_access',
        value=access_token,
        httponly=True,
        secure=True,
        samesite='None',
        path='/',
        max_age=86400,
    )
    if refresh_token:
        resp.set_cookie(
            'tg_refresh',
            value=refresh_token,
            httponly=True,
            secure=True,
            samesite='None',
            path='/auth/refresh',
            max_age=2592000,
        )


def _clear_auth_cookies(resp) -> None:
    """Expire both auth cookies immediately."""
    resp.set_cookie('tg_access',  value='', httponly=True, secure=True,
                    samesite='None', path='/', max_age=0)
    resp.set_cookie('tg_refresh', value='', httponly=True, secure=True,
                    samesite='None', path='/auth/refresh', max_age=0)


# ── Telegram login codes (in-memory, shared with telegram.py) ────────────────
_TG_LOGIN_CODES: dict = {}


@bp.route('/auth/register', methods=['POST'])
@ext.rate_limit('auth')
def auth_register():
    """
    POST /auth/register
    Body: { "user_id": "alice", "email": "alice@example.com", "password": "..." }
    """
    if not ext.HAS_AUTH:
        return jsonify({'error': 'auth not available — install PyJWT and bcrypt'}), 503

    data = request.get_json(force=True, silent=True) or {}

    # Beta access gate
    _beta_secret = os.environ.get('BETA_PASSWORD', '')
    _beta_given  = str(data.get('beta_password', ''))
    if not _beta_secret or _beta_given != _beta_secret:
        ext.log_audit_event(ext.DB_PATH, action='register',
                            ip_address=request.remote_addr,
                            user_agent=request.user_agent.string,
                            outcome='failure', detail={'reason': 'invalid beta password'})
        return jsonify({'error': 'Invalid beta access password.'}), 403

    if ext.HAS_VALIDATORS:
        result = ext.validate_register(data)
        if not result.valid:
            return jsonify({'error': 'validation_failed', 'details': result.errors}), 400

    user_id  = str(data.get('user_id', '')).strip()
    email    = str(data.get('email', '')).strip()
    password = str(data.get('password', ''))

    try:
        row = ext.register_user(ext.DB_PATH, user_id, email, password)
    except ValueError as e:
        ext.log_audit_event(ext.DB_PATH, action='register',
                            ip_address=request.remote_addr,
                            user_agent=request.user_agent.string,
                            outcome='failure', detail={'reason': str(e)})
        status_code = 409 if 'already registered' in str(e) else 400
        return jsonify({'error': str(e)}), status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if ext.HAS_PRODUCT_LAYER:
        try:
            ext.create_user(ext.DB_PATH, user_id)
        except Exception:
            pass

    ext.log_audit_event(ext.DB_PATH, action='register', user_id=user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
    return jsonify(row), 201


@bp.route('/auth/token', methods=['POST'])
@ext.rate_limit('auth')
def auth_token():
    """
    POST /auth/token
    Body: { "email": "alice@example.com", "password": "..." }
    """
    if not ext.HAS_AUTH:
        return jsonify({'error': 'auth not available — install PyJWT and bcrypt'}), 503

    data     = request.get_json(force=True, silent=True) or {}
    email    = str(data.get('email', '')).strip()
    password = str(data.get('password', ''))

    if not email or not password:
        return jsonify({'error': 'email and password are required'}), 400

    try:
        token_data = ext.authenticate_user(ext.DB_PATH, email, password)
    except ValueError as e:
        ext.log_audit_event(ext.DB_PATH, action='login_failure',
                            ip_address=request.remote_addr,
                            user_agent=request.user_agent.string,
                            outcome='failure', detail={'email': email, 'reason': str(e)})
        return jsonify({'error': str(e)}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    ext.log_audit_event(ext.DB_PATH, action='login_success', user_id=token_data['user_id'],
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
    try:
        refresh_data = ext.issue_refresh_token(ext.DB_PATH, token_data['user_id'])
        token_data['refresh_token']         = refresh_data['refresh_token']
        token_data['refresh_token_expires'] = refresh_data['expires_at']
    except Exception:
        pass
    resp = jsonify(token_data)
    _set_auth_cookies(resp, token_data['access_token'],
                      token_data.get('refresh_token', ''))
    return resp


@bp.route('/auth/refresh', methods=['POST'])
@ext.rate_limit('auth')
def auth_refresh():
    """
    POST /auth/refresh
    Body: { "refresh_token": "<opaque token string>" }
    """
    if not ext.HAS_AUTH:
        return jsonify({'error': 'auth not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    refresh_token = data.get('refresh_token', '').strip()
    if not refresh_token:
        refresh_token = request.cookies.get('tg_refresh', '').strip()
    if not refresh_token:
        return jsonify({'error': 'refresh_token is required'}), 400
    try:
        result = ext.rotate_refresh_token(ext.DB_PATH, refresh_token)
        ext.log_audit_event(ext.DB_PATH, action='token_refresh', user_id=result['user_id'],
                            ip_address=request.remote_addr,
                            user_agent=request.user_agent.string,
                            outcome='success')
        resp = jsonify(result)
        _set_auth_cookies(resp, result['access_token'], result['refresh_token'])
        return resp
    except ValueError as e:
        return jsonify({'error': 'token_expired' if 'expired' in str(e) else 'invalid_token',
                        'detail': str(e)}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/auth/logout', methods=['POST'])
@ext.require_auth
def auth_logout():
    """
    POST /auth/logout
    Body: { "refresh_token": "<opaque token string>" }  (optional)
    """
    if not ext.HAS_AUTH:
        return jsonify({'error': 'auth not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    refresh_token = data.get('refresh_token', '').strip()
    if refresh_token:
        try:
            conn = sqlite3.connect(ext.DB_PATH, timeout=10)
            conn.execute(
                "UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ? AND user_id = ?",
                (refresh_token, g.user_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
    ext.log_audit_event(ext.DB_PATH, action='logout', user_id=g.user_id,
                        ip_address=request.remote_addr,
                        user_agent=request.user_agent.string,
                        outcome='success')
    resp = jsonify({'logged_out': True})
    _clear_auth_cookies(resp)
    return resp


@bp.route('/auth/telegram/code', methods=['POST'])
@ext.limiter.exempt
def auth_telegram_code():
    """
    POST /auth/telegram/code
    Generate a one-time login code. Frontend opens t.me/bot?start=<code>.
    """
    import secrets, time as _time
    code = secrets.token_hex(4).upper()
    _TG_LOGIN_CODES[code] = {'chat_id': None, 'user_data': None, 'expires': _time.time() + 300}
    expired = [k for k, v in _TG_LOGIN_CODES.items() if v['expires'] < _time.time()]
    for k in expired:
        del _TG_LOGIN_CODES[k]
    return jsonify({'code': code})


@bp.route('/auth/telegram/verify', methods=['POST'])
@ext.limiter.exempt
def auth_telegram_verify():
    """
    POST /auth/telegram/verify
    Body: { "code": "ABC12345" }
    """
    import time as _time, base64 as _b64, json as _json
    data = request.get_json(force=True, silent=True) or {}
    code = (data.get('code') or '').strip().upper()
    entry = _TG_LOGIN_CODES.get(code)
    if not entry:
        return jsonify({'error': 'Invalid code'}), 400
    if _time.time() > entry['expires']:
        del _TG_LOGIN_CODES[code]
        return jsonify({'error': 'Code expired'}), 400
    if not entry.get('chat_id'):
        return jsonify({'error': 'Code not yet confirmed — send it to the bot first'}), 202
    tg_data = entry.get('user_data') or {}
    chat_id = entry['chat_id']
    del _TG_LOGIN_CODES[code]
    user_id = f"tg_{chat_id}"
    # Upsert user
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            from users.user_store import ensure_user_tables
            ensure_user_tables(conn)
            conn.execute(
                "INSERT OR IGNORE INTO user_preferences (user_id, onboarding_complete, tier) VALUES (?, 0, 'free')",
                (user_id,),
            )
            conn.execute(
                "UPDATE user_preferences SET telegram_chat_id=? WHERE user_id=?",
                (str(chat_id), user_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass
    # Issue token
    if ext.HAS_AUTH:
        try:
            from middleware.auth import _make_token
            access_token = _make_token(user_id, f"{user_id}@telegram.local")
        except Exception:
            access_token = _b64.urlsafe_b64encode(_json.dumps(
                {'user_id': user_id, 'sub': user_id, 'exp': int(_time.time()) + 86400 * 30}
            ).encode()).decode()
    else:
        access_token = _b64.urlsafe_b64encode(_json.dumps(
            {'user_id': user_id, 'sub': user_id, 'exp': int(_time.time()) + 86400 * 30}
        ).encode()).decode()
    resp = jsonify({
        'access_token': access_token,
        'user_id':      user_id,
        'first_name':   tg_data.get('first_name', ''),
        'username':     tg_data.get('username', ''),
        'tg_data':      tg_data,
    })
    if ext.HAS_AUTH and access_token and access_token.startswith('eyJ'):
        try:
            _set_auth_cookies(resp, access_token, '')
        except Exception:
            pass
    return resp


@bp.route('/auth/telegram', methods=['POST'])
@ext.limiter.exempt
def auth_telegram():
    """
    POST /auth/telegram
    Exchange Telegram Login Widget auth data for an app access token.
    """
    import hashlib
    import hmac
    import time

    data = request.get_json(force=True, silent=True) or {}
    tg_id = data.get('id')
    if not tg_id:
        return jsonify({'error': 'Telegram auth data missing id'}), 400

    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if bot_token:
        try:
            check_hash = data.get('hash', '')
            data_check = {k: v for k, v in data.items() if k != 'hash'}
            data_check_str = '\n'.join(f'{k}={v}' for k, v in sorted(data_check.items()))
            secret = hashlib.sha256(bot_token.encode()).digest()
            computed = hmac.new(secret, data_check_str.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(computed, check_hash):
                return jsonify({'error': 'Telegram auth hash invalid'}), 401
            auth_date = int(data.get('auth_date', 0))
            if time.time() - auth_date > 86400:
                return jsonify({'error': 'Telegram auth data expired'}), 401
        except Exception as e:
            return jsonify({'error': f'Hash verification error: {e}'}), 400

    user_id = f"tg_{tg_id}"

    if not ext.HAS_AUTH:
        import base64 as _b64, json as _json
        minimal = _b64.urlsafe_b64encode(_json.dumps({
            'user_id': user_id, 'sub': user_id, 'exp': int(time.time()) + 86400 * 30,
        }).encode()).decode()
        return jsonify({'access_token': minimal, 'user_id': user_id, 'token_type': 'Bearer'})

    try:
        from middleware.auth import _make_token

        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            from users.user_store import ensure_user_tables
            ensure_user_tables(conn)
            conn.execute(
                """INSERT OR IGNORE INTO user_preferences
                   (user_id, onboarding_complete, tier) VALUES (?, 0, 'free')""",
                (user_id,),
            )
            conn.execute(
                "UPDATE user_preferences SET telegram_chat_id=? WHERE user_id=?",
                (str(tg_id), user_id),
            )
            try:
                conn.execute("CREATE TABLE IF NOT EXISTS user_auth (user_id TEXT PRIMARY KEY, email TEXT UNIQUE, password_hash TEXT, created_at TEXT)")
            except Exception:
                pass
            conn.execute(
                """INSERT OR IGNORE INTO user_auth
                   (user_id, email, password_hash, created_at)
                   VALUES (?, ?, '', datetime('now'))""",
                (user_id, f"{user_id}@telegram.local"),
            )
            conn.commit()
        finally:
            conn.close()

        access_token = _make_token(user_id, f"{user_id}@telegram.local")
        resp = jsonify({
            'access_token': access_token,
            'user_id':      user_id,
            'token_type':   'Bearer',
            'first_name':   data.get('first_name', ''),
            'username':     data.get('username', ''),
        })
        _set_auth_cookies(resp, access_token, '')
        return resp
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/auth/me', methods=['GET'])
@ext.require_auth
def auth_me():
    """GET /auth/me — returns the authenticated user's profile."""
    user_id = g.user_id
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT email, first_name, last_name, phone FROM user_auth WHERE user_id=?",
            (user_id,)
        ).fetchone()
        conn.close()
    except Exception:
        row = None
    base = {'user_id': user_id, 'email': g.user_email}
    if row:
        base['email']      = row[0] or g.user_email
        base['first_name'] = row[1] or ''
        base['last_name']  = row[2] or ''
        base['phone']      = row[3] or ''
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify(base)
    try:
        user = ext.get_user(ext.DB_PATH, user_id)
        if user:
            user.update(base)
            return jsonify(user)
        return jsonify(base)
    except Exception:
        return jsonify(base)


@bp.route('/auth/change-password', methods=['POST'])
@ext.require_auth
def change_password():
    """POST /auth/change-password — Body: { "current_password": "...", "new_password": "..." }"""
    if not ext.HAS_AUTH:
        return jsonify({'error': 'auth not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    current_pw = str(data.get('current_password', ''))
    new_pw     = str(data.get('new_password', ''))
    if not current_pw or not new_pw:
        return jsonify({'error': 'current_password and new_password are required'}), 400
    if len(new_pw) < 8:
        return jsonify({'error': 'new password must be at least 8 characters'}), 400
    try:
        import bcrypt as _bcrypt
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT password_hash FROM user_auth WHERE user_id=?", (g.user_id,)
        ).fetchone()
        if not row:
            conn.close()
            return jsonify({'error': 'user not found'}), 404
        if not _bcrypt.checkpw(current_pw.encode(), row[0].encode()):
            conn.close()
            return jsonify({'error': 'current password is incorrect'}), 401
        new_hash = _bcrypt.hashpw(new_pw.encode(), _bcrypt.gensalt(rounds=12)).decode()
        conn.execute(
            "UPDATE user_auth SET password_hash=? WHERE user_id=?", (new_hash, g.user_id)
        )
        conn.commit()
        conn.close()
        ext.log_audit_event(ext.DB_PATH, action='password_change', user_id=g.user_id,
                            ip_address=request.remote_addr,
                            user_agent=request.user_agent.string,
                            outcome='success')
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True})


@bp.route('/admin/users/<target_user_id>/set-dev', methods=['POST'])
@ext.require_auth
def admin_set_dev(target_user_id):
    """POST /admin/users/<target_user_id>/set-dev — toggle is_dev flag."""
    _admin_ids = {
        uid.strip()
        for uid in os.environ.get('ADMIN_USER_IDS', '').split(',')
        if uid.strip()
    }
    if not _admin_ids or g.user_id not in _admin_ids:
        return jsonify({'error': 'forbidden'}), 403

    data    = request.get_json(force=True, silent=True) or {}
    is_dev  = bool(data.get('is_dev', False))

    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        from users.user_store import set_user_dev
        set_user_dev(ext.DB_PATH, target_user_id, is_dev)
        return jsonify({'ok': True, 'user_id': target_user_id, 'is_dev': is_dev})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/dev/upgrade-premium', methods=['POST'])
def dev_upgrade_premium():
    """POST /dev/upgrade-premium — TEMPORARY: self-upgrade to premium for testing."""
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    user_id = getattr(g, 'user_id', None)
    if not user_id:
        if ext.HAS_AUTH:
            try:
                from middleware.auth import _decode_token
                _tok = request.cookies.get('tg_access', '') or \
                       request.headers.get('Authorization', '').removeprefix('Bearer ').strip()
                if _tok:
                    user_id = _decode_token(_tok).get('user_id')
            except Exception:
                pass
    if not user_id:
        body = request.get_json(force=True, silent=True) or {}
        user_id = body.get('user_id')
    if not user_id:
        return jsonify({'error': 'user_id required'}), 400
    try:
        from users.user_store import set_user_tier as _set_tier
        _set_tier(ext.DB_PATH, user_id, 'premium')
        return jsonify({'ok': True, 'tier': 'premium', 'user_id': user_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

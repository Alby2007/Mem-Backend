"""routes_v2/auth.py — Phase 2: auth endpoints.

Gate: smoke test 7/7 pass against :8001.
"""

from __future__ import annotations

import os
import secrets
import sqlite3
import time

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user
from middleware.fastapi_rate_limiter import RATE_LIMITS, limiter

router = APIRouter()

# ── Shared login-code state ────────────────────────────────────────────────────
_TG_LOGIN_CODES: dict = {}


# ── Cookie helpers ─────────────────────────────────────────────────────────────

def _set_auth_cookies(response: Response, access_token: str, refresh_token: str) -> None:
    response.set_cookie(
        "tg_access", access_token,
        httponly=True, secure=True, samesite="none", path="/", max_age=86400,
    )
    if refresh_token:
        response.set_cookie(
            "tg_refresh", refresh_token,
            httponly=True, secure=True, samesite="none", path="/auth/refresh", max_age=2592000,
        )


def _clear_auth_cookies(response: Response) -> None:
    response.set_cookie("tg_access",  value="", httponly=True, secure=True,
                        samesite="none", path="/", max_age=0)
    response.set_cookie("tg_refresh", value="", httponly=True, secure=True,
                        samesite="none", path="/auth/refresh", max_age=0)


# ── Request models ─────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    user_id: str
    email: str
    password: str
    beta_password: str = ""


class LoginRequest(BaseModel):
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str = ""


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class TelegramVerifyRequest(BaseModel):
    code: str


class SetDevRequest(BaseModel):
    is_dev: bool = False


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.post("/auth/register", status_code=201)
@limiter.limit(RATE_LIMITS["auth"])
async def auth_register(request: Request, data: RegisterRequest, response: Response):
    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available — install PyJWT and bcrypt")

    _beta_secret = os.environ.get("BETA_PASSWORD", "")
    if _beta_secret and data.beta_password != _beta_secret:
        ext.log_audit_event(ext.DB_PATH, action="register",
                            ip_address=request.client.host if request.client else None,
                            user_agent=request.headers.get("user-agent"),
                            outcome="failure", detail={"reason": "invalid beta password"})
        raise HTTPException(403, detail="Invalid beta access password.")

    if ext.HAS_VALIDATORS:
        result = ext.validate_register(data.model_dump())
        if not result.valid:
            raise HTTPException(400, detail={"error": "validation_failed", "details": result.errors})

    try:
        row = ext.register_user(ext.DB_PATH, data.user_id, data.email, data.password)
    except ValueError as e:
        ext.log_audit_event(ext.DB_PATH, action="register",
                            ip_address=request.client.host if request.client else None,
                            user_agent=request.headers.get("user-agent"),
                            outcome="failure", detail={"reason": str(e)})
        status = 409 if "already registered" in str(e) else 400
        raise HTTPException(status, detail=str(e))

    if ext.HAS_PRODUCT_LAYER:
        try:
            ext.create_user(ext.DB_PATH, data.user_id)
        except Exception:
            pass

    ext.log_audit_event(ext.DB_PATH, action="register", user_id=data.user_id,
                        ip_address=request.client.host if request.client else None,
                        user_agent=request.headers.get("user-agent"),
                        outcome="success")
    return row


@router.post("/auth/token")
@limiter.limit(RATE_LIMITS["auth"])
async def auth_token(request: Request, data: LoginRequest, response: Response):
    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available — install PyJWT and bcrypt")
    if not data.email or not data.password:
        raise HTTPException(400, detail="email and password are required")

    try:
        token_data = ext.authenticate_user(ext.DB_PATH, data.email, data.password)
    except ValueError as e:
        ext.log_audit_event(ext.DB_PATH, action="login_failure",
                            ip_address=request.client.host if request.client else None,
                            user_agent=request.headers.get("user-agent"),
                            outcome="failure", detail={"email": data.email, "reason": str(e)})
        raise HTTPException(401, detail=str(e))

    ext.log_audit_event(ext.DB_PATH, action="login_success", user_id=token_data["user_id"],
                        ip_address=request.client.host if request.client else None,
                        user_agent=request.headers.get("user-agent"),
                        outcome="success")
    try:
        refresh_data = ext.issue_refresh_token(ext.DB_PATH, token_data["user_id"])
        token_data["refresh_token"]         = refresh_data["refresh_token"]
        token_data["refresh_token_expires"] = refresh_data["expires_at"]
    except Exception:
        pass

    _set_auth_cookies(response, token_data["access_token"],
                      token_data.get("refresh_token", ""))
    return token_data


@router.post("/auth/refresh")
@limiter.limit(RATE_LIMITS["auth"])
async def auth_refresh(request: Request, data: RefreshRequest, response: Response):
    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available")
    refresh_token = data.refresh_token.strip()
    if not refresh_token:
        refresh_token = request.cookies.get("tg_refresh", "").strip()
    if not refresh_token:
        raise HTTPException(400, detail="refresh_token is required")
    try:
        result = ext.rotate_refresh_token(ext.DB_PATH, refresh_token)
        ext.log_audit_event(ext.DB_PATH, action="token_refresh", user_id=result["user_id"],
                            ip_address=request.client.host if request.client else None,
                            user_agent=request.headers.get("user-agent"),
                            outcome="success")
        _set_auth_cookies(response, result["access_token"], result["refresh_token"])
        return result
    except ValueError as e:
        raise HTTPException(
            401,
            detail={"error": "token_expired" if "expired" in str(e) else "invalid_token",
                    "detail": str(e)},
        )


@router.post("/auth/logout")
async def auth_logout(
    request: Request,
    response: Response,
    data: RefreshRequest = RefreshRequest(),
    user_id: str = Depends(get_current_user),
):
    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available")
    refresh_token = data.refresh_token.strip()
    if refresh_token:
        try:
            conn = sqlite3.connect(ext.DB_PATH, timeout=10)
            conn.execute(
                "UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ? AND user_id = ?",
                (refresh_token, user_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
    ext.log_audit_event(ext.DB_PATH, action="logout", user_id=user_id,
                        ip_address=request.client.host if request.client else None,
                        user_agent=request.headers.get("user-agent"),
                        outcome="success")
    _clear_auth_cookies(response)
    return {"logged_out": True}


@router.post("/auth/telegram/code")
async def auth_telegram_code():
    code = secrets.token_hex(4).upper()
    _TG_LOGIN_CODES[code] = {"chat_id": None, "user_data": None, "expires": time.time() + 300}
    expired = [k for k, v in _TG_LOGIN_CODES.items() if v["expires"] < time.time()]
    for k in expired:
        del _TG_LOGIN_CODES[k]
    return {"code": code}




@router.post("/auth/telegram/link-code")
async def auth_telegram_link_code(request: Request):
    """
    Authenticated: generate a 5-minute link code for an existing Meridian account.
    User sends /link CODE to the Oracle bot which writes their chat_id and confirms.
    Returns: {code, bot_username, expires_in, instruction}
    """
    user_id = await get_current_user(request)
    if not user_id:
        raise HTTPException(401, detail="Authentication required")

    code = secrets.token_hex(4).upper()
    expires_at = time.time() + 300

    try:
        from routes_v2.telegram import _TG_LINK_CODES
        expired_keys = [k for k, v in _TG_LINK_CODES.items() if v["expires"] < time.time()]
        for k in expired_keys:
            del _TG_LINK_CODES[k]
        _TG_LINK_CODES[code] = {"user_id": user_id, "expires": expires_at}
    except Exception as e:
        raise HTTPException(500, detail=f"Could not create link code: {e}")

    bot_username = None
    try:
        import requests as _rq
        _token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        if _token:
            _me = _rq.get(f"https://api.telegram.org/bot{_token}/getMe", timeout=5)
            if _me.status_code == 200:
                bot_username = _me.json().get("result", {}).get("username")
    except Exception:
        pass

    return {
        "code":         code,
        "bot_username": bot_username,
        "expires_in":   300,
        "instruction":  f"Send /link {code} to @{bot_username or 'TradingGalaxyBot'} on Telegram",
    }

@router.post("/auth/telegram/verify")
async def auth_telegram_verify(data: TelegramVerifyRequest, response: Response):
    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available — install PyJWT and bcrypt")

    code = data.code.strip().upper()
    entry = _TG_LOGIN_CODES.get(code)
    if not entry:
        raise HTTPException(400, detail="Invalid code")
    if time.time() > entry["expires"]:
        del _TG_LOGIN_CODES[code]
        raise HTTPException(400, detail="Code expired")
    if not entry.get("chat_id"):
        raise HTTPException(202, detail="Code not yet confirmed — send it to the bot first")

    tg_data = entry.get("user_data") or {}
    chat_id = entry["chat_id"]
    del _TG_LOGIN_CODES[code]
    user_id = f"tg_{chat_id}"

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

    try:
        from middleware.auth import _make_token
        access_token = _make_token(user_id, f"{user_id}@telegram.local")
    except Exception as e:
        raise HTTPException(500, detail=f"token generation failed: {e}")

    if access_token.startswith("eyJ"):
        _set_auth_cookies(response, access_token, "")

    return {
        "access_token": access_token,
        "user_id":      user_id,
        "first_name":   tg_data.get("first_name", ""),
        "username":     tg_data.get("username", ""),
        "tg_data":      tg_data,
    }


@router.post("/auth/telegram")
async def auth_telegram(request: Request, response: Response):
    import hashlib
    import hmac

    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available — install PyJWT and bcrypt")

    data = await request.json()
    tg_id = data.get("id")
    if not tg_id:
        raise HTTPException(400, detail="Telegram auth data missing id")

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if bot_token:
        try:
            check_hash = data.get("hash", "")
            data_check = {k: v for k, v in data.items() if k != "hash"}
            data_check_str = "\n".join(f"{k}={v}" for k, v in sorted(data_check.items()))
            secret = hashlib.sha256(bot_token.encode()).digest()
            computed = hmac.new(secret, data_check_str.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(computed, check_hash):
                raise HTTPException(401, detail="Telegram auth hash invalid")
            auth_date = int(data.get("auth_date", 0))
            if time.time() - auth_date > 86400:
                raise HTTPException(401, detail="Telegram auth data expired")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, detail=f"Hash verification error: {e}")

    user_id = f"tg_{tg_id}"

    try:
        from middleware.auth import _make_token
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
                (str(tg_id), user_id),
            )
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO user_auth (user_id, email, password_hash, created_at) "
                    "VALUES (?, ?, '', datetime('now'))",
                    (user_id, f"{user_id}@telegram.local"),
                )
            except Exception:
                pass
            conn.commit()
        finally:
            conn.close()

        access_token = _make_token(user_id, f"{user_id}@telegram.local")
        _set_auth_cookies(response, access_token, "")
        return {
            "access_token": access_token,
            "user_id":      user_id,
            "token_type":   "Bearer",
            "first_name":   data.get("first_name", ""),
            "username":     data.get("username", ""),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/auth/me")
async def auth_me(user_id: str = Depends(get_current_user)):
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT email, first_name, last_name, phone FROM user_auth WHERE user_id=?",
            (user_id,)
        ).fetchone()
        conn.close()
    except Exception:
        row = None

    base: dict = {"user_id": user_id, "email": ""}
    if row:
        base["email"]      = row[0] or ""
        base["first_name"] = row[1] or ""
        base["last_name"]  = row[2] or ""
        base["phone"]      = row[3] or ""

    if not ext.HAS_PRODUCT_LAYER:
        return base
    try:
        user = ext.get_user(ext.DB_PATH, user_id)
        if user:
            user.update(base)
            return user
        return base
    except Exception:
        return base


@router.post("/auth/change-password")
async def change_password(
    request: Request,
    data: ChangePasswordRequest,
    user_id: str = Depends(get_current_user),
):
    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available")
    if len(data.new_password) < 8:
        raise HTTPException(400, detail="new password must be at least 8 characters")
    try:
        import bcrypt as _bcrypt
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT password_hash FROM user_auth WHERE user_id=?", (user_id,)
        ).fetchone()
        if not row:
            conn.close()
            raise HTTPException(404, detail="user not found")
        if not _bcrypt.checkpw(data.current_password.encode(), row[0].encode()):
            conn.close()
            raise HTTPException(401, detail="current password is incorrect")
        new_hash = _bcrypt.hashpw(data.new_password.encode(), _bcrypt.gensalt(rounds=12)).decode()
        conn.execute("UPDATE user_auth SET password_hash=? WHERE user_id=?", (new_hash, user_id))
        conn.commit()
        conn.close()
        # Revoke all existing access tokens — old tokens invalid immediately
        try:
            from middleware.auth import revoke_user_tokens
            revoke_user_tokens(ext.DB_PATH, user_id)
        except Exception:
            pass
        ext.log_audit_event(ext.DB_PATH, action="password_change", user_id=user_id,
                            ip_address=request.client.host if request.client else None,
                            user_agent=request.headers.get("user-agent"),
                            outcome="success")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    return {"ok": True}


@router.post("/dev/upgrade-premium")
async def dev_upgrade_premium(request: Request):
    """
    POST /dev/upgrade-premium — upgrade a user to premium tier.

    ONLY active when DEV_UPGRADE_KEY is set in the environment AND the
    request originates from localhost (127.0.0.1 or ::1).
    Used exclusively by the eval harness so quota enforcement doesn't block test users.
    """
    dev_key = os.environ.get("DEV_UPGRADE_KEY", "")
    if not dev_key:
        raise HTTPException(404)

    client_ip = request.client.host if request.client else ""
    if client_ip not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(404)

    provided = request.headers.get("X-Dev-Key", "")
    if not provided or provided != dev_key:
        raise HTTPException(403, detail="forbidden")

    data = await request.json()
    user_id = (data.get("user_id") or "").strip()
    if not user_id:
        raise HTTPException(400, detail="user_id required")

    if not ext.HAS_PRODUCT_LAYER:
        return {"ok": True, "skipped": True, "reason": "product layer not available"}

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences (user_id, tier) VALUES (?, 'premium')",
            (user_id,),
        )
        conn.execute(
            "UPDATE user_preferences SET tier='premium' WHERE user_id=?",
            (user_id,),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    return {"ok": True, "user_id": user_id, "tier": "premium"}


class GoogleAuthRequest(BaseModel):
    credential: str


@router.get("/auth/google/client-id")
async def google_client_id():
    cid = os.environ.get("GOOGLE_CLIENT_ID", "")
    if not cid:
        raise HTTPException(503, detail="Google OAuth not configured")
    return {"client_id": cid}


@router.post("/auth/google")
@limiter.limit(RATE_LIMITS["auth"])
async def auth_google(request: Request, data: GoogleAuthRequest, response: Response):
    if not ext.HAS_AUTH:
        raise HTTPException(503, detail="auth not available")

    google_client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    if not google_client_id:
        raise HTTPException(503, detail="Google OAuth not configured")

    # 1. Verify token with Google
    try:
        import requests as _rq
        r = _rq.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"id_token": data.credential},
            timeout=8,
        )
        if r.status_code != 200:
            raise HTTPException(401, detail="Invalid Google token")
        gdata = r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(401, detail=f"Google token verification failed: {e}")

    # 2. Validate audience
    if gdata.get("aud") != google_client_id:
        raise HTTPException(401, detail="Token audience mismatch")

    google_sub   = gdata.get("sub", "")
    google_email = gdata.get("email", "").lower().strip()
    google_name  = gdata.get("name", "")

    if not google_sub or not google_email:
        raise HTTPException(401, detail="Incomplete Google token claims")

    try:
        from datetime import datetime, timezone
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        now_iso = datetime.now(timezone.utc).isoformat()

        # 3. Look up by oauth_sub
        row = conn.execute(
            "SELECT user_id, email FROM user_auth "
            "WHERE oauth_provider='google' AND oauth_sub=?",
            (google_sub,)
        ).fetchone()

        is_new_user = False

        if row:
            user_id = row[0]
            email   = row[1]
        else:
            # Check if email exists with password account
            existing = conn.execute(
                "SELECT user_id, oauth_provider FROM user_auth WHERE email=?",
                (google_email,)
            ).fetchone()

            if existing and not existing[1]:
                conn.close()
                raise HTTPException(409, detail="account_exists_with_password")

            # New account — derive unique user_id from email prefix
            base   = google_email.split('@')[0]
            base   = ''.join(c for c in base if c.isalnum()).lower()[:12]
            suffix = secrets.token_hex(2)
            user_id = f"{base}_{suffix}"
            email   = google_email

            conn.execute(
                "INSERT INTO user_auth "
                "(user_id, email, password_hash, oauth_provider, oauth_sub, display_name, created_at) "
                "VALUES (?, ?, '__oauth__', 'google', ?, ?, ?)",
                (user_id, email, google_sub, google_name, now_iso)
            )
            conn.commit()

            if ext.HAS_PRODUCT_LAYER:
                try:
                    ext.create_user(ext.DB_PATH, user_id)
                except Exception:
                    pass

            is_new_user = True
            ext.log_audit_event(ext.DB_PATH, action="register_oauth", user_id=user_id,
                                ip_address=request.client.host if request.client else None,
                                user_agent=request.headers.get("user-agent"),
                                outcome="success", detail={"provider": "google"})

        # 4. Update last_login
        conn.execute(
            "UPDATE user_auth SET last_login=? WHERE user_id=?",
            (now_iso, user_id)
        )
        conn.commit()
        conn.close()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    # 5. Issue tokens
    from middleware.auth import _make_token, issue_refresh_token
    access_token  = _make_token(user_id, email, db_path=ext.DB_PATH)
    refresh_data  = issue_refresh_token(ext.DB_PATH, user_id)
    refresh_token = refresh_data.get("refresh_token", "")

    _set_auth_cookies(response, access_token, refresh_token)

    ext.log_audit_event(ext.DB_PATH, action="login_oauth", user_id=user_id,
                        ip_address=request.client.host if request.client else None,
                        user_agent=request.headers.get("user-agent"),
                        outcome="success", detail={"provider": "google"})

    return {
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "token_type":    "bearer",
        "user_id":       user_id,
        "email":         email,
        "display_name":  google_name,
        "is_new_user":   is_new_user,
    }


@router.post("/admin/users/{target_user_id}/set-dev")
async def admin_set_dev(
    target_user_id: str,
    data: SetDevRequest,
    user_id: str = Depends(get_current_user),
):
    _admin_ids = {u.strip() for u in os.environ.get("ADMIN_USER_IDS", "").split(",") if u.strip()}
    if not _admin_ids or user_id not in _admin_ids:
        raise HTTPException(403, detail="forbidden")
    if not ext.HAS_PRODUCT_LAYER:
        raise HTTPException(503, detail="product layer not available")
    try:
        from users.user_store import set_user_dev
        set_user_dev(ext.DB_PATH, target_user_id, data.is_dev)
        return {"ok": True, "user_id": target_user_id, "is_dev": data.is_dev}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

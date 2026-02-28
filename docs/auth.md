# Authentication — Frontend Integration Guide

## Overview

The API uses **JWT Bearer tokens** for authentication. Every protected endpoint
requires an `Authorization` header. Tokens are short-lived (default 24 h);
a long-lived refresh token is issued alongside the access token so the frontend
can renew silently without forcing re-login.

---

## Token lifecycle

```
POST /auth/register   →  create account
POST /auth/token      →  { access_token, refresh_token, expires_in }
                               │                │
                        attach to every     store securely
                        API request         (HttpOnly cookie or
                        as Bearer header    secure storage)
                               │
                        expires after JWT_EXPIRY_HOURS (default 24 h)
                               │
                        GET any endpoint → 401 { "error": "token_expired" }
                               │
                        POST /auth/refresh  →  new { access_token, refresh_token }
                               │
                        refresh expires after JWT_REFRESH_EXPIRY_DAYS (default 30 d)
                               │
                        POST /auth/token  (full re-login required)
```

---

## Endpoints

### `POST /auth/register`

Create a new account.

```http
POST /auth/register
Content-Type: application/json

{
  "user_id":  "alice",
  "email":    "alice@example.com",
  "password": "s3cur3P@ss"
}
```

**Response 201**
```json
{
  "user_id":    "alice",
  "email":      "alice@example.com",
  "created_at": "2026-02-25T07:00:00+00:00"
}
```

**Error responses**

| Status | `error` value | Meaning |
|--------|--------------|---------|
| 400 | `validation_failed` | Email format invalid, password < 8 chars, user_id missing |
| 409 | `"email already registered: ..."` | Duplicate email |

---

### `POST /auth/token`

Authenticate and obtain tokens.

```http
POST /auth/token
Content-Type: application/json

{
  "email":    "alice@example.com",
  "password": "s3cur3P@ss"
}
```

**Response 200**
```json
{
  "access_token":           "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token":          "R4nd0m0p4qu3Str1ng...",
  "refresh_token_expires":  "2026-03-26T07:00:00+00:00",
  "token_type":             "Bearer",
  "expires_in":             86400,
  "user_id":                "alice"
}
```

**Error responses**

| Status | `error` value | Meaning |
|--------|--------------|---------|
| 400 | `"email and password are required"` | Missing fields |
| 401 | `"invalid email or password"` | Wrong credentials |
| 401 | `"account temporarily locked — try again later"` | 5 failed attempts → 15-min lockout |

> **Do not distinguish between "user not found" and "wrong password" in your UI.**
> Both return the same 401 message intentionally (prevents user enumeration).

---

### `POST /auth/refresh`

Exchange a refresh token for a new access token + refresh token pair.
**The old refresh token is revoked immediately** — store the new one.

```http
POST /auth/refresh
Content-Type: application/json

{
  "refresh_token": "R4nd0m0p4qu3Str1ng..."
}
```

**Response 200** — same shape as `/auth/token` response.

**Error responses**

| Status | `error` value | Meaning |
|--------|--------------|---------|
| 400 | `"refresh_token is required"` | Missing field |
| 401 | `"token_expired"` | Refresh token past its expiry — force full re-login |
| 401 | `"invalid_token"` | Token not found or already revoked |

---

### `POST /auth/logout`

Revoke the refresh token. Access token expires naturally.

```http
POST /auth/logout
Authorization: Bearer eyJ...
Content-Type: application/json

{
  "refresh_token": "R4nd0m0p4qu3Str1ng..."
}
```

**Response 200**
```json
{ "logged_out": true }
```

After receiving 200, **discard both tokens from all local storage** immediately.

---

### `GET /auth/me`

Returns the authenticated user's profile. Useful for session restore on app boot.

```http
GET /auth/me
Authorization: Bearer eyJ...
```

**Response 200**
```json
{
  "user_id":            "alice",
  "onboarding_complete": 1,
  "telegram_chat_id":   "123456789",
  "delivery_time":      "08:00",
  "timezone":           "Europe/London",
  "selected_sectors":   ["technology", "financials"],
  "selected_risk":      "moderate"
}
```

---

## Making authenticated requests

**Every protected endpoint** requires:

```http
Authorization: Bearer <access_token>
```

There is no cookie-based auth. The header must be present on every request.

### JavaScript / fetch example

```js
const API = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
    ? ''
    : 'https://api.trading-galaxy.uk';

async function apiFetch(path, options = {}) {
  const token = localStorage.getItem('access_token');
  let res;
  try {
    res = await fetch(API + path, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      ...(options.headers || {}),
    },
  });

  } catch (e) {
    showToast('Connection error — check your internet');
    return null;
  }
  if (res.status === 429) {
    showToast('Too many requests — please wait a moment');
    return null;
  }
  if (res.status === 401) {
    const body = await res.json();
    if (body.error === 'token_expired') {
      // Attempt silent refresh
      const refreshed = await tryRefresh();
      if (refreshed) {
        // Retry original request once with new token
        return apiFetch(path, options);
      }
    }
    // Refresh failed or token invalid — force re-login
    redirectToLogin();
    return null;
  }

  return res;
}

async function tryRefresh() {
  const rt = localStorage.getItem('refresh_token');
  if (!rt) return false;
  const res = await fetch('/auth/refresh', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refresh_token: rt }),
  });
  if (!res.ok) return false;
  const data = await res.json();
  sessionStorage.setItem('access_token', data.access_token);
  localStorage.setItem('refresh_token', data.refresh_token);
  return true;
}
```

---

## 401 error semantics — critical distinction

The API returns `401` for two distinct situations. **Handle them differently.**

| `error` value | Meaning | UI action |
|---------------|---------|-----------|
| `"token_expired"` | Access token past its expiry | Call `POST /auth/refresh` silently; retry original request |
| `"invalid_token"` | Malformed token or missing `Authorization` header | Show login screen — do not retry |
| `"token_expired"` on `/auth/refresh` | Refresh token also expired (after 30 d of inactivity) | Show login screen — full re-auth required |
| `"invalid_token"` on `/auth/refresh` | Refresh token revoked or never existed | Show login screen |

> A 401 does **not** mean wrong credentials. Wrong credentials only come from
> `POST /auth/token` and always include a human-readable `error` string.
> Treat 401 on any other endpoint as a token lifecycle event, not a credentials
> failure — the user does not need to see "incorrect password".

---

## Token storage recommendations

| Storage | Access token | Refresh token | Notes |
|---------|-------------|---------------|-------|
| `localStorage` | ✅ **current implementation** | ✅ **current implementation** | Survives tab close — correct for an all-day dashboard |
| `sessionStorage` | ⚠️ | ❌ | Cleared on tab close; forces re-login per session (bad UX for dashboard) |
| HttpOnly cookie | ✅ best | ✅ best | Requires backend to set cookie — not current implementation |
| In-memory (React state) | ✅ safest | ❌ | Lost on refresh — not suitable for SPA without service worker |

**Current implementation:** both tokens stored in `localStorage` under `tg_token` and `tg_user_id`. This is correct for a dashboard used all day — re-login on every tab close would be unacceptable UX. The XSS risk is mitigated by the `Content-Security-Policy` header in the frontend.

---

## Security headers

Every API response includes:

```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
Referrer-Policy: strict-origin-when-cross-origin
Content-Security-Policy: default-src 'none'
```

---

## Rate limits (auth endpoints)

Auth endpoints are rate-limited to **10 requests / minute per IP**.
On limit breach the API returns `429 Too Many Requests`.
Implement exponential back-off — do not retry immediately.

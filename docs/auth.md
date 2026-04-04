# Authentication — Frontend Integration Guide

## Overview

The API uses **HttpOnly cookies** for authentication. On login, the backend sets two cookies:

| Cookie | Lifetime | Purpose |
|---|---|---|
| `tg_access` | 24 hours | JWT access token — sent automatically on every request |
| `tg_refresh` | 30 days | Opaque refresh token — used to renew the access token |

Both cookies are `HttpOnly`, `Secure`, `SameSite=None` — required for cross-origin use between the Cloudflare Pages frontend (`trading-galaxy.uk`) and the OCI API (`api.trading-galaxy.uk`).

The frontend does **not** store tokens in `localStorage`. Only non-sensitive display data (`tg_user_id`, `tg_user_data`) is stored there.

---

## Token lifecycle

```
POST /auth/token      →  sets tg_access + tg_refresh cookies
                               │
                        browser sends cookies automatically
                        on every cross-origin request
                        (credentials: 'include' required in fetch)
                               │
                        expires after 24 h
                               │
                        GET /auth/me → 401
                               │
                        POST /auth/refresh  →  new tg_access + tg_refresh cookies
                               │
                        refresh expires after 30 d
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

Cookies are sent automatically by the browser on every request — no `Authorization` header is needed in normal use. The only requirement is `credentials: 'include'` in every `fetch` call.

```js
const API = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
    ? 'http://localhost:5050'
    : 'https://api.trading-galaxy.uk';

let _refreshing = null; // deduplicate concurrent refresh attempts

async function apiFetch(path, options = {}) {
  let res;
  try {
    res = await fetch(API + path, {
      credentials: 'include',        // send HttpOnly cookies cross-origin
      ...options,
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    });
  } catch {
    showToast('Connection error — check your internet');
    return null;
  }
  if (res.status === 429) { showToast('Too many requests — please wait'); return null; }
  if (res.status === 401) {
    // Single shared refresh attempt — avoids race on concurrent calls
    if (!_refreshing) {
      _refreshing = fetch(API + '/auth/refresh', {
        method: 'POST', credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
      }).finally(() => { _refreshing = null; });
    }
    const ref = await _refreshing;
    if (ref && ref.ok) return apiFetch(path, options); // retry once with new cookie
    _handleSessionExpired();
    return null;
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}
```

---

## Session restore on app boot

On every page load the SPA calls `GET /auth/me` using the cookie. If it succeeds the user is considered authenticated; if it returns 401, the silent refresh is attempted once before showing the login screen.

```js
// Boot sequence (runs once on DOMContentLoaded)
try {
  const me = await apiFetch('/auth/me');
  if (me && me.user_id) {
    state.userId = me.user_id;
    state.isDev  = !!me.is_dev;
    state.tier   = (me.tier || 'free').toLowerCase();
    state.token  = '__cookie__'; // sentinel — actual token is in HttpOnly cookie
    navigate('dashboard');
    return;
  }
} catch { /* fall through */ }
showScreen('auth'); // show login form
```

---

## 401 error semantics

| Situation | Meaning | Handled by |
|---|---|---|
| 401 on any endpoint | Access cookie expired or missing | `apiFetch` attempts silent refresh via `POST /auth/refresh` |
| 401 on `/auth/refresh` | Refresh cookie expired (30 d) or revoked | `_handleSessionExpired()` — shows login screen in-place |
| 401 on `POST /auth/token` | Wrong credentials | Separate login form — user sees error message |

> A 401 is **never** shown to the user as "incorrect password" — that message only comes from the login form's explicit credential check against `POST /auth/token`.

---

## Token storage

| Where | What | Why |
|---|---|---|
| `tg_access` cookie (HttpOnly, Secure, SameSite=None) | JWT access token | Not accessible to JS — XSS-safe |
| `tg_refresh` cookie (HttpOnly, Secure, SameSite=None) | Refresh token | Not accessible to JS — XSS-safe |
| `localStorage['tg_user_id']` | User ID string | Display only — not a secret |
| `localStorage['tg_user_data']` | Cached profile JSON | Display only — not a secret |

`localStorage` access is wrapped in `try/catch` throughout the SPA to gracefully handle restrictive browser contexts (private browsing on iOS, locked-down enterprise browsers).

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

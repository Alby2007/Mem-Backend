# Frontend — Comprehensive Reference

**File:** `static/login/index.html` (CSS: `static/login/css/app.css`, JS: `static/login/js/*.js`)  
**Served at:** `https://trading-galaxy.uk/login` + all SPA routes (Cloudflare Pages — project `mem-backend2`, publish dir `static/`)  
**Architecture:** Zero build step, zero dependencies, zero bundler. CSS and JS extracted into separate files, loaded via `<link>` and `<script src>` tags.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Technology stack](#2-technology-stack)
3. [Design system](#3-design-system)
4. [Layout structure](#4-layout-structure)
5. [Screens](#5-screens)
6. [State management](#6-state-management)
7. [API layer](#7-api-layer)
8. [Authentication](#8-authentication)
9. [Navigation](#9-navigation)
10. [Utility functions](#10-utility-functions)
11. [Content Security Policy](#11-content-security-policy)
12. [Local storage](#12-local-storage)
13. [Adding a new screen](#13-adding-a-new-screen)

**Screens:** Auth · Dashboard · Portfolio · Chat · Tips · Patterns · Network · History · **Paper Trader** · Subscription · Profile

---

## 1. Overview

Trading Galaxy's frontend is a **Bloomberg-terminal-style SPA** — dark, mono-heavy, data-dense. It runs entirely in one HTML file with no external JS frameworks. All state lives in a single `state` object; all API calls go through a single `apiFetch` wrapper.

**Key design decisions:**
- No React / Vue / Angular — vanilla JS with direct DOM manipulation
- No bundler — the file is served as-is from Netlify CDN
- No separate CSS file — all styles are inline `<style>` in `<head>`
- No external JS libraries — TradingView iframe and Telegram widget are the only external scripts
- Tokens stored in **HttpOnly cookies** (`tg_access`, `tg_refresh`) set by the backend — not accessible to JS
- Only non-sensitive display data (`tg_user_id`, `tg_user_data`) in `localStorage`
- All `localStorage` calls wrapped in `try/catch` for restrictive browser contexts

---

## 2. Technology stack

| Concern | Solution |
|---|---|
| Language | Vanilla ES2022 (async/await, optional chaining, nullish coalescing) |
| Styling | CSS custom properties (variables), no preprocessor |
| Fonts | JetBrains Mono (monospace data), DM Sans (UI text) — Google Fonts, lazy-loaded |
| Charts | TradingView Advanced Chart widget (iframe embed, free tier) |
| Auth widget | Telegram Login Widget (`telegram.org/js/telegram-widget.js`) |
| Hosting | Cloudflare Pages (`mem-backend2`) |
| API | `https://api.trading-galaxy.uk` (production) · `http://localhost:5050` (dev) |

---

## 3. Design system

### CSS custom properties (`:root`)

```css
--bg:       #0a0a0a   /* page background */
--surface:  #111111   /* topbar, sidebar, panels */
--card:     #1a1a1a   /* cards, message bubbles */
--border:   #2a2a2a   /* dividers, input borders */
--accent:   #f59e0b   /* amber — primary highlight, CTA, active state */
--accent2:  #d97706   /* amber hover */
--green:    #10b981   /* bullish, success, connected */
--red:      #ef4444   /* bearish, error, warning */
--blue:     #3b82f6   /* medium-conviction badge */
--muted:    #6b7280   /* secondary text, icons */
--text:     #f5f5f5   /* primary text */
--font-sans: 'DM Sans', system-ui, sans-serif
--mono:     'JetBrains Mono', Consolas, monospace
--sidebar-w: 64px
--topbar-h:  48px
```

### Reusable CSS classes

**Badges** — colour-coded pill labels:

| Class | Colour | Use |
|---|---|---|
| `.badge-high` | Amber | High conviction / high priority |
| `.badge-medium` | Blue | Medium conviction |
| `.badge-low` | Muted grey | Low conviction |
| `.badge-avoid` | Red | Avoid signal |
| `.badge-bullish` | Green | Bullish direction |
| `.badge-bearish` | Red | Bearish direction |
| `.badge-open` | Green | Open position/pattern |

**Status dots:**

| Class | Colour | Use |
|---|---|---|
| `.dot-green` | Green + glow | Healthy / connected / running |
| `.dot-red` | Red | Error / disconnected |
| `.dot-amber` | Amber | Warning / running / pending |
| `.dot-muted` | Grey | Never run / unknown |

**Mono value helpers:**

| Class | Effect |
|---|---|
| `.mono` | JetBrains Mono font |
| `.mono-amber` | Mono + amber colour |
| `.mono-green` | Mono + green colour |
| `.mono-red` | Mono + red colour |
| `.mono-muted` | Mono + muted colour |

**Buttons:**

| Class | Style |
|---|---|
| `.btn-primary` | Amber fill, black text |
| `.btn-outline` | Transparent, border, transitions to amber on hover |
| `.btn-ghost` | Transparent, muted text |
| `.btn-danger` | Transparent, red border |
| `.btn-sm` | Smaller padding/font |
| `.btn:disabled` | 40% opacity, not-allowed cursor |

**Layout:**

| Class | Effect |
|---|---|
| `.two-col` | 2-column grid, 16px gap |
| `.three-col` | 3-column grid, 16px gap |
| `.stat-grid` | 4-column grid for stat cards |
| `.card` | `#1a1a1a` background, border, 6px radius, 16px padding |
| `.card-sm` | Same but 12px padding |
| `.section-title` | 10px uppercase muted label with bottom border |

**Tables:**

| Class | Element |
|---|---|
| `.tbl` | `<table>` — full width, collapsed borders |
| `.tbl th` | 10px uppercase muted header |
| `.tbl td` | 8px vertical padding, hover highlight |

### Stress card colouring

The stress stat card on the Dashboard changes colour class based on conflict ratio:

| Conflict ratio | Class | Colour |
|---|---|---|
| < 30% | `.stress-green` | Green |
| 30–60% | `.stress-amber` | Amber |
| > 60% | `.stress-red` | Red |

---

## 4. Layout structure

```
┌──────────────────────────────────────────────────┐
│  #topbar (48px, fixed)                           │
│  TRADING GALAXY  [spacer]  KB:N  ● CONNECTED     │
│                            [tg-user-chip / login] │
├───────┬──────────────────────────────────────────┤
│ #side │  #main (scrollable)                      │
│ bar   │                                          │
│ 64px  │  .screen.active                          │
│ (exp  │  (one at a time)                         │
│ ands  │                                          │
│ to    │                                          │
│ 180px │                                          │
│ on    │                                          │
│ hover)│                                          │
└───────┴──────────────────────────────────────────┘
│  #toast (fixed bottom-right, auto-hides 4s)      │
```

### Top bar (`#topbar`)

| Element | ID | Purpose |
|---|---|---|
| Wordmark | `.wordmark` | "TRADING GALAXY" in mono amber |
| KB count | `#kb-count` | Live atom count, updated on dashboard load |
| Connection dot | `#conn-dot` | Green (`.ok`) or red (`.err`) |
| Connection label | `#conn-label` | "CONNECTED" / "DISCONNECTED" |
| User badge | `#user-badge` | Shows `userId` when signed in |
| Telegram chip | `#tg-user-chip` | Avatar + name when signed in via Telegram |
| Telegram login btn | `#tg-login-btn` | Shown when signed out |

### Sidebar (`#sidebar`)

Collapses to 64px (icons only), expands to 180px on hover. Labels (`.nav-label`) fade in at `opacity: 1` on hover via CSS transition.

Hidden entirely (`display: none`) when `body.auth-mode` is active.

Each `.nav-item` has `data-screen` attribute matching the screen ID suffix.

### Main area (`#main`)

Fixed position, fills remaining space. Contains all `.screen` divs — only one has `.active` at a time. Scrollable (`overflow-y: auto`).

---

## 5. Screens

### Auth (`#screen-auth`)

Shown on boot if no token in `localStorage`. Hidden sidebar (`body.auth-mode`).

**Two-panel layout:**
- **Register panel** — email, password, confirm password → `POST /auth/register` → auto-fills login email on success
- **Login panel** — email, password → `POST /auth/token` → `_saveSession()` → navigates to Dashboard
- **Telegram login** — widget injected lazily into `#tg-widget-auth` on first show; callback `window.onTelegramAuth(user)` → `POST /auth/telegram`

Session restore on page load: calls `GET /auth/me` using the `tg_access` HttpOnly cookie. If 401, attempts silent refresh via `POST /auth/refresh`. If that also fails, shows the auth screen in-place (`showScreen('auth')`) — no page redirect.

---

### Dashboard (`#screen-dashboard`)

**Stat cards (4-up grid):**

| Card | ID | Source | Update interval |
|---|---|---|---|
| KB Facts | `#s-facts` | `GET /stats` → `total_facts` | 60s |
| Open Patterns | `#s-patterns` | `GET /stats` → `open_patterns` | 60s |
| Market Regime | `#s-regime` | `GET /stats` → top atom regime field | 60s |
| Conflict Stress | `#s-stress` | `GET /stats` → conflicts / total_facts | 60s |

**Conviction tickers** (`#dash-conviction`):  
`GET /portfolio/summary` → `top_conviction[0..2]` → rendered as conviction cards with tier badge and upside %.

**Adapter status** (`#dash-adapters`):  
`GET /ingest/status` → renders each adapter as a row with status dot, name, last-run time, atom count.

**Refresh button** (`#dash-refresh-btn`): calls `refreshDashboard()` immediately.

**Auto-refresh:** `setInterval(refreshDashboard, 60000)` — clears on every `loadDashboard()` call to avoid stacking intervals.

---

### Portfolio (`#screen-portfolio`)

**Three ways to load holdings:**

1. **Screenshot upload** — drag/drop or file input → `POST /users/{id}/history/screenshot` (multipart) → `llava` vision model extracts `[{ticker, quantity, avg_cost}]` → `renderHoldings()`

2. **FTSE sector quick-add** — 5 buttons (`+ FTSE Banks`, `+ FTSE Energy`, `+ FTSE Mining`, `+ FTSE Pharma`, `+ FTSE Tech`) add hardcoded tickers to `state.holdings` without overwriting existing ones

3. **Manual add** — ticker autocomplete (`#p-ticker` + `#p-ac-dropdown`) seeded from `GET /universe/coverage` + `_FTSE_FALLBACK` list → adds row to `state.holdings`

**Test profiles** (dropdown `#p-change-profile-select`):

| Profile key | Label | Holdings |
|---|---|---|
| `uk_banks` | UK Banks | BARC.L, HSBA.L, LLOY.L, NWG.L, STAN.L, LSEG.L |
| `us_tech` | US Tech | AAPL, MSFT, NVDA, GOOGL, META, AMZN, TSLA |
| `global_macro` | Global Macro | SPY, EEM, GLD, TLT, SHEL.L, BP.L, VOD.L |
| `dividend_income` | Dividend Income | ULVR.L, BATS.L, NG.L, TSCO.L, REL.L, AZN.L, GSK.L, BDEV.L |
| `crypto_growth` | Crypto & Growth | MSTR, COIN, NVDA, PLTR, ARKK, SQ, HOOD |
| `commodities` | Commodities & Energy | RIO.L, GLEN.L, AAL.L, BHP.L, SHEL.L, BP.L, GLD, SLV |

**Simulated portfolio banner:** if `localStorage.getItem('simulated_{userId}')` is set, renders an amber `⚠ Simulated` banner with profile metadata. Cleared on "Clear simulated flag" click or on real portfolio submit.

**Generate test portfolio** (`#p-gen-sim-btn`): `POST /users/{id}/portfolio/generate-sim` → API generates a random profile → stores in `localStorage` → renders simulated banner.

**Submit** (`#p-submit-btn`): `POST /users/{id}/portfolio` with `{ holdings: state.holdings }` → clears simulated flag.

**Portfolio model card** (`#p-model-card`): rendered from `GET /users/{id}/portfolio/model` → shows account size, risk tolerance, sectors, holding style, conviction tier.

**Ticker autocomplete:**
- Input: `#p-ticker`
- Dropdown: `#p-ac-dropdown` with class `.open`
- Keyboard nav: up/down arrows change `_acIdx`, Enter selects
- Matches top 8 tickers starting with typed text (case-insensitive)

---

### Chat (`#screen-chat`)

Full-height flex column. Message history scrolls; input bar is pinned to bottom.

**Message flow:**
1. User types in `#chat-input` (textarea, auto-resize)
2. Send on `Enter` (no Shift), or click `#chat-send-btn`
3. `POST /chat` with `{ message, session_id, goal?, topic? }`
4. Response rendered as `.msg-assistant` bubble with `mdToHtml()` formatting
5. If response contains `kb_atoms_used > 0`: `.msg-live-badge` shown ("● live KB data")
6. Pattern tags `[PATTERN:...]` in user messages are rendered as styled pills via `renderUserMsg()`

**Toggles in input bar:**
- **KB toggle** — includes KB retrieval context in the chat request
- **Overlay toggle** — requests overlay card (structured trade idea) alongside the response

**Pattern tag format:**
```
[PATTERN:TICKER DIRECTION TYPE... TIMEFRAME QSCORE ZONELO-HI]
```
Rendered as a styled pill: `⟁ | TICKER | DIRECTION | TYPE | TIMEFRAME · QSCORE · ZONE`

When sent to the chat endpoint, `expandPatternTag()` converts the pill back to a readable sentence for the LLM.

**Markdown rendering** (`mdToHtml()`):

| Markdown | Output |
|---|---|
| `### Heading` | `<h3>` |
| `**bold**` | `<strong>` |
| `*italic*` | `<em>` |
| `` `code` `` | `<code>` |
| `- item` / `* item` | `<ul><li>` |
| `1. item` | `<ul><li>` |
| Blank line | Paragraph break `<p>` |

HTML is escaped before markdown rendering — no XSS risk.

**Session ID:** generated once per page load as `crypto.randomUUID()` (or Date.now() fallback), persisted for the session.

---

### Markets (`#screen-markets`)

Split-panel layout: **TradingView chart** (left, flex-grow) + **KB Intelligence panel** (right, 268px fixed).

**Market categories** (`_MARKET_CATS`):

| Category | Count | Examples |
|---|---|---|
| `indices` | 12 | S&P 500, NASDAQ 100, FTSE 100, VIX, Nikkei 225 |
| `uk_equities` | 24 | BARC, HSBA, LLOY, AZN, SHEL, RIO |
| `us_equities` | 25 | AAPL, MSFT, NVDA, GOOGL, META, JPM, GS |
| `eu_equities` | 19 | SAP, ASML, NESN, NOVO B, BNP, BBVA |
| `commodities` | 16 | Gold, Silver, Crude Oil, Brent, Wheat, Coffee |
| `forex` | 10+ | EUR/USD, GBP/USD, USD/JPY, GBP/EUR |
| `crypto` | 8+ | BTC, ETH, SOL, XRP, BNB, DOGE |
| `bonds` | 6+ | US 10Y, US 2Y, UK 10Y, German 10Y |

**Chip click flow:**
1. Click chip → `_tvCurrentSym` = TradingView symbol, `_tvCurrentKb` = KB ticker
2. `_buildIframe(sym)` → creates `<iframe src="https://api.trading-galaxy.uk/markets/chart?sym=...">` (proxied TradingView widget)
3. `_loadKBPanel(kbTicker)` → `GET /tickers/{ticker}/summary` → populates KB Intelligence panel

**KB Intelligence panel (`#kb-panel`):**

| Row | Source field |
|---|---|
| Signal | `signal_direction` + `signal_confidence` |
| Price target | `price_target` |
| Last price | `last_price` |
| Conviction tier | `conviction_tier` |
| Patterns | Top 3 from `patterns` array |
| Epistemic stress | `stress.composite_stress` → colour-coded bar |

"Ask about [ticker]" button pre-fills the chat input and navigates to the Chat screen.

**Ticker search** (`#markets-search`): filters visible chips by label prefix match.

**Lazy init:** `_marketsInited` flag prevents re-initialising TradingView on repeated screen switches.

---

### Tips / History (`#screen-tips`)

**Tips history** (`loadTipsHistory()`): `GET /users/{id}/tips/history` → renders tip cards.

**Tip card structure:**
- Header: ticker (mono amber, large) + direction + timeframe
- Zone description
- 3-column grid: Entry · Stop · Target (amber / red / green)
- Meta row: Pattern type, regime, confidence, R:R ratio
- Skew warning banner if R:R < 1.5
- Feedback buttons: Hit T1 · Hit T2 · Hit T3 · Stop out · Still open · Skip
  - `POST /users/{id}/tips/{tip_id}/feedback` on click

---

### Patterns (`#screen-patterns`)

`loadPatterns()`: `GET /patterns` → renders detected patterns table.

Columns: Ticker · Direction badge · Pattern type · Timeframe · Quality score · Zone · Detected at

"Chat about pattern" button: renders pattern as `[PATTERN:...]` tag and navigates to Chat screen.

---

### Network (`#screen-network`)

`loadNetwork()`: `GET /users/{id}/network/effects` → renders network effect matrix.

Shows cross-asset correlation and influence scores between holdings.

---

### History (`#screen-history`)

`loadHistory()`: `GET /users/{id}/history` → renders screenshot upload history and extracted holdings.

---

## 6. State management

Single global object:

```js
const state = {
  token:    null,   // JWT access token — set on login, cleared on signOut()
  userId:   null,   // string user ID — set on login
  holdings: [],     // [{ticker, quantity, avg_cost}] — pending before submit
};
```

State is **in-memory only** — no reactive framework. When state changes, functions are called explicitly to re-render the relevant DOM.

---

## 7. API layer

### `API` constant

```js
const API = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
    ? ''
    : 'https://api.trading-galaxy.uk';
```

Same-origin on local dev (empty string), cross-origin HTTPS in production.

### `apiFetch(path, opts)`

Central fetch wrapper — all API calls must go through this:

```js
async function apiFetch(path, opts = {}) {
  const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
  if (state.token) headers['Authorization'] = `Bearer ${state.token}`;
  let res;
  try {
    res = await fetch(API + path, { ...opts, headers });
  } catch (e) {
    showToast('Connection error — check your internet');
    return null;
  }
  if (res.status === 401) { signOut(); showToast('Session expired — please sign in again'); return null; }
  if (res.status === 429) { showToast('Too many requests — please wait a moment'); return null; }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}
```

**Error handling:**

| Condition | Action |
|---|---|
| Network error (fetch throws) | Toast "Connection error", return `null` |
| `401` | `signOut()` + toast "Session expired", return `null` |
| `429` | Toast "Too many requests", return `null` |
| `!res.ok` (other 4xx/5xx) | Throw `Error(data.error \|\| HTTP status)` — caller handles |
| Parse error on `.json()` | Silently falls back to `{}` |

**Always check for `null`** before using the return value — a `null` return means the error was already handled.

---

## 8. Authentication

### Email/password flow

```
Register → POST /auth/register → 201 → auto-fill login email
Login    → POST /auth/token   → { access_token, user_id } → _saveSession()
```

### Telegram flow

```
Widget click → onTelegramAuth(tgData) → POST /auth/telegram → { access_token, user_id }
             → _saveSession(token, userId, tgData)
```

### `_saveSession(token, userId, tgData?)`

```js
state.token  = token;
state.userId = userId;
localStorage.setItem('tg_token',   token);
localStorage.setItem('tg_user_id', userId);
if (tgData) localStorage.setItem('tg_user_data', JSON.stringify(tgData));
_renderTgChip(tgData, userId);
```

### Session restore on page load

On `DOMContentLoaded`:
```js
const t = localStorage.getItem('tg_token');
const u = localStorage.getItem('tg_user_id');
if (t && u) { state.token = t; state.userId = u; showScreen('dashboard'); }
else         { showScreen('auth'); }
```

### `signOut()`

```js
state.token = null; state.userId = null; state.holdings = [];
localStorage.removeItem('tg_token');
localStorage.removeItem('tg_user_id');
localStorage.removeItem('tg_user_data');
localStorage.removeItem(`simulated_${userId}`);
showScreen('auth');
```

### Telegram chip (`#tg-user-chip`)

Shown when signed in. Displays avatar (`photo_url` from Telegram data) + display name. Hides the login button. Click logout icon → `signOut()`.

---

## 9. Navigation

```js
function showScreen(name) {
  // 1. Remove .active from all .screen and .nav-item elements
  // 2. Add .active to #screen-{name} and #nav-{name}
  // 3. Toggle body.auth-mode (hides sidebar when on auth screen)
  // 4. Call screen-specific load function:
  //    dashboard → loadDashboard()
  //    patterns  → loadPatterns()
  //    network   → loadNetwork()
  //    history   → loadHistory()
  //    tips      → loadTipsHistory()
  //    portfolio → loadPortfolioModel() + loadPortfolioHoldings() + loadTickerList() + loadSimBannerIfSet()
  //    markets   → initMarketsScreen()
  //    auth      → _injectAuthTgWidget()
}
```

Nav items fire `showScreen(el.dataset.screen)` on click. If unauthenticated and attempting a non-auth screen, redirects to auth.

---

## 10. Utility functions

| Function | Signature | Purpose |
|---|---|---|
| `showToast(msg, type)` | `type = 'error' \| 'ok'` | Red or green toast, auto-hides after 4s |
| `fmt(v, digits)` | `digits = 2` | Format number to N decimal places, `—` for null |
| `fmtTime(iso)` | ISO string | `HH:MM` local time |
| `fmtDate(iso)` | ISO string | Local date string |
| `escHtml(s)` | string | Escapes `&`, `<`, `>` — use on all user/API data before injecting into HTML |
| `tierBadge(tier)` | `'high' \| 'medium' \| 'low' \| 'avoid'` | Returns badge HTML span |
| `dirBadge(dir)` | `'bullish' \| 'bearish'` | Returns directional badge HTML |
| `dot(ok)` | boolean | Green or red dot span |
| `mdToHtml(s)` | markdown string | Converts subset of markdown to safe HTML |
| `renderUserMsg(msg)` | string | Escapes + replaces `[PATTERN:...]` with styled pills |
| `expandPatternTag(msg)` | string | Converts pattern pills back to readable sentences for LLM |

---

## 11. Content Security Policy

```
default-src 'self';
script-src  'self' 'unsafe-inline' https://telegram.org https://fonts.googleapis.com;
frame-src   https://oauth.telegram.org https://s.tradingview.com;
img-src     'self' data: https:;
style-src   'self' 'unsafe-inline' https://fonts.googleapis.com;
font-src    https://fonts.gstatic.com;
connect-src 'self' https://api.trading-galaxy.uk;
```

**Why `'unsafe-inline'` for scripts:** all JS is inline in the single HTML file — no nonce/hash system. Acceptable for a private internal tool.

**`connect-src` allows `api.trading-galaxy.uk`** — required for cross-origin `apiFetch` calls in production. On localhost the API is same-origin so `'self'` covers it.

**`frame-src` allows `s.tradingview.com`** — required for the TradingView chart iframe.

---

## 12. Local storage

| Key | Value | Set by | Cleared by |
|---|---|---|---|
| `tg_token` | JWT access token string | `_saveSession()` | `signOut()` |
| `tg_user_id` | User ID string | `_saveSession()` | `signOut()` |
| `tg_user_data` | JSON of Telegram user object | `_saveSession()` (Telegram flow only) | `signOut()` |
| `simulated_{userId}` | JSON of simulated profile `{title, description, ...}` | Generate test portfolio / test profile select | `signOut()`, real portfolio submit, "Clear simulated flag" |

---

## 13. Adding a new screen

1. **Add HTML** — create a `<div class="screen" id="screen-{name}">...</div>` inside `#main`

2. **Add nav item** — add `.nav-item` to `#sidebar` with `id="nav-{name}"` and `data-screen="{name}"`

3. **Add load function** — write `function load{Name}() { ... }` that calls `apiFetch` and renders into the screen div

4. **Wire to `showScreen`** — add `if (name === '{name}') load{Name}();` inside `showScreen()`

5. **No build step** — save and redeploy (`npx wrangler pages deploy static --project-name mem-backend2 --branch master --commit-dirty=true` from repo root)

**Template:**

```html
<!-- In #main -->
<div class="screen" id="screen-myscreen">
  <div class="section-title">My Screen</div>
  <div class="card" id="myscreen-content">
    <div class="empty text-sm mono-muted">Loading…</div>
  </div>
</div>
```

```js
// Load function
async function loadMyScreen() {
  const el = document.getElementById('myscreen-content');
  el.innerHTML = '<div class="empty mono-muted">Loading…</div>';
  const d = await apiFetch('/my-endpoint');
  if (!d) return;
  el.innerHTML = `<p>${escHtml(JSON.stringify(d))}</p>`;
}

// In showScreen():
if (name === 'myscreen') loadMyScreen();
```

```html
<!-- Nav item -->
<div class="nav-item" id="nav-myscreen" data-screen="myscreen">
  <span class="nav-icon">🔭</span>
  <span class="nav-label">My Screen</span>
</div>
```

# REST API Reference

Base URL: `https://api.trading-galaxy.uk` (production) · `http://localhost:5050` (local dev)

**Framework:** FastAPI (`api_v2.py`), served by Gunicorn + UvicornWorker.

**CORS:** Cross-origin requests are accepted from `https://trading-galaxy.uk`, `https://www.trading-galaxy.uk`, `https://*.pages.dev`, and `http://localhost:3000` / `http://localhost:5050`. Allowed methods: `GET POST PATCH OPTIONS`. The `Authorization` and `Content-Type` headers are allowed.

**Rate limiting:** `slowapi` (in-memory). Exempt for `EVAL_MODE=1` and localhost. `/waitlist` is 3/hour per IP; chat and other sensitive endpoints are individually gated.

All request/response bodies are JSON. All endpoints return `Content-Type: application/json`.

---

## `GET /health`

Liveness check.

**Response**
```json
{ "status": "ok", "db": "trading_knowledge.db", "facts": 8890 }
```

---

## `GET /stats`

KB statistics.

**Response**
```json
{
  "total_facts": 548,
  "unique_predicates": 15,
  "unique_subjects": 64
}
```

---

## `GET /ingest/status`

Health status for the background ingest scheduler. Use this to detect silent failures (missing API keys, rate limits, network errors).

**Response — scheduler running**
```json
{
  "scheduler": "running",
  "adapters": {
    "yfinance": {
      "name": "yfinance",
      "interval_sec": 300,
      "is_running": false,
      "last_run_at": "2026-02-24T15:14:09.927678+00:00",
      "last_success_at": "2026-02-24T15:14:09.927678+00:00",
      "last_error": null,
      "last_error_at": null,
      "total_runs": 3,
      "total_atoms": 256,
      "total_errors": 0
    },
    "fred": { ... },
    "edgar": { ... },
    "rss_news": { ... }
  }
}
```

**Response — scheduler not running**
```json
{
  "scheduler": "not_running",
  "reason": "ingest dependencies not installed or scheduler failed to start",
  "adapters": {}
}
```

**Diagnosing issues:**
- `last_error` non-null → adapter threw an exception (check value for detail)
- `total_atoms = 0` after multiple runs → data source returned nothing or all atoms deduped
- `is_running = true` for extended period → adapter may be hung (yfinance batch over 50 tickers normally takes ~90s)

---

## `POST /ingest`

Add one or more atoms to the KB manually.

**Request — single atom**
```json
{
  "subject":    "AAPL",
  "predicate":  "signal_direction",
  "object":     "long",
  "confidence": 0.85,
  "source":     "model_signal_momentum_v1",
  "metadata":   { "generated_at": "2026-02-24T12:00:00Z" }
}
```

**Request — batch**
```json
{
  "atoms": [
    { "subject": "AAPL", "predicate": "signal_direction", "object": "long", "confidence": 0.85, "source": "model_signal_v1" },
    { "subject": "MSFT", "predicate": "price_target", "object": "600", "confidence": 0.75, "source": "broker_research_gs_2026" }
  ]
}
```

**Response**
```json
{ "ingested": 2, "skipped": 0 }
```

**Notes:**
- `confidence` defaults to `0.5` if omitted
- `source` defaults to `unverified_api` if omitted — use a recognised prefix from `ingest/base.py` for proper authority weighting
- Atoms with the same `(subject, predicate, object)` are deduplicated (no error, just counted as `skipped`)
- `subject`, `predicate`, `object` are all required; missing any → atom skipped

---

## `POST /retrieve`

Smart multi-strategy retrieval for a natural-language or structured query. This is the primary endpoint for the copilot/LLM layer.

**Request**
```json
{
  "message":    "What is the current signal on NVDA and META?",
  "session_id": "user-abc-session-1",
  "goal":       "evaluate tech long book",
  "topic":      "technology sector",
  "turn_count": 1
}
```

Only `message` is required. All other fields are optional.

**Response**
```json
{
  "snippet": "=== TRADING KNOWLEDGE CONTEXT ===\n[Signals & Positioning]\n  nvda | signal_direction | long\n  nvda | price_target | 253.99\n  meta | signal_direction | long\n  meta | price_target | 861.3\n[Other]\n  nvda | sector | technology\n  nvda | last_price | 190.4",
  "atoms": [
    {
      "subject":    "nvda",
      "predicate":  "signal_direction",
      "object":     "long",
      "source":     "broker_research_yahoo_consensus_nvda",
      "confidence": 0.65
    }
  ],
  "stress": {
    "composite_stress":    0.093,
    "decay_pressure":      0.0,
    "authority_conflict":  0.025,
    "supersession_density": 0.0,
    "conflict_cluster":    0.0,
    "domain_entropy":      0.3
  },
  "prior_context": "..." // only present if session has prior working state
}
```

**Snippet sections:**

| Section header | Predicates included |
|---|---|
| `[Signals & Positioning]` | `signal_direction`, `signal_confidence`, `price_target`, `entry_condition`, `exit_condition`, `invalidation_condition` |
| `[Theses & Evidence]` | `premise`, `supporting_evidence`, `contradicting_evidence`, `risk_reward_ratio`, `position_sizing_note` |
| `[Macro / Regime]` | `regime_label`, `dominant_driver`, `central_bank_stance`, `risk_on_off` + all `macro_data_*` sources |
| `[Research]` | `rating`, `key_finding`, `compared_to_consensus` + all `broker_research_*` sources |
| `[Other]` | Everything else |

**Stress interpretation:**

| `composite_stress` | Meaning |
|---|---|
| < 0.15 | Low — context is fresh, coherent, authoritative |
| 0.15–0.35 | Moderate — some age or authority mix |
| 0.35–0.60 | Elevated — notable conflicts or decay; treat with care |
| > 0.60 | High — stale or heavily conflicted; consider refreshing |

**Intent keywords in `message`** that activate predicate boost (strategy 3):

| Keyword | Predicates fetched |
|---|---|
| `upside`, `target`, `analyst`, `consensus` | `price_target`, `signal_direction` |
| `signal`, `direction`, `long`, `short`, `momentum` | `signal_direction` |
| `regime`, `macro`, `inflation`, `rate` | `regime_label`, `central_bank_stance`, `dominant_driver`, … |
| `sector` | `sector` |
| `volatility`, `beta` | `volatility_regime` |
| `catalyst` | `catalyst` |
| `risk` | `risk_factor` |
| `earnings` | `earnings_quality` |
| `yield` | `risk_factor`, `dominant_driver` |

---

## `GET /query`

Direct triple-store query with optional filters. Returns raw facts without retrieval ranking.

**Query params:**

| Param | Type | Description |
|---|---|---|
| `subject` | string | Filter by subject (partial match) |
| `predicate` | string | Filter by predicate (exact match) |
| `object` | string | Filter by object (partial match) |
| `limit` | int | Max results (default 50) |

**Example**
```
GET /query?subject=NVDA&predicate=price_target
```

**Response**
```json
{
  "results": [
    {
      "subject": "nvda",
      "predicate": "price_target",
      "object": "253.99",
      "confidence": 0.75,
      "source": "broker_research_yahoo_consensus_nvda",
      "timestamp": "2026-02-24T15:04:36.375287"
    }
  ],
  "count": 1
}
```

---

## `GET /search`

Full-text search over the KB using SQLite FTS5.

**Query params:**

| Param | Type | Description |
|---|---|---|
| `q` | string | **Required.** Search text |
| `category` | string | Optional category filter |
| `limit` | int | Max results (default 20) |

**Example**
```
GET /search?q=fed+inflation+restrictive
```

**Response**
```json
{
  "results": [ { "subject": "us_macro", "predicate": "central_bank_stance", "object": "neutral_to_restrictive", ... } ],
  "count": 1
}
```

---

## `GET /context/<entity>`

Retrieve all facts for a specific entity (ticker, concept, or thesis ID). No ranking — returns everything.

**Path param:** `entity` — e.g. `AAPL`, `us_macro`, `fed_rate_thesis_2024`

**Example**
```
GET /context/NVDA
```

**Response**
```json
{
  "entity": "NVDA",
  "count": 14,
  "facts": [
    { "subject": "nvda", "predicate": "last_price", "object": "190.4", "confidence": 0.95, ... },
    { "subject": "nvda", "predicate": "price_target", "object": "253.99", "confidence": 0.75, ... },
    { "subject": "nvda", "predicate": "signal_direction", "object": "long", "confidence": 0.65, ... },
    ...
  ]
}
```

---

## `POST /waitlist`

Add an email to the beta waitlist. No authentication required.

**Rate limit:** 3 requests per hour per IP.

**Request**
```json
{
  "email":  "alice@example.com",
  "source": "landing"
}
```

`source` is optional (default `"landing"`).

**Response 200**
```json
{ "message": "You're on the list", "already": false }
```

If the email was already registered:
```json
{ "message": "You're already on the list", "already": true }
```

**Error responses**

| Status | `error` value | Meaning |
|--------|--------------|--------|
| 400 | `"Invalid email"` | Missing, malformed, or >254 chars |
| 429 | — | Rate limit exceeded (3/hour per IP) |

A Telegram notification is sent to the operator on every new signup.

---

## `GET /users/{id}/positions/open`

All open followups for a user — both auto-created `watching` (tip sent, user has not yet acted) and `active` (user accepted via 'taking it').

Requires authentication (HttpOnly cookie). Users can only query their own positions.

**Response**
```json
{
  "user_id": "alice",
  "count": 3,
  "positions": [
    {
      "id": 12,
      "ticker": "SHEL.L",
      "direction": "bullish",
      "status": "active",
      "pattern_type": "fvg",
      "timeframe": "4h",
      "entry_price": 26.80,
      "stop_loss": 26.10,
      "target_1": 27.50,
      "target_2": 28.20,
      "zone_low": 26.60,
      "zone_high": 27.00,
      "expires_at": "2026-03-15T08:00:00+00:00",
      "conviction_at_entry": "high",
      "regime_at_entry": "risk_on_expansion",
      "created_at": "2026-03-01T08:01:00+00:00"
    }
  ]
}
```

**Status values:**

| Value | Meaning |
|---|---|
| `watching` | Auto-created when tip was sent — user has not acted yet |
| `active` | User clicked 'taking it' — confirmed open position |

---

## `GET /users/{id}/positions/closed`

Recently closed, expired, or stopped-out followups. Defaults to last 30 days.

**Query params:**

| Param | Type | Default | Description |
|---|---|---|---|
| `since` | string | 30 days ago | `YYYY-MM-DD` lower bound on `closed_at` |

Requires `Authorization: Bearer <token>`.

**Response**
```json
{
  "user_id": "alice",
  "count": 2,
  "since": "2026-02-01",
  "positions": [
    {
      "id": 9,
      "ticker": "AZN.L",
      "direction": "bullish",
      "status": "closed",
      "entry_price": 118.50,
      "target_1": 121.00,
      "stop_loss": 116.00,
      "closed_at": "2026-02-28T14:32:00+00:00",
      "conviction_at_entry": "medium"
    }
  ]
}
```

**Status values in closed set:** `closed` · `expired` · `stopped`

---

## `POST /users/{id}/trader-level`

Set the authenticated user's trader level — controls LLM communication style and tip formatting.

Requires authentication (HttpOnly cookie). Users can only update their own level.

**Request**
```json
{ "level": "experienced" }
```

**Valid values:** `beginner` · `developing` · `experienced` · `quant`

| Level | Effect |
|---|---|
| `beginner` | Plain-English explanations, no jargon, no raw atom values |
| `developing` | Standard format with brief explanations (default) |
| `experienced` | Full signal detail including Greeks when present |
| `quant` | Raw atom dump, no prose, all values shown |

**Response 200**
```json
{ "trader_level": "experienced" }
```

**Error responses**

| Status | `error` value | Meaning |
|---|---|---|
| 400 | `"Invalid level"` | Value not in the allowed set |
| 401 | — | Not authenticated |
| 403 | — | Attempting to modify another user's level |

---

## `GET /waitlist/count`

Public signup counter for landing page social proof. No authentication required.

**Response**
```json
{ "count": 42 }
```

Returns `{"count": 0}` on any error — never 5xx.

---

## Error Responses

All errors return a JSON body with an `error` key:

```json
{ "error": "invalid JSON" }
```

**Rate limit responses** return HTTP `429` with a plain-text body:
```
3 per 1 hour
```

Handle `429` in your client — show a user-facing message and do not retry immediately.

---

## `POST /chat`

KB-grounded conversational endpoint. Retrieves relevant atoms, builds a context-aware prompt, and calls the LLM (Groq preferred, Ollama fallback). Requires authentication.

**Request**
```json
{
  "message":    "Is NVDA a good entry here?",
  "session_id": "user-abc-session-1",
  "turn_count": 2
}
```

Only `message` is required.

**Response**
```json
{
  "answer": "NVDA currently shows a bullish signal with high conviction...",
  "atoms_used": 12,
  "kb_depth": "deep",
  "quota_remaining": 45
}
```

**Notes:**
- Chat quota is enforced per user per day (varies by tier)
- `kb_depth`: `thin` (<5 atoms) · `shallow` (5–14) · `deep` (≥15)
- Returns `{"error": "quota_exceeded"}` with HTTP 429 when limit reached

---

## Paper Trader Endpoints

All paper trader endpoints require authentication and `pro` or `premium` tier. Users can only access their own paper account.

### `GET /users/{id}/paper/account`

Virtual account balance and summary.

**Response**
```json
{
  "user_id": "a1_svao9",
  "virtual_balance": 487234.50,
  "currency": "GBP",
  "open_positions": 3,
  "created_at": "2026-03-01T00:00:00+00:00"
}
```

---

### `GET /users/{id}/paper/positions`

All open paper positions.

**Response**
```json
{
  "positions": [
    {
      "id": 42,
      "ticker": "AAPL",
      "direction": "bullish",
      "entry_price": 227.50,
      "stop": 224.80,
      "t1": 232.90,
      "t2": 235.60,
      "quantity": 18.5185,
      "status": "open",
      "partial_closed": 0,
      "opened_at": "2026-03-06T10:00:00+00:00",
      "ai_reasoning": "supply_demand bullish | q=0.88 HIGH regime=risk_on | kb_depth=deep (18 atoms)"
    }
  ]
}
```

**Position statuses:** `open` · `stopped_out` · `t2_hit` · `t1_partial`

---

### `GET /users/{id}/paper/history`

Closed paper positions (stopped out or target hit).

**Query params:** `limit` (default 50), `since` (ISO date)

**Response** — same shape as `/paper/positions` with `exit_price`, `pnl_r`, `closed_at` fields populated.

---

### `GET /users/{id}/paper/log`

Agent activity log — scan starts, entries, skips, stops.

**Query params:** `limit` (default 50)

**Response**
```json
{
  "log": [
    {
      "event_type": "scan_start",
      "ticker": null,
      "detail": "Scanning open patterns for a1_svao9 (3/12 slots used, 2 on 24h cooldown)",
      "created_at": "2026-03-06T10:00:00+00:00"
    },
    {
      "event_type": "entry",
      "ticker": "AAPL",
      "detail": "bullish entry=227.5000 stop=224.8000 t1=232.9000 qty=18.5185 value=£4,210.71 cash_remaining=£483,023.79",
      "created_at": "2026-03-06T10:00:01+00:00"
    },
    {
      "event_type": "stopped_out",
      "ticker": "KB",
      "detail": "exit=112.80 P&L=-1.00R refund=£2,086.08",
      "created_at": "2026-03-06T10:02:00+00:00"
    }
  ]
}
```

**Event types:** `scan_start` · `entry` · `skip` · `stopped_out` · `t2_hit` · `t1_hit` · `monitor_run`

---

### `POST /users/{id}/paper/agent/run`

Trigger a one-shot agent scan synchronously. Returns the scan result immediately.

**Response**
```json
{ "status": "ok", "result": { "entries": 2, "skips": 14, "monitor_updates": [] } }
```

---

### `POST /users/{id}/paper/agent/start`

Start the continuous 30-minute background scanner for this user.

**Response**
```json
{ "status": "started", "message": "Continuous scanner started — scans every 30 min" }
```

Returns `{"status": "already_running"}` if scanner is already active.

---

### `POST /users/{id}/paper/agent/stop`

Stop the continuous scanner.

**Response**
```json
{ "status": "stopped", "message": "Scanner stopped" }
```

---

### `GET /users/{id}/paper/agent/status`

**Response**
```json
{ "running": true }
```

---

### Agent sizing rules

| Rule | Value |
|---|---|
| Starting balance | £500,000 virtual GBP |
| Risk per trade | `max_risk_per_trade_pct` from `user_preferences` (default 1%, hard cap 2%) |
| Max position value | 10% of current balance |
| Max open positions | 12 |
| Max new entries per scan | 3 |
| Stopped-out cooldown | 24 hours per ticker |
| Scan interval (scheduler) | 30 minutes |
| Pattern quality threshold | ≥ 0.70, conviction `high`/`confirmed`/`strong` |

# REST API Reference

Base URL: `http://localhost:5050`

All request/response bodies are JSON. All endpoints return `Content-Type: application/json`.

---

## `GET /health`

Liveness check.

**Response**
```json
{ "status": "ok", "db": "trading_knowledge.db" }
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

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TRADING_KB_DB` | `trading_knowledge.db` | Path to the SQLite database file |
| `PORT` | `5050` | Flask listening port |
| `FRED_API_KEY` | *(none)* | Required for `FREDAdapter` — get free key at fred.stlouisfed.org |
| `EDGAR_USER_AGENT` | `trading-galaxy-kb research@example.com` | Required by SEC EDGAR fair-use policy |

---

## Error Responses

All errors return a JSON body with an `error` key:

```json
{ "error": "invalid JSON" }
```

| HTTP status | When |
|---|---|
| `400` | Missing required field, invalid JSON |
| `500` | Unhandled server error (check server logs) |

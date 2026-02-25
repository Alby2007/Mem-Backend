# Hybrid Build Spec — Dynamic Universe + Personal KB Layer

Implement the hybrid architecture: a shared central KB with a personal curation layer per user, user-driven universe expansion, collective signal calibration, and a network effect engine that makes coverage and signal quality improve as the user base grows.

---

## Architecture Summary

```
SHARED KB LAYER        — live market data, signals, patterns, macro (same for all users)
      ↓ read
PERSONAL LAYER         — portfolio, preferences, expanded universe, feedback history
      ↓ shapes
DELIVERY LAYER         — personalised tips, briefings, overlay responses
```

---

## Phase 1 — Dynamic Watchlist (foundation everything else builds on)

### 1.1 `ingest/dynamic_watchlist.py` — new file
- `DynamicWatchlistManager` class with:
  - `get_active_tickers(db_path)` → merged list of `_DEFAULT_TICKERS` + promoted universe tickers, deduplicated by **string key** (not object identity) — if `AAPL` appears in both the default list and a user's expanded universe it appears exactly once; duplicate atoms and wasted yfinance quota prevented at source
  - `add_tickers(tickers, user_id, db_path)` → insert into `universe_tickers`, bump `coverage_count`
  - `get_priority_tickers(db_path)` → tickers with `coverage_count >= 3`
  - `get_user_tickers(user_id, db_path)` → tickers a specific user added
- Coverage-weighted refresh tiers:

| Tier | coverage | yfinance | options | patterns |
|------|----------|----------|---------|----------|
| nascent | 1–2 | 300s | 1800s | 900s |
| emerging | 3–9 | 180s | 900s | 450s |
| established | 10–49 | 60s | 300s | 120s |
| core | 50+ | 30s | 120s | 60s |

### 1.2 New DB tables (added to `users/user_store.py`)

```sql
universe_tickers (ticker, requested_by, sector_label, coverage_count, added_to_ingest, added_at)
ticker_staging   (ticker, user_id, requested_at, coverage_count, promoted, promoted_at, rejection_reason)
user_universe_expansions (id, user_id, description, sector_label, tickers JSON, etfs JSON,
                          keywords JSON, causal_edges JSON, status, requested_at, activated_at)
user_kb_context  (id, user_id, subject, predicate, object, confidence, source, created_at, updated_at)
user_engagement_events (id, user_id, event_type, ticker, pattern_type, sector, timestamp)
signal_calibration (id, ticker, pattern_type, timeframe, market_regime, sample_size,
                    hit_rate_t1, hit_rate_t2, hit_rate_t3, stopped_out_rate,
                    avg_time_to_target_hours, calibration_confidence, last_updated)
```

### 1.3 Wire adapters to `DynamicWatchlistManager`
- `ingest/yfinance_adapter.py` — `YFinanceAdapter.__init__` calls `DynamicWatchlistManager.get_active_tickers()` when no explicit tickers passed (replaces `_DEFAULT_TICKERS` fallback)
- `ingest/options_adapter.py` — same pattern, replaces `_OPTIONS_TICKERS` fallback
- `ingest/signal_enrichment_adapter.py` — already enriches everything in KB; no change needed
- `ingest/historical_adapter.py` — replace `_DEFAULT_TICKERS` import with `DynamicWatchlistManager`

**Promotion rule** (checked on every `add_tickers` call):
```python
SHARED_KB_PROMOTION = {
    'min_coverage_count': 3,
    'min_avg_daily_volume': 500_000,
    'min_price': 1.0,
}
```
Below threshold → `ticker_staging`. Once promoted → `universe_tickers.added_to_ingest = 1`.

---

## Phase 2 — Universe Expander

### 2.1 `analytics/universe_expander.py` — new file
- `resolve_interest(description, market_type, user_id, db_path) → UniverseExpansion`
  - Calls Ollama with structured prompt → returns `sector_label`, `tickers`, `etfs`, `keywords`, `causal_relationships`
  - Falls back to empty lists gracefully if Ollama unavailable
- `validate_tickers(tickers, market_region) → ValidationResult`
  - yfinance `fast_info` check: must have price, volume > 100k, no fetch error
  - Indian equities: auto-append `.NS` suffix; BSE: `.BO`
- `seed_causal_edges(edges, db_path) → int` — calls `knowledge/causal_graph.py:add_causal_edge()` with `source='user_expansion'`
- `bootstrap_ticker(ticker, db_path)` — sequential: fundamentals → historical → options → signal enrichment → pattern detection
- Cap: 20 tickers per expansion request; `basic` tier max 20 total, `pro` tier max 100 total

### 2.2 `knowledge/causal_graph.py` — minor addition
- No structural change needed — `add_causal_edge()` already exists; just call it with `source='user_expansion'`

---

## Phase 3 — Personal KB Layer

### 3.1 `users/personal_kb.py` — new file

**Structural isolation rule:** `personal_kb.py` must never import from or write to `knowledge/graph.py` or the shared `facts` table. The INSERT target is a **string literal** in the SQL — the table name is never parameterised, so it literally cannot be redirected. `write_atom()` also carries a class-level assertion on `_table`:
```python
_TABLE = 'user_kb_context'  # hardcoded — never a variable

def write_atom(self, user_id, subject, predicate, object_, confidence, source):
    assert self._table == 'user_kb_context', \
        f"personal_kb.write_atom must only write to user_kb_context, got {self._table}"
    conn.execute(f"INSERT OR REPLACE INTO user_kb_context ...")  # literal, not f-string on table
```
This avoids false-positives from path strings containing unrelated words (e.g. `/artifacts/trading.db`) and makes the constraint enforceable at the SQL level, not just at the Python level.

- `write_atom(user_id, subject, predicate, object_, confidence, source)`
- `read_atoms(user_id, predicates=None) → List[dict]`
- `get_context_document(user_id, db_path) → PersonalContext` dataclass
- `update_from_feedback(user_id, feedback_event, db_path)` — updates `fvg_hit_rate`, `preferred_pattern`, `avg_win_rate`
- `update_from_engagement(user_id, events, db_path)` — infers `high_engagement_sector`, `low_engagement_sector`, `preferred_upside_min`
- `infer_and_write_from_portfolio(user_id, db_path)` — writes `sector_affinity`, `risk_tolerance`, `holding_style` on portfolio submit

Atoms written automatically:

| Event | Atoms written |
|-------|--------------|
| Portfolio submit | `sector_affinity`, `risk_tolerance`, `holding_style`, `portfolio_beta` |
| Tip feedback | `{pattern}_hit_rate`, `preferred_pattern`, `avg_win_rate` |
| Universe expansion | `active_universe`, `niche_interest_N` |
| Engagement events | `high_engagement_sector`, `low_engagement_sector`, `tip_open_rate` |

---

## Phase 4 — Signal Calibration

### 4.1 `analytics/signal_calibration.py` — new file
- `update_calibration(ticker, pattern_type, timeframe, market_regime, outcome, db_path)` — called by `POST /feedback` handler
- `get_calibration(ticker, pattern_type, timeframe, db_path) → CalibrationResult | None`
- Confidence gating: `< 10 samples → None`, `< 30 → low`, `< 100 → moderate`, `>= 100 → established`

### 4.2 Tip formatter integration
- `notifications/tip_formatter.py` — when `calibration` is passed to `format_tip`, add a line:
  `Historical hit rate: 71% to T2 (47 trades, risk-on regime)` — only shown if `calibration_confidence >= 0.50`

---

## Phase 5 — Snapshot & Tip Scheduler integration

### 5.1 `analytics/snapshot_curator.py` — modify `curate_snapshot`
```python
# New: read personal context + expanded universe
personal_context = get_context_document(user_id, db_path)
expanded_tickers = DynamicWatchlistManager.get_user_tickers(user_id, db_path)

# Opportunity pool = portfolio tickers + expanded universe
all_tickers = existing_tickers + expanded_tickers
```
- `_score_opportunity` gains two new inputs: `personal_context` (pattern hit rates, sector affinity) + `calibration` (collective hit rates)
- Score adjustments: `+0.20` sector match, `+0.15` preferred pattern match, `−0.30` below `preferred_upside_min`, calibration delta scaled by `±0.20`
- **`calibration=None` is explicitly handled** — when `get_calibration()` returns `None` (< 10 samples) the calibration branch is skipped entirely; score proceeds on the non-calibration inputs only. No crash, no KeyError, no score adjustment from absent data.

### 5.2 `notifications/tip_scheduler.py` — modify `_pick_best_pattern`
- After selecting pattern, fetch `CalibrationResult` and attach to tip
- Filter: if user's personal `{pattern}_hit_rate < 0.40` for this pattern type AND `calibration.hit_rate_t2 < 0.45`, skip it

---

## Phase 6 — Network Effect Engine

### 6.1 `analytics/network_effect_engine.py` — new file
- `compute_coverage_tier(ticker, db_path) → CoverageTier`
- `promote_to_shared_kb(ticker, db_path) → bool`
- `update_refresh_schedule(db_path)` — updates coverage tiers; called by ingest scheduler on each cycle
- `detect_cohort_consensus(ticker, db_path) → CohortSignal` — requires `coverage_count >= 10`
- `compute_trending_markets(db_path) → List[TrendingMarket]` — 7-day coverage growth rate
- `compute_network_health(db_path) → NetworkHealthReport`

Cohort KB atoms emitted (when cohort_size ≥ 10):
```
{ticker} | cohort_consensus    | "long_0.78"
{ticker} | cohort_stop_cluster | "187.20_tight"
{ticker} | contrarian_flag     | "true"
```

---

## Phase 7 — New API Endpoints (api.py)

| Method | Route | Description |
|--------|-------|-------------|
| POST | `/users/{id}/expand-universe` | Resolve interest → validate tickers → register → bootstrap |
| GET | `/users/{id}/universe` | Current expanded watchlist + coverage tiers |
| DELETE | `/users/{id}/universe/{ticker}` | Remove from personal universe |
| POST | `/users/{id}/preferences/focus` | Explicit preference overrides |
| POST | `/users/{id}/engagement` | Log engagement event |
| GET | `/users/{id}/kb-context` | Personal KB atoms |
| GET | `/users/{id}/preferences/inferred` | What system has inferred |
| GET | `/universe/trending` | Fastest-growing coverage tickers (7d) |
| GET | `/universe/coverage` | Full coverage leaderboard |
| GET | `/universe/staging/global` | All staged tickers |
| GET | `/users/{id}/universe/staging` | User's staged tickers |
| GET | `/users/{id}/universe/bootstrap-status` | Per-ticker bootstrap completion for the user's expanded universe |
| GET | `/network/health` | Flywheel velocity, coverage distribution |
| GET | `/network/calibration/{ticker}` | Collective hit rates for ticker |
| GET | `/network/cohort/{ticker}` | Cohort consensus + stop cluster |

`GET /users/{id}/universe/bootstrap-status` response:
```json
{
  "tickers": [
    {"ticker": "CCJ", "added_to_ingest": true, "has_price": true, "has_signals": true, "has_patterns": true, "bootstrap_complete": true},
    {"ticker": "UEC", "added_to_ingest": true, "has_price": true, "has_signals": false, "has_patterns": false, "bootstrap_complete": false}
  ],
  "all_ready": false
}
```
`bootstrap_complete` is `true` when `universe_tickers.added_to_ingest = 1` AND the shared KB has `last_price`, `conviction_tier`, and at least one `pattern_signals` row for that ticker. Frontend polls this at a reasonable interval (e.g. 5s) until `all_ready = true`.

---

`POST /users/{id}/expand-universe` flow:
1. Validate `description` (min 3 chars), `market_type`
2. Call `resolve_interest()` — Ollama or graceful fallback
3. Validate tickers via yfinance
4. Write `user_universe_expansions` row
5. Call `DynamicWatchlistManager.add_tickers()` — inserts/bumps `universe_tickers`
6. Check promotion rules; promote if met
7. Seed causal edges
8. For each newly promoted ticker: run `bootstrap_ticker()` async (non-blocking)
9. Write `active_universe` personal KB atom
10. Compute `estimated_bootstrap_seconds` via formula (see below) and return response

`estimated_bootstrap_seconds` formula — computed at endpoint call time, not hardcoded:
```python
def estimate_bootstrap_seconds(n_tickers: int, db_path: str) -> int:
    base_per_ticker = 8   # fundamentals + historical + options + enrichment + patterns
    queue_depth = get_extraction_queue_depth(db_path)  # rows in llm_extraction_queue
    queue_delay = min(queue_depth * 2, 60)             # extraction queue adds latency, capped at 60s
    return (n_tickers * base_per_ticker) + queue_delay
```
`get_extraction_queue_depth()` reads the pending row count from the LLM extraction queue table. Returns 0 if the table doesn't exist yet (graceful). The cap of 60s on queue delay prevents absurdly large estimates when the queue is heavily loaded — in that case the estimate will be slightly optimistic but not by an order of magnitude.

Return: `{expansion_id, resolved_tickers, rejected_tickers, staging_tickers, causal_edges_seeded, estimated_bootstrap_seconds}`

---

## Files Changed / Created

**New:**
```
ingest/dynamic_watchlist.py
analytics/universe_expander.py
analytics/signal_calibration.py
analytics/network_effect_engine.py
users/personal_kb.py
```

**Modified:**
```
users/user_store.py              — 6 new tables + ensure functions
ingest/yfinance_adapter.py       — DynamicWatchlistManager fallback
ingest/options_adapter.py        — DynamicWatchlistManager fallback
ingest/historical_adapter.py     — DynamicWatchlistManager fallback
analytics/snapshot_curator.py    — personal context + expanded universe in scoring
notifications/tip_scheduler.py   — calibration-aware pattern selection
notifications/tip_formatter.py   — calibration hit rate line in message
api.py                           — 15 new endpoints
```

---

## Implementation Order

1. `users/user_store.py` — 6 new tables (everything depends on DB schema)
2. `ingest/dynamic_watchlist.py` — wire adapters immediately after
3. Update `ingest/yfinance_adapter.py`, `options_adapter.py`, `historical_adapter.py`
4. `users/personal_kb.py`
5. `analytics/universe_expander.py`
6. `analytics/signal_calibration.py` + wire into `POST /feedback`
7. `analytics/network_effect_engine.py`
8. `analytics/snapshot_curator.py` + `notifications/tip_scheduler.py` updates
9. `notifications/tip_formatter.py` calibration line
10. `api.py` — all 15 new endpoints (including bootstrap-status)
11. Re-run `live_comprehensive_qa.py` + new targeted tests

---

## Key Constraints & Guards

- Universe expander cap: 20 tickers/request, 20 total for `basic`, 100 for `pro`
- Promotion requires `coverage_count >= 3` AND `avg_daily_volume >= 500k` AND `price >= $1`
- Calibration not surfaced until `sample_size >= 10`
- Cohort signals not emitted until `cohort_size >= 10`
- Bootstrap runs async (non-blocking); endpoint returns immediately with `estimated_bootstrap_seconds`
- Frontend polls `GET /users/{id}/universe/bootstrap-status` to detect completion; `all_ready=true` is the signal to show "your tickers are ready"
- Ollama failures in universe expander are non-fatal: return `{resolved_tickers: [], error: "llm_unavailable"}` with 200
- Personal KB atoms never influence shared KB signal quality — they only affect curation ranking

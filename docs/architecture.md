# Architecture

## Overview

Trading Galaxy is a **knowledge-graph-powered trading intelligence platform** — a persistent, queryable, epistemically-aware triple store that:

1. **Ingests** live market data from multiple sources on automatic schedules
2. **Stores** knowledge as typed `(subject, predicate, object)` atoms with confidence and provenance
3. **Retrieves** context for natural-language or structured queries using multi-strategy ranking
4. **Monitors** epistemic health: knowledge decay, authority conflicts, composite stress
5. **Serves** a personalised daily briefing to users via the Product Layer
6. **Provides** a browser-based internal tool (SPA at `GET /`) for portfolio management and KB exploration

The system runs fully locally — KB, LLM inference (Ollama), and the frontend are all on-device. No external LLM API calls are required.

---

## Component Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                         External Data Sources                           │
│  Yahoo Finance · FRED API · SEC EDGAR · RSS Feeds · Options Chains      │
└──┬─────────────┬──────────────┬───────────┬──────────┬─────────────────┘
   │             │              │           │          │
   ▼             ▼              ▼           ▼          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         ingest/ package                              │
│                                                                      │
│  YFinanceAdapter   FREDAdapter     EDGARAdapter    RSSAdapter        │
│  OptionsAdapter    HistoricalBackfillAdapter       PatternAdapter    │
│  LLMExtractionAdapter  SignalEnrichmentAdapter  EDGARRealtimeAdapter │
│  DynamicWatchlistManager  SeedSyncClient                            │
│         └──────────────────┬─────────────────────────┘              │
│                      BaseIngestAdapter                              │
│                      fetch() → transform() → validate() → push()   │
│                      IngestScheduler (threading.Timer)              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │ RawAtom list
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       knowledge/ package                             │
│                                                                      │
│  TradingKnowledgeGraph (graph.py)                                   │
│  ├── SQLite WAL triple store (facts table)                           │
│  ├── FTS5 index (facts_fts)                                          │
│  ├── fact_conflicts · decay_log · causal_edges tables               │
│  └── thread-local connections                                        │
│                                                                      │
│  authority.py · decay.py · contradiction.py · epistemic_stress.py  │
│  working_state.py · graph_retrieval.py · causal_graph.py            │
│  kb_insufficiency_classifier.py · kb_repair_proposals.py           │
│  kb_repair_executor.py · kb_validation.py · confidence_intervals.py│
└────────────────────────────────┬────────────────────────────────────┘
                                 │ sqlite3.Connection
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         retrieval.py                                 │
│  0. Graph-relational (PageRank + clustering + BFS)                  │
│  1. Cross-asset GNN atoms                                            │
│  2. FTS5                                                             │
│  3. Predicate keyword boost                                          │
│  4. Direct ticker match                                              │
│  5a/5b. High-value predicates / fallback top-confidence             │
│  → Re-rank by authority.effective_score                              │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                    ┌────────────┼─────────────┐
                    ▼            ▼             ▼
┌────────────┐  ┌─────────┐  ┌──────────────────────────────────────┐
│  llm/      │  │analytics│  │         api.py (Flask)                │
│            │  │         │  │                                        │
│ ollama_    │  │portfolio│  │  KB core:  POST /ingest  GET /stats   │
│ client.py  │  │universe_│  │            POST /retrieve GET /query  │
│ (chat,     │  │expander │  │            GET /search   GET /health  │
│ chat_vision│  │pattern_ │  │  Repair:   POST /repair/diagnose      │
│ list_models│  │detector │  │            POST /repair/proposals     │
│ is_avail.) │  │snapshot_│  │            POST /repair/execute       │
│            │  │curator  │  │  Product:  POST /auth/register        │
│ overlay_   │  │user_    │  │            POST /auth/token           │
│ builder.py │  │modeller │  │            GET/POST /users/{id}/port. │
│            │  │network_ │  │  Screenshot: POST /users/{id}/        │
│ prompt_    │  │effect_  │  │              history/screenshot        │
│ builder.py │  │engine   │  │  Notifications, tips, alerts,         │
└────────────┘  │backtest │  │  network, patterns, universe          │
                │counter- │  │                                        │
                │factual  │  │  GET / → static/index.html (SPA)      │
                └─────────┘  └──────────────────────────────────────┘
                                              │
                              ┌───────────────┼──────────────────┐
                              ▼               ▼                  ▼
                    ┌──────────────┐  ┌────────────┐  ┌─────────────────┐
                    │ middleware/  │  │  users/    │  │ notifications/  │
                    │ auth.py      │  │ user_store │  │ tip_scheduler   │
                    │ rate_limiter │  │ personal_kb│  │ tip_formatter   │
                    │ validators   │  └────────────┘  │ snapshot_curator│
                    │ audit        │                   │ telegram_notif. │
                    └──────────────┘                   └─────────────────┘
```

---

## Data Flow

### Ingest Path (background)

```
Scheduler timer fires
    → adapter.fetch()          # pull from external API
    → adapter.transform()      # normalise to RawAtom list
    → atom.validate()          # drop malformed atoms
    → kg.add_fact()            # UPSERT into facts table
        → FTS5 index updated
        → contradiction detector runs
        → superseded atoms marked in metadata
    → AdapterStatus updated    # last_run_at, total_atoms, errors
```

### Retrieve Path (request)

```
POST /retrieve { message }
    → _extract_key_terms()     # lowercase, stopword-filtered
    → _extract_tickers()       # uppercase 2-5 char sequences
    → pre-compute boosted_predicates from _KEYWORD_PREDICATE_BOOST
    → strategy 0: graph-relational context (PageRank+clustering+BFS)
                  fires on relational/explanatory queries or no tickers
                  builds graph_snippet prepended to final output
    → strategy 1: GNN atoms (if cross-asset query)
    → strategy 2: FTS (skipped if tickers + intent keywords present)
    → strategy 3: predicate boost per ticker
    → strategy 4: direct ticker match (LIMIT 6)
    → strategy 5a: high-value predicates for first term
    → strategy 5b: fallback top-confidence (if < 8 results)
    → re-rank by authority.effective_score
    → apply AdaptationNudges (from prior-turn streak):
          prefer_recent=True       → sort by timestamp DESC
          prefer_high_authority    → filter atoms below authority cutoff
          retrieval_scope_broadened → broaden DB fetch if < 8 results
    → increment hit_count for all returned (subject, predicate) pairs
          feeds frequency term (δ) in PageRank importance formula
    → format into [Signals] [Theses] [Macro] [Research] [Other]
    → prepend graph_snippet if produced
    → compute_stress(atoms)
    → update session streak:
          stress >= 0.65 → streak++  else streak--
    → compute AdaptationNudges for next turn
    → if nudges.refresh_domain_queued → scheduler.run_now('yfinance')
    → if stress > 0.35 OR atoms < 8:
          classify_insufficiency(topic, stress_report, conn)
          → append kb_diagnosis to response
    → return { snippet, atoms, stress, adaptation?, kb_diagnosis? }
```

### Adaptation Path

```
GET  /adapt/status?session_id=X
    → return { streak, last_stress } for session

POST /adapt/reset { session_id }
    → zero streak + last_stress for session
    → use on topic shift or new conversation start
```

#### AdaptationNudges rules (EpistemicAdaptationEngine)

| Rule | Condition | Effect |
|---|---|---|
| 1. Scope broadening | domain_entropy < 0.35 + streak ≥ 2 | Broadens DB fetch to all sources |
| 2. Authority filter | authority_conflict > 0.55 + streak ≥ 2 | Drops atoms below authority cutoff |
| 3. Recency bias | decay_pressure > 0.50 + streak ≥ 2 | Sorts atoms by timestamp DESC |
| 4. Consolidation mode | streak ≥ 3 | Lowers escalation threshold, raises confidence floor |
| 5. Domain refresh queue | decay_pressure > 0.60 + streak ≥ 3 | Logs to domain_refresh_queue; dispatches run_now('yfinance') |
| 6. KB insufficiency detection | consolidation fires ≥ 5× in 7 days | Triggers classify_insufficiency + generate_repair_proposals |

### Governance Path (on-demand)

```
POST /repair/diagnose { topic }
    → retrieve(topic) → compute_stress(atoms)
    → classify_insufficiency(topic, stress, conn)
    → return InsufficiencyDiagnosis { types, signals, confidence }

POST /repair/proposals { topic }
    → diagnose(topic)
    → generate_repair_proposals(diagnosis, conn)
    → governance_verdict(validate_all(topic)) — applies confidence penalty
    → return proposals[] { strategy, preview, simulation, validation }

POST /repair/execute { proposal_id, dry_run=true }
    → fetch proposal from repair_proposals table
    → _snapshot_signals() before
    → apply mutations atomically (BEGIN IMMEDIATE)
    → _snapshot_signals() after
    → auto-rollback if stress worsens by > 0.05
    → write execution_log
    → return ExecutionResult { before, after, divergence, mutations_applied }

POST /kb/graph { message }
    → retrieve(message, limit=100)
    → build_graph_context(atoms) → PageRank + clusters + BFS paths
    → return graph-structured context string

POST /kb/traverse { topic }
    → broad DB fetch for topic (LIMIT 200)
    → what_do_i_know_about(topic, atoms) → BFS expansion depth 3
    → return traversal string + connected concept list
```

---

## Ingest Intervals

| Adapter | Interval | Rationale |
|---|---|---|
| `YFinanceAdapter` | 5 min | Price + signals near-real-time |
| `OptionsAdapter` | 15 min | Options chains update intraday |
| `RSSAdapter` | 15 min | Headlines cycle every 15–30 min |
| `SignalEnrichmentAdapter` | 30 min | Derived signals from historical + options data |
| `PatternAdapter` | 60 min | Pattern detection over rolling windows |
| `LLMExtractionAdapter` | 60 min | LLM-based entity and signal extraction from RSS |
| `EDGARAdapter` | 6 hours | Filings are rare; daily is sufficient |
| `EDGARRealtimeAdapter` | 30 min | 8-K real-time filings via EDGAR full-text search |
| `HistoricalBackfillAdapter` | On-demand | One-shot via `POST /ingest/historical` |
| `FREDAdapter` | 24 hours | FRED macro series update daily at most |

`SeedSyncClient` runs every hour (independent of the scheduler) to check for a newer KB seed on GitHub Releases and apply it if found.

---

## Persistence

- **Database**: SQLite with WAL journal mode (`trading_knowledge.db`)
- **Uniqueness**: `UNIQUE(subject, predicate, object)` — same triple never duplicated; updates happen via supersession metadata
- **Decay**: Background worker runs every 24h, decays confidence on old atoms using source-specific half-lives
- **FTS**: FTS5 virtual table `facts_fts` is kept in sync with the `facts` table for full-text search

---

## Epistemic Model

Every fact carries:

| Field | Purpose |
|---|---|
| `confidence` | Epistemic certainty [0.0–1.0], decays over time |
| `source` | Provenance string — prefix-matched for authority weight |
| `timestamp` | ISO-8601 ingestion time |
| `metadata` | JSON bag — `as_of`, `superseded_by`, `target_high`, etc. |

Authority weights are assigned by source prefix (see `knowledge/authority.py`). The `effective_score` function combines confidence × authority weight for re-ranking.

---

## Governance Stack (now live)

The JARVIS epistemic governance stack is fully wired:

| Module | Where wired | Role |
|---|---|---|
| `graph_retrieval.py` | `retrieval.py` strategy 0 | PageRank centrality, community clusters, BFS concept paths |
| `causal_graph.py` | Startup + overlay path + API | `ensure_causal_edges_table()` at startup, `traverse_causal()` via overlay builder, `/kb/causal-edge` + `/kb/causal-edges` endpoints |
| `kb_insufficiency_classifier.py` | `POST /retrieve` + `POST /repair/diagnose` | 9-rule insufficiency classifier — fires on elevated stress or thin coverage |
| `kb_validation.py` | `POST /repair/proposals` (governance hook) | 3-layer governance: schema, semantic, cross-topic validation |
| `kb_repair_proposals.py` | `POST /repair/proposals` | Generates repair proposals with preview + simulation + validation target |
| `kb_repair_executor.py` | `POST /repair/execute`, `/rollback`, `/impact` | Human-gated atomic execution with auto-rollback and divergence tracking |

### Insufficiency Types (9 rules)

| Type | Condition |
|---|---|
| `coverage_gap` | < 10 atoms AND narrow sourcing |
| `representation_inconsistency` | High conflict + high supersession |
| `authority_imbalance` | High authority conflict AND > 60% low-auth atoms |
| `semantic_duplication` | Many atoms + high Jaccard similarity |
| `granularity_too_fine` | Many atoms + low predicate diversity + short objects |
| `missing_schema` | Required predicates absent for detected domain |
| `domain_boundary_collapse` | High entropy + many source prefixes (topic too broad) |
| `semantic_incoherence` | Validation Layer 2 severity > 0.5 |
| `cross_topic_drift` | Validation Layer 3 severity > 0.4 |

### Repair Strategies

| Strategy | When proposed |
|---|---|
| `ingest_missing` | Coverage gap detected |
| `resolve_conflicts` | Representation inconsistency |
| `merge_atoms` | Semantic duplication |
| `introduce_predicates` | Missing schema predicates |
| `reweight_sources` | Authority imbalance |
| `deduplicate` | Semantic duplication (alt) |
| `split_domain` | Domain boundary collapse |
| `restore_atoms` | Entropy collapse after prior repair |
| `manual_review` | Unknown / no automated strategy |

## UK Market Context

The system is configured for UK/LSE-first operation:

| Setting | Value |
|---|---|
| Default timezone | `Europe/London` |
| Default delivery time | `07:30` |
| Default account currency | `GBP` |
| Default watchlist | FTSE 100 heavyweights + UK macro proxies (`.L` suffix) |
| Options watchlist | Top FTSE names with liquid options |
| LLM universe expansion | UK market context injected for `.L` tickers |
| Low-liquidity options | `_LOW_OPTIONS_LIQUIDITY` set — confidence capped for `iv_rank` (0.40) and `smart_money_signal` (0.35) |

---

## Frontend (Internal Tool)

`static/index.html` — single-page Bloomberg-terminal-style SPA served at `GET /`. Zero build step.

**Screens:** Auth · Dashboard · Portfolio · Chat · Tips · Patterns · Network

**Portfolio screen — three entry paths:**
1. **Screenshot upload** — drop/click broker screenshot → `POST /users/{id}/history/screenshot` → `llava` vision model extracts holdings JSON → auto-populates rows
2. **FTSE sector quick-add** — `[+ FTSE Banks]` `[+ FTSE Energy]` `[+ FTSE Mining]` `[+ FTSE Pharma]` `[+ FTSE Tech]` buttons
3. **Manual add with autocomplete** — seeds from `GET /universe/coverage`, falls back to hardcoded FTSE top-25 list; `.L` suffix aware

---

## Vision Pipeline

`POST /users/{id}/history/screenshot` — broker screenshot → holdings extraction:

```
multipart/form-data (file: image/png|jpeg, max 10 MB)
    → list_models() check — return vision_unavailable gracefully if llava absent
    → base64 encode image
    → chat_vision(image_b64, prompt, model='llava')
    → strip markdown fences from response
    → json.loads → normalise (uppercase ticker, float coercion)
    → return { holdings: [{ticker, quantity, avg_cost}], vision_available, count }
```

Requires `llava` pulled locally: `ollama pull llava` (or `make setup-models`).
Override model: `OLLAMA_VISION_MODEL` env var.

---

## Seed Management

| Script | Purpose |
|---|---|
| `scripts/export_seed.py` | Export shared KB tables to `tests/fixtures/kb_seed.sql` |
| `scripts/push_seed.py` | Export + upload to GitHub Releases (`seed-YYYYMMDD-HHMM` tag) |
| `scripts/load_seed.py` | Load seed SQL into local DB |
| `ingest/seed_sync.py` | Background hourly poll — downloads newer seed from GitHub Releases and applies shared tables only; never touches `user_*` tables |

Seed allowlist (tables synced): `facts`, `fact_conflicts`, `causal_edges`, `pattern_signals`, `signal_calibration`, and governance tables. Personal KB (`user_*`) is structurally protected.

---

## Module Status

Status taxonomy:

- **Live** — imported and executed in startup/request path
- **Partial** — schema/API wiring is live but downstream decision logic not yet integrated
- **Dormant** — file exists but is not wired into live runtime

### Dormant

| Module | Notes |
|---|---|
| `knowledge/graph_v2.py` | Requires `aiosqlite`; not imported in live path |
| `knowledge/graph_enhanced.py` | Standalone class; not wired into `api.py` or `retrieval.py` |

### Partial

| Module | Live today | Pending |
|---|---|---|
| `confidence_intervals.py` | `ensure_confidence_columns()` at startup; `GET /kb/confidence` exposed | Interval not yet fed into `position_size_pct` |

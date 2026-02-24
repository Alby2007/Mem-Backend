# Code Map

Complete file-by-file reference for the Trading Galaxy codebase.

---

## Root

### `api.py`
Flask REST API — the main entry point.

- Initialises `KnowledgeGraph`, `decay_worker`, and `IngestScheduler`
- Registers all four adapters with their intervals
- Exposes all HTTP endpoints
- Optional dependencies (`epistemic_stress`, `epistemic_adaptation`, `working_state`) are imported with graceful fallback

**Key globals:** `app`, `_kg`, `_decay_worker`, `_ingest_scheduler`

**Endpoints:** `POST /ingest`, `GET /query`, `POST /retrieve`, `GET /search`, `GET /context/<entity>`, `GET /stats`, `GET /health`, `GET /ingest/status`

---

### `retrieval.py`
Zero-LLM multi-strategy retrieval engine. Called by `POST /retrieve`.

**Public API:**
- `retrieve(message, conn, limit=30) → (snippet: str, atoms: List[dict])`

**Key constants:**
- `_KEYWORD_PREDICATE_BOOST` — maps query keywords (`upside`, `target`, `regime`, `sector`, …) to predicate names for intent-aware fetching
- `_HIGH_VALUE_PREDICATES` — predicates that always surface in strategy 5a
- `_NOISE_PREDICATES` — predicates never returned to callers
- `_CROSS_ASSET_KW` — trigger words that activate GNN atom strategy

**Helper functions:**
- `_extract_key_terms(message)` — lowercase, stopword-filtered term list
- `_extract_tickers(message)` — uppercase 2–5 char sequences minus stopwords

**Retrieval strategies (in order):**

| # | Name | Condition | Source |
|---|---|---|---|
| 1 | Cross-asset GNN | query contains compare/vs/portfolio/… | `source = 'cross_asset_gnn'` |
| 2 | FTS | no (tickers AND boosted_predicates) | `facts_fts MATCH` |
| 3 | Predicate boost | boosted_predicates non-empty | exact predicate + subject match |
| 4 | Direct ticker | tickers extracted | subject LIKE ticker |
| 5a | High-value predicates | always | predicate IN (…) AND subject/object LIKE term |
| 5b | Fallback | results < 8 | top confidence DESC |

**Output sections:** `[Signals & Positioning]`, `[Theses & Evidence]`, `[Macro / Regime]`, `[Research]`, `[Other]`

---

### `requirements.txt`
```
flask>=3.0.0,<4.0.0
werkzeug>=3.0.0,<4.0.0
yfinance>=0.2.0
fredapi>=0.5.0
feedparser>=6.0.0
requests>=2.28.0
```

---

## `ingest/`

### `ingest/__init__.py`
Package exports. All adapter imports are wrapped in `try/except` so missing optional deps don't break startup.

**Exports:** `BaseIngestAdapter`, `RawAtom`, `IngestScheduler`, `YFinanceAdapter`, `FREDAdapter`, `EDGARAdapter`, `RSSAdapter`

---

### `ingest/base.py`
Interface contract for all ingest adapters. Read this before building a new adapter.

**`RawAtom` dataclass:**
```
subject:    str           # entity (e.g. 'AAPL', 'us_macro')
predicate:  str           # relationship (e.g. 'signal_direction')
object:     str           # value (e.g. 'long', 'tight policy')
confidence: float = 0.5   # [0.0, 1.0]
source:     str           # must use a recognised prefix
metadata:   dict = {}     # any extra fields
```

**`BaseIngestAdapter` abstract class:**
- `fetch() → List[RawAtom]` — **must implement**
- `transform(atoms) → List[RawAtom]` — optional override
- `run() → List[RawAtom]` — calls fetch → transform → validate; drops invalid atoms
- `push(atoms, kg) → dict` — writes to `KnowledgeGraph`
- `run_and_push(kg) → dict` — convenience wrapper

Also contains `ExampleSignalAdapter` and `ExampleMacroAdapter` stubs.

---

### `ingest/scheduler.py`
Background scheduler that fires each adapter on its own `threading.Timer` loop.

**`AdapterStatus` dataclass:** `name`, `interval_sec`, `last_run_at`, `last_success_at`, `last_error`, `last_error_at`, `total_runs`, `total_atoms`, `total_errors`, `is_running`

**`IngestScheduler`:**
- `register(adapter, interval_sec)` — add an adapter
- `start()` — runs all adapters immediately in daemon threads, then re-arms timers
- `stop()` — cancels all pending timers
- `get_status() → dict` — returns all `AdapterStatus.to_dict()` keyed by name

A failed adapter never blocks others — errors are caught, logged, recorded in `AdapterStatus`, and the timer re-arms normally.

---

### `ingest/yfinance_adapter.py`
Pulls price, fundamentals, analyst consensus, and ETF-specific data from Yahoo Finance via `yfinance`.

**Watchlist:** ~50 tickers across all 11 S&P sectors + broad market ETFs + rates/credit ETFs + macro proxy ETFs (GLD, SLV, UUP, USO).

**Key constants:**
- `_WATCHLIST` — full 50-ticker list
- `_BATCH_SIZE = 10` — tickers per yfinance request
- `_BATCH_DELAY_SEC = 1.5` — seconds between batches
- `_ETF_QUOTE_TYPES` — quote types routed to ETF path
- `_ETF_CATEGORY_FALLBACK` — hardcoded category labels for 25 known ETFs

**Atoms produced (equity path):**

| Predicate | Example value | Confidence |
|---|---|---|
| `last_price` | `191.55` | 0.95 |
| `sector` | `technology` | 0.90 |
| `market_cap_tier` | `mega_cap` | 0.85 |
| `volatility_regime` | `high_volatility` | 0.80 |
| `price_target` | `253.99` | 0.75 |
| `signal_direction` | `long` (price < target) | 0.65 |
| `earnings_quality` | `next_earnings: 2026-02-25` | 0.85 |

**Atoms produced (ETF path):**

| Predicate | Example value | Confidence |
|---|---|---|
| `last_price` | `52.48` | 0.95 |
| `sector` | `etf:financial` | 0.90 |
| `market_cap_tier` | `large_cap` (from AUM) | 0.90 |
| `volatility_regime` | `medium_volatility` (from beta) | 0.75 |
| `signal_direction` | `mid_range` / `near_high` / `near_low` | 0.60 |

**Exponential backoff:** 3 retries on 429/rate/timeout errors with delays of 2, 4, 8 seconds.

---

### `ingest/fred_adapter.py`
Pulls macro indicators from the St. Louis Fed FRED API via `fredapi`.

**Requires:** `FRED_API_KEY` environment variable

**Series fetched:**

| FRED Series | Subject | Predicate |
|---|---|---|
| `FEDFUNDS` | `us_macro` | `dominant_driver` |
| `CPIAUCSL` | `us_macro` | `inflation_environment` |
| `GDP` | `us_macro` | `growth_environment` |
| `UNRATE` | `us_labor` | `dominant_driver` |
| `GS10` | `us_yields` | `dominant_driver` |
| `GS2` | `us_yields` | `dominant_driver` |
| `T10Y2Y` | `us_yields` | `risk_factor` (yield curve spread) |
| `BAMLH0A0HYM2` | `us_credit` | `risk_factor` (HY spread) |

Also derives `central_bank_stance` and `regime_label` from the fetched values.

---

### `ingest/edgar_adapter.py`
Pulls recent SEC filings from the EDGAR full-text search API (no API key required).

**Tickers queried:** Subset of the yfinance watchlist with active filing history.

**Atoms produced:**

| Form type | Predicate | Confidence |
|---|---|---|
| `8-K`, `10-Q`, `10-K`, `S-1`, `SC 13G` | `catalyst` | 0.85 |
| `Form 4` (insider transaction) | `risk_factor` | 0.80 |

**User-Agent:** Configurable via `EDGAR_USER_AGENT` env var (default: `trading-galaxy-kb research@example.com`).

---

### `ingest/rss_adapter.py`
Pulls financial news headlines from public RSS feeds via `feedparser`.

**Active feeds:**

| Key | URL |
|---|---|
| `ft_home` | `https://www.ft.com/rss/home` |
| `investing_com` | `https://www.investing.com/rss/news.rss` |
| `bbc_business` | `http://feeds.bbci.co.uk/news/business/rss.xml` |
| `cnbc_finance` | `https://search.cnbc.com/rs/search/…` |
| `marketwatch` | `http://feeds.marketwatch.com/marketwatch/topstories/` |

**Atoms produced:** `financial_news | key_finding | <headline>` at confidence 0.55.

Extracts uppercase ticker mentions from headlines to tag atoms with `ticker_mention` metadata.

---

## `knowledge/`

### `knowledge/__init__.py`
Re-exports `TradingKnowledgeGraph as KnowledgeGraph`.

Documents which modules are live vs. not-yet-wired.

---

### `knowledge/graph.py` ← **Core store**
`TradingKnowledgeGraph` — the primary triple store.

**Schema:**
```sql
facts (
    id INTEGER PRIMARY KEY,
    subject   TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object    TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    source     TEXT,
    timestamp  TEXT,
    metadata   TEXT          -- JSON
    UNIQUE(subject, predicate, object)
)
```
Plus `facts_fts` (FTS5), `fact_conflicts`, `decay_log` tables.

**Key methods:**
- `add_fact(subject, predicate, object, confidence, source, metadata)` — upsert; runs contradiction detection
- `query(subject, predicate, object, limit)` — filtered triple query
- `search(q, limit, category)` — FTS search
- `get_context(entity)` — all facts for a subject
- `get_stats()` — `{total_facts, unique_predicates, unique_subjects}`
- `thread_local_conn()` — safe per-thread connection for Flask workers

**WAL config:** `journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-64000` (64 MB).

---

### `knowledge/authority.py`
Source trust weights and effective score calculation.

**Source prefix → authority weight:**

| Prefix | Weight |
|---|---|
| `exchange_feed_*` | 1.0 |
| `regulatory_filing_*` | 0.95 |
| `curated_*` | 0.90 |
| `earnings_*` | 0.85 |
| `broker_research_*` | 0.80 |
| `macro_data_*` | 0.80 |
| `model_signal_*` | 0.70 |
| `technical_*` | 0.65 |
| `news_wire_*` | 0.60 |
| `alt_data_*` | 0.55 |
| `social_signal_*` | 0.35 |

**`effective_score(atom) → float`** — `confidence × authority_weight`, used for re-ranking in `retrieval.py`.

---

### `knowledge/decay.py`
Confidence decay engine.

- Half-lives per source prefix (e.g. `exchange_feed_*` = ~10 min, `macro_data_*` = 60 days)
- `get_decay_worker(db_path)` — starts a 24h background thread that applies decay to all facts
- `ensure_decay_column(conn)` — migration helper

---

### `knowledge/contradiction.py`
Conflict detection for mutually exclusive facts.

- Detects when two atoms share `(subject, predicate)` but have contradicting objects
- Logs to `fact_conflicts` table
- Used by `graph.py` on every `add_fact()` call

---

### `knowledge/epistemic_stress.py`
Computes a composite stress signal over a retrieved atom set.

**`compute_stress(atoms, key_terms, conn) → StressResult`**

**`StressResult` fields:**

| Field | Meaning |
|---|---|
| `composite_stress` | Weighted combination of all sub-signals |
| `decay_pressure` | Fraction of atoms with significantly decayed confidence |
| `authority_conflict` | Presence of atoms from conflicting authority levels |
| `supersession_density` | Fraction of atoms that have been superseded |
| `conflict_cluster` | Density of atoms in the `fact_conflicts` log |
| `domain_entropy` | Diversity of predicates (high = unfocused retrieval) |

---

### `knowledge/working_state.py`
Cross-session goal/topic memory.

- Persists `goal`, `topic`, conversation thread between sessions
- `maybe_persist(session_id, turn_count, goal, topic)` — saves on turn 1 or when forced
- `format_prior_context(session_id)` — formats stored state for injection into `/retrieve` response

---

### `knowledge/kb_domain_schemas.py`
Predicate ontology reference (not imported at runtime).

Defines allowed predicates per domain: `INSTRUMENT_PREDICATES`, `THESIS_PREDICATES`, `MACRO_PREDICATES`, `COMPANY_PREDICATES`, `REPORT_PREDICATES`.

---

### Not-Yet-Wired Modules

| File | Purpose |
|---|---|
| `graph_v2.py` | Extended graph with richer traversal methods |
| `graph_enhanced.py` | Additions to graph_v2 |
| `graph_retrieval.py` | PageRank, BFS, community-cluster traversal |
| `kb_validation.py` | Multi-layer atom validation pipeline |
| `kb_insufficiency_classifier.py` | Detects gaps / missing knowledge in the KB |
| `kb_repair_proposals.py` | Generates repair suggestions for KB gaps |
| `kb_repair_executor.py` | Executes approved repair proposals |

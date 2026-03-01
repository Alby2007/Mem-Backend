# Code Map

Complete file-by-file reference for the Trading Galaxy codebase.

---

## Root

### `api.py`
Flask REST API — the main entry point. ~7,200 lines.

- Initialises `KnowledgeGraph`, `decay_worker`, `IngestScheduler`, and `SeedSyncClient`
- Registers all adapters with their intervals
- Exposes all HTTP endpoints
- Optional dependencies imported with graceful fallback (`HAS_AUTH`, `HAS_PRODUCT_LAYER`, `HAS_LIMITER`, etc.)

**Key globals:** `app`, `_kg`, `_decay_worker`, `_ingest_scheduler`, `_DB_PATH`

**Endpoint groups:**

| Group | Key endpoints |
|---|---|
| KB core | `POST /ingest` · `GET /query` · `POST /retrieve` · `GET /search` · `GET /context/<e>` · `GET /stats` · `GET /health` |
| Ingest | `GET /ingest/status` · `POST /ingest/run-all` · `POST /ingest/historical` |
| Repair/governance | `POST /repair/diagnose` · `POST /repair/proposals` · `POST /repair/execute` · `POST /repair/rollback` · `GET /repair/impact` |
| KB graph | `POST /kb/graph` · `POST /kb/traverse` · `GET /kb/causal-chain` · `POST /kb/causal-edge` · `GET /kb/causal-edges` · `GET /kb/confidence` |
| Auth | `POST /auth/register` · `POST /auth/token` · `POST /auth/refresh` · `POST /auth/logout` |
| User / product | `GET/POST /users/<id>/portfolio` · `GET /users/<id>/model` · `POST /users/<id>/onboarding` · `GET /users/<id>/snapshot/preview` · `POST /users/<id>/snapshot/send-now` |
| Screenshot | `POST /users/<id>/history/screenshot` — vision model extraction |
| Tips | `GET /users/<id>/tip/preview` · `GET/POST /users/<id>/tip/config` · `GET /users/<id>/delivery-history` |
| Positions | `GET /users/<id>/positions/open` · `GET /users/<id>/positions/closed` |
| Alerts | `GET /users/<id>/alerts` · `GET /users/<id>/alerts/unread-count` |
| Universe | `POST /users/<id>/expand-universe` · `GET /universe/coverage` · `GET /universe/trending` · `GET /universe/staging/global` |
| Network | `GET /network/health` · `GET /network/calibration/<ticker>` · `GET /network/cohort/<ticker>` |
| Patterns | `GET /patterns/live` · `GET /patterns/<id>` |
| Feedback | `POST /feedback` |
| Chat | `POST /chat` (overlay-mode aware) |
| Frontend | `GET /` → `static/index.html` |

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

### `Makefile`
Developer convenience targets.

| Target | Purpose |
|---|---|
| `make setup` | `install` + `setup-models` |
| `make setup-models` | `ollama pull llava && ollama pull llama3.2` |
| `make install` | `pip install -r requirements.txt` |
| `make dev` | Start Flask on port 5051 |
| `make test` | Full pytest suite |
| `make test-screenshot` | Screenshot upload tests only |
| `make ingest` | `POST /ingest/run-all` |
| `make seed` / `push-seed` | Load / export+push KB seed |
| `make docker-up` | `docker-compose up --build` + pull models into container |
| `make lint` | `ruff check` |

---

### `requirements.txt`
```
flask>=3.0.0,<4.0.0
werkzeug>=3.0.0,<4.0.0
yfinance>=0.2.0
fredapi>=0.5.0
feedparser>=6.0.0
requests>=2.28.0
PyJWT>=2.8.0
bcrypt>=4.1.0
flask-limiter>=3.5.0
cryptography>=42.0.0
limits>=3.6.0
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

**Watchlist (UK/LSE-first):** FTSE 100 heavyweights (`.L` suffix), UK FX pairs (`GBPUSD=X`, `EURGBP=X`), UK gilt proxy, global macro anchors (GLD, ^VIX, ^GSPC).

**Dynamic watchlist:** `DynamicWatchlistManager` extends the base list with per-user expanded tickers.

**Key constants:**
- `_DEFAULT_TICKERS` — FTSE-first default list
- `_BATCH_SIZE = 10` — tickers per yfinance request
- `_BATCH_DELAY_SEC = 1.5` — seconds between batches
- `_ETF_QUOTE_TYPES` — quote types routed to ETF path
- `_ETF_CATEGORY_FALLBACK` — hardcoded category labels for known ETFs

**Atoms produced (equity path):**

| Predicate | Example value | Confidence |
|---|---|---|
| `last_price` | `27.50` | 0.95 |
| `sector` | `energy` | 0.90 |
| `market_cap_tier` | `mega_cap` | 0.85 |
| `volatility_regime` | `high_volatility` | 0.80 |
| `price_target` | `32.00` | 0.75 |
| `signal_direction` | `long` (price < target) | 0.65 |
| `earnings_quality` | `next_earnings: 2026-04-30` | 0.85 |

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

### Knowledge Extension Module Status

| File | Status | Notes |
|---|---|---|
| `graph_v2.py` | Dormant | Requires `aiosqlite`; not imported in live path |
| `graph_enhanced.py` | Dormant | Standalone class; not wired into `api.py` or `retrieval.py` |
| `confidence_intervals.py` | Partial | `ensure_confidence_columns()` at startup; `GET /kb/confidence` live; not yet fed into `position_size_pct` |
| `causal_graph.py` | Live | `ensure_causal_edges_table()` at startup; overlay traversal; `/kb/causal-chain`, `/kb/causal-edge`, `/kb/causal-edges` |
| `graph_retrieval.py` | Live | PageRank, BFS, community-cluster traversal — retrieval strategy 0 |
| `kb_validation.py` | Live | Governance validation hook in repair proposal flow |
| `kb_insufficiency_classifier.py` | Live | 9-rule insufficiency detection in retrieve/diagnose paths |
| `kb_repair_proposals.py` | Live | Generates repair proposals with preview + simulation |
| `kb_repair_executor.py` | Live | Atomic execution with auto-rollback and divergence tracking |

---

## `ingest/` — Additional Adapters

### `ingest/options_adapter.py`
Fetches options chain data for liquid FTSE names via `yfinance` and computes options-regime atoms.

**Ticker scope:** Top FTSE names with liquid options. SPY included for market-level tail risk atoms.

**Low-liquidity handling:** `_LOW_OPTIONS_LIQUIDITY` frozenset — `iv_rank` confidence capped at 0.40, `smart_money_signal` at 0.35 for thin UK options names.

**Atoms produced:**

| Predicate | Notes |
|---|---|
| `iv_rank` | 30d IV percentile of 52-week range (0–100) |
| `put_call_ratio` | Sum put OI / sum call OI, front two expirations |
| `options_regime` | `compressed` / `normal` / `elevated_vol` |
| `smart_money_signal` | `call_sweep` / `put_sweep` / `none` |
| `iv_skew_ratio` | OTM put IV / ATM IV |
| `iv_skew_25d` | OTM put IV − OTM call IV (5% wings) |
| `skew_regime` | `normal` / `elevated` / `spike` |
| `spy_skew_ratio` | Market-level SPY skew |
| `tail_risk` | `normal` / `moderate` / `elevated` / `extreme` |

---

### `ingest/historical_adapter.py`
One-shot backfill: fetches **5 years** of daily OHLCV via `yf.download()` and stores only derived summary atoms — never raw OHLCV.

**Atoms produced:** `return_1w/1m/3m/6m/1y/3y/5y`, `volatility_30d/90d/5y`, `max_drawdown_5y`, `drawdown_from_high`, `avg_volume_30d`, `price_52w_high/low`, `price_3y_ago`, `return_vs_spy_1m/3m`

**Key constants:** `_W_1Y=252`, `_W_3Y=756`, `_W_5Y=1260` trading day windows.

**Trigger:** `POST /ingest/historical` or called from `run-all`.

---

### `ingest/signal_enrichment_adapter.py`
Derives higher-level signal atoms by cross-referencing historical, price, and options data already in the KB.

**Atoms produced:** `price_regime`, `volume_trend`, `momentum_signal`, `risk_reward_ratio`, `thesis_risk_level`, `conviction_tier`, `position_size_pct`, `upside_pct`

---

### `ingest/pattern_adapter.py`
Detects multi-factor chart and signal patterns over rolling windows. Writes to `pattern_signals` table.

---

### `ingest/llm_extraction_adapter.py`
Runs LLM extraction over queued RSS headlines to produce structured atoms. Uses `EXTRACTION_MODEL` (default `phi3`).

---

### `ingest/edgar_realtime_adapter.py`
Polls EDGAR full-text search for 8-K filings every 30 min. Deduplicates via `edgar_realtime_seen` table.

---

### `ingest/dynamic_watchlist.py`
`DynamicWatchlistManager` — merges default tickers with per-user expanded tickers from `user_universes` table.

**Promotion logic:** `coverage_count ≥ 3` → `added_to_ingest=1` → ticker enters the yfinance scheduled ingest.

---

### `ingest/fca_short_interest_adapter.py`
Downloads the daily FCA short position XLSX and extracts significant short positions (≥ 0.5% of issued share capital) for UK-listed equities.

**Maps:** `_ISIN_TO_TICKER` (ISIN → yfinance `.L` symbol) · `_NAME_TO_TICKER` (company name fallback).

**Atoms produced:** `{ticker} | fca_short_interest | "3.45% (Bridgewater Associates)"`

---

### `ingest/discovery_pipeline.py`
Universal Discovery Pipeline. Scans FCA short interest, RSS ticker mentions, and user portfolio additions. Scores tickers by `coverage_count`; promotes to scheduled ingest at threshold.

**Endpoint:** `POST /discover/run`

---

### `ingest/gdelt_adapter.py`
Fetches geopolitical tension tone scores from the GDELT 2.0 GKG Doc API (tonechart mode). Queries country pairs (e.g. US–Iran, Russia–Ukraine) and computes a count-weighted average tone from `bin` buckets.

**Interval:** 12 hours  
**Atoms produced:** `gdelt_tension | {country_pair} | tension_score:{score}` (confidence 0.68, source `geopolitical_data_gdelt`, half-life 3d)

---

### `ingest/ucdp_adapter.py`
Country conflict intensity adapter — uses GDELT Doc API `artlist` mode as a proxy for UCDP data (UCDP REST API now requires auth). Counts conflict/war/military articles per country over 7 days.

**Interval:** 12 hours  
**Thresholds:** ≥50 articles → `active_war`; 10–49 → `minor_conflict`; <10 → `stable`  
**Atoms produced:** `ucdp_conflict | {iso3} | {label}` + `ucdp_conflict | global_war_count | {N}` (confidence 0.72, source `geopolitical_data_ucdp`)

---

### `ingest/seed_sync.py`
Background hourly client: polls `Alby2007/Mem-Backend` GitHub Releases. If `seed-YYYYMMDD-HHMM` tag is newer than local `kb_meta.seed_tag`, downloads and applies. Hard-coded `_ALLOWED_TABLES` allowlist prevents any `user_*` table from being overwritten.

---

## `llm/`

### `llm/ollama_client.py`
Thin wrapper around Ollama REST API (`http://localhost:11434`).

| Function | Purpose |
|---|---|
| `chat(messages, model, stream, timeout)` | Text chat — returns str or None |
| `chat_vision(image_b64, prompt, model, timeout)` | Multimodal image+text — used by screenshot endpoint |
| `list_models()` | Returns list of locally available model names |
| `is_available()` | Quick liveness check |

**Env vars:** `OLLAMA_BASE_URL` · `OLLAMA_MODEL` (default `llama3.2`) · `OLLAMA_EXTRACTION_MODEL` (default `phi3`) · `OLLAMA_VISION_MODEL` (default `llava`)

All functions return `None` / `[]` on connection errors — callers degrade gracefully.

---

### `llm/overlay_builder.py`
Builds structured `overlay_cards` from KB atoms for `POST /chat` when `overlay_mode=True`. No LLM involved — pure KB lookup.

**Card types:** `signal_summary` · `causal_context` · `stress_flag`

**Entity extraction:** regex `[A-Z]{2,5}` on `screen_context` → filtered against `_SCREEN_CONTEXT_STOPWORDS` → validated against known KB subjects.

---

### `llm/prompt_builder.py`
Builds the `[system, user]` message pair for the LLM. Dynamically assembles the system prompt by injecting context-specific rule blocks based on what is present:

| Rule block | Condition |
|---|---|
| `_SYSTEM_PORTFOLIO_RULE` | `portfolio_context` present |
| `_SYSTEM_SIZING_RULE` | `portfolio_context` present |
| `_SYSTEM_POSITIONS_RULE` | portfolio + position-opportunity keywords |
| `_SYSTEM_GEO_PORTFOLIO_RULE` | portfolio + geo/war keywords |
| `_SYSTEM_GEO_NO_PORTFOLIO_RULE` | **no portfolio** + geo/war keywords — ensures users without a portfolio still get structured geo answers |
| `_SYSTEM_LIVE_DATA_RULE` | `live_context` present |
| `_SYSTEM_SEARCH_RULE` | `live_context` + `web_searched` |
| `_SYSTEM_GENERATION_RULE` | `opportunity_scan_context` present |
| `_SYSTEM_CONTINUITY_RULE` | `has_history=True` |

---

## `analytics/`

### `analytics/portfolio.py`
`build_portfolio_summary(db_path)` — aggregates KB atoms into long book, avoid book, sector weights, macro alignment, and top conviction list. Called by `GET /portfolio/summary`.

---

### `analytics/universe_expander.py`
Resolves a user interest description into tickers, ETFs, keywords, and causal relationships via Ollama LLM. Falls back gracefully when Ollama is unavailable.

**UK context:** Injects `_UK_MARKET_CONTEXT` prompt when `market_type='uk'` or interests contain `.L` tickers — ensures LSE ticker resolution with correct `.L` suffix.

**Tier caps:** basic = 20 tickers · pro = 100 tickers. Max 20 tickers per expansion request.

---

### `analytics/pattern_detector.py`
Detects conviction, momentum, and composite patterns over KB atoms. Writes `PatternSignal` rows. Used by `GET /patterns/live`.

---

### `analytics/snapshot_curator.py`
Curates personalised daily snapshot from KB atoms for a user. Selects top signals, macro context, and pattern alerts. Called by `GET /users/<id>/snapshot/preview` and `delivery_scheduler.py`.

**Signature:** `curate_snapshot(user_id, db_path, tier='basic') → CuratedSnapshot`

Opportunity count is capped by `TIER_LIMITS[tier]['batch_size']` — not hardcoded.

---

### `analytics/user_modeller.py`
Derives `user_models` row from portfolio history, feedback, and engagement signals. Called on portfolio submit and feedback.

---

### `analytics/network_effect_engine.py`
Computes cross-user calibration signals and cohort performance. Used by `GET /network/health` and `/network/cohort/<ticker>`.

---

### `analytics/signal_calibration.py`
Updates `signal_calibration` table from `POST /feedback` submissions. Applies Bayesian confidence updates.

**Key functions:** `update_calibration(ticker, pattern_type, timeframe, market_regime, outcome, db_path)` · `get_calibration(...)` · `_confidence_score(n)` · `_confidence_label(score)`

---

### `analytics/historical_calibration.py`
Sliding-window backtester that back-populates `signal_calibration` with historical pattern outcome statistics **before any live user feedback exists**.

**Approach:** 100-candle detection window slid through N years of daily OHLCV in steps of 5 candles. Each detected pattern is checked against the following 20 candles for T1/T2/T3 hits or stop-out.

**Regime classification:** Cross-asset proxies (SPY, HYG, TLT, GLD, VIX) used to label each window `risk_on_expansion` / `risk_off_contraction` / `stagflation` / `recovery`.

**Class:** `HistoricalCalibrator(db_path, window_size, forward_horizon, step_size)`
- `calibrate_ticker(ticker, ohlcv_df, proxy_data, lookback_years)` → `{patterns_detected, calibration_rows_written}`
- `calibrate_watchlist(tickers, lookback_years)` → per-ticker summary dict

**CLI:** `python -m analytics.historical_calibration --years 3`
**Endpoint:** `POST /calibrate/historical`

---

### `analytics/regime_history.py`
Classifies each calendar month over the historical record into a macro regime using cross-asset proxy data, then writes regime-conditional performance atoms to the KB.

**Regime matrix:** `risk_off_contraction` (SPY↓ + VIX↑) · `stagflation` (SPY flat + GLD↑) · `recovery` (SPY↑ + TLT↑) · `risk_on_expansion` (SPY↑ baseline)

**Atoms written per equity ticker:**
```
global_macro_regime | regime_history_YYYY_MM | <regime>
{TICKER}            | return_in_{regime}     | avg monthly return %
{TICKER}            | regime_hit_rate_{regime}| % months ticker was up
{TICKER}            | best_regime / worst_regime
```

**Class:** `RegimeHistoryClassifier(db_path)` · `run(tickers, lookback_years)`
**CLI:** `python -m analytics.regime_history --years 5`
**Endpoint:** `POST /calibrate/regime-history`

---

### `analytics/position_calculator.py`
Computes position size, risk/reward, and stop-loss levels from account size + KB atoms. Used by tip formatter.

---

### `analytics/position_monitor.py`
Background thread (`PositionMonitor`) — polls all open `tip_followups` rows every 5 minutes and fires alerts.

**Key functions:**

| Function | Purpose |
|---|---|
| `_check_triggers(pos, price, db)` | Evaluates CRITICAL/HIGH/MEDIUM trigger conditions (stop zone, structural invalidation, regime shift, T1 approach, etc.) |
| `_compute_confidence(db, ticker, direction)` | Normalised confidence: `confirming_atoms / total_relevant` over 4 predicates. Returns `None` if <2 atoms. |
| `_is_actionable_hours()` | Mon–Fri 06:30–18:30 UTC gate — all alerts suppressed outside this window |
| `_is_market_hours()` | Mon–Fri 08:00–16:30 UTC — HIGH/MEDIUM-only gate |
| `_check_expiry(pos, db)` | Marks expired rows in DB only; no Telegram (batched to next briefing) |
| `_send_telegram_alert_with_confidence(...)` | Formats via `format_emergency_alert_with_confidence` and sends via `TelegramNotifier` |

**Alert priorities and cooldowns:**

| Priority | Triggers | Cooldown | Hours gate |
|---|---|---|---|
| CRITICAL | Stop zone, zone origin breach, KB signal contradiction, earnings imminent | 6h | Actionable (06:30–18:30 UTC) |
| HIGH | T1 approach, conviction tier drop, regime shift | 4h | Market hours only |
| MEDIUM | T2 approach, sector tailwind reversal, short squeeze building | 24h | Market hours only |

---

### `analytics/counterfactual.py`
Builds "what if" counterfactual scenarios from KB atoms. Called by `POST /analytics/counterfactual`.

---

### `analytics/backtest.py`
Runs simplified signal backtest over historical atoms in KB. Called by `POST /analytics/backtest`.

---

### `analytics/adversarial_stress.py`
Generates adversarial stress scenarios to test KB robustness. Used in governance/repair flows.

---

### `analytics/alerts.py`
Generates user alerts from KB signal changes. Writes to `user_alerts` table. Used by `GET /users/<id>/alerts`.

---

## `middleware/`

### `middleware/auth.py`
JWT authentication. Provides `require_auth` decorator, `assert_self` (horizontal escalation guard), `register_user`, `authenticate_user`.

**Env vars:** `JWT_SECRET_KEY` · `JWT_EXPIRY_HOURS` (default 24) · `JWT_REFRESH_EXPIRY_DAYS` (default 30)

**Tables:** `user_auth` · `refresh_tokens`

**Lockout:** 5 failed attempts → 15-minute lockout.

---

### `middleware/rate_limiter.py`
Flask rate limiter. Buckets: `auth` (strict) · `ingest` · `portfolio` · `default`. Uses in-memory sliding window.

---

### `middleware/validators.py`
Request body validators for portfolio submission, onboarding, tip config, ingest atoms, feedback, and register. Returns 400 with structured error on failure.

---

### `middleware/audit.py`
Writes audit log entries to `audit_log` table on sensitive operations (auth, repair execution).

---

### `middleware/encryption.py`
Field-level encryption helpers for sensitive user data at rest.

---

## `users/`

### `users/user_store.py`
All user-facing DB operations: portfolios, models, preferences, delivery logs, tip followups.

**Defaults (UK):** `delivery_time=07:30` · `timezone=Europe/London` · `account_currency=GBP`

**Key functions (core):** `create_user` · `get_portfolio` · `save_portfolio` · `get_tip_config` · `set_tip_config` · `log_delivery` · `get_delivery_history` · `log_feedback`

**Key functions (position lifecycle):**

| Function | Purpose |
|---|---|
| `upsert_tip_followup(...)` | Insert a new followup row; auto-computes `expires_at` from timeframe; schema-aware (detects server column layout via `PRAGMA table_info`) |
| `create_tip_followup(...)` | Wrapper that sets `status='active'` — called from `taking_it` path in `api.py` |
| `get_user_open_positions(db, user_id)` | Returns `watching` + `active` followups for a user |
| `get_recently_closed_positions(db, user_id, since_date)` | Returns `closed`/`expired`/`stopped` followups since a date |
| `expire_stale_followups(db)` | Bulk-closes past-expiry rows; returns them for inclusion in Monday/Wednesday briefings. No Telegram. |
| `get_kb_changes_since(db, since_iso, tickers=None)` | Queries `facts` table for significant predicate changes since a timestamp. Scoped to 14 high-signal predicates confirmed written by KB adapters. Uses `timestamp` column (not `created_at`). |
| `get_watching_followups(db)` | Returns all `watching`+`active` rows across all users — used by `PositionMonitor`. |
| `update_followup_status(db, id, status, ...)` | Updates `status`, `alert_level`, `closed_at` for a followup row. |

---

### `users/personal_kb.py`
Per-user KB layer on top of the shared KB. Manages `user_universes`, `user_watchlist`, `user_signals`, `user_alerts` tables. Provides personal context for snapshot and tip generation.

---

## `notifications/`

### `notifications/tip_scheduler.py`
Living portfolio briefing scheduler. Replaces the single daily tip with a full position lifecycle delivery system.

**Monday cycle** (fires at `tip_delivery_time` on Mondays):
1. `expire_stale_followups()` — DB-only, no Telegram
2. `get_user_open_positions()` + `get_recently_closed_positions(7 days)`
3. `_pick_batch(N)` — new pattern setups eligible for user's tier/timeframes
4. `format_monday_briefing()` — open positions + new setups + closed last week
5. Auto-create `status='watching'` followup for every new setup sent
6. Skip send if all three sections empty

**Wednesday cycle** (fires at `tip_delivery_time` on Wednesdays):
1. `expire_stale_followups()`
2. `get_user_open_positions()` + `get_kb_changes_since(monday_00:00, tickers=open_tickers)`
3. `format_wednesday_update()` — open positions + KB changes + expired this cycle
4. Skip send if nothing to report

**Other days:** standard single-tip delivery (unchanged for non-briefing days).

---

### `notifications/tip_formatter.py`
Formats all tip and briefing messages into Telegram MarkdownV2. Source of truth for tier configuration.

**`TIER_LIMITS` (canonical tier config):**

| Tier | `delivery_days` | `batch_size` | `patterns` | `timeframes` |
|---|---|---|---|---|
| `basic` | `['monday','wednesday']` | 2 | FVG, IFVG | 1h, 4h |
| `pro` | `['monday','wednesday']` | 5 | All 7 patterns | 1h, 4h, 1d |
| `premium` | `'daily'` | 1 | All 7 patterns | 15m, 1h, 4h, 1d |

| Function | Purpose |
|---|---|
| `format_tip(pattern, position, tier)` | Single pattern tip — entry zone, stop, targets, tier-gated T3 |
| `format_monday_briefing(open, new_setups, closed, tier, get_price_fn)` | Full week-ahead briefing: 📍 In Position / 🔓 On Radar · new setups · closed last week |
| `format_wednesday_update(open, kb_changes, expired, tier, get_price_fn)` | Compound update: all open positions + significant KB atom changes since Monday |
| `format_emergency_alert_with_confidence(alert_type, pos, price, confidence)` | Real-time alert with optional `█████░░░░░ 50%` confidence bar (shown only if ≥2 KB atoms found) |

**Helper functions:** `_format_open_position_line(pos, price)` — renders zone status (in-zone ✅ / approaching / away); `_format_closed_position_line(pos)` — renders outcome with P&L.

---

### `notifications/snapshot_formatter.py`
Formats raw snapshot atoms into human-readable daily briefing sections: top signals, macro regime, risk flags, calendar.

---

### `notifications/telegram_notifier.py`
Sends formatted messages via Telegram Bot API. Requires `TELEGRAM_BOT_TOKEN` env var.

---

### `notifications/delivery_scheduler.py`
Background thread that fires the **daily briefing** (snapshot) at each user's configured delivery time. Enforces tier-based weekday gate before sending:
- `basic` → Monday + Wednesday
- `pro` → Monday + Wednesday
- `premium` → daily

Loads user tier from `user_preferences`, calls `curate_snapshot(user_id, db_path, tier=tier)`, then `format_snapshot()` → `TelegramNotifier.send()`.

---

## `static/`

### `static/index.html`
Single-page application (SPA) served at `GET /`. Bloomberg-terminal dark aesthetic. Zero build step — pure HTML/CSS/JS.

**Screens:**

| Screen | Purpose |
|---|---|
| Auth | Register / login — `POST /auth/register`, `POST /auth/token` |
| Dashboard | KB stats, conviction tiers, adapter health |
| Portfolio | Three entry paths (screenshot · sector quick-add · manual + autocomplete) → `POST /users/<id>/portfolio` |
| Chat | KB-grounded chat with optional overlay mode — `POST /chat` |
| Tips | Tip configuration, preview, delivery history |
| Patterns | Live pattern signals |
| Network | Network health, calibration leaderboard |

**Key JS globals:** `state` (`{token, userId, holdings}`) · `apiFetch()` · `showScreen()` · `FTSE_SECTORS` · `loadTickerList()`

---

## `scripts/`

| Script | Purpose |
|---|---|
| `scripts/export_seed.py` | Export shared KB tables → `tests/fixtures/kb_seed.sql`. Runs quality gate (fact count, open patterns). |
| `scripts/push_seed.py` | Export + create GitHub Release tagged `seed-YYYYMMDD-HHMM` with `kb_seed.sql` asset. Prunes releases > 10. |
| `scripts/load_seed.py` | Load `kb_seed.sql` into local DB via `executescript()` |
| `scripts/check_module_status_docs.py` | Verifies module status table in `architecture.md` matches live import state |
| `scripts/cf_purge.py` | Purges Cloudflare zone cache for `api.trading-galaxy.uk` (zone cache only — not CF Pages assets) |
| `scripts/check_regime_now.py` | Checks market regime atoms in DB and `/stats` API response |
| `scripts/check_geo_atoms.py` | Checks geo atom counts in DB (gdelt_tension, ucdp_conflict, acled_unrest) |
| `scripts/check_geo_exposure.py` | Checks geopolitical_risk_exposure atoms per ticker |
| `scripts/test_gdelt.py` | Tests GDELT API tonechart/artlist/timelinevol modes |
| `scripts/check_tables.py` | Lists DB table names and row counts |

---

## `deploy/`

| File | Purpose |
|---|---|
| `deploy/oci-update.sh` | OCI deploy script: `git pull` → `pip install` → `systemctl restart trading-galaxy` → CF zone cache purge |
| `deploy/deploy.ps1` | One-shot PowerShell deploy: pushes to GitHub, deploys backend via SSH, deploys frontend via `wrangler pages deploy` |
| `deploy/Caddyfile` | Caddy reverse proxy config — automatic Let's Encrypt TLS, security headers, rate limiting |
| `deploy/seed-bootstrap.sh` | First-boot script: downloads latest `kb_seed.sql` from GitHub Releases and loads it |

---

## `tests/`

23 test files. Key ones:

| File | Covers |
|---|---|
| `test_screenshot_upload.py` | `chat_vision()` unit tests · holdings normalisation · live API endpoint (21 pass) |
| `test_portfolio.py` | `build_portfolio_summary()` aggregation math, sector weights, edge cases |
| `test_full_stack.py` | Full HTTP integration tests against live server at `:5050` |
| `test_options_adapter.py` | Options atom generation, confidence capping for low-liquidity tickers |
| `test_user_store.py` | UK defaults, portfolio CRUD |
| `test_pattern_detector.py` | Pattern detection over synthetic KB atoms |

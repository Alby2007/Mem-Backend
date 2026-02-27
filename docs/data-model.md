# Data Model

## The Atom

Every piece of knowledge in the KB is an **atom** — a typed RDF-style triple augmented with epistemic metadata.

```
(subject, predicate, object, confidence, source, timestamp, metadata)
```

| Field | Type | Description |
|---|---|---|
| `subject` | `TEXT` | The entity being described. Stored lowercase. E.g. `nvda`, `us_macro` |
| `predicate` | `TEXT` | The relationship type. E.g. `signal_direction`, `regime_label` |
| `object` | `TEXT` | The value or target. E.g. `long`, `tight policy`, `253.99` |
| `confidence` | `REAL [0,1]` | Epistemic certainty. Decays over time per source half-life |
| `source` | `TEXT` | Provenance string. Prefix-matched for authority weight |
| `timestamp` | `TEXT` | ISO-8601 ingestion time |
| `metadata` | `TEXT` | JSON bag — `as_of`, `superseded_by`, `target_high`, `currency`, etc. |

**Uniqueness constraint:** `UNIQUE(subject, predicate, object)` — the same triple is never duplicated. When the same `(subject, predicate)` gets a new object value (e.g. price changes), a new row is inserted and the prior row is marked `superseded_by` in its metadata.

---

## SQL Schema

```sql
CREATE TABLE facts (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    subject   TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object    TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    source    TEXT,
    timestamp TEXT,
    metadata  TEXT,
    UNIQUE(subject, predicate, object)
);

CREATE INDEX idx_subject   ON facts(subject);
CREATE INDEX idx_predicate ON facts(predicate);
CREATE INDEX idx_object    ON facts(object);

-- FTS5 virtual table for full-text search
CREATE VIRTUAL TABLE facts_fts USING fts5(
    subject, predicate, object,
    content='facts', content_rowid='id'
);

-- Contradiction / conflict audit log
CREATE TABLE fact_conflicts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id_a  INTEGER,
    fact_id_b  INTEGER,
    conflict_type TEXT,
    detected_at   TEXT
);

-- Decay event log
CREATE TABLE decay_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id    INTEGER,
    old_confidence REAL,
    new_confidence REAL,
    decayed_at TEXT
);
```

---

## `signal_calibration` Table

Stores historical pattern outcome statistics used to show intern-facing hit rates. Populated by `analytics/historical_calibration.py` (backtest) and `POST /feedback` (live user outcomes).

```sql
CREATE TABLE signal_calibration (
    ticker               TEXT NOT NULL,
    pattern_type         TEXT NOT NULL,
    timeframe            TEXT NOT NULL DEFAULT '1d',
    market_regime        TEXT,              -- NULL = all regimes blended
    sample_size          INTEGER DEFAULT 0,
    hit_rate_t1          REAL,              -- fraction reaching T1
    hit_rate_t2          REAL,              -- fraction reaching T2
    hit_rate_t3          REAL,              -- fraction reaching T3
    stopped_out_rate     REAL,              -- fraction stopped out
    avg_time_to_target_hours REAL,
    calibration_confidence   REAL,          -- 0–1, based on sample size
    last_updated         TEXT,
    PRIMARY KEY (ticker, pattern_type, timeframe, market_regime)
);
```

**`calibration_confidence` thresholds:**

| Sample size | Label | Score |
|---|---|---|
| ≥ 100 | `established` | ≥ 0.60 |
| 30–99 | `moderate` | 0.35–0.59 |
| 10–29 | `low` | 0.15–0.34 |
| < 10 | `insufficient` | < 0.15 |

**Market regimes:** `risk_on_expansion` · `risk_off_contraction` · `stagflation` · `recovery` · `NULL` (all regimes blended)

**Launch state (seed `seed-20260227-0741`):** 2,346 rows · 378,910 samples · 1,366 `established` rows across 77 tickers.

---

## Predicate Vocabulary

Predicates are defined in `knowledge/kb_domain_schemas.py`. The active predicates used by the ingest pipeline are:

### Trading Instruments

| Predicate | Example value | Source prefix |
|---|---|---|
| `last_price` | `190.4` | `exchange_feed_*` |
| `signal_direction` | `long` / `short` / `neutral` / `near_high` / `near_low` / `mid_range` | `exchange_feed_*`, `broker_research_*`, `model_signal_*` |
| `signal_confidence` | `0.82` | `model_signal_*` |
| `price_target` | `253.99` | `broker_research_*` |
| `sector` | `technology` / `etf:financial` | `exchange_feed_*` |
| `market_cap_tier` | `mega_cap` / `large_cap` / `mid_cap` / `small_cap` | `exchange_feed_*` |
| `volatility_regime` | `high_volatility` / `medium_volatility` / `low_volatility` | `exchange_feed_*` |
| `earnings_quality` | `next_earnings: 2026-02-25` | `earnings_*` |
| `catalyst` | `sec 8-k (2026-01-23): 8-k` | `regulatory_filing_*` |
| `risk_factor` | `insider_transaction (2026-01-15): form 4` | `regulatory_filing_*` |
| `correlation_to` | `0.87` | `model_signal_*` |
| `liquidity_profile` | `high_liquidity` | `exchange_feed_*` |
| `time_horizon` | `swing` / `position` / `intraday` | `model_signal_*` |

### Market Theses

| Predicate | Example value |
|---|---|
| `premise` | `Fed pivot thesis: rate cuts expected H2 2024` |
| `supporting_evidence` | `CPI trending down for 3 consecutive months` |
| `contradicting_evidence` | `Core PCE remains sticky above 3%` |
| `entry_condition` | `Break above 200 DMA with volume confirmation` |
| `exit_condition` | `Close below 50 DMA` |
| `invalidation_condition` | `CPI print > 4% reverses thesis` |
| `risk_reward_ratio` | `3.2` |
| `position_sizing_note` | `max 2% portfolio risk, scale in 3 tranches` |
| `invalidated_by` | `id:thesis_fed_pivot_2024_v1` |

### Macro Regime

| Predicate | Example value | Source |
|---|---|---|
| `regime_label` | `tight policy` | `macro_data_fred` |
| `dominant_driver` | `fed_funds_rate: 3.64%` | `macro_data_fred` |
| `central_bank_stance` | `neutral_to_restrictive` | `macro_data_fred` |
| `inflation_environment` | `cpi index: 326.6` | `macro_data_fred` |
| `growth_environment` | `moderate_growth: gdp +1.4% annualized` | `macro_data_fred` |
| `risk_on_off` | `risk_off` | `model_signal_*` |
| `asset_class_bias` | `equities_underweight` | `model_signal_*` |
| `sector_rotation` | `defensive_rotation` | `model_signal_*` |
| `regime_history_YYYY_MM` | `risk_off_contraction` | `macro_data_regime_history` |

### Regime-Conditional Performance (per equity ticker)

Written by `analytics/regime_history.py` via `POST /calibrate/regime-history`.

| Predicate | Example value | Notes |
|---|---|---|
| `return_in_risk_on_expansion` | `3.62` | Avg monthly return (%) in that regime |
| `return_in_risk_off_contraction` | `5.54` | |
| `return_in_stagflation` | `2.08` | |
| `return_in_recovery` | `0.3` | |
| `regime_hit_rate_risk_on_expansion` | `83.3` | % months ticker was up in regime |
| `regime_hit_rate_risk_off_contraction` | `66.7` | |
| `best_regime` | `risk_off_contraction (+5.5%/mo)` | Best performing regime |
| `worst_regime` | `recovery (+0.3%/mo)` | Worst performing regime |

### Research / News

| Predicate | Example value | Source |
|---|---|---|
| `key_finding` | `Fed's Goolsbee calls for a hold on cuts` | `news_wire_*` |
| `rating` | `overweight` | `broker_research_*` |
| `compared_to_consensus` | `above_consensus` | `broker_research_*` |
| `publisher` | `Goldman Sachs` | `broker_research_*` |
| `analyst` | `David Kostin` | `broker_research_*` |

---

## Source Naming Convention

Sources are prefix-matched against the authority table. **Always use the correct prefix.**

```
exchange_feed_<exchange>_<symbol>     e.g. exchange_feed_yahoo_nvda
regulatory_filing_<id>                e.g. regulatory_filing_sec
earnings_<ticker>_<quarter>           e.g. earnings_nvda_upcoming
broker_research_<firm>_<date>         e.g. broker_research_yahoo_consensus_nvda
macro_data_<source>                   e.g. macro_data_fred
model_signal_<model_name>             e.g. model_signal_momentum_v1
technical_<indicator>_<symbol>        e.g. technical_rsi_aapl
news_wire_<outlet>                    e.g. news_wire_cnbc
alt_data_<provider>                   e.g. alt_data_quandl_sentiment
social_signal_<platform>              e.g. social_signal_twitter
curated_<analyst_id>                  e.g. curated_internal_macro_desk
```

---

## Confidence Guidelines

| Value | Meaning |
|---|---|
| `1.0` | Directly observed, unambiguous (e.g. price from exchange) |
| `0.95` | Exchange feed with minor latency |
| `0.9` | Strongly supported by high-authority source |
| `0.85` | Well-supported, minor interpretation required |
| `0.80` | Macro data or well-sourced broker research |
| `0.75` | Analyst consensus — aggregated across many sources |
| `0.70` | Model output or derived, reasonable confidence |
| `0.65` | Price-vs-target derived signal |
| `0.60` | News/RSS — headline only, no verification |
| `0.55` | Low-signal or lightly sourced |
| `0.5` | Uncertain, placeholder |
| `0.3` | Speculative, conflicting evidence, or noisy source |

---

## Authority Weights by Source Prefix

Used by `knowledge/authority.py` for re-ranking retrieved atoms.

| Source prefix | Authority weight | Half-life |
|---|---|---|
| `exchange_feed_*` | 1.0 | ~10 min |
| `regulatory_filing_*` | 0.95 | ~1 year |
| `curated_*` | 0.90 | ~6 months |
| `earnings_*` | 0.85 | ~30 days |
| `broker_research_*` | 0.80 | ~21 days |
| `macro_data_*` | 0.80 | ~60 days |
| `model_signal_*` | 0.70 | ~12 hours |
| `technical_*` | 0.65 | ~6 hours |
| `news_wire_*` | 0.60 | ~1 day |
| `alt_data_*` | 0.55 | ~3 days |
| `social_signal_*` | 0.35 | ~12 hours |

---

## Metadata Convention

The `metadata` JSON field carries supplementary fields that don't fit the triple model:

```jsonc
// UK equity price atom
{ "as_of": "2026-02-24T15:04:17Z", "currency": "GBP", "quote_type": "EQUITY", "exchange": "LSE" }

// US equity price atom
{ "as_of": "2026-02-24T15:04:17Z", "currency": "USD", "quote_type": "EQUITY" }

// Superseded atom (stale)
{ "superseded_by": 553, "superseded_at": "2026-02-24T15:04:36Z" }

// Analyst consensus
{ "target_high": 352.0, "target_low": 140.0, "num_analysts": 57 }

// ETF category
{ "etf_category": "Financial" }

// Beta-derived volatility
{ "beta": 2.314, "etf": false }

// 52-week signal
{ "pct_from_52w_high": -10.7, "as_of": "2026-02-24T15:04:17Z" }

// SEC filing
{ "form_type": "8-K", "filing_date": "2026-01-23", "accession": "0001045810-26-000003" }

// Earnings date
{ "earnings_date": "2026-02-25" }

// FRED macro
{ "value": 3.64, "series_id": "FEDFUNDS", "units": "percent" }
```

---

## Subjects in the KB

> **This is a UK-first system.** The primary watchlist is FTSE 100 heavyweights with `.L` suffix tickers. US tickers are included as global macro anchors and cross-asset context. The frontend should treat `.L` tickers as the primary equity universe and US names as secondary.

---

### UK / LSE equities — `.L` suffix convention

All stored **lowercase** with the `.l` suffix preserved: e.g. `shel.l`, `azn.l`, `hsba.l`.

**FTSE 100 heavyweights (default watchlist):**

| Ticker | Company | Sector |
|---|---|---|
| `shel.l` | Shell | Energy |
| `azn.l` | AstraZeneca | Pharmaceuticals |
| `hsba.l` | HSBC | Banks |
| `ulvr.l` | Unilever | Consumer Staples |
| `bp.l` | BP | Energy |
| `gsk.l` | GSK | Pharmaceuticals |
| `rio.l` | Rio Tinto | Mining |
| `bats.l` | BAT | Consumer Staples |
| `vod.l` | Vodafone | Telecoms |
| `lloy.l` | Lloyds Banking | Banks |
| `barc.l` | Barclays | Banks |
| `nwg.l` | NatWest | Banks |
| `lseg.l` | London Stock Exchange Group | Financials |
| `rel.l` | RELX | Professional Services |
| `ng.l` | National Grid | Utilities |
| `ba.l` | BAE Systems | Defence |
| `qq.l` | Qinetiq | Defence |
| `rr.l` | Rolls-Royce | Aerospace / Defence |
| `tsco.l` | Tesco | Retail |
| `mks.l` | Marks & Spencer | Retail |
| `pson.l` | Pearson | Education / Media |
| `psn.l` | Persimmon | Housebuilders |

**Dynamic watchlist:** Additional `.L` tickers are added via the Discovery Pipeline when `coverage_count ≥ 3`. Query `GET /universe/coverage` for the current full list.

**`.L` ticker rules for frontend engineers:**
- Always display with uppercase suffix: `SHEL.L`, `AZN.L`, etc.
- The KB stores them lowercase internally: `shel.l`, `azn.l`
- `GET /context/SHEL.L` and `GET /context/shel.l` both work — the API normalises to lowercase
- FX pairs use `=X` suffix: `gbpusd=x`, `eurgbp=x`
- FTSE indices use `^` prefix: `^ftse` (FTSE 100), `^ftmc` (FTSE 250)

---

### US equities (global macro anchors)

Stored lowercase without suffix. Included as cross-asset context for global macro regime classification and portfolio correlation analysis.

`aapl`, `msft`, `googl`, `amzn`, `nvda`, `meta`, `tsla`, `jpm`, `v`, `unh`, `avgo`, `crm`, `adbe`, `amd`, `intc`, `qcom`, `mu`, `now`, `orcl`, `ibm`, `bac`, `gs`, `ms`, `wfc`, `axp`, `xom`, `cvx`, `cop`, `slb`, `mro`, `jnj`, `lly`, `abbv`, `mrk`, `pfe`, `hd`, `low`, `nke`, `sbux`, `mcd`, `pg`, `ko`, `pep`, `wmt`, `cost`, `amgn`, `brk-b`

---

### ETFs and macro proxies

| Subject | Type | Role |
|---|---|---|
| `spy`, `qqq`, `iwm`, `dia`, `vti` | US broad ETFs | Global risk appetite proxy |
| `xlf`, `xle`, `xlk`, `xlv`, `xli`, `xlc`, `xly`, `xlp`, `xlu`, `xlre`, `xlb` | US sector ETFs | Sector rotation signal |
| `tlt`, `hyg`, `lqd` | Bond ETFs | Rates / credit regime |
| `gld`, `slv` | Commodity ETFs | Inflation hedge |
| `uup` | USD index ETF | Dollar strength |
| `^ftse` | FTSE 100 index | UK equity market level |
| `^ftmc` | FTSE 250 index | UK mid-cap / domestic economy |
| `^gspc` | S&P 500 index | US equity market level |
| `^vix` | VIX index | Volatility / fear gauge |
| `gbpusd=x` | GBP/USD FX pair | Sterling strength |
| `eurgbp=x` | EUR/GBP FX pair | UK-EU trade signal |

---

### Macro subjects

| Subject | Populated by | Content |
|---|---|---|
| `us_macro` | `FREDAdapter` | Fed funds rate, CPI, GDP, regime label |
| `us_labor` | `FREDAdapter` | Unemployment rate |
| `us_yields` | `FREDAdapter` | 2y/10y treasury yields, yield curve spread |
| `us_credit` | `FREDAdapter` | HY spread |
| `uk_macro` | `BoEAdapter` | BoE base rate, UK CPI, GDP, regime label |
| `uk_yields` | `BoEAdapter` | UK gilt 10y/2y yields, yield environment |
| `global_macro_regime` | `RegimeHistoryClassifier` | Monthly regime history atoms (`regime_history_YYYY_MM`) |
| `financial_news` | `RSSAdapter` | Headline `key_finding` atoms with ticker mentions |

`global_macro_regime` holds 52 months of classified macro history at launch.

---

### UK-specific predicates

These predicates are unique to UK/LSE equities and will not appear on US subjects:

| Predicate | Source | Example value | Notes |
|---|---|---|---|
| `fca_short_interest` | `FCAShortInterestAdapter` | `3.45% (Bridgewater Associates)` | FCA-disclosed short positions ≥ 0.5% |
| `institutional_flow` | `LSEFlowAdapter` | `accumulating` / `distributing` / `neutral` | BTVR + VWPT + PVD microstructure proxies |
| `block_volume_ratio` | `LSEFlowAdapter` | `2.4` | Today's volume / 20-day average |
| `flow_conviction` | `LSEFlowAdapter` | `high` / `moderate` / `low` | Composite microstructure signal strength |
| `price_range_compression` | `LSEFlowAdapter` | `compressed` / `normal` | Wyckoff accumulation proxy |
| `uk_options_regime` | `OptionsAdapter` | `compressed` / `elevated_vol` | From LSE-listed options chains |
| `boe_base_rate` | `BoEAdapter` | `5.25%` | On `uk_macro` subject |
| `uk_cpi_yoy` | `BoEAdapter` | `CPI YoY: 4.0%` | On `uk_macro` subject |
| `uk_gilt_10y` | `BoEAdapter` | `4.35%` | On `uk_yields` subject |

**Source prefixes for UK data:**

| Prefix | Authority | Source |
|---|---|---|
| `macro_data_boe` | 0.80 | Bank of England Statistical API |
| `regulatory_filing_fca` | 0.90 | FCA short position disclosures |
| `alt_data_lse_flow` | 0.55 | LSE microstructure flow signals |

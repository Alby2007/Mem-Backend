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
// Price atom
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

### Equity tickers
All stored lowercase: `aapl`, `msft`, `googl`, `amzn`, `nvda`, `meta`, `tsla`, `jpm`, `v`, `unh`, `avgo`, `crm`, `adbe`, `amd`, `intc`, `qcom`, `mu`, `now`, `orcl`, `ibm`, `bac`, `gs`, `ms`, `wfc`, `axp`, `xom`, `cvx`, `cop`, `slb`, `mro`, `jnj`, `lly`, `abbv`, `mrk`, `pfe`, `hd`, `low`, `nke`, `sbux`, `mcd`, `pg`, `ko`, `pep`, `wmt`, `cost`, `amgn`, `brkb`

### ETFs
`spy`, `qqq`, `iwm`, `xlf`, `xle`, `xlk`, `xlv`, `xli`, `xlc`, `xly`, `xlp`, `xlu`, `xlre`, `xlb`, `tlt`, `hyg`, `lqd`, `gld`, `slv`, `uup`

### Macro subjects
`us_macro`, `us_labor`, `us_yields`, `us_credit`, `global_macro_regime`, `financial_news`

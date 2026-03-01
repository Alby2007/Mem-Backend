# Ingest Pipeline

## Overview

The ingest pipeline pulls live market data from four free external sources and converts it into typed knowledge atoms stored in the KB. All adapters run automatically on background threads managed by `IngestScheduler`.

```
External source
    → adapter.fetch()       # source-specific API call
    → adapter.transform()   # normalise → List[RawAtom]
    → atom.validate()       # drop malformed atoms
    → kg.add_fact()         # UPSERT into facts table
    → AdapterStatus updated
```

---

## Interfaces

### `RawAtom`

The universal atom schema shared by all adapters:

```python
@dataclass
class RawAtom:
    subject:    str          # entity (e.g. 'AAPL', 'us_macro')
    predicate:  str          # relationship (e.g. 'signal_direction')
    object:     str          # value (e.g. 'long', '253.99')
    confidence: float = 0.5  # [0.0, 1.0]
    source:     str   = 'unverified_ingest'
    metadata:   dict  = {}
```

### `BaseIngestAdapter`

All adapters subclass this. Required override: `fetch() → List[RawAtom]`.

```python
class MyAdapter(BaseIngestAdapter):
    def __init__(self):
        super().__init__(name='my_adapter')

    def fetch(self) -> List[RawAtom]:
        # pull from source, return atoms
        ...
```

The base class handles:
- `transform()` — post-processing hook (override optionally)
- `run()` — fetch → transform → validate → drop invalid
- `push(atoms, kg)` — writes to `KnowledgeGraph`
- `run_and_push(kg)` — convenience wrapper (called by scheduler)

---

## IngestScheduler

`IngestScheduler` fires each adapter on its own independent `threading.Timer` loop.

```python
scheduler = IngestScheduler(kg)
scheduler.register(YFinanceAdapter(), interval_sec=300)
scheduler.register(RSSAdapter(),      interval_sec=900)
scheduler.register(EDGARAdapter(),    interval_sec=21600)
scheduler.register(FREDAdapter(),     interval_sec=86400)
scheduler.start()   # non-blocking, all adapters fire immediately then re-arm
```

- Failed adapter runs are caught, logged, and recorded in `AdapterStatus`
- Other adapters are never affected by one adapter's failure
- The timer re-arms regardless of success or failure
- Health queryable at any time via `GET /ingest/status`

---

### `ingest/historical_adapter.py` (one-shot)

Downloads **5 years** of daily OHLCV for all watchlist tickers in a single bulk call and emits interpretable summary atoms. Not on a schedule — triggered via `POST /ingest/historical` or `POST /ingest/run-all`.

**Atoms produced:**

| Predicate | Description | Confidence |
|---|---|---|
| `return_1w/1m/3m/6m/1y` | % return over rolling windows | 0.90 |
| `return_3y` | % return over 3 years (756 trading days) | 0.85 |
| `return_5y` | % return over 5 years (1260 trading days) | 0.85 |
| `volatility_30d/90d` | Annualised realised volatility | 0.85 |
| `volatility_5y` | Annualised realised vol over full 5yr window | 0.80 |
| `max_drawdown_5y` | Peak-to-trough max drawdown over 5 years | 0.85 |
| `drawdown_from_high` | % below 52-week high | 0.90 |
| `avg_volume_30d` | Average daily volume, 30 days | 0.90 |
| `price_52w_high/low` | 52-week range reference prices | 0.95 |
| `price_3y_ago` | Reference anchor price 3 years ago | 0.85 |
| `return_vs_spy_1m/3m` | Alpha vs SPY over 1/3 months | 0.80 |

All atoms are `upsert=True` — safe to re-run.

---

## Adapter: YFinanceAdapter

**Source:** Yahoo Finance via `yfinance` library  
**Interval:** 5 minutes  
**Requires:** None (free, no API key)

### Watchlist (~50 tickers)

| Category | Tickers |
|---|---|
| Mega-cap tech | AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, AVGO |
| Financials | JPM, BAC, GS, MS, WFC, AXP, V |
| Healthcare | UNH, JNJ, LLY, ABBV, MRK, PFE |
| Consumer | HD, LOW, NKE, SBUX, MCD, PG, KO, PEP, WMT, COST |
| Tech/semis | CRM, ADBE, AMD, INTC, QCOM, MU, NOW, ORCL, IBM |
| Energy | XOM, CVX, COP, SLB, MRO |
| Other | AMGN, BRK-B |
| Sector ETFs | XLF, XLE, XLK, XLV, XLI |
| Broad ETFs | SPY, QQQ, IWM |
| Rates/credit | TLT, HYG, LQD |
| Macro proxies | GLD, SLV, UUP |

### Batching & Rate Limiting

Tickers are processed in batches of 10 with a 1.5-second delay between batches to avoid Yahoo Finance rate limiting. Exponential backoff (2s / 4s / 8s, up to 3 retries) is applied per-ticker on 429 or timeout errors.

### Equity Path

For `quoteType` = `EQUITY`, `STOCK`:

```
atoms produced per ticker:
  last_price          confidence=0.95   source=exchange_feed_yahoo_<symbol>
  sector              confidence=0.90
  market_cap_tier     confidence=0.85   (mega/large/mid/small/micro)
  volatility_regime   confidence=0.80   (derived from beta)
  price_target        confidence=0.75   (analyst consensus mean)
  signal_direction    confidence=0.65   (long if price < target, else neutral/short)
  earnings_quality    confidence=0.85   (next earnings date if available)
```

**`signal_direction` derivation:**
```
pct_upside = (target - price) / price × 100
if pct_upside > 10%  → 'long'
if pct_upside < -5%  → 'short'
else                 → 'neutral'
```

**`volatility_regime` from beta:**
```
beta > 1.5  → high_volatility
beta > 0.8  → medium_volatility
else        → low_volatility
```

### ETF Path

For `quoteType` in `{ETF, MUTUALFUND, INDEX, FUTURE, CRYPTOCURRENCY}`:

```
atoms produced:
  last_price          confidence=0.95
  sector              confidence=0.90   (from yfinance category, or _ETF_CATEGORY_FALLBACK)
  market_cap_tier     confidence=0.90   (from totalAssets AUM)
  volatility_regime   confidence=0.75   (from beta3Year or beta)
  signal_direction    confidence=0.60   (52-week momentum: near_high / near_low / mid_range)
```

**`signal_direction` for ETFs (52w momentum):**
```
pct_from_52w_high = (price - 52w_high) / 52w_high × 100
if pct_from_52w_high > -5%   → 'near_high'
if pct_from_52w_high < -20%  → 'near_low'
else                         → 'mid_range'
```

**`_ETF_CATEGORY_FALLBACK` map** (used when yfinance returns empty `category`):

| Ticker | Fallback category |
|---|---|
| XLF | `financials_sector` |
| XLE | `energy_sector` |
| XLK | `technology_sector` |
| XLV | `healthcare_sector` |
| XLI | `industrials_sector` |
| SPY | `broad_market_us_large_cap` |
| QQQ | `broad_market_nasdaq100` |
| TLT | `long_government_bonds` |
| HYG | `high_yield_credit` |
| GLD | `gold_commodity_inflation_hedge` |
| SLV | `silver_commodity_inflation_hedge` |
| UUP | `us_dollar_index` |
| *(+ 13 more)* | *(see `yfinance_adapter.py`)* |

---

## Adapter: FREDAdapter

**Source:** Federal Reserve Economic Data (FRED) via `fredapi`  
**Interval:** 24 hours  
**Requires:** `FRED_API_KEY` environment variable (free at fred.stlouisfed.org)

### Series Fetched

| FRED Series ID | Subject | Predicate | Example value |
|---|---|---|---|
| `FEDFUNDS` | `us_macro` | `dominant_driver` | `fed_funds_rate: 3.64%` |
| `CPIAUCSL` | `us_macro` | `inflation_environment` | `cpi index: 326.6` |
| `GDP` | `us_macro` | `growth_environment` | `moderate_growth: gdp +1.4% annualized` |
| `UNRATE` | `us_labor` | `dominant_driver` | `unemployment: 4.3%` |
| `GS10` | `us_yields` | `dominant_driver` | `10y_treasury: 4.21%` |
| `GS2` | `us_yields` | `dominant_driver` | `2y_treasury: 3.61%` |
| `T10Y2Y` | `us_yields` | `risk_factor` | `yield_curve_normal: 10y-2y spread +60bps` |
| `BAMLH0A0HYM2` | `us_credit` | `risk_factor` | `hy_spread: 310bps (elevated)` |

### Derived Atoms

After fetching raw series, the adapter derives:

- **`central_bank_stance`**: `restrictive` / `neutral_to_restrictive` / `neutral` / `accommodative` based on fed funds rate level
- **`regime_label`**: `tight policy` / `easing cycle` / `neutral policy` based on stance + growth

All FRED atoms use `source='macro_data_fred'` (authority 0.80, half-life ~60 days).

---

## Adapter: EDGARAdapter

**Source:** SEC EDGAR full-text search API (public, no key required)  
**Interval:** 6 hours  
**Requires:** `EDGAR_USER_AGENT` env var (defaults to `trading-galaxy-kb research@example.com`)

### Filing Types → Atom Mapping

| Form type | Predicate | Confidence | Meaning |
|---|---|---|---|
| `8-K` | `catalyst` | 0.85 | Material event (earnings, M&A, guidance) |
| `10-K` | `catalyst` | 0.85 | Annual report filed |
| `10-Q` | `catalyst` | 0.85 | Quarterly report filed |
| `S-1` | `catalyst` | 0.85 | IPO/registration filing |
| `SC 13G` | `catalyst` | 0.85 | Institutional holder disclosure |
| `Form 4` | `risk_factor` | 0.80 | Insider buy/sell transaction |

All EDGAR atoms use `source='regulatory_filing_sec'` (authority 0.95, half-life ~1 year).

### Object format
```
catalyst    → "sec 8-k (2026-01-23): <title>"
risk_factor → "insider_transaction (2026-01-15): form 4"
```

---

## Adapter: RSSAdapter

**Source:** Financial news RSS feeds via `feedparser`  
**Interval:** 15 minutes  
**Requires:** None

### Active Feeds

| Feed name | URL | Status |
|---|---|---|
| `ft_home` | `https://www.ft.com/rss/home` | ✅ Active |
| `investing_com` | `https://www.investing.com/rss/news.rss` | ✅ Active |
| `bbc_business` | `http://feeds.bbci.co.uk/news/business/rss.xml` | ✅ Active |
| `cnbc_finance` | `https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664` | ✅ Active |
| `marketwatch` | `http://feeds.marketwatch.com/marketwatch/topstories/` | ✅ Active |

> **Note:** The Yahoo Finance RSS feed (`feeds.finance.yahoo.com`) was removed — it returns malformed XML.

### Atom format

```
subject:    'financial_news'
predicate:  'key_finding'
object:     '<headline text>'
confidence: 0.55
source:     'news_wire_<feed_name>'
metadata:   { 'url': '...', 'published': '...', 'ticker_mentions': ['AAPL', 'NVDA'] }
```

Ticker mentions are extracted from headline text using the same uppercase-sequence pattern as the retrieval engine, filtered against a stopword list.

---

## Adapter: FCAShortInterestAdapter

**Source:** FCA short position disclosure XLSX (public)
**URL:** `https://www.fca.org.uk/publication/data/short-positions-daily-update.xlsx`
**Interval:** 24 hours  
**Requires:** None

Parses the daily FCA short-selling disclosure file to extract significant short positions (≥ 0.5% of issued share capital) for UK-listed companies.

**Atoms produced:**
```
{TICKER} | fca_short_interest | "3.45% (Bridgewater Associates)"
```

Uses ISIN-to-ticker and name-to-ticker fallback maps to resolve disclosures to yfinance ticker symbols.

---

## Discovery Pipeline

`ingest/discovery_pipeline.py` — Universal Discovery Pipeline

Runs on schedule or via `POST /discover/run`. Discovers new tickers from multiple signals and promotes them into the active watchlist when coverage exceeds threshold.

**Discovery stages:**
1. FCA short interest XLSX — surfaces heavily shorted UK names
2. RSS headline ticker extraction — tickers mentioned ≥ 3× in recent news
3. Coverage scoring — counts how many discovery sources surface the same ticker

**Promotion logic:**
- `coverage_count ≥ 3` → promoted to `added_to_ingest=1` (enters yfinance watchlist)
- Network effect: each new user who adds a ticker increments `coverage_count`

**DB tables:** `discovery_log` · `dynamic_watchlist`

---

## Seed Sync Client

`ingest/seed_sync.py` — runs hourly as a background thread independent of the ingest scheduler.

Polls `Alby2007/Mem-Backend` GitHub Releases. If the latest `seed-YYYYMMDD-HHMM` tag is newer than the locally stored `kb_meta.seed_tag`, downloads `kb_seed.sql` and applies shared tables only.

**Hard-coded allowlist** (`_ALLOWED_TABLES`): `facts`, `fact_conflicts`, `causal_edges`, `pattern_signals`, `signal_calibration`, and governance tables. **Never touches any `user_*` table.**

---

## Adapter: BoEAdapter

**Source:** Bank of England Statistical Interactive Dataset (public, no key)
**Interval:** 24 hours
**Requires:** None

Pulls UK macro indicators from the BoE API and derives regime atoms for the UK market context.

**Atoms produced:**

| Subject | Predicate | Example value |
|---|---|---|
| `uk_macro` | `boe_base_rate` | `5.25%` |
| `uk_macro` | `central_bank_stance` | `restrictive` / `neutral` / `accommodative` |
| `uk_macro` | `uk_cpi_yoy` | `CPI YoY: 4.0%` |
| `uk_macro` | `inflation_environment` | `high_inflation` / `normalising` / `low_inflation` |
| `uk_macro` | `uk_gdp_growth` | `GDP growth: 0.1%` |
| `uk_macro` | `growth_environment` | `contraction` / `stagnation` / `moderate` / `strong` |
| `uk_macro` | `uk_unemployment` | `4.2%` |
| `uk_macro` | `regime_label` | `restrictive_stagflation` |
| `uk_yields` | `uk_gilt_10y` | `4.35%` |
| `uk_yields` | `yield_environment` | `elevated` |

Source prefix: `macro_data_boe` (authority 0.80, half-life 60d)

---

## Adapter: OptionsAdapter

**Source:** Yahoo Finance options chains via `yfinance`
**Interval:** 15 minutes
**Requires:** None

Fetches options chains for liquid FTSE names and computes options-regime atoms. Low-liquidity FTSE names have confidence caps enforced via `_LOW_OPTIONS_LIQUIDITY` frozenset.

**Atoms produced:**

| Predicate | Description | Confidence |
|---|---|---|
| `iv_rank` | 30-day IV percentile of the 52-week range (0–100) | 0.80 (0.40 for thin names) |
| `put_call_ratio` | Sum put OI / sum call OI, front two expirations | 0.80 |
| `options_regime` | `compressed` / `normal` / `elevated_vol` | 0.80 |
| `smart_money_signal` | `call_sweep` / `put_sweep` / `none` | 0.75 (0.35 for thin names) |
| `iv_skew_ratio` | OTM put IV / ATM IV | 0.75 |
| `iv_skew_25d` | OTM put IV − OTM call IV (5% wings) | 0.75 |
| `skew_regime` | `normal` / `elevated` / `spike` | 0.75 |
| `spy_skew_ratio` | Market-level SPY skew (tail risk proxy) | 0.80 |
| `tail_risk` | `normal` / `moderate` / `elevated` / `extreme` | 0.80 |

---

## Adapter: SignalEnrichmentAdapter

**Source:** KB atoms (cross-referencing historical, price, and options data already in the KB)
**Interval:** 30 minutes
**Requires:** None (reads from KB, no external API)

Derives higher-level signal atoms by combining raw data atoms into actionable signals.

**Atoms produced:**

| Predicate | Description |
|---|---|
| `price_regime` | `uptrend` / `downtrend` / `range_bound` (from price + MA atoms) |
| `volume_trend` | `accumulation` / `distribution` / `neutral` |
| `momentum_signal` | `strong_bull` / `bull` / `neutral` / `bear` / `strong_bear` |
| `risk_reward_ratio` | Computed from price target, entry, and stop-loss atoms |
| `thesis_risk_level` | `low` / `moderate` / `high` / `extreme` |
| `conviction_tier` | `tier_1` / `tier_2` / `tier_3` (multi-factor conviction score) |
| `position_size_pct` | Suggested position size % of portfolio |
| `upside_pct` | % upside to price target |

---

## Adapter: LLMExtractionAdapter

**Source:** `extraction_queue` table (fed by `RSSAdapter`)
**Interval:** 60 minutes
**Requires:** Ollama running with `OLLAMA_EXTRACTION_MODEL` (default `phi3`)

Runs LLM extraction over queued RSS headlines to produce structured signal atoms. Falls back gracefully if Ollama is unavailable.

**Atoms produced:** Structured entities and signals extracted from news text — predicate and object depend on the headline content. Examples: `catalyst`, `risk_factor`, `key_finding`, `signal_direction`.

**Source prefix:** `llm_extracted_news_*` (authority 0.65, half-life 12h)

---

## Adapter: EDGARRealtimeAdapter

**Source:** SEC EDGAR full-text search API (public, no key)
**Interval:** 30 minutes
**Requires:** `EDGAR_USER_AGENT` env var

Polls for 8-K filings specifically (material events) with a 30-minute cadence. Deduplicates via `edgar_realtime_seen` table — each filing accession number is stored so re-runs never produce duplicate atoms.

**Atoms produced:** Same format as `EDGARAdapter` — `catalyst | "sec 8-k (date): title"` at confidence 0.85.

**Difference from EDGARAdapter:** EDGARRealtime polls only 8-K (immediate material events) every 30 min; the base EDGAR adapter polls all form types every 6 hours.

---

## Adapter: LSEFlowAdapter

**Source:** Yahoo Finance intraday OHLCV (`.L` suffix tickers)
**Interval:** 60 minutes
**Requires:** None

Derives institutional order-flow signals for LSE-listed equities using three microstructure proxies: block-trade volume ratio (BTVR), volume-weighted price trend (VWPT), and price-volume divergence (PVD, the Wyckoff accumulation signature).

**Atoms produced:**

| Predicate | Example value | Confidence |
|---|---|---|
| `institutional_flow` | `accumulating` / `distributing` / `neutral` | 0.55 |
| `block_volume_ratio` | `2.4` (today's vol / 20d avg) | 0.55 |
| `flow_conviction` | `high` / `moderate` / `low` | 0.55 |
| `volume_trend_5d` | `rising` / `falling` / `flat` | 0.55 |
| `price_range_compression` | `compressed` / `normal` / `expanded` | 0.55 |

Source prefix: `alt_data_lse_flow` (authority 0.55, half-life 3d)

---

## Adapter: EarningsCalendarAdapter

**Source:** KB atoms (reads `next_earnings_date` atoms written by `YFinanceAdapter`) + yfinance options chains for implied move
**Interval:** 60 minutes
**Requires:** None

Reads earnings dates already in the KB and enriches them with proximity risk flags and implied move estimates.

**Atoms produced:**

| Predicate | Example value | Notes |
|---|---|---|
| `earnings_date` | `2026-04-30` | YYYY-MM-DD |
| `days_to_earnings` | `3` | Integer days |
| `earnings_risk` | `elevated` / `moderate` / `low` | Based on proximity |
| `earnings_implied_move` | `±4.2%` | From ATM straddle; falls back to `volatility_30d × sqrt(1/252)` for thin markets |
| `pre_earnings_flag` | `within_48h` / `within_7d` / `clear` | Used by tip formatter to warn on position sizing |

Source prefix: `earnings_calendar_{ticker}` (authority 0.85, half-life 7d)

---

## Adapter: PatternAdapter

**Source:** KB atoms (reads OHLCV-derived atoms and signal atoms)
**Interval:** 60 minutes
**Requires:** None (reads from KB)

Detects multi-factor chart and signal patterns over rolling windows. Writes `PatternSignal` rows to the `pattern_signals` table (not the `facts` table).

**Pattern types detected:** conviction patterns, momentum patterns, composite multi-timeframe patterns.

**Endpoint:** Results surfaced via `GET /patterns/live` and `GET /patterns/<id>`.

---

## Full Adapter Stack

| Adapter | Interval | Data source | Key atoms |
|---|---|---|---|
| `YFinanceAdapter` | 5 min | Yahoo Finance | `last_price`, `signal_direction`, `price_target`, `sector` |
| `OptionsAdapter` | 15 min | Yahoo Finance options | `iv_rank`, `put_call_ratio`, `tail_risk` |
| `RSSAdapter` | 15 min | 5 financial RSS feeds | `key_finding` (headlines) |
| `SignalEnrichmentAdapter` | 30 min | KB cross-reference | `conviction_tier`, `momentum_signal`, `position_size_pct` |
| `EDGARRealtimeAdapter` | 30 min | SEC EDGAR 8-K | `catalyst` (real-time filings) |
| `PatternAdapter` | 60 min | KB atoms | `pattern_signals` table |
| `LLMExtractionAdapter` | 60 min | extraction_queue | Structured LLM-extracted signals |
| `LSEFlowAdapter` | 60 min | yfinance intraday | `institutional_flow`, `block_volume_ratio` |
| `EarningsCalendarAdapter` | 60 min | KB + yfinance options | `earnings_implied_move`, `pre_earnings_flag` |
| `EDGARAdapter` | 6 hours | SEC EDGAR all forms | `catalyst`, `risk_factor` (Form 4 insider) |
| `BoEAdapter` | 24 hours | BoE Statistical API | `boe_base_rate`, UK `regime_label` |
| `FREDAdapter` | 24 hours | St. Louis Fed FRED | US `regime_label`, `central_bank_stance` |
| `FCAShortInterestAdapter` | 24 hours | FCA XLSX | `fca_short_interest` |
| `GDELTAdapter` | 12 hours | GDELT 2.0 GKG Doc API (tonechart) | `gdelt_tension` (country pair tone scores) |
| `UCDPAdapter` | 12 hours | GDELT artlist proxy (UCDP API requires auth) | `ucdp_conflict` (country conflict intensity) |
| `HistoricalBackfillAdapter` | On-demand | yfinance 5yr daily | `return_3y/5y`, `max_drawdown_5y`, `volatility_5y` |

`SeedSyncClient` runs hourly (outside the scheduler) to sync the shared KB seed from GitHub Releases.

---

## Adding a New Adapter

1. Create `ingest/my_adapter.py` subclassing `BaseIngestAdapter`
2. Implement `fetch() → List[RawAtom]` using the correct source prefix and confidence values from `ingest/base.py`
3. Add a graceful import to `ingest/__init__.py`
4. Register it in `api.py`:
   ```python
   _ingest_scheduler.register(MyAdapter(), interval_sec=3600)
   ```

**Source prefix reference:** See `ingest/base.py` — `SOURCE NAMING CONVENTION` section.  
**Confidence reference:** See `ingest/base.py` — `CONFIDENCE GUIDELINES` section.  
**Predicate reference:** See `knowledge/kb_domain_schemas.py`.

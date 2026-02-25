# Trading Galaxy — Trading Copilot Knowledge Base

A persistent, epistemically-governed knowledge base for trading intelligence.
Provides signal storage, contradiction detection, confidence decay, and smart
retrieval — all zero-LLM, pure Python, sub-2ms per operation.

> **Ingest team:** see [CONTRIBUTING.md](CONTRIBUTING.md) for the full quickstart, adapter contract, predicate vocabulary, and source naming guide.

---

## Architecture

```
trading-galaxy/
├── knowledge/                   # Core KB engine (do not modify without review)
│   ├── graph.py                 # TradingKnowledgeGraph — RDF triple store (SQLite WAL)
│   ├── authority.py             # Source authority weights (trust hierarchy)
│   ├── decay.py                 # Confidence decay by age + background worker
│   ├── contradiction.py         # Conflict detection + resolution on ingest
│   ├── epistemic_stress.py      # 5 stress signals: staleness, conflict, entropy
│   ├── epistemic_adaptation.py  # Adaptive retrieval when stress is sustained
│   ├── working_state.py         # Cross-session persistent memory (goal, topic, threads)
│   ├── kb_domain_schemas.py     # Trading predicate ontology (instrument, thesis, macro...)
│   ├── graph_retrieval.py       # Graph traversal: PageRank, BFS, clusters (built, not in API path yet)
│   ├── kb_validation.py         # Atom validation layers (not yet wired)
│   └── graph_v2.py              # Async graph with versioning (requires aiosqlite, not yet wired)
├── retrieval.py                 # Smart multi-strategy retrieval engine
├── api.py                       # Flask REST API
├── ingest/
│   ├── base.py                          # BaseIngestAdapter + RawAtom contract
│   ├── scheduler.py                     # Background scheduler (threading.Timer)
│   ├── yfinance_adapter.py              # Yahoo Finance: price, fundamentals, targets
│   ├── signal_enrichment_adapter.py     # Second-order KB signals (no external API)
│   ├── historical_adapter.py            # 1y OHLCV summary backfill (returns, vol, drawdown)
│   ├── fred_adapter.py                  # FRED: macro regime atoms (requires FRED_API_KEY)
│   ├── edgar_adapter.py                 # SEC EDGAR: filings, insider transactions
│   ├── rss_adapter.py                   # RSS: BBC, CNBC, MarketWatch, Yahoo Finance
│   ├── pattern_adapter.py               # SMC pattern detection: OHLCV fetch + detect + fill tracking
│   └── __init__.py                      # exports all adapters + scheduler
├── analytics/
│   ├── user_modeller.py         # Portfolio → UserModel (risk, sector affinity, holding style)
│   ├── snapshot_curator.py      # CuratedSnapshot assembly from KB + user model
│   ├── pattern_detector.py      # 7 SMC pattern detectors (FVG, IFVG, BPR, OB, Breaker, LV, MB)
│   └── position_calculator.py   # Account-aware position sizing with R:R targets
├── notifications/
│   ├── snapshot_formatter.py    # CuratedSnapshot → Telegram MarkdownV2
│   ├── telegram_notifier.py     # Telegram Bot API wrapper (graceful degradation)
│   ├── delivery_scheduler.py    # Daily briefing scheduler (timezone-aware, local-date dedup)
│   ├── tip_formatter.py         # PatternSignal → Telegram MarkdownV2 tip (tier-gated)
│   └── tip_scheduler.py         # Daily tip scheduler (per-user time, dedup, tier gating)
├── llm/
│   └── overlay_builder.py       # Overlay card assembly + entity extraction
├── users/
│   └── user_store.py            # CRUD for 6 tables: portfolios, models, preferences,
│                                #   snapshot_delivery_log, pattern_signals, tip_delivery_log
├── CONTRIBUTING.md              # Ingest team guide
└── requirements.txt
```

---

## Quick Start

```bash
pip install -r requirements.txt
python api.py
# API running at http://localhost:5050
```

### Environment Variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `TRADING_KB_DB` | No | `trading_knowledge.db` | SQLite database path |
| `PORT` | No | `5050` | Flask server port |
| `FRED_API_KEY` | No | *(skip FRED adapter)* | Free key from [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| `EDGAR_USER_AGENT` | No | `TradingGalaxyKB admin@tradinggalaxy.dev` | SEC requires contact info in User-Agent |
| `TELEGRAM_BOT_TOKEN` | No | *(no Telegram delivery)* | Bot token from [@BotFather](https://t.me/BotFather) — required for daily briefings and tips |

```bash
# Full startup with all features:
FRED_API_KEY=your_key_here python api.py

# Without FRED (adapter skips gracefully):
python api.py
```

---

## Docker (recommended for sharing / collaboration)

### One-command startup

```bash
git clone <repo>
cd trading-galaxy
cp .env.example .env
# Optional: fill in FRED_API_KEY in .env for macro data
docker-compose up
```

This starts two containers:
- **`trading-galaxy`** — the API server + all ingest schedulers, DB at `/data/trading_knowledge.db` (persisted in the `kb-data` volume)
- **`ollama`** — the LLM backend sidecar (official `ollama/ollama` image)

The API is live at `http://localhost:5050` once both containers are healthy.

### Pull a model into Ollama (first boot only)

The Ollama container starts with no models. Pull your chosen model once:

```bash
docker-compose exec ollama ollama pull llama3.2
```

Set `OLLAMA_MODEL=llama3.2` (or whichever model you pulled) in `.env` — this is already the default in `.env.example`.

### Seed the KB on a fresh instance

After first boot the KB is empty. Trigger a full ingest immediately without waiting for the scheduler:

```bash
# bash / WSL / Git Bash / macOS:
curl -X POST http://localhost:5050/ingest/run-all
curl -X POST http://localhost:5050/ingest/historical
```

```powershell
# PowerShell (Windows) — curl is aliased to Invoke-WebRequest, use curl.exe or Invoke-RestMethod:
curl.exe -X POST http://localhost:5050/ingest/run-all
curl.exe -X POST http://localhost:5050/ingest/historical

# Or with native PowerShell syntax:
Invoke-RestMethod -Method POST http://localhost:5050/ingest/run-all
Invoke-RestMethod -Method POST http://localhost:5050/ingest/historical
```

> **Note:** The first `docker-compose up` is slow because it pulls `python:3.11-slim` (~200 MB) and `ollama/ollama` (~2 GB). Subsequent startups use cached images and are near-instant.

### Environment variables

All variables are documented in `.env.example`. Key ones:

| Variable | Default | Required | Purpose |
|---|---|---|---|
| `FRED_API_KEY` | *(empty)* | No | Free key from [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html). FRED adapter skips gracefully if unset. |
| `OLLAMA_MODEL` | `llama3.2` | No | Model name — must match what you pulled with `ollama pull`. |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | No | Set in `docker-compose.yml`. Only change if running Ollama on a remote host. |
| `TRADING_KB_DB` | `/data/trading_knowledge.db` | No | DB path inside the container. Mapped to the `kb-data` named volume. |
| `PORT` | `5050` | No | Host port for the API. |
| `EDGAR_USER_AGENT` | `TradingGalaxyKB admin@tradinggalaxy.dev` | No | SEC requires a contact string in the User-Agent header. |

### Without the Ollama sidecar

If you run Ollama locally outside Docker, set `OLLAMA_BASE_URL` in `.env` to point at your host:

```bash
# .env
OLLAMA_BASE_URL=http://host.docker.internal:11434
```

Then remove the `depends_on` clause from `docker-compose.yml` and start only the API container:

```bash
docker-compose up trading-galaxy
```

---

## Automated Ingest

On startup, `api.py` launches a background scheduler that runs five ingest adapters automatically:

| Adapter | Source | Interval | API Key? | Authority |
|---|---|---|---|---|
| **YFinance** | Yahoo Finance (price, fundamentals, analyst targets) | 5 min | No | 1.00 |
| **SignalEnrichment** | KB introspection — second-order derived signals | 5 min | No | 0.65 |
| **RSS News** | BBC, CNBC, MarketWatch, Yahoo Finance headlines | 15 min | No | 0.60 |
| **EDGAR** | SEC filings (8-K, 10-Q, insider transactions) | 6 hours | No | 0.95 |
| **FRED** | Fed funds rate, CPI, GDP, yield curve | 24 hours | Yes (free) | 0.80 |

All adapters are fault-tolerant — if one fails (missing key, rate limit, network error), the others continue. Check adapter health via `GET /ingest/status`.

Trigger an immediate full refresh without waiting for the next scheduled run:
```bash
curl -X POST http://localhost:5050/ingest/run-all
# or a subset:
curl -X POST http://localhost:5050/ingest/run-all -d '{"adapters":["yfinance","signal_enrichment"]}'
```

---

## REST API

### `POST /ingest` — Push atoms into the KB

Single atom:
```json
{
  "subject":    "AAPL",
  "predicate":  "signal_direction",
  "object":     "long",
  "confidence": 0.85,
  "source":     "model_signal_momentum_v1"
}
```

Batch:
```json
{
  "atoms": [
    { "subject": "AAPL", "predicate": "signal_direction", "object": "long",
      "confidence": 0.85, "source": "model_signal_momentum_v1" },
    { "subject": "AAPL", "predicate": "price_target", "object": "210",
      "confidence": 0.70, "source": "broker_research_gs_20240201" }
  ]
}
```

Response: `{ "ingested": 2, "skipped": 0 }`

---

### `POST /retrieve` — Smart retrieval for a query

```json
{
  "message":    "What is the current signal on AAPL?",
  "session_id": "session_abc",
  "goal":       "optional — persist current working goal",
  "topic":      "optional — persist current topic",
  "turn_count": 0
}
```

`turn_count=0` injects `prior_context` from the last session into the response.
`turn_count=1` anchors the session immediately (crash-safe).
State is persisted on explicit `goal`/`topic` or every 5 turns.

Response:
```json
{
  "snippet": "=== TRADING KNOWLEDGE CONTEXT ===\n[Signals & Positioning]\n  AAPL | signal_direction | long\n...",
  "atoms": [ ... ],
  "stress": {
    "composite_stress": 0.12,
    "decay_pressure": 0.05,
    "authority_conflict": 0.08,
    "supersession_density": 0.00,
    "conflict_cluster": 0.00,
    "domain_entropy": 0.95
  }
}
```

---

### `GET /query` — Direct triple-store query

```
GET /query?subject=AAPL&predicate=signal_direction&limit=10
GET /query?predicate=regime_label
```

---

### `GET /search?q=<text>` — Full-text search

```
GET /search?q=AAPL bullish catalyst&limit=20
GET /search?q=inflation&category=macro
```

---

### `GET /context/<entity>` — All facts about an entity

```
GET /context/AAPL
GET /context/fed_rate_hike_2024
```

---

### `GET /stats` — KB statistics

```json
{ "total_facts": 14832, "unique_subjects": 512, "unique_predicates": 43 }
```

---

### `GET /ingest/status` — Ingest scheduler health

```json
{
  "scheduler": "running",
  "adapters": {
    "yfinance":  { "last_run_at": "...", "last_success_at": "...", "total_atoms": 142, "total_errors": 0 },
    "rss_news":  { "last_run_at": "...", "total_atoms": 58, "last_error": null },
    "edgar":     { "last_run_at": "...", "total_atoms": 23, "total_errors": 0 },
    "fred":      { "last_run_at": null, "last_error": "FRED_API_KEY not set", "total_errors": 1 }
  }
}
```

Use this to detect when an adapter is silently failing.

---

## Building an Ingest Adapter

Subclass `BaseIngestAdapter` from `ingest/`:

```python
from ingest import BaseIngestAdapter, RawAtom

class MySignalFeed(BaseIngestAdapter):
    def __init__(self):
        super().__init__(name='my_signal_feed')

    def fetch(self):
        # Pull from your data source
        signals = my_data_api.get_signals()
        atoms = []
        for s in signals:
            atoms.append(RawAtom(
                subject    = s['ticker'],
                predicate  = 'signal_direction',
                object     = s['direction'],   # 'long' | 'short' | 'neutral'
                confidence = s['confidence'],
                source     = f'model_signal_{s["model_name"]}',
                metadata   = {'generated_at': s['timestamp']},
            ))
        return atoms

# Run and push to KB
from knowledge import KnowledgeGraph
kg = KnowledgeGraph()
feed = MySignalFeed()
feed.run_and_push(kg)
```

Or push via the API:
```python
import requests
requests.post('http://localhost:5050/ingest', json={'atoms': [...]})
```

---

## Predicate Vocabulary

Full schema in `knowledge/kb_domain_schemas.py`. Key predicates:

| Domain | Predicates |
|---|---|
| **trading_instrument** | `has_ticker`, `signal_direction`, `signal_confidence`, `price_target`, `catalyst`, `risk_factor`, `invalidation_condition`, `time_horizon`, `volatility_regime` |
| **market_thesis** | `premise`, `supporting_evidence`, `contradicting_evidence`, `entry_condition`, `exit_condition`, `invalidated_by`, `risk_reward_ratio` |
| **macro_regime** | `regime_label`, `dominant_driver`, `asset_class_bias`, `risk_on_off`, `central_bank_stance`, `inflation_environment` |
| **company** | `sector`, `market_cap_tier`, `earnings_quality`, `competitive_moat`, `revenue_trend`, `catalyst` |
| **research_report** | `publisher`, `analyst`, `rating`, `price_target`, `key_finding`, `compared_to_consensus` |
| **derived_signal** | `price_regime`, `upside_pct`, `signal_quality`, `macro_confirmation`, `invalidation_price`, `invalidation_distance`, `thesis_risk_level`, `conviction_tier`, `volatility_scalar`, `position_size_pct` |

### Derived Signal Predicates

All produced by `SignalEnrichmentAdapter` — computed over existing KB atoms, no external calls. Three logical layers, each building on the previous.

#### Layer 1 — Signal regime

| Predicate | Values | Meaning |
|---|---|---|
| `price_regime` | `near_52w_high` \| `mid_range` \| `near_52w_low` | Where price sits vs fair value (derived from last_price / price_target ratio) |
| `upside_pct` | e.g. `"34.2"` | % upside from last_price to consensus price_target. Negative = price above target. |
| `signal_quality` | `strong` \| `confirmed` \| `extended` \| `conflicted` \| `weak` | Coherence of signal_direction, vol_regime, price_regime, upside_pct composite |
| `macro_confirmation` | `confirmed` \| `partial` \| `unconfirmed` \| `no_data` | Cross-asset alignment: HYG (credit) + TLT (rates) + SPY (market) vs equity signal |

**signal_quality decision rules** (documented in adapter for auditability):
- `conflicted` — bullish signal but price > target; or bearish signal but large upside; or bullish + high_vol + near_52w_high
- `extended` — bullish signal but price already near_52w_high
- `strong` — bullish + ≥15% upside + not extended + low/med volatility
- `confirmed` — bullish + ≥8% upside + not extended
- `weak` — neutral direction or upside_pct unavailable

#### Layer 2 — Invalidation layer

Answers: *at what price is this thesis wrong?* Derived from `low_52w`, `last_price`, `volatility_30d`, `price_regime`.

| Predicate | Values | Meaning |
|---|---|---|
| `invalidation_price` | e.g. `"94.29"` | Structural stop level. IP1: 52w low anchor (if price >15% above it). IP2: 15%-floor when near 52w low. IP3: 20%-floor fallback when 52w low absent. |
| `invalidation_distance` | e.g. `"-51.18"` | `(invalidation_price − last_price) / last_price × 100`. Always negative for long thesis. |
| `thesis_risk_level` | `tight` \| `moderate` \| `wide` | Risk classification. `tight` if distance > −15% or `price_regime=near_52w_low`; `moderate` if distance > −30%; `wide` otherwise (vol-adjusted thresholds apply). |

**thesis_risk_level decision rules** (priority order):
- `tight` — R-T3: `price_regime=near_52w_low`; R-T1: distance > −15%; R-T2: distance > −25% AND vol > 50%
- `moderate` — R-M1: distance > −30%; R-M2: distance > −40% AND vol > 35%
- `wide` — R-W1: all remaining cases

#### Layer 3 — Position sizing layer

Answers: *how much should I allocate?* Derived from `signal_quality`, `thesis_risk_level`, `macro_confirmation`, `volatility_30d`.

| Predicate | Values | Meaning |
|---|---|---|
| `conviction_tier` | `high` \| `medium` \| `low` \| `avoid` \| `no_data` | Composite classification from signal quality × risk level × macro alignment. |
| `volatility_scalar` | `0.20` – `1.00` | `min(1.0, max(0.2, 20.0 / volatility_30d))`. Compresses allocation for high-vol names relative to SPY 20% reference vol. |
| `position_size_pct` | e.g. `"1.82"` | `base_alloc × volatility_scalar`. Base: high=5%, medium=3%, low=1.5%, avoid=0%. |

**conviction_tier decision rules** (priority order, first match wins):
- `avoid` — CT-A2: `signal_quality=conflicted`; CT-A1: `signal_quality=weak AND thesis_risk_level=tight`
- `low` — CT-L1: `signal_quality=weak`; CT-L2: `thesis_risk_level=tight` (non-strong signal); CT-L3: `macro_confirmation=unconfirmed`
- `medium` — CT-M2: `signal_quality=strong AND thesis_risk_level=tight`; CT-M1: `signal_quality=confirmed`
- `high` — CT-H1: `signal_quality=strong AND thesis_risk_level ∈ {moderate,wide} AND macro_confirmation ∈ {confirmed,partial}`

**Volatility scalar examples:**
- `vol=20` → `1.00` (no adjustment — at SPY reference)
- `vol=40` → `0.50` (half allocation)
- `vol=10` → `1.00` (capped — low vol doesn't inflate beyond base)
- `vol=200` → `0.20` (floored — extreme vol still gets minimal allocation)
- `vol missing` → `conviction_tier` only emitted; scalar and size skipped

---

## Source Naming & Authority

Sources are prefix-matched. Use the correct prefix to get accurate authority weighting and decay:

| Prefix | Authority | Half-life | Use for |
|---|---|---|---|
| `exchange_feed` | 1.00 | ~10 min | Direct price/OI/volume |
| `regulatory_filing` | 0.95 | 1 year | SEC/FCA filings |
| `earnings_` | 0.85 | 30 days | Earnings/guidance facts |
| `curated_` | 0.90 | 6 months | Hand-authored analyst notes |
| `broker_research` | 0.80 | 21 days | Institutional research |
| `macro_data` | 0.80 | 60 days | Central bank, government |
| `model_signal_` | 0.70 | 12 hours | Quant model outputs |
| `derived_signal_` | 0.65 | 5 min | Second-order KB signals (SignalEnrichmentAdapter) |
| `technical_` | 0.65 | 6 hours | Technical indicators |
| `news_wire_` | 0.60 | 1 day | Reuters, Bloomberg |
| `alt_data_` | 0.55 | 3 days | Satellite, web data |
| `social_signal_` | 0.35 | 12 hours | Twitter, Reddit, StockTwits |

---

## Epistemic Guarantees

These run automatically — the ingest team doesn't need to manage them:

- **Contradiction detection**: if `AAPL | signal_direction | short` already exists and you ingest `AAPL | signal_direction | long`, the weaker atom is marked `superseded_by` (never deleted). Full audit log in `fact_conflicts`.
- **Confidence decay**: `exchange_feed` atoms decay to near-zero after ~30 minutes. `regulatory_filing` atoms stay valid for a year. Background worker runs every 24h.
- **Epistemic stress**: the `/retrieve` response includes a `stress` object. `composite_stress > 0.65` means the KB is degraded for this topic — the copilot layer should signal lower confidence.
- **Cross-session memory**: the KB remembers the last working goal, topic, and open threads across restarts via `working_state`.

---

## Product Layer — Daily Briefing + Copilot Overlay

Two delivery modes built on top of the KB.

### Passive Mode — Scheduled Daily Briefing

A personalised "newspaper" delivered to Telegram at a user-configured time each day. No alerts, no noise — one daily snapshot per user.

#### How it works

```
POST /users/{id}/portfolio  →  build_user_model()  →  curate_snapshot()  →  format_snapshot()
                                    ↓
                           DeliveryScheduler (background thread, 60s tick)
                                    ↓
                           TelegramNotifier.send()  →  snapshot_delivery_log
```

1. User submits portfolio (or onboarding preferences as fallback)
2. Backend derives a `UserModel` (risk tolerance, sector affinity, holding style)
3. `curate_snapshot()` assembles a `CuratedSnapshot` — portfolio health, market context, 3–5 scored opportunities
4. `format_snapshot()` renders Telegram MarkdownV2 (all dynamic strings escaped via `_escape_mdv2`)
5. `DeliveryScheduler` fires once per day at the user's local `delivery_time` (DST-safe, local-date dedup)

#### Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Yes (for delivery) | Bot token from [@BotFather](https://t.me/BotFather) |

#### Passive mode endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/users/{id}/portfolio` | `POST` | Submit/replace holdings → triggers user model rebuild |
| `/users/{id}/portfolio` | `GET` | Get current holdings |
| `/users/{id}/model` | `GET` | Get derived user model (risk tolerance, sector affinity, etc.) |
| `/users/{id}/onboarding` | `POST` | Set sector preferences + delivery time/timezone (fallback path — no portfolio needed) |
| `/users/{id}/snapshot/preview` | `GET` | Generate full snapshot as JSON without sending to Telegram |
| `/users/{id}/snapshot/send-now` | `POST` | Trigger immediate Telegram delivery |
| `/users/{id}/delivery-history` | `GET` | Past delivery log (success/fail, regime, opportunities count) |
| `/notify/test` | `POST` | Send test message to a Telegram chat ID (onboarding verification) |

##### `POST /users/{id}/portfolio`

```json
{
  "holdings": [
    {"ticker": "AAPL", "quantity": 10, "avg_cost": 150.0, "sector": "Technology"},
    {"ticker": "MA",   "quantity": 5,  "avg_cost": 380.0, "sector": "Financials"}
  ]
}
```

Response: `{ "user_id": "...", "count": 2, "submitted_at": "...", "model": { "risk_tolerance": "moderate", ... } }`

##### `POST /users/{id}/onboarding`

```json
{
  "selected_sectors": ["technology", "financials"],
  "risk_tolerance": "moderate",
  "delivery_time": "08:00",
  "timezone": "Europe/London",
  "telegram_chat_id": "123456789"
}
```

##### `GET /users/{id}/snapshot/preview`

Returns the full `CuratedSnapshot` as JSON — use this to render a preview UI before the user subscribes.

```json
{
  "user_id": "alice",
  "generated_at": "2026-02-24T08:00:00Z",
  "portfolio_summary": [...],
  "holdings_at_risk": ["MSFT"],
  "holdings_performing": ["MA", "GOOGL"],
  "market_regime": "risk_on_expansion",
  "regime_implication": "Risk-On Expansion favours your Financials holdings",
  "macro_summary": "Fed: on hold | Yield curve: +60bps",
  "top_opportunities": [
    {
      "ticker": "BAC",
      "thesis": "Strong net interest income growth expected",
      "conviction_tier": "high",
      "upside_pct": 23.7,
      "invalidation_distance": -31.2,
      "asymmetry_ratio": 1.6,
      "position_size_pct": 3.81,
      "relevance_reason": "Financials affinity",
      "urgency": "immediate"
    }
  ],
  "opportunities_to_avoid": ["INTC"]
}
```

##### Opportunity scoring weights

| Signal | Score |
|---|---|
| `conviction_tier=high` | +1.0 base |
| `conviction_tier=medium` | +0.5 base |
| Sector matches `sector_affinity` | +0.30 |
| `options_regime=compressed` | +0.15 |
| `macro_confirmation=confirmed` | +0.10 |
| Risk profile match | +0.05–0.10 |
| `conviction_tier=avoid` | excluded |

Minimum score threshold: **0.5**. Top 5 surfaced.

##### User model inference rules

| Field | Rule |
|---|---|
| `risk_tolerance=aggressive` | ≥40% holdings in high-beta sectors (tech, biotech, semis) |
| `risk_tolerance=conservative` | ≥40% holdings in defensive sectors (utilities, staples, healthcare) |
| `holding_style=value` | avg `upside_pct` across holdings < 10% |
| `holding_style=momentum` | avg conviction tier score > 0.60 |
| `concentration_risk=diversified` | holdings span ≥5 distinct sectors |
| `sector_affinity` | sectors with ≥2 holdings (top-2 if none qualify) |

---

### Active Mode — Copilot Overlay

The existing `POST /chat` endpoint enhanced with screen context awareness. When the user is looking at a chart and asks a question, the overlay extracts the ticker from their screen and returns structured data cards alongside the prose answer.

#### Enhanced `POST /chat`

New optional request fields:

```json
{
  "message": "Is this a good setup?",
  "session_id": "user_abc",
  "screen_context": "NVDA Daily Chart — Price $192.95 — RSI 67 — Volume spike",
  "screen_entities": ["NVDA"],
  "overlay_mode": true
}
```

New response field (`overlay_cards`) when `overlay_mode=true`:

```json
{
  "answer": "NVDA is currently showing confirmed signal quality...",
  "overlay_cards": [
    {
      "type": "signal_summary",
      "ticker": "NVDA",
      "conviction_tier": "medium",
      "signal_quality": "confirmed",
      "position_size_pct": 1.60,
      "upside_pct": 31.7,
      "invalidation_distance": -51.2,
      "asymmetry_ratio": 1.6,
      "options_regime": "normal",
      "thesis_risk_level": "moderate",
      "macro_confirmation": "partial"
    },
    {
      "type": "causal_context",
      "event": "risk_on_expansion",
      "affected_tickers": ["NVDA", "AMD", "INTC"],
      "regime": "risk_on_expansion"
    },
    {
      "type": "stress_flag",
      "composite_stress": 0.21,
      "flag": null
    }
  ],
  "atoms_used": 14,
  "model": "llama3.2"
}
```

**Card types:**
- `signal_summary` — one per extracted ticker; all KB signal atoms
- `causal_context` — current market regime → BFS causal chain → affected tickers
- `stress_flag` — `composite_stress`; `flag="high_stress"` if > 0.60, else null

**Entity extraction** (`llm/overlay_builder.py`):
1. Regex `\b[A-Z]{2,5}\b` on `screen_context`
2. Filter via `_UPPERCASE_STOPWORDS` (covers RSI, GMT, USD, ETF, CEO, EMA, etc.)
3. Validate against known KB subjects
4. Merge with explicit `screen_entities` (bypass validation)

All new fields are additive — existing `POST /chat` clients are unaffected.

---

### New files (product layer)

| File | Purpose |
|---|---|
| `users/user_store.py` | CRUD for 6 tables: `user_portfolios`, `user_models`, `user_preferences`, `snapshot_delivery_log`, `pattern_signals`, `tip_delivery_log` |
| `analytics/user_modeller.py` | Portfolio analysis → derived `UserModel` |
| `analytics/snapshot_curator.py` | `CuratedSnapshot` assembly from KB + user model |
| `notifications/snapshot_formatter.py` | `CuratedSnapshot` → Telegram MarkdownV2 (with `_escape_mdv2` helper) |
| `notifications/telegram_notifier.py` | Telegram Bot API wrapper (graceful degradation if token unset) |
| `notifications/delivery_scheduler.py` | Background thread, timezone-aware, local-date dedup |
| `llm/overlay_builder.py` | Overlay card assembly + entity extraction |
| `analytics/pattern_detector.py` | 7 SMC price-action pattern detectors with quality scoring |
| `analytics/position_calculator.py` | Account-aware position sizing: stop, units, T1/T2/T3 targets |
| `ingest/pattern_adapter.py` | OHLCV fetch via yfinance → detect patterns → persist + fill tracking |
| `notifications/tip_formatter.py` | `PatternSignal` + `PositionRecommendation` → Telegram MarkdownV2 (tier-gated) |
| `notifications/tip_scheduler.py` | Daily tip background thread: per-user time, tier gating, dedup |

---

## Pattern Tip Pipeline

A second delivery mode that sends **one actionable trading tip per day** based on live price-action pattern detection — separate from the daily briefing.

### How it works

```
PatternAdapter (15 min)  →  pattern_signals table  →  TipScheduler (60s tick)
     ↓                                                      ↓
yfinance OHLCV                                    _pick_best_pattern()
     ↓                                                      ↓
detect_all_patterns()                             calculate_position()
     ↓                                                      ↓
upsert_pattern_signal()                           format_tip()  →  TelegramNotifier.send()
                                                               ↓
                                               mark_pattern_alerted() + log_tip_delivery()
```

1. **`PatternAdapter`** runs every 15 minutes — fetches OHLCV candles for all KB tickers, detects 7 patterns, enriches with KB conviction/regime context, and persists new rows to `pattern_signals` (dedup-guarded). On each run it also re-evaluates open patterns for fill/break status updates.
2. **`TipScheduler`** checks every 60 seconds — for each onboarded user whose `tip_delivery_time` matches the current local time and who has not yet received a tip today, it picks the highest-quality eligible pattern (tier- and timeframe-gated, not already alerted to this user), sizes the position, formats the tip, and sends it via Telegram.
3. **Local-date dedup** — same strategy as the briefing scheduler: `tip_delivery_log.delivered_at_local_date` stores the user's local date so DST transitions never cause double-sends.

### Patterns detected

| Pattern | Key | Timeframes | Notes |
|---|---|---|---|
| Fair Value Gap | `fvg` | any | 3-candle gap; bullish or bearish |
| Inverse FVG | `ifvg` | any | FVG that was fully filled — now acts as support/resistance |
| Balanced Price Range | `bpr` | any | Overlapping bullish + bearish FVGs |
| Order Block | `order_block` | any | Last opposite-colour candle before a strong move |
| Breaker Block | `breaker` | any | Order block that was violated — structure flip |
| Liquidity Void | `liquidity_void` | any | Thin-body candle with large range (≥85% wick) |
| Mitigation Block | `mitigation` | any | Retest of an order block after displacement |

### Quality scoring

Every `PatternSignal` carries a `quality_score` in [0, 1] derived from:

| Component | Weight | Detail |
|---|---|---|
| KB conviction | 0.30 | `high`/`strong`/`confirmed` → full score; `medium` → partial |
| KB regime alignment | 0.20 | `risk_on*` for bullish, `risk_off*` for bearish |
| KB signal direction | 0.15 | Direction matches pattern direction |
| Gap size vs ATR | 0.20 | Larger gap relative to ATR → higher score (capped at 2× ATR) |
| Recency | 0.15 | Decays by `_RECENCY_DECAY` per candle since formation |

### Position sizing

`calculate_position()` returns `None` if `account_size` is zero or unset — no recommendation without an account. Otherwise:

```
entry     = (zone_high + zone_low) / 2
buffer    = zone_size_pct × 0.10
stop_loss = zone_low  × (1 − buffer/100)   # bullish
          = zone_high × (1 + buffer/100)   # bearish
risk_£    = account_size × (max_risk_per_trade_pct / 100)
units     = risk_£ / (entry − stop_loss)
target_1  = entry + 1 × (entry − stop_loss)   # 1:1 R
target_2  = entry + 2 × (entry − stop_loss)   # 1:2 R
target_3  = entry + 3 × (entry − stop_loss)   # 1:3 R (pro tier only)
```

### Tier gating

| Feature | Basic | Pro |
|---|---|---|
| Patterns | FVG, IFVG | All 7 |
| Timeframes | 1H | 15M, 1H, 4H, Daily |
| Target shown | T1, T2 | T1, T2, T3 |
| Tips per day | 1 | Unlimited |

### Pattern Tip Pipeline endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/patterns/live` | `GET` | Query open/partially-filled pattern signals |
| `/users/{id}/tip/preview` | `GET` | Preview today's tip as JSON (no Telegram send) |
| `/users/{id}/tip-config` | `GET` | Get current tip configuration |
| `/users/{id}/tip-config` | `POST` | Update tip configuration |
| `/users/{id}/tip/history` | `GET` | Recent tip delivery log |

##### `GET /patterns/live`

Query parameters (all optional):

| Param | Type | Example | Description |
|---|---|---|---|
| `ticker` | string | `NVDA` | Filter by ticker |
| `pattern_type` | string | `fvg` | Filter by pattern type |
| `direction` | string | `bullish` | `bullish` or `bearish` |
| `timeframe` | string | `1h` | `15m`, `1h`, `4h`, `1d` |
| `min_quality` | float | `0.6` | Minimum quality score (0–1) |
| `limit` | int | `20` | Max rows returned (default 50) |

```json
{
  "patterns": [
    {
      "id": 42,
      "ticker": "NVDA",
      "pattern_type": "fvg",
      "direction": "bullish",
      "zone_high": 192.0,
      "zone_low": 189.0,
      "zone_size_pct": 1.587,
      "timeframe": "1h",
      "formed_at": "2026-02-25T07:00:00",
      "status": "open",
      "quality_score": 0.87,
      "kb_conviction": "high",
      "kb_regime": "risk_on_expansion",
      "kb_signal_dir": "long",
      "detected_at": "2026-02-25T08:15:00Z"
    }
  ],
  "count": 1
}
```

##### `GET /users/{id}/tip/preview`

Returns the tip that would be sent right now for this user — highest eligible pattern + position sizing — without sending to Telegram. Respects tier gating.

```json
{
  "tip": {
    "ticker": "NVDA",
    "pattern_type": "fvg",
    "direction": "bullish",
    "zone_high": 192.0,
    "zone_low": 189.0,
    "quality_score": 0.87,
    "tier": "basic",
    "position": {
      "suggested_entry": 190.5,
      "stop_loss": 188.71,
      "stop_distance_pct": 0.94,
      "account_size": 10000.0,
      "account_currency": "GBP",
      "risk_pct": 1.0,
      "risk_amount": 100.0,
      "position_size_units": 55,
      "position_value": 10477.5,
      "target_1": 192.29,
      "target_2": 194.08,
      "target_3": null,
      "expected_profit_t1": 100.0,
      "expected_profit_t2": 200.0,
      "expected_profit_t3": null
    }
  }
}
```

##### `GET|POST /users/{id}/tip-config`

`GET` returns the current tip configuration. `POST` updates it — all fields optional:

```json
{
  "tip_delivery_time":      "08:00",
  "tip_delivery_timezone":  "Europe/London",
  "tip_markets":            ["equities"],
  "tip_timeframes":         ["1h", "4h"],
  "tip_pattern_types":      ["fvg", "order_block"],
  "account_size":           12000.0,
  "max_risk_per_trade_pct": 1.5,
  "account_currency":       "GBP",
  "tier":                   "pro"
}
```

##### `GET /users/{id}/tip/history`

Query param: `limit` (default 30). Returns tip delivery log newest first:

```json
{
  "history": [
    {
      "id": 7,
      "user_id": "alice",
      "pattern_signal_id": 42,
      "delivered_at": "2026-02-25T08:01:03Z",
      "delivered_at_local_date": "2026-02-25",
      "success": 1,
      "message_length": 812
    }
  ],
  "count": 1
}
```

### DB schema additions

Two new tables in the same SQLite DB:

**`pattern_signals`** — one row per detected pattern instance:

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `ticker` | TEXT | e.g. `NVDA` |
| `pattern_type` | TEXT | `fvg`, `ifvg`, `bpr`, `order_block`, `breaker`, `liquidity_void`, `mitigation` |
| `direction` | TEXT | `bullish` or `bearish` |
| `zone_high` / `zone_low` | REAL | Price zone bounds |
| `zone_size_pct` | REAL | Zone size as % of `zone_low` |
| `timeframe` | TEXT | `15m`, `1h`, `4h`, `1d` |
| `formed_at` | TEXT | ISO timestamp of pattern formation |
| `status` | TEXT | `open`, `partially_filled`, `filled`, `broken` |
| `quality_score` | REAL | 0–1 composite score |
| `kb_conviction` | TEXT | KB conviction atom value |
| `kb_regime` | TEXT | KB regime atom value |
| `kb_signal_dir` | TEXT | KB signal direction atom value |
| `alerted_users` | TEXT | JSON array of user IDs already sent this pattern |
| `detected_at` | TEXT | ISO timestamp of detection run |

**`tip_delivery_log`** — one row per tip send attempt:

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `user_id` | TEXT | User ID |
| `pattern_signal_id` | INTEGER | FK → `pattern_signals.id` |
| `delivered_at` | TEXT | UTC ISO timestamp |
| `delivered_at_local_date` | TEXT | `YYYY-MM-DD` in user's local timezone (dedup key) |
| `success` | INTEGER | 1 = sent, 0 = failed |
| `message_length` | INTEGER | Characters in rendered message |

**New columns on `user_preferences`:**

| Column | Default | Description |
|---|---|---|
| `tier` | `basic` | `basic` or `pro` |
| `tip_delivery_time` | `08:00` | `HH:MM` in `tip_delivery_timezone` |
| `tip_delivery_timezone` | `UTC` | IANA timezone string |
| `tip_markets` | `["equities"]` | JSON array |
| `tip_timeframes` | `["1h"]` | JSON array of timeframe codes |
| `tip_pattern_types` | `null` | JSON array (null = all allowed for tier) |
| `account_size` | `null` | Account size in `account_currency` |
| `max_risk_per_trade_pct` | `1.0` | Max % of account to risk per trade |
| `account_currency` | `GBP` | ISO currency code |

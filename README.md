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
│   └── __init__.py                      # exports all adapters + scheduler
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

```bash
# Full startup with all features:
FRED_API_KEY=your_key_here python api.py

# Without FRED (adapter skips gracefully):
python api.py
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

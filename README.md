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
│   ├── base.py                  # BaseIngestAdapter + RawAtom contract (START HERE)
│   └── __init__.py              # exports BaseIngestAdapter, RawAtom
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

Override the DB path:
```bash
TRADING_KB_DB=/data/trading_knowledge.db python api.py
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

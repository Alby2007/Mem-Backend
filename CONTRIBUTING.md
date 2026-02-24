# Contributing — Signal Ingest Team

This document is the starting point for building signal ingest adapters on top of the Trading Galaxy knowledge base.

---

## What the KB does for you (automatically)

You push atoms. The KB handles everything else:

- **Deduplication** — duplicate `(subject, predicate, object)` triples are silently merged; higher confidence wins
- **Contradiction detection** — if you push `AAPL | signal_direction | short` when `long` already exists, the weaker atom is marked `superseded_by` and logged to `fact_conflicts`. Nothing is deleted.
- **Confidence decay** — atoms decay exponentially based on their source half-life (e.g. `model_signal_` atoms halve in 12h, `regulatory_filing` in 1yr). The decay worker runs every 24h.
- **Authority weighting** — retrieval re-ranks atoms by `confidence × source_authority`. Use the correct source prefix.
- **Full-text search + graph retrieval** — available immediately after ingest via `/retrieve` and `/search`.

---

## Quickstart

```bash
pip install -r requirements.txt
python api.py
# API at http://localhost:5050
```

Test the pipeline end-to-end:

```bash
# Push a signal
curl -X POST http://localhost:5050/ingest \
  -H 'Content-Type: application/json' \
  -d '{"subject":"AAPL","predicate":"signal_direction","object":"long","confidence":0.85,"source":"model_signal_momentum_v1"}'

# Retrieve it
curl -X POST http://localhost:5050/retrieve \
  -H 'Content-Type: application/json' \
  -d '{"message":"What is the AAPL signal?"}'

# Check health
curl http://localhost:5050/health

# Check stats
curl http://localhost:5050/stats
```

---

## Building an ingest adapter

Subclass `BaseIngestAdapter` from `ingest/base.py`:

```python
from ingest import BaseIngestAdapter, RawAtom

class MySignalFeed(BaseIngestAdapter):
    def __init__(self):
        super().__init__(name='my_signal_feed')

    def fetch(self) -> list[RawAtom]:
        # Pull from your data source
        return [
            RawAtom(
                subject    = 'AAPL',
                predicate  = 'signal_direction',
                object     = 'long',
                confidence = 0.85,
                source     = 'model_signal_momentum_v1',
                metadata   = {'generated_at': '2024-01-01T09:00:00Z'},
            )
        ]

# Run and push directly to the KB
from knowledge import KnowledgeGraph
kg = KnowledgeGraph()
feed = MySignalFeed()
feed.run_and_push(kg)
```

Or push via HTTP (preferred for decoupled services):

```python
import requests
requests.post('http://localhost:5050/ingest', json={
    'atoms': [
        {'subject': 'AAPL', 'predicate': 'signal_direction', 'object': 'long',
         'confidence': 0.85, 'source': 'model_signal_momentum_v1'}
    ]
})
```

---

## Atom schema

| Field | Type | Required | Notes |
|---|---|---|---|
| `subject` | str | ✅ | Entity being described. Use ticker, concept ID, or regime label. |
| `predicate` | str | ✅ | Relationship type. Must come from the predicate vocabulary below. |
| `object` | str | ✅ | Value or target. Keep it concise (< 300 chars). |
| `confidence` | float | ✅ | `[0.0, 1.0]`. See confidence guidelines below. |
| `source` | str | ✅ | Source string. **Must use a recognised prefix** — affects authority and decay. |
| `metadata` | dict | ❌ | Any extra context (analyst, url, timestamp, model version, etc). |

---

## Predicate vocabulary

Use predicates from `knowledge/kb_domain_schemas.py`. The most important ones for signal ingest:

| Domain | Predicates |
|---|---|
| **Signals** | `signal_direction`, `signal_confidence`, `price_target`, `entry_condition`, `exit_condition`, `invalidation_condition`, `time_horizon`, `volatility_regime` |
| **Thesis** | `premise`, `supporting_evidence`, `contradicting_evidence`, `risk_reward_ratio`, `position_sizing_note`, `invalidated_by` |
| **Macro** | `regime_label`, `dominant_driver`, `asset_class_bias`, `risk_on_off`, `central_bank_stance`, `inflation_environment` |
| **Company** | `sector`, `market_cap_tier`, `earnings_quality`, `competitive_moat`, `revenue_trend`, `catalyst` |
| **Research** | `publisher`, `analyst`, `rating`, `key_finding`, `compared_to_consensus` |

---

## Source naming (critical — affects authority and decay)

| Prefix | Authority | Half-life | Use for |
|---|---|---|---|
| `exchange_feed_` | 1.00 | ~10 min | Direct price/OI/volume |
| `regulatory_filing_` | 0.95 | 1 year | SEC/FCA filings |
| `curated_` | 0.90 | 6 months | Hand-authored analyst notes |
| `earnings_` | 0.85 | 30 days | Earnings/guidance facts |
| `broker_research_` | 0.80 | 21 days | Institutional research |
| `macro_data_` | 0.80 | 60 days | Central bank, government |
| `model_signal_` | 0.70 | 12 hours | Quant model outputs |
| `technical_` | 0.65 | 6 hours | Technical indicators |
| `news_wire_` | 0.60 | 1 day | Reuters, Bloomberg |
| `alt_data_` | 0.55 | 3 days | Satellite, web data |
| `social_signal_` | 0.35 | 12 hours | Twitter, Reddit, StockTwits |

---

## Confidence guidelines

| Value | Meaning |
|---|---|
| `1.0` | Directly observed, unambiguous (e.g. price from exchange) |
| `0.9` | Strongly supported by high-authority source |
| `0.8` | Well-supported, minor interpretation required |
| `0.7` | Model output or derived, reasonable confidence |
| `0.5` | Uncertain, placeholder, or low-signal |
| `0.3` | Speculative, conflicting evidence, or noisy source |

---

## API reference

| Endpoint | Method | Purpose |
|---|---|---|
| `/ingest` | POST | Push one or batch of atoms |
| `/retrieve` | POST | Smart retrieval for a natural-language query |
| `/query` | GET | Direct triple-store query (`?subject=AAPL&predicate=signal_direction`) |
| `/search` | GET | Full-text search (`?q=AAPL bullish&category=signal`) |
| `/context/<entity>` | GET | All facts about an entity |
| `/stats` | GET | Total facts, unique subjects, unique predicates |
| `/health` | GET | Liveness check |

---

## What NOT to touch

These files are the core KB engine. Do not modify without review:

- `knowledge/graph.py` — triple store
- `knowledge/authority.py` — source trust weights
- `knowledge/decay.py` — confidence decay worker
- `knowledge/contradiction.py` — conflict detection
- `knowledge/epistemic_stress.py` — stress signals
- `retrieval.py` — multi-strategy retrieval engine
- `api.py` — Flask wrapper

---

## Not yet wired (future PRs)

- `knowledge/graph_v2.py` — async graph with versioning (requires `aiosqlite`)
- `knowledge/graph_retrieval.py` — PageRank/BFS/cluster traversal (built, not in API path yet)
- `knowledge/kb_validation.py` — atom validation layers
- `knowledge/kb_repair_*.py` — KB gap detection and repair engine
- Hit-count tracking — `hit_count` column exists on `facts` table, increment on retrieval to activate frequency term in importance formula

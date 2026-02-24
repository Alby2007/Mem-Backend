# Architecture

## Overview

Trading Galaxy is a **zero-LLM trading knowledge base** — a persistent, queryable, epistemically-aware triple store that:

1. **Ingests** live market data from free data sources on automatic schedules
2. **Stores** knowledge as typed `(subject, predicate, object)` atoms with confidence and provenance
3. **Retrieves** context for natural-language or structured queries using multi-strategy ranking
4. **Monitors** epistemic health: knowledge decay, authority conflicts, composite stress

The system is designed to feed a copilot or LLM layer with accurate, ranked, non-stale trading context — without needing an LLM to reason about the data itself.

---

## Component Diagram

```
┌───────────────────────────────────────────────────────────────────┐
│                        External Data Sources                       │
│  Yahoo Finance (yfinance)  FRED API  SEC EDGAR  RSS Feeds          │
└────────┬──────────────────────┬──────────┬──────────┬─────────────┘
         │                      │          │          │
         ▼                      ▼          ▼          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ingest/ package                             │
│                                                                  │
│  YFinanceAdapter  FREDAdapter  EDGARAdapter  RSSAdapter          │
│         └──────────────┬──────────────────────┘                  │
│                  BaseIngestAdapter                               │
│                  fetch() → transform() → validate() → push()    │
│                                                                  │
│                  IngestScheduler                                 │
│                  (threading.Timer, per-adapter intervals)        │
│                  health status tracked per adapter               │
└────────────────────────────┬────────────────────────────────────┘
                             │ RawAtom list
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   knowledge/ package                             │
│                                                                  │
│  TradingKnowledgeGraph (graph.py)                               │
│  ├── SQLite WAL triple store (facts table)                       │
│  ├── FTS5 index (facts_fts)                                      │
│  ├── fact_conflicts audit log                                    │
│  ├── decay_log                                                   │
│  └── thread-local connections                                    │
│                                                                  │
│  authority.py        — source trust weights + effective_score    │
│  decay.py            — confidence decay + background worker      │
│  contradiction.py    — conflict detection                        │
│  epistemic_stress.py — composite stress signal                   │
│  working_state.py    — cross-session goal/topic memory           │
└────────────────────────────┬────────────────────────────────────┘
                             │ sqlite3.Connection
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    retrieval.py                                  │
│                                                                  │
│  Multi-strategy retrieve():                                      │
│  1. GNN atoms (cross-asset queries)                              │
│  2. FTS on key terms (skipped when tickers+intent present)       │
│  3. Predicate keyword boost (intent-aware predicate fetch)       │
│  4. Direct ticker/subject match                                  │
│  5a. High-value signal predicates                                │
│  5b. Fallback: top-confidence atoms                              │
│  → Re-rank by authority.effective_score                          │
│  → Format into labelled sections                                 │
└────────────────────────────┬────────────────────────────────────┘
                             │ (snippet, atoms[])
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                       api.py (Flask)                             │
│                                                                  │
│  POST /ingest          POST /retrieve      GET /query            │
│  GET  /search          GET  /context/:e    GET  /stats           │
│  GET  /health          GET  /ingest/status                       │
└─────────────────────────────────────────────────────────────────┘
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
    → strategy 1: GNN atoms (if cross-asset query)
    → strategy 2: FTS (skipped if tickers + intent keywords present)
    → strategy 3: predicate boost per ticker
    → strategy 4: direct ticker match (LIMIT 6)
    → strategy 5a: high-value predicates for first term
    → strategy 5b: fallback top-confidence (if < 8 results)
    → re-rank by authority.effective_score
    → format into [Signals] [Theses] [Macro] [Research] [Other]
    → compute_stress(atoms)
    → return { snippet, atoms, stress }
```

---

## Ingest Intervals

| Adapter | Interval | Rationale |
|---|---|---|
| `YFinanceAdapter` | 5 min | Price + signals need to be near-real-time |
| `RSSAdapter` | 15 min | Headlines cycle every 15–30 min |
| `EDGARAdapter` | 6 hours | Filings are rare; daily is sufficient |
| `FREDAdapter` | 24 hours | FRED macro series update daily at most |

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

## Built but Not Yet Wired

These modules exist in `knowledge/` but are not active in the request path:

| Module | Purpose |
|---|---|
| `graph_v2.py` | Extended graph with richer traversal |
| `graph_enhanced.py` | Extensions to graph_v2 |
| `graph_retrieval.py` | PageRank / BFS / cluster traversal |
| `kb_validation.py` | Atom validation layers |
| `kb_insufficiency_classifier.py` | KB gap detection |
| `kb_repair_proposals.py` | Repair suggestion engine |
| `kb_repair_executor.py` | Repair execution engine |

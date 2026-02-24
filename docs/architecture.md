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
│  0. Graph-relational context (PageRank + clustering + BFS paths) │
│  1. Cross-asset GNN atoms                                         │
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
│  POST /repair/diagnose POST /repair/proposals                    │
│  POST /repair/execute  POST /repair/rollback GET /repair/impact  │
│  POST /kb/graph        POST /kb/traverse                         │
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
    → strategy 0: graph-relational context (PageRank+clustering+BFS)
                  fires on relational/explanatory queries or no tickers
                  builds graph_snippet prepended to final output
    → strategy 1: GNN atoms (if cross-asset query)
    → strategy 2: FTS (skipped if tickers + intent keywords present)
    → strategy 3: predicate boost per ticker
    → strategy 4: direct ticker match (LIMIT 6)
    → strategy 5a: high-value predicates for first term
    → strategy 5b: fallback top-confidence (if < 8 results)
    → re-rank by authority.effective_score
    → apply AdaptationNudges (from prior-turn streak):
          prefer_recent=True       → sort by timestamp DESC
          prefer_high_authority    → filter atoms below authority cutoff
          retrieval_scope_broadened → broaden DB fetch if < 8 results
    → increment hit_count for all returned (subject, predicate) pairs
          feeds frequency term (δ) in PageRank importance formula
    → format into [Signals] [Theses] [Macro] [Research] [Other]
    → prepend graph_snippet if produced
    → compute_stress(atoms)
    → update session streak:
          stress >= 0.65 → streak++  else streak--
    → compute AdaptationNudges for next turn
    → if nudges.refresh_domain_queued → scheduler.run_now('yfinance')
    → if stress > 0.35 OR atoms < 8:
          classify_insufficiency(topic, stress_report, conn)
          → append kb_diagnosis to response
    → return { snippet, atoms, stress, adaptation?, kb_diagnosis? }
```

### Adaptation Path

```
GET  /adapt/status?session_id=X
    → return { streak, last_stress } for session

POST /adapt/reset { session_id }
    → zero streak + last_stress for session
    → use on topic shift or new conversation start
```

#### AdaptationNudges rules (EpistemicAdaptationEngine)

| Rule | Condition | Effect |
|---|---|---|
| 1. Scope broadening | domain_entropy < 0.35 + streak ≥ 2 | Broadens DB fetch to all sources |
| 2. Authority filter | authority_conflict > 0.55 + streak ≥ 2 | Drops atoms below authority cutoff |
| 3. Recency bias | decay_pressure > 0.50 + streak ≥ 2 | Sorts atoms by timestamp DESC |
| 4. Consolidation mode | streak ≥ 3 | Lowers escalation threshold, raises confidence floor |
| 5. Domain refresh queue | decay_pressure > 0.60 + streak ≥ 3 | Logs to domain_refresh_queue; dispatches run_now('yfinance') |
| 6. KB insufficiency detection | consolidation fires ≥ 5× in 7 days | Triggers classify_insufficiency + generate_repair_proposals |

### Governance Path (on-demand)

```
POST /repair/diagnose { topic }
    → retrieve(topic) → compute_stress(atoms)
    → classify_insufficiency(topic, stress, conn)
    → return InsufficiencyDiagnosis { types, signals, confidence }

POST /repair/proposals { topic }
    → diagnose(topic)
    → generate_repair_proposals(diagnosis, conn)
    → governance_verdict(validate_all(topic)) — applies confidence penalty
    → return proposals[] { strategy, preview, simulation, validation }

POST /repair/execute { proposal_id, dry_run=true }
    → fetch proposal from repair_proposals table
    → _snapshot_signals() before
    → apply mutations atomically (BEGIN IMMEDIATE)
    → _snapshot_signals() after
    → auto-rollback if stress worsens by > 0.05
    → write execution_log
    → return ExecutionResult { before, after, divergence, mutations_applied }

POST /kb/graph { message }
    → retrieve(message, limit=100)
    → build_graph_context(atoms) → PageRank + clusters + BFS paths
    → return graph-structured context string

POST /kb/traverse { topic }
    → broad DB fetch for topic (LIMIT 200)
    → what_do_i_know_about(topic, atoms) → BFS expansion depth 3
    → return traversal string + connected concept list
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

## Governance Stack (now live)

The JARVIS epistemic governance stack is fully wired:

| Module | Where wired | Role |
|---|---|---|
| `graph_retrieval.py` | `retrieval.py` strategy 0 | PageRank centrality, community clusters, BFS concept paths |
| `kb_insufficiency_classifier.py` | `POST /retrieve` + `POST /repair/diagnose` | 9-rule insufficiency classifier — fires on elevated stress or thin coverage |
| `kb_validation.py` | `POST /repair/proposals` (governance hook) | 3-layer governance: schema, semantic, cross-topic validation |
| `kb_repair_proposals.py` | `POST /repair/proposals` | Generates repair proposals with preview + simulation + validation target |
| `kb_repair_executor.py` | `POST /repair/execute`, `/rollback`, `/impact` | Human-gated atomic execution with auto-rollback and divergence tracking |

### Insufficiency Types (9 rules)

| Type | Condition |
|---|---|
| `coverage_gap` | < 10 atoms AND narrow sourcing |
| `representation_inconsistency` | High conflict + high supersession |
| `authority_imbalance` | High authority conflict AND > 60% low-auth atoms |
| `semantic_duplication` | Many atoms + high Jaccard similarity |
| `granularity_too_fine` | Many atoms + low predicate diversity + short objects |
| `missing_schema` | Required predicates absent for detected domain |
| `domain_boundary_collapse` | High entropy + many source prefixes (topic too broad) |
| `semantic_incoherence` | Validation Layer 2 severity > 0.5 |
| `cross_topic_drift` | Validation Layer 3 severity > 0.4 |

### Repair Strategies

| Strategy | When proposed |
|---|---|
| `ingest_missing` | Coverage gap detected |
| `resolve_conflicts` | Representation inconsistency |
| `merge_atoms` | Semantic duplication |
| `introduce_predicates` | Missing schema predicates |
| `reweight_sources` | Authority imbalance |
| `deduplicate` | Semantic duplication (alt) |
| `split_domain` | Domain boundary collapse |
| `restore_atoms` | Entropy collapse after prior repair |
| `manual_review` | Unknown / no automated strategy |

## Still Dormant

| Module | Purpose |
|---|---|
| `graph_v2.py` | Extended graph with richer traversal |
| `graph_enhanced.py` | Extensions to graph_v2 |

# Epistemic System

The epistemic system governs how the KB reasons about the **quality, freshness, authority, and consistency** of its own knowledge. It is what makes this more than a plain database — every atom carries provenance, ages over time, and conflicts are detected automatically.

---

## Components

| Module | Role |
|---|---|
| `knowledge/authority.py` | Source trust weights + effective score re-ranking |
| `knowledge/decay.py` | Confidence decay over time per source half-life |
| `knowledge/contradiction.py` | Conflict detection between mutually exclusive facts |
| `knowledge/epistemic_stress.py` | Composite stress signal at retrieval time |
| `knowledge/working_state.py` | Cross-session goal/topic memory |
| `knowledge/epistemic_adaptation.py` | Adaptive retrieval under sustained stress (Live — wired to `POST /retrieve`) |

---

## Authority

### Purpose

Not all facts are equally reliable. A price from an exchange feed (`exchange_feed_*`) should rank far above a claim from social media (`social_signal_*`), even if both have confidence 0.8. Authority weights encode this prior.

### Weights by Source Prefix

| Source prefix | Authority weight |
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

### `effective_score(atom)`

Used by `retrieval.py` to re-rank the final result set before formatting:

```python
effective_score = atom['confidence'] × authority_weight(atom['source'])
```

Example:
- Exchange price, confidence 0.95 → effective score = **0.95**
- Analyst consensus target, confidence 0.75 → 0.75 × 0.80 = **0.60**
- RSS headline, confidence 0.55 → 0.55 × 0.60 = **0.33**

---

## Decay

### Purpose

Market knowledge goes stale. A price atom from yesterday is worthless; a macro regime call from 3 months ago may still be valid. Decay models this by reducing confidence over time according to source-specific half-lives.

### Half-lives by Source Prefix

| Source prefix | Half-life |
|---|---|
| `exchange_feed_*` | ~10 minutes |
| `model_signal_*` | ~12 hours |
| `social_signal_*` | ~12 hours |
| `technical_*` | ~6 hours |
| `news_wire_*` | ~1 day |
| `earnings_*` | ~30 days |
| `broker_research_*` | ~21 days |
| `alt_data_*` | ~3 days |
| `macro_data_*` | ~60 days |
| `regulatory_filing_*` | ~1 year |
| `curated_*` | ~6 months |

### Decay formula

```
confidence(t) = confidence₀ × exp(−λt)
where λ = ln(2) / half_life
```

### Background worker

`get_decay_worker(db_path)` starts a daemon thread that applies decay to all facts every 24 hours. It logs each decay event to the `decay_log` table.

```python
# Started automatically in api.py:
_decay_worker = get_decay_worker(_DB_PATH)
```

---

## Contradiction Detection

### Purpose

When two atoms share the same `(subject, predicate)` but have different objects, they contradict each other. For example:
- `nvda | signal_direction | long` (confidence 0.65)
- `nvda | signal_direction | short` (confidence 0.60)

### Mechanism

`knowledge/contradiction.py` runs on every `kg.add_fact()` call. When a conflict is detected:

1. The conflict is recorded in the `fact_conflicts` table:
   ```sql
   fact_conflicts (id, fact_id_a, fact_id_b, conflict_type, detected_at)
   ```
2. The new atom is still stored (both coexist — the KB does not automatically resolve conflicts)
3. The older atom is tagged with `superseded_by: <new_id>` in its metadata
4. `supersession_density` in the stress signal reflects how many retrieved atoms have been superseded

### Conflict types

| Type | Condition |
|---|---|
| `direct_contradiction` | Same (subject, predicate), different object |
| `authority_conflict` | Lower-authority source contradicts higher-authority source |

---

## Epistemic Stress

### Purpose

At retrieval time, `compute_stress(atoms, key_terms, conn)` evaluates the quality of the retrieved atom set as a whole. The composite stress score is returned with every `/retrieve` response.

High stress means: treat the retrieved context with more caution, consider refreshing data, or flag uncertainty to the user.

### `StressResult` fields

| Field | Range | Meaning |
|---|---|---|
| `composite_stress` | 0.0–1.0 | Weighted combination of all sub-signals |
| `decay_pressure` | 0.0–1.0 | Fraction of atoms with significantly decayed confidence |
| `authority_conflict` | 0.0–1.0 | Presence of atoms from strongly conflicting authority levels |
| `supersession_density` | 0.0–1.0 | Fraction of retrieved atoms that have been superseded |
| `conflict_cluster` | 0.0–1.0 | Density of the result set in `fact_conflicts` log |
| `domain_entropy` | 0.0–1.0 | Predicate diversity (high = unfocused, low = tight topical retrieval) |

### Composite stress formula

```
composite_stress = (
    0.30 × decay_pressure +
    0.25 × authority_conflict +
    0.20 × supersession_density +
    0.15 × conflict_cluster +
    0.10 × domain_entropy
)
```

### Interpretation

| `composite_stress` | Guidance |
|---|---|
| < 0.15 | Low — context is fresh, coherent, authoritative |
| 0.15–0.35 | Moderate — minor age or authority mix; acceptable |
| 0.35–0.60 | Elevated — notable conflicts or decay; flag to user |
| > 0.60 | High — stale or heavily conflicted; refresh strongly recommended |

### Example

```json
"stress": {
  "composite_stress":     0.093,
  "decay_pressure":       0.0,
  "authority_conflict":   0.025,
  "supersession_density": 0.0,
  "conflict_cluster":     0.0,
  "domain_entropy":       0.3
}
```
→ Very low stress. The retrieved atoms are fresh exchange-feed prices + broker consensus. Safe to use directly.

---

## Working State

### Purpose

`knowledge/working_state.py` provides cross-session memory for the copilot layer — persisting the user's current `goal`, `topic`, and conversation thread between turns.

### API

```python
ws = get_working_state_store(db_path)

# Persist on first turn or when forced:
ws.maybe_persist(session_id, turn_count, goal=goal, topic=topic, force=True)

# Get prior context for injection:
prior_context = ws.format_prior_context(session_id)
```

The formatted prior context is included in `POST /retrieve` responses when `turn_count == 0` (new session resuming prior state):

```json
{
  "snippet": "...",
  "atoms": [...],
  "stress": {...},
  "prior_context": "Prior goal: evaluate tech long book\nPrior topic: technology sector\n..."
}
```

---

## Epistemic Governance Stack

All five modules below are **live and wired** as of the governance integration session. They were built and tested (160 tests passing) before being connected to the live request path. The `architecture.md` module status table reflects their current state.

---

### `epistemic_adaptation.py` — **Live**

Adaptive retrieval engine that adjusts strategy when stress is sustained above threshold across multiple turns.

**Wiring:** `POST /retrieve` → `EpistemicAdaptationEngine` → produces `AdaptationNudges` applied to the next retrieval turn.

**AdaptationNudges rules:**

| Rule | Condition | Effect |
|---|---|---|
| Scope broadening | `domain_entropy < 0.35` + streak ≥ 2 | Broadens DB fetch to all sources |
| Authority filter | `authority_conflict > 0.55` + streak ≥ 2 | Drops atoms below authority cutoff |
| Recency bias | `decay_pressure > 0.50` + streak ≥ 2 | Sorts atoms by timestamp DESC |
| Consolidation mode | streak ≥ 3 | Lowers escalation threshold |
| Domain refresh queue | `decay_pressure > 0.60` + streak ≥ 3 | Dispatches `run_now('yfinance')` |
| KB insufficiency detection | consolidation fires ≥ 5× in 7 days | Triggers classifier + repair proposals |

**Session state:** `GET /adapt/status?session_id=X` · `POST /adapt/reset`

---

### `kb_validation.py` — **Live**

Three-layer governance validation pipeline. Runs as a hook inside `POST /repair/proposals`.

| Layer | What it checks |
|---|---|
| Schema (L1) | Predicate in allowed vocabulary, confidence in [0,1], subject non-empty |
| Semantic (L2) | Cross-predicate consistency (e.g. `signal_direction=long` + `price_target` below current price) |
| Cross-topic (L3) | Atom doesn't introduce cross-topic drift (subject domain consistent with existing KB cluster) |

Violations produce a `governance_verdict` with a confidence penalty applied to the repair proposal score.

---

### `kb_insufficiency_classifier.py` — **Live**

9-rule classifier that fires on `POST /retrieve` (stress ≥ 0.35 or atoms < 8) and on `POST /repair/diagnose`.

| Insufficiency type | Condition |
|---|---|
| `coverage_gap` | < 10 atoms AND narrow sourcing |
| `representation_inconsistency` | High conflict + high supersession |
| `authority_imbalance` | High authority conflict AND > 60% low-auth atoms |
| `semantic_duplication` | Many atoms + high Jaccard similarity |
| `granularity_too_fine` | Many atoms + low predicate diversity + short objects |
| `missing_schema` | Required predicates absent for detected domain |
| `domain_boundary_collapse` | High entropy + many source prefixes |
| `semantic_incoherence` | Validation Layer 2 severity > 0.5 |
| `cross_topic_drift` | Validation Layer 3 severity > 0.4 |

Returns `InsufficiencyDiagnosis { types, signals, confidence }` appended to the `/retrieve` response as `kb_diagnosis`.

---

### `kb_repair_proposals.py` — **Live**

Generates structured repair proposals from an `InsufficiencyDiagnosis`. Each proposal includes:
- `strategy` — one of 9 repair strategies (see table below)
- `preview` — what atoms would be added/removed
- `simulation` — projected stress change if applied
- `validation` — governance verdict from `kb_validation.py`

**Endpoint:** `POST /repair/proposals { topic }`

| Strategy | When proposed |
|---|---|
| `ingest_missing` | Coverage gap |
| `resolve_conflicts` | Representation inconsistency |
| `merge_atoms` | Semantic duplication |
| `introduce_predicates` | Missing schema predicates |
| `reweight_sources` | Authority imbalance |
| `deduplicate` | Semantic duplication (alt) |
| `split_domain` | Domain boundary collapse |
| `restore_atoms` | Entropy collapse after prior repair |
| `manual_review` | Unknown / no automated strategy |

---

### `kb_repair_executor.py` — **Live**

Human-gated atomic execution with auto-rollback and divergence tracking.

**Endpoints:**
- `POST /repair/execute { proposal_id, dry_run=true }` — apply mutations atomically; auto-rollbacks if stress worsens by > 0.05
- `POST /repair/rollback { execution_id }` — revert a prior execution
- `GET /repair/impact { proposal_id }` — preview projected stress change

**Execution flow:**
```
_snapshot_signals() before
→ apply mutations atomically (BEGIN IMMEDIATE)
→ _snapshot_signals() after
→ if stress_after > stress_before + 0.05: auto-rollback
→ write to repair_execution_log + repair_rollback_log
→ return ExecutionResult { before, after, divergence, mutations_applied }
```

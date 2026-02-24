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
| `knowledge/epistemic_adaptation.py` | Adaptive retrieval under sustained stress (built, not yet wired) |

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

## Not-Yet-Wired Epistemic Modules

### `epistemic_adaptation.py`

Adaptive retrieval engine designed to adjust retrieval strategy when stress is sustained above a threshold. Under elevated stress it can:
- Widen the retrieval window
- Prefer higher-authority sources exclusively
- Inject a conflict warning into the snippet

**Status:** Built, not wired to `api.py`.

### `kb_validation.py`

Multi-layer atom validation pipeline that checks atoms against the domain schema (`kb_domain_schemas.py`) before ingestion. Catches predicate typos, subject format violations, confidence range errors.

**Status:** Built, not wired.

### `kb_insufficiency_classifier.py`

Detects KB gaps — when a query is well-formed but the retrieved atoms don't adequately cover it. Classifies the type of insufficiency (missing subject, missing predicate, stale data, etc.).

**Status:** Built, not wired.

### `kb_repair_proposals.py` / `kb_repair_executor.py`

Generates and executes repair plans for KB gaps identified by the insufficiency classifier. Can trigger targeted ingest runs for specific tickers or topics.

**Status:** Built, not wired.

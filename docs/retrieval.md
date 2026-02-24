# Retrieval Engine

## Overview

`retrieval.py` implements a **zero-LLM, pure Python** multi-strategy retrieval engine over the SQLite triple store. It is called by `POST /retrieve` and returns a ranked, formatted snippet plus a raw atom list for epistemic stress computation.

```python
from retrieval import retrieve

snippet, atoms = retrieve(message, conn, limit=30)
```

---

## Strategy Pipeline

Strategies execute in sequence. Each adds atoms to a shared `results` list, deduplicated by `(subject[:60], predicate, object[:60])`. Once an atom is in `seen`, it cannot be added again by later strategies.

**This ordering matters**: higher-priority atoms must be collected before lower-priority ones, or they will be blocked by the dedup set. The FTS skip rule (below) exists specifically to enforce this.

### Strategy 1 — Cross-Asset GNN Atoms
**Trigger:** query contains any of: `compare`, `versus`, `vs`, `correlat`, `relation`, `between`, `and`, `all`, `portfolio`, `cross`, `relative`

Fetches atoms with `source = 'cross_asset_gnn'` ordered by confidence DESC.

*Use case:* "Compare AAPL vs MSFT", "portfolio cross-asset signals"

---

### Strategy 2 — FTS (Full-Text Search)
**Trigger:** `use_fts = True` AND `terms` non-empty

**Skipped when:** the query has **both** explicit tickers AND boosted predicates — because FTS would flood `seen` with low-value `sector`/`last_price` atoms before `price_target`/`signal_direction` atoms get a chance to be collected by strategy 3.

```python
use_fts = not (tickers and boosted_predicates)
```

Uses SQLite FTS5 `facts_fts MATCH` with the first 6 extracted terms OR'd together.

*Use case:* "inflation restrictive fed", "yield curve inversion signal" (no explicit tickers)

---

### Strategy 3 — Predicate Keyword Boost *(intent-aware)*
**Trigger:** `boosted_predicates` non-empty (computed from `_KEYWORD_PREDICATE_BOOST`)

Pre-computed at the start of `retrieve()` before FTS, so the skip decision is available.

When tickers are present: runs `SELECT … WHERE predicate IN (…) AND LOWER(subject) = ?` per ticker.

When no tickers: fetches top-20 atoms globally for the boosted predicate set.

**`_KEYWORD_PREDICATE_BOOST` map:**

| Query keyword | Predicates fetched |
|---|---|
| `upside`, `target`, `consensus`, `analyst` | `price_target`, `signal_direction` |
| `signal`, `direction`, `long`, `short`, `momentum` | `signal_direction`, `signal_confidence` |
| `regime`, `macro` | `regime_label`, `central_bank_stance`, `dominant_driver`, `growth_environment`, `inflation_environment` |
| `inflation` | `inflation_environment`, `dominant_driver`, `regime_label` |
| `rate` | `central_bank_stance`, `dominant_driver` |
| `yield` | `risk_factor`, `dominant_driver` |
| `sector` | `sector` |
| `volatility`, `beta` | `volatility_regime` |
| `catalyst` | `catalyst` |
| `risk` | `risk_factor` |
| `earnings` | `earnings_quality` |

*Use case:* "NVDA META GOOGL upside target" → collects `price_target` and `signal_direction` for each ticker before any bulk fetch

---

### Strategy 4 — Direct Ticker / Subject Match
**Trigger:** tickers extracted from query

Runs per-ticker: `SELECT … WHERE LOWER(subject) LIKE '%ticker%' … LIMIT 6`

Capped at 6 per ticker to prevent flooding `seen` with historical price atoms at the expense of signal atoms.

*Use case:* "What's going on with AAPL?" — fetches all atom types for AAPL

---

### Strategy 5a — High-Value Predicates
**Trigger:** `terms` non-empty

Uses `terms[0]` (first extracted key term) to filter:
```sql
WHERE predicate IN (signal_direction, price_target, catalyst, …)
AND (LOWER(subject) LIKE ? OR LOWER(object) LIKE ?)
LIMIT 12
```

`_HIGH_VALUE_PREDICATES`:
```python
('signal_direction', 'signal_confidence', 'price_target',
 'catalyst', 'invalidation_condition', 'supporting_evidence',
 'contradicting_evidence', 'regime_label', 'risk_factor',
 'entry_condition', 'exit_condition', 'rating', 'key_finding')
```

---

### Strategy 5b — Fallback
**Trigger:** `len(results) < 8`

Fetches top-20 atoms by confidence DESC, excluding noise predicates.

Ensures the response is never empty even for unusual queries.

---

## Re-Ranking

After all strategies, results are re-ranked by `authority.effective_score`:

```python
effective_score(atom) = atom['confidence'] × authority_weight(atom['source'])
```

Authority weights range from 1.0 (`exchange_feed_*`) to 0.35 (`social_signal_*`). See `knowledge/authority.py` for the full table.

Results are then truncated to `limit` (default 30).

---

## Output Formatting

Results are sorted into labelled sections based on predicate and source:

| Section | Condition |
|---|---|
| `[Signals & Positioning]` | `predicate in (signal_direction, signal_confidence, price_target, entry_condition, exit_condition, invalidation_condition)` |
| `[Theses & Evidence]` | `predicate in (premise, supporting_evidence, contradicting_evidence, risk_reward_ratio, position_sizing_note)` |
| `[Macro / Regime]` | `source.startswith('macro_data')` OR `predicate in (regime_label, dominant_driver, central_bank_stance, risk_on_off)` |
| `[Research]` | `source.startswith('broker_research')` OR `predicate in (rating, key_finding, compared_to_consensus)` |
| `[Other]` | Everything else |

Each section is capped:
- `[Signals]` — 10 atoms
- `[Theses]` — 8 atoms
- `[Macro]` — 6 atoms
- `[Research]` — 6 atoms
- `[Other]` — 6 atoms

**Example output:**
```
=== TRADING KNOWLEDGE CONTEXT ===
[Signals & Positioning]
  nvda | price_target | 253.99
  nvda | signal_direction | long
  meta | price_target | 861.3
  meta | signal_direction | long
[Macro / Regime]
  us_macro | central_bank_stance | neutral_to_restrictive
  us_macro | regime_label | tight policy
[Other]
  nvda | sector | technology
  nvda | last_price | 190.4
```

---

## Noise Predicates

These predicates are **never returned** to callers:

```python
_NOISE_PREDICATES = {
    'source_code', 'has_title', 'has_section', 'has_content'
}
```

---

## Term & Ticker Extraction

### `_extract_key_terms(message)`
- `re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)` — at least 2 chars
- Lowercased, stopword-filtered (`_STOPWORDS` set of ~40 words)
- Deduplicated, order-preserving

### `_extract_tickers(message)`
- `re.findall(r'\b[A-Z]{2,5}\b', message)` — 2–5 uppercase chars
- Filtered against `_UPPERCASE_STOPWORDS` (~40 common uppercase abbreviations)

---

## Extending the Engine

### Adding a new predicate to the boost map

```python
# In retrieval.py, _KEYWORD_PREDICATE_BOOST:
'dividend': ('dividend_yield', 'payout_ratio'),
```

Any query containing "dividend" will now fetch `dividend_yield` and `payout_ratio` atoms for matched tickers.

### Adding a new output section

In the formatting block at the bottom of `retrieve()`:
```python
dividends = []
for r in results:
    pred = r['predicate']
    if pred in ('dividend_yield', 'payout_ratio'):
        dividends.append(r)
# ... then add to output:
if dividends:
    lines.append('[Dividends & Income]')
    lines.extend(_fmt(r) for r in dividends[:6])
```

### Adjusting per-strategy limits

- Strategy 3 (boost): `LIMIT 10` per ticker — increase for broader context
- Strategy 4 (ticker match): `LIMIT 6` per ticker — keep low to avoid price atom flooding
- Strategy 5a (high-value): `LIMIT 12` — safe to increase
- Global `limit=30` — passed to `retrieve()` from `api.py`

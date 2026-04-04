# Performance Benchmarks

Measured on OCI VM (1 OCPU / 1 GB RAM, Oracle Linux) against live production KB.
All timings are wall-clock Python `time.perf_counter()`, single-threaded, no warm-up.
KB state at measurement: **13,633 facts · 1,781 unique subjects**.

Last run: 2026-03-08

---

## retrieval.retrieve()

Multi-strategy KB retrieval (geo fetch → graph traversal → FTS → ticker pinned → predicate boost → fallback).
Includes inline decay re-ranking pass.

| Query | Atoms returned | Time (ms) |
|---|---|---|
| `what is the signal for BP.L?` | 12 | 31.8 |
| `explain the impact of a Fed rate hike` | 30 | 28.0 |
| `what do you know about NVDA?` | 29 | 29.4 |
| `compare SHEL.L and BP.L` | 34 | 28.9 |
| `geopolitical risk in Middle East` | 30 | 27.7 |

| Metric | Value |
|---|---|
| p50 | **28.9 ms** |
| p95 | **31.8 ms** |
| max | **31.8 ms** |

Budget: ≤ 50 ms. ✅

---

## causal_graph.traverse_causal()

BFS traversal of causal graph. Depth=4, min_confidence=0.5.

| Seed | Hops | Time (ms) |
|---|---|---|
| `fed_rate_cut` | 14 | 64.7 |
| `fed_rate_hike` | 13 | 58.8 |
| `energy_prices_rise` | 15 | 68.6 |
| `risk_off_rotation` | 1 | 4.5 |
| `dollar_strengthens` | 2 | 9.4 |

| Metric | Value |
|---|---|
| p50 | **58.8 ms** |
| max | **68.6 ms** |

Note: seeds with shallow graphs (≤2 hops) complete in <10 ms.
High-hop seeds (14–15 hops) peak at ~69 ms. Budget: ≤ 100 ms. ✅

---

## scenario_engine.run_scenario() — narrative=False

Includes seed resolution + causal traversal + portfolio impact filtering.
LLM narrative excluded (adds ~1–2 s Groq, ~5–10 s Ollama).

| Shock | Chain hops | Conf (geom) | Time (ms) |
|---|---|---|---|
| `fed rate cut` | 14 | 0.816 | 64.6 |
| `oil spike` | 15 | 0.807 | 69.2 |
| `risk off` | 1 | 0.800 | 5.4 |
| `dollar strengthens` | 2 | 0.775 | 10.1 |
| `boe cut` | 0 | 1.000 | 0.6 |

| Metric | Value |
|---|---|
| p50 | **10.1 ms** |
| max | **69.2 ms** |

Budget: ≤ 100 ms (dry-run), ≤ 10 s (narrative=True with Groq). ✅

---

## graph_retrieval.build_graph_context()

PageRank + degree centrality + BFS concept paths over 200 atoms.

| Input atoms | Output chars | Time (ms) |
|---|---|---|
| 200 | 5,984 | 5.1 |

Budget: ≤ 20 ms. ✅

---

## Summary

| Component | p50 | max | Budget |
|---|---|---|---|
| `retrieve()` | 28.9 ms | 31.8 ms | 50 ms ✅ |
| `traverse_causal()` | 58.8 ms | 68.6 ms | 100 ms ✅ |
| `run_scenario()` dry-run | 10.1 ms | 69.2 ms | 100 ms ✅ |
| `build_graph_context()` | 5.1 ms | 5.1 ms | 20 ms ✅ |

All zero-LLM paths complete well within budget.
Full chat round-trip (retrieve + build_prompt + LLM) adds ~1–2 s Groq latency.

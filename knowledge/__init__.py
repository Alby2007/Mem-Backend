from .graph import TradingKnowledgeGraph as KnowledgeGraph

__all__ = ['KnowledgeGraph']

# ── Module status ──────────────────────────────────────────────────────────────
# Status taxonomy:
#   LIVE    — imported and executed in startup/request path
#   PARTIAL — wired for schema/API visibility but not yet feeding downstream decision logic
#   DORMANT — file exists but not wired into live runtime path
#
# LIVE (active in the request path):
#   graph.py              — core triple store (WAL SQLite)
#   authority.py          — source trust weights + effective_score re-ranking
#   decay.py              — confidence decay + 24h background worker
#   contradiction.py      — conflict detection + fact_conflicts audit log
#   epistemic_stress.py   — composite stress signals on /retrieve
#   epistemic_adaptation.py — adaptive retrieval under sustained stress
#   working_state.py      — cross-session goal/topic/thread memory
#   kb_domain_schemas.py  — predicate ontology (reference only, not imported)
#   graph_retrieval.py    — PageRank / BFS / cluster traversal (strategy 0 in retrieval.py)
#   causal_graph.py       — causal edge table init + traversal + causal-edge APIs
#   kb_insufficiency_classifier.py — KB gap detection (wired into /retrieve + /repair/diagnose)
#   kb_validation.py      — governance validation (wired into /repair/proposals)
#   kb_repair_proposals.py — repair proposal generator (wired into /repair/proposals)
#   kb_repair_executor.py  — repair execution engine (wired into /repair/execute|rollback|impact)
#
# PARTIAL (wired, but integration not complete):
#   confidence_intervals.py — conf_n/conf_var schema + /kb/confidence API are live;
#                             interval output does not yet feed position_size_pct (planned v2)
#
# DORMANT (not wired):
#   graph_v2.py           — async graph with versioning (requires aiosqlite)
#   graph_enhanced.py     — sync graph with taxonomy system (separate DB schema)

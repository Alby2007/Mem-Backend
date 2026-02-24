from .graph import TradingKnowledgeGraph as KnowledgeGraph

__all__ = ['KnowledgeGraph']

# ── Module status ──────────────────────────────────────────────────────────────
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
#   kb_insufficiency_classifier.py — KB gap detection (wired into /retrieve + /repair/diagnose)
#   kb_validation.py      — governance validation (wired into /repair/proposals)
#   kb_repair_proposals.py — repair proposal generator (wired into /repair/proposals)
#   kb_repair_executor.py  — repair execution engine (wired into /repair/execute|rollback|impact)
#   epistemic_adaptation.py — AdaptationNudges (wired into /retrieve streak tracker)
#                             rule 1: scope broadening (low entropy)
#                             rule 2: authority filter (high conflict)
#                             rule 3: recency bias (high decay)
#                             rule 4: consolidation mode (streak >= 3)
#                             rule 5+6: queued actions + KB insufficiency detection
#
# NOT YET WIRED (JARVIS-specific, incompatible schema or requires aiosqlite):
#   graph_v2.py           — async graph with versioning (requires aiosqlite)
#   graph_enhanced.py     — sync graph with taxonomy system (separate DB schema)

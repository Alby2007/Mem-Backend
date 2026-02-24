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
#   kb_insufficiency_classifier.py — KB gap detection (wired into /retrieve)
#   kb_validation.py      — governance validation (wired into /repair endpoints)
#   kb_repair_proposals.py — repair proposal generator (wired into /repair/proposals)
#   kb_repair_executor.py  — repair execution engine (wired into /repair/execute)
#
# NOT YET WIRED:
#   graph_v2.py           — extended graph with richer traversal
#   graph_enhanced.py     — graph_v2 extensions

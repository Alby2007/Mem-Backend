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
#
# NOT YET WIRED (built, under review):
#   graph_v2.py           — extended graph with richer traversal
#   graph_enhanced.py     — graph_v2 extensions
#   graph_retrieval.py    — PageRank / BFS / cluster traversal
#   kb_validation.py      — atom validation layers
#   kb_insufficiency_classifier.py — KB gap detection
#   kb_repair_proposals.py — repair suggestion engine
#   kb_repair_executor.py  — repair execution engine

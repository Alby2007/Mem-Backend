import sys; sys.path.insert(0, '.')
from knowledge.graph import TradingKnowledgeGraph
from retrieval import retrieve

kg = TradingKnowledgeGraph('trading_knowledge.db')
conn = kg.thread_local_conn()

INV_PREDS = {'invalidation_price','invalidation_distance','thesis_risk_level'}

for q in [
    "NVDA is long with signal_quality confirmed. What is the invalidation_price, invalidation_distance, and thesis_risk_level?",
    "Compare META and MSFT on thesis_risk_level, invalidation_distance, and upside_pct.",
    "Which tickers in the KB have thesis_risk_level of tight?",
]:
    snippet, atoms = retrieve(q, conn, limit=50)
    inv = [a for a in atoms if a.get('predicate') in INV_PREDS]
    print(f"\nQ: {q[:80]}")
    print(f"  total={len(atoms)}  invalidation_atoms={len(inv)}")
    for a in inv:
        print(f"    {a['subject']:6s} | {a['predicate']:25s} | {a['object']}")

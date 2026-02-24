import sys, sqlite3; sys.path.insert(0, '.')
from knowledge.graph import TradingKnowledgeGraph
from retrieval import retrieve

kg = TradingKnowledgeGraph('trading_knowledge.db')
conn = kg.thread_local_conn()

q = "Compare META and MSFT on thesis_risk_level, invalidation_distance, and upside_pct."
snippet, atoms = retrieve(q, conn, limit=50)
inv_preds = {'invalidation_price','invalidation_distance','thesis_risk_level','upside_pct','signal_quality'}
print(f"total={len(atoms)}")
for a in atoms:
    if a['subject'].lower() in ('meta','msft') or a.get('predicate') in inv_preds:
        print(f"  {a['subject']:6s} | {a['predicate']:25s} | {a['object']}")

# Also check what's in DB for these tickers
print("\n-- DB check --")
c = sqlite3.connect('trading_knowledge.db').cursor()
for ticker in ['meta','msft']:
    c.execute("SELECT predicate, object FROM facts WHERE subject=? AND predicate IN ('thesis_risk_level','invalidation_distance','invalidation_price','upside_pct','signal_quality')", (ticker,))
    for r in c.fetchall():
        print(f"  {ticker.upper():5s} | {r[0]:25s} | {r[1]}")

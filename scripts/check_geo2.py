import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

print("=== GEO PREDICATES ===")
c.execute("""SELECT predicate, COUNT(*) FROM facts
WHERE predicate LIKE '%geo%' OR predicate LIKE '%regime%'
   OR predicate LIKE '%tension%' OR predicate LIKE '%conflict%'
   OR predicate LIKE '%macro%' OR predicate LIKE '%war%'
   OR predicate LIKE '%sanction%' OR predicate LIKE '%political%'
GROUP BY predicate ORDER BY COUNT(*) DESC LIMIT 20""")
for r in c.fetchall(): print(r)

print("\n=== MACRO/REGIME SUBJECTS ===")
c.execute("""SELECT subject, predicate, object FROM facts
WHERE subject IN ('macro_regime','market','us_macro','fed','ecb','global_macro')
LIMIT 20""")
for r in c.fetchall(): print(r)

print("\n=== NEWS WIRE SUBJECTS (all) ===")
c.execute("SELECT DISTINCT subject FROM facts WHERE subject LIKE 'news_wire%'")
for r in c.fetchall(): print(r[0])

print("\n=== INGEST LOG (last 10) ===")
try:
    c.execute("SELECT adapter, last_run_at, atoms_written FROM ingest_log ORDER BY last_run_at DESC LIMIT 10")
    for r in c.fetchall(): print(r)
except Exception as e:
    print("No ingest_log:", e)

print("\n=== RETRIEVAL TEST: query='geopolitical tension' ===")
import sys, os
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
try:
    from retrieval import retrieve
    atoms = retrieve("geopolitical tension world monitor", db_path=DB)
    print(f"Atoms returned: {len(atoms)}")
    for a in atoms[:10]: print(a)
except Exception as e:
    print("Retrieval error:", e)

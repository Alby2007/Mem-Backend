"""Check what geopolitical/world monitor data is in the KB."""
import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)

print("=== Geopolitical/tension predicates ===")
rows = conn.execute("""
    SELECT subject, predicate, value FROM facts
    WHERE predicate LIKE '%tension%'
       OR predicate LIKE '%geopolit%'
       OR predicate LIKE '%gdelt%'
       OR predicate LIKE '%conflict%'
       OR predicate LIKE '%bilateral%'
       OR subject LIKE '%tension%'
    LIMIT 30
""").fetchall()
for r in rows:
    print(r)

print(f"\nTotal matching: {len(rows)}")

print("\n=== Sources in facts ===")
sources = conn.execute("""
    SELECT source, COUNT(*) as cnt FROM facts
    GROUP BY source ORDER BY cnt DESC LIMIT 20
""").fetchall()
for s in sources:
    print(s)

print("\n=== Sample GDELT atoms ===")
gdelt = conn.execute("""
    SELECT subject, predicate, value FROM facts
    WHERE source LIKE '%gdelt%' OR source LIKE '%tension%'
    LIMIT 15
""").fetchall()
for r in gdelt:
    print(r)

conn.close()

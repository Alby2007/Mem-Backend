"""Check what geopolitical/world monitor data is in the KB."""
import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)

print("=== facts table columns ===")
cols = [d[1] for d in conn.execute('PRAGMA table_info(facts)').fetchall()]
print(cols)

print("\n=== Sources in facts (top 25) ===")
for row in conn.execute("SELECT source, COUNT(*) FROM facts GROUP BY source ORDER BY COUNT(*) DESC LIMIT 25").fetchall():
    print(row)

print("\n=== Geo subjects (gdelt/acled/ucdp/seismic) ===")
for row in conn.execute("""
    SELECT subject, predicate, object FROM facts
    WHERE subject IN ('gdelt_tension','acled_unrest','geo_exposure','ucdp_conflict','usgs_seismic')
    LIMIT 30
""").fetchall():
    print(row)

print("\n=== Geo predicate search ===")
for row in conn.execute("""
    SELECT subject, predicate, object FROM facts
    WHERE predicate LIKE '%tension%'
       OR predicate LIKE '%geopolit%'
       OR predicate LIKE '%conflict%'
       OR predicate LIKE '%bilateral%'
       OR predicate LIKE '%unrest%'
       OR predicate LIKE '%seismic%'
    LIMIT 20
""").fetchall():
    print(row)

print("\n=== world monitor retrieval test ===")
import sys, os
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')
try:
    from retrieval import retrieve
    result = retrieve('geopolitical tensions world monitor', conn, limit=5)
    atoms = result.get('atom_count', 0) if isinstance(result, dict) else '?'
    snippet = result.get('snippet', '')[:200] if isinstance(result, dict) else str(result)[:200]
    print(f"atoms={atoms}  snippet={snippet!r}")
except Exception as e:
    print(f"retrieval error: {e}")

conn.close()

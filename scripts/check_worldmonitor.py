"""Check what geopolitical/world monitor data is in the KB."""
import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)

print("=== facts table schema ===")
cols = [d[1] for d in conn.execute('PRAGMA table_info(facts)').fetchall()]
print(cols)

# Build column references dynamically
# Standard KB schema: ticker, predicate, object (or atom / content)
# Try to detect which columns exist
col_set = set(cols)
subj_col = 'ticker' if 'ticker' in col_set else cols[0]
pred_col = 'predicate' if 'predicate' in col_set else cols[1]
obj_col  = 'object' if 'object' in col_set else ('atom' if 'atom' in col_set else cols[2])

print(f"Using: subject={subj_col}, predicate={pred_col}, value={obj_col}\n")

print("=== Sources in facts (top 20) ===")
try:
    sources = conn.execute("SELECT source, COUNT(*) as cnt FROM facts GROUP BY source ORDER BY cnt DESC LIMIT 20").fetchall()
    for s in sources:
        print(s)
except Exception as e:
    print(f"No source column: {e}")

print("\n=== Geopolitical/tension atoms ===")
q = f"""
    SELECT {subj_col}, {pred_col}, {obj_col} FROM facts
    WHERE {pred_col} LIKE '%tension%'
       OR {pred_col} LIKE '%geopolit%'
       OR {pred_col} LIKE '%gdelt%'
       OR {pred_col} LIKE '%conflict%'
       OR {pred_col} LIKE '%bilateral%'
       OR {subj_col} LIKE '%tension%'
       OR {subj_col} LIKE '%world%'
    LIMIT 30
"""
rows = conn.execute(q).fetchall()
for r in rows:
    print(r)
print(f"Total: {len(rows)}")

print("\n=== GDELT source atoms ===")
try:
    gdelt = conn.execute(f"SELECT {subj_col}, {pred_col}, {obj_col} FROM facts WHERE source LIKE '%gdelt%' LIMIT 15").fetchall()
    for r in gdelt:
        print(r)
except Exception as e:
    print(f"Error: {e}")

conn.close()

import sqlite3, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

db = 'trading_knowledge.db'
conn = sqlite3.connect(db)

print("=== Gold/Silver subjects in KB ===")
rows = conn.execute(
    "SELECT DISTINCT subject FROM facts WHERE "
    "subject LIKE '%xau%' OR subject LIKE '%XAU%' OR "
    "subject LIKE '%gold%' OR subject LIKE '%GOLD%' OR "
    "subject LIKE '%xag%' OR subject LIKE '%XAG%' OR "
    "subject LIKE '%silver%' OR subject LIKE '%SILVER%' OR "
    "subject LIKE '%GLD%' OR subject LIKE '%SLV%'"
).fetchall()
print(rows)

print("\n=== All unique subjects (sample) ===")
rows2 = conn.execute("SELECT DISTINCT subject FROM facts ORDER BY subject LIMIT 60").fetchall()
for r in rows2:
    print(r[0])

print("\n=== Ingest ticker config ===")
try:
    rows3 = conn.execute("SELECT * FROM ingest_config LIMIT 30").fetchall()
    for r in rows3: print(r)
except Exception as e:
    print("No ingest_config table:", e)

conn.close()

"""Check what geo/world-monitor sources and timestamp coverage exist in the KB."""
import sqlite3, os, sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.environ.setdefault('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
db = os.environ['TRADING_KB_DB']
conn = sqlite3.connect(db)

print("=== GEO SOURCES ===")
rows = conn.execute(
    "SELECT DISTINCT source FROM facts WHERE "
    "source LIKE '%world%' OR source LIKE '%monitor%' OR "
    "source LIKE '%gdelt%' OR source LIKE '%acled%' OR source LIKE '%ucdp%' "
    "OR source LIKE '%news_wire%' OR source LIKE '%geopolitical%'"
).fetchall()
for r in rows:
    print(f"  {r[0]}")

print("\n=== TIMESTAMP COLUMN EXISTS? ===")
cols = conn.execute("PRAGMA table_info(facts)").fetchall()
for c in cols:
    print(f"  {c[1]} ({c[2]})")

print("\n=== SAMPLE TIMESTAMPS FOR GEO ATOMS ===")
rows = conn.execute(
    "SELECT subject, predicate, timestamp, SUBSTR(object,1,50) FROM facts "
    "WHERE (source LIKE '%news_wire%' OR source LIKE '%gdelt%' OR source LIKE '%ucdp%') "
    "AND predicate IN ('key_finding','headline','catalyst','conflict_status') "
    "ORDER BY timestamp DESC LIMIT 10"
).fetchall()
for r in rows:
    print(f"  {r[0]}|{r[1]}|ts={r[2]}|{r[3]}")

print("\n=== COUNT GEO ATOMS BY SOURCE ===")
rows = conn.execute(
    "SELECT source, COUNT(*) FROM facts WHERE "
    "source LIKE '%news_wire%' OR source LIKE '%gdelt%' OR "
    "source LIKE '%acled%' OR source LIKE '%ucdp%' OR source LIKE '%geopolitical%' "
    "GROUP BY source ORDER BY COUNT(*) DESC"
).fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

print("\n=== RUSSIA ATOMS WITH TIMESTAMPS ===")
rows = conn.execute(
    "SELECT subject, predicate, timestamp, SUBSTR(object,1,60) FROM facts "
    "WHERE LOWER(object) LIKE '%russia%' "
    "AND predicate IN ('key_finding','headline','catalyst','risk_factor','summary') "
    "ORDER BY timestamp DESC LIMIT 15"
).fetchall()
for r in rows:
    print(f"  {r[0]}|{r[1]}|ts={r[2]}|{r[3]}")
conn.close()

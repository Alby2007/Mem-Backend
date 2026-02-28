"""Quick KB diagnostic — run on OCI server."""
import sqlite3, sys

db = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(db)

# Check gold-related subjects
print("=== gold/xau/platinum subjects ===")
rows = c.execute(
    "SELECT DISTINCT subject FROM facts WHERE subject LIKE '%gld%' OR subject LIKE '%xau%' OR subject LIKE '%platinum%' OR subject LIKE '%gc%' LIMIT 20"
).fetchall()
print(rows)

# Check GLD atoms
print("\n=== GLD atoms ===")
rows2 = c.execute("SELECT subject, predicate, object FROM facts WHERE subject='gld' LIMIT 10").fetchall()
for r in rows2: print(r)

# Check alias table
print("\n=== alias table ===")
try:
    rows3 = c.execute("SELECT * FROM ticker_aliases WHERE alias LIKE '%gold%' OR canonical LIKE '%gold%' OR alias='gld' LIMIT 10").fetchall()
    print(rows3)
except Exception as e:
    print("no alias table:", e)

# Check retrieval for 'gold market'
print("\n=== subjects matching 'gold' ===")
rows4 = c.execute(
    "SELECT DISTINCT subject FROM facts WHERE object LIKE '%gold%' OR subject LIKE '%gold%' LIMIT 20"
).fetchall()
print(rows4)

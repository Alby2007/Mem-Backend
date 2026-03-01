"""
Delete stale/wrong atoms that were graduated from old LLM responses into the KB.
These were created when the LLM said 'no Russia data' and the conversation atom
extractor wrote misleading facts like 'russia|war|(empty)', 'war in russia|has_data|false'.
"""
import os, sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.environ.setdefault('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')

db = os.environ['TRADING_KB_DB']
conn = sqlite3.connect(db)

# Find and delete stale atoms from conversation graduation
stale_queries = [
    "DELETE FROM facts WHERE subject='russia' AND predicate='war' AND (object='' OR object IS NULL)",
    "DELETE FROM facts WHERE subject='russia' AND predicate=''",
    "DELETE FROM facts WHERE subject='war in russia' AND predicate='has_data'",
    "DELETE FROM facts WHERE subject LIKE '%war in russia%'",
    "DELETE FROM facts WHERE source='conversation' AND subject IN ('russia','ukraine','iran','war','conflict') AND (object='' OR object IS NULL OR object='false' OR object='true' OR object='unknown')",
]

total = 0
for q in stale_queries:
    try:
        c = conn.execute(q)
        if c.rowcount > 0:
            print(f"Deleted {c.rowcount} rows: {q[:80]}")
            total += c.rowcount
    except Exception as e:
        print(f"Error on {q[:60]}: {e}")

conn.commit()
conn.close()
print(f"\nTotal deleted: {total} stale atoms")

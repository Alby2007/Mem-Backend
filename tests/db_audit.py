"""tests/db_audit.py — quick DB state audit"""
import sqlite3
from collections import defaultdict

DB = 'trading_knowledge.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# Total facts
c.execute("SELECT COUNT(*) FROM facts")
print(f"Total facts in DB: {c.fetchone()[0]}")

# All distinct subjects
c.execute("SELECT DISTINCT subject FROM facts ORDER BY subject LIMIT 80")
subjects = [r[0] for r in c.fetchall()]
print(f"\nAll distinct subjects ({len(subjects)}):")
for s in subjects:
    print(f"  '{s}'")

# Predicates available
c.execute("SELECT predicate, COUNT(*) FROM facts GROUP BY predicate ORDER BY COUNT(*) DESC")
print("\nPredicate counts:")
for r in c.fetchall():
    print(f"  {r[1]:5d}  {r[0]}")

# Sample 10 most recent atoms
c.execute("SELECT subject, predicate, object, source, timestamp FROM facts ORDER BY rowid DESC LIMIT 15")
print("\nMost recent 15 atoms:")
for r in c.fetchall():
    print(f"  {r[0]:12s} | {r[1]:20s} | {r[2][:30]:30s} | {r[4][:19]}")

conn.close()

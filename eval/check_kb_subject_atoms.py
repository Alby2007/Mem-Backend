import sqlite3
conn = sqlite3.connect('trading_knowledge.db')
rows = conn.execute(
    "SELECT subject, predicate, object, source FROM facts "
    "WHERE LOWER(subject) = 'kb' LIMIT 30"
).fetchall()
print(f"{len(rows)} atoms with subject='kb':")
for r in rows:
    print(f"  {r[0]} | {r[1]} | {str(r[2])[:60]} | src={r[3]}")
conn.close()

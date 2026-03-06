import sqlite3

conn = sqlite3.connect('trading_knowledge.db')
rows = conn.execute(
    "SELECT subject, predicate, object FROM facts "
    "WHERE LOWER(subject) IN ('madeupticker','fakeco','notreal99','randomticker123','blobcorp99') "
    "LIMIT 30"
).fetchall()
print(f"{len(rows)} fake atoms currently in KB:")
for r in rows:
    print(f"  {r[0]} | {r[1]} | {str(r[2])[:60]}")
conn.close()

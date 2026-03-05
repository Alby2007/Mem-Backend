import sqlite3

db = sqlite3.connect('/opt/trading-galaxy/data/trading_knowledge.db')
c = db.cursor()

print('=== Atoms containing 36.46 or 28.4 ===')
rows = c.execute(
    "SELECT subject, predicate, object FROM facts WHERE object LIKE '%36.46%' OR object LIKE '%28.4%'"
).fetchall()
for r in rows:
    print(r)

print()
print('=== All atoms with subject=kb ===')
rows2 = c.execute(
    "SELECT subject, predicate, object FROM facts WHERE subject='kb' LIMIT 30"
).fetchall()
for r in rows2:
    print(r)

print()
print('=== Predicates that look like portfolio-level metrics ===')
rows3 = c.execute(
    "SELECT DISTINCT predicate FROM facts WHERE predicate LIKE '%portfolio%' OR predicate LIKE '%volatil%' OR predicate LIKE '%return%'"
).fetchall()
for r in rows3:
    print(r)

db.close()

import sqlite3, json

c = sqlite3.connect('/home/ubuntu/trading-galaxy/trading_knowledge.db')

print("=== War/conflict atoms ===")
rows = c.execute("""
    SELECT subject, predicate, object, timestamp FROM facts
    WHERE subject LIKE '%war%' OR subject LIKE '%conflict%' OR subject LIKE '%geopolit%'
       OR object LIKE '%war%' OR object LIKE '%conflict%' OR object LIKE '%ukraine%'
       OR object LIKE '%russia%' OR object LIKE '%israel%' OR object LIKE '%gaza%'
       OR subject LIKE '%ukraine%' OR subject LIKE '%russia%' OR subject LIKE '%gdelt%'
    ORDER BY timestamp DESC LIMIT 30
""").fetchall()
for r in rows:
    print(r)

print("\n=== GDELT adapter atoms (last 10) ===")
rows2 = c.execute("""
    SELECT subject, predicate, object, timestamp FROM facts
    WHERE source = 'gdelt_tension' OR source LIKE '%gdelt%'
    ORDER BY timestamp DESC LIMIT 10
""").fetchall()
for r in rows2:
    print(r)

print("\n=== ucpd_conflict atoms (last 10) ===")
rows3 = c.execute("""
    SELECT subject, predicate, object, timestamp FROM facts
    WHERE source = 'ucpd_conflict' OR source LIKE '%conflict%'
    ORDER BY timestamp DESC LIMIT 10
""").fetchall()
for r in rows3:
    print(r)

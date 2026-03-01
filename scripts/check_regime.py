import sqlite3
db = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(db)
# Check market_regime predicate
rows = c.execute(
    "SELECT subject, predicate, object, timestamp FROM facts "
    "WHERE predicate='market_regime' ORDER BY timestamp DESC LIMIT 5"
).fetchall()
print("=== market_regime rows ===")
for r in rows:
    print(r)

# Also check what distinct predicates exist for subject='market'
rows2 = c.execute(
    "SELECT predicate, object, timestamp FROM facts "
    "WHERE subject='market' ORDER BY timestamp DESC LIMIT 10"
).fetchall()
print("\n=== subject='market' rows ===")
for r in rows2:
    print(r)

# Also check regime_label
rows3 = c.execute(
    "SELECT subject, predicate, object, timestamp FROM facts "
    "WHERE predicate IN ('regime_label','current_regime') ORDER BY timestamp DESC LIMIT 5"
).fetchall()
print("\n=== regime_label / current_regime ===")
for r in rows3:
    print(r)

c.close()

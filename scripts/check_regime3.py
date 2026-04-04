import sqlite3, json, urllib.request

# 1. Check facts table schema
db = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(db)
cols = [r[1] for r in c.execute("PRAGMA table_info(facts)").fetchall()]
print("facts columns:", cols)

# 2. Try exact query the /stats endpoint now uses
try:
    row = c.execute(
        "SELECT object FROM facts WHERE subject = 'market' AND predicate = 'market_regime' ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    print("stats query result:", row)
except Exception as e:
    print("stats query FAILED:", e)

# 3. Check the live /stats API response
try:
    r = urllib.request.urlopen('http://localhost:5050/stats', timeout=5)
    d = json.loads(r.read())
    print("API market_regime:", d.get('market_regime'))
except Exception as e:
    print("API call failed:", e)

c.close()

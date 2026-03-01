import sqlite3, json, urllib.request

# Check the DB directly
c = sqlite3.connect('/opt/trading-galaxy/data/trading_knowledge.db')

print("=== Market regime atoms ===")
rows = c.execute("""
    SELECT subject, predicate, object, source, confidence, timestamp
    FROM facts WHERE predicate='market_regime'
    ORDER BY timestamp DESC LIMIT 5
""").fetchall()
for r in rows:
    print(r)

print("\n=== /stats API response ===")
try:
    req = urllib.request.Request('http://localhost:5050/stats',
        headers={'Origin': 'https://app.trading-galaxy.uk'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    print("market_regime:", data.get('market_regime'))
    print("kb_size:", data.get('kb_size'))
except Exception as e:
    print("ERROR:", e)

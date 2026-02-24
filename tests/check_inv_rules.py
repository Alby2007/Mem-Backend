import sys, sqlite3
sys.path.insert(0, '.')

conn = sqlite3.connect('trading_knowledge.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

# Read the metadata stored in the invalidation atoms to see which rule fired
for ticker in ['meta', 'msft', 'nvda', 'intc']:
    c.execute("SELECT predicate, object FROM facts WHERE subject=? AND predicate IN ('last_price','low_52w','volatility_30d','invalidation_price','invalidation_distance','thesis_risk_level','price_regime')", (ticker,))
    d = {r['predicate']: r['object'] for r in c.fetchall()}
    print(f"{ticker.upper():5s}: last={d.get('last_price','?'):8s} low_52w={d.get('low_52w','?'):8s} vol30d={d.get('volatility_30d','?'):6s} "
          f"inv_price={d.get('invalidation_price','?'):8s} inv_dist={d.get('invalidation_distance','?'):8s} "
          f"price_regime={d.get('price_regime','?'):15s} risk={d.get('thesis_risk_level','?')}")

conn.close()

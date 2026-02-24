"""Debug: show CT rule inputs for key tickers."""
import sys, sqlite3; sys.path.insert(0, '.')

c = sqlite3.connect('trading_knowledge.db').cursor()
tickers = ['nvda', 'intc', 'meta', 'aapl', 'amd', 'msft', 'avgo']
preds = ('signal_quality', 'thesis_risk_level', 'macro_confirmation',
         'volatility_30d', 'conviction_tier', 'position_size_pct', 'volatility_scalar')

for ticker in tickers:
    c.execute(
        f"SELECT predicate, object FROM facts WHERE subject=? AND predicate IN {preds}",
        (ticker,)
    )
    rows = {r[0]: r[1] for r in c.fetchall()}
    print(f"\n{ticker.upper()}")
    for p in preds:
        print(f"  {p:25s}: {rows.get(p, 'MISSING')}")

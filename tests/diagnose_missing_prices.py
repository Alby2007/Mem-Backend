"""
tests/diagnose_missing_prices.py — diagnose which tickers are missing last_price atoms
and do a live yfinance probe to find why META/GOOGL aren't landing.
"""
import sqlite3, json, sys
sys.path.insert(0, '.')

DB = 'trading_knowledge.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

WATCHLIST = ['AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','AVGO',
             'JPM','V','MA','UNH','XOM','CVX','SPY','QQQ','XLF','XLE','XLK','TLT','HYG']

print("=== live atoms in KB (deleted=0) ===")
c.execute("""
    SELECT subject, predicate, object, source, timestamp
    FROM facts
    WHERE subject IN ({})
    ORDER BY subject, predicate
""".format(','.join('?'*len(WATCHLIST))), WATCHLIST)
rows = c.fetchall()
from collections import defaultdict
by_ticker = defaultdict(list)
for r in rows:
    by_ticker[r[0]].append((r[1], r[2], r[3][:40], r[4][:19]))

for ticker in WATCHLIST:
    atoms = by_ticker.get(ticker, [])
    predicates = [a[0] for a in atoms]
    has_price  = 'last_price'  in predicates
    has_target = 'price_target' in predicates
    price_val  = next((a[1] for a in atoms if a[0]=='last_price'), 'MISSING')
    target_val = next((a[1] for a in atoms if a[0]=='price_target'), 'MISSING')
    flag = '' if has_price else ' ← NO PRICE'
    print(f"  {ticker:6s}  last_price={price_val:12s}  price_target={target_val:10s}  total_atoms={len(atoms)}{flag}")

conn.close()

print()
print("=== live yfinance probe for META and GOOGL ===")
try:
    import yfinance as yf
    for sym in ['META', 'GOOGL', 'NVDA']:
        tk = yf.Ticker(sym)
        try:
            info = tk.info or {}
        except Exception as e:
            print(f"  {sym}: info() threw {e}")
            continue
        nav     = info.get('navPrice')
        current = info.get('currentPrice')
        regular = info.get('regularMarketPrice')
        target  = info.get('targetMeanPrice')
        qt      = info.get('quoteType','?')
        print(f"  {sym}: quoteType={qt}  navPrice={nav}  currentPrice={current}  regularMarketPrice={regular}  targetMeanPrice={target}")
except ImportError:
    print("  yfinance not installed")

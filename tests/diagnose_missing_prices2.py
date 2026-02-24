"""tests/diagnose_missing_prices2.py — audit with correct lowercase subjects"""
import sqlite3
from collections import defaultdict

DB = 'trading_knowledge.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

WATCHLIST = ['aapl','msft','googl','amzn','nvda','meta','tsla','avgo',
             'jpm','v','ma','unh','xom','cvx','spy','qqq','xlf','xle','xlk','tlt','hyg']

c.execute("""
    SELECT subject, predicate, object, timestamp
    FROM facts
    WHERE subject IN ({})
    AND predicate IN ('last_price','price_target','signal_direction')
    ORDER BY subject, predicate, timestamp DESC
""".format(','.join('?'*len(WATCHLIST))), WATCHLIST)

rows = c.fetchall()
by_ticker = defaultdict(dict)
for subj, pred, obj, ts in rows:
    # keep most recent per (subject, predicate)
    if pred not in by_ticker[subj]:
        by_ticker[subj][pred] = (obj, ts[:19])

print(f"{'TICKER':8s} {'last_price':14s} {'price_target':14s} {'signal_dir':14s}  issues")
print('-'*75)
issues = []
for t in WATCHLIST:
    d = by_ticker.get(t, {})
    price  = d.get('last_price',  ('MISSING',''))[0]
    target = d.get('price_target',('MISSING',''))[0]
    sig    = d.get('signal_direction',('MISSING',''))[0]
    flags  = []
    if price  == 'MISSING': flags.append('NO_PRICE')
    if target == 'MISSING': flags.append('NO_TARGET')
    if sig    == 'MISSING': flags.append('NO_SIGNAL')
    flag_str = ' '.join(flags)
    print(f"  {t.upper():6s}  {price:14s} {target:14s} {sig:14s}  {flag_str}")
    if flags:
        issues.append((t.upper(), flags))

print()
print(f"Tickers with gaps ({len(issues)}):")
for t, f in issues:
    print(f"  {t}: {f}")

# How many last_price rows per ticker (all time — detect duplicates)
print()
print("last_price row counts (all history):")
c.execute("""
    SELECT subject, COUNT(*) as n FROM facts
    WHERE predicate='last_price' AND subject IN ({})
    GROUP BY subject ORDER BY n DESC
""".format(','.join('?'*len(WATCHLIST))), WATCHLIST)
for r in c.fetchall():
    print(f"  {r[0].upper():8s}: {r[1]} rows")

conn.close()

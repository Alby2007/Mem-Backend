"""tests/db_full_audit.py — comprehensive atom count breakdown"""
import sqlite3
from collections import defaultdict

DB = 'trading_knowledge.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

c.execute("SELECT COUNT(*) FROM facts")
total = c.fetchone()[0]
print(f"Total facts: {total}")

c.execute("SELECT COUNT(DISTINCT subject) FROM facts")
print(f"Distinct subjects: {c.fetchone()[0]}")

c.execute("SELECT COUNT(DISTINCT predicate) FROM facts")
print(f"Distinct predicates: {c.fetchone()[0]}")

print()
print("=== Predicate breakdown ===")
c.execute("SELECT predicate, COUNT(*) n FROM facts GROUP BY predicate ORDER BY n DESC")
for r in c.fetchall():
    print(f"  {r['n']:5d}  {r['predicate']}")

# Key watchlist tickers — show last_price + price_target + signal per ticker
CORE = ['aapl','msft','googl','amzn','nvda','meta','tsla','avgo',
        'jpm','v','ma','unh','xom','cvx',
        'spy','qqq','xlf','xle','xlk','tlt','hyg',
        'amd','intc','crm','adbe','blk','lly','gs']

print()
print("=== Core ticker coverage ===")
c.execute("""
    SELECT subject, predicate, object, timestamp
    FROM facts
    WHERE predicate IN ('last_price','price_target','signal_direction')
    ORDER BY subject, predicate, timestamp DESC
""")
rows = c.fetchall()
by_ticker = defaultdict(dict)
for r in rows:
    subj = r['subject']
    pred = r['predicate']
    if pred not in by_ticker[subj]:
        by_ticker[subj][pred] = r['object']

print(f"  {'TICKER':8s} {'price':12s} {'target':12s} {'signal':14s}")
print(f"  {'-'*50}")
for t in CORE:
    d = by_ticker.get(t, {})
    price  = d.get('last_price', 'MISSING')
    target = d.get('price_target', 'n/a')
    sig    = d.get('signal_direction', 'n/a')
    gap = ' ← NO PRICE' if price == 'MISSING' else ''
    print(f"  {t.upper():8s} {price:12s} {target:12s} {sig:14s}{gap}")

# last_price row count per ticker (should be 1 after upsert)
print()
print("=== last_price row count per core ticker (should be 1 after upsert) ===")
c.execute("""
    SELECT subject, COUNT(*) n FROM facts
    WHERE predicate='last_price'
    AND subject IN ({})
    GROUP BY subject ORDER BY subject
""".format(','.join('?'*len(CORE))), CORE)
for r in c.fetchall():
    flag = ' ← DUPLICATE ROWS' if r['n'] > 1 else ''
    print(f"  {r['subject'].upper():8s}: {r['n']} row(s){flag}")

# News atom freshness
print()
print("=== Most recent 10 news atoms (catalyst/risk_factor/key_finding) ===")
c.execute("""
    SELECT subject, predicate, object, timestamp FROM facts
    WHERE predicate IN ('catalyst','risk_factor','key_finding')
    ORDER BY timestamp DESC LIMIT 10
""")
for r in c.fetchall():
    print(f"  {r['timestamp'][:19]}  {r['subject']:12s}  {r['predicate']:12s}  {r['object'][:60]}")

conn.close()

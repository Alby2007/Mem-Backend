import sqlite3

DB = '/home/ubuntu/trading-galaxy/trading_knowledge.db'
c = sqlite3.connect(DB)

total = c.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
tickers = c.execute('SELECT COUNT(DISTINCT subject) FROM facts WHERE predicate="last_price"').fetchone()[0]

sources = c.execute('SELECT source, COUNT(*) FROM facts GROUP BY source ORDER BY 2 DESC LIMIT 60').fetchall()
preds = c.execute('SELECT predicate, COUNT(*) FROM facts GROUP BY predicate ORDER BY 2 DESC LIMIT 20').fetchall()
patterns = c.execute('SELECT status, COUNT(*) FROM pattern_signals GROUP BY status').fetchall()

oldest = c.execute('SELECT MIN(created_at) FROM facts').fetchone()[0]
newest = c.execute('SELECT MAX(created_at) FROM facts').fetchone()[0]

print(f'TOTAL FACTS:    {total:,}')
print(f'UNIQUE TICKERS: {tickers:,}')
print(f'DATE RANGE:     {oldest}  -->  {newest}')
print()
print('TOP SOURCES:')
for s, n in sources:
    print(f'  {n:>8,}  {s}')
print()
print('TOP PREDICATES:')
for p, n in preds:
    print(f'  {n:>8,}  {p}')
print()
print('PATTERN SIGNALS:')
for s, n in patterns:
    print(f'  {n:>6,}  {s}')

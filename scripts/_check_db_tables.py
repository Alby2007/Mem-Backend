import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(DB)

tables = c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print('=== TABLE ROW COUNTS ===')
for (t,) in tables:
    try:
        n = c.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f'  {n:>10,}  {t}')
    except Exception as e:
        print(f'  {"ERR":>10}  {t}  ({e})')

print()
cols = [r[1] for r in c.execute('PRAGMA table_info(facts)').fetchall()]
print('FACTS COLUMNS:', cols)

total = c.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
print(f'\nTOTAL FACTS: {total:,}')

if total > 0:
    sources = c.execute('SELECT source, COUNT(*) FROM facts GROUP BY source ORDER BY 2 DESC LIMIT 40').fetchall()
    preds = c.execute('SELECT predicate, COUNT(*) FROM facts GROUP BY predicate ORDER BY 2 DESC LIMIT 20').fetchall()
    print('\nTOP SOURCES:')
    for s, n in sources:
        print(f'  {n:>8,}  {s}')
    print('\nTOP PREDICATES:')
    for p, n in preds:
        print(f'  {n:>8,}  {p}')
else:
    print('Facts table is empty — checking if a different DB path is used...')
    import os, glob
    for f in glob.glob('/home/ubuntu/**/*.db', recursive=True):
        try:
            sz = os.path.getsize(f)
            print(f'  {sz:>12,} bytes  {f}')
        except Exception:
            pass

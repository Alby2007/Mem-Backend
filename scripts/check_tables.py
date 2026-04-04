import sqlite3
c = sqlite3.connect('/home/ubuntu/trading-galaxy/trading_knowledge.db')
tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print("Tables:", tables)
# Check the atoms/facts table
for t in tables:
    if 'atom' in t.lower() or 'fact' in t.lower() or 'know' in t.lower():
        n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})").fetchall()]
        print(f"  {t}: {n} rows, cols={cols[:8]}")

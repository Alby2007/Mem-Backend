import sqlite3

db = sqlite3.connect('/opt/trading-galaxy/data/trading_knowledge.db')
c = db.cursor()

# Show what we're deleting
rows = c.execute("SELECT subject, predicate, object FROM facts WHERE subject='portfolio'").fetchall()
print(f'Found {len(rows)} atoms with subject=portfolio:')
for r in rows:
    print(f'  {r}')

# Also check subject='volatility' — another generic subject that could bleed across sessions
rows2 = c.execute("SELECT subject, predicate, object FROM facts WHERE subject='volatility'").fetchall()
print(f'\nFound {len(rows2)} atoms with subject=volatility:')
for r in rows2:
    print(f'  {r}')

# Delete both
c.execute("DELETE FROM facts WHERE subject='portfolio'")
c.execute("DELETE FROM facts WHERE subject='volatility'")
db.commit()
print(f'\nDeleted {len(rows) + len(rows2)} stale shared-subject atoms.')
db.close()

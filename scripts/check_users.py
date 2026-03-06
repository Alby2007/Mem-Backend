#!/usr/bin/env python3
import sqlite3, os
DB = os.environ.get('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
c = sqlite3.connect(f'file:{DB}?mode=ro', uri=True)
rows = c.execute("SELECT user_id, email, tier FROM users LIMIT 10").fetchall()
print("Users:")
for r in rows:
    print(f"  {r[0]}  {r[1]}  tier={r[2]}")
c.close()

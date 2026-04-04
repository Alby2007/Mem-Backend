#!/usr/bin/env python3
import sqlite3, os
# Users are in the main app DB (trading_galaxy.db), not the KB DB
DB = '/opt/trading-galaxy/data/trading_galaxy.db'
if not os.path.exists(DB):
    DB = '/home/ubuntu/trading-galaxy/trading_galaxy.db'
print(f'Using: {DB}')
c = sqlite3.connect(f'file:{DB}?mode=ro', uri=True)
rows = c.execute("SELECT user_id, email, tier FROM users LIMIT 10").fetchall()
print("Users:")
for r in rows:
    print(f"  {r[0]}  {r[1]}  tier={r[2]}")
c.close()

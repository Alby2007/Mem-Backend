import sqlite3, os

db = "/home/ubuntu/trading-galaxy/trading_galaxy.db"
c = sqlite3.connect(db)

print("=== all tables ===")
for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
    print(r[0])

print("\n=== paper_agent_log (last 10) ===")
try:
    for r in c.execute(
        "SELECT event_type, ticker, detail, created_at FROM paper_agent_log ORDER BY created_at DESC LIMIT 10"
    ).fetchall():
        print(r)
except Exception as e:
    print("error:", e)

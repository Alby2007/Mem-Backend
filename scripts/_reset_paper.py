import sqlite3

db = "/opt/trading-galaxy/data/trading_knowledge.db"
c = sqlite3.connect(db)

c.execute("UPDATE paper_account SET virtual_balance = 500000.0")
c.execute("DELETE FROM paper_positions")
c.execute("DELETE FROM paper_agent_log")
c.commit()

rows = c.execute("SELECT user_id, virtual_balance FROM paper_account").fetchall()
for r in rows:
    print("account:", r)
print("positions cleared:", c.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0])
print("log cleared:", c.execute("SELECT COUNT(*) FROM paper_agent_log").fetchone()[0])
c.close()

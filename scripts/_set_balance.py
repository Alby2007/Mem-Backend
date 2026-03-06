import sqlite3

db = "/opt/trading-galaxy/data/trading_knowledge.db"
c = sqlite3.connect(db)
c.execute("UPDATE paper_account SET virtual_balance = 500000.0")
c.commit()
rows = c.execute("SELECT user_id, virtual_balance FROM paper_account").fetchall()
for r in rows:
    print(r)
c.close()

import sqlite3
DB = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(DB)
c.execute("UPDATE user_preferences SET tier='premium', is_dev=1 WHERE user_id='albertjemmettwaite_uggwq'")
c.commit()
rows = c.execute('SELECT user_id, tier, is_dev FROM user_preferences ORDER BY rowid DESC LIMIT 10').fetchall()
for r in rows:
    print(r)
c.close()

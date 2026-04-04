import sqlite3
db = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(db, timeout=30)
c.execute('UPDATE user_preferences SET is_dev=1')
c.commit()
rows = c.execute('SELECT user_id, is_dev FROM user_preferences').fetchall()
for r in rows:
    print(r)
c.close()
print('Done.')

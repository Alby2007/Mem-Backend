import sqlite3
db = '/opt/trading-galaxy/data/trading_knowledge.db'
c = sqlite3.connect(db)
row = c.execute("SELECT user_id, is_dev FROM user_preferences WHERE user_id='a2_0mk9r'").fetchone()
print('before:', row)
if row:
    c.execute("UPDATE user_preferences SET is_dev=1 WHERE user_id='a2_0mk9r'")
else:
    c.execute("INSERT INTO user_preferences (user_id, is_dev) VALUES ('a2_0mk9r', 1)")
c.commit()
r = c.execute("SELECT user_id, is_dev FROM user_preferences WHERE user_id='a2_0mk9r'").fetchone()
print('after:', r)
c.close()


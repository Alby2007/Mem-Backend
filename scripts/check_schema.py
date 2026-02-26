import sqlite3, os
db = os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')
conn = sqlite3.connect(db)
print("=== facts table schema ===")
for row in conn.execute("SELECT sql FROM sqlite_master WHERE name='facts'").fetchall():
    print(row[0])
print("\n=== facts columns ===")
for row in conn.execute("PRAGMA table_info(facts)").fetchall():
    print(row)
print("\n=== facts indexes ===")
for row in conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='facts'").fetchall():
    print(row[0])
conn.close()

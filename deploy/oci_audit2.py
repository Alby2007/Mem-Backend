import sqlite3, extensions as ext
conn = sqlite3.connect(ext.DB_PATH)
rows = conn.execute(
    "SELECT user_id, tier FROM user_preferences "
    "WHERE user_id NOT LIKE 'eval_%' AND user_id NOT LIKE 'debug_%' AND user_id != 'eval_smoke_test_001' "
    "ORDER BY tier, user_id"
).fetchall()
print("=== NON-EVAL USER TIERS ===")
for r in rows:
    print(f"  {r[0]:35s}  {r[1]}")

# Specifically check tester5
t5 = conn.execute("SELECT user_id, tier FROM user_preferences WHERE user_id LIKE 'tester5%'").fetchall()
print("\n=== TESTER5 ===")
for r in t5:
    print(f"  {r[0]}  {r[1]}")

conn.close()

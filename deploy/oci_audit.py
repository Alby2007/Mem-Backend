import sqlite3, extensions as ext
conn = sqlite3.connect(ext.DB_PATH)
conn.row_factory = sqlite3.Row

print("=== ALL USER TIERS ===")
rows = conn.execute("SELECT user_id, tier FROM user_preferences ORDER BY tier, user_id").fetchall()
for r in rows:
    print(f"  {r['user_id']:30s}  tier={r['tier']}")

print("\n=== NON-FREE USERS ===")
rows = conn.execute("SELECT user_id, tier FROM user_preferences WHERE tier != 'free' ORDER BY tier").fetchall()
for r in rows:
    print(f"  {r['user_id']:30s}  tier={r['tier']}")

print("\n=== AUDIT LOG (last 20 register events) ===")
try:
    rows = conn.execute(
        "SELECT action, user_id, outcome, detail, created_at FROM audit_log WHERE action='register' ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    for r in rows:
        print(f"  {r['created_at']}  {r['user_id']}  {r['outcome']}  {r['detail']}")
except Exception as e:
    print(f"  (audit_log error: {e})")

conn.close()

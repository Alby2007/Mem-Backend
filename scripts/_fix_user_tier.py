"""Temporary script: set specific users to premium tier directly in DB."""
import sqlite3, sys, os

DB = os.environ.get('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
USERS = ['shahkrish2003_vr4jw']

conn = sqlite3.connect(DB, timeout=10)
try:
    for uid in USERS:
        conn.execute(
            "INSERT INTO user_preferences (user_id, tier) VALUES (?, 'premium') "
            "ON CONFLICT(user_id) DO UPDATE SET tier='premium'",
            (uid,)
        )
        print(f"Set {uid} -> premium")
    conn.commit()
    # Verify
    for uid in USERS:
        row = conn.execute("SELECT tier FROM user_preferences WHERE user_id=?", (uid,)).fetchone()
        print(f"Verified {uid}: {row}")
finally:
    conn.close()

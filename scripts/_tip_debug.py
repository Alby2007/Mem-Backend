"""Debug why tips aren't showing for the real user."""
import json
import os
import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Find real (non-eval) users
print('=== REAL USERS ===')
users = conn.execute(
    "SELECT user_id, email, created_at FROM user_auth "
    "WHERE user_id NOT LIKE 'eval_%' AND user_id NOT LIKE 'debug_%' "
    "ORDER BY created_at DESC LIMIT 20"
).fetchall()
for u in users:
    print(dict(u))

# Pick the most recent non-eval user
if not users:
    print('No real users found')
    conn.close()
    exit()

# Check user_preferences for all real users
print('\n=== USER PREFERENCES ===')
for u in users[:5]:
    uid = u['user_id']
    pref = conn.execute(
        'SELECT * FROM user_preferences WHERE user_id=?', (uid,)
    ).fetchone()
    print(f'{uid}:', dict(pref) if pref else 'NO PREFS')

# Check tip_followups / tip_delivery_log for the main user
print('\n=== TIP DELIVERY LOG (last 10) ===')
rows = conn.execute(
    'SELECT * FROM tip_delivery_log ORDER BY rowid DESC LIMIT 10'
).fetchall()
for r in rows:
    print(dict(r))

# Check pattern_signals (what tips come from)
print('\n=== PATTERN SIGNALS (last 10, any status) ===')
sigs = conn.execute(
    'SELECT id, ticker, pattern_type, status, quality_score, kb_conviction, detected_at '
    'FROM pattern_signals ORDER BY detected_at DESC LIMIT 10'
).fetchall()
for s in sigs:
    print(dict(s))

# Check tip_followups
print('\n=== TIP FOLLOWUPS (last 10) ===')
tfu = conn.execute(
    'SELECT * FROM tip_followups ORDER BY rowid DESC LIMIT 10'
).fetchall()
for t in tfu:
    print(dict(t))

# Check user_models (tip pool)
print('\n=== USER MODELS (first 5 non-eval) ===')
mods = conn.execute(
    "SELECT user_id, tip_count, last_tip_at FROM user_models "
    "WHERE user_id NOT LIKE 'eval_%' LIMIT 5"
).fetchall()
for m in mods:
    print(dict(m))

# Check TipScheduler status via scheduler health endpoint isn't accessible directly,
# so check logs for clues
print('\n=== RECENT SERVICE LOGS ===')
import subprocess
try:
    out = subprocess.check_output(
        ['journalctl', '-u', 'trading-galaxy', '-n', '40', '--no-pager',
         '--output=short', '--grep', 'tip'],
        stderr=subprocess.DEVNULL
    ).decode()
    print(out[-3000:] if len(out) > 3000 else out)
except Exception as e:
    print('log error:', e)

conn.close()

"""Force-fire a tip delivery for the real user, bypassing the time/dedup gate."""
import os
import sqlite3
import sys

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
TARGET_USER = 'albertjemmettwaite_uggwq'

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Load prefs
prefs_row = conn.execute(
    'SELECT * FROM user_preferences WHERE user_id=?', (TARGET_USER,)
).fetchone()
if not prefs_row:
    print('ERROR: user prefs not found')
    conn.close()
    sys.exit(1)

prefs = dict(prefs_row)
print('User prefs:', {k: prefs[k] for k in ('user_id', 'tier', 'telegram_chat_id', 'tip_delivery_time', 'tip_delivery_timezone')})

# Delete today's tip log entry so the gate allows re-delivery
from datetime import datetime, timezone
import pytz
tz = prefs.get('tip_delivery_timezone') or 'UTC'
try:
    local_now = datetime.now(pytz.timezone(tz))
except Exception:
    local_now = datetime.now(timezone.utc)
local_date_str = local_now.strftime('%Y-%m-%d')
print(f'Local date: {local_date_str} (tz={tz})')

deleted = conn.execute(
    'DELETE FROM tip_delivery_log WHERE user_id=? AND delivered_at_local_date=?',
    (TARGET_USER, local_date_str)
).rowcount
conn.commit()
conn.close()
print(f'Deleted {deleted} existing tip log entry for today')

# Now call _deliver_tip_to_user directly
from notifications.tip_scheduler import _deliver_tip_to_user
print('\nFiring _deliver_tip_to_user...')
try:
    _deliver_tip_to_user(DB, TARGET_USER, prefs, weekday='daily')
    print('Done.')
except Exception as e:
    import traceback
    print('ERROR:', e)
    traceback.print_exc()

# Check result
conn2 = sqlite3.connect(DB)
conn2.row_factory = sqlite3.Row
log = conn2.execute(
    'SELECT * FROM tip_delivery_log ORDER BY rowid DESC LIMIT 3'
).fetchall()
print('\nTip delivery log (last 3):')
for r in log:
    print(dict(r))

followups = conn2.execute(
    'SELECT id, ticker, direction, status, created_at FROM tip_followups WHERE user_id=? ORDER BY id DESC LIMIT 5',
    (TARGET_USER,)
).fetchall()
print('\nTip followups (last 5):')
for f in followups:
    print(dict(f))
conn2.close()

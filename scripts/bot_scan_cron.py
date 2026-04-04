#!/usr/bin/env python3
import sys, os, sqlite3, logging
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')
logging.disable(logging.WARNING)

from services.bot_runner import BotRunner
DB = '/opt/trading-galaxy/data/trading_knowledge.db'

# Ensure WAL mode before scanning to avoid locking the auth endpoint
_c = sqlite3.connect(DB, timeout=5)
_c.execute('PRAGMA journal_mode=WAL')
_c.execute('PRAGMA busy_timeout=30000')
_c.close()

runner = BotRunner(DB)
conn = sqlite3.connect(DB, timeout=10)
bots = conn.execute(
    "SELECT bot_id FROM paper_bot_configs WHERE user_id='alby2007' AND active=1 AND killed_at IS NULL"
).fetchall()
conn.close()

total = 0
for (bot_id,) in bots:
    try:
        r = runner._bot_scan_once(bot_id)
        total += r.get('entries', 0)
    except: pass

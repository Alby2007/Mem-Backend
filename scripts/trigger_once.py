#!/usr/bin/env python3
"""
Run the paper agent once directly (no HTTP, no DB lock contention).
Run on OCI: cd /home/ubuntu/trading-galaxy && source .venv/bin/activate
            TRADING_KB_DB=/opt/trading-galaxy/data/trading_knowledge.db python3 scripts/trigger_once.py
"""
import os, sys, sqlite3, json

DB = os.environ.get('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
USER_ID = 'albertjemmettwaite_uggwq'

# Quick pre-check (read-only)
c = sqlite3.connect(f'file:{DB}?mode=ro', uri=True)
bal = c.execute("SELECT virtual_balance FROM paper_account WHERE user_id=?", (USER_ID,)).fetchone()
open_cnt = c.execute("SELECT COUNT(*) FROM paper_positions WHERE user_id=? AND status='open'", (USER_ID,)).fetchone()[0]
c.close()
print(f'Balance: {bal[0] if bal else "?"}  Open positions: {open_cnt}')

# Import api — this triggers Flask init but we won't serve requests
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.environ['TRADING_KB_DB'] = DB

print('Importing api module...')
from api import _paper_ai_run
print('Running _paper_ai_run...')
result = _paper_ai_run(USER_ID)
print('Result:', json.dumps(result, indent=2))

# Show last 10 non-scan_start log entries
conn = sqlite3.connect(f'file:{DB}?mode=ro', uri=True)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT event_type, ticker, detail, created_at FROM paper_agent_log "
    "WHERE user_id=? AND event_type != 'scan_start' ORDER BY id DESC LIMIT 10",
    (USER_ID,)
).fetchall()
conn.close()
print(f'\n--- Last 10 non-scan_start entries ---')
for r in rows:
    ts = str(r['created_at'])[:19]
    ticker = (r['ticker'] or '').ljust(8)
    print(f"[{ts}] {r['event_type']:12s} {ticker} {r['detail'][:120]}")

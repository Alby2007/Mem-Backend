import sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
from services.paper_trading import ai_run
result = ai_run('a2_0mk9r')
print('RESULT:', result)

import sqlite3, extensions as ext
conn = sqlite3.connect(ext.DB_PATH)
conn.row_factory = sqlite3.Row
logs = conn.execute(
    "SELECT event_type, ticker, detail, created_at FROM paper_agent_log "
    "WHERE user_id='a2_0mk9r' ORDER BY created_at DESC LIMIT 25"
).fetchall()
print('\n=== agent log (last 25) ===')
for lg in logs:
    print(f"  [{lg['created_at']}] {lg['event_type']:15} {lg['ticker'] or '':12} {(lg['detail'] or '')[:100]}")
pos = conn.execute(
    "SELECT ticker, direction, entry_price, stop, t1, quantity, status, opened_at FROM paper_positions "
    "WHERE user_id='a2_0mk9r' AND status='open'"
).fetchall()
print(f'\n=== open positions: {len(pos)} ===')
for p in pos:
    print(f"  {p['ticker']:12} {p['direction']:8} entry={p['entry_price']} stop={p['stop']} qty={p['quantity']}")
conn.close()

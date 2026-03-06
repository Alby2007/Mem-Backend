#!/usr/bin/env python3
"""One-shot paper agent test — call _paper_ai_run and dump the agent log."""
import sys, json, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

from api import _paper_ai_run, _DB_PATH

USER_ID = 'albertjemmettwaite_uggwq'

print(f'Running paper agent for {USER_ID}...')
result = _paper_ai_run(USER_ID)
print('Result:', json.dumps(result, indent=2))

# Show last 10 log entries with reasoning
conn = sqlite3.connect(_DB_PATH)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT event_type, ticker, detail, created_at FROM paper_agent_log "
    "WHERE user_id=? ORDER BY id DESC LIMIT 10",
    (USER_ID,)
).fetchall()
conn.close()

print('\n--- Last 10 agent log entries ---')
for r in reversed(rows):
    print(f"[{r['created_at']}] {r['event_type']:12s} {r['ticker'] or '':8s} {r['detail']}")

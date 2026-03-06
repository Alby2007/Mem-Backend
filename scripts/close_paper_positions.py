#!/usr/bin/env python3
"""Close all open paper positions so the agent can enter new ones."""
import sqlite3, os
from datetime import datetime, timezone

DB = os.environ.get('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
now = datetime.now(timezone.utc).isoformat()

c = sqlite3.connect(DB)
c.row_factory = sqlite3.Row

open_pos = c.execute(
    "SELECT id, ticker, entry_price, quantity FROM paper_positions WHERE user_id='albertjemmettwaite_uggwq' AND status='open'"
).fetchall()

print(f"Open positions: {len(open_pos)}")
for p in open_pos:
    print(f"  {p['ticker']:8s}  entry={p['entry_price']}  qty={p['quantity']}")

if open_pos:
    c.execute(
        "UPDATE paper_positions SET status='closed', filled_at=? WHERE user_id='albertjemmettwaite_uggwq' AND status='open'",
        (now,)
    )
    c.execute(
        "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
        ('albertjemmettwaite_uggwq', 'admin', None, f'Manually closed {len(open_pos)} open positions for agent test', now)
    )
    c.commit()
    print(f"\nClosed {len(open_pos)} positions.")
else:
    print("No open positions to close.")

c.close()

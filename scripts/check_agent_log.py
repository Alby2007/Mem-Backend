#!/usr/bin/env python3
"""
Read the last 20 paper_agent_log entries directly from the live DB (read-only).
Run on OCI: TRADING_KB_DB=/opt/trading-galaxy/data/trading_knowledge.db python3 scripts/check_agent_log.py
"""
import sqlite3, os, sys

DB = os.environ.get('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
USER_ID = 'albertjemmettwaite_uggwq'

c = sqlite3.connect(f'file:{DB}?mode=ro', uri=True)  # read-only, no DB lock
c.row_factory = sqlite3.Row

# Check open position count
open_count = c.execute(
    "SELECT COUNT(*) FROM paper_positions WHERE user_id=? AND status='open'", (USER_ID,)
).fetchone()[0]
print(f'Open positions: {open_count}')

# Sample open positions
positions = c.execute(
    "SELECT ticker, entry_price, quantity, direction FROM paper_positions WHERE user_id=? AND status='open' LIMIT 10",
    (USER_ID,)
).fetchall()
for p in positions:
    print(f'  {p["ticker"]:8s} {p["direction"]:5s} entry={p["entry_price"]} qty={p["quantity"]}')

# Last 20 agent log entries
rows = c.execute(
    "SELECT event_type, ticker, detail, created_at FROM paper_agent_log "
    "WHERE user_id=? ORDER BY id DESC LIMIT 20",
    (USER_ID,)
).fetchall()

print(f'\n--- Last {len(rows)} agent log entries (newest first) ---')
for r in rows:
    ts = str(r['created_at'])[:19]
    ticker = (r['ticker'] or '').ljust(8)
    print(f"[{ts}] {r['event_type']:12s} {ticker} {r['detail']}")

c.close()

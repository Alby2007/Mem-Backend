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

print('\n--- Pattern filter diagnosis ---')
conviction_dist = c.execute(
    "SELECT kb_conviction, COUNT(*) n FROM pattern_signals WHERE status NOT IN ('filled','broken') GROUP BY kb_conviction ORDER BY n DESC LIMIT 10"
).fetchall()
print(f'Conviction distribution (active): {list(conviction_dist)}')

passing = c.execute(
    "SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken') AND quality_score >= 0.70 AND LOWER(kb_conviction) IN ('high','confirmed','strong')"
).fetchone()[0]
print(f'Pass filter (quality>=0.70, conviction high/confirmed/strong): {passing}')

# Looser check
passing_any = c.execute(
    "SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken') AND quality_score >= 0.70"
).fetchone()[0]
print(f'Pass quality filter only (quality>=0.70, any conviction): {passing_any}')

# Sample 5 to see actual values
sample = c.execute(
    "SELECT ticker, quality_score, kb_conviction, kb_regime, status FROM pattern_signals WHERE status NOT IN ('filled','broken') ORDER BY quality_score DESC LIMIT 5"
).fetchall()
print(f'Top 5 active patterns: {list(sample)}')

c.close()

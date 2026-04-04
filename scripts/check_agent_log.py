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

non_scan = c.execute(
    "SELECT event_type, ticker, detail, created_at FROM paper_agent_log "
    "WHERE user_id=? AND event_type != 'scan_start' ORDER BY id DESC LIMIT 15",
    (USER_ID,)
).fetchall()
print(f'\n--- Last {len(non_scan)} non-scan_start entries ---')
for r in non_scan:
    ts = str(r['created_at'])[:19]
    ticker = (r['ticker'] or '').ljust(8)
    print(f"[{ts}] {r['event_type']:12s} {ticker} {r['detail']}")

print('\n--- Pattern filter diagnosis ---')
conviction_dist = c.execute(
    "SELECT kb_conviction, COUNT(*) n FROM pattern_signals WHERE status NOT IN ('filled','broken') GROUP BY kb_conviction ORDER BY n DESC LIMIT 10"
).fetchall()
for row in conviction_dist:
    print(f'  kb_conviction={row[0]!r:25s}  count={row[1]}')

passing = c.execute(
    "SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken') AND quality_score >= 0.70 AND LOWER(kb_conviction) IN ('high','confirmed','strong')"
).fetchone()[0]
print(f'Pass filter (quality>=0.70, conviction high/confirmed/strong): {passing}')

passing_any = c.execute(
    "SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken') AND quality_score >= 0.70"
).fetchone()[0]
print(f'Pass quality filter only (quality>=0.70, any conviction): {passing_any}')

# Sample with zone values to check for zero zones
print('\nTop 5 active patterns (with zones):')
sample = c.execute(
    "SELECT ticker, quality_score, kb_conviction, zone_low, zone_high, direction FROM pattern_signals"
    " WHERE status NOT IN ('filled','broken') ORDER BY quality_score DESC LIMIT 5"
).fetchall()
for row in sample:
    print(f'  {row[0]:8s} q={row[1]:.2f} conv={row[2]!r:12s} dir={row[5]:8s} zone={row[3]:.4f}-{row[4]:.4f}')

open_set = tuple(p['ticker'] for p in positions)
acct = c.execute("SELECT virtual_balance FROM paper_account WHERE user_id=?", (USER_ID,)).fetchone()
print(f'Virtual balance: {acct[0] if acct else "no account row"}')
print(f'\nOpen tickers: {open_set}')

placeholders = ','.join('?' * len(open_set))
if open_set:
    new_tickers = c.execute(
        f"SELECT COUNT(DISTINCT ticker) FROM pattern_signals"
        f" WHERE status NOT IN ('filled','broken') AND quality_score >= 0.70"
        f" AND LOWER(kb_conviction) IN ('high','confirmed','strong')"
        f" AND ticker NOT IN ({placeholders})",
        open_set
    ).fetchone()[0]
    print(f'Passing patterns for NEW (not-open) tickers: {new_tickers}')
    top_new = c.execute(
        f"SELECT ticker, quality_score, kb_conviction, zone_low, zone_high, direction FROM pattern_signals"
        f" WHERE status NOT IN ('filled','broken') AND quality_score >= 0.70"
        f" AND LOWER(kb_conviction) IN ('high','confirmed','strong')"
        f" AND ticker NOT IN ({placeholders})"
        f" ORDER BY quality_score DESC LIMIT 5",
        open_set
    ).fetchall()
    print('Top new-ticker candidates:')
    for row in top_new:
        print(f'  {row[0]:8s} q={row[1]:.2f} conv={row[2]!r:12s} dir={row[5]:8s} zone={row[3]:.4f}-{row[4]:.4f}')

c.close()

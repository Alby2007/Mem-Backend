import sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
import extensions as ext

conn = sqlite3.connect(ext.DB_PATH)
conn.row_factory = sqlite3.Row

# 1. How many pattern_signals candidates exist?
rows = conn.execute(
    "SELECT ticker, direction, quality_score, kb_conviction, kb_signal_dir, zone_low, zone_high "
    "FROM pattern_signals WHERE status NOT IN ('filled','broken') "
    "ORDER BY quality_score DESC LIMIT 20"
).fetchall()
print(f"=== pattern_signals candidates: {len(rows)} ===")
for r in rows:
    d = dict(r)
    print(f"  {d['ticker']:10} {d['direction']:8} q={d['quality_score']} conv={d['kb_conviction']} sig={d['kb_signal_dir']} zone={d['zone_low']}-{d['zone_high']}")

# 2. Check account balance
acct = conn.execute("SELECT * FROM paper_account WHERE user_id='a2_0mk9r'").fetchone()
print(f"\n=== paper_account: {dict(acct) if acct else 'MISSING'} ===")

# 3. Recent agent log
logs = conn.execute(
    "SELECT event_type, ticker, detail, created_at FROM paper_agent_log "
    "WHERE user_id='a2_0mk9r' ORDER BY created_at DESC LIMIT 20"
).fetchall()
print(f"\n=== agent log (last 20) ===")
for lg in logs:
    print(f"  [{lg['created_at']}] {lg['event_type']:15} {lg['ticker'] or '':10} {(lg['detail'] or '')[:120]}")

conn.close()

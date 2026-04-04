import sqlite3, os

db = os.path.expanduser("/home/ubuntu/trading-galaxy/trading_galaxy.db")
c = sqlite3.connect(db)

print("=== pattern_signals status counts ===")
for r in c.execute("SELECT status, COUNT(*) FROM pattern_signals GROUP BY status").fetchall():
    print(r)

print("\n=== open patterns with quality/conviction ===")
for r in c.execute(
    "SELECT ticker, direction, quality_score, kb_conviction, status "
    "FROM pattern_signals WHERE status IN ('open','partially_filled') "
    "ORDER BY quality_score DESC LIMIT 20"
).fetchall():
    print(r)

print("\n=== paper_agent_log (last 10) ===")
try:
    for r in c.execute(
        "SELECT event_type, ticker, detail, created_at FROM paper_agent_log ORDER BY created_at DESC LIMIT 10"
    ).fetchall():
        print(r)
except Exception as e:
    print("paper_agent_log error:", e)

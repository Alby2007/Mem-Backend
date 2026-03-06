import sqlite3

db = "/opt/trading-galaxy/data/trading_knowledge.db"
c = sqlite3.connect(db)

print("=== all tables ===")
for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
    print(r[0])

print("\n=== pattern_signals status counts ===")
try:
    for r in c.execute("SELECT status, COUNT(*) FROM pattern_signals GROUP BY status").fetchall():
        print(r)
except Exception as e:
    print("error:", e)

print("\n=== open patterns sample ===")
try:
    for r in c.execute(
        "SELECT ticker, direction, quality_score, kb_conviction, status "
        "FROM pattern_signals WHERE status IN ('open','partially_filled') "
        "ORDER BY quality_score DESC LIMIT 10"
    ).fetchall():
        print(r)
except Exception as e:
    print("error:", e)

print("\n=== paper_agent_log (last 10) ===")
try:
    for r in c.execute(
        "SELECT event_type, ticker, detail, created_at FROM paper_agent_log ORDER BY created_at DESC LIMIT 10"
    ).fetchall():
        print(r)
except Exception as e:
    print("error:", e)

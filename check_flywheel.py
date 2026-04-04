import sqlite3, sys

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)

print("=== LINK 1: paper_positions pattern_id coverage ===")
r = conn.execute("""
    SELECT COUNT(*) as total,
           SUM(CASE WHEN pattern_id IS NOT NULL THEN 1 ELSE 0 END) as with_pattern,
           SUM(CASE WHEN pattern_id IS NULL THEN 1 ELSE 0 END) as without_pattern
    FROM paper_positions WHERE status != 'open'
""").fetchone()
print(f"  total={r[0]}  with_pattern={r[1]}  without_pattern={r[2]}")

print("\n=== LINK 2: calibration_observations ===")
try:
    r2 = conn.execute("SELECT COUNT(*) FROM calibration_observations").fetchone()
    print(f"  total rows: {r2[0]}")
    rows = conn.execute("SELECT source, COUNT(*) FROM calibration_observations GROUP BY source").fetchall()
    for row in rows:
        print(f"  source={row[0]}  n={row[1]}")
    if r2[0] == 0:
        print("  WARNING: table exists but empty")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n=== LINK 2b: signal_calibration ===")
try:
    r3 = conn.execute("SELECT COUNT(*) FROM signal_calibration").fetchone()
    print(f"  total rows: {r3[0]}")
    rows = conn.execute("SELECT source, COUNT(*) FROM signal_calibration GROUP BY source LIMIT 10").fetchall()
    for row in rows:
        print(f"  source={row[0]}  n={row[1]}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n=== LINK 5: prediction_ledger ===")
try:
    r4 = conn.execute("SELECT COUNT(*) FROM prediction_ledger").fetchone()
    print(f"  total rows: {r4[0]}")
    rows = conn.execute("SELECT source, COUNT(*) FROM prediction_ledger GROUP BY source").fetchall()
    for row in rows:
        print(f"  source={row[0]}  n={row[1]}")
    recent = conn.execute("SELECT ticker, source, created_at FROM prediction_ledger ORDER BY created_at DESC LIMIT 3").fetchall()
    for row in recent:
        print(f"  recent: {row[0]}  {row[1]}  {row[2]}")
except Exception as e:
    print(f"  ERROR: {e}")

conn.close()

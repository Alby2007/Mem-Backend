"""One-shot backtest runner — called remotely via SSH."""
import json, sqlite3, sys, os

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
db = 'trading_knowledge.db'
conn = sqlite3.connect(db)

# ── Diagnostics ──────────────────────────────────────────────────────────────
diag = {}
try:
    diag['snapshots_count'] = conn.execute('SELECT COUNT(*) FROM signal_snapshots').fetchone()[0]
    diag['distinct_dates']  = [r[0] for r in conn.execute(
        'SELECT DISTINCT snapshot_date FROM signal_snapshots ORDER BY snapshot_date').fetchall()]
except Exception as e:
    diag['snapshots_error'] = str(e)

try:
    diag['conviction_tier_atoms'] = conn.execute(
        "SELECT COUNT(*) FROM facts WHERE predicate='conviction_tier'").fetchone()[0]
except Exception as e:
    diag['conviction_error'] = str(e)

try:
    diag['return_1m_atoms'] = conn.execute(
        "SELECT COUNT(*) FROM facts WHERE predicate='return_1m'").fetchone()[0]
except Exception as e:
    diag['return_1m_error'] = str(e)

conn.close()
print('=== DIAGNOSTICS ===')
print(json.dumps(diag, indent=2))

# ── Take snapshot ─────────────────────────────────────────────────────────────
print('\n=== SNAPSHOT ===')
from analytics.backtest import take_snapshot, run_backtest, run_regime_backtest
snap = take_snapshot(db)
print(json.dumps(snap, indent=2))

# ── Re-check snapshot count ───────────────────────────────────────────────────
conn2 = sqlite3.connect(db)
n = conn2.execute('SELECT COUNT(*) FROM signal_snapshots').fetchone()[0]
dates = [r[0] for r in conn2.execute(
    'SELECT DISTINCT snapshot_date FROM signal_snapshots ORDER BY snapshot_date').fetchall()]
conn2.close()
print(f'\nAfter snapshot: {n} rows, dates={dates}')

# ── Run backtests ──────────────────────────────────────────────────────────────
print('\n=== BACKTEST (1m) ===')
r = run_backtest(db, window='1m')
print(json.dumps(r, indent=2))

print('\n=== REGIME BACKTEST ===')
rr = run_regime_backtest(db)
print(json.dumps(rr, indent=2))

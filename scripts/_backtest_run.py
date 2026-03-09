"""One-shot backtest runner — called remotely via SSH."""
import json
import os
import sqlite3

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
db = 'trading_knowledge.db'

# ── KB atom diagnostics ───────────────────────────────────────────────────────
conn = sqlite3.connect(db)
diag = {}

try:
    diag['snapshots_count'] = conn.execute(
        'SELECT COUNT(*) FROM signal_snapshots').fetchone()[0]
    diag['distinct_dates'] = [r[0] for r in conn.execute(
        'SELECT DISTINCT snapshot_date FROM signal_snapshots ORDER BY snapshot_date'
    ).fetchall()]
except Exception as e:
    diag['snapshots_error'] = str(e)

for pred in ('conviction_tier', 'return_1m', 'return_1w', 'signal_quality',
             'signal_direction', 'last_price', 'price_regime'):
    try:
        diag[pred + '_count'] = conn.execute(
            'SELECT COUNT(*) FROM facts WHERE predicate=?', (pred,)
        ).fetchone()[0]
    except Exception as e:
        diag[pred + '_error'] = str(e)

# Top predicates
try:
    top = conn.execute(
        'SELECT predicate, COUNT(*) n FROM facts GROUP BY predicate ORDER BY n DESC LIMIT 20'
    ).fetchall()
    diag['top_predicates'] = {r[0]: r[1] for r in top}
except Exception as e:
    diag['top_predicates_error'] = str(e)

conn.close()
print('=== DIAGNOSTICS ===')
print(json.dumps(diag, indent=2))

# ── Run enrichment if conviction_tier atoms are missing ───────────────────────
if diag.get('conviction_tier_count', 0) == 0:
    print('\n=== RUNNING SignalEnrichmentAdapter ===')
    try:
        import extensions as ext  # loads DB_PATH, kg, etc.
        from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
        sea = SignalEnrichmentAdapter(db_path=ext.DB_PATH)
        result = sea.run()
        print('enrichment result length:', len(result) if result else 0)
    except Exception as e:
        print('enrichment error:', e)

    # Re-check
    conn3 = sqlite3.connect(db)
    ct = conn3.execute(
        'SELECT COUNT(*) FROM facts WHERE predicate=?', ('conviction_tier',)
    ).fetchone()[0]
    conn3.close()
    print('conviction_tier atoms after enrichment:', ct)

# ── Take snapshot ─────────────────────────────────────────────────────────────
print('\n=== SNAPSHOT ===')
from analytics.backtest import run_backtest, run_regime_backtest, take_snapshot

snap = take_snapshot(db)
print(json.dumps(snap, indent=2))

conn4 = sqlite3.connect(db)
n = conn4.execute('SELECT COUNT(*) FROM signal_snapshots').fetchone()[0]
dates = [r[0] for r in conn4.execute(
    'SELECT DISTINCT snapshot_date FROM signal_snapshots ORDER BY snapshot_date'
).fetchall()]
conn4.close()
print('After snapshot: %d rows, dates=%s' % (n, dates))

# ── Run backtests ─────────────────────────────────────────────────────────────
print('\n=== BACKTEST (1m) ===')
r = run_backtest(db, window='1m')
print(json.dumps(r, indent=2))

print('\n=== REGIME BACKTEST ===')
rr = run_regime_backtest(db)
print(json.dumps(rr, indent=2))

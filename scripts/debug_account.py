"""Debug script: test prediction_ledger write directly."""
import sys, os, traceback, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DB = '/opt/trading-galaxy/data/trading_knowledge.db'

import extensions as ext
ext.DB_PATH = DB

# 1. Check prediction_ledger row count
conn = sqlite3.connect(DB)
count = conn.execute('SELECT COUNT(*) FROM prediction_ledger').fetchone()[0]
print(f'prediction_ledger rows: {count}')
conn.close()

# 2. Check ext.prediction_ledger is available
print(f'ext.prediction_ledger: {getattr(ext, "prediction_ledger", "NOT SET")}')

# 3. Try initialising it directly and writing a test row
try:
    from analytics.prediction_ledger import PredictionLedger
    ledger = PredictionLedger(DB)
    print('PredictionLedger init: OK')
    ok = ledger.record_prediction(
        ticker='TEST',
        pattern_type='debug_test',
        timeframe='4h',
        entry_price=100.0,
        target_1=110.0,
        target_2=120.0,
        stop_loss=95.0,
        p_hit_t1=0.6,
        p_hit_t2=0.36,
        p_stopped_out=0.4,
        market_regime=None,
        conviction_tier=None,
        source='debug_script',
    )
    print(f'record_prediction inserted: {ok}')
    conn2 = sqlite3.connect(DB)
    count2 = conn2.execute('SELECT COUNT(*) FROM prediction_ledger').fetchone()[0]
    print(f'prediction_ledger rows after write: {count2}')
    # Clean up test row
    conn2.execute("DELETE FROM prediction_ledger WHERE source='debug_script'")
    conn2.commit()
    conn2.close()
except Exception:
    traceback.print_exc()

# 4. Check recent paper_positions entries and whether ledger write would have fired
try:
    conn3 = sqlite3.connect(DB)
    rows = conn3.execute(
        "SELECT ticker, direction, opened_at FROM paper_positions WHERE status='open' ORDER BY opened_at DESC LIMIT 5"
    ).fetchall()
    print(f'\nOpen positions ({len(rows)}):')
    for r in rows:
        print(f'  {r[0]} {r[1]} opened={r[2]}')
    conn3.close()
except Exception as e:
    print(f'positions query failed: {e}')

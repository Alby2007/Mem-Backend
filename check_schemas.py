import sqlite3
DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)

for table in ['calibration_observations', 'signal_calibration', 'prediction_ledger']:
    print(f"\n=== {table} ===")
    try:
        cols = conn.execute(f'PRAGMA table_info({table})').fetchall()
        for c in cols:
            print(f"  {c[1]:30} {c[2]}")
    except Exception as e:
        print(f"  ERROR: {e}")

# Try a test insert into calibration_observations to see exact error
print("\n=== TEST INSERT calibration_observations ===")
try:
    conn.execute("""INSERT INTO calibration_observations
        (ticker, pattern_type, timeframe, market_regime, outcome, source, bot_id, observed_at)
        VALUES ('TEST','fvg','4h',NULL,'hit_t1','paper_bot',NULL,'2026-01-01T00:00:00')""")
    conn.rollback()
    print("  INSERT OK (rolled back)")
except Exception as e:
    print(f"  INSERT FAILED: {e}")

# Check prediction_ledger schema
print("\n=== TEST INSERT prediction_ledger ===")
try:
    conn.execute("""INSERT OR IGNORE INTO prediction_ledger
        (ticker, pattern_type, timeframe, entry_price, target_1, target_2, stop_loss,
         p_hit_t1, p_hit_t2, p_stopped_out, market_regime, conviction_tier, issued_at,
         expires_at, source)
        VALUES ('TEST','fvg','4h',100,110,120,90,0.6,0.36,0.4,NULL,NULL,'2026-01-01T00:00:00',
        '2026-02-01T00:00:00','paper_bot')""")
    conn.rollback()
    print("  INSERT OK (rolled back)")
except Exception as e:
    print(f"  INSERT FAILED: {e}")

# Check adapter _logger issue
print("\n=== ADAPTER _logger check ===")
import sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
for cls_path in [
    ('ingest.historical_calibration_adapter', 'HistoricalCalibrationAdapter'),
    ('ingest.anomaly_detector_adapter', 'AnomalyDetectorAdapter'),
]:
    mod_name, cls_name = cls_path
    try:
        mod = __import__(mod_name, fromlist=[cls_name])
        cls = getattr(mod, cls_name)
        obj = cls.__new__(cls)
        has_logger = hasattr(obj, '_logger')
        print(f"  {cls_name}: _logger={'YES' if has_logger else 'MISSING - check __init__'}")
    except Exception as e:
        print(f"  {cls_name}: import/check failed: {e}")

conn.close()

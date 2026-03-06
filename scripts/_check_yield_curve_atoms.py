"""Check yield_curve atoms in KB and trigger adapter if empty."""
import sqlite3
import sys
import os

DB = '/opt/trading-galaxy/data/trading_knowledge.db'

conn = sqlite3.connect(DB)
rows = conn.execute(
    "SELECT predicate, object, timestamp FROM facts "
    "WHERE source='yield_curve' ORDER BY timestamp DESC LIMIT 15"
).fetchall()
conn.close()

if rows:
    print(f"[OK] {len(rows)} yield_curve atoms found:")
    for r in rows:
        print(f"  {r[0]:30s} | {r[1]:20s} | {r[2]}")
else:
    print("[EMPTY] No yield_curve atoms — triggering adapter now...")
    sys.path.insert(0, '/home/ubuntu/trading-galaxy')
    os.chdir('/home/ubuntu/trading-galaxy')
    try:
        from dotenv import load_dotenv
        load_dotenv('/home/ubuntu/trading-galaxy/.env')
    except ImportError:
        pass
    from ingest.yield_curve_adapter import YieldCurveAdapter
    adapter = YieldCurveAdapter()
    atoms = adapter.fetch()
    print(f"  Adapter returned {len(atoms)} atoms")
    if atoms:
        import sqlite3 as _sq
        _c = _sq.connect(DB, timeout=10)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        for a in atoms:
            _c.execute(
                "INSERT OR REPLACE INTO facts"
                " (subject, predicate, object, source, confidence, timestamp)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (a.subject, a.predicate, a.object, a.source, a.confidence, now)
            )
            print(f"  -> {a.predicate:30s} | {a.object}")
        _c.commit()
        _c.close()
    print("[DONE]")

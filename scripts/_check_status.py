"""Quick adapter health + KB fact count check."""
import requests
import sqlite3

# Adapter status
r = requests.get('http://localhost:5050/ingest/status', timeout=10)
d = r.json()
adapters = d.get('adapters', {})
print("=== ADAPTER STATUS ===")
for k, v in sorted(adapters.items()):
    print(
        f"  {k:25s} runs={v.get('total_runs',0):3d} "
        f"atoms={v.get('total_atoms',0):6d} "
        f"errors={v.get('total_errors',0):2d} "
        f"running={v.get('is_running',False)}"
    )

# KB counts
conn = sqlite3.connect('trading_knowledge.db')
total_facts     = conn.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
open_patterns   = conn.execute("SELECT COUNT(*) FROM pattern_signals WHERE status='open'").fetchone()[0]
total_patterns  = conn.execute('SELECT COUNT(*) FROM pattern_signals').fetchone()[0]
conflicts       = conn.execute('SELECT COUNT(*) FROM fact_conflicts').fetchone()[0]
queue_remaining = conn.execute('SELECT COUNT(*) FROM extraction_queue WHERE processed=0').fetchone()[0]
conn.close()

print("\n=== KB STATS ===")
print(f"  Total facts       : {total_facts:,}")
print(f"  Open patterns     : {open_patterns:,}")
print(f"  Total patterns    : {total_patterns:,}")
print(f"  Fact conflicts    : {conflicts:,}  ({conflicts/max(total_facts,1):.1%})")
print(f"  Extraction queue  : {queue_remaining:,} unprocessed")
print(f"\n  Conflict ratio    : {'OK' if conflicts/max(total_facts,1) <= 0.5 else 'HIGH'}")
print(f"  Quality gate      : {'PASS' if total_facts >= 2000 and open_patterns >= 1 else 'FAIL'}")

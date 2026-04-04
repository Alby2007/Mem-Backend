import urllib.request, json, sqlite3

# Check ingest status
try:
    with urllib.request.urlopen('http://localhost:5050/ingest/status', timeout=5) as r:
        d = json.load(r)
    adapters = d.get('adapters', {})
    print("=== ADAPTER STATUS ===")
    for k, v in adapters.items():
        print(f"{k}: last_run={v.get('last_run_at','never')} atoms={v.get('atoms_written',0)} err={v.get('last_error','')[:60] if v.get('last_error') else ''}")
except Exception as e:
    print("Status error:", e)

# Check news wire atoms directly
DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

print("\n=== NEWS WIRE ATOMS ===")
c.execute("SELECT subject, COUNT(*) as n FROM facts WHERE subject LIKE 'news_wire%' GROUP BY subject ORDER BY n DESC")
rows = c.fetchall()
if rows:
    for r in rows: print(r)
else:
    print("NONE - RSS adapter has not written any atoms")

print("\n=== SAMPLE NEWS WIRE ATOMS (any source with 'news') ===")
c.execute("SELECT subject, predicate, object FROM facts WHERE source LIKE '%news%' OR source LIKE '%rss%' OR source LIKE '%wire%' LIMIT 10")
for r in c.fetchall(): print(r)

print("\n=== RETRIEVAL PATH TEST (geo query) ===")
import sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
try:
    import retrieval as R
    import sqlite3 as sq
    conn2 = sq.connect(DB)
    conn2.row_factory = sq.Row
    snippet, atoms = R.retrieve("geopolitical tension world monitor signals", conn=conn2)
    print(f"Atoms: {len(atoms)}")
    print(f"Snippet (first 500 chars):\n{snippet[:500]}")
except Exception as e:
    print("Retrieval error:", e)
    import traceback; traceback.print_exc()

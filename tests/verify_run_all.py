"""tests/verify_run_all.py — verify POST /ingest/run-all and check DB growth"""
import requests, json, time, sqlite3

BASE = 'http://127.0.0.1:5050'

def db_count():
    c = sqlite3.connect('trading_knowledge.db').cursor()
    c.execute("SELECT COUNT(*) FROM facts")
    return c.fetchone()[0]

before = db_count()
print(f"Facts before run-all: {before}")

# Trigger run-all
r = requests.post(f'{BASE}/ingest/run-all', json={})
print(f"\nrun-all HTTP {r.status_code}")
d = r.json()
print(f"  dispatched: {d.get('dispatched')}")
print(f"  skipped:    {d.get('skipped')}")
print(f"  note:       {d.get('note')}")

# Poll status every 3s for up to 60s until all adapters stop running
print("\nPolling adapter status...")
deadline = time.time() + 60
while time.time() < deadline:
    time.sleep(3)
    r2 = requests.get(f'{BASE}/ingest/status')
    adapters = r2.json().get('adapters', {})
    any_running = any(a['is_running'] for a in adapters.values())
    counts = {n: a['total_atoms'] for n, a in adapters.items()}
    print(f"  {counts}  any_running={any_running}")
    if not any_running:
        break

after = db_count()
print(f"\nFacts after run-all:  {after}  (delta: {after - before:+d})")

print("\nFinal adapter status:")
r3 = requests.get(f'{BASE}/ingest/status')
for name, st in r3.json().get('adapters', {}).items():
    err = st['last_error'] or '-'
    print(f"  {name:12s}  runs={st['total_runs']:2d}  atoms={st['total_atoms']:4d}  err={err[:60]}")

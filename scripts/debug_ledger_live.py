"""Hit the running API to check prediction_ledger state via health endpoint."""
import urllib.request, json, sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'

# Check live via health/detailed
try:
    req = urllib.request.Request('http://localhost:5050/health/detailed')
    with urllib.request.urlopen(req, timeout=5) as r:
        data = json.loads(r.read())
    print('health/detailed:', json.dumps(data, indent=2))
except Exception as e:
    print(f'health/detailed failed: {e}')

# Check the DB directly
conn = sqlite3.connect(DB)
rows = conn.execute("SELECT COUNT(*) FROM prediction_ledger").fetchone()
print(f'\nprediction_ledger rows: {rows[0]}')

# Check paper_agent_log for recent entries that should have triggered ledger write
recent = conn.execute(
    "SELECT user_id, ticker, detail, created_at FROM paper_agent_log "
    "WHERE event_type='entry' ORDER BY created_at DESC LIMIT 10"
).fetchall()
print(f'\nRecent paper_agent_log entries ({len(recent)}):')
for r in recent:
    print(f'  {r[3][:19]} {r[1]} | {r[2][:80]}')

# Check if ledger write warning shows up
conn.close()

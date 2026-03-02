"""Debug yield_curve fetch — check raw bar counts."""
import sys, os
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
import urllib.request, json
from datetime import datetime, timedelta, timezone

KEY = os.environ.get('POLYGON_API_KEY', '')
BASE = 'https://api.polygon.io'

end   = datetime.now(timezone.utc).date()
start = end - timedelta(days=6)
print(f"Date range: {start} to {end}  (today={datetime.now(timezone.utc).strftime('%A')})")

for ticker in ('TLT', 'IEF', 'SHY'):
    url = (f'{BASE}/v2/aggs/ticker/{ticker}/range/1/day'
           f'/{start}/{end}'
           f'?adjusted=true&sort=asc&limit=10&apiKey={KEY}')
    with urllib.request.urlopen(url, timeout=10) as r:
        d = json.loads(r.read())
    results = d.get('results') or []
    print(f"{ticker}: {len(results)} bars  status={d.get('status')}")
    for b in results:
        import datetime as dt
        ts = dt.datetime.fromtimestamp(b['t']/1000, tz=timezone.utc).date()
        print(f"  {ts}  close={b['c']}")

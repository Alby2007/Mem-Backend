"""Test Polygon v3 short interest, short volume, and treasury yield endpoints."""
import urllib.request
import json
import os

KEY = os.environ.get('POLYGON_API_KEY', 'dL4dmrvjQMo_bIN9VQkLfiYxlgi5y8Uo')

TESTS = [
    # Try ETF proxies for yield curve — these ARE equity tickers on any plan
    ('TLT aggs',    'https://api.polygon.io/v2/aggs/ticker/TLT/range/1/day/2026-02-01/2026-03-01?adjusted=true&sort=asc&limit=5&apiKey=' + KEY),
    ('IEF aggs',    'https://api.polygon.io/v2/aggs/ticker/IEF/range/1/day/2026-02-01/2026-03-01?adjusted=true&sort=asc&limit=5&apiKey=' + KEY),
    ('SHY aggs',    'https://api.polygon.io/v2/aggs/ticker/SHY/range/1/day/2026-02-01/2026-03-01?adjusted=true&sort=asc&limit=5&apiKey=' + KEY),
    # Forex-style rate tickers
    ('I:SPX',       'https://api.polygon.io/v2/aggs/ticker/I:SPX/range/1/day/2026-02-01/2026-03-01?apiKey=' + KEY),
    # Snapshot to check what indices are available
    ('Indices snap','https://api.polygon.io/v3/snapshot/indices?limit=5&apiKey=' + KEY),
]

for label, url in TESTS:
    print(f'\n=== {label} ===')
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            d = json.loads(r.read())
        status = d.get('status')
        count  = d.get('count') or d.get('resultsCount')
        print(f'status: {status}  count: {count}')
        results = d.get('results') or []
        if results:
            print(json.dumps(results[0], indent=2))
        else:
            print(json.dumps(d, indent=2)[:600])
    except Exception as e:
        print(f'ERROR: {e}')

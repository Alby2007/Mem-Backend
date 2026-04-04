import urllib.request, json, sys

API_KEY = 'dL4dmrvjQMo_bIN9VQkLfiYxlgi5y8Uo'
url = f'https://api.polygon.io/v3/snapshot/options/AAPL?limit=5&apiKey={API_KEY}'

try:
    with urllib.request.urlopen(url, timeout=15) as resp:
        d = json.loads(resp.read())
except Exception as e:
    print(f'ERROR fetching: {e}')
    sys.exit(1)

status = d.get('status', 'UNKNOWN')
error  = d.get('error', '')
results = d.get('results', [])

print(f'status: {status}')
if error:
    print(f'error: {error}')
print(f'results count: {len(results)}')

if results:
    r0 = results[0]
    print(f'first result keys: {list(r0.keys())}')
    greeks = r0.get('greeks', {})
    print(f'greeks: {greeks}')
    print(f'implied_volatility: {r0.get("implied_volatility")}')
    print(f'open_interest: {r0.get("open_interest")}')
    details = r0.get('details', {})
    print(f'details: {details}')

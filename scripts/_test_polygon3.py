import urllib.request, json

API_KEY = 'dL4dmrvjQMo_bIN9VQkLfiYxlgi5y8Uo'
url = f'https://api.polygon.io/v3/snapshot/options/AAPL?limit=50&apiKey={API_KEY}'

with urllib.request.urlopen(url, timeout=15) as resp:
    d = json.loads(resp.read())

print('top-level keys:', list(d.keys()))
print('underlying_asset:', d.get('underlying_asset'))

results = d.get('results', [])
with_greeks = [r for r in results if r.get('greeks') and any(r['greeks'].values())]
with_iv     = [r for r in results if r.get('implied_volatility') is not None]

print(f'\ntotal results: {len(results)}')
print(f'with greeks: {len(with_greeks)}')
print(f'with iv: {len(with_iv)}')

if with_greeks:
    print('\n--- Best ATM candidate (delta closest to 0.5) ---')
    best = min(with_greeks, key=lambda r: abs((r['greeks'].get('delta') or 0) - 0.5))
    print(f'strike: {best["details"].get("strike_price")}')
    print(f'expiry: {best["details"].get("expiration_date")}')
    print(f'type: {best["details"].get("contract_type")}')
    print(f'greeks: {best["greeks"]}')
    print(f'iv: {best.get("implied_volatility")}')
    print(f'oi: {best.get("open_interest")}')

import urllib.request, json, sys

API_KEY = 'dL4dmrvjQMo_bIN9VQkLfiYxlgi5y8Uo'

for ticker in ['AAPL', 'SPY', 'NVDA']:
    url = f'https://api.polygon.io/v3/snapshot/options/{ticker}?limit=50&apiKey={API_KEY}'
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            d = json.loads(resp.read())
    except Exception as e:
        print(f'{ticker}: ERROR {e}')
        continue

    results = d.get('results', [])
    status  = d.get('status', 'UNKNOWN')
    error   = d.get('error', '')

    with_greeks = [r for r in results if r.get('greeks') and any(r['greeks'].values())]
    with_iv     = [r for r in results if r.get('implied_volatility') is not None]

    print(f'\n{ticker}: status={status}, total={len(results)}, '
          f'with_greeks={len(with_greeks)}, with_iv={len(with_iv)}')
    if error:
        print(f'  error: {error}')
    if with_greeks:
        g = with_greeks[0]
        print(f'  sample greeks: {g["greeks"]}')
        print(f'  iv: {g.get("implied_volatility")}')
        print(f'  strike: {g["details"].get("strike_price")}, '
              f'exp: {g["details"].get("expiration_date")}, '
              f'type: {g["details"].get("contract_type")}')
    elif results:
        print(f'  NO greeks on any of {len(results)} contracts')
        print(f'  sample keys: {list(results[0].keys())}')
        print(f'  sample day: {results[0].get("day", {})}')

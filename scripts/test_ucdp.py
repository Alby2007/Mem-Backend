import urllib.request, json

# Test UCDP v1 public endpoint (no auth required)
for url in [
    'https://ucdpapi.pcr.uu.se/api/gedevents/24.01?pagesize=5&page=1',
    'https://ucdpapi.pcr.uu.se/api/conflict/24.01?pagesize=5&page=1',
    'https://ucdpapi.pcr.uu.se/api/ucdpprioconflict/24.01?pagesize=5&page=1',
]:
    print("Testing:", url)
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'TradingKB/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read().decode())
        print("  OK — keys:", list(d.keys())[:5], "| TotalCount:", d.get('TotalCount', d.get('totalCount', '?')))
    except Exception as e:
        print("  ERROR:", e)

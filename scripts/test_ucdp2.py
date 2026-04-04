import urllib.request, json

# UCDP moved to versioned dataset downloads — test public download API
urls = [
    # GED CSV download (public, no auth)
    'https://ucdpapi.pcr.uu.se/api/gedevents/23.1?pagesize=3&page=1',
    # Version 23 format
    'https://ucdpapi.pcr.uu.se/api/gedevents/23.01?pagesize=3&page=1',
    # Try without version
    'https://ucdpapi.pcr.uu.se/api/gedevents?pagesize=3&page=1',
    # UCDP PRIO conflict v22 (last truly public)
    'https://ucdpapi.pcr.uu.se/api/ucdpprioconflict/22.1?pagesize=3&page=1',
    # REST Countries - free conflict proxy
    'https://restcountries.com/v3.1/name/ukraine?fields=name',
]
for url in urls:
    print(f"GET {url}")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'TradingKB/1.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read(500).decode(errors='replace')
            print(f"  OK: {body[:200]}")
    except Exception as e:
        print(f"  FAIL: {e}")
    print()

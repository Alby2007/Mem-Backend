#!/usr/bin/env python3
"""Find correct FRED series IDs for GPR index. Run: python3 scripts/_find_gpr_fred.py"""
import json, os, sys, urllib.parse, urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
if os.path.exists(_env):
    for line in open(_env):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip())

KEY = os.environ.get('FRED_API_KEY', '')
if not KEY:
    print('No FRED_API_KEY'); sys.exit(1)

for term in ('geopolitical risk caldara', 'geopolitical risk press', 'GPR'):
    p = urllib.parse.urlencode({'search_text': term, 'api_key': KEY,
                                'file_type': 'json', 'limit': 10,
                                'search_type': 'full_text'})
    with urllib.request.urlopen(f'https://api.stlouisfed.org/fred/series/search?{p}', timeout=15) as r:
        d = json.loads(r.read())
    print(f'\n=== search: {term!r} ===')
    for s in d.get('seriess', []):
        print(f"  {s['id']:20s}  {s['title'][:70]}")

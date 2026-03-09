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

print('=== probing known GPR series IDs directly ===')
CANDIDATES = [
    'GPRD', 'GPRC_GBR', 'GPRD_AUS', 'GPRC_USA', 'GEOPOLRISK',
    'GPRUSA', 'GPRC_AUS', 'GPRC_BRA', 'GPRC_CHN', 'GPRC_FRA',
    'GPRC_DEU', 'GPRC_IND', 'GPRC_ITA', 'GPRC_JPN', 'GPRC_RUS',
]
obs_base = 'https://api.stlouisfed.org/fred/series/observations'
info_base = 'https://api.stlouisfed.org/fred/series'
for sid in CANDIDATES:
    p = urllib.parse.urlencode({
        'series_id': sid, 'api_key': KEY,
        'sort_order': 'desc', 'limit': 1, 'file_type': 'json',
    })
    try:
        with urllib.request.urlopen(f'{obs_base}?{p}', timeout=10) as r:
            d = json.loads(r.read())
        obs = d.get('observations', [])
        val = obs[0] if obs else 'no obs'
        print(f'  OK  {sid:20s}  {val}')
    except Exception as e:
        print(f'  ERR {sid:20s}  {str(e)[:60]}')

print()
print('=== FRED series search: geopolitical risk ===')
for term in ('geopolitical risk', 'caldara iacoviello'):
    p = urllib.parse.urlencode({
        'search_text': term, 'api_key': KEY,
        'file_type': 'json', 'limit': 8,
    })
    try:
        with urllib.request.urlopen(
            f'https://api.stlouisfed.org/fred/series/search?{p}', timeout=15
        ) as r:
            d = json.loads(r.read())
        print(f'\n  query: {term!r}')
        for s in d.get('seriess', []):
            print(f"    {s['id']:22s}  {s['title'][:65]}")
    except Exception as e:
        print(f'  search failed for {term!r}: {e}')

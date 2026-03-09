#!/usr/bin/env python3
"""
Check FRED for GPR series availability, then test XLS binary parse via openpyxl.
Run on OCI: python3 scripts/_test_gpr_fred.py
"""
import json
import os
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FRED_KEY = os.environ.get('FRED_API_KEY', '')

# GPR series available on FRED
GPR_SERIES = {
    'GPRH':  'GPR Headline Index',
    'GPRT':  'GPR Threats Sub-Index',
    'GPRA':  'GPR Acts Sub-Index',
}

print('=== FRED GPR series check ===')
for sid, desc in GPR_SERIES.items():
    if not FRED_KEY:
        print(f'  SKIP {sid} — no FRED_API_KEY')
        continue
    params = urllib.parse.urlencode({
        'series_id': sid,
        'api_key':   FRED_KEY,
        'sort_order': 'desc',
        'limit':     3,
        'file_type': 'json',
    })
    url = f'https://api.stlouisfed.org/fred/series/observations?{params}'
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            d = json.loads(r.read())
        obs = d.get('observations', [])
        print(f'  OK {sid} ({desc}): latest = {obs[0] if obs else "none"}')
    except Exception as e:
        print(f'  FAIL {sid}: {e}')

print()
print('=== XLS binary check (openpyxl cannot read .xls) ===')
print('  Testing raw XLS byte signature...')
try:
    req = urllib.request.Request(
        'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls',
        headers={'User-Agent': 'TradingGalaxyKB/1.0'},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        header = r.read(8)
    hex_sig = header.hex()
    print(f'  First 8 bytes: {hex_sig}')
    if hex_sig.startswith('d0cf11e0'):
        print('  -> Confirmed: legacy BIFF8 XLS (openpyxl cannot read)')
    elif hex_sig.startswith('504b0304'):
        print('  -> Actually XLSX (ZIP), openpyxl CAN read this')
    else:
        print(f'  -> Unknown format: {hex_sig}')
except Exception as e:
    print(f'  FAIL: {e}')

"""Debug yield_curve_adapter fetch — step by step."""
import sys, os
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')
try:
    from dotenv import load_dotenv
    load_dotenv('/home/ubuntu/trading-galaxy/.env')
except ImportError:
    pass

import logging
logging.basicConfig(level=logging.DEBUG)

from ingest.yield_curve_adapter import _fetch_last_two_closes, _api_key, _classify_regime, _classify_slope

key = _api_key()
print(f"API key present: {bool(key)}")

print("\n--- TLT ---")
r = _fetch_last_two_closes('TLT', key)
print(f"result: {r}")

print("\n--- SHY ---")
r2 = _fetch_last_two_closes('SHY', key)
print(f"result: {r2}")

print("\n--- IEF ---")
r3 = _fetch_last_two_closes('IEF', key)
print(f"result: {r3}")

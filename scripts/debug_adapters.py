"""Test adapter fixes directly on the server."""
import sys, os, traceback
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
import extensions as ext
ext.DB_PATH = DB

# 1. FRED direct HTTP
print('=== FRED ===')
try:
    fred_key = os.environ.get('FRED_API_KEY', '')
    print(f'Key prefix: {fred_key[:8]}...')
    from ingest.fred_adapter import _get_latest_value, _FredAuthError
    v = _get_latest_value(fred_key, 'FEDFUNDS')
    print(f'FEDFUNDS: {v}')
    v2 = _get_latest_value(fred_key, 'DGS10')
    print(f'DGS10 (10Y yield): {v2}')
except Exception:
    traceback.print_exc()

# 2. SignalDecayPredictor
print('\n=== SignalDecayPredictor ===')
try:
    from analytics.signal_decay_predictor import SignalDecayPredictor
    sdp = SignalDecayPredictor(DB)
    result = sdp.run()
    print(f'Result: {result}')
except Exception:
    traceback.print_exc()

# 3. GPR (xlrd)
print('\n=== GPR xlrd ===')
try:
    import xlrd
    print(f'xlrd version: {xlrd.__version__}')
    from ingest.gpr_adapter import _fetch_xls_bytes, _parse_xls
    raw = _fetch_xls_bytes()
    if raw:
        print(f'Downloaded {len(raw)} bytes')
        parsed = _parse_xls(raw)
        print(f'Parsed: {parsed}')
    else:
        print('Download failed')
except Exception:
    traceback.print_exc()

# 4. EIA
print('\n=== EIA ===')
try:
    eia_key = os.environ.get('EIA_API_KEY', '')
    print(f'Key prefix: {eia_key[:8]}...')
    from ingest.eia_adapter import _eia_fetch
    wti = _eia_fetch(eia_key, 'petroleum/pri/spt/data', {'product': ['EPCWTI']}, 2)
    print(f'WTI primary path: {wti}')
    if not wti:
        wti2 = _eia_fetch(eia_key, 'petroleum/pri/spt/data', {'series': ['RWTC']}, 2)
        print(f'WTI fallback path: {wti2}')
except Exception:
    traceback.print_exc()

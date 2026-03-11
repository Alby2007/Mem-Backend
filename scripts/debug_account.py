"""Debug script: call get_account directly and print any exception."""
import sys, os, traceback
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import extensions as ext
ext.DB_PATH = '/opt/trading-galaxy/data/trading_knowledge.db'

try:
    from services.paper_trading import get_account
    result = get_account('a2_0mk3r')
    print('OK:', list(result.keys()))
    print('account_size_set:', result.get('account_size_set'))
    print('virtual_balance:', result.get('virtual_balance'))
except Exception:
    traceback.print_exc()

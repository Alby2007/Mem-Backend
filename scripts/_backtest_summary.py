"""Print backtest summary without ticker_detail."""
import json, os
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analytics.backtest import run_backtest, run_regime_backtest
r = run_backtest('trading_knowledge.db')
r.pop('ticker_detail', None)
print('=== BACKTEST SUMMARY ===')
print(json.dumps(r, indent=2))
print('\n=== REGIME BACKTEST ===')
rr = run_regime_backtest('trading_knowledge.db')
print(json.dumps(rr, indent=2))

import sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
cc = sqlite3.connect(DB, timeout=5)
tk = 'coin'

PREDS = ['signal_direction','conviction_tier','price_regime',
         'volatility_regime','sector','implied_volatility',
         'put_call_oi_ratio','smart_money_signal']

ga = {}
for pred in PREDS:
    row = cc.execute(
        "SELECT object, confidence FROM facts WHERE subject=? AND predicate=? AND (object IS NOT NULL AND object != '') ORDER BY confidence DESC LIMIT 1",
        (tk, pred)
    ).fetchone()
    print(f"{pred:30s} -> {row}")
    if row:
        ga[pred] = row[0]

print('\ngrounding_atoms result:', ga)

# Also check what _extract_tickers returns for the test message
from retrieval import _extract_tickers
print('\n_extract_tickers("what is the signal on COIN?") ->', _extract_tickers('what is the signal on COIN?'))
print('_extract_tickers("COIN signal") ->', _extract_tickers('COIN signal'))

import sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
from retrieval import _extract_tickers
tests = [
    'what is the signal on COIN?',
    'COIN signal',
    'tell me about COIN',
]
for t in tests:
    print(repr(t), '->', _extract_tickers(t))

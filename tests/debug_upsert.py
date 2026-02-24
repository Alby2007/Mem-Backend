"""tests/debug_upsert.py — find exact UNIQUE constraint source"""
import sys, traceback
sys.path.insert(0, '.')
from ingest.yfinance_adapter import YFinanceAdapter
from knowledge.graph import TradingKnowledgeGraph

kg = TradingKnowledgeGraph('trading_knowledge.db')
adapter = YFinanceAdapter(['AAPL', 'META', 'GOOGL'])
try:
    result = adapter.run_and_push(kg)
    print('OK:', result)
except Exception as e:
    traceback.print_exc()
    print('ERROR:', e)

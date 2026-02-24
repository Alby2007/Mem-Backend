"""
tests/run_full_historical_backfill.py — backfill full default watchlist once.
"""
import sys
sys.path.insert(0, '.')

from ingest.historical_adapter import HistoricalBackfillAdapter
from knowledge.graph import TradingKnowledgeGraph

kg = TradingKnowledgeGraph('trading_knowledge.db')
adapter = HistoricalBackfillAdapter()
print(f'Running backfill for {len(adapter.tickers)} tickers ...')
result = adapter.run_and_push(kg)
print(f"Done — ingested={result['ingested']}  skipped={result['skipped']}")

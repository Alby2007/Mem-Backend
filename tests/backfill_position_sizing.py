"""Backfill position sizing atoms into the KB."""
import sys; sys.path.insert(0, '.')
from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
from knowledge.graph import TradingKnowledgeGraph
import sqlite3

kg = TradingKnowledgeGraph('trading_knowledge.db')
adapter = SignalEnrichmentAdapter(db_path='trading_knowledge.db')
atoms = adapter.run()
result = adapter.push(atoms, kg)
print(f"ingested={result['ingested']}  skipped={result['skipped']}")

c = sqlite3.connect('trading_knowledge.db').cursor()
for ticker in ['nvda', 'intc', 'meta', 'aapl', 'amd']:
    c.execute(
        "SELECT predicate, object FROM facts WHERE subject=? "
        "AND predicate IN ('conviction_tier','volatility_scalar','position_size_pct')",
        (ticker,)
    )
    for r in c.fetchall():
        print(f"  {ticker.upper():5s} | {r[0]:20s} | {r[1]}")

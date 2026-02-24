"""Force re-write position sizing atoms by deleting old ones first then re-running."""
import sys, sqlite3; sys.path.insert(0, '.')
from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
from knowledge.graph import TradingKnowledgeGraph

# Delete existing position sizing atoms so upsert fires cleanly
conn = sqlite3.connect('trading_knowledge.db')
c = conn.cursor()
c.execute("DELETE FROM facts WHERE predicate IN ('conviction_tier','volatility_scalar','position_size_pct')")
deleted = c.rowcount
conn.commit()
conn.close()
print(f"Deleted {deleted} stale atoms")

kg = TradingKnowledgeGraph('trading_knowledge.db')
adapter = SignalEnrichmentAdapter(db_path='trading_knowledge.db')
atoms = adapter.run()
result = adapter.push(atoms, kg)
print(f"ingested={result['ingested']}  skipped={result['skipped']}")

c = sqlite3.connect('trading_knowledge.db').cursor()
for ticker in ['nvda', 'intc', 'meta', 'aapl', 'amd', 'msft']:
    c.execute(
        "SELECT predicate, object FROM facts WHERE subject=? "
        "AND predicate IN ('conviction_tier','volatility_scalar','position_size_pct')",
        (ticker,)
    )
    rows = {r[0]: r[1] for r in c.fetchall()}
    ct  = rows.get('conviction_tier', 'MISSING')
    vs  = rows.get('volatility_scalar', 'MISSING')
    ps  = rows.get('position_size_pct', 'MISSING')
    print(f"  {ticker.upper():5s} | ct={ct:8s} | scalar={vs:6s} | size={ps}")

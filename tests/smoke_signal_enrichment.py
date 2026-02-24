"""
tests/smoke_signal_enrichment.py — verify SignalEnrichmentAdapter output
"""
import sys, sqlite3
sys.path.insert(0, '.')

from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
from knowledge.graph import TradingKnowledgeGraph

DB = 'trading_knowledge.db'
kg = TradingKnowledgeGraph(DB)
adapter = SignalEnrichmentAdapter(db_path=DB)

result = adapter.run_and_push(kg)
print(f"run_and_push: ingested={result['ingested']}  skipped={result['skipped']}")

# Show derived atoms for a sample of core tickers
SAMPLE = ['nvda', 'meta', 'aapl', 'msft', 'amd', 'xom', 'spy', 'hyg', 'tlt']
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

print()
print(f"{'TICKER':8s} {'PREDICATE':22s} {'OBJECT':20s} {'CONFIDENCE':10s} SOURCE")
print('-' * 90)
for ticker in SAMPLE:
    c.execute("""
        SELECT predicate, object, confidence, source FROM facts
        WHERE subject = ?
        AND predicate IN ('signal_quality','macro_confirmation','price_regime','upside_pct')
        ORDER BY predicate
    """, (ticker,))
    rows = c.fetchall()
    for r in rows:
        print(f"  {ticker.upper():6s}  {r['predicate']:22s} {r['object']:20s} {r['confidence']:.2f}  {r['source']}")
    if not rows:
        print(f"  {ticker.upper():6s}  (no derived atoms)")

# Verify signal_quality distribution
print()
print("=== signal_quality value distribution ===")
c.execute("""
    SELECT object, COUNT(*) n FROM facts
    WHERE predicate = 'signal_quality'
    GROUP BY object ORDER BY n DESC
""")
for r in c.fetchall():
    print(f"  {r['object']:15s}: {r['n']}")

print()
print("=== macro_confirmation value distribution ===")
c.execute("""
    SELECT object, COUNT(*) n FROM facts
    WHERE predicate = 'macro_confirmation'
    GROUP BY object ORDER BY n DESC
""")
for r in c.fetchall():
    print(f"  {r['object']:15s}: {r['n']}")

print()
print("=== price_regime value distribution ===")
c.execute("""
    SELECT object, COUNT(*) n FROM facts
    WHERE predicate = 'price_regime'
    GROUP BY object ORDER BY n DESC
""")
for r in c.fetchall():
    print(f"  {r['object']:15s}: {r['n']}")

conn.close()

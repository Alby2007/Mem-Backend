"""
tests/smoke_invalidation.py — verify invalidation layer atoms
"""
import sys, sqlite3
sys.path.insert(0, '.')

from ingest.signal_enrichment_adapter import SignalEnrichmentAdapter
from knowledge.graph import TradingKnowledgeGraph

DB = 'trading_knowledge.db'
kg = TradingKnowledgeGraph(DB)

SAMPLE = ['nvda', 'meta', 'aapl', 'msft', 'intc', 'amd', 'xom']
adapter = SignalEnrichmentAdapter(tickers=SAMPLE, db_path=DB)
result = adapter.run_and_push(kg)
print(f"run_and_push: ingested={result['ingested']}  skipped={result['skipped']}")

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

INV_PREDS = ['invalidation_price', 'invalidation_distance', 'thesis_risk_level']

print()
print(f"{'TICKER':6s} {'PREDICATE':22s} {'VALUE':18s} {'CONF':6s} RULE (from metadata)")
print('-' * 85)
for ticker in SAMPLE:
    c.execute("""
        SELECT predicate, object, confidence, source FROM facts
        WHERE subject = ? AND predicate IN ({})
        ORDER BY predicate
    """.format(','.join('?' * len(INV_PREDS))),
    (ticker, *INV_PREDS))
    rows = c.fetchall()
    for r in rows:
        print(f"  {ticker.upper():4s}  {r['predicate']:22s} {r['object']:18s} {r['confidence']:.2f}  {r['source']}")
    if not rows:
        print(f"  {ticker.upper():4s}  (no invalidation atoms)")

# Also show upside_pct alongside for asymmetry context
print()
print("=== Asymmetry picture (upside_pct vs invalidation_distance) ===")
print(f"{'TICKER':6s} {'upside_pct':12s} {'inv_dist':12s} {'risk_level':12s} {'signal_quality':14s}")
print('-' * 60)
for ticker in SAMPLE:
    c.execute("""
        SELECT predicate, object FROM facts
        WHERE subject = ? AND predicate IN ('upside_pct','invalidation_distance','thesis_risk_level','signal_quality')
    """, (ticker,))
    d = {r['predicate']: r['object'] for r in c.fetchall()}
    print(f"  {ticker.upper():4s}  {d.get('upside_pct','n/a'):12s} {d.get('invalidation_distance','n/a'):12s} "
          f"{d.get('thesis_risk_level','n/a'):12s} {d.get('signal_quality','n/a')}")

# Distribution check
print()
print("=== thesis_risk_level distribution across all tickers ===")
c.execute("SELECT object, COUNT(*) n FROM facts WHERE predicate='thesis_risk_level' GROUP BY object ORDER BY n DESC")
for r in c.fetchall():
    print(f"  {r['object']:12s}: {r['n']}")

conn.close()

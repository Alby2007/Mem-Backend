import os, sys
sys.path.insert(0, '.')
os.environ['FRED_API_KEY'] = '56e13d48cb2ceea001dda377893b469a'
from ingest.fred_adapter import FREDAdapter
from knowledge.graph import TradingKnowledgeGraph
kg = TradingKnowledgeGraph('trading_knowledge.db')
adapter = FREDAdapter()
atoms = adapter.run()
result = adapter.push(atoms, kg)
print(f"FRED: ingested={result['ingested']} skipped={result['skipped']}")
if atoms:
    for a in atoms[:10]:
        print(f"  {a.subject} | {a.predicate} | {a.object} (conf={a.confidence})")

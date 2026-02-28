import sys, logging
logging.basicConfig(level=logging.WARNING)
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

from ingest.rss_adapter import RSSAdapter

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
r = RSSAdapter(db_path=DB)
atoms = r.fetch()
print("atoms fetched:", len(atoms))
for a in atoms[:5]:
    print(" ", a.subject, "|", a.predicate, "|", a.object[:80])

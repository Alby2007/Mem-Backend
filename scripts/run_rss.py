import sys, sqlite3, logging
logging.basicConfig(level=logging.INFO)
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

from ingest.rss_adapter import RSSAdapter, HAS_FEEDPARSER
print("feedparser available:", HAS_FEEDPARSER)

r = RSSAdapter(conn)
try:
    result = r.run()
    print("run() result:", result)
except Exception as e:
    print("run() error:", e)
    import traceback; traceback.print_exc()

# Count news_wire atoms after
c = conn.cursor()
c.execute("SELECT subject, COUNT(*) FROM facts WHERE subject LIKE 'news_wire%' GROUP BY subject")
rows = c.fetchall()
print("news_wire atoms:", rows)
c.execute("SELECT subject, COUNT(*) FROM facts WHERE source LIKE 'news_wire%' GROUP BY subject LIMIT 5")
print("atoms by news_wire source:", c.fetchall())

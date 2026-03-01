"""
Run a geo retrieval for 'tell me about the war in russia' and print the snippet.
Usage: python3 scripts/test_geo_snippet.py
"""
import os, sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.environ.setdefault('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')

from retrieval import retrieve
from knowledge.knowledge_graph import KnowledgeGraph

db = os.environ['TRADING_KB_DB']
kg = KnowledgeGraph(db_path=db)
conn = kg.thread_local_conn()

snippet, atoms = retrieve('tell me about the war in russia', conn, limit=30)

print(f"=== ATOMS RETURNED: {len(atoms)} ===")
print()
print(snippet[:4000])

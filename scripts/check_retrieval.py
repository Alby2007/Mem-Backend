"""Test retrieval for gold-related queries."""
import sys, os, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')

from retrieval import retrieve

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)

queries = ['gold market', 'gold', 'gld', 'xauusd', 'tell me about gold', 'BP.L outlook']
for q in queries:
    result = retrieve(q, conn, limit=5)
    snippet = result.get('snippet', '') if isinstance(result, dict) else str(result)
    atoms = result.get('atom_count', 0) if isinstance(result, dict) else '?'
    print(f"Q={q!r:40} atoms={atoms}  snippet[:100]={snippet[:100]!r}")

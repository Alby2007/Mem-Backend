"""Test retrieval for gold-related queries."""
import sys, os
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.chdir('/home/ubuntu/trading-galaxy')

from retrieval import retrieve

queries = ['gold market', 'gold', 'gld', 'xauusd', 'tell me about gold']
for q in queries:
    result = retrieve(q, limit=5)
    snippet = result.get('snippet', '') if isinstance(result, dict) else str(result)
    atoms = result.get('atom_count', 0) if isinstance(result, dict) else '?'
    print(f"Q={q!r:40} atoms={atoms}  snippet[:80]={snippet[:80]!r}")

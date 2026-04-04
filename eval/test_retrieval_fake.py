#!/usr/bin/env python3
"""Test what atoms retrieval returns for fake ticker queries."""
import sys
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
from retrieval import retrieve
import sqlite3

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

for query in ['What does the KB say about FAKECO', "What's the signal on NOTREAL99"]:
    snippet, atoms = retrieve(query, conn, limit=30)
    print(f'Query: {query}')
    print(f'Atoms returned: {len(atoms)}')
    for a in atoms[:10]:
        subj = a['subject']
        pred = a['predicate']
        obj = str(a['object'])[:60]
        print(f'  {subj} | {pred} | {obj}')
    print('Snippet preview:')
    print(snippet[:800])
    print('---')

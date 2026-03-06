#!/usr/bin/env python3
"""Check and delete fake ticker atoms from the KB facts table."""
import sqlite3
import os

DB = os.environ.get('KB_PATH', '/opt/trading-galaxy/data/trading_knowledge.db')

PATTERNS = ['FAKE%', 'NOTREAL%', 'BLOB%', 'RANDOM%', 'MADEUP%', 'TESTCO%', 'NODATA%']

conn = sqlite3.connect(DB)
cur = conn.cursor()

print('=== Fake ticker atoms in KB ===')
total = 0
for pat in PATTERNS:
    cur.execute('SELECT subject, predicate, object FROM facts WHERE subject LIKE ?', (pat,))
    rows = cur.fetchall()
    if rows:
        for r in rows:
            print(f'  {r[0]} | {r[1]} | {r[2][:60]}')
        total += len(rows)

print(f'\nTotal fake atoms found: {total}')

if total > 0:
    confirm = input('Delete all? [y/N] ').strip().lower()
    if confirm == 'y':
        for pat in PATTERNS:
            cur.execute('DELETE FROM facts WHERE subject LIKE ?', (pat,))
        conn.commit()
        print('Deleted.')
else:
    print('Nothing to delete.')

conn.close()

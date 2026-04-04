#!/usr/bin/env python3
"""
Remove fake/test ticker atoms from the production KB.
These leak into no_data eval queries and cause the model to hallucinate data.

Run on OCI: python3 eval/clean_kb_fakes.py
"""
import sqlite3
import os

DB = os.environ.get('KB_PATH', '/opt/trading-galaxy/data/trading_knowledge.db')

FAKE_TICKERS = [
    'FAKECO', 'BLOBCORP99', 'RANDOMTICKER123', 'NOTREAL99', 'MADEUPTICKER',
    'ZZNOTREAL', 'XYZFAKE', 'TESTCO', 'FAKECORP', 'NODATA99',
]

conn = sqlite3.connect(DB)
cur = conn.cursor()

total_deleted = 0
for t in FAKE_TICKERS:
    cur.execute('SELECT COUNT(*) FROM facts WHERE subject = ?', (t,))
    n = cur.fetchone()[0]
    if n:
        print(f'  Deleting {n} atoms for {t}')
        cur.execute('DELETE FROM facts WHERE subject = ?', (t,))
        total_deleted += n

conn.commit()
conn.close()
print(f'Done. Removed {total_deleted} fake ticker atoms.')

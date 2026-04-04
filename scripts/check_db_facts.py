#!/usr/bin/env python3
"""Check which DB file has the live KB facts."""
import sqlite3, glob, os

for path in sorted(glob.glob('/home/ubuntu/trading-galaxy/*.db')):
    size = os.path.getsize(path)
    try:
        c = sqlite3.connect(path, timeout=3)
        facts = c.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
        patterns = 0
        try:
            patterns = c.execute('SELECT COUNT(*) FROM pattern_signals').fetchone()[0]
        except Exception:
            pass
        last_price = c.execute("SELECT COUNT(DISTINCT subject) FROM facts WHERE predicate='last_price'").fetchone()[0]
        c.close()
        print(f'{path} ({size//1024}KB): facts={facts}, last_price_subjects={last_price}, patterns={patterns}')
    except Exception as e:
        print(f'{path} ({size//1024}KB): ERROR {e}')

"""
scripts/backfill_earnings_date.py

One-shot migration: re-key existing earnings_quality atoms that contain
'next_earnings: YYYY-MM-DD' into clean earnings_date + earnings_proximity_days
predicates. Run once after the yfinance_adapter earnings fix is deployed.

Usage:
    python3 scripts/backfill_earnings_date.py
"""

import sqlite3
from datetime import datetime, date

DB = '/opt/trading-galaxy/data/trading_knowledge.db'


def main():
    conn = sqlite3.connect(DB, timeout=10)
    conn.execute('PRAGMA journal_mode=WAL')
    now = datetime.utcnow().isoformat()
    today = date.today()

    rows = conn.execute("""
        SELECT subject, object FROM facts
        WHERE predicate = 'earnings_quality'
          AND object LIKE 'next_earnings: %'
          AND timestamp > datetime('now', '-30 days')
    """).fetchall()

    inserted = 0
    for subject, obj in rows:
        date_str = obj.replace('next_earnings: ', '').strip()[:10]
        try:
            ed = datetime.strptime(date_str, '%Y-%m-%d').date()
            days_to = (ed - today).days
        except ValueError:
            continue

        conn.execute("""
            INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
            VALUES (?, 'earnings_date', ?, 0.85, 'backfill_earnings_date', ?)
            ON CONFLICT(subject, predicate, object) DO UPDATE SET
              source=excluded.source, timestamp=excluded.timestamp
        """, (subject, date_str, now))

        if 0 <= days_to <= 90:
            conn.execute("""
                INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
                VALUES (?, 'earnings_proximity_days', ?, 0.90, 'backfill_earnings_proximity', ?)
                ON CONFLICT(subject, predicate, object) DO UPDATE SET
                  source=excluded.source, timestamp=excluded.timestamp
            """, (subject, str(days_to), now))

        inserted += 1

    conn.commit()
    conn.close()
    print(f'Backfilled {inserted} earnings_date atoms')


if __name__ == '__main__':
    main()

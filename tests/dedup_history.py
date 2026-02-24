"""
tests/dedup_history.py — deduplicate historical price/target/signal rows.

For each (subject, predicate, source) group, keep only the row with the
most recent timestamp. Delete all older duplicates and rebuild FTS index.
"""
import sqlite3

DB = 'trading_knowledge.db'
conn = sqlite3.connect(DB, timeout=30)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA busy_timeout=30000')
c = conn.cursor()

UPSERT_PREDICATES = ('last_price', 'price_target', 'signal_direction', 'volatility_regime', 'earnings_quality')

before = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
print(f"Facts before dedup: {before}")

deleted_total = 0
for pred in UPSERT_PREDICATES:
    # Find all (subject, source) groups that have more than one row
    c.execute("""
        SELECT subject, source, COUNT(*) n, MAX(timestamp) latest_ts
        FROM facts
        WHERE predicate = ?
        GROUP BY subject, source
        HAVING n > 1
    """, (pred,))
    groups = c.fetchall()

    for row in groups:
        subj, src, count, latest_ts = row[0], row[1], row[2], row[3]
        # Get the id to KEEP (most recent timestamp)
        c.execute("""
            SELECT id FROM facts
            WHERE subject = ? AND predicate = ? AND source = ?
            ORDER BY timestamp DESC LIMIT 1
        """, (subj, pred, src))
        keep_id = c.fetchone()[0]

        # Delete all other rows for this (subj, pred, src)
        c.execute("""
            DELETE FROM facts
            WHERE subject = ? AND predicate = ? AND source = ? AND id != ?
        """, (subj, pred, src, keep_id))
        deleted = c.rowcount
        deleted_total += deleted

        # Clean up FTS for deleted rows (rebuild below)

conn.commit()

# Rebuild FTS index from scratch
print(f"Deleted {deleted_total} duplicate rows")
print("Rebuilding FTS index...")
conn.execute("INSERT INTO facts_fts(facts_fts) VALUES('rebuild')")
conn.commit()

after = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
print(f"Facts after dedup:  {after}  (removed {before - after})")

# Verify: check last_price row counts for key tickers
print()
print("last_price row counts after dedup (should all be 1):")
c.execute("""
    SELECT subject, COUNT(*) n FROM facts
    WHERE predicate = 'last_price'
    AND subject IN ('aapl','msft','googl','amzn','nvda','meta','tsla','jpm','v','ma','unh')
    GROUP BY subject ORDER BY subject
""")
for r in c.fetchall():
    flag = ' ← STILL DUPED' if r[1] > 1 else ' ✓'
    print(f"  {r[0].upper():8s}: {r[1]}{flag}")

conn.close()

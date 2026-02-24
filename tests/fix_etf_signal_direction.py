"""
Remove stale signal_direction atoms written by the ETF path in yfinance_adapter.
These should be price_regime atoms — the vocabulary mismatch fix.

ETF subjects: all tickers in the yfinance _ETF_QUOTE_TYPES path.
We identify them by the source prefix exchange_feed_yahoo and
a signal_direction value of near_high / mid_range / near_low
(i.e. NOT long/short/neutral which are equity directional values).
"""
import sqlite3, sys
sys.path.insert(0, '.')

conn = sqlite3.connect('trading_knowledge.db')
c = conn.cursor()

# Delete signal_direction atoms that contain price-regime vocabulary
# (near_high, mid_range, near_low) — these are the mis-labelled ETF atoms
c.execute("""
    DELETE FROM facts
    WHERE predicate = 'signal_direction'
      AND object IN ('near_high', 'mid_range', 'near_low')
      AND source LIKE 'exchange_feed_yahoo%'
""")
deleted = c.rowcount
conn.commit()

print(f"Deleted {deleted} stale ETF signal_direction atoms")

# Verify no near_high/mid_range/near_low values remain in signal_direction
c.execute("""
    SELECT COUNT(*) FROM facts
    WHERE predicate = 'signal_direction'
      AND object IN ('near_high', 'mid_range', 'near_low')
""")
remaining = c.fetchone()[0]
print(f"Remaining mis-labelled signal_direction atoms: {remaining}")

# Show remaining signal_direction vocabulary
c.execute("""
    SELECT object, COUNT(*) FROM facts
    WHERE predicate = 'signal_direction'
    GROUP BY object ORDER BY COUNT(*) DESC
""")
print("\nCurrent signal_direction values in KB:")
for r in c.fetchall():
    print(f"  {r[1]:4d}  {r[0]}")

conn.close()

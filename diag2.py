import sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
import extensions as ext

conn = sqlite3.connect(ext.DB_PATH)
conn.row_factory = sqlite3.Row

# What distinct tickers are in pattern_signals (active)?
rows = conn.execute(
    "SELECT ticker, COUNT(*) as cnt, MAX(quality_score) as max_q "
    "FROM pattern_signals WHERE status NOT IN ('filled','broken') "
    "GROUP BY ticker ORDER BY max_q DESC"
).fetchall()
print(f"=== Active pattern_signals tickers: {len(rows)} ===")
for r in rows:
    print(f"  {r['ticker']:15} cnt={r['cnt']} max_q={r['max_q']:.3f}")

# Check what suffixes exist
suffixes = {}
for r in rows:
    t = r['ticker']
    if '.' in t:
        sfx = '.' + t.split('.')[-1]
        suffixes[sfx] = suffixes.get(sfx, 0) + 1
    else:
        suffixes['(none/US)'] = suffixes.get('(none/US)', 0) + 1
print(f"\n=== Ticker suffixes ===")
for s, c in sorted(suffixes.items()):
    print(f"  {s}: {c}")

# Test _is_market_open on current candidates
from services.paper_trading import _is_market_open
print(f"\n=== Market open check (now) ===")
for r in rows[:20]:
    t = r['ticker']
    print(f"  {t:15} open={_is_market_open(t)}")

conn.close()

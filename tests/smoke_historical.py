"""
tests/smoke_historical.py — verify HistoricalBackfillAdapter output
"""
import sys, sqlite3
sys.path.insert(0, '.')

from ingest.historical_adapter import HistoricalBackfillAdapter
from knowledge.graph import TradingKnowledgeGraph

DB = 'trading_knowledge.db'
kg = TradingKnowledgeGraph(DB)

# Run on a small subset first so the smoke test is fast (~5s download)
SAMPLE = ['NVDA', 'META', 'AAPL', 'MSFT', 'XOM', 'SPY', 'TLT']
adapter = HistoricalBackfillAdapter(tickers=SAMPLE)
result = adapter.run_and_push(kg)
print(f"run_and_push: ingested={result['ingested']}  skipped={result['skipped']}")

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

HIST_PREDICATES = [
    'return_1w', 'return_1m', 'return_3m', 'return_6m', 'return_1y',
    'volatility_30d', 'volatility_90d', 'drawdown_from_52w_high',
    'high_52w', 'low_52w', 'price_6m_ago', 'price_1y_ago',
    'avg_volume_30d', 'return_vs_spy_1m', 'return_vs_spy_3m',
]

print()
print(f"{'TICKER':6s} {'PREDICATE':25s} {'VALUE':15s} CONF")
print('-' * 62)
for ticker in SAMPLE:
    c.execute("""
        SELECT predicate, object, confidence FROM facts
        WHERE subject = ? AND predicate IN ({})
        ORDER BY predicate
    """.format(','.join('?' * len(HIST_PREDICATES))),
    (ticker.lower(), *HIST_PREDICATES))
    rows = c.fetchall()
    for r in rows:
        print(f"  {ticker:4s}  {r['predicate']:25s} {r['object']:15s} {r['confidence']:.2f}")
    if not rows:
        print(f"  {ticker:4s}  (no historical atoms)")

# Distribution of historical predicate coverage
print()
print("=== Historical predicate coverage across all tickers ===")
for pred in ['return_1m', 'return_3m', 'return_1y', 'volatility_30d',
             'drawdown_from_52w_high', 'return_vs_spy_1m']:
    c.execute("SELECT COUNT(*) FROM facts WHERE predicate = ?", (pred,))
    n = c.fetchone()[0]
    print(f"  {pred:30s}: {n}")

conn.close()
print("\nDone.")

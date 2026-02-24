"""Direct simulation of the pinned pre-fetch for META/MSFT."""
import sys, sqlite3; sys.path.insert(0, '.')

conn = sqlite3.connect('trading_knowledge.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

_PINNED_PREDICATES = (
    'last_price', 'price_target', 'signal_direction', 'earnings_quality',
    'signal_quality', 'macro_confirmation', 'price_regime', 'upside_pct',
    'return_1m', 'return_3m', 'return_6m', 'return_1y',
    'volatility_30d', 'volatility_90d', 'drawdown_from_52w_high',
    'return_vs_spy_1m', 'return_vs_spy_3m',
    'invalidation_price', 'invalidation_distance', 'thesis_risk_level',
)
ph = ','.join('?' * len(_PINNED_PREDICATES))

for ticker in ['meta', 'msft']:
    c.execute(f"""
        SELECT subject, predicate, object, confidence
        FROM facts
        WHERE LOWER(subject) = ?
        AND predicate IN ({ph})
        ORDER BY confidence DESC
    """, (ticker, *_PINNED_PREDICATES))
    rows = c.fetchall()
    print(f"\n{ticker.upper()} — {len(rows)} pinned rows:")
    for r in rows:
        print(f"  {r['predicate']:25s} | {r['object']:20s} | conf={r['confidence']:.2f}")

conn.close()

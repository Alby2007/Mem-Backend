"""
tests/debug_retrieval_direct.py — test retrieval directly (no server needed)
"""
import sys, sqlite3
sys.path.insert(0, '.')

from knowledge.graph import TradingKnowledgeGraph
from retrieval import retrieve

kg = TradingKnowledgeGraph('trading_knowledge.db')
conn = kg.thread_local_conn()

QUERIES = [
    "Compare AMD, NVDA and INTC on return_1y, return_3m, and volatility_90d.",
    "Which tickers have outperformed SPY over the last month? Rank by return_vs_spy_1m.",
    "Compare XOM and CVX versus NVDA and AMD on return_3m and return_vs_spy_3m.",
]

HIST_PREDS = {
    'return_1m','return_3m','return_6m','return_1y','return_1w',
    'volatility_30d','volatility_90d','drawdown_from_52w_high',
    'return_vs_spy_1m','return_vs_spy_3m','high_52w','low_52w',
    'price_6m_ago','price_1y_ago','avg_volume_30d',
}

for q in QUERIES:
    snippet, atoms = retrieve(q, conn, limit=50)
    hist = [a for a in atoms if a.get('predicate') in HIST_PREDS]
    print(f"\nQ: {q[:80]}")
    print(f"  total atoms={len(atoms)}  historical={len(hist)}")
    for a in hist[:15]:
        print(f"    {a['subject']:6s} | {a['predicate']:25s} | {a['object']}")

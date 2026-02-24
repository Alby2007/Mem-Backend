"""
tests/debug_historical_retrieval.py — inspect what retrieval returns for historical queries
"""
import sys, sqlite3
sys.path.insert(0, '.')

import requests

BASE = 'http://127.0.0.1:5050'

queries = [
    "Compare AMD, NVDA and INTC on return_1y, return_3m, and volatility_90d.",
    "Which tickers have outperformed SPY over the last month? Rank by return_vs_spy_1m.",
    "Compare XOM and CVX versus NVDA and AMD on return_3m and return_vs_spy_3m.",
]

for q in queries:
    r = requests.post(f"{BASE}/retrieve", json={"message": q, "session_id": "debug_hist"})
    d = r.json()
    atoms = d.get("atoms", [])
    print(f"\nQ: {q[:80]}")
    print(f"  atoms returned: {len(atoms)}")
    hist_preds = {'return_1m','return_3m','return_6m','return_1y',
                  'volatility_30d','volatility_90d','drawdown_from_52w_high',
                  'return_vs_spy_1m','return_vs_spy_3m','high_52w','low_52w',
                  'price_6m_ago','price_1y_ago','avg_volume_30d'}
    hist_atoms = [a for a in atoms if a.get('predicate') in hist_preds]
    print(f"  historical atoms in result: {len(hist_atoms)}")
    for a in hist_atoms:
        print(f"    {a['subject']:6s} | {a['predicate']:25s} | {a['object']}")
    if not hist_atoms:
        print("  (all returned atoms):")
        for a in atoms[:10]:
            print(f"    {a['subject']:6s} | {a['predicate']:25s} | {a['object'][:40]}")

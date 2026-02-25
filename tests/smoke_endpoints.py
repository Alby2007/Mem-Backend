"""Quick smoke test of new endpoints against the live server."""
import urllib.request, json, sys

BASE = 'http://localhost:5050'

def get(path):
    with urllib.request.urlopen(BASE + path, timeout=15) as r:
        return json.loads(r.read())

print('=== /analytics/backtest?window=1m ===')
d = get('/analytics/backtest?window=1m')
print(f"  methodology:     {d['methodology']}")
print(f"  methodology_note present: {'methodology_note' in d}")
print(f"  alpha_threshold: {d['alpha_threshold_pp']}")
print(f"  alpha_signal:    {d['alpha_signal']}")
print(f"  alpha_explanation: {d['alpha_explanation']}")
print(f"  total_tickers:   {d['total_tickers']}")
print(f"  portfolio_return: {d['portfolio_return']}")
print(f"  portfolio_vs_spy: {d['portfolio_vs_spy']}")
print()
print('  Cohorts (non-empty):')
for k, v in d['cohorts'].items():
    if v['n'] > 0:
        print(f"    {k:<20s} n={v['n']} mean={v['mean_return']} hit={v['hit_rate']}")

print()
print('=== /portfolio/summary ===')
p = get('/portfolio/summary')
lb = p['long_book']
ab = p['avoid_book']
print(f"  long_book.tickers:             {lb['tickers']}")
print(f"  long_book.total_position_pct:  {lb['total_position_pct']}")
print(f"  total_position_pct_note present: {'total_position_pct_note' in lb}")
print(f"  avg_conviction_weighted_upside: {lb['avg_conviction_weighted_upside']}")
print(f"  conviction_tier breakdown:     {lb['conviction_tier']}")
print(f"  signal_quality breakdown:      {lb['signal_quality']}")
print(f"  avoid_book.tickers:            {ab['tickers']}")
print(f"  avoid_book.names:              {ab['names']}")
print(f"  sector_weights keys:           {list(p['sector_weights'].keys())[:5]}")
print(f"  macro_alignment:               {p['macro_alignment']}")
print(f"  top_conviction[0]:             {p['top_conviction'][0] if p['top_conviction'] else 'empty'}")

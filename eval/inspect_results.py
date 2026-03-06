import json, sys

path = sys.argv[1] if len(sys.argv) > 1 else 'eval/results/eval_20260303_042414.json'
with open(path) as f:
    results = json.load(f)

intent = sys.argv[2] if len(sys.argv) > 2 else 'portfolio_review'
rows = [r for r in results if r['intent'] == intent]
print(f'{intent}: {len(rows)} rows')
passing = [r for r in rows if r['pass']]
print(f'  passing: {len(passing)}')
for r in rows[:10]:
    cov = r['scores'].get('holdings_coverage', 'N/A')
    full = r['scores'].get('info_full_coverage', 'N/A')
    sig = r['scores'].get('has_signal', 'N/A')
    ranked = r['scores'].get('is_ranked', 'N/A')
    print(f"  pass={r['pass']} cov={cov} full={full} ranked={ranked} sig={sig}")
    print(f"    q={r['query'][:60]}")
    print(f"    preview={r['response_preview'][:150]}")
    print()

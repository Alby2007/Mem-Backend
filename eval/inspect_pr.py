import json, re
from collections import Counter

with open('/home/ubuntu/trading-galaxy/eval/results/eval_20260305_165630.json') as f:
    results = json.load(f)

pr_pass = [r for r in results if r['intent'] == 'portfolio_review' and r.get('pass')]
pr_fail = [r for r in results if r['intent'] == 'portfolio_review' and not r.get('pass')]
print(f'Passing: {len(pr_pass)}, Failing: {len(pr_fail)}')

cov_pass = [round(r['scores'].get('holdings_coverage', 0), 2) for r in pr_pass]
print('Coverage distribution (passing):', sorted(Counter(cov_pass).items()))
print()

# Break down by what mechanism drove the pass
agg_kws = ('how is my', 'give me an overview', 'overview of my',
           'how am i doing', 'portfolio doing', 'portfolio performance')
single_kws = ('which', 'best', 'top', 'strongest', 'highest', 'most',
              'worst', 'biggest', 'lowest', 'largest')

for r in pr_pass:
    q = r.get('query', '').lower()
    r['_mech'] = ('aggregate' if any(k in q for k in agg_kws) else
                  'single_best' if any(k in q for k in single_kws) else 'ratio')

by_mech = Counter(r['_mech'] for r in pr_pass)
print('Pass mechanism breakdown:', dict(by_mech))
print()

print('=== RATIO passes (genuine coverage >= 0.25) ===')
for r in [x for x in pr_pass if x['_mech'] == 'ratio'][:6]:
    cov = round(r['scores'].get('holdings_coverage', 0), 2)
    sz = r.get('portfolio_size', 0)
    approx = round(cov * sz) if sz else '?'
    preview = r.get('response_preview', '')[:280]
    print(f"Q: {r['query']}  |  cov={cov}  |  size={sz}  |  ~{approx} tickers")
    print(f"R: {preview}")
    print()

print('=== AGGREGATE auto-passes ===')
for r in [x for x in pr_pass if x['_mech'] == 'aggregate'][:3]:
    preview = r.get('response_preview', '')[:280]
    print(f"Q: {r['query']}  |  size={r.get('portfolio_size')}")
    print(f"R: {preview}")
    print()

print('=== SINGLE_BEST passes ===')
for r in [x for x in pr_pass if x['_mech'] == 'single_best'][:3]:
    cov = round(r['scores'].get('holdings_coverage', 0), 2)
    preview = r.get('response_preview', '')[:280]
    print(f"Q: {r['query']}  |  cov={cov}  |  size={r.get('portfolio_size')}")
    print(f"R: {preview}")
    print()

print('=== FAILURES ===')
for r in pr_fail[:3]:
    cov = round(r['scores'].get('holdings_coverage', 0), 2)
    preview = r.get('response_preview', '')[:280]
    print(f"Q: {r['query']}  |  cov={cov}  |  size={r.get('portfolio_size')}")
    print(f"R: {preview}")
    print()

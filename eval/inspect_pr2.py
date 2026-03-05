import json
from collections import Counter

with open('/home/ubuntu/trading-galaxy/eval/results/eval_20260305_165630.json') as f:
    results = json.load(f)

pr_pass = [r for r in results if r['intent'] == 'portfolio_review' and r.get('pass')]

agg_kws = ('how is my', 'give me an overview', 'overview of my',
           'how am i doing', 'portfolio doing', 'portfolio performance')
single_kws = ('which', 'best', 'top', 'strongest', 'highest', 'most',
              'worst', 'biggest', 'lowest', 'largest')

for r in pr_pass:
    q = r.get('query', '').lower()
    r['_mech'] = ('aggregate' if any(k in q for k in agg_kws) else
                  'single_best' if any(k in q for k in single_kws) else 'ratio')

agg_passes = [r for r in pr_pass if r['_mech'] == 'aggregate']

print(f'=== ALL {len(agg_passes)} AGGREGATE auto-passes (full response_preview) ===')
for i, r in enumerate(agg_passes):
    prev = r.get('response_preview', '')
    print(f"[{i+1}] Q: {r['query']}  |  portfolio_size={r.get('portfolio_size')}  |  portfolio_idx={r.get('portfolio_idx')}")
    print(f"     R: {prev[:400]}")
    print()

# Check for suspicious repeated numbers across aggregate passes
import re
all_numbers = []
for r in agg_passes:
    prev = r.get('response_preview', '')
    nums = re.findall(r'\d+\.\d+', prev)
    all_numbers.extend(nums)

num_counts = Counter(all_numbers)
print('=== Numbers appearing in 3+ aggregate responses (suspicious shared atoms) ===')
for num, count in sorted(num_counts.items(), key=lambda x: -x[1]):
    if count >= 3:
        print(f"  {num}  appears in {count} responses")

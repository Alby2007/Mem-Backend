"""Inspect failing responses for a given intent from the latest eval results."""
import json, sys, glob, os

intent_filter = sys.argv[1] if len(sys.argv) > 1 else None
results_dir = os.path.join(os.path.dirname(__file__), 'results')
files = sorted(glob.glob(os.path.join(results_dir, 'eval_*.json')))
if not files:
    print('No results found'); sys.exit(1)

latest = files[-1]
print(f'Reading: {latest}\n')
with open(latest) as f:
    results = json.load(f)

fails = [r for r in results if not r['pass'] and (not intent_filter or r['intent'] == intent_filter)]
print(f'{len(fails)} failures' + (f' for intent={intent_filter}' if intent_filter else '') + '\n')

for r in fails[:8]:
    print(f"intent={r['intent']}  ticker={r.get('ticker','')}  level={r.get('trader_level','')}")
    print(f"Q: {r['query']}")
    print(f"failing: {[k for k,v in r['scores'].items() if v is False or (isinstance(v,float) and v < 0.5)]}")
    print(f"R: {r.get('response_preview','')[:300]}")
    print()

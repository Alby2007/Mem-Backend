"""Replay the updated scorer against the latest result file without re-running the harness."""
import json, os, sys
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scorers import score_response, is_pass

results_dir = os.path.join(os.path.dirname(__file__), 'results')
files = sorted([f for f in os.listdir(results_dir) if f.endswith('.json')], reverse=True)

for fname in files:
    with open(os.path.join(results_dir, fname)) as f:
        results = json.load(f)
    if not results or results[0].get('response_preview', '') == '':
        continue
    print(f"replaying {fname}  ({len(results)} records)\n")
    break

from collections import defaultdict
intent_pass = defaultdict(lambda: [0, 0])
nd_fail = []

for r in results:
    intent = r['intent']
    response = r.get('response_preview', '')
    # Reconstruct portfolio stub for scorers that need it
    portfolio = {'holdings': [], 'cash': 0, 'currency': 'USD', 'trader_level': r.get('trader_level', 'developing')}
    ticker = r.get('ticker')

    new_scores = score_response(response, intent, portfolio, ticker, kb_has_yield=True)
    new_pass = is_pass(new_scores)
    intent_pass[intent][1] += 1
    if new_pass:
        intent_pass[intent][0] += 1
    if intent == 'no_data' and not new_pass:
        nd_fail.append((r['query'], new_scores, response[:200]))

print("=== REPLAYED SCORES ===")
for intent, (p, t) in sorted(intent_pass.items()):
    old_p = sum(1 for r in results if r['intent'] == intent and r['pass'])
    print(f"  {intent:20s}  {p}/{t} ({100*p//t}%)  [was {old_p}/{t} ({100*old_p//t}%)]")

print(f"\nno_data failures remaining: {len(nd_fail)}")
for q, sc, prev in nd_fail[:8]:
    print(f"\n  Q: {q}")
    print(f"     gives_no_data: {sc.get('gives_no_data_response')}  no_invented: {sc.get('no_invented_data')}")
    print(f"     preview: {prev[:180]}")

#!/usr/bin/env python3
"""Re-score existing eval results with updated scorers to project improvement."""
import json, sys, os
EVAL_DIR = '/home/ubuntu/trading-galaxy/eval'
REPO_DIR = '/home/ubuntu/trading-galaxy'
sys.path.insert(0, EVAL_DIR)
sys.path.insert(0, REPO_DIR)

from scorers import score_response, is_pass, _FLOAT_THRESHOLDS, _DEFAULT_FLOAT_THRESHOLD
from collections import defaultdict

RESULTS_FILE = os.path.join(EVAL_DIR, 'results', 'eval_20260305_150406.json')

with open(RESULTS_FILE) as f:
    results = json.load(f)

by_intent = defaultdict(list)
total_pass = 0
total = 0

TARGET = {
    'no_data': 0.95,
    'single_ticker': 0.85,
    'portfolio_review': 0.80,
    'opportunity': 0.80,
    'geo': 0.75,
    'greeks': 0.85,
    'macro': 0.90,
}

for r in results:
    if r.get('error'):
        by_intent[r['intent']].append(False)
        total += 1
        continue
    intent = r.get('intent', '')
    portfolio = {'holdings': [], 'tier': r.get('tier', 'basic'), 'trader_level': r.get('trader_level', 'developing')}
    ticker = r.get('ticker')
    response = r.get('response_preview', '') + ('...' if r.get('response_length', 0) > 300 else '')
    try:
        scores = score_response(response, intent, portfolio, ticker, kb_has_yield=False)
        passed = is_pass(scores)
    except Exception as e:
        passed = r.get('pass', False)
    by_intent[intent].append(passed)
    if passed:
        total_pass += 1
    total += 1

print('=' * 64)
print('REPLAYED SCORES (updated scorers)')
print('=' * 64)
print(f'{"Intent":<20} {"Pass":>5} {"Total":>6} {"Rate":>7} {"Target":>8} {"Status":>8}')
print('-' * 64)
all_ok = True
for intent, passes in sorted(by_intent.items()):
    n = len(passes)
    p = sum(passes)
    rate = p / n if n else 0
    target = TARGET.get(intent, 0.8)
    ok = rate >= target
    if not ok:
        all_ok = False
    status = '✓' if ok else '✗ BELOW'
    print(f'{intent:<20} {p:>5} {n:>6} {100*rate:>6.1f}% {100*target:>7.0f}%  {status}')
print('-' * 64)
print(f'Overall: {total_pass}/{total} ({100*total_pass/total:.1f}%)')
print('=' * 64)

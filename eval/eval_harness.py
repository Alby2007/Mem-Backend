"""
eval/eval_harness.py — Eval harness for Trading Galaxy KB chat.

Runs parameterised queries against a LOCAL running API server, scores each
response against per-intent rules, and outputs timestamped JSON + summary.

Prerequisites:
  1. Local server running: python api.py (or gunicorn)
  2. Portfolios generated: python eval/generate_portfolios.py
  3. (Optional) KB synced from prod: bash eval/sync_kb.sh

Usage:
    python eval/eval_harness.py                    # 50 portfolios, 2 queries/intent
    python eval/eval_harness.py --n 10             # sanity run
    python eval/eval_harness.py --n 500 --qpi 3   # overnight full run
    python eval/eval_harness.py --n 100 --csv      # also write CSV
    python eval/eval_harness.py --base http://127.0.0.1:5050
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_BASE     = 'http://127.0.0.1:5050'
EVAL_DIR         = Path(__file__).parent
RESULTS_DIR      = EVAL_DIR / 'results'
PORTFOLIOS_PATH  = EVAL_DIR / 'portfolios.json'
QUERIES_PATH     = EVAL_DIR / 'queries.json'
TIMEOUT          = 120   # seconds — LLM can be slow
SLEEP_BETWEEN    = 0.8   # seconds — local server still rate-limits
TARGET_PASS_RATES = {
    'no_data':         0.95,
    'single_ticker':   0.85,
    'portfolio_review':0.80,
    'opportunity':     0.80,
    'geo':             0.75,
    'greeks':          0.85,
    'macro':           0.90,
}

# Add eval/ parent to path so scorers.py is importable
sys.path.insert(0, str(EVAL_DIR))
sys.path.insert(0, str(EVAL_DIR.parent))

from scorers import score_response, is_pass  # noqa: E402


# ── Auth helpers ──────────────────────────────────────────────────────────────

def _register_eval_user(base: str) -> tuple[str, str]:
    """
    Register a fresh ephemeral eval user. Returns (user_id, token).
    Falls back gracefully if /auth/register is unavailable.
    """
    run_id   = uuid.uuid4().hex[:8]
    user_id  = f'eval_{run_id}'
    email    = f'{user_id}@eval.local'
    password = 'Ev@lH4rness!'

    try:
        r = requests.post(
            f'{base}/auth/register',
            json={'user_id': user_id, 'email': email, 'password': password},
            timeout=15,
        )
        if r.status_code in (200, 201):
            token = r.json().get('token', '')
            return user_id, token

        # Try login if already registered
        r2 = requests.post(
            f'{base}/auth/login',
            json={'email': email, 'password': password},
            timeout=15,
        )
        if r2.status_code == 200:
            token = r2.json().get('token', '')
            return user_id, token
    except Exception as e:
        print(f'[warn] auth failed: {e} — running without auth token')

    return user_id, ''


def _set_trader_level(base: str, user_id: str, token: str, level: str) -> None:
    try:
        headers = {'Authorization': f'Bearer {token}'} if token else {}
        requests.post(
            f'{base}/users/{user_id}/trader-level',
            json={'level': level},
            headers=headers,
            timeout=10,
        )
    except Exception:
        pass


def _submit_portfolio(base: str, user_id: str, token: str, portfolio: dict) -> None:
    try:
        headers = {'Authorization': f'Bearer {token}'} if token else {}
        requests.post(
            f'{base}/portfolio',
            json={
                'user_id':  user_id,
                'holdings': portfolio['holdings'],
                'cash':     portfolio['cash'],
                'currency': portfolio['currency'],
            },
            headers=headers,
            timeout=15,
        )
    except Exception:
        pass


# ── Query helpers ──────────────────────────────────────────────────────────────

def _fill_template(template: str, portfolio: dict) -> tuple[str, Optional[str]]:
    """Replace {ticker} placeholder with a random holding ticker."""
    if '{ticker}' in template:
        ticker = random.choice(portfolio['holdings'])['ticker']
        return template.replace('{ticker}', ticker), ticker
    return template, None


def _run_query(base: str, user_id: str, token: str, message: str) -> str:
    """POST /chat and return the answer string. Raises on HTTP error."""
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'

    r = requests.post(
        f'{base}/chat',
        json={'message': message, 'user_id': user_id},
        headers=headers,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return data.get('answer') or data.get('response') or ''


# ── Main eval loop ─────────────────────────────────────────────────────────────

def run_eval(
    base: str = DEFAULT_BASE,
    n_portfolios: int = 50,
    queries_per_intent: int = 2,
    write_csv: bool = False,
    seed: Optional[int] = None,
) -> list[dict]:

    if seed is not None:
        random.seed(seed)

    if not PORTFOLIOS_PATH.exists():
        print(f'ERROR: {PORTFOLIOS_PATH} not found. Run: python eval/generate_portfolios.py')
        sys.exit(1)

    if not QUERIES_PATH.exists():
        print(f'ERROR: {QUERIES_PATH} not found.')
        sys.exit(1)

    with open(PORTFOLIOS_PATH) as f:
        all_portfolios = json.load(f)

    with open(QUERIES_PATH) as f:
        query_bank = json.load(f)

    portfolios = random.sample(all_portfolios, min(n_portfolios, len(all_portfolios)))

    RESULTS_DIR.mkdir(exist_ok=True)

    # Register a single shared eval user for the run
    print(f'\nConnecting to {base} …')
    user_id, token = _register_eval_user(base)
    print(f'Eval user: {user_id}  token: {"yes" if token else "none"}')

    results: list[dict] = []
    total = n_portfolios * len(query_bank) * queries_per_intent
    done  = 0
    errors = 0

    print(f'Running {n_portfolios} portfolios × {len(query_bank)} intents × {queries_per_intent} queries = ~{total} requests\n')

    for p_idx, portfolio in enumerate(portfolios):

        # Configure user for this portfolio
        _submit_portfolio(base, user_id, token, portfolio)
        _set_trader_level(base, user_id, token, portfolio['trader_level'])

        for intent, templates in query_bank.items():
            sample = random.sample(templates, min(queries_per_intent, len(templates)))

            for template in sample:
                query, ticker = _fill_template(template, portfolio)

                try:
                    response = _run_query(base, user_id, token, query)
                    scores   = score_response(response, intent, portfolio, ticker)
                    passed   = is_pass(scores)

                    results.append({
                        'portfolio_idx':   p_idx,
                        'portfolio_size':  len(portfolio['holdings']),
                        'trader_level':    portfolio['trader_level'],
                        'tier':            portfolio['tier'],
                        'intent':          intent,
                        'query':           query,
                        'ticker':          ticker,
                        'response_length': len(response),
                        'scores':          scores,
                        'pass':            passed,
                        'response_preview': response[:300],
                    })

                except Exception as e:
                    errors += 1
                    results.append({
                        'portfolio_idx': p_idx,
                        'intent':       intent,
                        'query':        query,
                        'ticker':       ticker,
                        'error':        str(e),
                        'pass':         False,
                        'scores':       {},
                        'response_preview': '',
                    })

                done += 1
                if done % 10 == 0:
                    pct = 100 * done / total
                    print(f'  {done}/{total} ({pct:.0f}%)  errors={errors}')

                time.sleep(SLEEP_BETWEEN)

    # ── Save results ────────────────────────────────────────────────────────
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_path = RESULTS_DIR / f'eval_{timestamp}.json'
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f'\nResults saved → {json_path}')

    if write_csv:
        csv_path = RESULTS_DIR / f'eval_{timestamp}.csv'
        _write_csv(results, csv_path)
        print(f'CSV saved      → {csv_path}')

    _print_summary(results)
    return results


# ── Output helpers ─────────────────────────────────────────────────────────────

def _write_csv(results: list[dict], path: Path) -> None:
    flat_rows = []
    for r in results:
        base_row = {
            'portfolio_size': r.get('portfolio_size', ''),
            'trader_level':   r.get('trader_level', ''),
            'tier':           r.get('tier', ''),
            'intent':         r.get('intent', ''),
            'query':          r.get('query', ''),
            'ticker':         r.get('ticker', ''),
            'response_length':r.get('response_length', ''),
            'pass':           r.get('pass', False),
            'error':          r.get('error', ''),
            'response_preview': r.get('response_preview', ''),
        }
        for check, val in r.get('scores', {}).items():
            base_row[f'check_{check}'] = val
        flat_rows.append(base_row)

    if not flat_rows:
        return

    all_keys = list(flat_rows[0].keys())
    for row in flat_rows[1:]:
        for k in row:
            if k not in all_keys:
                all_keys.append(k)

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(flat_rows)


def _print_summary(results: list[dict]) -> None:
    by_intent: dict = defaultdict(list)
    for r in results:
        by_intent[r.get('intent', 'unknown')].append(r)

    total_pass = sum(1 for r in results if r.get('pass'))
    total      = len(results)
    errors     = sum(1 for r in results if r.get('error'))

    print('\n' + '=' * 64)
    print('EVAL SUMMARY')
    print('=' * 64)
    print(f'Overall:  {total_pass}/{total} passed ({100*total_pass/total:.1f}%)  errors={errors}\n')

    print(f'{"Intent":<20} {"Pass":>6} {"Total":>6} {"Rate":>7}  {"Target":>7}  Status')
    print('-' * 64)

    all_ok = True
    for intent in sorted(by_intent.keys()):
        rs     = by_intent[intent]
        passes = sum(1 for r in rs if r.get('pass'))
        rate   = passes / len(rs) if rs else 0.0
        target = TARGET_PASS_RATES.get(intent, 0.80)
        ok     = rate >= target
        if not ok:
            all_ok = False
        status = '✓' if ok else '✗ BELOW TARGET'
        print(f'{intent:<20} {passes:>6} {len(rs):>6} {100*rate:>6.1f}%  {100*target:>6.0f}%  {status}')

    # Per-intent: surface failing checks
    print('\nFailing checks (< 80% pass rate):')
    found_any = False
    for intent in sorted(by_intent.keys()):
        rs = by_intent[intent]
        check_vals: dict = defaultdict(list)
        for r in rs:
            for check, val in r.get('scores', {}).items():
                check_vals[check].append(val if isinstance(val, bool) else val >= 0.8)
        for check, vals in check_vals.items():
            rate = sum(vals) / len(vals) if vals else 0.0
            if rate < 0.80:
                found_any = True
                print(f'  {intent:<20} {check:<35} {100*rate:.0f}%')
    if not found_any:
        print('  (none)')

    # By trader level
    print('\nBy trader level:')
    for level in ('beginner', 'developing', 'experienced', 'quant'):
        rs = [r for r in results if r.get('trader_level') == level]
        if rs:
            passes = sum(1 for r in rs if r.get('pass'))
            print(f'  {level:<14} {100*passes/len(rs):5.1f}%  ({passes}/{len(rs)})')

    # Response length distribution
    lengths = [r.get('response_length', 0) for r in results if not r.get('error')]
    if lengths:
        print(f'\nResponse length: min={min(lengths)} avg={sum(lengths)//len(lengths)} max={max(lengths)}')

    print('=' * 64)
    if all_ok:
        print('All intents at or above target pass rates. ✓')
    else:
        print('Some intents below target — inspect failing checks above.')
    print()


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Trading Galaxy eval harness')
    parser.add_argument('--base',  default=DEFAULT_BASE, help='API base URL')
    parser.add_argument('--n',     type=int, default=50,  help='Number of portfolios')
    parser.add_argument('--qpi',   type=int, default=2,   help='Queries per intent')
    parser.add_argument('--csv',   action='store_true',   help='Also write CSV output')
    parser.add_argument('--seed',  type=int, default=None,help='Random seed for reproducibility')
    args = parser.parse_args()

    run_eval(
        base=args.base,
        n_portfolios=args.n,
        queries_per_intent=args.qpi,
        write_csv=args.csv,
        seed=args.seed,
    )

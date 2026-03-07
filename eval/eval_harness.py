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
    python eval/eval_harness.py --smoke            # 1 portfolio × 7 intents × 1 = 7 queries (~30s)
    python eval/eval_harness.py --n 10             # sanity run
    python eval/eval_harness.py --n 500 --qpi 3   # overnight full run
    python eval/eval_harness.py --n 100 --csv      # also write CSV
    python eval/eval_harness.py --workers 6        # parallel requests (default 4)
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_BASE     = 'http://127.0.0.1:5050'
EVAL_DIR         = Path(__file__).parent
RESULTS_DIR      = EVAL_DIR / 'results'
PORTFOLIOS_PATH  = EVAL_DIR / 'portfolios.json'
QUERIES_PATH     = EVAL_DIR / 'queries.json'
TIMEOUT          = 180   # seconds — LLM can be slow
SLEEP_BETWEEN    = 0.5   # seconds — per-thread cooldown between requests
RETRY_ON_EMPTY   = 2     # retry count when response is empty (Ollama overload)
DEFAULT_WORKERS  = 4     # parallel request threads
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

from scorers import score_response, is_pass, kb_has_yield_atoms  # noqa: E402


# ── Auth helpers ──────────────────────────────────────────────────────────────

def _register_eval_user(base: str, beta_password: str = '') -> tuple[str, str]:
    """
    Register a fresh ephemeral eval user. Returns (user_id, token).
    Falls back gracefully if /auth/register is unavailable.
    beta_password: passed explicitly or falls back to BETA_PASSWORD env var.
    """
    run_id        = uuid.uuid4().hex[:8]
    user_id       = f'eval_{run_id}'
    email         = f'{user_id}@eval.local'
    password      = 'Ev@lH4rness!'
    beta_password = beta_password or os.environ.get('BETA_PASSWORD', '')

    try:
        reg_body = {'user_id': user_id, 'email': email, 'password': password}
        if beta_password:
            reg_body['beta_password'] = beta_password

        r = requests.post(f'{base}/auth/register', json=reg_body, timeout=15)
        if r.status_code in (200, 201):
            # Register returns {user_id, email, created_at} — no token.
            # Must call /auth/token immediately after to get JWT.
            r_tok = requests.post(
                f'{base}/auth/token',
                json={'email': email, 'password': password},
                timeout=15,
            )
            if r_tok.status_code == 200:
                token = r_tok.json().get('access_token', '')
                return user_id, token

        # Try login if user already exists from a previous run
        r2 = requests.post(
            f'{base}/auth/token',
            json={'email': email, 'password': password},
            timeout=15,
        )
        if r2.status_code == 200:
            token = r2.json().get('access_token', '')
            return user_id, token

        print(f'[warn] auth register returned {r.status_code}: {r.text[:120]} — running without token')
    except Exception as e:
        print(f'[warn] auth failed: {e} — running without auth token')

    return user_id, ''


def _upgrade_eval_user(base: str, user_id: str, token: str) -> None:
    """Upgrade eval user to premium so they pass the chat quota gate."""
    try:
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        dev_key = os.environ.get('DEV_UPGRADE_KEY', '')
        if dev_key:
            headers['X-Dev-Key'] = dev_key
        requests.post(
            f'{base}/dev/upgrade-premium',
            json={'user_id': user_id},
            headers=headers,
            timeout=10,
        )
    except Exception:
        pass


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
    if not token:
        return  # portfolio endpoint requires auth — skip if no token
    try:
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        requests.post(
            f'{base}/users/{user_id}/portfolio',
            json={
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
    """
    POST /chat and return the answer string.
    When a valid token is present, send it so portfolio context is injected.
    When no token, omit user_id from the body — avoids quota enforcement on
    unauthenticated requests and still tests raw KB retrieval quality.
    """
    headers = {'Content-Type': 'application/json'}
    body: dict = {'message': message}
    if token:
        headers['Authorization'] = f'Bearer {token}'
        body['session_id'] = user_id  # isolate portfolio ticker cache per eval user
        # user_id resolved from token by the server — don't double-send
    # No token: don't send user_id either — that would trigger quota check
    # for a user with no tier row, causing 403 on every request.

    for attempt in range(RETRY_ON_EMPTY + 1):
        r = requests.post(f'{base}/chat', json=body, headers=headers, timeout=TIMEOUT)
        if r.status_code == 503:
            if attempt < RETRY_ON_EMPTY:
                time.sleep(5 * (attempt + 1))
                continue
            return ''
        r.raise_for_status()
        data = r.json()
        answer = data.get('answer') or data.get('response') or ''
        if answer or attempt == RETRY_ON_EMPTY:
            return answer
        # Empty answer — Ollama may have been busy; wait and retry
        time.sleep(5 * (attempt + 1))
    return ''


# ── Main eval loop ─────────────────────────────────────────────────────────────

def _provision_portfolio(base: str, portfolio: dict, p_idx: int,
                         beta_password: str) -> tuple[str, str, bool]:
    """Register user, upgrade, submit portfolio. Returns (user_id, token, ok)."""
    user_id, token = _register_eval_user(base, beta_password=beta_password)
    if not token:
        print(f'[warn] portfolio {p_idx}: no token — skipping (check BETA_PASSWORD)')
        return user_id, '', False
    _upgrade_eval_user(base, user_id, token)
    _submit_portfolio(base, user_id, token, portfolio)
    _set_trader_level(base, user_id, token, portfolio['trader_level'])
    return user_id, token, True


def _eval_single_query(
    base: str, user_id: str, token: str,
    intent: str, query: str, ticker: Optional[str],
    portfolio: dict, p_idx: int,
    kb_has_yield: bool,
) -> dict:
    """Execute one eval query and return its result dict."""
    try:
        _token = '' if intent == 'no_data' else token
        response = _run_query(base, user_id, _token, query)
        scores   = score_response(response, intent, portfolio, ticker,
                                  kb_has_yield=kb_has_yield, query=query)
        passed   = is_pass(scores)
        return {
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
        }
    except Exception as e:
        return {
            'portfolio_idx': p_idx,
            'intent':       intent,
            'query':        query,
            'ticker':       ticker,
            'error':        str(e),
            'pass':         False,
            'scores':       {},
            'response_preview': '',
        }


def run_eval(
    base: str = DEFAULT_BASE,
    n_portfolios: int = 50,
    queries_per_intent: int = 2,
    write_csv: bool = False,
    seed: Optional[int] = None,
    beta_password: str = '',
    workers: int = DEFAULT_WORKERS,
) -> list[dict]:

    if seed is not None:
        random.seed(seed)

    # Check once at startup whether the KB has yield curve atoms.
    # Score_macro skips mentions_yield_or_rates when False — avoids penalising
    # the model for correctly saying it has no yield data when the KB is empty.
    _kb_has_yield = kb_has_yield_atoms()
    print(f'[harness] kb_has_yield={_kb_has_yield}')

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

    print(f'\nConnecting to {base} …')

    # ── Build work items ──────────────────────────────────────────────────
    work_items: list[dict] = []
    for p_idx, portfolio in enumerate(portfolios):
        for intent, templates in query_bank.items():
            sample = random.sample(templates, min(queries_per_intent, len(templates)))
            for template in sample:
                query, ticker = _fill_template(template, portfolio)
                work_items.append({
                    'p_idx': p_idx, 'portfolio': portfolio,
                    'intent': intent, 'query': query, 'ticker': ticker,
                })

    total = len(work_items)
    print(f'Running {n_portfolios} portfolios × {len(query_bank)} intents × {queries_per_intent} queries = {total} requests')
    print(f'Workers: {workers} parallel threads\n')

    # ── Provision users (sequential — lightweight HTTP) ───────────────────
    user_cache: dict[int, tuple[str, str]] = {}  # p_idx -> (user_id, token)
    for p_idx, portfolio in enumerate(portfolios):
        user_id, token, ok = _provision_portfolio(base, portfolio, p_idx, beta_password)
        if ok:
            user_cache[p_idx] = (user_id, token)
        else:
            user_cache[p_idx] = (user_id, '')

    # ── Execute queries in parallel ───────────────────────────────────────
    results: list[dict] = []
    done   = 0
    errors = 0
    _progress_lock = Lock()

    def _worker(item: dict) -> dict:
        nonlocal done, errors
        p_idx = item['p_idx']
        user_id, token = user_cache[p_idx]
        if not token:
            r = {
                'portfolio_idx': p_idx, 'intent': item['intent'],
                'query': item['query'], 'ticker': item['ticker'],
                'error': 'no auth token', 'pass': False,
                'scores': {}, 'response_preview': '',
            }
            with _progress_lock:
                done += 1
                errors += 1
            return r

        r = _eval_single_query(
            base, user_id, token,
            item['intent'], item['query'], item['ticker'],
            item['portfolio'], p_idx, _kb_has_yield,
        )
        time.sleep(SLEEP_BETWEEN)

        with _progress_lock:
            done += 1
            if r.get('error'):
                errors += 1
            if done % 10 == 0:
                pct = 100 * done / total
                print(f'  {done}/{total} ({pct:.0f}%)  errors={errors}')
        return r

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_worker, item) for item in work_items]
        for f in as_completed(futures):
            results.append(f.result())

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
                if check.startswith('info_'):
                    continue
                from scorers import _FLOAT_THRESHOLDS, _DEFAULT_FLOAT_THRESHOLD
                thresh = _FLOAT_THRESHOLDS.get(check, _DEFAULT_FLOAT_THRESHOLD)
                check_vals[check].append(val if isinstance(val, bool) else val >= thresh)
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
    parser.add_argument('--seed',          type=int, default=None, help='Random seed for reproducibility')
    parser.add_argument('--beta-password', default='',             help='Beta access password for /auth/register')
    parser.add_argument('--smoke', action='store_true',
                        help='Smoke test: 1 portfolio × 7 intents × 1 query = 7 requests (~30s)')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS,
                        help=f'Parallel request threads (default {DEFAULT_WORKERS})')
    args = parser.parse_args()

    # --smoke overrides --n and --qpi
    n_port = 1 if args.smoke else args.n
    qpi    = 1 if args.smoke else args.qpi
    wk     = 1 if args.smoke else args.workers  # smoke stays sequential for clarity

    run_eval(
        base=args.base,
        n_portfolios=n_port,
        queries_per_intent=qpi,
        write_csv=args.csv,
        seed=args.seed,
        beta_password=args.beta_password,
        workers=wk,
    )

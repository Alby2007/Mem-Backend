"""
eval/generate_portfolios.py — Synthetic portfolio generator for eval harness.

Generates a bank of randomised portfolios covering FTSE 100 + US large-cap tickers
across all trader levels and subscription tiers.

Usage:
    python eval/generate_portfolios.py          # generates 1000 portfolios
    python eval/generate_portfolios.py --n 100  # smaller bank
"""
from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path

FTSE_TICKERS = [
    'BARC.L', 'HSBA.L', 'LLOY.L', 'STAN.L', 'LSEG.L',
    'BP.L',   'SHEL.L', 'RIO.L',  'GSK.L',  'AZN.L',
    'BT-A.L', 'VOD.L',  'IAG.L',  'DGE.L',  'ULVR.L',
    'REL.L',  'HL.L',   'MNG.L',  'NWG.L',  'TSCO.L',
]
US_TICKERS = [
    'AAPL', 'MSFT', 'META', 'GOOGL', 'NVDA',
    'MA',   'V',    'COIN', 'PLTR',  'AMZN',
    'TSLA', 'JPM',  'BAC',  'GS',    'MS',
    'XOM',  'CVX',  'WMT',  'HD',    'UNH',
]
ALL_TICKERS = FTSE_TICKERS + US_TICKERS

TRADER_LEVELS = ['beginner', 'developing', 'experienced', 'quant']
TIERS         = ['basic', 'pro', 'premium']


def generate_portfolio(n: int | None = None) -> dict:
    n = n or random.randint(2, 8)
    tickers = random.sample(ALL_TICKERS, min(n, len(ALL_TICKERS)))
    holdings = []
    for t in tickers:
        holdings.append({
            'ticker':   t,
            'quantity': random.randint(10, 500),
            'avg_cost': round(random.uniform(50, 500), 2),
        })
    return {
        'holdings':             holdings,
        'cash':                 round(random.uniform(500, 10000), 2),
        'currency':             random.choice(['GBP', 'USD']),
        'risk_pct':             random.choice([0.5, 1.0, 1.5, 2.0]),
        'trader_level':         random.choice(TRADER_LEVELS),
        'tier':                 random.choice(TIERS),
    }


def generate_portfolio_bank(n: int = 1000) -> None:
    out_dir = Path(__file__).parent
    out_path = out_dir / 'portfolios.json'
    portfolios = [generate_portfolio() for _ in range(n)]
    with open(out_path, 'w') as f:
        json.dump(portfolios, f, indent=2)
    print(f"Generated {n} portfolios → {out_path}")

    # Distribution summary
    from collections import Counter
    levels = Counter(p['trader_level'] for p in portfolios)
    tiers  = Counter(p['tier'] for p in portfolios)
    sizes  = [len(p['holdings']) for p in portfolios]
    print(f"  trader_level: {dict(levels)}")
    print(f"  tier:         {dict(tiers)}")
    print(f"  holdings:     min={min(sizes)} avg={sum(sizes)//len(sizes)} max={max(sizes)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate synthetic eval portfolios')
    parser.add_argument('--n', type=int, default=1000, help='Number of portfolios to generate')
    args = parser.parse_args()
    generate_portfolio_bank(args.n)

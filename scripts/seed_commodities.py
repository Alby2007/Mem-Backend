"""
Seed commodity, forex, and index atoms into the KB using yfinance.
Run once: python scripts/seed_commodities.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import sqlite3
from datetime import datetime, timezone

DB_PATH = os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')

SEEDS = [
    # (kb_ticker, yf_symbol, display_name)
    ('XAUUSD', 'GC=F',      'Gold'),
    ('XAGUSD', 'SI=F',      'Silver'),
    ('XPTUSD', 'PL=F',      'Platinum'),
    ('XCUUSD', 'HG=F',      'Copper'),
    ('CL',     'CL=F',      'Crude Oil WTI'),
    ('BZ',     'BZ=F',      'Brent Crude'),
    ('NG',     'NG=F',      'Natural Gas'),
    ('EURUSD', 'EURUSD=X',  'EUR/USD'),
    ('GBPUSD', 'GBPUSD=X',  'GBP/USD'),
    ('USDJPY', 'JPY=X',     'USD/JPY'),
    ('DXY',    'DX-Y.NYB',  'US Dollar Index'),
    ('SPX',    '^GSPC',     'S&P 500'),
    ('NDX',    '^NDX',      'NASDAQ 100'),
    ('FTSE',   '^FTSE',     'FTSE 100'),
    ('VIX',    '^VIX',      'VIX'),
]

def upsert(conn, subject, predicate, obj, confidence, source, now_iso):
    subj = subject.lower()
    obj_s = str(obj)
    # Delete any existing row for this (subject, predicate) first, then insert fresh
    conn.execute(
        "DELETE FROM facts WHERE subject=? AND predicate=?",
        (subj, predicate)
    )
    conn.execute(
        """INSERT INTO facts (subject, predicate, object, confidence, source, timestamp)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (subj, predicate, obj_s, confidence, source, now_iso)
    )

conn = sqlite3.connect(DB_PATH, timeout=10)

# Ensure upsert support — check if unique constraint exists
try:
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_subj_pred ON facts(subject, predicate)")
    conn.commit()
except Exception as e:
    print(f"Index note: {e}")

now_iso = datetime.now(timezone.utc).isoformat()
total = 0

for kb_ticker, yf_sym, name in SEEDS:
    print(f"Fetching {name} ({kb_ticker} via {yf_sym})...")
    try:
        t = yf.Ticker(yf_sym)
        fi = t.fast_info
        price = getattr(fi, 'last_price', None) or getattr(fi, 'regularMarketPrice', None)
        high  = getattr(fi, 'year_high', None)  or getattr(fi, 'fiftyTwoWeekHigh', None)
        low   = getattr(fi, 'year_low',  None)  or getattr(fi, 'fiftyTwoWeekLow',  None)

        if price:
            upsert(conn, kb_ticker, 'last_price', f'{price:.4f}', 0.95, 'yfinance_seed', now_iso)
            upsert(conn, kb_ticker, 'asset_name', name, 1.0, 'seed_static', now_iso)
            total += 2
            print(f"  price={price:.4f}")

        if price and high and low and high > low:
            ratio = (price - low) / (high - low)
            if ratio > 0.7:
                regime = 'upper_range'
            elif ratio > 0.3:
                regime = 'mid_range'
            else:
                regime = 'lower_range'
            upsert(conn, kb_ticker, 'price_regime', regime, 0.85, 'yfinance_seed', now_iso)
            upsert(conn, kb_ticker, 'year_high', f'{high:.4f}', 0.90, 'yfinance_seed', now_iso)
            upsert(conn, kb_ticker, 'year_low',  f'{low:.4f}',  0.90, 'yfinance_seed', now_iso)
            total += 3
            print(f"  52w range={low:.2f}-{high:.2f}, regime={regime}")

    except Exception as e:
        print(f"  ERROR: {e}")

conn.commit()
conn.close()
print(f"\nDone — {total} atoms seeded into {DB_PATH}")

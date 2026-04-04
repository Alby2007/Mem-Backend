"""
scripts/seed_patterns.py

Fetches daily OHLCV candles from yfinance for all KB tickers that have a
last_price atom, runs detect_all_patterns() on each, and inserts the results
into the pattern_signals table.

Run from the project root:
    python scripts/seed_patterns.py
"""

import sqlite3
import sys
import os
from datetime import datetime, timezone
from dataclasses import asdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import yfinance as yf
from analytics.pattern_detector import detect_all_patterns, OHLCV

DB_PATH = os.environ.get('TRADING_KB_DB', 'trading_knowledge.db')

# yfinance symbol overrides for commodity/forex/index KB tickers
_YF_MAP = {
    'xauusd': 'GC=F',  'xagusd': 'SI=F',  'xptusd': 'PL=F',
    'cl':     'CL=F',  'bz':     'BZ=F',  'ng':     'NG=F',
    'gbpusd': 'GBPUSD=X', 'eurusd': 'EURUSD=X', 'usdjpy': 'JPY=X',
    'gbpeur': 'GBPEUR=X', 'audusd': 'AUDUSD=X', 'usdcad': 'CAD=X',
    'dxy':    'DX-Y.NYB',
    'spx':    '^GSPC',  'ndx':    '^NDX',   'dji':    '^DJI',
    'ftse':   '^FTSE',  'dax':    '^GDAXI', 'vix':    '^VIX',
}

TIMEFRAMES = [
    ('1d',  '1d',  '6mo'),   # daily candles, 6 months
    ('4h',  '1h',  '60d'),   # 1h proxy for 4h (yfinance doesn't do 4h directly; we'll resample)
]


def _get_kb_tickers(conn: sqlite3.Connection):
    rows = conn.execute(
        "SELECT DISTINCT subject FROM facts WHERE predicate = 'last_price'"
    ).fetchall()
    return [r[0] for r in rows]


def _get_kb_atoms(conn: sqlite3.Connection, ticker: str) -> dict:
    rows = conn.execute(
        "SELECT predicate, object FROM facts WHERE subject = ?", (ticker,)
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _fetch_candles_1d(yf_sym: str, period: str = '6mo'):
    hist = yf.Ticker(yf_sym).history(period=period, interval='1d', auto_adjust=True)
    if hist.empty:
        return []
    candles = []
    for ts, row in hist.iterrows():
        candles.append(OHLCV(
            timestamp=ts.isoformat(),
            open=float(row['Open']),
            high=float(row['High']),
            low=float(row['Low']),
            close=float(row['Close']),
            volume=float(row.get('Volume', 0) or 0),
        ))
    return candles


def _pattern_exists(conn: sqlite3.Connection, ticker: str, pattern_type: str,
                    formed_at: str, timeframe: str) -> bool:
    r = conn.execute(
        """SELECT 1 FROM pattern_signals
           WHERE ticker=? AND pattern_type=? AND formed_at=? AND timeframe=?
           LIMIT 1""",
        (ticker, pattern_type, formed_at, timeframe),
    ).fetchone()
    return r is not None


def _insert_pattern(conn: sqlite3.Connection, sig, detected_at: str):
    conn.execute(
        """INSERT INTO pattern_signals
           (ticker, pattern_type, direction, zone_high, zone_low,
            zone_size_pct, timeframe, formed_at, status,
            quality_score, kb_conviction, kb_regime, kb_signal_dir,
            alerted_users, detected_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'[]',?)""",
        (
            sig.ticker, sig.pattern_type, sig.direction,
            sig.zone_high, sig.zone_low, sig.zone_size_pct,
            sig.timeframe, sig.formed_at, sig.status,
            sig.quality_score, sig.kb_conviction,
            sig.kb_regime, sig.kb_signal_dir, detected_at,
        ),
    )


def main():
    conn = sqlite3.connect(DB_PATH, timeout=15)

    # Ensure pattern_signals table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_signals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT NOT NULL,
            pattern_type  TEXT NOT NULL,
            direction     TEXT NOT NULL,
            zone_high     REAL NOT NULL,
            zone_low      REAL NOT NULL,
            zone_size_pct REAL,
            timeframe     TEXT NOT NULL,
            formed_at     TEXT,
            status        TEXT NOT NULL DEFAULT 'open',
            filled_at     TEXT,
            quality_score REAL,
            kb_conviction TEXT DEFAULT '',
            kb_regime     TEXT DEFAULT '',
            kb_signal_dir TEXT DEFAULT '',
            alerted_users TEXT DEFAULT '[]',
            detected_at   TEXT
        )
    """)
    conn.commit()

    tickers = _get_kb_tickers(conn)
    print(f"Found {len(tickers)} KB tickers with last_price atoms")

    now_iso = datetime.now(timezone.utc).isoformat()
    total_inserted = 0

    for ticker in tickers:
        yf_sym = _YF_MAP.get(ticker.lower(), ticker.upper())
        atoms = _get_kb_atoms(conn, ticker)
        kb_conviction = atoms.get('conviction_tier', '')
        kb_regime     = atoms.get('price_regime', '')
        kb_signal_dir = atoms.get('signal_direction', '')

        print(f"  {ticker.upper():12s} → {yf_sym}", end='', flush=True)
        try:
            candles = _fetch_candles_1d(yf_sym)
            if len(candles) < 10:
                print(f"  SKIP (only {len(candles)} candles)")
                continue

            signals = detect_all_patterns(
                candles,
                ticker=ticker.upper(),
                timeframe='1d',
                kb_conviction=kb_conviction,
                kb_regime=kb_regime,
                kb_signal_dir=kb_signal_dir,
            )

            inserted = 0
            for sig in signals:
                if _pattern_exists(conn, sig.ticker, sig.pattern_type, sig.formed_at, sig.timeframe):
                    continue
                _insert_pattern(conn, sig, now_iso)
                inserted += 1

            conn.commit()
            total_inserted += inserted
            print(f"  {len(candles)} candles → {len(signals)} patterns ({inserted} new)")

        except Exception as e:
            print(f"  ERROR: {e}")

    conn.close()
    print(f"\nDone. Total patterns inserted: {total_inserted}")
    final = sqlite3.connect(DB_PATH).execute("SELECT COUNT(*) FROM pattern_signals").fetchone()[0]
    print(f"pattern_signals total rows: {final}")


if __name__ == '__main__':
    main()

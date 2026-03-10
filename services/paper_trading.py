"""
services/paper_trading.py — Paper trading business logic.

Extracted from routes/paper.py to separate DB/agent logic from HTTP handling.
Routes become thin wrappers that call these functions.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import random
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import extensions as ext

_logger = logging.getLogger('paper_agent')

# ── Constants ─────────────────────────────────────────────────────────────────

_YF_MAP = {
    'xauusd': 'GC=F',  'xagusd': 'SI=F',  'xptusd': 'PL=F',
    'cl': 'CL=F',      'bz': 'BZ=F',       'ng': 'NG=F',
    'gbpusd': 'GBPUSD=X', 'eurusd': 'EURUSD=X', 'usdjpy': 'JPY=X',
    'dxy': 'DX-Y.NYB',
    'spx': '^GSPC',    'ndx': '^NDX',       'dji': '^DJI',
    'ftse': '^FTSE',   'dax': '^GDAXI',     'vix': '^VIX',
}

_PAPER_AGENT_SYSTEM = """You are an autonomous paper-trading agent for Trading Galaxy.
You make ENTRY or SKIP decisions based on KB signal data.
ALWAYS respond with JSON only:
{"action": "ENTER"|"SKIP", "entry": float, "stop": float, "t1": float, "t2": float, "reasoning": "cite signal data, max 120 chars"}"""

_PAPER_MAX_OPEN_POSITIONS = 12


def _is_market_open(ticker: str) -> bool:
    """Return True only if the primary exchange for this ticker is currently open.

    US equities (default): NYSE/NASDAQ 14:30-21:00 UTC Mon-Fri
    UK equities (.L suffix): LSE 08:00-16:30 UTC Mon-Fri
    Futures/FX/indices: treated as always-open (24h products)
    """
    always_open_prefixes = ('GC=F', 'SI=F', 'PL=F', 'CL=F', 'BZ=F', 'NG=F',
                            'GBPUSD=X', 'EURUSD=X', 'JPY=X', 'DX-Y.NYB')
    yf_sym = _YF_MAP.get(ticker.lower(), ticker)
    if any(yf_sym.startswith(p) or yf_sym == p for p in always_open_prefixes):
        return True

    now_utc = datetime.now(timezone.utc)
    weekday = now_utc.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:
        return False

    if ticker.upper().endswith('.L'):
        market_open  = now_utc.replace(hour=8,  minute=0,  second=0, microsecond=0)
        market_close = now_utc.replace(hour=16, minute=30, second=0, microsecond=0)
    else:
        market_open  = now_utc.replace(hour=14, minute=30, second=0, microsecond=0)
        market_close = now_utc.replace(hour=21, minute=0,  second=0, microsecond=0)

    return market_open <= now_utc <= market_close


_PAPER_MAX_NEW_PER_SCAN = 3


# ── DDL + tier check ─────────────────────────────────────────────────────────

def ensure_paper_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_account (
            user_id TEXT PRIMARY KEY,
            virtual_balance REAL NOT NULL DEFAULT 500000.0,
            currency TEXT NOT NULL DEFAULT 'GBP',
            created_at TEXT NOT NULL
        )
    """)
    # Idempotent: add columns if they don't exist yet
    try:
        conn.execute('ALTER TABLE paper_account ADD COLUMN agent_running INTEGER NOT NULL DEFAULT 0')
    except Exception:
        pass
    try:
        conn.execute('ALTER TABLE paper_account ADD COLUMN account_size_set INTEGER NOT NULL DEFAULT 0')
    except Exception:
        pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_equity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            equity_value REAL NOT NULL,
            cash_balance REAL NOT NULL,
            open_positions INTEGER NOT NULL DEFAULT 0,
            logged_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            pattern_id INTEGER,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop REAL NOT NULL,
            t1 REAL NOT NULL,
            t2 REAL,
            quantity REAL NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'open',
            partial_closed INTEGER NOT NULL DEFAULT 0,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            exit_price REAL,
            pnl_r REAL,
            note TEXT,
            ai_reasoning TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_agent_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            ticker TEXT,
            detail TEXT,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()


def paper_tier_check(user_id: str) -> tuple[str, Optional[str]]:
    """Return (tier, error_message) — error_message is None if tier is pro/premium."""
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT tier FROM user_preferences WHERE user_id=?", (user_id,)
        ).fetchone()
        conn.close()
        tier = (row[0] if row else 'basic') or 'basic'
    except Exception:
        tier = 'basic'
    if tier not in ('pro', 'premium'):
        return tier, 'paper_trading_requires_pro'
    return tier, None


# ── Live price helpers ────────────────────────────────────────────────────────

def fetch_live_prices(tickers: list[str]) -> dict[str, float]:
    """Fetch latest prices — KB first, OHLCV cache second, yfinance last resort.

    Priority:
      1. KB last_price atoms (refreshed every 30 min by yfinance adapter)
      2. ohlcv_cache latest close (refreshed every 30 min)
      3. yf.Ticker.fast_info per-ticker (last resort, not batch — avoids rate limit)
    """
    if not tickers:
        return {}

    prices: dict[str, float] = {}
    missing: list[str] = []

    # Pass 1: KB last_price atoms (fastest, no external calls)
    try:
        import sqlite3 as _sq
        _kb_conn = _sq.connect(ext.DB_PATH, timeout=5)
        for tk in tickers:
            _tk_lookup = _YF_MAP.get(tk.lower(), tk).lower()
            row = _kb_conn.execute(
                "SELECT object FROM facts WHERE subject=? AND predicate='last_price' "
                "ORDER BY confidence DESC, timestamp DESC LIMIT 1",
                (_tk_lookup,)
            ).fetchone()
            if not row:
                # also try the original ticker key (KB stores by uppercase ticker sometimes)
                row = _kb_conn.execute(
                    "SELECT object FROM facts WHERE LOWER(subject)=? AND predicate='last_price' "
                    "ORDER BY confidence DESC, timestamp DESC LIMIT 1",
                    (tk.lower(),)
                ).fetchone()
            if row:
                try:
                    val = float(str(row[0]).split()[0].replace(',', ''))
                    if val > 0:
                        prices[tk] = val
                        continue
                except (ValueError, IndexError):
                    pass
            missing.append(tk)
        _kb_conn.close()
    except Exception:
        missing = list(tickers)

    # Pass 2: ohlcv_cache latest close
    if missing:
        still_missing: list[str] = []
        try:
            import sqlite3 as _sq2
            _oc_conn = _sq2.connect(ext.DB_PATH, timeout=5)
            for tk in missing:
                yf_sym = _YF_MAP.get(tk.lower(), tk)
                row = _oc_conn.execute(
                    "SELECT close FROM ohlcv_cache WHERE ticker=? AND interval='1d' "
                    "ORDER BY ts DESC LIMIT 1",
                    (yf_sym,)
                ).fetchone()
                if row and row[0] and float(row[0]) > 0:
                    prices[tk] = float(row[0])
                else:
                    still_missing.append(tk)
            _oc_conn.close()
        except Exception:
            still_missing = missing
        missing = still_missing

    # Pass 3: per-ticker fast_info (last resort, not batch — avoids rate limit)
    if missing:
        try:
            import yfinance as _yf
        except ImportError:
            return prices
        for tk in missing[:10]:  # cap at 10 to avoid hammering Yahoo
            try:
                import time as _t
                _t.sleep(0.3)
                fi = _yf.Ticker(_YF_MAP.get(tk.lower(), tk)).fast_info
                val = getattr(fi, 'last_price', None) or getattr(fi, 'regularMarketPrice', None)
                if val and float(val) > 0:
                    prices[tk] = float(val)
            except Exception:
                pass

    return prices


def compute_pnl_r(direction: str, entry: float, exit_p: float, stop: float) -> float:
    """Compute PnL in R-multiples."""
    risk = abs(entry - stop)
    if risk <= 0:
        return 0.0
    if direction == 'bullish':
        return round((exit_p - entry) / risk, 2)
    else:
        return round((entry - exit_p) / risk, 2)


# ── Account operations ────────────────────────────────────────────────────────

def get_account(user_id: str) -> dict:
    """Fetch paper account summary with live unrealised PnL."""
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,100000.0,'GBP',?)",
        (user_id, now_iso)
    )
    conn.commit()
    ensure_paper_tables(conn)
    row = conn.execute(
        'SELECT virtual_balance, currency, created_at, account_size_set FROM paper_account WHERE user_id=?',
        (user_id,)
    ).fetchone()
    total = conn.execute(
        "SELECT COUNT(*) FROM paper_positions WHERE user_id=?", (user_id,)
    ).fetchone()[0]
    open_c = conn.execute(
        "SELECT COUNT(*) FROM paper_positions WHERE user_id=? AND status='open'", (user_id,)
    ).fetchone()[0]
    closed_rows = conn.execute(
        "SELECT pnl_r, status FROM paper_positions WHERE user_id=? AND status IN ('t1_hit','t2_hit','stopped_out','closed') AND pnl_r IS NOT NULL",
        (user_id,)
    ).fetchall()
    wins = sum(1 for r in closed_rows if r[0] > 0)
    total_closed = len(closed_rows)
    win_rate = round(wins / total_closed * 100, 1) if total_closed else None
    avg_r = round(sum(r[0] for r in closed_rows) / total_closed, 2) if total_closed else None
    open_pos = conn.execute(
        "SELECT ticker, direction, entry_price, stop, quantity FROM paper_positions WHERE user_id=? AND status='open'",
        (user_id,)
    ).fetchall()
    conn.close()

    open_tickers = list({r[0] for r in open_pos})
    live = fetch_live_prices(open_tickers)

    unrealised_pnl = 0.0
    open_positions_value = 0.0
    for r in open_pos:
        tk, direction, entry, stop_, qty = r
        cp = live.get(tk)
        effective_price = cp if cp is not None else entry  # fall back to entry cost if no live price
        if effective_price and qty:
            open_positions_value += effective_price * qty
        if cp is not None and entry and qty:
            unrealised_pnl += (cp - entry) * qty if direction == 'bullish' else (entry - cp) * qty

    # account_value = free cash + current market value of open positions
    free_cash = float(row[0])
    account_value = round(free_cash + open_positions_value, 2)
    return {
        'user_id': user_id,
        'virtual_balance': row[0],
        'account_value': account_value,
        'unrealised_pnl': round(unrealised_pnl, 2),
        'free_cash': round(free_cash, 2),
        'open_positions_value': round(open_positions_value, 2),
        'currency': row[1],
        'created_at': row[2],
        'account_size_set': bool(row[3]) if len(row) > 3 else False,
        'total_trades': total,
        'open_positions': open_c,
        'closed_trades': total_closed,
        'win_rate_pct': win_rate,
        'avg_r': avg_r,
    }


def get_equity_log(user_id: str, days: int = 90) -> list[dict]:
    """Return equity log rows for the last N days, ordered ascending for charting."""
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    ensure_paper_tables(conn)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = conn.execute(
        'SELECT logged_at, equity_value, cash_balance, open_positions FROM paper_equity_log '
        'WHERE user_id=? AND logged_at >= ? ORDER BY logged_at ASC',
        (user_id, cutoff)
    ).fetchall()
    conn.close()
    return [
        {'logged_at': r[0], 'equity_value': r[1], 'cash_balance': r[2], 'open_positions': r[3]}
        for r in rows
    ]


def update_account_size(user_id: str, virtual_balance: Optional[float], mark_set: bool = True) -> dict:
    """Update virtual_balance and optionally mark account_size_set=1.

    If virtual_balance is None, only mark_set is written (balance unchanged).
    """
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    ensure_paper_tables(conn)
    now_iso = datetime.now(timezone.utc).isoformat()
    if virtual_balance is None:
        # "Not now" path — just mark the flag, don't touch the balance
        conn.execute(
            'INSERT INTO paper_account (user_id, virtual_balance, currency, created_at, account_size_set) '
            'VALUES (?, 100000.0, \'GBP\', ?, 1) '
            'ON CONFLICT(user_id) DO UPDATE SET account_size_set=1',
            (user_id, now_iso)
        )
        conn.commit()
        conn.close()
        return {'status': 'dismissed'}
    if virtual_balance < 100 or virtual_balance > 10_000_000:
        conn.close()
        return {'error': 'account size must be between 100 and 10,000,000'}
    conn.execute(
        'INSERT INTO paper_account (user_id, virtual_balance, currency, created_at, account_size_set) '
        'VALUES (?, ?, \'GBP\', ?, ?) '
        'ON CONFLICT(user_id) DO UPDATE SET virtual_balance=excluded.virtual_balance, '
        'account_size_set=excluded.account_size_set',
        (user_id, virtual_balance, now_iso, 1 if mark_set else 0)
    )
    conn.commit()
    conn.close()
    # Auto-start scanner when balance is set — user walks away, agent runs
    if mark_set:
        try:
            _status, _ = start_scanner(user_id)
            _logger.info('Auto-started scanner for %s after balance set: %s', user_id, _status)
        except Exception as _e:
            _logger.warning('Auto-start scanner failed for %s: %s', user_id, _e)
    return {'user_id': user_id, 'virtual_balance': virtual_balance, 'account_size_set': mark_set}


# ── Position operations ───────────────────────────────────────────────────────

def list_positions(user_id: str, status_filter: str = 'all') -> dict:
    """List paper positions with optional live price enrichment."""
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    if status_filter == 'open':
        rows = conn.execute(
            "SELECT * FROM paper_positions WHERE user_id=? AND status='open' ORDER BY opened_at DESC",
            (user_id,)
        ).fetchall()
    elif status_filter == 'closed':
        rows = conn.execute(
            "SELECT * FROM paper_positions WHERE user_id=? AND status NOT IN ('open') ORDER BY closed_at DESC",
            (user_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM paper_positions WHERE user_id=? ORDER BY opened_at DESC",
            (user_id,)
        ).fetchall()
    positions = [dict(r) for r in rows]
    conn.close()

    open_tickers = list({p['ticker'] for p in positions if p['status'] == 'open'})
    live_prices = fetch_live_prices(open_tickers)
    for p in positions:
        if p['status'] == 'open' and p['ticker'] in live_prices:
            cp = live_prices[p['ticker']]
            p['current_price'] = cp
            risk = abs(p['entry_price'] - p['stop'])
            if risk > 0:
                if p['direction'] == 'bullish':
                    p['unrealised_pnl_r'] = round((cp - p['entry_price']) / risk, 2)
                else:
                    p['unrealised_pnl_r'] = round((p['entry_price'] - cp) / risk, 2)
            else:
                p['unrealised_pnl_r'] = None
        else:
            p['current_price'] = None
            p['unrealised_pnl_r'] = None
    return {'positions': positions, 'count': len(positions)}


def open_position(user_id: str, data: dict) -> tuple[dict, int]:
    """Open a new paper position. Returns (response_dict, http_status)."""
    ticker    = (data.get('ticker') or '').strip().upper()
    direction = (data.get('direction') or '').strip().lower()
    try:
        entry = float(data['entry_price'])
        stop  = float(data['stop'])
        t1    = float(data['t1'])
        t2    = float(data['t2']) if data.get('t2') is not None else None
        qty   = float(data.get('quantity', 1))
    except (KeyError, ValueError, TypeError) as exc:
        return {'error': f'missing or invalid field: {exc}'}, 400
    if not ticker:
        return {'error': 'ticker is required'}, 400
    if direction not in ('bullish', 'bearish'):
        return {'error': 'direction must be bullish or bearish'}, 400
    pattern_id = data.get('pattern_id')
    note       = data.get('note', '')
    now_iso    = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.execute(
        "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,100000.0,'GBP',?)",
        (user_id, now_iso)
    )
    cur = conn.execute(
        """INSERT INTO paper_positions
           (user_id, pattern_id, ticker, direction, entry_price, stop, t1, t2,
            quantity, status, partial_closed, opened_at, note)
           VALUES (?,?,?,?,?,?,?,?,?,'open',0,?,?)""",
        (user_id, pattern_id, ticker, direction, entry, stop, t1, t2, qty, now_iso, note)
    )
    pos_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {
        'id': pos_id, 'ticker': ticker, 'direction': direction,
        'entry_price': entry, 'stop': stop, 't1': t1, 't2': t2,
        'quantity': qty, 'status': 'open', 'opened_at': now_iso,
    }, 201


def close_position(user_id: str, pos_id: int, exit_price=None) -> tuple[dict, int]:
    """Close a paper position. Returns (response_dict, http_status)."""
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    pos = conn.execute(
        "SELECT * FROM paper_positions WHERE id=? AND user_id=?", (pos_id, user_id)
    ).fetchone()
    if not pos:
        conn.close()
        return {'error': 'position not found'}, 404
    if pos['status'] != 'open':
        conn.close()
        return {'error': 'position already closed'}, 400
    ep = float(exit_price) if exit_price is not None else pos['entry_price']
    qty = float(pos['quantity'])
    pnl_r = compute_pnl_r(pos['direction'], pos['entry_price'], ep, pos['stop'])
    conn.execute(
        "UPDATE paper_positions SET status='closed', exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
        (ep, pnl_r, now_iso, pos_id)
    )
    # Bug 1 fix: restore position value to balance on close
    conn.execute(
        'UPDATE paper_account SET virtual_balance = virtual_balance + ? WHERE user_id=?',
        (ep * qty, user_id)
    )
    conn.commit()
    conn.close()
    return {'id': pos_id, 'status': 'closed', 'exit_price': ep, 'pnl_r': pnl_r, 'closed_at': now_iso}, 200


# ── Monitor ───────────────────────────────────────────────────────────────────

def monitor_positions(user_id: str) -> dict:
    """Check open positions vs live prices, update stops/targets hit."""
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(ext.DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    open_pos = conn.execute(
        "SELECT * FROM paper_positions WHERE user_id=? AND status='open'", (user_id,)
    ).fetchall()
    updates = []
    if not open_pos:
        conn.close()
        return {'checked': 0, 'updates': []}

    yf_syms = {pos['ticker']: _YF_MAP.get(pos['ticker'].lower(), pos['ticker']) for pos in open_pos}
    live_prices = fetch_live_prices(list(yf_syms.values()))
    sym_to_ticker = {v: k for k, v in yf_syms.items()}
    prices_by_ticker = {sym_to_ticker[sym]: price for sym, price in live_prices.items()}

    for pos in open_pos:
        ticker = pos['ticker']
        # Skip stop/target checks when market is closed — stale prices must not trigger exits
        if not _is_market_open(ticker):
            continue
        price  = prices_by_ticker.get(ticker, 0)
        if price <= 0:
            continue
        entry = pos['entry_price']
        stop  = pos['stop']
        t1    = pos['t1']
        t2    = pos['t2']
        risk  = abs(entry - stop) if abs(entry - stop) > 0 else 1
        direction = pos['direction']
        new_status = None
        exit_p = None
        qty = float(pos['quantity'])
        if direction == 'bullish':
            if price <= stop:
                new_status = 'stopped_out'; exit_p = price
            elif t2 is not None and price >= t2:
                new_status = 't2_hit'; exit_p = price
            elif not pos['partial_closed'] and price >= t1:
                # Bug 3 fix: partial close — halve quantity, restore half value, log t1 pnl_r
                half_qty = round(qty / 2, 6)
                t1_pnl_r = compute_pnl_r(direction, entry, price, stop)
                partial_value = round(price * half_qty, 2)
                conn.execute(
                    'UPDATE paper_positions SET partial_closed=1, quantity=? WHERE id=?',
                    (half_qty, pos['id'])
                )
                conn.execute(
                    'UPDATE paper_account SET virtual_balance = virtual_balance + ? WHERE user_id=?',
                    (partial_value, user_id)
                )
                conn.execute(
                    'INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)',
                    (user_id, 't1_hit', ticker,
                     f't1_hit at {price:.4f} | partial close {half_qty} units £{partial_value:,.2f} | pnl_r={t1_pnl_r} on closed half',
                     now_iso)
                )
                updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price, 'pnl_r': t1_pnl_r})
                # Calibration feedback for T1 partial close
                if pos['pattern_id']:
                    try:
                        from analytics.signal_calibration import update_calibration as _upd_cal_t1
                        _pat_t1 = conn.execute(
                            "SELECT pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                            (pos['pattern_id'],)
                        ).fetchone()
                        if _pat_t1:
                            _upd_cal_t1(
                                ticker=ticker,
                                pattern_type=(_pat_t1[0] or 'unknown'),
                                timeframe=(_pat_t1[1] or '4h'),
                                market_regime=_pat_t1[2],
                                outcome='hit_t1',
                                db_path=ext.DB_PATH,
                            )
                    except Exception as _cal_t1_e:
                        _logger.debug('t1 calibration feedback failed for %s: %s', ticker, _cal_t1_e)
        else:
            if price >= stop:
                new_status = 'stopped_out'; exit_p = price
            elif t2 is not None and price <= t2:
                new_status = 't2_hit'; exit_p = price
            elif not pos['partial_closed'] and price <= t1:
                # Bug 3 fix: partial close — halve quantity, restore half value, log t1 pnl_r
                half_qty = round(qty / 2, 6)
                t1_pnl_r = compute_pnl_r(direction, entry, price, stop)
                partial_value = round(price * half_qty, 2)
                conn.execute(
                    'UPDATE paper_positions SET partial_closed=1, quantity=? WHERE id=?',
                    (half_qty, pos['id'])
                )
                conn.execute(
                    'UPDATE paper_account SET virtual_balance = virtual_balance + ? WHERE user_id=?',
                    (partial_value, user_id)
                )
                conn.execute(
                    'INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)',
                    (user_id, 't1_hit', ticker,
                     f't1_hit at {price:.4f} | partial close {half_qty} units £{partial_value:,.2f} | pnl_r={t1_pnl_r} on closed half',
                     now_iso)
                )
                updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price, 'pnl_r': t1_pnl_r})
        if new_status and exit_p is not None:
            pnl_r = compute_pnl_r(direction, entry, exit_p, stop)
            conn.execute(
                'UPDATE paper_positions SET status=?, exit_price=?, pnl_r=?, closed_at=? WHERE id=?',
                (new_status, exit_p, pnl_r, now_iso, pos['id'])
            )
            # Bug 1 fix: restore full remaining position value to balance on full exit
            conn.execute(
                'UPDATE paper_account SET virtual_balance = virtual_balance + ? WHERE user_id=?',
                (exit_p * qty, user_id)
            )
            updates.append({'id': pos['id'], 'ticker': ticker, 'event': new_status, 'price': price, 'pnl_r': pnl_r})
            # Calibration feedback — write outcome so conviction tiers improve
            if pos['pattern_id']:
                try:
                    from analytics.signal_calibration import update_calibration as _upd_cal
                    _cal_outcome_map = {'stopped_out': 'stopped_out', 't1_hit': 'hit_t1', 't2_hit': 'hit_t2'}
                    _cal_outcome = _cal_outcome_map.get(new_status)
                    if _cal_outcome:
                        _pat = conn.execute(
                            "SELECT pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                            (pos['pattern_id'],)
                        ).fetchone()
                        if _pat:
                            _upd_cal(
                                ticker=ticker,
                                pattern_type=(_pat[0] or 'unknown'),
                                timeframe=(_pat[1] or '4h'),
                                market_regime=_pat[2],
                                outcome=_cal_outcome,
                                db_path=ext.DB_PATH,
                            )
                except Exception as _cal_e:
                    _logger.debug('calibration feedback failed for %s: %s', ticker, _cal_e)
    conn.commit()
    conn.close()
    return {'checked': len(open_pos), 'updates': updates}


# ── Stats ─────────────────────────────────────────────────────────────────────

def get_stats(user_id: str) -> dict:
    """Performance breakdown by conviction and pattern type."""
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    closed = conn.execute(
        """SELECT p.*, ps.kb_conviction, ps.pattern_type
           FROM paper_positions p
           LEFT JOIN pattern_signals ps ON p.pattern_id = ps.id
           WHERE p.user_id=? AND p.status NOT IN ('open') AND p.pnl_r IS NOT NULL
           ORDER BY p.closed_at DESC""",
        (user_id,)
    ).fetchall()
    conn.close()
    rows = [dict(r) for r in closed]

    def _group_stats(items, key):
        groups = {}
        for r in items:
            k = r.get(key) or 'unknown'
            groups.setdefault(k, []).append(r['pnl_r'])
        result = []
        for k, pnls in groups.items():
            wins = sum(1 for p in pnls if p > 0)
            result.append({
                'label': k, 'trades': len(pnls), 'wins': wins,
                'win_rate_pct': round(wins / len(pnls) * 100, 1),
                'avg_r': round(sum(pnls) / len(pnls), 2),
            })
        return sorted(result, key=lambda x: -x['trades'])

    best  = max(rows, key=lambda r: r['pnl_r'], default=None)
    worst = min(rows, key=lambda r: r['pnl_r'], default=None)
    return {
        'total_closed': len(rows),
        'by_conviction': _group_stats(rows, 'kb_conviction'),
        'by_pattern_type': _group_stats(rows, 'pattern_type'),
        'best_trade': best,
        'worst_trade': worst,
    }


# ── Agent log ─────────────────────────────────────────────────────────────────

def get_agent_log(user_id: str, limit: int = 100) -> list[dict]:
    """Return last N agent activity entries."""
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, user_id, event_type, ticker, detail, created_at
           FROM paper_agent_log WHERE user_id=? ORDER BY created_at DESC LIMIT ?""",
        (user_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def reset_paper_trader(user_id: str) -> dict:
    """Factory reset: stop scanner, delete all positions/logs/equity, re-seed account at £100k."""
    stop_scanner(user_id)
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        conn.execute("DELETE FROM paper_positions WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM paper_agent_log WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM paper_equity_log WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM paper_account WHERE user_id=?", (user_id,))
        # Re-insert with default £100k balance and account_size_set=0 so onboarding modal fires
        ensure_paper_tables(conn)
        conn.execute(
            "INSERT INTO paper_account (user_id, virtual_balance, currency, created_at, account_size_set) "
            "VALUES (?, 100000.0, 'GBP', ?, 0)",
            (user_id, now_iso)
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "reset", "user_id": user_id}


def export_log_csv(user_id: str) -> bytes:
    """Export full audit log + positions as CSV bytes."""
    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    log_rows = conn.execute(
        "SELECT id, event_type, ticker, detail, created_at FROM paper_agent_log WHERE user_id=? ORDER BY created_at ASC",
        (user_id,)
    ).fetchall()
    pos_rows = conn.execute(
        """SELECT id, ticker, direction, entry_price, stop, t1, t2, quantity,
                  status, partial_closed, opened_at, closed_at, exit_price, pnl_r, ai_reasoning
           FROM paper_positions WHERE user_id=? ORDER BY opened_at ASC""",
        (user_id,)
    ).fetchall()
    conn.close()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['=== POSITIONS ==='])
    w.writerow(['id', 'ticker', 'direction', 'entry_price', 'stop', 't1', 't2', 'quantity',
                 'status', 'partial_closed', 'opened_at', 'closed_at', 'exit_price', 'pnl_r', 'ai_reasoning'])
    for r in pos_rows:
        w.writerow(list(r))
    w.writerow([])
    w.writerow(['=== AGENT LOG ==='])
    w.writerow(['id', 'event_type', 'ticker', 'detail', 'created_at'])
    for r in log_rows:
        w.writerow(list(r))
    return buf.getvalue().encode('utf-8')


# ── KB-aware agent ────────────────────────────────────────────────────────────

def paper_kb_chat(ticker: str, question: str, kg_conn):
    """Route a paper-agent decision through KB-aware pipeline."""
    if not ext.HAS_LLM:
        return None, 0
    try:
        snippet, atoms = ext.retrieve(question, kg_conn, limit=30)
        atom_count = len(atoms)
        messages = ext.build_prompt(
            user_message=question, snippet=snippet,
            atom_count=atom_count, trader_level='developing',
        )
        if messages and messages[0]['role'] == 'system':
            messages[0]['content'] = _PAPER_AGENT_SYSTEM + '\n\n' + messages[0]['content']
        return ext.llm_chat(messages), atom_count
    except Exception as _e:
        _logger.warning('paper_kb_chat error for %s: %s', ticker, _e)
        return None, 0


def _should_enter(candidate: dict, remaining_cash: float, risk_per_trade: float) -> tuple[bool, str]:
    """Data-driven entry decision. Returns (should_enter, reasoning).

    Replaces LLM for every candidate — <1ms per call vs 13s LLM round-trip.
    LLM remains available as an optional confirmation layer (PAPER_AGENT_USE_LLM=1).
    """
    quality    = candidate.get('quality_score', 0) or 0
    conviction = (candidate.get('kb_conviction') or '').lower()
    cal_hr     = candidate.get('cal_hit_rate')
    cal_n      = candidate.get('cal_samples', 0) or 0
    signal_dir = (candidate.get('kb_signal_dir') or '').lower()
    direction  = candidate.get('direction', '')

    # Hard reject: quality too low regardless of anything else
    if quality < 0.60:
        return False, f'quality {quality:.2f} below 0.60'

    # Hard reject: insufficient cash (below minimum viable 2× risk)
    if remaining_cash < risk_per_trade * 2:
        return False, f'insufficient cash {remaining_cash:.0f} < {risk_per_trade * 2:.0f}'

    # Calibration-proven: enter if hit rate > 55% with ≥15 samples
    if cal_hr is not None and cal_hr > 0.55 and cal_n >= 15:
        return True, f'calibration-proven: hr={cal_hr:.0%} n={cal_n}'

    # Calibration negative: skip if hit rate < 40% with ≥10 samples
    if cal_hr is not None and cal_hr < 0.40 and cal_n >= 10:
        return False, f'calibration-negative: hr={cal_hr:.0%} n={cal_n}'

    # High quality + strong conviction: enter
    if quality >= 0.75 and conviction in ('high', 'confirmed', 'strong'):
        return True, f'high quality+conviction: q={quality:.2f} {conviction}'

    # Quality ≥ 0.70 + signal direction alignment: enter
    if quality >= 0.70 and signal_dir:
        bull_aligned = direction == 'bullish' and signal_dir in ('bullish', 'long', 'near_high')
        bear_aligned = direction == 'bearish' and signal_dir in ('bearish', 'short', 'near_low')
        if bull_aligned or bear_aligned:
            return True, f'quality+signal aligned: q={quality:.2f} signal={signal_dir}'

    # Quality ≥ 0.70 + moderate conviction: enter
    if quality >= 0.70 and conviction in ('medium', 'moderate'):
        return True, f'quality+moderate conviction: q={quality:.2f} {conviction}'

    # Default: skip (conservative)
    return False, f'no strong signal: q={quality:.2f} conv={conviction} cal_hr={cal_hr}'


def ai_run(user_id: str) -> dict:
    """Core autonomous paper trading agent for one user.

    Called by two paths:
      1. PaperAgentAdapter (scheduler) via ai_global_run() — every 30 min, all pro/premium users.
      2. continuous_scan per-user thread — every 30 min, only when user explicitly starts scanner.
    A per-user trylock ensures concurrent calls are dropped rather than queued.
    """
    with _run_locks_lock:
        lock = _run_locks.setdefault(user_id, threading.Lock())
    if not lock.acquire(blocking=False):
        _logger.debug('ai_run skipped for %s — already in progress', user_id)
        return {'entries': 0, 'skips': 0, 'monitor_updates': [], 'skipped': True}
    try:
        return _ai_run_inner(user_id)
    finally:
        lock.release()


def _ai_run_inner(user_id: str) -> dict:
    """Inner body of ai_run — only called when per-user lock is held."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # Check exits before looking for new entries; wrapped so yfinance failure can't abort scan
    _monitor_updates = []
    try:
        _mon = monitor_positions(user_id)
        _monitor_updates = _mon.get('updates', [])
    except Exception as _mon_e:
        _logger.warning('monitor_positions failed in ai_run for %s: %s', user_id, _mon_e)

    try:
        # Use isolation_level=DEFERRED (default) but with an explicit BEGIN IMMEDIATE so
        # the open-positions read is serialised against any other concurrent scan for this user.
        conn = sqlite3.connect(ext.DB_PATH, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        conn.row_factory = sqlite3.Row

        # Ensure account row exists
        conn.execute(
            "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,100000.0,'GBP',?)",
            (user_id, now_iso)
        )
        conn.commit()

        open_rows = conn.execute(
            "SELECT ticker FROM paper_positions WHERE user_id=? AND status='open'", (user_id,)
        ).fetchall()
        open_tickers = {r['ticker'] for r in open_rows}

        _cooldown_cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        _cooldown_rows = conn.execute(
            "SELECT DISTINCT ticker FROM paper_positions WHERE user_id=? AND status='stopped_out' AND closed_at > ?",
            (user_id, _cooldown_cutoff)
        ).fetchall()
        cooled_tickers = {r['ticker'] for r in _cooldown_rows}

        if len(open_tickers) >= _PAPER_MAX_OPEN_POSITIONS:
            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                (user_id, 'scan_start', None,
                 f'Scan skipped — already at max {_PAPER_MAX_OPEN_POSITIONS} open positions', now_iso)
            )
            conn.commit()
            conn.close()
            return {'entries': 0, 'skips': 0, 'monitor_updates': []}

        acct_row = conn.execute(
            "SELECT virtual_balance FROM paper_account WHERE user_id=?", (user_id,)
        ).fetchone()
        balance = float(acct_row['virtual_balance']) if acct_row else 500000.0

        _pref_row = None
        try:
            _pref_row = conn.execute(
                "SELECT max_risk_per_trade_pct FROM user_preferences WHERE user_id=?", (user_id,)
            ).fetchone()
        except Exception:
            pass
        risk_pct = float((_pref_row[0] if _pref_row and _pref_row[0] else None) or 1.0)
        risk_pct = min(risk_pct, 2.0)
        risk_per_trade = balance * risk_pct / 100.0
        # Hard cap: no single position may exceed 20% of account notional
        max_position_value = balance * 0.20

        # balance (virtual_balance) already has position costs deducted on entry
        # and position proceeds restored on close — it IS the free cash.
        # DO NOT subtract committed capital again (that causes double-deduction).
        remaining_cash = balance

        entries = 0
        skips = 0

        conn.execute(
            "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
            (user_id, 'scan_start', None,
             f'Scanning open patterns for {user_id} ({len(open_tickers)}/{_PAPER_MAX_OPEN_POSITIONS} slots used, {len(cooled_tickers)} on 24h cooldown)', now_iso)
        )

        candidate_rows = conn.execute(
            """SELECT p.id, p.ticker, p.pattern_type, p.direction, p.zone_high, p.zone_low,
                      p.quality_score, p.kb_conviction, p.kb_regime, p.kb_signal_dir
               FROM pattern_signals p
               INNER JOIN (
                   SELECT ticker, MAX(quality_score) AS best_q
                   FROM pattern_signals
                   WHERE status NOT IN ('filled','broken')
                     AND (
                       (quality_score >= 0.70 AND LOWER(kb_conviction) IN ('high','confirmed','strong'))
                       OR (quality_score >= 0.65 AND (kb_conviction IS NULL OR kb_conviction = ''))
                     )
                   GROUP BY ticker
               ) best ON best.ticker = p.ticker AND best.best_q = p.quality_score
               WHERE p.status NOT IN ('filled','broken')
                 AND (
                   (p.quality_score >= 0.70 AND LOWER(p.kb_conviction) IN ('high','confirmed','strong'))
                   OR (p.quality_score >= 0.65 AND (p.kb_conviction IS NULL OR p.kb_conviction = ''))
                 )
               ORDER BY RANDOM()
               LIMIT 100"""
        ).fetchall()

        all_cands = [dict(r) for r in candidate_rows]

        # Part 9: enrich each candidate with calibration hit rate for sorting
        for _c in all_cands:
            try:
                from analytics.signal_calibration import get_calibration as _get_cal
                _cal = _get_cal(
                    ticker=_c['ticker'],
                    pattern_type=_c.get('pattern_type', ''),
                    timeframe='4h',
                    db_path=ext.DB_PATH,
                )
                _c['cal_hit_rate'] = _cal.hit_rate_t1 if _cal else None
                _c['cal_samples']  = _cal.sample_size  if _cal else 0
            except Exception:
                _c['cal_hit_rate'] = None
                _c['cal_samples']  = 0

        # Re-sort: calibration-proven patterns first, then by quality
        all_cands.sort(key=lambda _x: (
            _x.get('cal_hit_rate') or 0.0,
            _x.get('quality_score') or 0.0,
        ), reverse=True)

        candidates = all_cands[:50]

        conn.row_factory = None
        scanned = len(candidates)

        for c in candidates:
            ticker = c['ticker']
            direction = c['direction']
            quality = c.get('quality_score') or 0
            conviction = (c.get('kb_conviction') or '').upper()
            zone_low = float(c.get('zone_low') or 0)
            zone_high = float(c.get('zone_high') or 0)
            regime = (c.get('kb_regime') or '').lower()
            pattern_id = c['id']

            if ticker in open_tickers or ticker in cooled_tickers:
                skips += 1
                continue

            # Market hours guard — no entries when exchange is closed (stale prices)
            if not _is_market_open(ticker):
                skips += 1
                conn.execute(
                    "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                    (user_id, 'skip', ticker, f'{ticker} skipped — market closed', now_iso)
                )
                continue

            # Regime alignment filter — hard skip misaligned entries
            _regime_lower = regime.lower()
            _regime_misaligned = (
                (direction == 'bullish' and any(x in _regime_lower for x in ('risk_off', 'bearish', 'bear')))
                or (direction == 'bearish' and any(x in _regime_lower for x in ('risk_on', 'bullish', 'bull')))
            )
            if _regime_misaligned and _regime_lower:
                conn.execute(
                    "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                    (user_id, 'regime_skip', ticker,
                     f'{ticker} skipped — {direction} in {regime}', now_iso)
                )
                skips += 1
                continue

            midpoint = (zone_low + zone_high) / 2.0 if zone_low and zone_high else None
            if not midpoint or midpoint <= 0:
                skips += 1
                continue

            if direction == 'bullish':
                entry_p = midpoint
                stop_p = round(zone_low * 0.995, 6)
                risk = entry_p - stop_p
                t1_p = round(entry_p + risk * 2, 6)
                t2_p = round(entry_p + risk * 3, 6)
            else:
                entry_p = midpoint
                stop_p = round(zone_high * 1.005, 6)
                risk = stop_p - entry_p
                t1_p = round(entry_p - risk * 2, 6)
                t2_p = round(entry_p - risk * 3, 6)

            if risk <= 0:
                skips += 1
                continue

            pattern_type = c.get('pattern_type', '?')
            kb_signal_dir = (c.get('kb_signal_dir') or '').lower() or '?'
            cal_hr  = c.get('cal_hit_rate')
            cal_n   = c.get('cal_samples', 0)

            # Part 10: data-driven entry decision (replaces LLM for every candidate)
            # LLM used only as optional confirmation when PAPER_AGENT_USE_LLM=1 in env.
            should_enter, data_reason = _should_enter(c, remaining_cash, risk_per_trade)
            reasoning = (
                f'{pattern_type} {direction} | q={quality:.2f} {conviction} '
                f'regime={regime or "?"} signal_dir={kb_signal_dir} | {data_reason}'
            )
            action = 'ENTER' if should_enter else 'SKIP'

            # Optional LLM confirmation (env-gated; skipped by default for speed)
            import os as _os
            if action == 'ENTER' and _os.environ.get('PAPER_AGENT_USE_LLM') == '1':
                try:
                    kb_question = (
                        f"Paper trading decision for {ticker}: should I enter a {direction} position? "
                        f"Pattern type: {pattern_type}. Quality: {quality:.2f}. Conviction: {conviction}. "
                        f"Regime: {regime or '?'}. Signal direction: {c.get('kb_signal_dir','?')}. "
                        f"Zone: {zone_low}\u2013{zone_high}. "
                        f"Entry: {entry_p:.4f}, stop: {stop_p:.4f}, t1: {t1_p:.4f}, t2: {t2_p:.4f}. "
                        f"Reply with JSON only."
                    )
                    kg_conn = ext.kg.thread_local_conn()
                    raw, atom_count = paper_kb_chat(ticker, kb_question, kg_conn)
                    kb_depth = 'deep' if atom_count >= 15 else 'shallow' if atom_count >= 5 else 'thin'
                    if raw:
                        raw = raw.strip()
                        _s = raw.find('{')
                        _e = raw.rfind('}') + 1
                        if _s >= 0 and _e > _s:
                            parsed = json.loads(raw[_s:_e])
                            action = parsed.get('action', 'SKIP').upper()
                            llm_reasoning = parsed.get('reasoning', reasoning)[:200]
                            reasoning = f'{llm_reasoning} | {data_reason} | kb={kb_depth}({atom_count})'
                            if action == 'ENTER':
                                _llm_entry = float(parsed.get('entry', entry_p))
                                _llm_stop  = float(parsed.get('stop', stop_p))
                                if _llm_stop > 0 and abs(_llm_stop - stop_p) / stop_p <= 0.05:
                                    entry_p = _llm_entry
                                    stop_p  = _llm_stop
                                    t1_p = float(parsed.get('t1', t1_p))
                                    t2_p = float(parsed.get('t2', t2_p))
                                risk = abs(entry_p - stop_p)
                    else:
                        action = 'SKIP'
                        reasoning = f'{reasoning} | llm_no_response'
                except Exception as _llm_err:
                    _logger.debug('LLM confirmation skipped for %s: %s', ticker, _llm_err)

            if action == 'ENTER' and entries >= _PAPER_MAX_NEW_PER_SCAN:
                skips += 1
                continue

            if action == 'ENTER' and risk > 0:
                # Re-read open slot count from DB to prevent stale-read race
                # when two scan paths fire close together.
                _cur_open = conn.execute(
                    "SELECT COUNT(*) FROM paper_positions WHERE user_id=? AND status='open'", (user_id,)
                ).fetchone()[0]
                if _cur_open >= _PAPER_MAX_OPEN_POSITIONS:
                    skips += 1
                    continue

                # Size by risk: qty = risk_per_trade / stop_distance
                # Cap by notional: qty = min(risk_per_trade / risk, max_position_value / entry_p)
                # This prevents tight stops (e.g. 0.5%) from producing 200%+ notional positions
                qty_by_risk    = risk_per_trade / risk
                qty_by_notional = max_position_value / entry_p
                qty = round(min(qty_by_risk, qty_by_notional), 4)
                qty = max(qty, 0.0001)
                position_value = round(entry_p * qty, 2)
                if qty_by_risk > qty_by_notional:
                    _logger.info(
                        '%s: notional cap applied — risk-based qty=%.4f capped to %.4f (%.0f%% of acct)',
                        ticker, qty_by_risk, qty, position_value / balance * 100
                    )
                # Issue 2 fix: instead of hard-rejecting when position_value > remaining_cash,
                # scale down qty to fit available cash (minimum viable notional = risk_per_trade * 2)
                if position_value > remaining_cash:
                    if remaining_cash < risk_per_trade * 2:
                        skips += 1
                        conn.execute(
                            "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                            (user_id, 'skip', ticker,
                             f'Insufficient cash: need £{position_value:,.2f}, have £{remaining_cash:,.2f} (below min viable)', now_iso)
                        )
                        continue
                    qty = round(remaining_cash / entry_p, 4)
                    position_value = round(entry_p * qty, 2)
                remaining_cash -= position_value
                conn.execute(
                    "UPDATE paper_account SET virtual_balance = virtual_balance - ? WHERE user_id=?",
                    (position_value, user_id)
                )
                conn.execute(
                    """INSERT INTO paper_positions
                       (user_id, pattern_id, ticker, direction, entry_price, stop, t1, t2,
                        quantity, status, partial_closed, opened_at, note, ai_reasoning)
                       VALUES (?,?,?,?,?,?,?,?,?,'open',0,?,?,?)""",
                    (user_id, pattern_id, ticker, direction,
                     entry_p, stop_p, t1_p, t2_p, qty,
                     now_iso, f'AI agent: {pattern_type}', reasoning)
                )
                conn.execute(
                    "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                    (user_id, 'entry', ticker,
                     f'{direction} entry={entry_p:.4f} stop={stop_p:.4f} t1={t1_p:.4f} qty={qty:.4f} | {reasoning}', now_iso)
                )
                open_tickers.add(ticker)
                entries += 1
            else:
                skips += 1

        if skips > 0 or entries > 0:
            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                (user_id, 'skip', None,
                 f'Scanned {scanned} patterns — {entries} entr{"y" if entries==1 else "ies"}, {skips} skipped', now_iso)
            )
        # Write equity snapshot after all entries/exits are committed
        try:
            acct_now = conn.execute(
                'SELECT virtual_balance FROM paper_account WHERE user_id=?', (user_id,)
            ).fetchone()
            open_pos_rows = conn.execute(
                'SELECT entry_price, quantity FROM paper_positions WHERE user_id=? AND status=?',
                (user_id, 'open')
            ).fetchall()
            cash_now = float(acct_now[0]) if acct_now else 500000.0
            open_value = sum(float(r[0]) * float(r[1]) for r in open_pos_rows)
            equity_now = round(cash_now + open_value, 2)
            open_count = len(open_pos_rows)
            conn.execute(
                'INSERT INTO paper_equity_log (user_id, equity_value, cash_balance, open_positions, logged_at) VALUES (?,?,?,?,?)',
                (user_id, equity_now, cash_now, open_count, now_iso)
            )
        except Exception as _eq_e:
            _logger.warning('equity log write failed for %s: %s', user_id, _eq_e)
        conn.commit()
        conn.close()

        # Issue 3 fix: run monitor again after entries so any positions that gap through
        # their stop during LLM-decision latency are caught in the same cycle.
        if entries > 0:
            try:
                _mon2 = monitor_positions(user_id)
                _monitor_updates.extend(_mon2.get('updates', []))
            except Exception as _mon2_e:
                _logger.warning('post-entry monitor_positions failed for %s: %s', user_id, _mon2_e)

        return {'entries': entries, 'skips': skips, 'monitor_updates': _monitor_updates}

    except Exception as e:
        _logger.error('ai_run error for %s: %s', user_id, e)
        try:
            conn.close()
        except Exception:
            pass
        return {'error': str(e)}


# ── Continuous scanner ────────────────────────────────────────────────────────

_scanner_threads: dict = {}
_scanner_lock = threading.Lock()

# Per-user lock: dropped (not queued) if ai_run() is already in progress for that user
_run_locks: dict[str, threading.Lock] = {}
_run_locks_lock = threading.Lock()


def _set_agent_running_db(user_id: str, running: bool) -> None:
    """Persist agent running state to DB so it survives server restarts."""
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        ensure_paper_tables(conn)
        # Upsert: works whether or not the paper_account row exists yet
        conn.execute(
            '''
            INSERT INTO paper_account (user_id, virtual_balance, currency, created_at, agent_running)
            VALUES (?, 100000.0, 'GBP', datetime('now'), ?)
            ON CONFLICT(user_id) DO UPDATE SET agent_running=excluded.agent_running
            ''',
            (user_id, 1 if running else 0)
        )
        conn.commit()
        conn.close()
    except Exception as _e:
        _logger.warning('Failed to persist agent_running for %s: %s', user_id, _e)


def continuous_scan(user_id: str, stop_event: threading.Event, interval_sec: int = 1800, startup_delay: int = 0):
    """Loop: scan every interval_sec until stop_event is set."""
    _logger.info('Continuous scanner started for %s (startup_delay=%ds)', user_id, startup_delay)
    if startup_delay and stop_event.wait(startup_delay):
        _logger.info('Continuous scanner cancelled during startup delay for %s', user_id)
        return
    while not stop_event.is_set():
        try:
            ai_run(user_id)
        except Exception as _e:
            _logger.error('Scanner error for %s: %s', user_id, _e)
        stop_event.wait(interval_sec)
    _logger.info('Continuous scanner stopped for %s', user_id)


def start_scanner(user_id: str, startup_delay: int = 0) -> tuple[str, str]:
    """Start continuous scanner for user. Returns (status, message)."""
    with _scanner_lock:
        if user_id in _scanner_threads and not _scanner_threads[user_id].is_set():
            return 'already_running', 'Scanner already running'
        stop_ev = threading.Event()
        _scanner_threads[user_id] = stop_ev
    t = threading.Thread(target=continuous_scan, args=(user_id, stop_ev, 1800, startup_delay), daemon=True)
    t.start()
    _set_agent_running_db(user_id, True)
    return 'started', 'Continuous scanner started — scans every 30 min'


def stop_scanner(user_id: str) -> tuple[str, str]:
    """Stop continuous scanner for user. Returns (status, message)."""
    with _scanner_lock:
        ev = _scanner_threads.pop(user_id, None)
    if ev:
        ev.set()
    _set_agent_running_db(user_id, False)
    if ev:
        return 'stopped', 'Scanner stopped'
    return 'not_running', 'Scanner was not running'


def scanner_running(user_id: str) -> bool:
    """Is the continuous scanner running for this user?
    Checks in-memory thread first; falls back to DB flag for post-restart accuracy.
    """
    with _scanner_lock:
        ev = _scanner_threads.get(user_id)
    if ev is not None:
        return not ev.is_set()
    # No in-memory thread — check persisted DB flag
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        row = conn.execute(
            'SELECT agent_running FROM paper_account WHERE user_id=?', (user_id,)
        ).fetchone()
        conn.close()
        return bool(row and row[0])
    except Exception:
        return False


def restore_scanners() -> None:
    """Re-launch scanners for all users with paper accounts configured.
    Starts for agent_running=1 (was running before restart) AND account_size_set=1
    (set a balance but may never have clicked Start Agent).
    """
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        ensure_paper_tables(conn)
        rows = conn.execute(
            'SELECT user_id FROM paper_account WHERE agent_running=1 OR account_size_set=1'
        ).fetchall()
        conn.close()
        for (uid,) in rows:
            status, _ = start_scanner(uid, startup_delay=60)
            _logger.info('restore_scanners: %s → %s', uid, status)
    except Exception as _e:
        _logger.error('restore_scanners failed: %s', _e)


# ── Global agent run (scheduler adapter) ──────────────────────────────────────

def ai_global_run():
    """Called by PaperAgentAdapter scheduler — runs agent for every pro/premium user."""
    try:
        from users.user_store import get_pro_premium_users
        users = get_pro_premium_users(ext.DB_PATH)
    except Exception:
        users = []
    for uid in users:
        try:
            ai_run(uid)
        except Exception:
            pass


class PaperAgentAdapter:
    """Ingest-scheduler-compatible adapter that runs the autonomous paper trading agent."""
    name = 'paper_agent'

    def run(self) -> None:
        ai_global_run()

"""routes/paper.py — Paper trading endpoints: account, positions, monitor, agent, stats."""

from __future__ import annotations

import csv
import io
import json
import logging
import random
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from flask import Blueprint, Response, g, jsonify, request

import extensions as ext

bp = Blueprint('paper', __name__)
_logger = logging.getLogger('paper_agent')


# ── Paper tables DDL ─────────────────────────────────────────────────────────

def _ensure_paper_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_account (
            user_id TEXT PRIMARY KEY,
            virtual_balance REAL NOT NULL DEFAULT 500000.0,
            currency TEXT NOT NULL DEFAULT 'GBP',
            created_at TEXT NOT NULL
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
    try:
        conn.execute("ALTER TABLE paper_positions ADD COLUMN ai_reasoning TEXT")
    except Exception:
        pass
    conn.commit()


def _paper_tier_check(user_id):
    """Return (tier, error_response) — error_response is None if tier is pro/premium."""
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
        return tier, (jsonify({'error': 'paper_trading_requires_pro', 'tier': tier}), 403)
    return tier, None


# ── YF ticker map for paper monitor ──────────────────────────────────────────

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
_PAPER_MAX_NEW_PER_SCAN = 3


# ── Route endpoints ──────────────────────────────────────────────────────────

@bp.route('/users/<user_id>/paper/account', methods=['GET'])
@ext.require_auth
def paper_account_get(user_id):
    """GET /users/<user_id>/paper/account — virtual balance + summary stats."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        now_iso = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,500000.0,'GBP',?)",
            (user_id, now_iso)
        )
        conn.commit()
        row = conn.execute(
            "SELECT virtual_balance, currency, created_at FROM paper_account WHERE user_id=?",
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
        open_tickers2 = list({r[0] for r in open_pos})
        live2 = {}
        if open_tickers2:
            try:
                import yfinance as _yf2
                batch2 = _yf2.download(
                    open_tickers2, period='1d', interval='1m',
                    progress=False, auto_adjust=True, threads=False
                )
                for tk in open_tickers2:
                    try:
                        if len(open_tickers2) == 1:
                            live2[tk] = float(batch2['Close'].dropna().iloc[-1])
                        else:
                            live2[tk] = float(batch2['Close'][tk].dropna().iloc[-1])
                    except Exception:
                        pass
            except Exception:
                pass
        unrealised_cash = 0.0
        for r in open_pos:
            tk, direction, entry, stop_, qty = r
            cp = live2.get(tk)
            if cp is not None and entry and stop_:
                risk = abs(entry - stop_)
                if risk > 0:
                    if direction == 'bullish':
                        pnl_r = (cp - entry) / risk
                    else:
                        pnl_r = (entry - cp) / risk
                    unrealised_cash += pnl_r * risk * qty
        account_value = round(row[0] + unrealised_cash, 2)
        return jsonify({
            'user_id': user_id,
            'virtual_balance': row[0],
            'account_value': account_value,
            'unrealised_pnl': round(unrealised_cash, 2),
            'currency': row[1],
            'created_at': row[2],
            'total_trades': total,
            'open_positions': open_c,
            'closed_trades': total_closed,
            'win_rate_pct': win_rate,
            'avg_r': avg_r,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/positions', methods=['GET'])
@ext.require_auth
def paper_positions_list(user_id):
    """GET /users/<user_id>/paper/positions?status=open|closed|all"""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    status_filter = request.args.get('status', 'all')
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
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
        live_prices = {}
        if open_tickers:
            try:
                import yfinance as _yf
                data_batch = _yf.download(
                    open_tickers, period='1d', interval='1m',
                    progress=False, auto_adjust=True, threads=False
                )
                for tk in open_tickers:
                    try:
                        if len(open_tickers) == 1:
                            price = float(data_batch['Close'].dropna().iloc[-1])
                        else:
                            price = float(data_batch['Close'][tk].dropna().iloc[-1])
                        live_prices[tk] = round(price, 4)
                    except Exception:
                        pass
            except Exception:
                pass
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
        return jsonify({'positions': positions, 'count': len(positions)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/positions', methods=['POST'])
@ext.require_auth
def paper_position_open(user_id):
    """POST /users/<user_id>/paper/positions — open a new paper position."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    data = request.get_json(force=True, silent=True) or {}
    ticker    = (data.get('ticker') or '').strip().upper()
    direction = (data.get('direction') or '').strip().lower()
    try:
        entry = float(data['entry_price'])
        stop  = float(data['stop'])
        t1    = float(data['t1'])
        t2    = float(data['t2']) if data.get('t2') is not None else None
        qty   = float(data.get('quantity', 1))
    except (KeyError, ValueError, TypeError) as exc:
        return jsonify({'error': f'missing or invalid field: {exc}'}), 400
    if not ticker:
        return jsonify({'error': 'ticker is required'}), 400
    if direction not in ('bullish', 'bearish'):
        return jsonify({'error': 'direction must be bullish or bearish'}), 400
    pattern_id = data.get('pattern_id')
    note       = data.get('note', '')
    now_iso    = datetime.now(timezone.utc).isoformat()
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.execute(
            "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,500000.0,'GBP',?)",
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
        return jsonify({'id': pos_id, 'ticker': ticker, 'direction': direction,
                        'entry_price': entry, 'stop': stop, 't1': t1, 't2': t2,
                        'quantity': qty, 'status': 'open', 'opened_at': now_iso}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/positions/<int:pos_id>/close', methods=['POST'])
@ext.require_auth
def paper_position_close(user_id, pos_id):
    """POST /users/<user_id>/paper/positions/<id>/close — manually close a position."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    data       = request.get_json(force=True, silent=True) or {}
    exit_price = data.get('exit_price')
    now_iso    = datetime.now(timezone.utc).isoformat()
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.row_factory = sqlite3.Row
        pos = conn.execute(
            "SELECT * FROM paper_positions WHERE id=? AND user_id=?", (pos_id, user_id)
        ).fetchone()
        if not pos:
            conn.close()
            return jsonify({'error': 'position not found'}), 404
        if pos['status'] != 'open':
            conn.close()
            return jsonify({'error': 'position already closed'}), 400
        ep = float(exit_price) if exit_price is not None else pos['entry_price']
        risk = abs(pos['entry_price'] - pos['stop'])
        if risk > 0:
            if pos['direction'] == 'bullish':
                pnl_r = round((ep - pos['entry_price']) / risk, 2)
            else:
                pnl_r = round((pos['entry_price'] - ep) / risk, 2)
        else:
            pnl_r = 0.0
        conn.execute(
            "UPDATE paper_positions SET status='closed', exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
            (ep, pnl_r, now_iso, pos_id)
        )
        conn.commit()
        conn.close()
        return jsonify({'id': pos_id, 'status': 'closed', 'exit_price': ep, 'pnl_r': pnl_r, 'closed_at': now_iso})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/monitor', methods=['POST'])
@ext.require_auth
def paper_monitor(user_id):
    """POST /users/<user_id>/paper/monitor — check open positions vs live prices."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        import yfinance as _yf
        conn = sqlite3.connect(ext.DB_PATH, timeout=15)
        _ensure_paper_tables(conn)
        conn.row_factory = sqlite3.Row
        open_pos = conn.execute(
            "SELECT * FROM paper_positions WHERE user_id=? AND status='open'", (user_id,)
        ).fetchall()
        updates = []
        for pos in open_pos:
            ticker  = pos['ticker']
            yf_sym  = _YF_MAP.get(ticker.lower(), ticker)
            try:
                info  = _yf.Ticker(yf_sym).fast_info
                price = float(info.get('last_price') or info.get('regularMarketPrice') or 0)
            except Exception:
                try:
                    hist  = _yf.Ticker(yf_sym).history(period='1d', interval='1m')
                    price = float(hist['Close'].iloc[-1]) if not hist.empty else 0
                except Exception:
                    price = 0
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
            if direction == 'bullish':
                if price <= stop:
                    new_status = 'stopped_out'; exit_p = price
                elif t2 is not None and price >= t2:
                    new_status = 't2_hit'; exit_p = price
                elif not pos['partial_closed'] and price >= t1:
                    conn.execute("UPDATE paper_positions SET partial_closed=1 WHERE id=?", (pos['id'],))
                    updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price})
            else:
                if price >= stop:
                    new_status = 'stopped_out'; exit_p = price
                elif t2 is not None and price <= t2:
                    new_status = 't2_hit'; exit_p = price
                elif not pos['partial_closed'] and price <= t1:
                    conn.execute("UPDATE paper_positions SET partial_closed=1 WHERE id=?", (pos['id'],))
                    updates.append({'id': pos['id'], 'ticker': ticker, 'event': 't1_hit', 'price': price})
            if new_status and exit_p is not None:
                if direction == 'bullish':
                    pnl_r = round((exit_p - entry) / risk, 2)
                else:
                    pnl_r = round((entry - exit_p) / risk, 2)
                conn.execute(
                    "UPDATE paper_positions SET status=?, exit_price=?, pnl_r=?, closed_at=? WHERE id=?",
                    (new_status, exit_p, pnl_r, now_iso, pos['id'])
                )
                updates.append({'id': pos['id'], 'ticker': ticker, 'event': new_status, 'price': price, 'pnl_r': pnl_r})
        conn.commit()
        conn.close()
        return jsonify({'checked': len(open_pos), 'updates': updates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/stats', methods=['GET'])
@ext.require_auth
def paper_stats(user_id):
    """GET /users/<user_id>/paper/stats — performance breakdown."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
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
        return jsonify({
            'total_closed': len(rows),
            'by_conviction': _group_stats(rows, 'kb_conviction'),
            'by_pattern_type': _group_stats(rows, 'pattern_type'),
            'best_trade': best,
            'worst_trade': worst,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Paper Agent (autonomous) ─────────────────────────────────────────────────

def _paper_kb_chat(ticker: str, question: str, kg_conn):
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
        _logger.warning('_paper_kb_chat error for %s: %s', ticker, _e)
        return None, 0


def _paper_ai_run(user_id: str) -> dict:
    """Core autonomous paper trading agent for one user."""
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=15)
        _ensure_paper_tables(conn)
        conn.row_factory = sqlite3.Row

        conn.execute(
            "INSERT OR IGNORE INTO paper_account (user_id, virtual_balance, currency, created_at) VALUES (?,500000.0,'GBP',?)",
            (user_id, now_iso)
        )

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
        max_position_value = balance * 0.10
        remaining_cash = balance

        entries = 0
        skips = 0

        conn.execute(
            "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
            (user_id, 'scan_start', None,
             f'Scanning open patterns for {user_id} ({len(open_tickers)}/{_PAPER_MAX_OPEN_POSITIONS} slots used, {len(cooled_tickers)} on 24h cooldown)', now_iso)
        )
        conn.commit()

        candidate_rows = conn.execute(
            """SELECT p.id, p.ticker, p.pattern_type, p.direction, p.zone_high, p.zone_low,
                      p.quality_score, p.kb_conviction, p.kb_regime, p.kb_signal_dir
               FROM pattern_signals p
               INNER JOIN (
                   SELECT ticker, MAX(quality_score) AS best_q
                   FROM pattern_signals
                   WHERE status NOT IN ('filled','broken')
                     AND quality_score >= 0.70
                     AND LOWER(kb_conviction) IN ('high','confirmed','strong')
                   GROUP BY ticker
               ) best ON best.ticker = p.ticker AND best.best_q = p.quality_score
               WHERE p.status NOT IN ('filled','broken')
                 AND p.quality_score >= 0.70
                 AND LOWER(p.kb_conviction) IN ('high','confirmed','strong')
               ORDER BY RANDOM()
               LIMIT 100"""
        ).fetchall()

        all_cands = [dict(r) for r in candidate_rows]
        high_band = [c for c in all_cands if c['quality_score'] >= 0.85]
        mid_band  = [c for c in all_cands if 0.75 <= c['quality_score'] < 0.85]
        low_band  = [c for c in all_cands if c['quality_score'] < 0.75]
        random.shuffle(high_band)
        random.shuffle(mid_band)
        random.shuffle(low_band)
        candidates = (high_band + mid_band + low_band)[:50]

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

            action = 'ENTER'
            pattern_type = c.get('pattern_type', '?')
            kb_signal_dir = (c.get('kb_signal_dir') or '').lower() or '?'
            reasoning = (
                f'{pattern_type} {direction} | q={quality:.2f} {conviction} '
                f'regime={regime or "?"} signal_dir={kb_signal_dir}'
            )

            # KB-aware LLM decision
            try:
                kb_question = (
                    f"Paper trading decision for {ticker}: should I enter a {direction} position? "
                    f"Pattern type: {pattern_type}. Quality: {quality:.2f}. Conviction: {conviction}. "
                    f"Regime: {regime or '?'}. Signal direction: {c.get('kb_signal_dir','?')}. "
                    f"Zone: {zone_low}–{zone_high}. "
                    f"Entry: {entry_p:.4f}, stop: {stop_p:.4f}, t1: {t1_p:.4f}, t2: {t2_p:.4f}. "
                    f"Reply with JSON only."
                )
                kg_conn = ext.kg.thread_local_conn()
                raw, atom_count = _paper_kb_chat(ticker, kb_question, kg_conn)
                kb_depth = 'deep' if atom_count >= 15 else 'shallow' if atom_count >= 5 else 'thin'
                if raw:
                    raw = raw.strip()
                    start = raw.find('{')
                    end = raw.rfind('}') + 1
                    if start >= 0 and end > start:
                        parsed = json.loads(raw[start:end])
                        action = parsed.get('action', 'SKIP').upper()
                        llm_reasoning = parsed.get('reasoning', reasoning)[:200]
                        reasoning = f'{llm_reasoning} | kb_depth={kb_depth} ({atom_count} atoms)'
                        if action == 'ENTER':
                            entry_p = float(parsed.get('entry', entry_p))
                            stop_p = float(parsed.get('stop', stop_p))
                            t1_p = float(parsed.get('t1', t1_p))
                            t2_p = float(parsed.get('t2', t2_p))
                            risk = abs(entry_p - stop_p)
                else:
                    reasoning = f'{reasoning} | kb_depth={kb_depth} ({atom_count} atoms)'
            except Exception as llm_err:
                _logger.warning('KB-chat paper agent error for %s: %s', ticker, llm_err)

            if action == 'ENTER' and entries >= _PAPER_MAX_NEW_PER_SCAN:
                skips += 1
                continue

            if action == 'ENTER' and risk > 0:
                qty = round(risk_per_trade / risk, 4) if risk > 0 else 1.0
                qty = max(qty, 0.0001)
                position_value = round(entry_p * qty, 2)
                if position_value > max_position_value:
                    qty = round(max_position_value / entry_p, 4)
                    position_value = round(entry_p * qty, 2)
                if position_value > remaining_cash:
                    skips += 1
                    conn.execute(
                        "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                        (user_id, 'skip', ticker,
                         f'Insufficient cash: need £{position_value:,.2f}, have £{remaining_cash:,.2f}', now_iso)
                    )
                    continue
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

        if skips > 0:
            conn.execute(
                "INSERT INTO paper_agent_log (user_id, event_type, ticker, detail, created_at) VALUES (?,?,?,?,?)",
                (user_id, 'skip', None,
                 f'Scanned {scanned} patterns — {entries} entr{"y" if entries==1 else "ies"}, {skips} skipped', now_iso)
            )
        conn.commit()
        conn.close()
        return {'entries': entries, 'skips': skips, 'monitor_updates': []}

    except Exception as e:
        _logger.error('_paper_ai_run error for %s: %s', user_id, e)
        return {'error': str(e)}


# ── Continuous scanner state ──────────────────────────────────────────────────
_paper_scanner_threads: dict = {}


def _paper_continuous_scan(user_id, stop_event, interval_sec=120):
    """Loop: scan every interval_sec until stop_event is set."""
    _logger.info('Continuous scanner started for %s', user_id)
    while not stop_event.is_set():
        try:
            _paper_ai_run(user_id)
        except Exception as _e:
            _logger.error('Scanner error for %s: %s', user_id, _e)
        stop_event.wait(interval_sec)
    _logger.info('Continuous scanner stopped for %s', user_id)


@bp.route('/users/<user_id>/paper/agent/log', methods=['GET'])
@ext.require_auth
def paper_agent_log_get(user_id):
    """GET /users/<user_id>/paper/agent/log — last 100 agent activity entries."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, user_id, event_type, ticker, detail, created_at
               FROM paper_agent_log WHERE user_id=? ORDER BY created_at DESC LIMIT 100""",
            (user_id,)
        ).fetchall()
        conn.close()
        return jsonify({'log': [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/agent/run', methods=['POST'])
@ext.require_auth
def paper_agent_run_once(user_id):
    """POST /users/<user_id>/paper/agent/run — one-shot scan."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    try:
        result = _paper_ai_run(user_id)
        return jsonify({'status': 'ok', 'result': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/paper/agent/start', methods=['POST'])
@ext.require_auth
def paper_agent_start(user_id):
    """POST /users/<user_id>/paper/agent/start — start continuous scanner."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    if user_id in _paper_scanner_threads and _paper_scanner_threads[user_id].is_set() is False:
        return jsonify({'status': 'already_running', 'message': 'Scanner already running'})
    stop_ev = threading.Event()
    _paper_scanner_threads[user_id] = stop_ev
    t = threading.Thread(target=_paper_continuous_scan, args=(user_id, stop_ev, 1800), daemon=True)
    t.start()
    return jsonify({'status': 'started', 'message': 'Continuous scanner started — scans every 30 min'})


@bp.route('/users/<user_id>/paper/agent/stop', methods=['POST'])
@ext.require_auth
def paper_agent_stop(user_id):
    """POST /users/<user_id>/paper/agent/stop — stop continuous scanner."""
    err = ext.assert_self(user_id)
    if err: return err
    ev = _paper_scanner_threads.get(user_id)
    if ev:
        ev.set()
        del _paper_scanner_threads[user_id]
        return jsonify({'status': 'stopped', 'message': 'Scanner stopped'})
    return jsonify({'status': 'not_running', 'message': 'Scanner was not running'})


@bp.route('/users/<user_id>/paper/agent/status', methods=['GET'])
@ext.require_auth
def paper_agent_status(user_id):
    """GET /users/<user_id>/paper/agent/status — is scanner running?"""
    err = ext.assert_self(user_id)
    if err: return err
    running = user_id in _paper_scanner_threads and not _paper_scanner_threads[user_id].is_set()
    return jsonify({'running': running})


@bp.route('/users/<user_id>/paper/agent/log/export', methods=['GET'])
@ext.require_auth
def paper_agent_log_export(user_id):
    """GET /users/<user_id>/paper/agent/log/export — full audit log as CSV."""
    err = ext.assert_self(user_id)
    if err: return err
    _, terr = _paper_tier_check(user_id)
    if terr: return terr
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        _ensure_paper_tables(conn)
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
        csv_bytes = buf.getvalue().encode('utf-8')
        fname = f'paper_trade_log_{user_id}_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}.csv'
        return Response(
            csv_bytes, mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{fname}"'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Paper Agent Adapter (for ingest scheduler) ───────────────────────────────

def _paper_ai_global_run():
    """Called by PaperAgentAdapter scheduler — runs agent for every pro/premium user."""
    try:
        from users.user_store import get_pro_premium_users
        users = get_pro_premium_users(ext.DB_PATH)
    except Exception:
        users = []
    for uid in users:
        try:
            _paper_ai_run(uid)
        except Exception:
            pass


class PaperAgentAdapter:
    """Ingest-scheduler-compatible adapter that runs the autonomous paper trading agent."""
    name = 'paper_agent'

    def run(self) -> None:
        _paper_ai_global_run()

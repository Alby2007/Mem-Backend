"""routes_v2/telegram.py — Phase 8: Telegram bot webhook, callback, and registration.

Key changes from Flask version:
- current_app.logger → logging.getLogger(__name__)
- current_app.test_request_context hack in /telegram/callback → direct service calls
- request.get_json() → await request.json()
- webhook secret validation logic identical to Flask version
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

import extensions as ext

router = APIRouter()
_logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tg_api(method: str, payload: dict) -> bool:
    try:
        import requests as _rq
    except ImportError:
        return False
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        return False
    try:
        r = _rq.post(f"https://api.telegram.org/bot{token}/{method}",
                     json=payload, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


# ── /telegram/webhook ─────────────────────────────────────────────────────────

@router.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    _webhook_secret = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
    if _webhook_secret:
        sent_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if sent_token != _webhook_secret:
            _logger.warning("telegram_webhook: invalid secret token from %s",
                            request.client.host if request.client else "?")
            return JSONResponse({"ok": False}, status_code=403)

    update = await request.json()
    if "callback_query" in update:
        _handle_tg_callback(update["callback_query"])
    elif "message" in update:
        _handle_tg_message(update["message"])
    return {"ok": True}


# ── Oracle command dispatcher ─────────────────────────────────────────────────

import time as _time_module

# Link codes for existing Meridian accounts: code → {user_id, expires}
# Populated by POST /auth/telegram/link-code (authenticated endpoint)
_TG_LINK_CODES: dict = {}


def _e(text: str) -> str:
    """Escape freeform text for Telegram MarkdownV2."""
    _SPECIAL = r'\_*[]()~`>#+-=|{}.!'
    result = []
    for ch in str(text):
        if ch in _SPECIAL:
            result.append('\\')
        result.append(ch)
    return ''.join(result)


def _send(chat_id: str, text: str, parse_mode: str = 'MarkdownV2') -> None:
    _tg_api("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": parse_mode})


def _handle_tg_command(chat_id: str, text: str) -> None:
    """Dispatch slash commands for the Oracle Telegram bot."""
    parts = text.strip().split(maxsplit=1)
    cmd   = parts[0].lower().split('@')[0]   # strip @botname suffix
    arg   = parts[1].strip() if len(parts) > 1 else ''

    # ── /start [CODE] ─────────────────────────────────────────────────────
    if cmd == '/start':
        code = arg.strip().upper()
        if code:
            # Try login code flow (new account via Telegram)
            try:
                from routes_v2.auth import _TG_LOGIN_CODES
                if code in _TG_LOGIN_CODES:
                    entry = _TG_LOGIN_CODES[code]
                    if _time_module.time() < entry['expires']:
                        entry['chat_id'] = chat_id
                        entry['user_data'] = {}
                        _send(chat_id,
                              r"✅ *Account linked\." + "\n\n"
                              r"Return to the Meridian dashboard to complete setup\.")
                        return
                    else:
                        _send(chat_id,
                              r"⚠️ Code expired\. Generate a new one in Meridian › Profile › Oracle Bot\.")
                        return
            except Exception:
                pass
        _send(chat_id,
              "👋 *Oracle — Trading Galaxy*\n\n"
              r"I'm your AI trading assistant, connected to the live knowledge base\." + "\n\n"
              "*Commands:*\n"
              r"  /briefing — Daily market summary" + "\n"
              r"  /regime — Current market regime" + "\n"
              r"  /signals TICKER — Signal for any ticker" + "\n"
              r"  /positions — Your open paper positions" + "\n"
              r"  /track — System accuracy record" + "\n"
              r"  /link CODE — Link your Meridian account" + "\n"
              r"  /help — This message" + "\n\n"
              r"_Or just ask me anything in plain English\._")
        return

    # ── /link CODE ────────────────────────────────────────────────────────
    if cmd == '/link':
        code = arg.strip().upper()
        if not code:
            _send(chat_id,
                  r"Usage: /link YOUR\_CODE" + "\n\n"
                  r"Generate a code in Meridian › Profile › Oracle Bot\.")
            return
        try:
            # Clean expired codes
            expired = [k for k, v in _TG_LINK_CODES.items()
                       if v['expires'] < _time_module.time()]
            for k in expired:
                del _TG_LINK_CODES[k]

            entry = _TG_LINK_CODES.get(code)
            if not entry:
                _send(chat_id,
                      r"⚠️ Code not found\. Generate a new one in Meridian › Profile › Oracle Bot\.")
                return
            if _time_module.time() > entry['expires']:
                del _TG_LINK_CODES[code]
                _send(chat_id,
                      r"⚠️ Code expired\. Generate a new one in Meridian › Profile › Oracle Bot\.")
                return

            user_id = entry['user_id']
            del _TG_LINK_CODES[code]

            conn_link = sqlite3.connect(ext.DB_PATH, timeout=10)
            try:
                conn_link.execute(
                    "UPDATE user_preferences SET telegram_chat_id=? WHERE user_id=?",
                    (str(chat_id), user_id)
                )
                conn_link.commit()
            finally:
                conn_link.close()

            _send(chat_id,
                  r"✅ *Oracle connected\.*" + "\n\n"
                  r"Your Meridian account is now linked\. "
                  r"You'll receive daily briefings and tips here\." + "\n\n"
                  r"Try /briefing or ask me anything\.")
        except Exception as e:
            _logger.error("_handle_tg_command /link error: %s", e)
            _send(chat_id, r"⚠️ Something went wrong\. Please try again\.")
        return

    # ── /help ─────────────────────────────────────────────────────────────
    if cmd == '/help':
        _send(chat_id,
              r"*Oracle Commands*" + "\n\n"
              r"/briefing — Daily market summary" + "\n"
              r"/regime — Current market regime \+ top patterns" + "\n"
              r"/signals TICKER — Live signals for any ticker" + "\n"
              r"/positions — Your open paper positions" + "\n"
              r"/track — System prediction accuracy" + "\n"
              r"/link CODE — Link your Meridian account" + "\n\n"
              r"_Or ask anything in plain English\._")
        return

    # ── /regime ───────────────────────────────────────────────────────────
    if cmd == '/regime':
        try:
            conn_r = sqlite3.connect(ext.DB_PATH, timeout=5)
            regime_row = conn_r.execute(
                "SELECT object FROM facts WHERE subject='market' AND predicate='market_regime' "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            signal_row = conn_r.execute(
                "SELECT object FROM facts WHERE subject='market' AND predicate='signal_direction' "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            regime_val = regime_row[0] if regime_row else 'unknown'
            top_pats = conn_r.execute("""
                SELECT pattern_type, ROUND(AVG(hit_rate_t1)*100) as hr, SUM(sample_size) as n
                FROM signal_calibration
                WHERE market_regime=? AND sample_size >= 20
                GROUP BY pattern_type ORDER BY hr DESC LIMIT 4
            """, (regime_val,)).fetchall()
            fwd_row = conn_r.execute("""
                SELECT ROUND(AVG(forward_return_1w)*100, 2) as avg_1w, COUNT(*) as n
                FROM state_transitions WHERE scope='global'
                AND from_state_id LIKE ? AND forward_return_1w IS NOT NULL
            """, (f'%{regime_val.split("_")[0]}%',)).fetchone()
            conn_r.close()

            regime_emoji = (
                '🔴' if 'risk_off' in regime_val or 'contraction' in regime_val else
                '🟢' if 'risk_on' in regime_val or 'expansion' in regime_val else '🟡'
            )
            lines = [
                f"{regime_emoji} *Regime:* {_e(regime_val.replace('_', ' ').title())}",
                f"📡 *Signal:* {_e((signal_row[0] if signal_row else 'neutral').title())}",
            ]
            if fwd_row and fwd_row[0] is not None and (fwd_row[1] or 0) >= 3:
                arrow = '↑' if fwd_row[0] > 0 else '↓'
                lines.append(
                    f"📊 *Hist fwd 1w:* {arrow} {_e(str(fwd_row[0]))}% "
                    f"\\(n\\={_e(str(fwd_row[1]))}\\)"
                )
            if top_pats:
                lines.append("\n*Top Patterns in Regime:*")
                for pt, hr, n in top_pats:
                    bar_len = int((hr or 0) // 10)
                    bar = '█' * bar_len + '░' * (10 - bar_len)
                    lines.append(
                        f"  {_e(pt.replace('_', ' '))} `{bar}` {_e(str(int(hr or 0)))}%"
                    )
            _send(chat_id, '\n'.join(lines))
        except Exception as e:
            _logger.error("_handle_tg_command /regime error: %s", e)
            _send(chat_id, r"⚠️ Could not load regime data\.")
        return

    # ── /briefing ─────────────────────────────────────────────────────────
    if cmd == '/briefing':
        try:
            from datetime import datetime, timezone, timedelta
            conn_b = sqlite3.connect(ext.DB_PATH, timeout=5)
            row_b = conn_b.execute(
                "SELECT user_id FROM user_preferences WHERE telegram_chat_id=? LIMIT 1",
                (str(chat_id),)
            ).fetchone()
            user_id_b = row_b[0] if row_b else None
            regime_row_b = conn_b.execute(
                "SELECT object FROM facts WHERE subject='market' AND predicate='market_regime' "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            yesterday = (datetime.now(timezone.utc) - timedelta(hours=20)).isoformat()
            night = conn_b.execute("""
                SELECT COUNT(*) as n,
                       SUM(CASE WHEN status IN ('t1_hit','t2_hit','t1_hit_partial') THEN 1 ELSE 0 END) as wins,
                       ROUND(AVG(pnl_r), 2) as avg_r
                FROM paper_positions
                WHERE status IN ('t1_hit','t2_hit','stopped_out','t1_hit_partial')
                  AND user_id NOT IN ('system_discovery','observatory_engine')
                  AND opened_at >= ?
            """, (yesterday,)).fetchone()
            opps = conn_b.execute("""
                SELECT ticker, pattern_type, direction, ROUND(quality_score, 3)
                FROM pattern_signals
                WHERE status NOT IN ('filled','broken','expired')
                  AND kb_conviction NOT IN ('avoid','') AND kb_conviction IS NOT NULL
                ORDER BY quality_score DESC LIMIT 5
            """).fetchall()
            open_count = 0
            if user_id_b:
                open_count = conn_b.execute(
                    "SELECT COUNT(*) FROM paper_positions WHERE user_id=? AND status='open'",
                    (user_id_b,)
                ).fetchone()[0]
            conn_b.close()

            regime_val_b = regime_row_b[0] if regime_row_b else 'unknown'
            regime_emoji_b = (
                '🔴' if 'risk_off' in regime_val_b else
                '🟢' if 'risk_on' in regime_val_b else '🟡'
            )
            lines = [
                "*📋 Morning Briefing*",
                f"{regime_emoji_b} *{_e(regime_val_b.replace('_', ' ').title())}*",
                "",
            ]
            if night and night[0]:
                wr = f"{int(night[1] / night[0] * 100)}%" if night[0] else '—'
                avg_r_str = (
                    f"{'+'  if night[2] and night[2] >= 0 else ''}{night[2]}R"
                    if night[2] else '—'
                )
                lines.append(
                    f"*Overnight:* {_e(str(night[0]))} trades · "
                    f"{_e(wr)} win rate · avg {_e(avg_r_str)}"
                )
            if user_id_b and open_count:
                lines.append(f"*Open positions:* {_e(str(open_count))}")
            if opps:
                lines.append("\n*Top Setups:*")
                for ticker_b, pt_b, dir_b, qs_b in opps:
                    arrow_b = '📈' if dir_b == 'bullish' else '📉'
                    lines.append(
                        f"  {arrow_b} {_e(ticker_b)} {_e(pt_b.replace('_', ' '))} "
                        f"\\(q\\={_e(str(qs_b))}\\)"
                    )
            lines.append("\n" + r"_Ask about any ticker or use /signals TICKER_")
            _send(chat_id, '\n'.join(lines))
        except Exception as e:
            _logger.error("_handle_tg_command /briefing error: %s", e)
            _send(chat_id, r"⚠️ Could not load briefing\.")
        return

    # ── /signals TICKER ───────────────────────────────────────────────────
    if cmd == '/signals':
        ticker = arg.upper().strip()
        if not ticker:
            _send(chat_id, r"Usage: /signals TICKER — e\.g\. /signals AAPL")
            return
        try:
            conn_s = sqlite3.connect(ext.DB_PATH, timeout=5)
            preds = [
                'conviction_tier', 'signal_direction', 'signal_quality', 'price_regime',
                'macro_confirmation', 'last_price', 'price_target', 'upside_pct',
                'auto_thesis', 'sector_tailwind',
            ]
            rows_s = conn_s.execute(
                f"SELECT predicate, object FROM facts WHERE LOWER(subject)=? "
                f"AND predicate IN ({','.join('?' * len(preds))}) ORDER BY timestamp DESC",
                [ticker.lower()] + preds
            ).fetchall()
            patterns_s = conn_s.execute("""
                SELECT pattern_type, direction, timeframe, ROUND(quality_score, 3)
                FROM pattern_signals
                WHERE UPPER(ticker)=? AND status NOT IN ('filled','broken','expired')
                ORDER BY quality_score DESC LIMIT 3
            """, (ticker,)).fetchall()
            conn_s.close()

            d = {p: v for p, v in rows_s}
            if not d and not patterns_s:
                _send(chat_id,
                      f"No KB data for {_e(ticker)}\\. Try asking me in plain English\\.")
                return

            lines = [f"*📊 {_e(ticker)}*\n"]
            if d.get('conviction_tier'):
                tier_s = d['conviction_tier']
                t_emoji = (
                    '🟢' if tier_s == 'high' else
                    '🟡' if tier_s == 'medium' else
                    '🔴' if tier_s == 'avoid' else '⚪'
                )
                lines.append(f"{t_emoji} *Conviction:* {_e(tier_s.title())}")
            if d.get('signal_direction'):
                arr = (
                    '📈' if d['signal_direction'] == 'bullish' else
                    '📉' if d['signal_direction'] == 'bearish' else '➡️'
                )
                lines.append(f"{arr} *Signal:* {_e(d['signal_direction'].title())}")
            if d.get('signal_quality'):
                lines.append(f"🎯 *Quality:* {_e(d['signal_quality'].title())}")
            if d.get('macro_confirmation'):
                lines.append(
                    f"🌍 *Macro:* {_e(d['macro_confirmation'].replace('_', ' ').title())}"
                )
            if d.get('last_price'):
                regime_sfx = (
                    f" \\({_e(d['price_regime'].replace('_', ' '))}\\)"
                    if d.get('price_regime') else ''
                )
                lines.append(f"💰 *Price:* {_e(d['last_price'])}{regime_sfx}")
            if d.get('price_target') and d.get('upside_pct'):
                lines.append(
                    f"🎯 *Target:* {_e(d['price_target'])} "
                    f"\\({_e(d['upside_pct'])}% upside\\)"
                )
            if d.get('auto_thesis') and len(d['auto_thesis']) > 20:
                thesis_short = d['auto_thesis'][:200] + (
                    '…' if len(d['auto_thesis']) > 200 else ''
                )
                lines.append(f"\n📝 _{_e(thesis_short)}_")
            if patterns_s:
                lines.append("\n*Open Patterns:*")
                for pt_s, dir_s, tf_s, qs_s in patterns_s:
                    a_s = '📈' if dir_s == 'bullish' else '📉'
                    lines.append(
                        f"  {a_s} {_e(pt_s.replace('_', ' '))} {_e(tf_s)} "
                        f"\\(q\\={_e(str(qs_s))}\\)"
                    )
            _send(chat_id, '\n'.join(lines))
        except Exception as e:
            _logger.error("_handle_tg_command /signals error: %s", e)
            _send(chat_id, r"⚠️ Could not load signals\.")
        return

    # ── /positions ────────────────────────────────────────────────────────
    if cmd == '/positions':
        try:
            conn_p = sqlite3.connect(ext.DB_PATH, timeout=5)
            row_p = conn_p.execute(
                "SELECT user_id FROM user_preferences WHERE telegram_chat_id=? LIMIT 1",
                (str(chat_id),)
            ).fetchone()
            if not row_p:
                conn_p.close()
                _send(chat_id,
                      r"⚠️ No account linked\. Use /link CODE or connect in Meridian Profile\.")
                return
            user_id_p = row_p[0]
            positions_p = conn_p.execute("""
                SELECT ticker, direction, entry_price, stop, t1, opened_at
                FROM paper_positions WHERE user_id=? AND status='open'
                ORDER BY opened_at DESC LIMIT 10
            """, (user_id_p,)).fetchall()
            conn_p.close()

            if not positions_p:
                _send(chat_id, r"No open positions\.")
                return

            from datetime import datetime, timezone
            now_p = datetime.now(timezone.utc)
            lines = [f"*📋 Open Positions \\({_e(str(len(positions_p)))}\\)*\n"]
            for ticker_p, dir_p, entry_p, stop_p, t1_p, opened_p in positions_p:
                try:
                    dt_p = datetime.fromisoformat(opened_p.replace('Z', '+00:00'))
                    age_p = f"{(now_p - dt_p).days}d"
                except Exception:
                    age_p = "?"
                a_p = '📈' if dir_p == 'bullish' else '📉'
                rr_str = ''
                if entry_p and t1_p and stop_p and abs(entry_p - stop_p) > 0:
                    rr_val = abs(t1_p - entry_p) / abs(entry_p - stop_p)
                    rr_str = f" R:R {_e(f'{rr_val:.1f}')}"
                lines.append(
                    f"{a_p} *{_e(ticker_p)}* @ {_e(str(round(entry_p, 4)))} "
                    f"\\| Stop {_e(str(round(stop_p, 4)))} "
                    f"\\| T1 {_e(str(round(t1_p, 4)))}{rr_str} "
                    f"\\| _{_e(age_p)}_"
                )
            _send(chat_id, '\n'.join(lines))
        except Exception as e:
            _logger.error("_handle_tg_command /positions error: %s", e)
            _send(chat_id, r"⚠️ Could not load positions\.")
        return

    # ── /track ────────────────────────────────────────────────────────────
    if cmd == '/track':
        try:
            conn_t = sqlite3.connect(ext.DB_PATH, timeout=5)
            row_t = conn_t.execute("""
                SELECT COUNT(*) as n,
                       AVG(CASE WHEN outcome IN ('hit_t1','hit_t2','t1_hit') THEN 1.0 ELSE 0.0 END) as hr,
                       AVG(brier_t1) as brier
                FROM prediction_ledger
                WHERE outcome IS NOT NULL AND outcome != 'expired'
            """).fetchone()
            by_pat_t = conn_t.execute("""
                SELECT pattern_type, COUNT(*) as n,
                       ROUND(AVG(CASE WHEN outcome IN ('hit_t1','hit_t2','t1_hit')
                                      THEN 1.0 ELSE 0.0 END) * 100) as hr
                FROM prediction_ledger
                WHERE outcome IS NOT NULL AND outcome != 'expired'
                GROUP BY pattern_type ORDER BY hr DESC
            """).fetchall()
            conn_t.close()

            n_t, hr_t, brier_t = row_t
            hr_str_t = f"{int(hr_t * 100)}%" if hr_t else "—"
            brier_str_t = f"{round(brier_t, 3)}" if brier_t else "—"
            brier_lbl = (
                "excellent" if brier_t and brier_t < 0.10 else
                "good"      if brier_t and brier_t < 0.15 else
                "calibrating" if brier_t and brier_t < 0.25 else "developing"
            )
            lines = [
                r"*📈 System Track Record*" + "\n",
                f"✅ *{_e(hr_str_t)}* T1 hit rate",
                f"📊 *{_e(str(n_t or 0))}* predictions resolved",
                f"🎯 Brier: {_e(brier_str_t)} \\({_e(brier_lbl)}\\)\n",
            ]
            if by_pat_t:
                lines.append("*By Pattern:*")
                for pt_t, n_pt, hr_pt in by_pat_t:
                    bar_t = '█' * int((hr_pt or 0) // 10) + '░' * (10 - int((hr_pt or 0) // 10))
                    e_t = '🟢' if hr_pt and hr_pt >= 60 else '🟡' if hr_pt and hr_pt >= 40 else '🔴'
                    lines.append(
                        f"  {e_t} {_e(pt_t.replace('_', ' '))} `{bar_t}` "
                        f"{_e(str(int(hr_pt or 0)))}%"
                    )
            lines.append("\n" + r"_View full record at trading\-galaxy\.uk_")
            _send(chat_id, '\n'.join(lines))
        except Exception as e:
            _logger.error("_handle_tg_command /track error: %s", e)
            _send(chat_id, r"⚠️ Could not load track record\.")
        return

    # Unknown command — fall through to the LLM handler below
    pass


def _handle_tg_message(msg: dict) -> None:
    chat_id = str(msg.get("chat", {}).get("id", ""))
    text    = (msg.get("text") or "").strip()
    if not chat_id or not text:
        return

    if text.startswith("/"):
        _handle_tg_command(chat_id, text)
        return

    if not ext.HAS_PRODUCT_LAYER:
        return
    try:
        from users.user_store import get_user_by_chat_id
        user_id = get_user_by_chat_id(ext.DB_PATH, chat_id)
    except Exception as e:
        _logger.error("telegram_webhook: user lookup failed: %s", e)
        return

    if not user_id:
        _tg_api("sendMessage", {
            "chat_id":    chat_id,
            "text":       "⚠️ Your Telegram account is not linked\\. Visit [trading\\-galaxy\\.uk](https://trading-galaxy.uk) to connect your account\\.",
            "parse_mode": "MarkdownV2",
        })
        return

    session_id = f"TG_{chat_id}"
    history_messages = []
    _cs = None
    try:
        from knowledge.conversation_store import ConversationStore
        _cs = ConversationStore(ext.DB_PATH)
        history_messages = _cs.get_recent_messages_for_context(session_id, n_turns=6)
    except Exception:
        _cs = None

    _TG_PORTFOLIO_INTENT_KWS = (
        "my portfolio","my holdings","my positions","my stocks","my shares",
        "my book","my p&l","my pnl","my exposure","my allocation",
        "discuss my","analyse my","analyze my","review my",
        "affect my","impact my","affect portfolio","impact portfolio",
        "portfolio","holdings","positions",
    )
    _tg_wants_portfolio = any(kw in text.lower() for kw in _TG_PORTFOLIO_INTENT_KWS)

    conn = ext.kg.thread_local_conn()
    try:
        _retrieve_text = text
        if _tg_wants_portfolio and ext.HAS_PRODUCT_LAYER:
            try:
                from users.user_store import get_portfolio as _gp
                from retrieval import _extract_tickers as _et_tg
                _tg_cur_tickers = _et_tg(text)
                if not _tg_cur_tickers:
                    _holdings = _gp(ext.DB_PATH, user_id)
                    _port_tickers = [h["ticker"] for h in (_holdings or []) if h.get("ticker")]
                    if _port_tickers:
                        _retrieve_text = text + " " + " ".join(_port_tickers)
            except Exception:
                pass
        _tg_limit = 80 if _tg_wants_portfolio else 50
        snippet, atoms = ext.retrieve(_retrieve_text, conn, limit=_tg_limit)
    except Exception as e:
        _logger.error("telegram_webhook: retrieve failed: %s", e)
        snippet, atoms = "", []

    portfolio_context = None
    if _tg_wants_portfolio:
        try:
            from users.user_store import get_portfolio as _gp2, get_user_model as _gum2
            _holdings2 = _gp2(ext.DB_PATH, user_id)
            _model     = _gum2(ext.DB_PATH, user_id)
            if _holdings2:
                _h_parts = [f"{h['ticker']} ×{int(h['quantity'])}" for h in _holdings2[:20]]
                _total_cost = sum(
                    h["quantity"] * h["avg_cost"]
                    for h in _holdings2 if h.get("quantity") and h.get("avg_cost")
                )
                _lines = ["=== USER PORTFOLIO ===", f"Holdings: {', '.join(_h_parts)}"]
                if _total_cost > 0:
                    _lines.append(f"Total invested (cost basis): £{_total_cost:,.0f}")
                if _model:
                    _profile = " · ".join(p for p in [
                        _model.get("risk_tolerance",""),
                        _model.get("holding_style",""),
                        ", ".join(_model.get("sector_affinity") or []),
                    ] if p)
                    if _profile:
                        _lines.append(f"Risk profile: {_profile}")
                _lines.append("\nPer-holding KB signals:")
                for _ht in [h["ticker"] for h in _holdings2]:
                    try:
                        _ht_rows = conn.execute(
                            "SELECT predicate, object FROM facts WHERE subject=? AND predicate IN "
                            "('last_price','price_regime','signal_direction','signal_quality',"
                            "'return_1m','return_1y','upside_pct','conviction_tier','macro_confirmation','price_target') "
                            "ORDER BY predicate",
                            (_ht.lower(),),
                        ).fetchall()
                        if _ht_rows:
                            _d = {p: v for p, v in _ht_rows}
                            _price = _d.get("last_price","?"); _regime = _d.get("price_regime","?").replace("_"," ")
                            _dir = _d.get("signal_direction","?"); _qual = _d.get("signal_quality","?")
                            _conv = _d.get("conviction_tier","?"); _up = _d.get("upside_pct","?")
                            _target = _d.get("price_target",""); _ret1m = _d.get("return_1m","")
                            _implied = ""
                            try:
                                if _target and _price and _price != "?":
                                    _move = float(_target) - float(_price)
                                    _implied = f" Target: {_target} ({'up to' if _move >= 0 else 'down to'}, {_up}% upside)."
                            except Exception:
                                pass
                            _sent = (f"  {_ht}: price {_price} ({_regime}). Signal: {_dir}.{_implied} "
                                     f"Quality: {_qual}. Conviction: {_conv}.")
                            if _ret1m: _sent += f" 1m return: {_ret1m}%."
                            _lines.append(_sent)
                        else:
                            _lines.append(f"  {_ht}: No KB signals — answer from general knowledge.")
                    except Exception:
                        _lines.append(f"  {_ht}: No KB signals — answer from general knowledge.")
                portfolio_context = "\n".join(_lines)
        except Exception:
            pass

    tg_live_context = ""
    if ext.HAS_WORKING_MEMORY and ext.working_memory is not None:
        try:
            from retrieval import _extract_tickers as _et_tg2
            from knowledge.working_memory import _YF_TICKER_MAP, MAX_ON_DEMAND_TICKERS
            _tg_tickers = _et_tg2(text)
            if not _tg_tickers and _tg_wants_portfolio:
                try:
                    from users.user_store import get_portfolio as _gp3
                    _tg_ph = _gp3(ext.DB_PATH, user_id)
                    _tg_tickers = [h["ticker"] for h in (_tg_ph or []) if h.get("ticker")]
                except Exception:
                    pass
            _tg_wm_session = f"TG_WM_{chat_id}"
            _yf_vals = set(_YF_TICKER_MAP.values())
            _tg_missing = [
                t for t in _tg_tickers[:MAX_ON_DEMAND_TICKERS]
                if not ext.kb_has_atoms(t, ext.DB_PATH) or t in _YF_TICKER_MAP or t in _yf_vals
            ]
            if _tg_missing:
                ext.working_memory.open_session(_tg_wm_session)
                for _tt in _tg_missing[:MAX_ON_DEMAND_TICKERS]:
                    ext.working_memory.fetch_on_demand(_tt, _tg_wm_session, ext.DB_PATH)
                tg_live_context = ext.working_memory.get_session_snippet(_tg_wm_session)
        except Exception as e:
            _logger.debug("telegram_webhook: live fetch failed: %s", e)

    tg_stress_dict = None
    if ext.HAS_STRESS and atoms:
        try:
            _words = re.findall(r"\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b", text)
            _key_terms = list({w.lower() for w in _words if len(w) > 2})[:10]
            _sr = ext.compute_stress(atoms, _key_terms, conn)
            tg_stress_dict = {
                "composite_stress":      _sr.composite_stress,
                "decay_pressure":        _sr.decay_pressure,
                "authority_conflict":    _sr.authority_conflict,
                "supersession_density":  _sr.supersession_density,
                "conflict_cluster":      _sr.conflict_cluster,
                "domain_entropy":        _sr.domain_entropy,
            }
        except Exception:
            pass

    _tg_trader_level = "developing"
    try:
        from users.user_store import get_user as _get_tg_user
        _tg_user_row = _get_tg_user(ext.DB_PATH, user_id)
        if _tg_user_row:
            _tg_trader_level = _tg_user_row.get("trader_level") or "developing"
    except Exception:
        pass

    try:
        from llm.prompt_builder import build as _build_prompt
        messages = _build_prompt(
            user_message=text, snippet=snippet,
            portfolio_context=portfolio_context, atom_count=len(atoms),
            live_context=tg_live_context or None, stress=tg_stress_dict,
            has_history=bool(history_messages), telegram_mode=True,
            trader_level=_tg_trader_level,
        )
        if history_messages and len(messages) >= 2:
            _clean_history = [
                {"role": m["role"], "content": m["content"]}
                for m in history_messages
                if m.get("role") in ("user","assistant") and m.get("content")
            ]
            messages = [messages[0]] + _clean_history + [messages[-1]]
    except Exception as e:
        _logger.error("telegram_webhook: prompt build failed: %s", e)
        return

    try:
        answer = ext.llm_chat(messages)
    except Exception as e:
        _logger.error("telegram_webhook: LLM call failed: %s", e)
        answer = None

    if not answer:
        _tg_api("sendMessage", {"chat_id": chat_id,
                                "text": "⚠️ The AI is temporarily unavailable. Please try again in a moment."})
        return

    from notifications.telegram_notifier import escape_mdv2
    _tg_api("sendMessage", {"chat_id": chat_id, "text": escape_mdv2(answer), "parse_mode": "MarkdownV2"})

    if _cs is not None:
        try:
            _cs.add_message(session_id, "user",      text,   user_id=user_id)
            _cs.add_message(session_id, "assistant",  answer, user_id=user_id)
        except Exception:
            pass


def _handle_tg_callback(cb: dict) -> None:
    callback_id = cb.get("id", "")
    chat_id = str(cb.get("from", {}).get("id", ""))
    data    = (cb.get("data") or "").strip()

    _tg_api("answerCallbackQuery", {"callback_query_id": callback_id})

    if not data.startswith("pos:"):
        return
    parts = data.split(":")
    if len(parts) < 3:
        return
    try:
        followup_id = int(parts[1])
    except ValueError:
        return
    action = parts[2]

    if not ext.HAS_PRODUCT_LAYER:
        return
    try:
        from users.user_store import update_followup_status, get_user_by_chat_id
        from datetime import datetime, timezone
        user_id = get_user_by_chat_id(ext.DB_PATH, chat_id)
        if not user_id:
            return
        now_iso = datetime.now(timezone.utc).isoformat()
        if action in ("closed","stopped","partial"):
            status = "closed" if action != "stopped" else "stopped"
            update_followup_status(ext.DB_PATH, followup_id, status=status, closed_at=now_iso)
            _reply = "✅ Position marked as closed\\." if action != "stopped" else "🛑 Position marked as stopped out\\."
            _tg_api("sendMessage", {"chat_id": chat_id, "text": _reply, "parse_mode": "MarkdownV2"})
        elif action in ("hold_t2","override"):
            _tg_api("sendMessage", {"chat_id": chat_id, "text": "👍 Noted — still watching this position\\.",
                                    "parse_mode": "MarkdownV2"})
        elif action == "more":
            conn2 = sqlite3.connect(ext.DB_PATH, timeout=5)
            _row = conn2.execute("SELECT ticker FROM tip_followups WHERE id=?", (followup_id,)).fetchone()
            conn2.close()
            if _row:
                _ticker = _row[0]
                _conn = ext.kg.thread_local_conn()
                _snip, _atoms = ext.retrieve(f"{_ticker} signal conviction outlook", _conn, limit=20)
                if _snip:
                    from notifications.telegram_notifier import escape_mdv2
                    _tg_api("sendMessage", {"chat_id": chat_id,
                                            "text": escape_mdv2(f"📊 KB signals for {_ticker}:\n\n{_snip[:800]}"),
                                            "parse_mode": "MarkdownV2"})
    except Exception as e:
        _logger.error("telegram_webhook: callback handling failed: %s", e)


# ── /telegram/bot (legacy) ─────────────────────────────────────────────────────

@router.post("/telegram/bot")
async def telegram_bot_webhook(request: Request):
    import time as _time

    _webhook_secret = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
    if _webhook_secret:
        sent_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if sent_token != _webhook_secret:
            return JSONResponse({"ok": False}, status_code=403)

    update   = await request.json()
    msg      = update.get("message", {})
    text     = (msg.get("text") or "").strip()
    chat_id  = msg.get("chat", {}).get("id")
    from_user = msg.get("from", {})
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")

    def _bot_send(cid, text_msg):
        if not bot_token:
            return
        try:
            import urllib.request as _ur
            payload = json.dumps({"chat_id": cid, "text": text_msg}).encode()
            req = _ur.Request(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                              data=payload, headers={"Content-Type": "application/json"})
            _ur.urlopen(req, timeout=10)
        except Exception:
            pass

    if not text:
        return {"ok": True}

    if text.startswith("/start"):
        parts = text.split(maxsplit=1)
        code  = parts[1].strip().upper() if len(parts) > 1 else ""
        try:
            from routes_v2.auth import _TG_LOGIN_CODES
        except ImportError:
            _TG_LOGIN_CODES = {}
        if code and code in _TG_LOGIN_CODES:
            entry = _TG_LOGIN_CODES[code]
            if _time.time() < entry["expires"]:
                entry["chat_id"]   = chat_id
                entry["user_data"] = {
                    "id":         chat_id,
                    "first_name": from_user.get("first_name",""),
                    "last_name":  from_user.get("last_name",""),
                    "username":   from_user.get("username",""),
                }
                _bot_send(chat_id, "✅ Logged in! Return to the Trading Galaxy dashboard.")
            else:
                _bot_send(chat_id, "⚠️ That login code has expired. Please request a new one.")
        else:
            _bot_send(chat_id, "👋 Welcome to Trading Galaxy!\n\nSend me any question about markets, tickers, or your portfolio and I'll answer from the live knowledge base.\n\nTo link your account, use the Sign In button on the dashboard.")
        return {"ok": True}

    if text.startswith("/help"):
        _bot_send(chat_id,
            "Trading Galaxy Bot\n\nAsk me anything about markets, tickers, signals, or your portfolio.\n\n"
            "Examples:\n• Tell me about NVDA\n• What's the current market regime?\n• What's the signal on gold?\n\n"
            "Your chat is linked to your Trading Galaxy account if you've signed in via the dashboard.")
        return {"ok": True}

    if text.startswith("/"):
        return {"ok": True}

    if not ext.HAS_LLM or not ext.is_available():
        _bot_send(chat_id, "⚠️ The knowledge engine is temporarily unavailable. Please try again shortly.")
        return {"ok": True}

    tg_user_id = None
    try:
        conn4 = sqlite3.connect(ext.DB_PATH, timeout=5)
        tr = conn4.execute("SELECT user_id FROM user_preferences WHERE telegram_chat_id=? LIMIT 1",
                           (str(chat_id),)).fetchone()
        conn4.close()
        if tr:
            tg_user_id = tr[0]
    except Exception:
        pass

    try:
        conn5   = ext.kg.thread_local_conn()
        snippet, atoms = ext.retrieve(text, conn5, limit=25)

        _tg_trader_level = "developing"
        if tg_user_id and ext.HAS_PRODUCT_LAYER:
            try:
                _tl = ext.get_user(ext.DB_PATH, tg_user_id)
                if _tl:
                    _tg_trader_level = _tl.get("trader_level") or "developing"
            except Exception:
                pass

        stress_dict = None
        if ext.HAS_STRESS and atoms:
            try:
                _words = re.findall(r"\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b", text)
                _key_terms = list({w.lower() for w in _words if len(w) > 2})[:10]
                _sr = ext.compute_stress(atoms, _key_terms, conn5)
                stress_dict = {"composite_stress": _sr.composite_stress,
                               "decay_pressure": _sr.decay_pressure,
                               "authority_conflict": _sr.authority_conflict,
                               "supersession_density": _sr.supersession_density,
                               "conflict_cluster": _sr.conflict_cluster,
                               "domain_entropy": _sr.domain_entropy}
            except Exception:
                pass

        messages = ext.build_prompt(user_message=text, snippet=snippet, stress=stress_dict,
                                    atom_count=len(atoms), trader_level=_tg_trader_level)
        answer = ext.llm_chat(messages, model=ext.DEFAULT_MODEL if ext.HAS_LLM else "llama3.2")

        if answer:
            plain = re.sub(r"\*\*(.+?)\*\*", r"\1", answer)
            plain = re.sub(r"\*(.+?)\*", r"\1", plain)
            _bot_send(chat_id, plain[:4000])
        else:
            _bot_send(chat_id, "⚠️ Couldn't generate a response right now. Please try again.")
    except Exception as e:
        _logger.error("telegram_bot_webhook chat error: %s", e)
        _bot_send(chat_id, "⚠️ Something went wrong. Please try again in a moment.")

    return {"ok": True}


# ── /telegram/callback ─────────────────────────────────────────────────────────

@router.post("/telegram/callback")
async def telegram_callback(request: Request):
    data = await request.json()
    callback_query = data.get("callback_query", {})
    if not callback_query:
        return {"ok": True}

    callback_data = callback_query.get("data", "")
    from_user     = callback_query.get("from", {})
    tg_user_id    = str(from_user.get("id", ""))

    try:
        conn6 = sqlite3.connect(ext.DB_PATH, timeout=5)
        row   = conn6.execute("SELECT user_id FROM user_preferences WHERE telegram_chat_id=?",
                              (tg_user_id,)).fetchone()
        conn6.close()
        user_id = row[0] if row else None
    except Exception:
        user_id = None

    if not user_id:
        return {"ok": True}

    try:
        parts = callback_data.split(":")
        if len(parts) >= 3 and parts[0] == "tip":
            tip_id     = int(parts[1])
            action_map = {"taking": "taking_it","more": "tell_me_more","skip": "not_for_me",
                          "taking_it": "taking_it","tell_me_more": "tell_me_more","not_for_me": "not_for_me"}
            action = action_map.get(parts[2], parts[2])
            from routes_v2.patterns import tip_feedback_action as _tfa
            from pydantic import BaseModel as _BM
            class _TFR(_BM):
                user_id: Optional[str] = user_id
                action: str = action
                rejection_reason: str = "no_reason"
                pattern_id: Optional[int] = None
            await _tfa(tip_id, request, _TFR())

        elif len(parts) >= 3 and parts[0] == "pos":
            followup_id   = int(parts[1])
            pos_action_map = {"closed":"closed","hold_t2":"hold_t2","partial":"partial",
                              "override":"override","more":"override"}
            action = pos_action_map.get(parts[2], parts[2])
            from routes_v2.patterns import tip_position_update as _tpu
            from pydantic import BaseModel as _BM2
            class _PUR(_BM2):
                user_id: Optional[str] = user_id
                action: str = action
                exit_price: Optional[float] = None
                shares_closed: Optional[float] = None
                close_method: str = "manual"
            await _tpu(followup_id, request, _PUR())

    except Exception as e:
        _logger.warning("telegram_callback error: %s", e)

    return {"ok": True}


# ── /telegram/webhook/register ─────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    base_url: str = ""


@router.post("/telegram/webhook/register")
async def telegram_webhook_register(request: Request, data: RegisterRequest = RegisterRequest()):
    try:
        import requests as _rq
    except ImportError:
        raise HTTPException(503, detail="requests library not available")

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise HTTPException(503, detail="TELEGRAM_BOT_TOKEN not configured")

    base_url = data.base_url.rstrip("/")
    if not base_url:
        base_url = f"https://{request.headers.get('host','localhost')}"

    webhook_url = f"{base_url}/telegram/webhook"
    secret      = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")

    payload: dict = {"url": webhook_url, "allowed_updates": ["message","callback_query"]}
    if secret:
        payload["secret_token"] = secret

    try:
        resp    = _rq.post(f"https://api.telegram.org/bot{token}/setWebhook",
                           json=payload, timeout=10)
        tg_data = resp.json()
        return {"ok": resp.status_code == 200, "telegram_response": tg_data, "webhook_url": webhook_url}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

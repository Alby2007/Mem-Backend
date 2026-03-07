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


def _handle_tg_message(msg: dict) -> None:
    chat_id = str(msg.get("chat", {}).get("id", ""))
    text    = (msg.get("text") or "").strip()
    if not chat_id or not text:
        return

    if text.startswith("/"):
        if text == "/start":
            _tg_api("sendMessage", {
                "chat_id":    chat_id,
                "text":       "👋 *Trading Galaxy Bot*\n\nYour account is linked\\. Ask me anything about your portfolio, market signals, or geopolitical risks\\.",
                "parse_mode": "MarkdownV2",
            })
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
            # Direct call — no test_request_context needed in FastAPI
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

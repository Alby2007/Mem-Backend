"""routes/telegram.py — Telegram bot webhook, callback, and registration."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3

from flask import Blueprint, current_app, g, jsonify, request

import extensions as ext

bp = Blueprint('telegram', __name__)
_logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tg_api(method: str, payload: dict) -> bool:
    """Call a Telegram Bot API method. Returns True on HTTP 200."""
    try:
        import requests as _rq
    except ImportError:
        return False
    token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not token:
        return False
    try:
        r = _rq.post(
            f'https://api.telegram.org/bot{token}/{method}',
            json=payload, timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False


# ── /telegram/webhook ─────────────────────────────────────────────────────────

@bp.route('/telegram/webhook', methods=['POST'])
@ext.limiter.exempt
def telegram_webhook():
    """
    POST /telegram/webhook

    Receives Telegram update payloads pushed by Telegram servers.
    Handles two update types:
      - message       → KB-grounded chat reply to the sending user
      - callback_query → inline keyboard action (position close/hold/more)

    Security: validates X-Telegram-Bot-Api-Secret-Token header against
    TELEGRAM_WEBHOOK_SECRET env var (skipped if env var not set — dev mode).
    """
    _webhook_secret = os.environ.get('TELEGRAM_WEBHOOK_SECRET', '')
    if _webhook_secret:
        _sent_token = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if _sent_token != _webhook_secret:
            current_app.logger.warning('telegram_webhook: invalid secret token from %s', request.remote_addr)
            return jsonify({'ok': False}), 403

    update = request.get_json(force=True, silent=True) or {}

    if 'callback_query' in update:
        _handle_tg_callback(update['callback_query'])
    elif 'message' in update:
        _handle_tg_message(update['message'])

    return jsonify({'ok': True})


def _handle_tg_message(msg: dict) -> None:
    """
    Process an inbound Telegram text message:
    1. Identify user by chat_id → user_id lookup
    2. Load conversation history from ConversationStore
    3. Run KB retrieve + prompt build + LLM
    4. Reply with MarkdownV2-escaped answer
    5. Persist both turns to ConversationStore
    """
    chat_id = str(msg.get('chat', {}).get('id', ''))
    text = (msg.get('text') or '').strip()
    if not chat_id or not text:
        return

    # Ignore bot commands like /start
    if text.startswith('/'):
        if text == '/start':
            _tg_api('sendMessage', {
                'chat_id': chat_id,
                'text': '👋 *Trading Galaxy Bot*\n\nYour account is linked\\. Ask me anything about your portfolio, market signals, or geopolitical risks\\.',
                'parse_mode': 'MarkdownV2',
            })
        return

    # User lookup
    if not ext.HAS_PRODUCT_LAYER:
        return
    try:
        from users.user_store import get_user_by_chat_id
        user_id = get_user_by_chat_id(ext.DB_PATH, chat_id)
    except Exception as _e:
        current_app.logger.error('telegram_webhook: user lookup failed: %s', _e)
        return

    if not user_id:
        _tg_api('sendMessage', {
            'chat_id': chat_id,
            'text': '⚠️ Your Telegram account is not linked\\. Visit [trading\\-galaxy\\.uk](https://trading-galaxy.uk) to connect your account\\.',
            'parse_mode': 'MarkdownV2',
        })
        return

    # Conversation history
    session_id = f'TG_{chat_id}'
    history_messages = []
    _cs = None
    try:
        from knowledge.conversation_store import ConversationStore
        _cs = ConversationStore(ext.DB_PATH)
        history_messages = _cs.get_recent_messages_for_context(session_id, n_turns=6)
    except Exception:
        _cs = None

    # Portfolio-intent detection
    _TG_PORTFOLIO_INTENT_KWS = (
        'my portfolio', 'my holdings', 'my positions', 'my stocks', 'my shares',
        'my book', 'my p&l', 'my pnl', 'my exposure', 'my allocation',
        'discuss my', 'analyse my', 'analyze my', 'review my',
        'affect my', 'impact my', 'affect portfolio', 'impact portfolio',
        'portfolio', 'holdings', 'positions',
    )
    _tg_wants_portfolio = any(kw in text.lower() for kw in _TG_PORTFOLIO_INTENT_KWS)

    # KB retrieval
    conn = ext.kg.thread_local_conn()
    try:
        _retrieve_text = text
        if _tg_wants_portfolio and ext.HAS_PRODUCT_LAYER:
            try:
                from users.user_store import get_portfolio as _gp
                from retrieval import _extract_tickers as _et_tg
                _tg_cur_tickers = _et_tg(text)
                if not _tg_cur_tickers:
                    _tg_holdings = _gp(ext.DB_PATH, user_id)
                    _tg_port_tickers = [h['ticker'] for h in (_tg_holdings or []) if h.get('ticker')]
                    if _tg_port_tickers:
                        _retrieve_text = text + ' ' + ' '.join(_tg_port_tickers)
            except Exception:
                pass
        _tg_limit = 80 if _tg_wants_portfolio else 50
        snippet, atoms = ext.retrieve(_retrieve_text, conn, limit=_tg_limit)
    except Exception as _re:
        current_app.logger.error('telegram_webhook: retrieve failed: %s', _re)
        snippet, atoms = '', []

    # Portfolio context (per-ticker KB signals)
    portfolio_context = None
    if _tg_wants_portfolio:
        try:
            from users.user_store import get_portfolio as _gp2, get_user_model as _gum2
            _holdings = _gp2(ext.DB_PATH, user_id)
            _model = _gum2(ext.DB_PATH, user_id)
            if _holdings:
                _h_parts = [f"{h['ticker']} ×{int(h['quantity'])}" for h in _holdings[:20]]
                _pos_values = [
                    h['quantity'] * h['avg_cost']
                    for h in _holdings if h.get('quantity') and h.get('avg_cost')
                ]
                _total_cost = sum(_pos_values)
                _lines = ["=== USER PORTFOLIO ===",
                          f"Holdings: {', '.join(_h_parts)}"]
                if _total_cost > 0:
                    _lines.append(f"Total invested (cost basis): £{_total_cost:,.0f}")
                if _model:
                    _risk = _model.get('risk_tolerance', '')
                    _style = _model.get('holding_style', '')
                    _sectors = ', '.join(_model.get('sector_affinity') or [])
                    _profile = ' · '.join(p for p in [_risk, _style, _sectors] if p)
                    if _profile:
                        _lines.append(f"Risk profile: {_profile}")
                _holding_tickers = [h['ticker'] for h in _holdings]
                _lines.append("\nPer-holding KB signals:")
                for _ht in _holding_tickers:
                    try:
                        _ht_rows = conn.execute(
                            """SELECT predicate, object FROM facts
                               WHERE subject=? AND predicate IN
                               ('last_price','price_regime','signal_direction',
                                'signal_quality','return_1m','return_1y',
                                'upside_pct','conviction_tier','macro_confirmation',
                                'price_target')
                               ORDER BY predicate""",
                            (_ht.lower(),)
                        ).fetchall()
                        if _ht_rows:
                            _d = {p: v for p, v in _ht_rows}
                            _price = _d.get('last_price', '?')
                            _regime = _d.get('price_regime', '?').replace('_', ' ')
                            _dir = _d.get('signal_direction', '?')
                            _qual = _d.get('signal_quality', '?')
                            _conv = _d.get('conviction_tier', '?')
                            _up = _d.get('upside_pct', '?')
                            _target = _d.get('price_target', '')
                            _ret1m = _d.get('return_1m', '')
                            _ret1y = _d.get('return_1y', '')
                            _implied = ''
                            try:
                                if _target and _price and _price != '?' and _target != '?':
                                    _move = float(_target) - float(_price)
                                    _move_dir = 'up to' if _move >= 0 else 'down to'
                                    _implied = f" Target: {_target} ({_move_dir}, {_up}% upside)."
                            except Exception:
                                pass
                            _sent = (
                                f"  {_ht}: price {_price} ({_regime}). "
                                f"Signal: {_dir}.{_implied} "
                                f"Quality: {_qual}. Conviction: {_conv}."
                            )
                            if _ret1m:
                                _sent += f" 1m return: {_ret1m}%."
                            if _ret1y:
                                _sent += f" 1y return: {_ret1y}%."
                            _lines.append(_sent)
                        else:
                            _lines.append(f"  {_ht}: No KB signals — answer from general knowledge.")
                    except Exception:
                        _lines.append(f"  {_ht}: No KB signals — answer from general knowledge.")
                portfolio_context = '\n'.join(_lines)
        except Exception:
            pass

    # Live data fetch
    tg_live_context = ''
    if ext.HAS_WORKING_MEMORY and ext.working_memory is not None:
        try:
            from retrieval import _extract_tickers as _et_tg2
            from knowledge.working_memory import _YF_TICKER_MAP, MAX_ON_DEMAND_TICKERS
            _tg_tickers = _et_tg2(text)
            if not _tg_tickers and _tg_wants_portfolio:
                try:
                    from users.user_store import get_portfolio as _gp3
                    _tg_ph = _gp3(ext.DB_PATH, user_id)
                    _tg_tickers = [h['ticker'] for h in (_tg_ph or []) if h.get('ticker')]
                except Exception:
                    pass
            _tg_wm_session = f'TG_WM_{chat_id}'
            _yf_vals = set(_YF_TICKER_MAP.values())
            _tg_missing = [
                t for t in _tg_tickers[:MAX_ON_DEMAND_TICKERS]
                if not ext.kb_has_atoms(t, ext.DB_PATH)
                or t in _YF_TICKER_MAP
                or t in _yf_vals
            ]
            if _tg_missing:
                ext.working_memory.open_session(_tg_wm_session)
                for _tt in _tg_missing[:MAX_ON_DEMAND_TICKERS]:
                    ext.working_memory.fetch_on_demand(_tt, _tg_wm_session, ext.DB_PATH)
                tg_live_context = ext.working_memory.get_session_snippet(_tg_wm_session)
        except Exception as _tg_wm_err:
            current_app.logger.debug('telegram_webhook: live fetch failed: %s', _tg_wm_err)

    # Epistemic stress
    tg_stress_dict = None
    if ext.HAS_STRESS and atoms:
        try:
            _tg_words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', text)
            _tg_key_terms = list({w.lower() for w in _tg_words if len(w) > 2})[:10]
            _tg_stress = ext.compute_stress(atoms, _tg_key_terms, conn)
            tg_stress_dict = {
                'composite_stress': _tg_stress.composite_stress,
                'decay_pressure': _tg_stress.decay_pressure,
                'authority_conflict': _tg_stress.authority_conflict,
                'supersession_density': _tg_stress.supersession_density,
                'conflict_cluster': _tg_stress.conflict_cluster,
                'domain_entropy': _tg_stress.domain_entropy,
            }
        except Exception:
            pass

    # Trader level
    _tg_trader_level = 'developing'
    try:
        from users.user_store import get_user as _get_tg_user
        _tg_user_row = _get_tg_user(ext.DB_PATH, user_id)
        if _tg_user_row:
            _tg_trader_level = _tg_user_row.get('trader_level') or 'developing'
    except Exception:
        pass

    # Build prompt
    try:
        from llm.prompt_builder import build as _build_prompt
        messages = _build_prompt(
            user_message=text,
            snippet=snippet,
            portfolio_context=portfolio_context,
            atom_count=len(atoms),
            live_context=tg_live_context or None,
            stress=tg_stress_dict,
            has_history=bool(history_messages),
            telegram_mode=True,
            trader_level=_tg_trader_level,
        )
        if history_messages and len(messages) >= 2:
            _clean_history = [
                {'role': m['role'], 'content': m['content']}
                for m in history_messages
                if m.get('role') in ('user', 'assistant') and m.get('content')
            ]
            messages = [messages[0]] + _clean_history + [messages[-1]]
    except Exception as _pe:
        current_app.logger.error('telegram_webhook: prompt build failed: %s', _pe)
        return

    # LLM call
    try:
        answer = ext.llm_chat(messages)
    except Exception as _le:
        current_app.logger.error('telegram_webhook: LLM call failed: %s', _le)
        answer = None

    if not answer:
        _tg_api('sendMessage', {
            'chat_id': chat_id,
            'text': '⚠️ The AI is temporarily unavailable. Please try again in a moment.',
        })
        return

    # Send reply (MarkdownV2-escaped)
    from notifications.telegram_notifier import escape_mdv2
    _tg_api('sendMessage', {
        'chat_id': chat_id,
        'text': escape_mdv2(answer),
        'parse_mode': 'MarkdownV2',
    })

    # Persist both turns
    if _cs is not None:
        try:
            _cs.add_message(session_id, 'user', text, user_id=user_id)
            _cs.add_message(session_id, 'assistant', answer, user_id=user_id)
        except Exception:
            pass


def _handle_tg_callback(cb: dict) -> None:
    """
    Handle a Telegram inline keyboard callback_query.
    Callback data format: pos:{followup_id}:{action}
    Actions: closed | stopped | partial | hold_t2 | override | more
    """
    callback_id = cb.get('id', '')
    chat_id = str(cb.get('from', {}).get('id', ''))
    data = (cb.get('data') or '').strip()

    _tg_api('answerCallbackQuery', {'callback_query_id': callback_id})

    if not data.startswith('pos:'):
        return

    parts = data.split(':')
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

        if action in ('closed', 'stopped', 'partial'):
            status = 'closed' if action != 'stopped' else 'stopped'
            update_followup_status(ext.DB_PATH, followup_id, status=status, closed_at=now_iso)
            _reply = '✅ Position marked as closed\\.' if action != 'stopped' else '🛑 Position marked as stopped out\\.'
            _tg_api('sendMessage', {'chat_id': chat_id, 'text': _reply, 'parse_mode': 'MarkdownV2'})

        elif action in ('hold_t2', 'override'):
            _tg_api('sendMessage', {
                'chat_id': chat_id,
                'text': '👍 Noted — still watching this position\\.',
                'parse_mode': 'MarkdownV2',
            })

        elif action == 'more':
            _c3 = sqlite3.connect(ext.DB_PATH, timeout=5)
            _row = _c3.execute(
                "SELECT ticker FROM tip_followups WHERE id=?", (followup_id,)
            ).fetchone()
            _c3.close()
            if _row:
                _ticker = _row[0]
                _conn = ext.kg.thread_local_conn()
                _snip, _atoms = ext.retrieve(f'{_ticker} signal conviction outlook', _conn, limit=20)
                if _snip:
                    from notifications.telegram_notifier import escape_mdv2
                    _tg_api('sendMessage', {
                        'chat_id': chat_id,
                        'text': escape_mdv2(f'📊 KB signals for {_ticker}:\n\n{_snip[:800]}'),
                        'parse_mode': 'MarkdownV2',
                    })

    except Exception as _ce:
        current_app.logger.error('telegram_webhook: callback handling failed: %s', _ce)


# ── /telegram/bot (legacy simple webhook) ─────────────────────────────────────

@bp.route('/telegram/bot', methods=['POST'])
@ext.limiter.exempt
def telegram_bot_webhook():
    """
    POST /telegram/bot — Legacy Telegram bot webhook.
    Handles /start <code>, /help, and general text → KB chat pipeline.
    """
    import time as _time

    update = request.get_json(force=True, silent=True) or {}
    msg = update.get('message', {})
    text = (msg.get('text') or '').strip()
    chat = msg.get('chat', {})
    chat_id = chat.get('id')
    from_user = msg.get('from', {})

    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')

    def _bot_send(cid, text_msg):
        if not bot_token:
            return
        try:
            import urllib.request as _ur
            payload = json.dumps({'chat_id': cid, 'text': text_msg}).encode()
            req = _ur.Request(
                f'https://api.telegram.org/bot{bot_token}/sendMessage',
                data=payload, headers={'Content-Type': 'application/json'}
            )
            _ur.urlopen(req, timeout=10)
        except Exception:
            pass

    if not text:
        return jsonify({'ok': True})

    # /start <code> — login code flow
    if text.startswith('/start'):
        parts = text.split(maxsplit=1)
        code = parts[1].strip().upper() if len(parts) > 1 else ''
        from routes.auth import _TG_LOGIN_CODES
        if code and code in _TG_LOGIN_CODES:
            entry = _TG_LOGIN_CODES[code]
            if _time.time() < entry['expires']:
                entry['chat_id'] = chat_id
                entry['user_data'] = {
                    'id': chat_id,
                    'first_name': from_user.get('first_name', ''),
                    'last_name': from_user.get('last_name', ''),
                    'username': from_user.get('username', ''),
                }
                _bot_send(chat_id, "✅ Logged in! Return to the Trading Galaxy dashboard.")
            else:
                _bot_send(chat_id, "⚠️ That login code has expired. Please request a new one.")
        else:
            _bot_send(chat_id, "👋 Welcome to Trading Galaxy!\n\nSend me any question about markets, tickers, or your portfolio and I'll answer from the live knowledge base.\n\nTo link your account, use the Sign In button on the dashboard.")
        return jsonify({'ok': True})

    if text.startswith('/help'):
        _bot_send(chat_id,
            "Trading Galaxy Bot\n\n"
            "Ask me anything about markets, tickers, signals, or your portfolio.\n\n"
            "Examples:\n"
            "• Tell me about NVDA\n"
            "• What's the current market regime?\n"
            "• What's the signal on gold?\n"
            "• What market are we in?\n\n"
            "Your chat is linked to your Trading Galaxy account if you've signed in via the dashboard."
        )
        return jsonify({'ok': True})

    if text.startswith('/'):
        return jsonify({'ok': True})

    # General chat — route through KB pipeline
    if not ext.HAS_LLM or not ext.is_available():
        _bot_send(chat_id, "⚠️ The knowledge engine is temporarily unavailable. Please try again shortly.")
        return jsonify({'ok': True})

    tg_user_id = None
    try:
        _tc = sqlite3.connect(ext.DB_PATH, timeout=5)
        _tr = _tc.execute(
            "SELECT user_id FROM user_preferences WHERE telegram_chat_id=? LIMIT 1",
            (str(chat_id),)
        ).fetchone()
        _tc.close()
        if _tr:
            tg_user_id = _tr[0]
    except Exception:
        pass

    try:
        conn = ext.kg.thread_local_conn()
        snippet, atoms = ext.retrieve(text, conn, limit=25)

        _tg_trader_level = 'developing'
        if tg_user_id and ext.HAS_PRODUCT_LAYER:
            try:
                _tl = ext.get_user(ext.DB_PATH, tg_user_id)
                if _tl:
                    _tg_trader_level = _tl.get('trader_level') or 'developing'
            except Exception:
                pass

        stress_dict = None
        if ext.HAS_STRESS and atoms:
            try:
                _words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', text)
                _key_terms = list({w.lower() for w in _words if len(w) > 2})[:10]
                _sr = ext.compute_stress(atoms, _key_terms, conn)
                stress_dict = {
                    'composite_stress': _sr.composite_stress,
                    'decay_pressure': _sr.decay_pressure,
                    'authority_conflict': _sr.authority_conflict,
                    'supersession_density': _sr.supersession_density,
                    'conflict_cluster': _sr.conflict_cluster,
                    'domain_entropy': _sr.domain_entropy,
                }
            except Exception:
                pass

        messages = ext.build_prompt(
            user_message=text, snippet=snippet, stress=stress_dict,
            atom_count=len(atoms), trader_level=_tg_trader_level,
        )
        answer = ext.llm_chat(messages, model=ext.DEFAULT_MODEL if ext.HAS_LLM else 'llama3.2')

        if answer:
            plain = re.sub(r'\*\*(.+?)\*\*', r'\1', answer)
            plain = re.sub(r'\*(.+?)\*', r'\1', plain)
            plain = plain[:4000]
            _bot_send(chat_id, plain)
        else:
            _bot_send(chat_id, "⚠️ Couldn't generate a response right now. Please try again.")
    except Exception as _exc:
        _logger.error('telegram_bot_webhook chat error: %s', _exc)
        _bot_send(chat_id, "⚠️ Something went wrong. Please try again in a moment.")

    return jsonify({'ok': True})


# ── /telegram/callback ────────────────────────────────────────────────────────

@bp.route('/telegram/callback', methods=['POST'])
@ext.limiter.exempt
def telegram_callback():
    """
    POST /telegram/callback
    Receives Telegram inline keyboard callback_query updates.
    Dispatches to tip_feedback_action or tip_position_update based on callback_data.
    """
    data = request.get_json(force=True, silent=True) or {}

    callback_query = data.get('callback_query', {})
    if not callback_query:
        return jsonify({'ok': True})

    callback_data = callback_query.get('data', '')
    from_user = callback_query.get('from', {})
    tg_user_id = str(from_user.get('id', ''))

    try:
        _c = sqlite3.connect(ext.DB_PATH, timeout=5)
        row = _c.execute(
            "SELECT user_id FROM user_preferences WHERE telegram_chat_id=?",
            (tg_user_id,),
        ).fetchone()
        _c.close()
        user_id = row[0] if row else None
    except Exception:
        user_id = None

    if not user_id:
        return jsonify({'ok': True})

    try:
        parts = callback_data.split(':')
        if len(parts) >= 3 and parts[0] == 'tip':
            tip_id = int(parts[1])
            action_map = {
                'taking': 'taking_it', 'more': 'tell_me_more', 'skip': 'not_for_me',
                'taking_it': 'taking_it', 'tell_me_more': 'tell_me_more', 'not_for_me': 'not_for_me',
            }
            action = action_map.get(parts[2], parts[2])
            with current_app.test_request_context(
                f'/tips/{tip_id}/feedback',
                method='POST',
                json={'user_id': user_id, 'action': action},
                content_type='application/json',
            ):
                g.user_id = user_id
                from routes.patterns import tip_feedback_action
                tip_feedback_action(tip_id)

        elif len(parts) >= 3 and parts[0] == 'pos':
            followup_id = int(parts[1])
            pos_action_map = {
                'closed': 'closed', 'hold_t2': 'hold_t2',
                'partial': 'partial', 'override': 'override', 'more': 'override',
            }
            action = pos_action_map.get(parts[2], parts[2])
            with current_app.test_request_context(
                f'/tips/{followup_id}/position-update',
                method='POST',
                json={'user_id': user_id, 'action': action},
                content_type='application/json',
            ):
                g.user_id = user_id
                from routes.patterns import tip_position_update
                tip_position_update(followup_id)

    except Exception as e:
        _logger.warning('telegram_callback error: %s', e)

    return jsonify({'ok': True})


# ── /telegram/webhook/register ────────────────────────────────────────────────

@bp.route('/telegram/webhook/register', methods=['POST'])
def telegram_webhook_register():
    """
    POST /telegram/webhook/register
    Calls Telegram's setWebhook API to point Telegram at /telegram/webhook.
    Body (optional): { "base_url": "https://api.trading-galaxy.uk" }
    """
    import requests as _rq

    token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not token:
        return jsonify({'error': 'TELEGRAM_BOT_TOKEN not configured'}), 503

    data = request.get_json(force=True, silent=True) or {}
    base_url = (data.get('base_url') or '').rstrip('/')
    if not base_url:
        scheme = 'https' if request.is_secure else 'http'
        base_url = f"{scheme}://{request.host}"

    webhook_url = f'{base_url}/telegram/webhook'
    secret = os.environ.get('TELEGRAM_WEBHOOK_SECRET', '')

    payload: dict = {'url': webhook_url, 'allowed_updates': ['message', 'callback_query']}
    if secret:
        payload['secret_token'] = secret

    try:
        resp = _rq.post(
            f'https://api.telegram.org/bot{token}/setWebhook',
            json=payload, timeout=10,
        )
        tg_data = resp.json()
        return jsonify({'ok': resp.status_code == 200, 'telegram_response': tg_data,
                        'webhook_url': webhook_url})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

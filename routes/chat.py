"""routes/chat.py — Chat endpoints: main chat, clear, history, atoms, metrics, models, opportunities."""

from __future__ import annotations

import re
import threading
from datetime import datetime, timezone
from typing import Optional

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('chat', __name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

_LIVE_PRICE_KEYWORDS = (
    'current', 'currently', 'right now', 'right-now', 'today',
    'trading at', 'trading now', 'priced at', 'price now', 'price today',
    'what is', "what's", 'whats', 'how much', 'worth', 'value',
    'rate', 'rates', 'level', 'levels', 'spot', 'live', 'latest',
    'at the moment', 'at this moment', 'as of now',
)


def _query_wants_live(message: str) -> bool:
    m = message.lower()
    return any(kw in m for kw in _LIVE_PRICE_KEYWORDS)


def _sid_for_user(user_id):
    """Resolve the conversation session ID for a user."""
    if ext.HAS_CONV_STORE:
        return ext.session_id_for_user(user_id)
    return 'default'


# ── Main chat endpoint ────────────────────────────────────────────────────────

@bp.route('/chat', methods=['POST'])
@ext.rate_limit('chat')
def chat_endpoint():
    """
    KB-grounded chat. Retrieves structured context, builds a KB-aware prompt,
    and calls the LLM to produce an answer.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'error': 'invalid JSON'}), 400

    message = data.get('message', '').strip()
    if not message:
        return jsonify({'error': 'message is required'}), 400

    session_id      = data.get('session_id', 'default')
    model           = data.get('model', ext.DEFAULT_MODEL if ext.HAS_LLM else 'llama3.2')
    goal            = data.get('goal')
    topic           = data.get('topic')
    turn_count      = int(data.get('turn_count', 1))
    limit           = int(data.get('limit', 30))
    screen_context  = data.get('screen_context', '')
    screen_entities = data.get('screen_entities') or []
    overlay_mode    = bool(data.get('overlay_mode', False))
    chat_user_id    = getattr(g, 'user_id', None) or data.get('user_id') or None

    # ── Trader level ──────────────────────────────────────────────────────
    _chat_trader_level = 'developing'
    if chat_user_id and ext.HAS_PRODUCT_LAYER:
        try:
            _tl_row = ext.get_user(ext.DB_PATH, chat_user_id)
            if _tl_row:
                _chat_trader_level = _tl_row.get('trader_level') or 'developing'
        except Exception:
            pass

    # ── Chat quota enforcement ────────────────────────────────────────────
    if chat_user_id and ext.HAS_TIERS and ext.HAS_PATTERN_LAYER:
        try:
            _chat_tier = ext.get_user_tier_for_request(chat_user_id)
            from core.tiers import get_tier as _get_tier, _next_tier as _next_tier_name
            _quota = _get_tier(_chat_tier).get('chat_queries_per_day')
            if _quota is not None and _quota == 0:
                return jsonify({
                    'error': 'upgrade_required', 'feature': 'chat_queries_per_day',
                    'current_tier': _chat_tier, 'upgrade_to': _next_tier_name(_chat_tier),
                    'queries_used': 0, 'queries_limit': 0,
                    'message': 'Chat is not available on the free plan. Subscribe to unlock.',
                }), 403
            if _quota is not None and _quota > 0:
                _used = ext.get_today_chat_count(ext.DB_PATH, chat_user_id)
                if _used >= _quota:
                    return jsonify({
                        'error': 'upgrade_required', 'feature': 'chat_queries_per_day',
                        'current_tier': _chat_tier, 'upgrade_to': _next_tier_name(_chat_tier),
                        'queries_used': _used, 'queries_limit': _quota,
                        'message': f'Daily chat limit of {_quota} reached. Upgrade to Pro for unlimited queries.',
                    }), 403
        except Exception:
            pass

    # ── Portfolio-intent detection ────────────────────────────────────────
    _PORTFOLIO_INTENT_KWS = (
        'my portfolio', 'my holdings', 'my positions', 'my stocks', 'my shares',
        'my book', 'my p&l', 'my pnl', 'my exposure', 'my allocation',
        'discuss my', 'analyse my', 'analyze my', 'review my',
        'affect my', 'impact my', 'affect portfolio', 'impact portfolio',
        'portfolio', 'holdings', 'positions',
    )
    _msg_lower_port = message.lower()
    _wants_portfolio = any(kw in _msg_lower_port for kw in _PORTFOLIO_INTENT_KWS)

    if chat_user_id and _wants_portfolio:
        limit = max(limit, 80)

    conn = ext.kg.thread_local_conn()

    # ── Prior session context ─────────────────────────────────────────────
    prior_context = None
    if ext.HAS_WORKING_STATE:
        try:
            ws = ext.get_working_state_store(ext.DB_PATH)
            if turn_count == 0:
                prior_context = ws.format_prior_context(session_id) or None
            ws.maybe_persist(session_id, turn_count, goal=goal, topic=topic,
                             force=(turn_count == 1))
        except Exception:
            pass

    # ── Adaptation nudges ─────────────────────────────────────────────────
    nudges = None
    if ext.HAS_ADAPTATION and ext.HAS_STRESS:
        try:
            from knowledge.epistemic_adaptation import ensure_adaptation_tables
            ensure_adaptation_tables(conn)
            engine = ext.get_adaptation_engine(session_id, db_path=ext.DB_PATH)
            engine._session_id = session_id
            sess = ext.sessions.get_streak(session_id)

            class _StateStub:
                pass
            state_stub = _StateStub()
            state_stub.epistemic_stress_streak = sess['streak']
            state_stub._session_id = session_id

            class _StressStub:
                composite_stress     = sess['last_stress']
                decay_pressure       = 0.0
                authority_conflict   = 0.0
                supersession_density = 0.0
                conflict_cluster     = 0.0
                domain_entropy       = 1.0
            nudges = engine.compute(state_stub, _StressStub(), topic=topic, key_terms=[])
        except Exception:
            nudges = None

    # ── Ticker carry-forward ──────────────────────────────────────────────
    try:
        from retrieval import _extract_tickers as _et
        _cur_tickers = _et(message)
    except Exception:
        _cur_tickers = []

    if not ext.sessions.get_portfolio_tickers(session_id) and chat_user_id and ext.HAS_PRODUCT_LAYER:
        try:
            _ph = ext.get_portfolio(ext.DB_PATH, chat_user_id)
            _pticks = [h['ticker'] for h in (_ph or []) if h.get('ticker')]
            if _pticks:
                ext.sessions.set_portfolio_tickers(session_id, _pticks)
        except Exception:
            pass

    _retrieve_message = message
    _aug_tickers: list = []
    if not _cur_tickers and ext.sessions.has_tickers(session_id):
        _aug_tickers = list(ext.sessions.get_tickers(session_id) or [])
    if _wants_portfolio:
        _port_ticks = ext.sessions.get_portfolio_tickers(session_id) or []
        for _pt in _port_ticks:
            if _pt not in _aug_tickers and _pt not in _cur_tickers:
                _aug_tickers.append(_pt)
    if _aug_tickers:
        _retrieve_message = message + ' ' + ' '.join(_aug_tickers)

    # ── Retrieve KB context ───────────────────────────────────────────────
    snippet, atoms = ext.retrieve(_retrieve_message, conn, limit=limit, nudges=nudges)

    if _cur_tickers:
        ext.sessions.set_tickers(session_id, _cur_tickers)
    elif not ext.sessions.has_tickers(session_id) and atoms:
        _seen = list({a['subject'].upper() for a in atoms if 'subject' in a})[:4]
        if _seen:
            ext.sessions.set_tickers(session_id, _seen)

    # ── Working memory: on-demand fetch ───────────────────────────────────
    live_context = ''
    live_fetched = []
    wm_session_id = f'wm_{session_id}'
    if ext.HAS_WORKING_MEMORY and ext.working_memory is not None:
        try:
            from retrieval import _extract_tickers
            from knowledge.working_memory import _YF_TICKER_MAP, MAX_ON_DEMAND_TICKERS
            tickers_in_query = _extract_tickers(message)
            _JUNK_WORDS = {
                'BEARISH','BULLISH','ORDER','BREAKER','CONFIRMED','UNCONFIRMED','PARTIAL',
                'SIGNAL','QUALITY','ZONE','PATTERN','LONG','SHORT','NEUTRAL','AVOID',
                'HIGH','LOW','MID','RANGE','NEAR','MEDIUM','STRONG','WEAK','MACRO',
                'CONVICTION','TIER','PRICE','TARGET','REGIME','DIRECTION','RETURN',
                'YEAR','MONTH','WEEK','DAY','LIVE','DATA','CONTEXT','DETECTED',
                'TIMEFRAME','SCORE','PERIOD','OPEN','CLOSE','VOLUME','MARKET',
                'SECTOR','FACTOR','RISK','CATALYST','THESIS','BASIS','COST',
                'HOLDINGS','HOLDING','PORTFOLIO','ANALYSIS','ANALYSIS','PLEASE',
                'DISCUSS','PROVIDE','CONTEXT','INSIGHT','GIVEN','BASED','NOTE',
            }
            tickers_in_query = [
                t for t in tickers_in_query
                if t not in _JUNK_WORDS
                and (
                    len(t) <= 5
                    or any(c in t for c in ('.', '-', '=', '^', '/'))
                    or t in _YF_TICKER_MAP
                    or any(c.isdigit() for c in t)
                )
            ]
            if not tickers_in_query:
                if chat_user_id and ext.HAS_PRODUCT_LAYER:
                    try:
                        _ph = ext.get_portfolio(ext.DB_PATH, chat_user_id)
                        tickers_in_query = [h['ticker'] for h in (_ph or []) if h.get('ticker')]
                    except Exception:
                        pass
                if not tickers_in_query:
                    tickers_in_query = list(ext.sessions.get_tickers(session_id) or [])
                if not tickers_in_query and atoms:
                    tickers_in_query = list({a['subject'].upper() for a in atoms if 'subject' in a})
            from knowledge.working_memory import kb_has_atoms
            missing_from_kb = [
                t for t in tickers_in_query[:MAX_ON_DEMAND_TICKERS]
                if not kb_has_atoms(t, ext.DB_PATH)
            ]
            _yf_values = set(_YF_TICKER_MAP.values())
            def _is_live_asset(t: str) -> bool:
                tu = t.upper()
                if tu in _YF_TICKER_MAP: return True
                if t in _yf_values: return True
                if (t.endswith('-USD') or t.endswith('=X') or t.endswith('=F')
                        or t.startswith('^') or t.endswith('.NYB')):
                    return True
                return False
            live_always = [
                t for t in tickers_in_query[:MAX_ON_DEMAND_TICKERS]
                if _is_live_asset(t) and t not in missing_from_kb
            ]
            to_fetch = missing_from_kb + live_always
            if missing_from_kb or live_always:
                ext.working_memory.open_session(wm_session_id)
                for ticker in to_fetch[:MAX_ON_DEMAND_TICKERS]:
                    ext.working_memory.fetch_on_demand(ticker, wm_session_id, ext.DB_PATH)
                live_context = ext.working_memory.get_session_snippet(wm_session_id)
                live_fetched = ext.working_memory.get_fetched_tickers(wm_session_id)
        except Exception:
            pass

    # ── Async discovery ───────────────────────────────────────────────────
    if ext.discovery_pipeline is not None:
        try:
            from retrieval import _extract_tickers as _et_disc
            _disc_tickers = _et_disc(message)
            for _dt in _disc_tickers[:3]:
                _stale = ext.discovery_pipeline.assess_staleness(_dt)
                if _stale:
                    threading.Thread(
                        target=ext.discovery_pipeline.discover,
                        args=(_dt, 'user_query', chat_user_id),
                        daemon=True,
                    ).start()
        except Exception:
            pass

    # ── Epistemic stress ──────────────────────────────────────────────────
    stress_report = None
    stress_dict   = None
    if ext.HAS_STRESS and atoms:
        try:
            words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)
            key_terms = list({w.lower() for w in words if len(w) > 2})[:10]
            stress_report = ext.compute_stress(atoms, key_terms, conn)
            stress_dict = {
                'composite_stress':     stress_report.composite_stress,
                'decay_pressure':       stress_report.decay_pressure,
                'authority_conflict':   stress_report.authority_conflict,
                'supersession_density': stress_report.supersession_density,
                'conflict_cluster':     stress_report.conflict_cluster,
                'domain_entropy':       stress_report.domain_entropy,
            }
        except Exception:
            pass

    # ── Update session streak ─────────────────────────────────────────────
    if ext.HAS_ADAPTATION and stress_report:
        try:
            from knowledge.epistemic_adaptation import _STRESS_STREAK_THRESHOLD
            sess = ext.sessions.get_streak(session_id)
            if stress_report.composite_stress >= _STRESS_STREAK_THRESHOLD:
                sess['streak'] = sess.get('streak', 0) + 1
            else:
                sess['streak'] = max(0, sess.get('streak', 0) - 1)
            sess['last_stress'] = stress_report.composite_stress
            ext.sessions.set_streak(session_id, sess)
        except Exception:
            pass

    # ── KB insufficiency diagnosis ────────────────────────────────────────
    kb_diagnosis = None
    if ext.HAS_CLASSIFIER and stress_report and atoms:
        try:
            _tickers = [t for t in re.findall(r'\b[A-Z]{2,5}\b', message)
                        if t not in {'THE','IS','AT','ON','AN','AND','OR','FOR','IN','OF',
                                     'TO','THAT','THIS','WITH','FROM','BY','ARE','WAS','BE',
                                     'HAS','HAVE','HAD','ITS','DO','DID','WHAT','HOW','WHY',
                                     'WHEN','WHERE','WHO','CAN','WILL','NOT','BUT','ALL'}]
            _terms = [w.lower() for w in re.findall(r'\b[a-zA-Z][a-zA-Z0-9]{2,}\b', message)]
            composite = getattr(stress_report, 'composite_stress', 0.0)
            atom_count = len(atoms)
            if composite > 0.35 or atom_count < 8:
                topic_hint = (topic or
                              (_tickers[0] if _tickers else None) or
                              (_terms[0] if _terms else None) or
                              message[:40])
                diag = ext.classify_insufficiency(topic_hint, stress_report, conn)
                kb_diagnosis = {
                    'topic':         diag.topic,
                    'types':         [t.value for t in diag.types],
                    'primary_type':  diag.primary_type().value,
                    'confidence':    diag.confidence,
                    'matched_rules': diag.matched_rules,
                    'signals':       diag.signals,
                }
        except Exception:
            pass

    # ── Overlay cards ─────────────────────────────────────────────────────
    overlay_cards = None
    if overlay_mode and ext.HAS_OVERLAY:
        try:
            overlay_tickers = ext.extract_overlay_tickers(screen_context, conn, screen_entities)
            overlay_cards = ext.build_overlay_cards(overlay_tickers, conn, stress_dict)
        except Exception:
            overlay_cards = []

    # ── Build response skeleton ───────────────────────────────────────────
    response: dict = {
        'answer':     None,
        'model':      model,
        'atoms_used': len(atoms),
        'snippet':    snippet,
    }
    if overlay_cards is not None:
        response['overlay_cards'] = overlay_cards
    if stress_dict:
        response['stress'] = stress_dict
    if kb_diagnosis:
        response['kb_diagnosis'] = kb_diagnosis
    if nudges is not None and nudges.is_active():
        response['adaptation'] = {
            'streak':                    nudges.streak,
            'consolidation_mode':        nudges.consolidation_mode,
            'retrieval_scope_broadened': nudges.retrieval_scope_broadened,
            'prefer_high_authority':     nudges.prefer_high_authority,
            'prefer_recent':             nudges.prefer_recent,
        }

    # ── Call LLM ──────────────────────────────────────────────────────────
    if not ext.HAS_LLM:
        response['error'] = 'llm package not available'
        return jsonify(response), 503

    if not ext.is_available():
        response['error'] = 'Ollama not reachable — KB context returned without LLM answer'
        return jsonify(response), 503

    # ── Portfolio context ─────────────────────────────────────────────────
    portfolio_context = None
    if chat_user_id and ext.HAS_PRODUCT_LAYER and _wants_portfolio:
        try:
            _holdings = ext.get_portfolio(ext.DB_PATH, chat_user_id)
            _model = ext.get_user_model(ext.DB_PATH, chat_user_id)
            if _holdings:
                _h_parts = [f"{h['ticker']} ×{int(h['quantity'])}" for h in _holdings[:20]]
                _pos_values = [
                    h['quantity'] * h['avg_cost']
                    for h in _holdings if h.get('quantity') and h.get('avg_cost')
                ]
                _total_cost = sum(_pos_values)
                _largest_pct = (
                    round(max(_pos_values) / _total_cost * 100)
                    if _total_cost > 0 and _pos_values else None
                )
                _lines = ["=== USER PORTFOLIO ===",
                          f"Holdings: {', '.join(_h_parts)}"]
                if _total_cost > 0:
                    _lines.append(f"Total invested (cost basis): £{_total_cost:,.0f}")
                if _largest_pct is not None:
                    _largest_ticker = max(
                        (h for h in _holdings if h.get('quantity') and h.get('avg_cost')),
                        key=lambda h: h['quantity'] * h['avg_cost']
                    )['ticker']
                    _lines.append(f"Largest single position: {_largest_pct}% ({_largest_ticker})")
                if _model:
                    _risk    = _model.get('risk_tolerance', '')
                    _style   = _model.get('holding_style', '')
                    _sectors = ', '.join(_model.get('sector_affinity') or [])
                    _profile = ' · '.join(p for p in [_risk, _style, _sectors] if p)
                    if _profile:
                        _lines.append(f"Risk profile: {_profile}")
                _holding_tickers = [h['ticker'] for h in _holdings]
                _ticker_atoms: dict = {}
                for _ht in _holding_tickers:
                    try:
                        _ht_rows = conn.execute(
                            """SELECT predicate, object FROM facts
                               WHERE subject=? AND predicate IN
                               ('last_price','currency','price_regime','signal_direction',
                                'signal_quality','return_1m','return_3m','return_1y',
                                'upside_pct','conviction_tier','macro_confirmation')
                               ORDER BY predicate""",
                            (_ht.lower(),)
                        ).fetchall()
                        if _ht_rows:
                            _ticker_atoms[_ht] = _ht_rows
                    except Exception:
                        pass
                _lines.append("\nPer-holding KB signals:")
                for _ht in _holding_tickers:
                    _rows = _ticker_atoms.get(_ht, [])
                    if not _rows:
                        _lines.append(f"  {_ht}: No KB signals available — discuss based on general knowledge of this ticker.")
                        continue
                    _d = {p: v for p, v in _rows}
                    _price    = _d.get('last_price', '?')
                    _regime   = _d.get('price_regime', '?').replace('_', ' ')
                    _dir      = _d.get('signal_direction', '?')
                    _qual     = _d.get('signal_quality', '?')
                    _macro    = _d.get('macro_confirmation', '?')
                    _conv     = _d.get('conviction_tier', '?')
                    _upside   = _d.get('upside_pct', '?')
                    _ret1m    = _d.get('return_1m', '')
                    _ret1y    = _d.get('return_1y', '')
                    _target   = _d.get('price_target', '')
                    _implied = ''
                    try:
                        if _target and _price and _price != '?' and _target != '?':
                            _move = float(_target) - float(_price)
                            _move_dir = 'up to' if _move >= 0 else 'down to'
                            _implied = (f" The KB price target is {_target}, implying a move "
                                        f"{_move_dir} {_target} ({_upside}% from current price).")
                    except Exception:
                        pass
                    _sent = (
                        f"  {_ht}: Current price {_price} ({_regime} regime). "
                        f"KB signal direction is {_dir}.{_implied} "
                        f"Signal quality: {_qual}. Macro confirmation: {_macro}. "
                        f"Conviction tier: {_conv}."
                    )
                    if _ret1m:
                        _sent += f" 1-month return: {_ret1m}%."
                    if _ret1y:
                        _sent += f" 1-year return: {_ret1y}%."
                    _lines.append(_sent)

                portfolio_context = '\n'.join(_lines)

                # Geo-risk context injection
                try:
                    import sqlite3 as _sq
                    _gc = _sq.connect(ext.DB_PATH, timeout=5)
                    _geo_lines = []
                    for _ht in _holding_tickers:
                        _geo_row = _gc.execute(
                            """SELECT object FROM facts
                               WHERE subject=? AND predicate='geopolitical_risk_exposure'
                               ORDER BY confidence DESC LIMIT 1""",
                            (_ht.lower(),),
                        ).fetchone()
                        if _geo_row and _geo_row[0] in ('elevated', 'moderate'):
                            _geo_lines.append(f"  {_ht}: geopolitical_risk_exposure={_geo_row[0]}")
                    _shock_row = _gc.execute(
                        """SELECT object FROM facts
                           WHERE subject='macro_regime' AND predicate='energy_shock_risk'
                           ORDER BY confidence DESC LIMIT 1"""
                    ).fetchone()
                    _gc.close()
                    if _geo_lines or (_shock_row and _shock_row[0] in ('elevated', 'moderate')):
                        portfolio_context += '\n=== GEOPOLITICAL RISK FLAGS ==='
                        if _shock_row and _shock_row[0] in ('elevated', 'moderate'):
                            portfolio_context += f'\n  Energy shock risk: {_shock_row[0]} (WTI/Middle East tension)'
                        if _geo_lines:
                            portfolio_context += '\n' + '\n'.join(_geo_lines)
                except Exception:
                    pass
        except Exception:
            portfolio_context = None

    # ── Pass 1: LLM-initiated data request ────────────────────────────────
    llm_requested_tickers: list = []
    web_searched: str | None = None
    if (ext.HAS_WORKING_MEMORY and ext.working_memory is not None
            and not live_fetched
            and (len(atoms) < 8 or _query_wants_live(message))):
        try:
            from knowledge.working_memory import (
                DATA_REQUEST_SYSTEM_PROMPT, parse_llm_response
            )
            _p1_ctx = snippet or '(No KB context)'
            if portfolio_context:
                _p1_ctx = portfolio_context + '\n\n' + _p1_ctx
            _p1_messages = [
                {'role': 'system', 'content': DATA_REQUEST_SYSTEM_PROMPT},
                {'role': 'user',   'content': f"{_p1_ctx}\n\nQuestion: {message}"},
            ]
            _p1_raw = ext.llm_chat(_p1_messages, model=model)
            if _p1_raw:
                _mode, _payload = parse_llm_response(_p1_raw)
                if _mode == 'data_request' and _payload:
                    llm_requested_tickers = _payload
                    ext.working_memory.open_session(wm_session_id)
                    for _t in llm_requested_tickers:
                        ext.working_memory.fetch_on_demand(_t, wm_session_id, ext.DB_PATH)
                    live_context = ext.working_memory.get_session_snippet(wm_session_id)
                    live_fetched = ext.working_memory.get_fetched_tickers(wm_session_id)
                elif _mode == 'search_request' and _payload:
                    _search_query = _payload[0]
                    ext.working_memory.open_session(wm_session_id)
                    _search_atoms = ext.working_memory.web_search_on_demand(
                        _search_query, wm_session_id
                    )
                    if _search_atoms:
                        web_searched = _search_query
                        live_context = ext.working_memory.get_session_snippet(wm_session_id)
                        live_fetched = ext.working_memory.get_fetched_tickers(wm_session_id)
        except Exception:
            pass

    # ── Resolved aliases ──────────────────────────────────────────────────
    _resolved_aliases: dict = {}
    if snippet and 'is an alias' in snippet:
        for _m in re.finditer(
            r"INSTRUCTION: '(\S+)' is an alias\. The KB data below \(subject='(\S+)'\)",
            snippet
        ):
            _resolved_aliases[_m.group(1)] = _m.group(2).upper()

    # ── Prior conversation turns ──────────────────────────────────────────
    _has_prior_turns = False
    if ext.conv_store is not None:
        try:
            _conv_session_id_check = _sid_for_user(chat_user_id)
            _check_hist = ext.conv_store.get_recent_messages_for_context(_conv_session_id_check, n_turns=2)
            _has_prior_turns = len(_check_hist) > 1
        except Exception:
            pass

    # ── On-demand tip intent detection ────────────────────────────────────
    _TIP_INTENT_PHRASES = (
        'give me a tip', 'give me tip', 'daily tip', 'today\'s tip',
        'what should i trade', 'what should i buy', 'what should i sell',
        'any setups worth', 'best opportunity right now', 'what\'s looking good',
        "what's looking good", 'best setup today', 'top trade today',
        'trade of the day', 'tip of the day', 'recommend a trade',
        'show me a trade', 'suggest a trade',
    )
    _msg_lower_tip = message.lower()
    _is_tip_request = any(ph in _msg_lower_tip for ph in _TIP_INTENT_PHRASES)

    if _is_tip_request and ext.HAS_PATTERN_LAYER and chat_user_id:
        try:
            from notifications.tip_scheduler import _pick_best_pattern, _get_local_now
            from notifications.tip_formatter import format_tip, tip_to_dict, TIER_LIMITS
            from analytics.pattern_detector import PatternSignal
            from analytics.position_calculator import calculate_position
            import sqlite3 as _sq2

            _prefs_row = None
            _c_tip = _sq2.connect(ext.DB_PATH, timeout=5)
            try:
                _prefs_row = _c_tip.execute(
                    """SELECT tier, tip_timeframes, tip_pattern_types,
                              account_size, max_risk_per_trade_pct, account_currency
                       FROM user_preferences WHERE user_id=?""", (chat_user_id,)
                ).fetchone()
            finally:
                _c_tip.close()

            _tier = 'basic'
            _tip_prefs: dict = {}
            if _prefs_row:
                import json as _json
                _tier = _prefs_row[0] or 'basic'
                def _j(v):
                    try: return _json.loads(v) if v else None
                    except Exception: return None
                _limits = TIER_LIMITS.get(_tier, TIER_LIMITS['basic'])
                _tip_prefs = {
                    'account_size': _prefs_row[3] or 10000,
                    'max_risk_per_trade_pct': _prefs_row[4] or 1.0,
                    'account_currency': _prefs_row[5] or 'GBP',
                    'tier': _tier,
                    'tip_timeframes': _j(_prefs_row[1]) or _limits['timeframes'],
                    'tip_pattern_types': _j(_prefs_row[2]),
                }

            _pat_row = _pick_best_pattern(
                ext.DB_PATH, chat_user_id, _tier,
                _tip_prefs.get('tip_timeframes', ['1h']),
                _tip_prefs.get('tip_pattern_types'),
            )
            if _pat_row:
                _sig = PatternSignal(
                    pattern_type=_pat_row['pattern_type'], ticker=_pat_row['ticker'],
                    direction=_pat_row['direction'], zone_high=_pat_row['zone_high'],
                    zone_low=_pat_row['zone_low'], zone_size_pct=_pat_row.get('zone_size_pct', 0.0),
                    timeframe=_pat_row['timeframe'], formed_at=_pat_row.get('formed_at', ''),
                    quality_score=_pat_row.get('quality_score') or 0.0, status=_pat_row['status'],
                    kb_conviction=_pat_row.get('kb_conviction', ''),
                    kb_regime=_pat_row.get('kb_regime', ''),
                    kb_signal_dir=_pat_row.get('kb_signal_dir', ''),
                )
                _tip_pos = calculate_position(_sig, _tip_prefs) if _tip_prefs else None
                _tip_dict = tip_to_dict(_sig, _tip_pos, tier=_tier)
                response['tip_card'] = {
                    **_tip_dict, 'tip_id': None, 'pattern_id': _pat_row.get('id'),
                    'feedback_actions': ['taking_it', 'tell_me_more', 'not_for_me'],
                }
        except Exception as _tip_err:
            import logging as _logging
            _logging.getLogger(__name__).warning('on-demand tip failed: %s', _tip_err)

    # ── Opportunity generation scan ───────────────────────────────────────
    _opportunity_scan_context: Optional[str] = None
    try:
        from analytics.opportunity_engine import (
            classify_intent as _classify_intent,
            run_opportunity_scan as _run_opportunity_scan,
            format_scan_as_context as _format_scan_as_context,
        )
        _gen_modes = _classify_intent(message)
        _GEN_SKIP_KEYWORDS = (
            'what is', 'what\'s', 'tell me about', 'explain', 'why is', 'how is',
            'price of', 'signal for', 'analyse my portfolio', 'analyze my portfolio',
            'portfolio', 'my holdings',
        )
        _is_gen_query = not any(kw in message.lower() for kw in _GEN_SKIP_KEYWORDS)
        _GEN_TRIGGER_WORDS = (
            'strategy', 'strateg', 'trade', 'trading', 'opportunity', 'opportunit',
            'setup', 'setups', 'find me', 'show me', 'make me', 'give me',
            'where are', 'what sectors', 'momentum', 'squeeze', 'gap', 'intraday',
            'daytime', 'ideas', 'idea', 'rotation', 'reversal', 'breakout',
            'best trade', 'top trade', 'mean reversion', 'play',
        )
        _has_gen_trigger = any(kw in message.lower() for kw in _GEN_TRIGGER_WORDS)
        if _is_gen_query and _has_gen_trigger and _gen_modes:
            _scan = _run_opportunity_scan(
                query=message, db_path=ext.DB_PATH,
                modes=_gen_modes, limit_per_mode=6,
            )
            _opportunity_scan_context = _format_scan_as_context(_scan)
            response['opportunity_scan'] = {
                'mode': _scan.mode, 'results': len(_scan.results), 'regime': _scan.market_regime,
            }
    except Exception as _opp_err:
        import logging as _logging
        _logging.getLogger(__name__).warning('opportunity scan failed: %s', _opp_err)

    # ── Build full prompt ─────────────────────────────────────────────────
    messages = ext.build_prompt(
        user_message=message, snippet=snippet, stress=stress_dict,
        kb_diagnosis=kb_diagnosis, prior_context=prior_context,
        portfolio_context=portfolio_context, atom_count=len(atoms),
        live_context=live_context or None,
        resolved_aliases=_resolved_aliases or None,
        web_searched=web_searched or None,
        has_history=_has_prior_turns,
        opportunity_scan_context=_opportunity_scan_context,
        trader_level=_chat_trader_level,
    )

    # ── Persist user turn + inject DB-backed conversation history ─────────
    _conv_session_id = _sid_for_user(chat_user_id)
    _user_msg_record = None
    if ext.conv_store is not None:
        try:
            _user_msg_record = ext.conv_store.add_message(
                _conv_session_id, 'user', message, user_id=chat_user_id
            )
            _db_history = ext.conv_store.get_recent_messages_for_context(
                _conv_session_id, n_turns=8
            )
            _just_id = _user_msg_record.get('id') if _user_msg_record else None
            _db_hist_msgs = [
                {'role': m['role'], 'content': m['content']}
                for m in _db_history if m.get('id') != _just_id
            ]
            _last_user_msg = next(
                (m['content'] for m in reversed(_db_hist_msgs) if m['role'] == 'user'), None
            )
            _is_retry = (
                _last_user_msg is not None
                and message.strip().lower() == _last_user_msg.strip().lower()
            )
            if _db_hist_msgs and len(messages) >= 2 and not _is_retry:
                messages = [messages[0]] + _db_hist_msgs + [messages[-1]]
        except Exception:
            pass

    answer = ext.llm_chat(messages, model=model)
    if answer is None:
        if ext.HAS_WORKING_MEMORY and ext.working_memory:
            ext.working_memory.close_without_commit(wm_session_id)
        response['error'] = 'Ollama returned no response'
        return jsonify(response), 503

    response['answer'] = answer
    if llm_requested_tickers:
        response['llm_requested_tickers'] = llm_requested_tickers
    if web_searched:
        response['web_searched'] = web_searched

    # ── Persist assistant turn + async atom extraction → KB graduation ────
    if ext.conv_store is not None:
        try:
            _stress_val = stress_dict.get('composite_stress') if stress_dict else None
            _asst_meta = {
                'tickers': ext.sessions.get_tickers(session_id) or [],
                'stress':  _stress_val,
                'atoms':   len(atoms),
            }
            _asst_msg_record = ext.conv_store.add_message(
                _conv_session_id, 'assistant', answer,
                metadata=_asst_meta, user_id=chat_user_id
            )
        except Exception:
            _asst_msg_record = None

        _atom_msg_id  = _asst_msg_record.get('id') if _asst_msg_record else None
        _atom_user_q  = message
        _atom_answer  = answer
        _atom_cs_id   = _conv_session_id
        _atom_sess_id = session_id
        _atom_turn    = turn_count
        _atom_goal    = goal

        def _extract_and_graduate():
            try:
                if _atom_msg_id is None:
                    return
                from llm.ollama_client import chat as _oc
                _atom_prompt = [
                    {'role': 'system', 'content': (
                        'You are a knowledge extractor for a trading intelligence system. '
                        'Extract exactly 3-6 knowledge atoms from the conversation turn. '
                        'Prefer these predicates where applicable: '
                        'signal_direction, conviction_tier, price_target, risk_factor, '
                        'catalyst, thesis_premise, invalidation_condition, sector_bias, '
                        'user_interest, pattern_preference, regime_view. '
                        'Each atom must be a JSON object with keys: '
                        'subject (ticker or concept), predicate (from vocabulary or freeform), '
                        'object (value), atom_type (fact|intent|topic|signal), source (user|assistant). '
                        'Respond with ONLY a JSON array. No preamble, no explanation.'
                    )},
                    {'role': 'user', 'content': (
                        f'User said: "{_atom_user_q[:300]}"\n'
                        f'Assistant replied: "{_atom_answer[:400]}"'
                    )},
                ]
                _raw = _oc(_atom_prompt, model='llama3.2')
                if not _raw:
                    return
                import json as _json
                _s = _raw.find('[')
                _e = _raw.rfind(']') + 1
                if _s == -1 or _e <= 0:
                    return
                _atoms = _json.loads(_raw[_s:_e])
                if not isinstance(_atoms, list):
                    return
                ext.conv_store.add_turn_atoms(_atom_msg_id, _atom_cs_id, _atoms)

                import math as _math
                _salient = ext.conv_store.get_salient_atoms(_atom_cs_id, limit=30, min_salience=0.1)
                _graduated = []
                _PRICE_PREDICATES = {
                    'last_price', 'price', 'price_target', 'price_range',
                    'invalidation_price', 'nav_price', 'close_price',
                    'open_price', 'high_price', 'low_price',
                    'high_52w', 'low_52w', 'pe_ratio', 'eps', 'revenue',
                    'market_cap', 'market_cap_tier', 'return_1m', 'return_1y',
                    'return_1w', 'return_3m', 'return_6m', 'drawdown_from_52w_high',
                    'upside_pct', 'volatility_30d', 'volatility_90d',
                }
                for _at in _salient:
                    if _at.get('graduated'):
                        continue
                    _is_user_intent = (
                        _at.get('source') == 'user' and _at.get('atom_type') == 'intent'
                    )
                    _threshold = 0.25 if _is_user_intent else 0.40
                    if _at.get('predicate') in _PRICE_PREDICATES:
                        continue
                    if _at['effective_salience'] >= _threshold:
                        try:
                            ext.kg.add_fact(
                                _at['subject'], _at['predicate'], _at['object'],
                                source='conversation',
                                confidence=round(_at['effective_salience'], 3),
                            )
                            ext.conv_store.mark_atom_graduated(_at['id'])
                            _graduated.append(_at)
                        except Exception:
                            pass

                if ext.HAS_WORKING_STATE and _graduated:
                    try:
                        _ws2 = ext.get_working_state_store(ext.DB_PATH)
                        _top_subj = list(dict.fromkeys(
                            a['subject'] for a in _graduated
                        ))[:3]
                        _ws2.maybe_persist(
                            _atom_sess_id, _atom_turn,
                            goal=_atom_goal, topic=', '.join(_top_subj),
                            last_intent=_atom_user_q[:120], force=True,
                        )
                    except Exception:
                        pass
            except Exception:
                pass

        threading.Thread(target=_extract_and_graduate, daemon=True).start()

    # ── Commit working memory atoms back to KB ────────────────────────────
    if ext.HAS_WORKING_MEMORY and ext.working_memory and live_fetched:
        try:
            commit_result = ext.working_memory.commit_session(wm_session_id, ext.kg)
            response['kb_enriched']     = commit_result.committed > 0
            response['live_fetched']    = live_fetched
            response['atoms_committed'] = commit_result.committed
        except Exception:
            ext.working_memory.close_without_commit(wm_session_id)

    return jsonify(response)


# ── Secondary chat endpoints ──────────────────────────────────────────────────

@bp.route('/chat/clear', methods=['POST'])
def chat_clear():
    """POST /chat/clear — clear conversation history for a session."""
    data = request.get_json(force=True, silent=True) or {}
    user_id = data.get('user_id') or getattr(g, 'user_id', None)
    purge   = bool(data.get('purge', False))
    conv_sid = _sid_for_user(user_id)
    _clear_sid = data.get('session_id', 'default')
    ext.sessions.pop_tickers(_clear_sid)
    ext.sessions.pop_portfolio_tickers(_clear_sid)
    deleted = 0
    if purge and ext.conv_store is not None:
        try:
            deleted = ext.conv_store.delete_session_messages(conv_sid)
        except Exception:
            pass
    return jsonify({
        'session_id': conv_sid, 'turns_deleted': deleted,
        'purge': purge, 'cleared': True,
    })


@bp.route('/chat/history', methods=['GET'])
def chat_history():
    """GET /chat/history — read-only conversation timeline."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    limit    = min(int(request.args.get('limit', 50)), 200)
    offset   = int(request.args.get('offset', 0))
    search   = request.args.get('search', '').strip()
    try:
        entries = ext.conv_store.get_timeline(conv_sid, limit=limit, offset=offset, search=search)
        total   = ext.conv_store.get_total_turn_count(conv_sid)
        return jsonify({'session_id': conv_sid, 'entries': entries,
                        'total': total, 'offset': offset})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/history/<int:message_id>', methods=['GET'])
def chat_history_turn(message_id):
    """GET /chat/history/<message_id> — full text of a user+assistant turn pair."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    try:
        pair = ext.conv_store.get_message_pair(message_id)
        if not pair:
            return jsonify({'error': 'Message not found'}), 404
        return jsonify(pair)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/atoms', methods=['GET'])
def chat_atoms():
    """GET /chat/atoms — conversation atoms with salience and graduation status."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    limit    = min(int(request.args.get('limit', 50)), 200)
    try:
        atoms     = ext.conv_store.get_atoms_with_status(conv_sid, limit=limit)
        total     = len(atoms)
        graduated = sum(1 for a in atoms if a.get('graduated'))
        return jsonify({
            'session_id': conv_sid, 'total_atoms': total,
            'graduated_to_kb': graduated, 'pending': total - graduated,
            'atoms': atoms,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/metrics', methods=['GET'])
def chat_metrics():
    """GET /chat/metrics — longitudinal cognitive metrics."""
    if ext.conv_store is None:
        return jsonify({'error': 'ConversationStore not available'}), 503
    user_id  = getattr(g, 'user_id', None) or request.args.get('user_id')
    conv_sid = _sid_for_user(user_id)
    try:
        return jsonify(ext.conv_store.get_cognitive_metrics(conv_sid))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/chat/models', methods=['GET'])
def chat_models():
    """GET /chat/models — list locally available Ollama models."""
    if not ext.HAS_LLM:
        return jsonify({'models': [], 'available': False,
                        'error': 'llm package not available'}), 503
    models = ext.list_models()
    return jsonify({
        'models': models, 'default': ext.DEFAULT_MODEL,
        'available': bool(models),
    })


# ── Opportunities (POST — authenticated, tier-gated) ─────────────────────────

@bp.route('/opportunities', methods=['POST'])
@ext.require_auth
@ext.require_feature('opportunity_scan')
def opportunities_endpoint():
    """POST /opportunities — on-demand opportunity scan."""
    try:
        from analytics.opportunity_engine import run_opportunity_scan, format_scan_as_context
    except ImportError:
        return jsonify({'error': 'opportunity engine not available'}), 503

    data  = request.get_json(force=True, silent=True) or {}
    query = data.get('query', '')
    modes = data.get('modes') or None
    limit = int(data.get('limit', 6))

    if not query and not modes:
        query = 'broad screen'

    try:
        scan = run_opportunity_scan(
            query=query, db_path=ext.DB_PATH, modes=modes, limit_per_mode=limit,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'mode':          scan.mode,
        'generated_at':  scan.generated_at,
        'market_regime': scan.market_regime,
        'market_context': scan.market_context,
        'results': [
            {
                'ticker': r.ticker, 'mode': r.mode,
                'score': round(r.score, 3), 'conviction_tier': r.conviction_tier,
                'signal_direction': r.signal_direction, 'signal_quality': r.signal_quality,
                'upside_pct': r.upside_pct, 'position_size_pct': r.position_size_pct,
                'thesis': r.thesis, 'rationale': r.rationale,
                'pattern': r.pattern, 'extra': r.extra,
            }
            for r in scan.results
        ],
        'scan_notes': scan.scan_notes,
    })

"""
services/chat_pipeline.py — KB-grounded chat pipeline.

Extracts the multi-pass retrieval → prompt build → LLM orchestration
from routes/chat.py into a testable module. The route handler becomes
a thin wrapper that parses the request and calls run().
"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any, Dict, List, Optional, Tuple

import extensions as ext

_logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_PORTFOLIO_INTENT_KWS = (
    'my portfolio', 'my holdings', 'my positions', 'my stocks', 'my shares',
    'my book', 'my p&l', 'my pnl', 'my exposure', 'my allocation',
    'discuss my', 'analyse my', 'analyze my', 'review my',
    'affect my', 'impact my', 'affect portfolio', 'impact portfolio',
    'portfolio', 'holdings', 'positions',
)

_LIVE_PRICE_KEYWORDS = (
    'right now', 'right-now', 'trading at', 'trading now',
    'priced at', 'price now', 'price today',
    'how much is', 'how much does', 'worth right now', 'value right now',
    'spot price', 'live price', 'latest price',
    'at the moment', 'at this moment', 'as of now',
    'current price', 'current rate', 'current level',
)

_TIP_INTENT_PHRASES = (
    'give me a tip', 'give me tip', 'daily tip', 'today\'s tip',
    'what should i trade', 'what should i buy', 'what should i sell',
    'any setups worth', 'best opportunity right now', 'what\'s looking good',
    "what's looking good", 'best setup today', 'top trade today',
    'trade of the day', 'tip of the day', 'recommend a trade',
    'show me a trade', 'suggest a trade',
)

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

_STOPWORDS = {
    'THE','IS','AT','ON','AN','AND','OR','FOR','IN','OF',
    'TO','THAT','THIS','WITH','FROM','BY','ARE','WAS','BE',
    'HAS','HAVE','HAD','ITS','DO','DID','WHAT','HOW','WHY',
    'WHEN','WHERE','WHO','CAN','WILL','NOT','BUT','ALL',
}

_GEN_SKIP_KEYWORDS = (
    'what is', 'what\'s', 'tell me about', 'explain', 'why is', 'how is',
    'price of', 'signal for', 'analyse my portfolio', 'analyze my portfolio',
    'portfolio', 'my holdings',
)

_GEN_TRIGGER_WORDS = (
    'strategy', 'strateg', 'trade', 'trading', 'opportunity', 'opportunit',
    'setup', 'setups', 'find me', 'show me', 'make me', 'give me',
    'where are', 'what sectors', 'momentum', 'squeeze', 'gap', 'intraday',
    'daytime', 'ideas', 'idea', 'rotation', 'reversal', 'breakout',
    'best trade', 'top trade', 'mean reversion', 'play',
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _query_wants_live(message: str) -> bool:
    m = message.lower()
    return any(kw in m for kw in _LIVE_PRICE_KEYWORDS)


def _wants_portfolio(message: str) -> bool:
    m = message.lower()
    return any(kw in m for kw in _PORTFOLIO_INTENT_KWS)


def _is_tip_request(message: str) -> bool:
    m = message.lower()
    return any(ph in m for ph in _TIP_INTENT_PHRASES)


def sid_for_user(user_id):
    """Resolve the conversation session ID for a user."""
    if ext.HAS_CONV_STORE:
        return ext.session_id_for_user(user_id)
    return 'default'


# ── Pipeline stages ──────────────────────────────────────────────────────────

def _get_trader_level(user_id: Optional[str]) -> str:
    """Fetch trader level for user, default 'developing'."""
    if user_id and ext.HAS_PRODUCT_LAYER:
        try:
            row = ext.get_user(ext.DB_PATH, user_id)
            if row:
                return row.get('trader_level') or 'developing'
        except Exception:
            pass
    return 'developing'


def _check_chat_quota(user_id: Optional[str]) -> Optional[Dict]:
    """Check chat quota. Returns error dict if quota exceeded, else None."""
    if not (user_id and ext.HAS_TIERS and ext.HAS_PATTERN_LAYER):
        return None
    try:
        tier = ext.get_user_tier_for_request(user_id)
        from core.tiers import get_tier as _get_tier, _next_tier as _next_tier_name
        quota = _get_tier(tier).get('chat_queries_per_day')
        if quota is not None and quota == 0:
            return {
                'error': 'upgrade_required', 'feature': 'chat_queries_per_day',
                'current_tier': tier, 'upgrade_to': _next_tier_name(tier),
                'queries_used': 0, 'queries_limit': 0,
                'message': 'Chat is not available on the free plan. Subscribe to unlock.',
            }
        if quota is not None and quota > 0:
            used = ext.get_today_chat_count(ext.DB_PATH, user_id)
            if used >= quota:
                return {
                    'error': 'upgrade_required', 'feature': 'chat_queries_per_day',
                    'current_tier': tier, 'upgrade_to': _next_tier_name(tier),
                    'queries_used': used, 'queries_limit': quota,
                    'message': f'Daily chat limit of {quota} reached. Upgrade to Pro for unlimited queries.',
                }
    except Exception:
        pass
    return None


def _compute_adaptation_nudges(session_id: str, topic: Optional[str], conn):
    """Compute adaptation nudges from prior stress streak."""
    if not (ext.HAS_ADAPTATION and ext.HAS_STRESS):
        return None
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
        return engine.compute(state_stub, _StressStub(), topic=topic, key_terms=[])
    except Exception:
        return None


def _ticker_carry_forward(
    message: str, session_id: str, user_id: Optional[str],
    wants_portfolio: bool,
) -> Tuple[str, List[str]]:
    """Handle ticker extraction and carry-forward from previous turns.

    Returns (augmented_retrieve_message, current_tickers).
    """
    try:
        from retrieval import _extract_tickers as _et
        cur_tickers = _et(message)
    except Exception:
        cur_tickers = []

    if not ext.sessions.get_portfolio_tickers(session_id) and user_id and ext.HAS_PRODUCT_LAYER:
        try:
            ph = ext.get_portfolio(ext.DB_PATH, user_id)
            pticks = [h['ticker'] for h in (ph or []) if h.get('ticker')]
            if pticks:
                ext.sessions.set_portfolio_tickers(session_id, pticks)
        except Exception:
            pass

    retrieve_message = message
    aug_tickers: list = []
    if not cur_tickers and ext.sessions.has_tickers(session_id):
        aug_tickers = list(ext.sessions.get_tickers(session_id) or [])
    if wants_portfolio:
        port_ticks = ext.sessions.get_portfolio_tickers(session_id) or []
        for pt in port_ticks:
            if pt not in aug_tickers and pt not in cur_tickers:
                aug_tickers.append(pt)
    if aug_tickers:
        retrieve_message = message + ' ' + ' '.join(aug_tickers)

    return retrieve_message, cur_tickers


def _update_session_tickers(session_id: str, cur_tickers: List[str], atoms: List[Dict]):
    """Persist current tickers or infer from atoms."""
    if cur_tickers:
        ext.sessions.set_tickers(session_id, cur_tickers)
    elif not ext.sessions.has_tickers(session_id) and atoms:
        seen = list({a['subject'].upper() for a in atoms if 'subject' in a})[:4]
        if seen:
            ext.sessions.set_tickers(session_id, seen)


def _fetch_working_memory(
    message: str, session_id: str, user_id: Optional[str],
    atoms: List[Dict], wm_session_id: str,
) -> Tuple[str, List[str]]:
    """On-demand fetch of live data via working memory.

    Returns (live_context_snippet, live_fetched_tickers).
    """
    if not (ext.HAS_WORKING_MEMORY and ext.working_memory is not None):
        return '', []
    try:
        from retrieval import _extract_tickers
        from knowledge.working_memory import _YF_TICKER_MAP, MAX_ON_DEMAND_TICKERS

        tickers_in_query = _extract_tickers(message)
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
            if user_id and ext.HAS_PRODUCT_LAYER:
                try:
                    ph = ext.get_portfolio(ext.DB_PATH, user_id)
                    tickers_in_query = [h['ticker'] for h in (ph or []) if h.get('ticker')]
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
            return live_context, live_fetched
    except Exception:
        pass
    return '', []


def _trigger_async_discovery(message: str, user_id: Optional[str]):
    """Fire background discovery for stale tickers mentioned in the query."""
    if ext.discovery_pipeline is None:
        return
    try:
        from retrieval import _extract_tickers as _et_disc
        disc_tickers = _et_disc(message)
        for dt in disc_tickers[:3]:
            stale = ext.discovery_pipeline.assess_staleness(dt)
            if stale:
                threading.Thread(
                    target=ext.discovery_pipeline.discover,
                    args=(dt, 'user_query', user_id),
                    daemon=True,
                ).start()
    except Exception:
        pass


def _compute_stress(message: str, atoms: List[Dict], conn) -> Tuple[Any, Optional[Dict]]:
    """Compute epistemic stress. Returns (stress_report, stress_dict)."""
    if not (ext.HAS_STRESS and atoms):
        return None, None
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
        return stress_report, stress_dict
    except Exception:
        return None, None


def _update_session_streak(session_id: str, stress_report):
    """Update the epistemic adaptation streak after stress computation."""
    if not (ext.HAS_ADAPTATION and stress_report):
        return
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


def _diagnose_kb_insufficiency(
    message: str, topic: Optional[str], stress_report, atoms: List[Dict], conn,
) -> Optional[Dict]:
    """Run KB insufficiency classifier if stress is high or atom count is low."""
    if not (ext.HAS_CLASSIFIER and stress_report and atoms):
        return None
    try:
        tickers = [t for t in re.findall(r'\b[A-Z]{2,5}\b', message)
                    if t not in _STOPWORDS]
        terms = [w.lower() for w in re.findall(r'\b[a-zA-Z][a-zA-Z0-9]{2,}\b', message)]
        composite = getattr(stress_report, 'composite_stress', 0.0)
        atom_count = len(atoms)
        if composite > 0.35 or atom_count < 8:
            topic_hint = (topic or
                          (tickers[0] if tickers else None) or
                          (terms[0] if terms else None) or
                          message[:40])
            diag = ext.classify_insufficiency(topic_hint, stress_report, conn)
            return {
                'topic':         diag.topic,
                'types':         [t.value for t in diag.types],
                'primary_type':  diag.primary_type().value,
                'confidence':    diag.confidence,
                'matched_rules': diag.matched_rules,
                'signals':       diag.signals,
            }
    except Exception:
        pass
    return None


def _build_portfolio_context(user_id: Optional[str], conn) -> Optional[str]:
    """Build rich portfolio context string for the LLM prompt."""
    if not (user_id and ext.HAS_PRODUCT_LAYER):
        return None
    try:
        holdings = ext.get_portfolio(ext.DB_PATH, user_id)
        model = ext.get_user_model(ext.DB_PATH, user_id)
        if not holdings:
            return None
        h_parts = [f"{h['ticker']} ×{int(h['quantity'])}" for h in holdings[:20]]
        pos_values = [
            h['quantity'] * h['avg_cost']
            for h in holdings if h.get('quantity') and h.get('avg_cost')
        ]
        total_cost = sum(pos_values)
        largest_pct = (
            round(max(pos_values) / total_cost * 100)
            if total_cost > 0 and pos_values else None
        )
        lines = ["=== USER PORTFOLIO ===",
                  f"Holdings: {', '.join(h_parts)}"]
        if total_cost > 0:
            lines.append(f"Total invested (cost basis): £{total_cost:,.0f}")
        if largest_pct is not None:
            largest_ticker = max(
                (h for h in holdings if h.get('quantity') and h.get('avg_cost')),
                key=lambda h: h['quantity'] * h['avg_cost']
            )['ticker']
            lines.append(f"Largest single position: {largest_pct}% ({largest_ticker})")
        if model:
            risk    = model.get('risk_tolerance', '')
            style   = model.get('holding_style', '')
            sectors = ', '.join(model.get('sector_affinity') or [])
            profile = ' · '.join(p for p in [risk, style, sectors] if p)
            if profile:
                lines.append(f"Risk profile: {profile}")
        holding_tickers = [h['ticker'] for h in holdings]
        ticker_atoms: dict = {}
        for ht in holding_tickers:
            try:
                ht_rows = conn.execute(
                    """SELECT predicate, object FROM facts
                       WHERE subject=? AND predicate IN
                       ('last_price','currency','price_regime','signal_direction',
                        'signal_quality','return_1m','return_3m','return_1y',
                        'upside_pct','conviction_tier','macro_confirmation')
                       ORDER BY predicate""",
                    (ht.lower(),)
                ).fetchall()
                if ht_rows:
                    ticker_atoms[ht] = ht_rows
            except Exception:
                pass
        lines.append("\nPer-holding KB signals:")
        for ht in holding_tickers:
            rows = ticker_atoms.get(ht, [])
            if not rows:
                lines.append(f"  {ht}: No KB signals available — discuss based on general knowledge of this ticker.")
                continue
            d = {p: v for p, v in rows}
            price    = d.get('last_price', '?')
            regime   = d.get('price_regime', '?').replace('_', ' ')
            dir_     = d.get('signal_direction', '?')
            qual     = d.get('signal_quality', '?')
            macro    = d.get('macro_confirmation', '?')
            conv     = d.get('conviction_tier', '?')
            upside   = d.get('upside_pct', '?')
            ret1m    = d.get('return_1m', '')
            ret1y    = d.get('return_1y', '')
            target   = d.get('price_target', '')
            implied = ''
            try:
                if target and price and price != '?' and target != '?':
                    move = float(target) - float(price)
                    move_dir = 'up to' if move >= 0 else 'down to'
                    implied = (f" The KB price target is {target}, implying a move "
                                f"{move_dir} {target} ({upside}% from current price).")
            except Exception:
                pass
            sent = (
                f"  {ht}: Current price {price} ({regime} regime). "
                f"KB signal direction is {dir_}.{implied} "
                f"Signal quality: {qual}. Macro confirmation: {macro}. "
                f"Conviction tier: {conv}."
            )
            if ret1m:
                sent += f" 1-month return: {ret1m}%."
            if ret1y:
                sent += f" 1-year return: {ret1y}%."
            lines.append(sent)

        portfolio_context = '\n'.join(lines)

        # Geo-risk context injection
        try:
            import sqlite3 as _sq
            gc = _sq.connect(ext.DB_PATH, timeout=5)
            geo_lines = []
            for ht in holding_tickers:
                geo_row = gc.execute(
                    """SELECT object FROM facts
                       WHERE subject=? AND predicate='geopolitical_risk_exposure'
                       ORDER BY confidence DESC LIMIT 1""",
                    (ht.lower(),),
                ).fetchone()
                if geo_row and geo_row[0] in ('elevated', 'moderate'):
                    geo_lines.append(f"  {ht}: geopolitical_risk_exposure={geo_row[0]}")
            shock_row = gc.execute(
                """SELECT object FROM facts
                   WHERE subject='macro_regime' AND predicate='energy_shock_risk'
                   ORDER BY confidence DESC LIMIT 1"""
            ).fetchone()
            gc.close()
            if geo_lines or (shock_row and shock_row[0] in ('elevated', 'moderate')):
                portfolio_context += '\n=== GEOPOLITICAL RISK FLAGS ==='
                if shock_row and shock_row[0] in ('elevated', 'moderate'):
                    portfolio_context += f'\n  Energy shock risk: {shock_row[0]} (WTI/Middle East tension)'
                if geo_lines:
                    portfolio_context += '\n' + '\n'.join(geo_lines)
        except Exception:
            pass

        return portfolio_context
    except Exception:
        return None


def _llm_data_request_pass(
    message: str, snippet: str, portfolio_context: Optional[str],
    model: str, wm_session_id: str, atoms: List[Dict],
    live_fetched: List[str],
) -> Tuple[str, List[str], List[str], Optional[str]]:
    """Pass 1: ask LLM if it needs more data before answering.

    Returns (live_context, live_fetched, llm_requested_tickers, web_searched).
    """
    if not (ext.HAS_WORKING_MEMORY and ext.working_memory is not None
            and not live_fetched
            and (len(atoms) < 8 or _query_wants_live(message))):
        return '', live_fetched, [], None
    try:
        from knowledge.working_memory import (
            DATA_REQUEST_SYSTEM_PROMPT, parse_llm_response
        )
        p1_ctx = snippet or '(No KB context)'
        if portfolio_context:
            p1_ctx = portfolio_context + '\n\n' + p1_ctx
        p1_messages = [
            {'role': 'system', 'content': DATA_REQUEST_SYSTEM_PROMPT},
            {'role': 'user',   'content': f"{p1_ctx}\n\nQuestion: {message}"},
        ]
        p1_raw = ext.llm_chat(p1_messages, model=model)
        if p1_raw:
            mode, payload = parse_llm_response(p1_raw)
            if mode == 'data_request' and payload:
                ext.working_memory.open_session(wm_session_id)
                for t in payload:
                    ext.working_memory.fetch_on_demand(t, wm_session_id, ext.DB_PATH)
                live_context = ext.working_memory.get_session_snippet(wm_session_id)
                live_fetched = ext.working_memory.get_fetched_tickers(wm_session_id)
                return live_context, live_fetched, payload, None
            elif mode == 'search_request' and payload:
                search_query = payload[0]
                ext.working_memory.open_session(wm_session_id)
                search_atoms = ext.working_memory.web_search_on_demand(
                    search_query, wm_session_id
                )
                if search_atoms:
                    live_context = ext.working_memory.get_session_snippet(wm_session_id)
                    live_fetched = ext.working_memory.get_fetched_tickers(wm_session_id)
                    return live_context, live_fetched, [], search_query
    except Exception:
        pass
    return '', live_fetched, [], None


def _resolve_aliases(snippet: str) -> Dict[str, str]:
    """Extract ticker alias mappings from snippet."""
    aliases: Dict[str, str] = {}
    if snippet and 'is an alias' in snippet:
        for m in re.finditer(
            r"INSTRUCTION: '(\S+)' is an alias\. The KB data below \(subject='(\S+)'\)",
            snippet
        ):
            aliases[m.group(1)] = m.group(2).upper()
    return aliases


def _detect_tip_intent(
    message: str, user_id: Optional[str], response: Dict,
):
    """Detect tip request intent and attach tip card to response."""
    if not (_is_tip_request(message) and ext.HAS_PATTERN_LAYER and user_id):
        return
    try:
        from notifications.tip_scheduler import _pick_best_pattern, _get_local_now
        from notifications.tip_formatter import format_tip, tip_to_dict, TIER_LIMITS
        from analytics.pattern_detector import PatternSignal
        from analytics.position_calculator import calculate_position
        import sqlite3 as _sq2

        prefs_row = None
        c_tip = _sq2.connect(ext.DB_PATH, timeout=5)
        try:
            prefs_row = c_tip.execute(
                """SELECT tier, tip_timeframes, tip_pattern_types,
                          account_size, max_risk_per_trade_pct, account_currency
                   FROM user_preferences WHERE user_id=?""", (user_id,)
            ).fetchone()
        finally:
            c_tip.close()

        tier = 'basic'
        tip_prefs: dict = {}
        if prefs_row:
            import json as _json
            tier = prefs_row[0] or 'basic'
            def _j(v):
                try: return _json.loads(v) if v else None
                except Exception: return None
            limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
            tip_prefs = {
                'account_size': prefs_row[3] or 10000,
                'max_risk_per_trade_pct': prefs_row[4] or 1.0,
                'account_currency': prefs_row[5] or 'GBP',
                'tier': tier,
                'tip_timeframes': _j(prefs_row[1]) or limits['timeframes'],
                'tip_pattern_types': _j(prefs_row[2]),
            }

        pat_row = _pick_best_pattern(
            ext.DB_PATH, user_id, tier,
            tip_prefs.get('tip_timeframes', ['1h']),
            tip_prefs.get('tip_pattern_types'),
        )
        if pat_row:
            sig = PatternSignal(
                pattern_type=pat_row['pattern_type'], ticker=pat_row['ticker'],
                direction=pat_row['direction'], zone_high=pat_row['zone_high'],
                zone_low=pat_row['zone_low'], zone_size_pct=pat_row.get('zone_size_pct', 0.0),
                timeframe=pat_row['timeframe'], formed_at=pat_row.get('formed_at', ''),
                quality_score=pat_row.get('quality_score') or 0.0, status=pat_row['status'],
                kb_conviction=pat_row.get('kb_conviction', ''),
                kb_regime=pat_row.get('kb_regime', ''),
                kb_signal_dir=pat_row.get('kb_signal_dir', ''),
            )
            tip_pos = calculate_position(sig, tip_prefs) if tip_prefs else None
            tip_dict = tip_to_dict(sig, tip_pos, tier=tier)
            response['tip_card'] = {
                **tip_dict, 'tip_id': None, 'pattern_id': pat_row.get('id'),
                'feedback_actions': ['taking_it', 'tell_me_more', 'not_for_me'],
            }
    except Exception as e:
        _logger.warning('on-demand tip failed: %s', e)


def _run_opportunity_scan(message: str, response: Dict) -> Optional[str]:
    """Run opportunity scan if message matches trigger words. Returns context string."""
    try:
        from analytics.opportunity_engine import (
            classify_intent as _classify_intent,
            run_opportunity_scan as _run_scan,
            format_scan_as_context as _format_ctx,
        )
        gen_modes = _classify_intent(message)
        is_gen_query = not any(kw in message.lower() for kw in _GEN_SKIP_KEYWORDS)
        has_gen_trigger = any(kw in message.lower() for kw in _GEN_TRIGGER_WORDS)
        if is_gen_query and has_gen_trigger and gen_modes:
            scan = _run_scan(
                query=message, db_path=ext.DB_PATH,
                modes=gen_modes, limit_per_mode=6,
            )
            response['opportunity_scan'] = {
                'mode': scan.mode, 'results': len(scan.results), 'regime': scan.market_regime,
            }
            return _format_ctx(scan)
    except Exception as e:
        _logger.warning('opportunity scan failed: %s', e)
    return None


_TOKEN_BUDGET = 100_000
_CHARS_PER_TOKEN = 4


def _estimate_tokens(messages: List[Dict]) -> int:
    return sum(len(m.get('content', '')) for m in messages) // _CHARS_PER_TOKEN


def _persist_and_inject_history(
    message: str, user_id: Optional[str],
    messages: List[Dict], conv_session_id: str,
) -> Tuple[List[Dict], Optional[Dict]]:
    """Persist user turn and inject DB-backed conversation history.

    Returns (updated_messages, user_msg_record).
    Enforces a token budget: oldest history turns are dropped first if over budget.
    """
    if ext.conv_store is None:
        return messages, None
    try:
        user_msg_record = ext.conv_store.add_message(
            conv_session_id, 'user', message, user_id=user_id
        )
        db_history = ext.conv_store.get_recent_messages_for_context(
            conv_session_id, n_turns=8
        )
        just_id = user_msg_record.get('id') if user_msg_record else None
        db_hist_msgs = [
            {'role': m['role'], 'content': m['content']}
            for m in db_history if m.get('id') != just_id
        ]
        last_user_msg = next(
            (m['content'] for m in reversed(db_hist_msgs) if m['role'] == 'user'), None
        )
        is_retry = (
            last_user_msg is not None
            and message.strip().lower() == last_user_msg.strip().lower()
        )
        if db_hist_msgs and len(messages) >= 2 and not is_retry:
            system_msg = messages[0]
            final_user_msg = messages[-1]
            base_tokens = _estimate_tokens([system_msg, final_user_msg])
            history_budget = _TOKEN_BUDGET - base_tokens
            # Drop oldest turns first until within budget
            trimmed = list(db_hist_msgs)
            while trimmed and _estimate_tokens(trimmed) > history_budget:
                trimmed = trimmed[2:] if len(trimmed) >= 2 else []
            if trimmed:
                messages = [system_msg] + trimmed + [final_user_msg]
                if len(trimmed) < len(db_hist_msgs):
                    _logger.debug(
                        'History trimmed %d→%d turns for session %s (token budget)',
                        len(db_hist_msgs) // 2, len(trimmed) // 2, conv_session_id,
                    )
        return messages, user_msg_record
    except Exception:
        return messages, None


def _persist_assistant_and_graduate(
    answer: str, message: str, session_id: str,
    conv_session_id: str, user_id: Optional[str],
    stress_dict: Optional[Dict], atoms: List[Dict],
    turn_count: int, goal: Optional[str],
):
    """Persist assistant turn and fire async atom extraction → KB graduation."""
    if ext.conv_store is None:
        return
    try:
        stress_val = stress_dict.get('composite_stress') if stress_dict else None
        asst_meta = {
            'tickers': ext.sessions.get_tickers(session_id) or [],
            'stress':  stress_val,
            'atoms':   len(atoms),
        }
        asst_msg_record = ext.conv_store.add_message(
            conv_session_id, 'assistant', answer,
            metadata=asst_meta, user_id=user_id
        )
    except Exception:
        asst_msg_record = None

    atom_msg_id  = asst_msg_record.get('id') if asst_msg_record else None
    atom_user_q  = message
    atom_answer  = answer
    atom_cs_id   = conv_session_id
    atom_sess_id = session_id
    atom_turn    = turn_count
    atom_goal    = goal

    def _extract_and_graduate():
        try:
            if atom_msg_id is None:
                return
            from llm.ollama_client import chat as _oc
            import sqlite3 as _sq
            _grad_conn = _sq.connect(ext.DB_PATH, timeout=10)
            atom_prompt = [
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
                    f'User said: "{atom_user_q[:300]}"\n'
                    f'Assistant replied: "{atom_answer[:400]}"'
                )},
            ]
            raw = _oc(atom_prompt, model='llama3.2')
            _grad_conn.close()
            if not raw:
                return
            import json as _json
            s = raw.find('[')
            e = raw.rfind(']') + 1
            if s == -1 or e <= 0:
                return
            extracted_atoms = _json.loads(raw[s:e])
            if not isinstance(extracted_atoms, list):
                return
            ext.conv_store.add_turn_atoms(atom_msg_id, atom_cs_id, extracted_atoms)

            import math as _math
            salient = ext.conv_store.get_salient_atoms(atom_cs_id, limit=30, min_salience=0.1)
            graduated = []
            _PRICE_PREDICATES = {
                'last_price', 'price', 'price_target', 'price_range',
                'invalidation_price', 'nav_price', 'close_price',
                'open_price', 'high_price', 'low_price',
                'high_52w', 'low_52w', 'pe_ratio', 'eps', 'revenue',
                'market_cap', 'market_cap_tier', 'return_1m', 'return_1y',
                'return_1w', 'return_3m', 'return_6m', 'drawdown_from_52w_high',
                'upside_pct', 'volatility_30d', 'volatility_90d',
            }
            for at in salient:
                if at.get('graduated'):
                    continue
                is_user_intent = (
                    at.get('source') == 'user' and at.get('atom_type') == 'intent'
                )
                threshold = 0.25 if is_user_intent else 0.40
                if at.get('predicate') in _PRICE_PREDICATES:
                    continue
                if at['effective_salience'] >= threshold:
                    try:
                        ext.kg.add_fact(
                            at['subject'], at['predicate'], at['object'],
                            source='conversation',
                            confidence=round(at['effective_salience'], 3),
                        )
                        ext.conv_store.mark_atom_graduated(at['id'])
                        graduated.append(at)
                    except Exception:
                        pass
            del _grad_conn

            if ext.HAS_WORKING_STATE and graduated:
                try:
                    ws2 = ext.get_working_state_store(ext.DB_PATH)
                    top_subj = list(dict.fromkeys(
                        a['subject'] for a in graduated
                    ))[:3]
                    ws2.maybe_persist(
                        atom_sess_id, atom_turn,
                        goal=atom_goal, topic=', '.join(top_subj),
                        last_intent=atom_user_q[:120], force=True,
                    )
                except Exception:
                    pass
        except Exception:
            pass

    threading.Thread(target=_extract_and_graduate, daemon=True).start()


def _commit_working_memory(wm_session_id: str, live_fetched: List[str], response: Dict):
    """Commit working memory atoms back to KB."""
    if ext.HAS_WORKING_MEMORY and ext.working_memory and live_fetched:
        try:
            commit_result = ext.working_memory.commit_session(wm_session_id, ext.kg)
            response['kb_enriched']     = commit_result.committed > 0
            response['live_fetched']    = live_fetched
            response['atoms_committed'] = commit_result.committed
        except Exception:
            ext.working_memory.close_without_commit(wm_session_id)


# ── Main pipeline entry point ────────────────────────────────────────────────

def run(
    message: str,
    session_id: str = 'default',
    model: str | None = None,
    goal: str | None = None,
    topic: str | None = None,
    turn_count: int = 1,
    limit: int = 30,
    screen_context: str = '',
    screen_entities: list | None = None,
    overlay_mode: bool = False,
    user_id: str | None = None,
) -> Tuple[Dict, int]:
    """Execute the full KB-grounded chat pipeline.

    Returns (response_dict, http_status_code).
    """
    if model is None:
        model = ext.DEFAULT_MODEL if ext.HAS_LLM else 'llama3.2'

    # ── Trader level ──────────────────────────────────────────────────────
    trader_level = _get_trader_level(user_id)

    # ── Chat quota ────────────────────────────────────────────────────────
    quota_error = _check_chat_quota(user_id)
    if quota_error:
        return quota_error, 403

    # ── Portfolio intent ──────────────────────────────────────────────────
    portfolio_wanted = _wants_portfolio(message)
    if user_id and portfolio_wanted:
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
    nudges = _compute_adaptation_nudges(session_id, topic, conn)

    # ── Ticker carry-forward + KB retrieval ───────────────────────────────
    retrieve_message, cur_tickers = _ticker_carry_forward(
        message, session_id, user_id, portfolio_wanted,
    )
    snippet, atoms = ext.retrieve(retrieve_message, conn, limit=limit, nudges=nudges)
    _update_session_tickers(session_id, cur_tickers, atoms)

    # ── Working memory ────────────────────────────────────────────────────
    wm_session_id = f'wm_{session_id}'
    live_context, live_fetched = _fetch_working_memory(
        message, session_id, user_id, atoms, wm_session_id,
    )

    # ── Async discovery ───────────────────────────────────────────────────
    _trigger_async_discovery(message, user_id)

    # ── Epistemic stress ──────────────────────────────────────────────────
    stress_report, stress_dict = _compute_stress(message, atoms, conn)
    _update_session_streak(session_id, stress_report)

    # ── KB insufficiency ──────────────────────────────────────────────────
    kb_diagnosis = _diagnose_kb_insufficiency(message, topic, stress_report, atoms, conn)

    # ── Overlay cards ─────────────────────────────────────────────────────
    overlay_cards = None
    if overlay_mode and ext.HAS_OVERLAY:
        try:
            overlay_tickers = ext.extract_overlay_tickers(screen_context, conn, screen_entities or [])
            overlay_cards = ext.build_overlay_cards(overlay_tickers, conn, stress_dict)
        except Exception:
            overlay_cards = []

    # ── Build response skeleton ───────────────────────────────────────────
    response: Dict = {
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

    # ── LLM availability ─────────────────────────────────────────────────
    if not ext.HAS_LLM:
        response['error'] = 'llm package not available'
        return response, 503
    if not ext.is_available():
        response['error'] = 'Ollama not reachable — KB context returned without LLM answer'
        return response, 503

    # ── Portfolio context ─────────────────────────────────────────────────
    portfolio_context = None
    if portfolio_wanted:
        portfolio_context = _build_portfolio_context(user_id, conn)

    # ── Pass 1: LLM data request ─────────────────────────────────────────
    live_context_p1, live_fetched, llm_requested_tickers, web_searched = \
        _llm_data_request_pass(
            message, snippet, portfolio_context, model,
            wm_session_id, atoms, live_fetched,
        )
    if live_context_p1:
        live_context = live_context_p1

    # ── Resolved aliases ──────────────────────────────────────────────────
    resolved_aliases = _resolve_aliases(snippet)

    # ── Prior conversation turns ──────────────────────────────────────────
    has_prior_turns = False
    if ext.conv_store is not None:
        try:
            conv_sid_check = sid_for_user(user_id)
            check_hist = ext.conv_store.get_recent_messages_for_context(conv_sid_check, n_turns=2)
            has_prior_turns = len(check_hist) > 1
        except Exception:
            pass

    # ── Tip intent ────────────────────────────────────────────────────────
    _detect_tip_intent(message, user_id, response)

    # ── Opportunity scan ──────────────────────────────────────────────────
    opportunity_scan_context = _run_opportunity_scan(message, response)

    # ── Build full prompt ─────────────────────────────────────────────────
    messages = ext.build_prompt(
        user_message=message, snippet=snippet, stress=stress_dict,
        kb_diagnosis=kb_diagnosis, prior_context=prior_context,
        portfolio_context=portfolio_context, atom_count=len(atoms),
        live_context=live_context or None,
        resolved_aliases=resolved_aliases or None,
        web_searched=web_searched or None,
        has_history=has_prior_turns,
        opportunity_scan_context=opportunity_scan_context,
        trader_level=trader_level,
    )

    # ── Persist user turn + inject history ────────────────────────────────
    conv_session_id = sid_for_user(user_id)
    messages, _user_msg_record = _persist_and_inject_history(
        message, user_id, messages, conv_session_id,
    )

    # ── LLM call ──────────────────────────────────────────────────────────
    answer = ext.llm_chat(messages, model=model)
    if answer is None:
        if ext.HAS_WORKING_MEMORY and ext.working_memory:
            ext.working_memory.close_without_commit(wm_session_id)
        response['error'] = 'Ollama returned no response'
        return response, 503

    response['answer'] = answer
    if llm_requested_tickers:
        response['llm_requested_tickers'] = llm_requested_tickers
    if web_searched:
        response['web_searched'] = web_searched

    # ── Calibration lookup — best-evidenced row for primary ticker ────────────
    if cur_tickers:
        try:
            import sqlite3 as _sq_cal
            _cc = _sq_cal.connect(ext.DB_PATH, timeout=5)
            _cal_row = _cc.execute(
                """SELECT pattern_type, timeframe, sample_size,
                          hit_rate_t1, hit_rate_t2, calibration_confidence, confidence_label
                   FROM signal_calibration
                   WHERE ticker = ?
                   ORDER BY calibration_confidence DESC, sample_size DESC
                   LIMIT 1""",
                (cur_tickers[0].lower(),),
            ).fetchone()
            _cc.close()
            if _cal_row and _cal_row[2] >= 10:
                response['calibration'] = {
                    'pattern_type':           _cal_row[0],
                    'timeframe':              _cal_row[1],
                    'n_total':                _cal_row[2],
                    'hit_rate_t1':            _cal_row[3],
                    'hit_rate_t2':            _cal_row[4],
                    'calibration_confidence': _cal_row[5],
                    'confidence_label':       _cal_row[6],
                }
        except Exception:
            pass

    # ── Persist assistant turn + KB graduation ────────────────────────────
    _persist_assistant_and_graduate(
        answer, message, session_id, conv_session_id, user_id,
        stress_dict, atoms, turn_count, goal,
    )

    # ── Commit working memory ─────────────────────────────────────────────
    _commit_working_memory(wm_session_id, live_fetched, response)

    return response, 200

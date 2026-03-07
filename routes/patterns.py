"""routes/patterns.py — Pattern endpoints: open patterns, feedback, tip performance."""

from __future__ import annotations

import logging
import sqlite3

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('patterns', __name__)
_logger = logging.getLogger(__name__)


@bp.route('/patterns/open', methods=['GET'])
@ext.limiter.exempt
def patterns_open():
    """
    GET /patterns/open?ticker=NVDA&min_quality=0.5&limit=50

    Return open (unfilled) pattern signals from the pattern_signals table.
    """
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    ticker      = request.args.get('ticker', '').strip().upper() or None
    min_quality = float(request.args.get('min_quality', 0.0))
    limit       = int(request.args.get('limit', 50))

    try:
        patterns = ext.get_open_patterns(
            ext.DB_PATH, ticker=ticker, min_quality=min_quality, limit=limit,
        )
        return jsonify({'patterns': patterns, 'count': len(patterns)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/patterns/<int:pattern_id>/feedback', methods=['POST'])
@ext.require_auth
def pattern_feedback(pattern_id: int):
    """
    POST /patterns/<pattern_id>/feedback
    Body: { "action": "taking_it"|"tell_me_more"|"not_for_me", "comment": "..." }
    """
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    data    = request.get_json(force=True, silent=True) or {}
    action  = str(data.get('action', '')).strip()
    comment = str(data.get('comment', ''))[:500]

    _VALID_ACTIONS = {'taking_it', 'tell_me_more', 'not_for_me',
                      'skip', 'like', 'dislike'}
    if action not in _VALID_ACTIONS:
        return jsonify({'error': f'action must be one of: {sorted(_VALID_ACTIONS)}'}), 400

    try:
        ext.ensure_tip_feedback_table(ext.DB_PATH)
        ext.log_tip_feedback(
            ext.DB_PATH,
            user_id=g.user_id,
            pattern_signal_id=pattern_id,
            action=action,
            comment=comment,
        )

        # Log engagement for personalisation
        if ext.HAS_HYBRID:
            try:
                conn = sqlite3.connect(ext.DB_PATH, timeout=5)
                row = conn.execute(
                    "SELECT ticker, pattern_type FROM pattern_signals WHERE id=?",
                    (pattern_id,),
                ).fetchone()
                conn.close()
                if row:
                    ext.log_engagement_event(
                        ext.DB_PATH, g.user_id, f'tip_{action}',
                        ticker=row[0], pattern_type=row[1],
                    )
                    ext.update_from_feedback(g.user_id, ext.DB_PATH)
            except Exception:
                pass

        return jsonify({'ok': True, 'pattern_id': pattern_id, 'action': action})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/tip/performance', methods=['GET'])
@ext.require_auth
def tip_performance():
    """GET /tip/performance — global tip performance stats."""
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503
    try:
        perf = ext.get_tip_performance(ext.DB_PATH, g.user_id)
        return jsonify(perf)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/patterns/live', methods=['GET'])
def patterns_live():
    """
    GET /patterns/live?ticker=NVDA&pattern_type=fvg&direction=bullish
                      &timeframe=1h&min_quality=0.5&limit=20
    """
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503
    ticker = request.args.get('ticker')
    pattern_type = request.args.get('pattern_type')
    direction = request.args.get('direction')
    timeframe = request.args.get('timeframe')
    try:
        min_quality = float(request.args.get('min_quality', 0.0))
        limit = int(request.args.get('limit', 50))
    except (ValueError, TypeError):
        return jsonify({'error': 'min_quality and limit must be numeric'}), 400
    try:
        patterns = ext.get_open_patterns(
            ext.DB_PATH,
            ticker=ticker or None,
            pattern_type=pattern_type or None,
            direction=direction or None,
            timeframe=timeframe or None,
            min_quality=min_quality,
            limit=limit,
        )
        return jsonify({'patterns': patterns, 'count': len(patterns)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/patterns/<int:pattern_id>', methods=['GET'])
def pattern_detail(pattern_id: int):
    """GET /patterns/<id>?user_id=<uid> — full detail for a single pattern signal."""
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    import json as _json
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            """SELECT id, ticker, pattern_type, direction, zone_high, zone_low,
                      zone_size_pct, timeframe, formed_at, status, filled_at,
                      quality_score, kb_conviction, kb_regime, kb_signal_dir,
                      alerted_users, detected_at
               FROM pattern_signals WHERE id = ?""",
            (pattern_id,),
        ).fetchone()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if row is None:
        return jsonify({'error': 'pattern not found'}), 404

    cols = ['id', 'ticker', 'pattern_type', 'direction', 'zone_high', 'zone_low',
            'zone_size_pct', 'timeframe', 'formed_at', 'status', 'filled_at',
            'quality_score', 'kb_conviction', 'kb_regime', 'kb_signal_dir',
            'alerted_users', 'detected_at']
    pattern = dict(zip(cols, row))
    try:
        pattern['alerted_users'] = _json.loads(pattern['alerted_users'] or '[]')
    except Exception:
        pattern['alerted_users'] = []

    position = None
    user_id = request.args.get('user_id')
    if user_id:
        try:
            from analytics.pattern_detector import PatternSignal
            c = sqlite3.connect(ext.DB_PATH, timeout=5)
            pref_row = c.execute(
                """SELECT account_size, max_risk_per_trade_pct, account_currency
                   FROM user_preferences WHERE user_id = ?""",
                (user_id,),
            ).fetchone()
            c.close()
            if pref_row:
                prefs = dict(zip(['account_size', 'max_risk_per_trade_pct', 'account_currency'], pref_row))
                sig = PatternSignal(
                    pattern_type=pattern['pattern_type'], ticker=pattern['ticker'],
                    direction=pattern['direction'], zone_high=pattern['zone_high'],
                    zone_low=pattern['zone_low'], zone_size_pct=pattern['zone_size_pct'],
                    timeframe=pattern['timeframe'], formed_at=pattern['formed_at'],
                    quality_score=pattern['quality_score'] or 0.0, status=pattern['status'],
                    kb_conviction=pattern.get('kb_conviction', ''),
                    kb_regime=pattern.get('kb_regime', ''),
                    kb_signal_dir=pattern.get('kb_signal_dir', ''),
                )
                pos = ext.calculate_position(sig, prefs)
                if pos is not None:
                    from dataclasses import asdict
                    position = asdict(pos)
        except Exception:
            position = None

    return jsonify({'pattern': pattern, 'position': position})


@bp.route('/feedback', methods=['POST'])
def submit_feedback():
    """
    POST /feedback
    Body: { "user_id": "alice", "tip_id": 4, "pattern_id": 42, "outcome": "hit_t2" }
    """
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    user_id = getattr(g, 'user_id', None) or str(data.get('user_id', '')).strip()
    outcome = str(data.get('outcome', '')).strip()

    if not user_id:
        return jsonify({'error': 'user_id is required'}), 400

    _VALID_OUTCOMES = {'hit_t1', 'hit_t2', 'hit_t3', 'stopped_out', 'pending', 'skipped'}
    if outcome not in _VALID_OUTCOMES:
        return jsonify({'error': f'outcome must be one of: {", ".join(sorted(_VALID_OUTCOMES))}'}), 400

    try:
        tip_id = int(data['tip_id']) if data.get('tip_id') is not None else None
        pattern_id = int(data['pattern_id']) if data.get('pattern_id') is not None else None
    except (TypeError, ValueError) as e:
        return jsonify({'error': f'tip_id and pattern_id must be integers: {e}'}), 400

    try:
        row = ext.log_tip_feedback(ext.DB_PATH, user_id, outcome,
                                   tip_id=tip_id, pattern_id=pattern_id)

        if ext.HAS_HYBRID and pattern_id is not None:
            try:
                _conn = sqlite3.connect(ext.DB_PATH, timeout=5)
                try:
                    prow = _conn.execute(
                        "SELECT ticker, pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                        (pattern_id,),
                    ).fetchone()
                finally:
                    _conn.close()
                if prow:
                    ext.update_calibration(
                        ticker=prow[0], pattern_type=prow[1],
                        timeframe=prow[2], market_regime=prow[3] or None,
                        outcome=outcome, db_path=ext.DB_PATH,
                    )
                    ext.update_from_feedback(user_id,
                        {'pattern_type': prow[1], 'outcome': outcome}, ext.DB_PATH)
            except Exception:
                pass

        return jsonify({'id': row['id'], 'recorded': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/tips/<int:tip_id>/feedback', methods=['POST'])
@ext.limiter.exempt
def tip_feedback_action(tip_id: int):
    """
    POST /tips/<tip_id>/feedback
    Body: { "user_id": "alice", "action": "taking_it"|"tell_me_more"|"not_for_me",
            "rejection_reason": "too_risky", "pattern_id": 42 }
    """
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    from datetime import datetime, timezone

    data = request.get_json(force=True, silent=True) or {}
    user_id = getattr(g, 'user_id', None) or str(data.get('user_id', '')).strip()
    action = str(data.get('action', '')).strip()
    pattern_id = data.get('pattern_id')

    if not user_id:
        return jsonify({'error': 'user_id required'}), 400
    if action not in ('taking_it', 'tell_me_more', 'not_for_me'):
        return jsonify({'error': 'action must be taking_it|tell_me_more|not_for_me'}), 400

    try:
        pattern_row = None
        if pattern_id:
            _c = sqlite3.connect(ext.DB_PATH, timeout=5)
            try:
                r = _c.execute(
                    """SELECT id, ticker, pattern_type, direction, timeframe,
                              zone_low, zone_high, quality_score, status,
                              kb_conviction, kb_regime, kb_signal_dir
                       FROM pattern_signals WHERE id=?""",
                    (int(pattern_id),),
                ).fetchone()
                if r:
                    cols = ['id', 'ticker', 'pattern_type', 'direction', 'timeframe',
                            'zone_low', 'zone_high', 'quality_score', 'status',
                            'kb_conviction', 'kb_regime', 'kb_signal_dir']
                    pattern_row = dict(zip(cols, r))
            finally:
                _c.close()

        # Path A: Taking it
        if action == 'taking_it':
            from users.user_store import create_tip_followup, ensure_tip_feedback_table
            from analytics.pattern_detector import PatternSignal
            from analytics.position_calculator import calculate_position

            if not pattern_row:
                return jsonify({'error': 'pattern_id required for taking_it'}), 400

            _c2 = sqlite3.connect(ext.DB_PATH, timeout=5)
            try:
                prefs_row = _c2.execute(
                    """SELECT account_size, max_risk_per_trade_pct, account_currency, tier
                       FROM user_preferences WHERE user_id=?""", (user_id,)
                ).fetchone()
            finally:
                _c2.close()
            prefs = {}
            if prefs_row:
                prefs = {
                    'account_size': prefs_row[0] or 10000,
                    'max_risk_per_trade_pct': prefs_row[1] or 1.0,
                    'account_currency': prefs_row[2] or 'GBP',
                    'tier': prefs_row[3] or 'basic',
                }

            sig = PatternSignal(
                pattern_type=pattern_row['pattern_type'], ticker=pattern_row['ticker'],
                direction=pattern_row['direction'], zone_high=pattern_row['zone_high'],
                zone_low=pattern_row['zone_low'], zone_size_pct=0.0,
                timeframe=pattern_row['timeframe'], formed_at='',
                quality_score=pattern_row['quality_score'] or 0.0,
                status=pattern_row['status'],
                kb_conviction=pattern_row.get('kb_conviction', ''),
                kb_regime=pattern_row.get('kb_regime', ''),
                kb_signal_dir=pattern_row.get('kb_signal_dir', ''),
            )

            price_at_feedback = None
            price_at_generation = (pattern_row['zone_low'] + pattern_row['zone_high']) / 2.0
            try:
                _cp = sqlite3.connect(ext.DB_PATH, timeout=5)
                _pr = _cp.execute(
                    """SELECT object FROM facts
                       WHERE LOWER(subject)=? AND predicate='last_price'
                       ORDER BY created_at DESC LIMIT 1""",
                    (pattern_row['ticker'].lower(),),
                ).fetchone()
                _cp.close()
                if _pr:
                    price_at_feedback = float(_pr[0])
                    _zone_half = (pattern_row['zone_high'] - pattern_row['zone_low']) / 2.0
                    sig = PatternSignal(
                        pattern_type=pattern_row['pattern_type'], ticker=pattern_row['ticker'],
                        direction=pattern_row['direction'],
                        zone_high=price_at_feedback + _zone_half,
                        zone_low=price_at_feedback - _zone_half, zone_size_pct=0.0,
                        timeframe=pattern_row['timeframe'], formed_at='',
                        quality_score=pattern_row['quality_score'] or 0.0,
                        status=pattern_row['status'],
                        kb_conviction=pattern_row.get('kb_conviction', ''),
                        kb_regime=pattern_row.get('kb_regime', ''),
                        kb_signal_dir=pattern_row.get('kb_signal_dir', ''),
                    )
            except Exception:
                pass

            pos = calculate_position(sig, prefs) if prefs else None

            cash_result = None
            try:
                from users.user_store import deduct_from_cash
                _pos_value = getattr(pos, 'position_value', None) or (
                    (pos.position_size_units * (price_at_feedback or price_at_generation))
                    if pos and pos.position_size_units else 0.0
                )
                if _pos_value:
                    cash_result = deduct_from_cash(ext.DB_PATH, user_id, _pos_value, tip_id=tip_id)
            except Exception:
                pass

            followup = create_tip_followup(
                ext.DB_PATH, user_id=user_id, ticker=pattern_row['ticker'],
                tip_id=tip_id, pattern_id=pattern_row['id'],
                direction=pattern_row['direction'],
                entry_price=pos.suggested_entry if pos else pattern_row['zone_low'],
                stop_loss=pos.stop_loss if pos else None,
                target_1=pos.target_1 if pos else None,
                target_2=pos.target_2 if pos else None,
                target_3=pos.target_3 if pos else None,
                position_size=pos.position_size_units if pos else None,
                regime_at_entry=pattern_row.get('kb_regime'),
                conviction_at_entry=pattern_row.get('kb_conviction'),
                pattern_type=pattern_row.get('pattern_type'),
                timeframe=pattern_row.get('timeframe'),
                zone_low=pattern_row.get('zone_low'),
                zone_high=pattern_row.get('zone_high'),
            )

            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pattern_row['ticker'],
                               'user_action', 'opened_position', ext.DB_PATH)
                except Exception:
                    pass

            return jsonify({
                'action': 'taking_it', 'followup_id': followup['id'],
                'ticker': pattern_row['ticker'],
                'entry_price': pos.suggested_entry if pos else None,
                'stop_loss': pos.stop_loss if pos else None,
                'target_1': pos.target_1 if pos else None,
                'target_2': pos.target_2 if pos else None,
                'position_size': int(pos.position_size_units) if pos else None,
                'price_at_generation': round(price_at_generation, 4),
                'price_at_feedback': round(price_at_feedback, 4) if price_at_feedback else None,
                'cash_after': cash_result.get('new_balance') if cash_result else None,
                'cash_is_negative': cash_result.get('is_negative', False) if cash_result else False,
                'cash_deduction_skipped': cash_result.get('skipped', False) if cash_result else False,
                'message': (
                    f"{pattern_row['ticker']} added to monitoring — "
                    f"position monitor activated. You'll be alerted when action is needed."
                ),
            })

        # Path B: Tell me more
        if action == 'tell_me_more':
            return jsonify({
                'action': 'tell_me_more', 'tip_id': tip_id,
                'pattern': pattern_row,
                'message': 'Tip context loaded. Ask me anything about this setup.',
                'suggested_questions': [
                    'What is the risk if it breaks below the zone?',
                    'How has this pattern performed in the current regime?',
                    'Does this conflict with my existing positions?',
                ],
            })

        # Path C: Not for me
        if action == 'not_for_me':
            reason = str(data.get('rejection_reason', 'no_reason')).strip()
            _VALID_REASONS = {'too_risky', 'wrong_setup', 'wrong_timing',
                              'dont_know_stock', 'prefer_uk', 'no_reason'}
            if reason not in _VALID_REASONS:
                reason = 'no_reason'

            ext.log_tip_feedback(ext.DB_PATH, user_id, 'skipped',
                                 tip_id=tip_id, pattern_id=pattern_id)

            if ext.HAS_HYBRID and pattern_row:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pattern_row['ticker'],
                               'user_rejection_reason', reason, ext.DB_PATH)
                    ext.update_from_feedback(user_id,
                        {'pattern_type': pattern_row['pattern_type'],
                         'outcome': 'skipped', 'rejection_reason': reason}, ext.DB_PATH)
                except Exception:
                    pass

            return jsonify({
                'action': 'not_for_me', 'rejection_reason': reason,
                'recorded': True,
                'message': 'Thanks — this helps improve future tips for you.',
            })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/tips/<int:followup_id>/position-update', methods=['POST'])
@ext.limiter.exempt
def tip_position_update(followup_id: int):
    """
    POST /tips/<followup_id>/position-update
    Body: { "user_id": "alice", "action": "closed"|"hold_t2"|"partial"|"override",
            "exit_price": 923.40, "shares_closed": 6, "close_method": "hit_t1" }
    """
    from datetime import datetime, timezone
    from users.user_store import (
        get_user_followups, update_followup_status, ensure_tip_followups_table,
    )

    data = request.get_json(force=True, silent=True) or {}
    user_id = getattr(g, 'user_id', None) or str(data.get('user_id', '')).strip()
    action = str(data.get('action', '')).strip()

    if not user_id:
        return jsonify({'error': 'user_id required'}), 400
    if action not in ('closed', 'hold_t2', 'partial', 'override'):
        return jsonify({'error': 'action must be closed|hold_t2|partial|override'}), 400

    _c = sqlite3.connect(ext.DB_PATH, timeout=5)
    try:
        ensure_tip_followups_table(_c)
        row = _c.execute(
            """SELECT id, user_id, tip_id, pattern_id, ticker, direction,
                      entry_price, stop_loss, target_1, target_2, target_3,
                      position_size, tracking_target, status,
                      regime_at_entry, conviction_at_entry
               FROM tip_followups WHERE id=? AND user_id=?""",
            (followup_id, user_id),
        ).fetchone()
    finally:
        _c.close()

    if not row:
        return jsonify({'error': 'followup not found'}), 404
    cols = ['id', 'user_id', 'tip_id', 'pattern_id', 'ticker', 'direction',
            'entry_price', 'stop_loss', 'target_1', 'target_2', 'target_3',
            'position_size', 'tracking_target', 'status',
            'regime_at_entry', 'conviction_at_entry']
    pos = dict(zip(cols, row))

    try:
        if action == 'closed':
            exit_price = float(data.get('exit_price', pos['entry_price'] or 0))
            close_method = str(data.get('close_method', 'manual'))
            entry = pos['entry_price'] or exit_price
            position_size = pos['position_size'] or 1
            bullish = pos['direction'] != 'bearish'
            pnl_raw = (exit_price - entry) * position_size
            if not bullish:
                pnl_raw = -pnl_raw
            pnl_pct = ((exit_price - entry) / entry * 100) if entry else 0.0

            update_followup_status(ext.DB_PATH, followup_id, status='closed')

            if ext.HAS_PATTERN_LAYER and pos.get('pattern_id'):
                try:
                    from analytics.prediction_ledger import PredictionLedger
                    pl = PredictionLedger(ext.DB_PATH)
                    pl.on_price_written(pos['ticker'], exit_price)
                except Exception:
                    pass

            outcome_map = {
                'hit_t1': 'hit_t1', 'hit_t2': 'hit_t2', 'hit_t3': 'hit_t3',
                'stopped_out': 'stopped_out', 'manual': 'manual',
            }
            cal_outcome = outcome_map.get(close_method, 'manual')
            if ext.HAS_HYBRID and pos.get('pattern_id'):
                try:
                    _c2 = sqlite3.connect(ext.DB_PATH, timeout=5)
                    prow = _c2.execute(
                        "SELECT ticker, pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                        (pos['pattern_id'],),
                    ).fetchone()
                    _c2.close()
                    if prow:
                        ext.update_calibration(
                            ticker=prow[0], pattern_type=prow[1],
                            timeframe=prow[2], market_regime=prow[3] or None,
                            outcome=cal_outcome, db_path=ext.DB_PATH,
                        )
                        ext.update_from_feedback(user_id,
                            {'pattern_type': prow[1], 'outcome': cal_outcome}, ext.DB_PATH)
                except Exception:
                    pass

            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos['ticker'], 'trade_outcome', cal_outcome, ext.DB_PATH)
                    write_atom(user_id, pos['ticker'], 'realised_pnl_pct',
                               f'{pnl_pct:+.1f}%', ext.DB_PATH)
                except Exception:
                    pass

            ext.log_tip_feedback(ext.DB_PATH, user_id, cal_outcome,
                                 tip_id=pos.get('tip_id'), pattern_id=pos.get('pattern_id'))

            return jsonify({
                'action': 'closed', 'ticker': pos['ticker'],
                'exit_price': exit_price, 'entry_price': entry,
                'pnl_gbp': round(pnl_raw, 2), 'pnl_pct': round(pnl_pct, 2),
                'outcome': cal_outcome,
                'message': (
                    f"Trade closed — {pos['ticker']}: "
                    f"{'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%. Calibration updated."
                ),
            })

        elif action == 'hold_t2':
            new_stop = pos['entry_price']
            update_followup_status(ext.DB_PATH, followup_id,
                                   status='watching', tracking_target='T2',
                                   stop_loss=new_stop)
            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos['ticker'],
                               'user_position_intent', 'holding_for_t2', ext.DB_PATH)
                except Exception:
                    pass
            return jsonify({
                'action': 'hold_t2', 'ticker': pos['ticker'],
                'tracking_target': 'T2', 'new_stop': new_stop,
                'message': f"Stop moved to breakeven ({new_stop}) — risk-free position. Watching for T2.",
            })

        elif action == 'partial':
            shares_closed = float(data.get('shares_closed', 0))
            exit_price = float(data.get('exit_price', pos['entry_price'] or 0))
            orig_size = pos['position_size'] or 0
            remainder = max(0, orig_size - shares_closed)
            entry = pos['entry_price'] or exit_price
            partial_pnl = (exit_price - entry) * shares_closed

            _c3 = sqlite3.connect(ext.DB_PATH, timeout=5)
            try:
                ensure_tip_followups_table(_c3)
                _c3.execute(
                    "UPDATE tip_followups SET position_size=?, status='partial', updated_at=? WHERE id=?",
                    (remainder, datetime.now(timezone.utc).isoformat(), followup_id),
                )
                _c3.commit()
            finally:
                _c3.close()

            return jsonify({
                'action': 'partial', 'ticker': pos['ticker'],
                'shares_closed': shares_closed, 'remainder': remainder,
                'partial_pnl': round(partial_pnl, 2), 'exit_price': exit_price,
                'message': (
                    f"Partial exit recorded — {int(shares_closed)} shares closed at {exit_price}. "
                    f"{int(remainder)} shares remaining. Monitor continues."
                ),
            })

        elif action == 'override':
            update_followup_status(ext.DB_PATH, followup_id,
                                   status='watching', alert_level='OVERRIDE')
            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos['ticker'],
                               'user_override', 'held_past_stop_zone', ext.DB_PATH)
                except Exception:
                    pass
            return jsonify({
                'action': 'override', 'ticker': pos['ticker'],
                'message': 'Override noted — monitoring every 15 minutes. If stop is breached a CRITICAL alert will fire.',
            })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

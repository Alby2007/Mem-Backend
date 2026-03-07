"""routes/users.py — User management endpoints: onboarding, portfolio, prefs, watchlist, universe."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('users', __name__)


@bp.route('/users/<user_id>/onboarding', methods=['POST'])
@ext.require_auth
def user_onboarding(user_id: str):
    """POST /users/<user_id>/onboarding — submit onboarding data."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    if ext.HAS_VALIDATORS:
        result = ext.validate_onboarding(data)
        if not result.valid:
            return jsonify({'error': 'validation_failed', 'details': result.errors}), 400

    try:
        ext.update_preferences(ext.DB_PATH, user_id, data)
        if 'portfolio' in data:
            ext.upsert_portfolio(ext.DB_PATH, user_id, data['portfolio'])
        return jsonify({'ok': True, 'user_id': user_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/portfolio', methods=['GET'])
@ext.require_auth
def user_portfolio_get(user_id: str):
    """GET /users/<user_id>/portfolio — retrieve user's portfolio holdings."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    try:
        holdings = ext.get_portfolio(ext.DB_PATH, user_id)
        return jsonify({'holdings': holdings or [], 'count': len(holdings or [])})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/portfolio', methods=['POST'])
@ext.require_auth
def user_portfolio_update(user_id: str):
    """POST /users/<user_id>/portfolio — update portfolio holdings."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    holdings = data.get('portfolio') or data.get('holdings') or []

    if ext.HAS_VALIDATORS:
        result = ext.validate_portfolio_submission({'portfolio': holdings})
        if not result.valid:
            return jsonify({'error': 'validation_failed', 'details': result.errors}), 400

    try:
        ext.upsert_portfolio(ext.DB_PATH, user_id, holdings)
        # Infer user model from new portfolio
        if ext.HAS_HYBRID:
            try:
                ext.infer_and_write_from_portfolio(user_id, ext.DB_PATH)
            except Exception:
                pass
        return jsonify({'ok': True, 'count': len(holdings)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/tip-config', methods=['GET', 'POST'])
@ext.require_auth
def user_tip_config(user_id: str):
    """
    GET  /users/<user_id>/tip-config — return current tip configuration.
    POST /users/<user_id>/tip-config — update tip configuration.
    """
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    if request.method == 'GET':
        try:
            conn = sqlite3.connect(ext.DB_PATH, timeout=10)
            row = conn.execute(
                """SELECT user_id, tier, tip_delivery_time, tip_delivery_timezone,
                          tip_markets, tip_timeframes, tip_pattern_types,
                          account_size, max_risk_per_trade_pct, account_currency,
                          available_cash
                   FROM user_preferences WHERE user_id = ?""",
                (user_id,),
            ).fetchone()
            conn.close()
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        if row is None:
            return jsonify({'error': 'user not found'}), 404
        cols = ['user_id', 'tier', 'tip_delivery_time', 'tip_delivery_timezone',
                'tip_markets', 'tip_timeframes', 'tip_pattern_types',
                'account_size', 'max_risk_per_trade_pct', 'account_currency',
                'available_cash']
        d = dict(zip(cols, row))
        for jcol in ('tip_markets', 'tip_timeframes', 'tip_pattern_types'):
            try:
                d[jcol] = json.loads(d[jcol]) if d[jcol] else None
            except Exception:
                d[jcol] = None
        try:
            from users.user_store import get_available_cash as _gc
            _cd = _gc(ext.DB_PATH, user_id)
            d['cash_currency'] = _cd.get('cash_currency', 'GBP')
        except Exception:
            d['cash_currency'] = 'GBP'
        return jsonify(d)

    # POST
    data = request.get_json(force=True, silent=True) or {}
    if ext.HAS_VALIDATORS:
        result = ext.validate_tip_config(data)
        if not result.valid:
            return jsonify({'error': 'validation_failed', 'details': result.errors}), 400

    try:
        # Snapshot old prefs before update (for diff message)
        _old_prefs: dict = {}
        try:
            _oc = sqlite3.connect(ext.DB_PATH, timeout=5)
            _or = _oc.execute(
                """SELECT telegram_chat_id, tip_markets, tip_timeframes, tip_pattern_types,
                          tip_delivery_time, tip_delivery_timezone, tier
                   FROM user_preferences WHERE user_id=?""", (user_id,)
            ).fetchone()
            _oc.close()
            if _or:
                _cols = ['telegram_chat_id', 'tip_markets', 'tip_timeframes',
                         'tip_pattern_types', 'tip_delivery_time', 'tip_delivery_timezone', 'tier']
                _old_prefs = dict(zip(_cols, _or))
                for _jc in ('tip_markets', 'tip_timeframes', 'tip_pattern_types'):
                    try:
                        _old_prefs[_jc] = json.loads(_old_prefs[_jc]) if _old_prefs[_jc] else None
                    except Exception:
                        _old_prefs[_jc] = None
        except Exception:
            pass

        updated = ext.update_tip_config(
            ext.DB_PATH, user_id,
            tip_delivery_time=data.get('tip_delivery_time'),
            tip_delivery_timezone=data.get('tip_delivery_timezone'),
            tip_markets=data.get('tip_markets'),
            tip_timeframes=data.get('tip_timeframes'),
            tip_pattern_types=data.get('tip_pattern_types'),
            account_size=data.get('account_size'),
            max_risk_per_trade_pct=data.get('max_risk_per_trade_pct'),
            account_currency=data.get('account_currency'),
            tier=data.get('tier'),
        )

        # Send preference confirmation via Telegram (non-fatal)
        try:
            _chat_id = (_old_prefs.get('telegram_chat_id') or '').strip()
            if _chat_id:
                _msg = _build_prefs_confirmation(data, _old_prefs)
                if _msg:
                    from notifications.telegram_notifier import TelegramNotifier as _TGN
                    _TGN().send(_chat_id, _msg, parse_mode='MarkdownV2')
        except Exception:
            pass

        return jsonify(updated)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/tips', methods=['GET'])
@ext.require_auth
def user_tips(user_id: str):
    """GET /users/<user_id>/tips — tip delivery history."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    limit = int(request.args.get('limit', 50))
    try:
        tips = ext.get_tip_history(ext.DB_PATH, user_id, limit=limit)
        return jsonify({'tips': tips, 'count': len(tips)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/notification-prefs', methods=['PATCH'])
@ext.require_auth
def update_notification_prefs(user_id):
    """PATCH /users/<user_id>/notification-prefs — save notification toggle states."""
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}

    try:
        user = ext.get_user(ext.DB_PATH, user_id)
        tier = (user.get('tier') or 'basic').lower() if user else 'basic'
    except Exception:
        tier = 'basic'

    PRO_ONLY = {'profit_lock_alerts', 'trailing_alerts'}
    ALLOWED = {'monday_briefing', 'wednesday_update', 'zone_alerts',
               'thesis_alerts', 'profit_lock_alerts', 'trailing_alerts'}

    prefs = {}
    for key in ALLOWED:
        if key not in data:
            continue
        if key in PRO_ONLY and tier == 'basic':
            return jsonify({'error': f'{key} requires Pro or Premium tier'}), 403
        prefs[key] = bool(data[key])

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            conn.execute(
                "ALTER TABLE user_preferences ADD COLUMN notification_prefs TEXT DEFAULT '{}'"
            )
        except Exception:
            pass
        row = conn.execute(
            "SELECT notification_prefs FROM user_preferences WHERE user_id=?",
            (user_id,)
        ).fetchone()
        existing = {}
        if row and row[0]:
            try:
                existing = json.loads(row[0])
            except Exception:
                pass
        existing.update(prefs)
        conn.execute(
            "UPDATE user_preferences SET notification_prefs=? WHERE user_id=?",
            (json.dumps(existing), user_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'ok': True, 'prefs': existing})


@bp.route('/users/<user_id>/trading-prefs', methods=['PATCH'])
@ext.require_auth
def update_trading_prefs(user_id):
    """PATCH /users/<user_id>/trading-prefs — save trading preference fields."""
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}

    risk_pct  = data.get('max_risk_per_trade_pct')
    broker    = str(data.get('preferred_broker',  '') or '')[:100]
    exp_level = str(data.get('experience_level',  '') or '')[:100]
    bio       = str(data.get('trading_bio',       '') or '')[:1000]

    if risk_pct is not None:
        try:
            risk_pct = float(risk_pct)
            if not (0 < risk_pct <= 100):
                return jsonify({'error': 'max_risk_per_trade_pct must be between 0 and 100'}), 400
        except (TypeError, ValueError):
            return jsonify({'error': 'max_risk_per_trade_pct must be a number'}), 400

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        for col, default in [
            ('preferred_broker',  "TEXT DEFAULT ''"),
            ('experience_level',  "TEXT DEFAULT ''"),
            ('trading_bio',       "TEXT DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE user_preferences ADD COLUMN {col} {default}")
            except Exception:
                pass
        updates, params = [], []
        if risk_pct is not None:
            updates.append('max_risk_per_trade_pct=?'); params.append(risk_pct)
        if broker:
            updates.append('preferred_broker=?'); params.append(broker)
        if exp_level:
            updates.append('experience_level=?'); params.append(exp_level)
        if bio is not None:
            updates.append('trading_bio=?'); params.append(bio)
        if updates:
            params.append(user_id)
            conn.execute(
                f"UPDATE user_preferences SET {', '.join(updates)} WHERE user_id=?",
                params
            )
            conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    return jsonify({'ok': True})


@bp.route('/users/<user_id>', methods=['DELETE'])
@ext.require_auth
def delete_account(user_id):
    """DELETE /users/<user_id> — permanently delete user account."""
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        conn.execute("DELETE FROM user_auth WHERE user_id=?", (user_id,))
        try:
            conn.execute("DELETE FROM user_preferences WHERE user_id=?", (user_id,))
        except Exception:
            pass
        try:
            conn.execute("DELETE FROM refresh_tokens WHERE user_id=?", (user_id,))
        except Exception:
            pass
        conn.commit()
        conn.close()
        ext.log_audit_event(ext.DB_PATH, action='account_deleted', user_id=user_id,
                            ip_address=request.remote_addr,
                            user_agent=request.user_agent.string,
                            outcome='success')
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    resp = jsonify({'deleted': True})
    return resp


@bp.route('/users/<user_id>/watchlist/signals', methods=['GET'])
@ext.require_auth
def watchlist_signals(user_id: str):
    """GET /users/<user_id>/watchlist/signals — signal summary for portfolio tickers."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    try:
        tickers = ext.get_user_watchlist_tickers(ext.DB_PATH, user_id)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if not tickers:
        return jsonify({'signals': [], 'count': 0})

    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    try:
        placeholders = ','.join('?' for _ in tickers)
        rows = conn.execute(
            f"""SELECT UPPER(subject) as ticker, predicate, object
                FROM facts
                WHERE UPPER(subject) IN ({placeholders})
                  AND predicate IN ('conviction_tier','signal_quality','upside_pct','position_size_pct')
                ORDER BY UPPER(subject), predicate""",
            tickers,
        ).fetchall()
    finally:
        conn.close()

    by_ticker: dict = {t: {} for t in tickers}
    for ticker, pred, obj in rows:
        if ticker in by_ticker and pred not in by_ticker[ticker]:
            by_ticker[ticker][pred] = obj

    if ext.HAS_PATTERN_LAYER:
        for ticker in tickers:
            try:
                pats = ext.get_open_patterns(ext.DB_PATH, ticker=ticker, limit=100)
                by_ticker[ticker]['pattern_count'] = len(pats)
            except Exception:
                by_ticker[ticker]['pattern_count'] = 0

    if ext.HAS_ANALYTICS:
        try:
            tip_logs = {}
            c = sqlite3.connect(ext.DB_PATH, timeout=5)
            log_rows = c.execute(
                """SELECT ps.ticker, MAX(t.delivered_at)
                   FROM tip_delivery_log t
                   JOIN pattern_signals ps ON ps.id = t.pattern_signal_id
                   WHERE t.user_id = ? AND t.success = 1
                   GROUP BY ps.ticker""",
                (user_id,),
            ).fetchall()
            c.close()
            for t, dt in log_rows:
                tip_logs[t.upper()] = dt
        except Exception:
            tip_logs = {}

        for ticker in tickers:
            by_ticker[ticker]['last_tip_date'] = tip_logs.get(ticker)

    signals = [{'ticker': t, **v} for t, v in by_ticker.items()]
    return jsonify({'signals': signals, 'count': len(signals)})


@bp.route('/users/<user_id>/alerts/unread-count', methods=['GET'])
@ext.require_auth
def user_alerts_unread_count(user_id: str):
    """GET /users/<user_id>/alerts/unread-count — lightweight unseen alert count."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        if ext.HAS_PRODUCT_LAYER:
            tickers = ext.get_user_watchlist_tickers(ext.DB_PATH, user_id)
        else:
            tickers = []

        unseen = ext.get_alerts(ext.DB_PATH, unseen_only=True, limit=10000)
        if tickers:
            unseen = [a for a in unseen if a.get('ticker') in tickers]
        return jsonify({'count': len(unseen)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/alerts', methods=['GET'])
@ext.require_auth
def user_alerts(user_id: str):
    """GET /users/<user_id>/alerts — alerts scoped to user's portfolio."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        unseen_only = request.args.get('all', '').lower() != 'true'
        limit       = int(request.args.get('limit', 50))
    except (TypeError, ValueError):
        limit = 50

    try:
        if ext.HAS_PRODUCT_LAYER:
            tickers = ext.get_user_watchlist_tickers(ext.DB_PATH, user_id)
        else:
            tickers = []

        rows = ext.get_alerts(ext.DB_PATH, unseen_only=unseen_only, limit=10000)
        if tickers:
            rows = [a for a in rows if a.get('ticker') in tickers]
        rows = rows[:limit]
        return jsonify({'alerts': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/onboarding-status', methods=['GET'])
@ext.require_auth
def user_onboarding_status(user_id: str):
    """GET /users/<user_id>/onboarding-status — structured onboarding step status."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            """SELECT onboarding_complete, telegram_chat_id, tip_delivery_time,
                      tip_delivery_timezone, account_size, selected_sectors
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        portfolio_count = conn.execute(
            "SELECT COUNT(*) FROM user_portfolios WHERE user_id = ?",
            (user_id,),
        ).fetchone()[0]
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if row is None:
        return jsonify({'error': 'user not found'}), 404

    onboarding_complete, chat_id, tip_time, tip_tz, account_size, sectors = row

    telegram_connected = bool(chat_id and chat_id.strip())
    portfolio_submitted = portfolio_count > 0
    tip_config_set = bool(
        (tip_time and tip_time != '07:30') or
        (tip_tz and tip_tz != 'Europe/London')
    )
    account_size_set = account_size is not None and float(account_size or 0) > 0

    try:
        sector_list = json.loads(sectors or '[]')
    except Exception:
        sector_list = []
    preferences_set = len(sector_list) > 0

    all_complete = all([
        portfolio_submitted, telegram_connected,
        tip_config_set, account_size_set,
    ])

    return jsonify({
        'portfolio_submitted': portfolio_submitted,
        'telegram_connected':  telegram_connected,
        'tip_config_set':      tip_config_set,
        'account_size_set':    account_size_set,
        'preferences_set':     preferences_set,
        'complete':            all_complete,
    })


@bp.route('/users/<user_id>/telegram/verify', methods=['POST'])
@ext.require_auth
def user_telegram_verify(user_id: str):
    """POST /users/<user_id>/telegram/verify — send a test message."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    try:
        user = ext.get_user(ext.DB_PATH, user_id)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if user is None:
        return jsonify({'error': 'user not found'}), 404

    chat_id = (user.get('telegram_chat_id') or '').strip()
    if not chat_id:
        return jsonify({'error': 'no telegram_chat_id on record'}), 400

    try:
        notifier = ext.TelegramNotifier()
        sent = notifier.send_test(chat_id)
        return jsonify({'sent': sent})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/trader-level', methods=['POST'])
@ext.require_auth
def user_set_trader_level(user_id: str):
    """POST /users/<user_id>/trader-level — set experience level."""
    err = ext.assert_self(user_id)
    if err:
        return err
    body = request.get_json(force=True, silent=True) or {}
    level = (body.get('level') or '').strip().lower()
    _valid = {'beginner', 'developing', 'experienced', 'quant'}
    if level not in _valid:
        return jsonify({'error': f"Invalid level '{level}'. Must be one of: {sorted(_valid)}"}), 400
    try:
        from users.user_store import set_trader_level as _set_level
        _set_level(ext.DB_PATH, user_id, level)
        return jsonify({'trader_level': level})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/telegram', methods=['DELETE'])
@ext.require_auth
def user_telegram_delink(user_id: str):
    """DELETE /users/<user_id>/telegram — clear telegram_chat_id."""
    err = ext.assert_self(user_id)
    if err:
        return err
    try:
        _c = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            _c.execute(
                "UPDATE user_preferences SET telegram_chat_id = NULL WHERE user_id = ?",
                (user_id,)
            )
            _c.commit()
        finally:
            _c.close()
        ext.log_audit_event(ext.DB_PATH, action='telegram_delink', user_id=user_id,
                            ip_address=request.remote_addr,
                            user_agent=request.user_agent.string,
                            outcome='success')
        return jsonify({'delinked': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/performance', methods=['GET'])
@ext.require_auth
def user_performance(user_id: str):
    """GET /users/<user_id>/performance — tip performance summary."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503

    try:
        perf = ext.get_tip_performance(ext.DB_PATH, user_id)
        return jsonify(perf)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Hybrid Build (universe expansion, personal KB) ────────────────────────────

@bp.route('/users/<user_id>/expand-universe', methods=['POST'])
@ext.require_auth
def expand_universe(user_id: str):
    """POST /users/<user_id>/expand-universe — resolve interest → validate → bootstrap."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503

    data = request.get_json(force=True, silent=True) or {}
    description = str(data.get('description', '')).strip()
    market_type = str(data.get('market_type', 'equities')).strip()

    if len(description) < 3:
        return jsonify({'error': 'description must be at least 3 characters'}), 400
    if not market_type:
        return jsonify({'error': 'market_type is required'}), 400

    tier = 'basic'
    try:
        tier = ext.get_user_tier(ext.DB_PATH, user_id)
    except Exception:
        pass
    max_universe = 100 if tier == 'pro' else 20
    current_count = len(ext.DynamicWatchlistManager.get_user_tickers(user_id, ext.DB_PATH))
    if current_count >= max_universe:
        return jsonify({'error': f'universe limit reached ({max_universe} tickers for {tier} tier)'}), 400

    _MAX_TICKERS_PER_REQUEST = 20

    try:
        expansion = ext.resolve_interest(description, market_type, user_id, ext.DB_PATH)

        if expansion.error == 'llm_unavailable':
            return jsonify({'resolved_tickers': [], 'rejected_tickers': [],
                            'staging_tickers': [], 'causal_edges_seeded': 0,
                            'estimated_bootstrap_seconds': 0,
                            'error': 'llm_unavailable'}), 200

        all_candidates = expansion.tickers[:_MAX_TICKERS_PER_REQUEST]
        validation = ext.validate_tickers(all_candidates, market_region=market_type)

        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        try:
            ext.ensure_hybrid_tables(conn)
            cur = conn.execute(
                """INSERT INTO user_universe_expansions
                   (user_id, description, sector_label, tickers, etfs, keywords,
                    causal_edges, status, requested_at, activated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (user_id, description, expansion.sector_label,
                 json.dumps(validation.valid),
                 json.dumps(expansion.etfs),
                 json.dumps(expansion.keywords),
                 json.dumps(expansion.causal_relationships),
                 'active', now, now),
            )
            expansion_id = cur.lastrowid
            conn.commit()
        finally:
            conn.close()

        result = ext.DynamicWatchlistManager.add_tickers(
            validation.valid, user_id, ext.DB_PATH,
            sector_label=expansion.sector_label,
        )
        promoted = result['promoted']
        staged   = result['staged']

        edges_seeded = ext.seed_causal_edges(expansion.causal_relationships, ext.DB_PATH)

        for t in promoted:
            ext.bootstrap_ticker_async(t, ext.DB_PATH)

        ext.write_universe_atoms(user_id, validation.valid, description, ext.DB_PATH)
        est_seconds = ext.estimate_bootstrap_seconds(len(promoted), ext.DB_PATH)

        return jsonify({
            'expansion_id':               expansion_id,
            'resolved_tickers':           validation.valid,
            'rejected_tickers':           validation.rejected,
            'staging_tickers':            staged,
            'causal_edges_seeded':        edges_seeded,
            'estimated_bootstrap_seconds': est_seconds,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/universe', methods=['GET'])
@ext.require_auth
def get_user_universe(user_id: str):
    """GET /users/<user_id>/universe — current expanded watchlist + coverage tiers."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        tickers = ext.DynamicWatchlistManager.get_user_tickers(user_id, ext.DB_PATH)
        result = []
        for t in tickers:
            ct = ext.compute_coverage_tier(t, ext.DB_PATH)
            result.append({
                'ticker':       t,
                'coverage_tier': ct.tier if ct else 'unknown',
                'coverage_count': ct.coverage_count if ct else 0,
            })
        return jsonify({'tickers': result, 'count': len(result)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/universe/<ticker>', methods=['DELETE'])
@ext.require_auth
def remove_universe_ticker(user_id: str, ticker: str):
    """DELETE /users/<user_id>/universe/<ticker> — remove from personal universe."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        removed = ext.DynamicWatchlistManager.remove_ticker(ticker, user_id, ext.DB_PATH)
        if not removed:
            return jsonify({'error': 'ticker not found or not owned by this user'}), 404
        return jsonify({'removed': ticker.upper(), 'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/universe/bootstrap-status', methods=['GET'])
@ext.require_auth
def universe_bootstrap_status(user_id: str):
    """GET /users/<user_id>/universe/bootstrap-status."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        status = ext.DynamicWatchlistManager.get_bootstrap_status(user_id, ext.DB_PATH)
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/universe/staging', methods=['GET'])
@ext.require_auth
def user_universe_staging(user_id: str):
    """GET /users/<user_id>/universe/staging — staged tickers."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        rows = ext.get_staged_tickers(ext.DB_PATH, user_id=user_id)
        return jsonify({'staging': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/preferences/focus', methods=['POST'])
@ext.require_auth
def set_user_focus(user_id: str):
    """POST /users/<user_id>/preferences/focus — explicit preference overrides."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    try:
        from users.personal_kb import write_atom as pkb_write
        written = []
        if 'preferred_upside_min' in data:
            pkb_write(user_id, user_id, 'preferred_upside_min',
                      str(float(data['preferred_upside_min'])), 0.9, 'user_override', ext.DB_PATH)
            written.append('preferred_upside_min')
        if 'preferred_pattern' in data:
            pkb_write(user_id, user_id, 'preferred_pattern',
                      str(data['preferred_pattern']), 0.9, 'user_override', ext.DB_PATH)
            written.append('preferred_pattern')
        return jsonify({'updated': written, 'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/engagement', methods=['POST'])
@ext.require_auth
def log_user_engagement(user_id: str):
    """POST /users/<user_id>/engagement — log an engagement event."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    event_type = str(data.get('event_type', '')).strip()
    if not event_type:
        return jsonify({'error': 'event_type is required'}), 400
    try:
        ext.log_engagement_event(
            ext.DB_PATH, user_id, event_type,
            ticker=data.get('ticker'),
            pattern_type=data.get('pattern_type'),
            sector=data.get('sector'),
        )
        ext.update_from_engagement(user_id, ext.DB_PATH)
        return jsonify({'logged': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/kb-context', methods=['GET'])
@ext.require_auth
def user_kb_context(user_id: str):
    """GET /users/<user_id>/kb-context — personal KB atoms."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        from users.personal_kb import read_atoms as pkb_read
        atoms = pkb_read(user_id, ext.DB_PATH)
        return jsonify({'atoms': atoms, 'count': len(atoms)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/preferences/inferred', methods=['GET'])
@ext.require_auth
def user_inferred_preferences(user_id: str):
    """GET /users/<user_id>/preferences/inferred — system-inferred preferences."""
    err = ext.assert_self(user_id)
    if err:
        return err
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        ctx = ext.get_context_document(user_id, ext.DB_PATH)
        return jsonify({
            'sector_affinity':         ctx.sector_affinity,
            'risk_tolerance':          ctx.risk_tolerance,
            'holding_style':           ctx.holding_style,
            'portfolio_beta':          ctx.portfolio_beta,
            'preferred_pattern':       ctx.preferred_pattern,
            'avg_win_rate':            ctx.avg_win_rate,
            'high_engagement_sector':  ctx.high_engagement_sector,
            'low_engagement_sector':   ctx.low_engagement_sector,
            'preferred_upside_min':    ctx.preferred_upside_min,
            'active_universe':         ctx.active_universe,
            'pattern_hit_rates':       ctx.pattern_hit_rates,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Helper: tip-config Telegram confirmation ──────────────────────────────────

def _build_prefs_confirmation(new: dict, old: dict) -> str:
    """Build a MarkdownV2 Telegram confirmation message describing what changed."""
    from notifications.tip_formatter import _escape_mdv2, _PATTERN_LABELS, _TF_LABELS

    lines = []

    # Markets
    new_markets = new.get('tip_markets')
    old_markets = old.get('tip_markets')
    if new_markets != old_markets and 'tip_markets' in new:
        if not new_markets:
            lines.append('🌐 You\'ll now receive tips from *all available markets*\\.')
        else:
            tickers_str = ', '.join(_escape_mdv2(t) for t in new_markets[:10])
            suffix = f' \\+{len(new_markets) - 10} more' if len(new_markets) > 10 else ''
            if old_markets:
                added = [t for t in new_markets if t not in old_markets]
                removed = [t for t in old_markets if t not in new_markets]
                if added and removed:
                    a_str = ', '.join(_escape_mdv2(t) for t in added[:5])
                    r_str = ', '.join(_escape_mdv2(t) for t in removed[:5])
                    lines.append(f'📊 You\'ll now see *more {a_str}* tips and *fewer {r_str}* tips\\.')
                elif added:
                    a_str = ', '.join(_escape_mdv2(t) for t in added[:5])
                    lines.append(f'📊 Added to your watchlist: *{a_str}*\\.')
                elif removed:
                    r_str = ', '.join(_escape_mdv2(t) for t in removed[:5])
                    lines.append(f'📊 Removed from your watchlist: *{r_str}*\\.')
            else:
                lines.append(f'🎯 Your tips will now focus on: *{tickers_str}{suffix}*\\.')

    # Pattern types
    new_patterns = new.get('tip_pattern_types')
    old_patterns = old.get('tip_pattern_types')
    if new_patterns != old_patterns and 'tip_pattern_types' in new:
        if not new_patterns:
            lines.append('📐 Pattern filter cleared — you\'ll see *all pattern types*\\.')
        else:
            added_p = [p for p in new_patterns if not old_patterns or p not in old_patterns]
            removed_p = [p for p in (old_patterns or []) if p not in new_patterns]
            def _plabel(p):
                return _escape_mdv2(_PATTERN_LABELS.get(p, p.replace('_', ' ').title()))
            if added_p and removed_p:
                lines.append(
                    f'📐 You\'ll now see more *{", ".join(_plabel(p) for p in added_p)}* '
                    f'and fewer *{", ".join(_plabel(p) for p in removed_p)}* patterns\\.'
                )
            elif added_p:
                lines.append(f'📐 Added pattern types: *{", ".join(_plabel(p) for p in added_p)}*\\.')
            elif removed_p:
                lines.append(f'📐 Removed pattern types: *{", ".join(_plabel(p) for p in removed_p)}*\\.')

    # Timeframes
    new_tfs = new.get('tip_timeframes')
    old_tfs = old.get('tip_timeframes')
    if new_tfs != old_tfs and 'tip_timeframes' in new:
        if not new_tfs:
            lines.append('⏱ Timeframe filter cleared — you\'ll see *all timeframes*\\.')
        else:
            def _tflabel(tf):
                return _escape_mdv2(_TF_LABELS.get(tf, tf.upper()))
            added_tf = [tf for tf in new_tfs if not old_tfs or tf not in old_tfs]
            removed_tf = [tf for tf in (old_tfs or []) if tf not in new_tfs]
            if added_tf and removed_tf:
                lines.append(
                    f'⏱ More *{", ".join(_tflabel(t) for t in added_tf)}* tips, '
                    f'fewer *{", ".join(_tflabel(t) for t in removed_tf)}* tips\\.'
                )
            elif added_tf:
                lines.append(f'⏱ Added timeframes: *{", ".join(_tflabel(t) for t in added_tf)}*\\.')
            elif removed_tf:
                lines.append(f'⏱ Removed timeframes: *{", ".join(_tflabel(t) for t in removed_tf)}*\\.')

    # Delivery time / timezone
    new_time = new.get('tip_delivery_time')
    new_tz = new.get('tip_delivery_timezone')
    old_time = old.get('tip_delivery_time')
    old_tz = old.get('tip_delivery_timezone')
    time_changed = (new_time and new_time != old_time) or (new_tz and new_tz != old_tz)
    if time_changed:
        t = _escape_mdv2(new_time or old_time or '?')
        tz = _escape_mdv2(new_tz or old_tz or 'UTC')
        lines.append(f'🕐 Tips will now arrive at *{t}* \\({tz}\\)\\.')

    # Tier
    new_tier = new.get('tier')
    old_tier = old.get('tier')
    if new_tier and new_tier != old_tier:
        _TIER_DISPLAY = {
            'basic': 'Basic \\(Mon weekly batch\\)',
            'pro': 'Pro \\(Mon \\+ Wed batch\\)',
            'premium': 'Premium \\(daily tips\\)',
        }
        tier_label = _TIER_DISPLAY.get(new_tier, _escape_mdv2(new_tier.title()))
        lines.append(f'⭐ Tier updated to *{tier_label}*\\.')

    if not lines:
        return ''

    header = '✅ *Tip preferences updated\\!*\n'
    footer = '\n_Changes take effect from your next scheduled tip\\._'
    return header + '\n'.join(lines) + footer


# ── Additional user routes ────────────────────────────────────────────────────

@bp.route('/users/<user_id>/model', methods=['GET'])
@ext.require_auth
def user_model_get(user_id):
    """GET /users/<user_id>/model — derived user model."""
    err = ext.assert_self(user_id)
    if err: return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        model = ext.get_user_model(ext.DB_PATH, user_id)
        if model is None:
            return jsonify({'error': 'no model found — submit portfolio first'}), 404
        return jsonify(model)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/cash', methods=['GET', 'POST'])
@ext.require_auth
def user_cash(user_id: str):
    """GET/POST /users/<user_id>/cash — available cash balance."""
    err = ext.assert_self(user_id)
    if err: return err

    if request.method == 'GET':
        try:
            from users.user_store import get_available_cash
            result = get_available_cash(ext.DB_PATH, user_id)
            return jsonify(result)
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # POST
    data = request.get_json(force=True, silent=True) or {}
    if 'available_cash' not in data:
        return jsonify({'error': 'available_cash is required'}), 400
    raw = data.get('available_cash')
    cash_currency = str(data.get('cash_currency', 'GBP')).upper().strip() or 'GBP'
    if raw is None:
        try:
            _cc = sqlite3.connect(ext.DB_PATH, timeout=5)
            _cc.execute("UPDATE user_preferences SET available_cash = NULL WHERE user_id = ?", (user_id,))
            _cc.commit()
            _cc.close()
            return jsonify({'user_id': user_id, 'available_cash': None, 'cash_currency': cash_currency})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    try:
        amount = float(raw)
    except (TypeError, ValueError):
        return jsonify({'error': 'available_cash must be a number'}), 400
    try:
        from users.user_store import update_available_cash
        result = update_available_cash(ext.DB_PATH, user_id, amount, cash_currency=cash_currency)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/positions/open', methods=['GET'])
@ext.require_auth
def user_positions_open(user_id: str):
    """GET /users/<user_id>/positions/open — open (watching + active) followups."""
    err = ext.assert_self(user_id)
    if err: return err
    try:
        from users.user_store import get_user_open_positions
        positions = get_user_open_positions(ext.DB_PATH, user_id)
        return jsonify({'user_id': user_id, 'positions': positions, 'count': len(positions)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/positions/closed', methods=['GET'])
@ext.require_auth
def user_positions_closed(user_id: str):
    """GET /users/<user_id>/positions/closed — recently closed followups."""
    err = ext.assert_self(user_id)
    if err: return err
    try:
        from users.user_store import get_recently_closed_positions
        since = request.args.get('since', '')
        if not since:
            from datetime import timedelta
            since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        positions = get_recently_closed_positions(ext.DB_PATH, user_id, since)
        return jsonify({'user_id': user_id, 'positions': positions, 'count': len(positions), 'since': since})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/history/screenshot', methods=['POST'])
@ext.require_auth
def user_portfolio_screenshot(user_id: str):
    """POST /users/<user_id>/history/screenshot — extract holdings from broker screenshot."""
    err = ext.assert_self(user_id)
    if err: return err

    import base64 as _b64
    from llm.ollama_client import chat_vision, list_models, VISION_MODEL

    available_models = list_models()
    vision_available = any(VISION_MODEL.split(':')[0] in m for m in available_models)
    if not vision_available:
        return jsonify({
            'holdings': [], 'vision_available': False,
            'reason': 'vision_model_unavailable', 'available_models': available_models,
        }), 200

    if 'file' not in request.files:
        return jsonify({'error': 'file field required'}), 400

    f = request.files['file']
    if not f.content_type or not f.content_type.startswith('image/'):
        return jsonify({'error': 'file must be an image (image/png or image/jpeg)'}), 400

    image_bytes = f.read()
    if len(image_bytes) > 10 * 1024 * 1024:
        return jsonify({'error': 'image too large (max 10 MB)'}), 400

    image_b64 = _b64.b64encode(image_bytes).decode('utf-8')

    prompt = (
        "This is a screenshot of a stock brokerage portfolio page. "
        "Extract all stock holdings visible in the image. "
        "For each holding, identify: the ticker symbol, the quantity held, and the average cost/price per share if visible. "
        "LSE-listed UK stocks use a .L suffix (e.g. SHEL.L, BARC.L). "
        "Respond with ONLY valid JSON — no markdown, no explanation. "
        "Format: [{\"ticker\": \"SHEL.L\", \"quantity\": 10, \"avg_cost\": 27.50}, ...] "
        "If avg_cost is not visible, set it to null. "
        "If no holdings are visible, return []."
    )

    try:
        raw = chat_vision(image_b64, prompt, timeout=90)
        if not raw:
            return jsonify({'holdings': [], 'vision_available': True, 'reason': 'model_returned_empty'}), 200

        raw = raw.strip()
        if raw.startswith('```'):
            raw = '\n'.join(l for l in raw.split('\n') if not l.startswith('```'))

        holdings = json.loads(raw)
        if not isinstance(holdings, list):
            holdings = []

        clean = []
        for h in holdings:
            ticker = str(h.get('ticker') or '').strip().upper()
            if not ticker:
                continue
            try:
                qty = float(h.get('quantity') or 0)
            except (TypeError, ValueError):
                qty = 0.0
            avg_cost = h.get('avg_cost')
            try:
                avg_cost = float(avg_cost) if avg_cost is not None else None
            except (TypeError, ValueError):
                avg_cost = None
            clean.append({'ticker': ticker, 'quantity': qty, 'avg_cost': avg_cost})

        return jsonify({'holdings': clean, 'vision_available': True, 'count': len(clean)})

    except Exception as e:
        return jsonify({'error': str(e), 'holdings': [], 'vision_available': True}), 500


@bp.route('/users/<user_id>/snapshot/preview', methods=['GET'])
@ext.require_auth
def user_snapshot_preview(user_id):
    """GET /users/<user_id>/snapshot/preview — personalised snapshot as JSON."""
    err = ext.assert_self(user_id)
    if err: return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        snapshot = ext.curate_snapshot(user_id, ext.DB_PATH)
        return jsonify(ext.snapshot_to_dict(snapshot))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/snapshot/send-now', methods=['POST'])
@ext.require_auth
@ext.rate_limit('snapshot')
def user_snapshot_send_now(user_id):
    """POST /users/<user_id>/snapshot/send-now — trigger immediate Telegram delivery."""
    err = ext.assert_self(user_id)
    if err: return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    try:
        from users.user_store import log_delivery
        user = ext.get_user(ext.DB_PATH, user_id)
        chat_id = (user or {}).get('telegram_chat_id')
        if not chat_id:
            return jsonify({'error': 'no telegram_chat_id — complete onboarding first'}), 400
        snapshot = ext.curate_snapshot(user_id, ext.DB_PATH)
        message = ext.format_snapshot(snapshot)
        notifier = ext.TelegramNotifier()
        sent = notifier.send(chat_id, message)
        local_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        log_delivery(
            ext.DB_PATH, user_id, success=sent, message_length=len(message),
            regime_at_delivery=snapshot.market_regime,
            opportunities_count=len(snapshot.top_opportunities),
            local_date=local_date,
        )
        return jsonify({
            'sent': sent, 'opportunities': len(snapshot.top_opportunities),
            'regime': snapshot.market_regime,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/delivery-history', methods=['GET'])
@ext.require_auth
def user_delivery_history(user_id):
    """GET /users/<user_id>/delivery-history — past delivery log entries."""
    err = ext.assert_self(user_id)
    if err: return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    limit = int(request.args.get('limit', 30))
    try:
        history = ext.get_delivery_history(ext.DB_PATH, user_id, limit=limit)
        return jsonify({'user_id': user_id, 'history': history, 'count': len(history)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/tip/preview', methods=['GET'])
@ext.require_auth
def tip_preview(user_id: str):
    """GET /users/<user_id>/tip/preview — preview tip without sending."""
    err = ext.assert_self(user_id)
    if err: return err
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        row = conn.execute(
            """SELECT tier, tip_timeframes, tip_pattern_types, tip_markets,
                      account_size, max_risk_per_trade_pct, account_currency
               FROM user_preferences WHERE user_id = ?""",
            (user_id,),
        ).fetchone()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if row is None:
        return jsonify({'error': 'user not found'}), 404

    cols = ['tier', 'tip_timeframes', 'tip_pattern_types', 'tip_markets',
            'account_size', 'max_risk_per_trade_pct', 'account_currency']
    prefs = dict(zip(cols, row))
    tier = prefs.get('tier') or 'basic'

    for jcol in ('tip_timeframes', 'tip_pattern_types', 'tip_markets'):
        try:
            prefs[jcol] = json.loads(prefs[jcol]) if prefs[jcol] else None
        except Exception:
            prefs[jcol] = None

    from core.tiers import TIER_CONFIG as TIER_LIMITS
    limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
    tip_timeframes = prefs.get('tip_timeframes') or limits['timeframes']
    tip_pattern_tys = prefs.get('tip_pattern_types')
    tip_markets = prefs.get('tip_markets')
    delivery_days = limits.get('delivery_days', 'daily')
    is_weekly = delivery_days != 'daily'

    from analytics.pattern_detector import PatternSignal

    if is_weekly:
        from notifications.tip_scheduler import _pick_batch
        batch_size = limits.get('batch_size', 3)
        batch, tip_source = _pick_batch(
            ext.DB_PATH, user_id, tier, tip_timeframes, tip_pattern_tys, tip_markets, batch_size
        )
        if not batch:
            return jsonify({'tip': None, 'tips': [], 'reason': 'no eligible patterns',
                            'cadence': 'weekly', 'tip_source': None}), 200
        tips = []
        for row in batch:
            sig = PatternSignal(
                pattern_type=row['pattern_type'], ticker=row['ticker'],
                direction=row['direction'], zone_high=row['zone_high'],
                zone_low=row['zone_low'], zone_size_pct=row['zone_size_pct'],
                timeframe=row['timeframe'], formed_at=row['formed_at'],
                quality_score=row['quality_score'] or 0.0, status=row['status'],
                kb_conviction=row.get('kb_conviction', ''),
                kb_regime=row.get('kb_regime', ''),
                kb_signal_dir=row.get('kb_signal_dir', ''),
            )
            pos = ext.calculate_position(sig, prefs)
            tips.append(ext.tip_to_dict(sig, pos, tier=tier))
        return jsonify({
            'tip': tips[0] if tips else None, 'tips': tips,
            'tip_source': tip_source, 'cadence': 'weekly',
            'delivery_days': delivery_days,
        })

    # Premium — single daily tip
    from notifications.tip_scheduler import _pick_best_pattern
    pattern_row = _pick_best_pattern(ext.DB_PATH, user_id, tier, tip_timeframes, tip_pattern_tys, tip_markets)
    if pattern_row is None:
        return jsonify({'tip': None, 'tips': [], 'reason': 'no eligible patterns',
                        'cadence': 'daily', 'tip_source': None}), 200
    sig = PatternSignal(
        pattern_type=pattern_row['pattern_type'], ticker=pattern_row['ticker'],
        direction=pattern_row['direction'], zone_high=pattern_row['zone_high'],
        zone_low=pattern_row['zone_low'], zone_size_pct=pattern_row['zone_size_pct'],
        timeframe=pattern_row['timeframe'], formed_at=pattern_row['formed_at'],
        quality_score=pattern_row['quality_score'] or 0.0, status=pattern_row['status'],
        kb_conviction=pattern_row.get('kb_conviction', ''),
        kb_regime=pattern_row.get('kb_regime', ''),
        kb_signal_dir=pattern_row.get('kb_signal_dir', ''),
    )
    position = ext.calculate_position(sig, prefs)
    tip_dict = ext.tip_to_dict(sig, position, tier=tier)
    tip_source = pattern_row.get('tip_source')
    return jsonify({'tip': tip_dict, 'tips': [tip_dict], 'tip_source': tip_source, 'cadence': 'daily'})


@bp.route('/users/<user_id>/tip/history', methods=['GET'])
@ext.require_auth
def tip_history(user_id: str):
    """GET /users/<user_id>/tip/history — recent tip delivery log."""
    err = ext.assert_self(user_id)
    if err: return err
    if not ext.HAS_PATTERN_LAYER:
        return jsonify({'error': 'pattern layer not available'}), 503
    try:
        limit = int(request.args.get('limit', 30))
    except (ValueError, TypeError):
        limit = 30
    try:
        history = ext.get_tip_history(ext.DB_PATH, user_id, limit=limit)
        return jsonify({'history': history, 'count': len(history)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/users/<user_id>/profile', methods=['PATCH'])
@ext.require_auth
def update_user_profile(user_id):
    """PATCH /users/<user_id>/profile — update first_name, last_name, phone."""
    if g.user_id != user_id:
        return jsonify({'error': 'forbidden'}), 403
    data = request.get_json(force=True, silent=True) or {}
    first_name = str(data.get('first_name', '') or '').strip()[:100]
    last_name = str(data.get('last_name', '') or '').strip()[:100]
    phone = str(data.get('phone', '') or '').strip()[:30]
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        for col in ('first_name', 'last_name', 'phone'):
            try:
                conn.execute(f"ALTER TABLE user_auth ADD COLUMN {col} TEXT DEFAULT ''")
            except Exception:
                pass
        conn.execute(
            "UPDATE user_auth SET first_name=?, last_name=?, phone=? WHERE user_id=?",
            (first_name, last_name, phone, user_id),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'ok': True, 'first_name': first_name, 'last_name': last_name, 'phone': phone})


@bp.route('/users/<user_id>/portfolio/generate-sim', methods=['POST'])
@ext.require_auth
def user_portfolio_generate_sim(user_id):
    """POST /users/<user_id>/portfolio/generate-sim — generate seeded test portfolio."""
    err = ext.assert_self(user_id)
    if err: return err
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503

    import hashlib as _hashlib
    import random as _random

    _ARCHETYPES = [
        {
            'key': 'conservative_income', 'title': 'Conservative Income Trader',
            'description': 'Focuses on FTSE defensive names and dividend payers.',
            'tips_alignment': 'Tips favour low-risk setups: mitigation blocks and IFVG patterns.',
            'risk_tolerance': 'conservative', 'holding_style': 'value',
            'sectors': ['utilities', 'consumer_staples', 'healthcare', 'financials'],
            'holdings': [
                {'ticker': 'ULVR.L', 'quantity': 120, 'avg_cost': 3820.0, 'sector': 'consumer_staples'},
                {'ticker': 'NG.L', 'quantity': 400, 'avg_cost': 1042.0, 'sector': 'utilities'},
                {'ticker': 'TSCO.L', 'quantity': 350, 'avg_cost': 295.0, 'sector': 'consumer_staples'},
                {'ticker': 'GSK.L', 'quantity': 180, 'avg_cost': 1685.0, 'sector': 'healthcare'},
                {'ticker': 'BATS.L', 'quantity': 160, 'avg_cost': 2460.0, 'sector': 'consumer_staples'},
                {'ticker': 'NWG.L', 'quantity': 900, 'avg_cost': 285.0, 'sector': 'financials'},
            ],
        },
        {
            'key': 'ftse_momentum', 'title': 'FTSE Momentum Trader',
            'description': 'Chases high-conviction breakouts in FTSE growth names.',
            'tips_alignment': 'Tips favour momentum breakouts: FVG and order block patterns.',
            'risk_tolerance': 'moderate', 'holding_style': 'momentum',
            'sectors': ['technology', 'industrials', 'healthcare', 'financials'],
            'holdings': [
                {'ticker': 'AZN.L', 'quantity': 80, 'avg_cost': 11200.0, 'sector': 'healthcare'},
                {'ticker': 'LSEG.L', 'quantity': 100, 'avg_cost': 9850.0, 'sector': 'financials'},
                {'ticker': 'RR.L', 'quantity': 600, 'avg_cost': 415.0, 'sector': 'industrials'},
                {'ticker': 'BA.L', 'quantity': 250, 'avg_cost': 1295.0, 'sector': 'industrials'},
                {'ticker': 'AUTO.L', 'quantity': 200, 'avg_cost': 630.0, 'sector': 'technology'},
                {'ticker': 'SAGE.L', 'quantity': 220, 'avg_cost': 1105.0, 'sector': 'technology'},
            ],
        },
        {
            'key': 'energy_commodities', 'title': 'Commodities & Energy Trader',
            'description': 'Concentrated in FTSE energy and mining.',
            'tips_alignment': 'Tips favour commodity cycle plays.',
            'risk_tolerance': 'aggressive', 'holding_style': 'mixed',
            'sectors': ['energy', 'materials', 'mining'],
            'holdings': [
                {'ticker': 'SHEL.L', 'quantity': 200, 'avg_cost': 2680.0, 'sector': 'energy'},
                {'ticker': 'BP.L', 'quantity': 500, 'avg_cost': 445.0, 'sector': 'energy'},
                {'ticker': 'RIO.L', 'quantity': 120, 'avg_cost': 4950.0, 'sector': 'materials'},
                {'ticker': 'GLEN.L', 'quantity': 800, 'avg_cost': 420.0, 'sector': 'materials'},
                {'ticker': 'AAL.L', 'quantity': 450, 'avg_cost': 225.0, 'sector': 'materials'},
                {'ticker': 'BHP.L', 'quantity': 150, 'avg_cost': 2150.0, 'sector': 'materials'},
            ],
        },
        {
            'key': 'financials_heavy', 'title': 'Financials-Concentrated Trader',
            'description': 'Heavily weighted to UK banks and financial infrastructure.',
            'tips_alignment': 'Tips lean financials-sector.',
            'risk_tolerance': 'moderate', 'holding_style': 'value',
            'sectors': ['financials'],
            'holdings': [
                {'ticker': 'HSBA.L', 'quantity': 700, 'avg_cost': 680.0, 'sector': 'financials'},
                {'ticker': 'BARC.L', 'quantity': 900, 'avg_cost': 205.0, 'sector': 'financials'},
                {'ticker': 'LLOY.L', 'quantity': 2500, 'avg_cost': 52.0, 'sector': 'financials'},
                {'ticker': 'STAN.L', 'quantity': 400, 'avg_cost': 690.0, 'sector': 'financials'},
                {'ticker': 'NWG.L', 'quantity': 800, 'avg_cost': 285.0, 'sector': 'financials'},
                {'ticker': 'LSEG.L', 'quantity': 90, 'avg_cost': 9850.0, 'sector': 'financials'},
            ],
        },
        {
            'key': 'high_conviction_growth', 'title': 'High-Conviction Growth Trader',
            'description': 'Concentrated in FTSE tech and pharma growth names.',
            'tips_alignment': 'Tips favour aggressive growth plays.',
            'risk_tolerance': 'aggressive', 'holding_style': 'momentum',
            'sectors': ['technology', 'healthcare'],
            'holdings': [
                {'ticker': 'AZN.L', 'quantity': 100, 'avg_cost': 11200.0, 'sector': 'healthcare'},
                {'ticker': 'SAGE.L', 'quantity': 300, 'avg_cost': 1105.0, 'sector': 'technology'},
                {'ticker': 'LSEG.L', 'quantity': 120, 'avg_cost': 9850.0, 'sector': 'technology'},
                {'ticker': 'RR.L', 'quantity': 500, 'avg_cost': 415.0, 'sector': 'industrials'},
                {'ticker': 'AUTO.L', 'quantity': 280, 'avg_cost': 630.0, 'sector': 'technology'},
                {'ticker': 'HIK.L', 'quantity': 200, 'avg_cost': 1820.0, 'sector': 'healthcare'},
            ],
        },
    ]

    seed_int = int(_hashlib.md5(user_id.encode()).hexdigest(), 16)
    archetype = _ARCHETYPES[seed_int % len(_ARCHETYPES)]

    rng = _random.Random(seed_int)
    holdings = []
    for h in archetype['holdings']:
        jitter = 1.0 + rng.uniform(-0.10, 0.10)
        holdings.append({
            'ticker': h['ticker'], 'quantity': h['quantity'],
            'avg_cost': round(h['avg_cost'] * jitter, 2), 'sector': h['sector'],
        })

    try:
        result = ext.upsert_portfolio(ext.DB_PATH, user_id, holdings)
        model = ext.build_user_model(user_id, ext.DB_PATH)
        result['model'] = model
        if ext.HAS_HYBRID:
            try:
                ext.infer_and_write_from_portfolio(user_id, ext.DB_PATH)
            except Exception:
                pass
        return jsonify({
            'simulated': True, 'archetype': archetype['key'],
            'title': archetype['title'], 'description': archetype['description'],
            'tips_alignment': archetype['tips_alignment'],
            'risk_tolerance': archetype['risk_tolerance'],
            'holding_style': archetype['holding_style'],
            'sectors': archetype['sectors'],
            'holdings': holdings, 'count': len(holdings), 'model': model,
        }), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/notify/test', methods=['POST'])
def notify_test():
    """POST /notify/test — send a test Telegram message."""
    if not ext.HAS_PRODUCT_LAYER:
        return jsonify({'error': 'product layer not available'}), 503
    data = request.get_json(force=True, silent=True) or {}
    chat_id = str(data.get('chat_id', '')).strip()
    if not chat_id:
        return jsonify({'error': 'chat_id is required'}), 400
    try:
        notifier = ext.TelegramNotifier()
        sent = notifier.send_test(chat_id)
        return jsonify({'sent': sent})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

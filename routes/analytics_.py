"""routes/analytics_.py — Analytics endpoints: alerts, snapshots, backtest, stress, counterfactual, portfolio."""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request

import extensions as ext

bp = Blueprint('analytics', __name__)
_logger = logging.getLogger(__name__)


@bp.route('/alerts', methods=['GET'])
def alerts_list():
    """
    List alerts.

    Query params:
      all    — if 'true', return all alerts (default: unseen only)
      since  — ISO-8601 datetime, only return alerts triggered after this
      limit  — max rows (default 200)

    Returns:
      { "alerts": [...], "count": N }
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        unseen_only = request.args.get('all', '').lower() != 'true'
        since_iso   = request.args.get('since') or None
        limit       = int(request.args.get('limit', 200))
        rows = ext.get_alerts(ext.DB_PATH, unseen_only=unseen_only,
                              since_iso=since_iso, limit=limit)
        return jsonify({'alerts': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/alerts/mark-seen', methods=['POST'])
def alerts_mark_seen():
    """
    Mark alerts as seen.

    Body: { "ids": [1, 2, 3] }

    Returns:
      { "updated": N }
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        body = request.get_json(force=True) or {}
        ids  = [int(i) for i in body.get('ids', [])]
        updated = ext.mark_alerts_seen(ext.DB_PATH, ids)
        return jsonify({'updated': updated})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/analytics/snapshot', methods=['POST'])
def analytics_snapshot():
    """
    Capture the current KB conviction state into the signal_snapshots table.
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        result   = ext.take_snapshot(ext.DB_PATH)
        snaps    = ext.list_snapshots(ext.DB_PATH)
        result['snapshot_count'] = len(snaps)
        result['snapshots']      = snaps
        return jsonify(result)
    except Exception as e:
        _logger.error('snapshot failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/analytics/snapshot', methods=['GET'])
def analytics_snapshot_list():
    """
    List all recorded signal snapshot dates.
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        snaps = ext.list_snapshots(ext.DB_PATH)
        return jsonify({'snapshot_count': len(snaps), 'snapshots': snaps})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/analytics/backtest', methods=['GET'])
def analytics_backtest():
    """
    Cross-sectional KB backtest — measures whether high-conviction names
    show better trailing returns than low-conviction names.

    Query params:
      window  — return window: '1w', '1m' (default), '3m'
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    window = request.args.get('window', '1m')
    if window not in ('1w', '1m', '3m'):
        return jsonify({'error': "window must be '1w', '1m', or '3m'"}), 400

    try:
        result = ext.run_backtest(ext.DB_PATH, window=window)
        return jsonify(result)
    except Exception as e:
        _logger.error('backtest failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/analytics/backtest/regime', methods=['GET'])
def analytics_backtest_regime():
    """
    Regime-conditional cross-sectional KB backtest.
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        result = ext.run_regime_backtest(ext.DB_PATH)
        return jsonify(result)
    except Exception as e:
        _logger.error('regime backtest failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/analytics/stress-test', methods=['POST'])
def analytics_stress_test():
    """
    Adversarial signal stress test.
    Body (all optional): { "scenarios": ["bear_analyst", "risk_off_regime"] }
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    body      = request.get_json(force=True) or {}
    scenarios = body.get('scenarios') or None

    try:
        result = ext.run_stress_test(ext.DB_PATH, scenarios=scenarios)
        return jsonify(result)
    except Exception as e:
        _logger.error('stress test failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/analytics/counterfactual', methods=['POST'])
def analytics_counterfactual():
    """
    Counterfactual reasoning — "what if X changed?"
    Body: { "scenario": { ... } }
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    body     = request.get_json(force=True) or {}
    scenario = body.get('scenario') or {}

    if not scenario:
        return jsonify({'error': 'scenario is required and must not be empty'}), 400

    try:
        result = ext.run_counterfactual(ext.DB_PATH, scenario=scenario)
        return jsonify(result)
    except Exception as e:
        _logger.error('counterfactual failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/portfolio/summary', methods=['GET'])
@ext.limiter.exempt
def portfolio_summary():
    """
    Aggregated portfolio view from current KB signal atoms.
    """
    if not ext.HAS_ANALYTICS:
        return jsonify({'error': 'analytics module not available'}), 503

    try:
        result = ext.build_portfolio_summary(ext.DB_PATH)
        return jsonify(result)
    except Exception as e:
        _logger.error('portfolio summary failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/ledger/performance', methods=['GET'])
@ext.limiter.exempt
def ledger_performance():
    """
    GET /ledger/performance — system's prediction accuracy record.
    """
    if ext.prediction_ledger is None:
        return jsonify({'error': 'prediction_ledger_not_initialised'}), 503
    try:
        report = ext.prediction_ledger.get_performance_report()
        return jsonify(report)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/ledger/open', methods=['GET'])
@ext.require_auth
def ledger_open():
    """
    GET /ledger/open — all open (unresolved) prediction ledger entries.
    """
    if ext.prediction_ledger is None:
        return jsonify({'error': 'prediction_ledger_not_initialised'}), 503
    try:
        predictions = ext.prediction_ledger.get_open_predictions()
        return jsonify({'predictions': predictions, 'count': len(predictions)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/forecast/<ticker>/<pattern_type>', methods=['GET'])
@ext.require_auth
def forecast_signal(ticker: str, pattern_type: str):
    """
    GET /forecast/<ticker>/<pattern_type>?timeframe=1d&account_size=10000&risk_pct=1.0
    """
    try:
        from analytics.signal_forecaster import SignalForecaster
        timeframe    = request.args.get('timeframe', '1d')
        account_size = float(request.args.get('account_size', 10000))
        risk_pct     = float(request.args.get('risk_pct', 1.0))
        forecaster   = SignalForecaster(ext.DB_PATH)
        result       = forecaster.forecast(
            ticker       = ticker,
            pattern_type = pattern_type,
            timeframe    = timeframe,
            account_size = account_size,
            risk_pct     = risk_pct,
            seed         = None,
        )
        return jsonify({
            'ticker':                result.ticker,
            'pattern_type':          result.pattern_type,
            'timeframe':             result.timeframe,
            'market_regime':         result.market_regime,
            'p_hit_t1':              result.p_hit_t1,
            'p_hit_t2':              result.p_hit_t2,
            'p_stopped_out':         result.p_stopped_out,
            'p_expired':             result.p_expired,
            'expected_value_gbp':    result.expected_value_gbp,
            'ci_90_low':             result.ci_90_low,
            'ci_90_high':            result.ci_90_high,
            'days_to_target_median': result.days_to_target_median,
            'regime_adjustment_pct': result.regime_adjustment_pct,
            'iv_adjustment_pct':     result.iv_adjustment_pct,
            'macro_adjustment_pct':  result.macro_adjustment_pct,
            'short_adjustment_pct':  result.short_adjustment_pct,
            'calibration_samples':   result.calibration_samples,
            'used_prior':            result.used_prior,
            'generated_at':          result.generated_at,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/causal/shocks', methods=['GET'])
@ext.require_auth
def causal_shocks():
    """
    GET /causal/shocks?n=50 — recent causal shock propagation events.
    """
    if ext.shock_engine is None:
        return jsonify({'shocks': [], 'note': 'shock_engine_not_initialised'})
    try:
        n      = min(int(request.args.get('n', 50)), 200)
        shocks = ext.shock_engine.get_recent_shocks(n=n)
        return jsonify({'shocks': shocks, 'count': len(shocks)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/signals/stress-test', methods=['POST'])
@ext.require_auth
def signals_stress_test():
    """
    POST /signals/stress-test
    Body: { "ticker": "HSBA.L", "pattern_id": 42 }
    """
    try:
        from analytics.adversarial_tester import AdversarialTester
        data       = request.get_json(silent=True) or {}
        ticker     = data.get('ticker', '')
        pattern_id = data.get('pattern_id')
        if not ticker:
            return jsonify({'error': 'ticker required'}), 400

        patterns = ext.get_open_patterns(ext.DB_PATH, min_quality=0.0, limit=500)
        pattern  = None
        for p in patterns:
            if p['ticker'].upper() == ticker.upper():
                if pattern_id is None or p['id'] == pattern_id:
                    pattern = p
                    break
        if pattern is None:
            return jsonify({'error': 'no open pattern found for ticker'}), 404

        tester = AdversarialTester(ext.DB_PATH)
        result = tester.stress_test_signal(ticker, pattern)
        return jsonify({
            'ticker':                 ticker.upper(),
            'pattern_type':           pattern.get('pattern_type'),
            'survival_rate':          result.survival_rate,
            'robustness_label':       result.robustness_label,
            'invalidating_scenarios': result.invalidating_scenarios,
            'earnings_warning':       result.earnings_proximity_warning,
            'scenarios_tested':       result.scenarios_tested,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

"""routes/network.py — Network effect endpoints: health, calibration, cohort, convergence."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

import extensions as ext

bp = Blueprint('network', __name__)


@bp.route('/network/health', methods=['GET'])
def network_health():
    """GET /network/health — flywheel velocity, coverage distribution."""
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        report = ext.compute_network_health(ext.DB_PATH)
        return jsonify({
            'total_tickers':          report.total_tickers,
            'total_users':            report.total_users,
            'tickers_by_tier':        report.tickers_by_tier,
            'coverage_distribution':  report.coverage_distribution,
            'flywheel_velocity':      report.flywheel_velocity,
            'cohort_signals_active':  report.cohort_signals_active,
            'generated_at':           report.generated_at,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/network/calibration/<ticker>', methods=['GET'])
def network_calibration(ticker: str):
    """GET /network/calibration/<ticker> — collective hit rates for ticker."""
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    pattern_type = request.args.get('pattern_type', 'fvg')
    timeframe    = request.args.get('timeframe', '1h')
    try:
        cal = ext.get_calibration(ticker, pattern_type, timeframe, ext.DB_PATH)
        if cal is None:
            return jsonify({'calibration': None,
                            'reason': 'insufficient_samples (< 10)'}), 200
        return jsonify({
            'ticker':                   cal.ticker,
            'pattern_type':             cal.pattern_type,
            'timeframe':                cal.timeframe,
            'market_regime':            cal.market_regime,
            'sample_size':              cal.sample_size,
            'hit_rate_t1':              cal.hit_rate_t1,
            'hit_rate_t2':              cal.hit_rate_t2,
            'hit_rate_t3':              cal.hit_rate_t3,
            'stopped_out_rate':         cal.stopped_out_rate,
            'calibration_confidence':   cal.calibration_confidence,
            'confidence_label':         cal.confidence_label,
            'last_updated':             cal.last_updated,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/network/cohort/<ticker>', methods=['GET'])
def network_cohort(ticker: str):
    """GET /network/cohort/<ticker> — cohort consensus + stop cluster."""
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        signal = ext.detect_cohort_consensus(ticker, ext.DB_PATH)
        if signal is None:
            return jsonify({'cohort_signal': None,
                            'reason': 'insufficient_cohort (< 10 users)'}), 200
        return jsonify({
            'ticker':               signal.ticker,
            'cohort_size':          signal.cohort_size,
            'consensus_direction':  signal.consensus_direction,
            'consensus_strength':   signal.consensus_strength,
            'stop_cluster':         signal.stop_cluster,
            'contrarian_flag':      signal.contrarian_flag,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/network/convergence', methods=['GET'])
@ext.require_auth
def network_convergence():
    """
    GET /network/convergence?lookback_hours=24

    Tickers where >= 3 independent users have queried organically.
    """
    try:
        from analytics.network_effect_engine import NetworkEffectEngine
        lookback = int(request.args.get('lookback_hours', 24))
        engine   = NetworkEffectEngine(ext.DB_PATH)
        signals  = engine.detect_convergence(lookback_hours=lookback)
        return jsonify({
            'convergence_signals': [
                {
                    'ticker':          s.ticker,
                    'distinct_users':  s.distinct_users,
                    'lookback_hours':  s.lookback_hours,
                    'kb_signal':       s.kb_signal_direction,
                    'organic':         s.is_organic,
                    'detected_at':     s.detected_at,
                }
                for s in signals
            ],
            'count': len(signals),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/universe/trending', methods=['GET'])
def universe_trending():
    """GET /universe/trending — fastest-growing coverage tickers (7d)."""
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        trending = ext.compute_trending_markets(ext.DB_PATH)
        return jsonify({
            'trending': [
                {
                    'ticker':         t.ticker,
                    'coverage_count': t.coverage_count,
                    'coverage_7d_ago': t.coverage_7d_ago,
                    'growth_rate':    t.growth_rate,
                    'sector_label':   t.sector_label,
                }
                for t in trending
            ]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/universe/coverage', methods=['GET'])
def universe_coverage():
    """GET /universe/coverage — full coverage leaderboard."""
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        rows = ext.get_universe_tickers(ext.DB_PATH)
        return jsonify({'tickers': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/universe/staging/global', methods=['GET'])
def universe_staging_global():
    """GET /universe/staging/global — all staged (not yet promoted) tickers."""
    if not ext.HAS_HYBRID:
        return jsonify({'error': 'hybrid layer not available'}), 503
    try:
        rows = ext.get_staged_tickers(ext.DB_PATH)
        return jsonify({'staging': rows, 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

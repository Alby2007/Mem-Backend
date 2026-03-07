"""routes/ingest_routes.py — Ingest endpoints: status, run-all, historical, calibration, patterns, discover."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, g, jsonify, request

import extensions as ext

bp = Blueprint('ingest', __name__)
_logger = logging.getLogger(__name__)


@bp.route('/ingest/status', methods=['GET'])
@ext.limiter.exempt
def ingest_status():
    """Health check for the ingest scheduler."""
    if not ext.ingest_scheduler:
        return jsonify({
            'scheduler': 'not_running',
            'reason': 'ingest dependencies not installed or scheduler failed to start',
            'adapters': {},
        })

    adapter_status = ext.ingest_scheduler.get_status()
    try:
        _sc = sqlite3.connect(ext.DB_PATH, timeout=5)
        try:
            _src_patterns = {
                'yfinance':              ['exchange_feed_yahoo%', 'yfinance%'],
                'signal_enrichment':     ['derived_signal%', 'signal_enrichment%'],
                'rss_news':              ['news_wire%', 'rss_%'],
                'llm_extraction':        ['llm_extract%'],
                'fred':                  ['macro_data_fred%', 'fred%'],
                'edgar':                 ['regulatory_filing_sec%', 'edgar%'],
                'bne':                   ['bne%'],
                'options':               ['options%'],
                'earnings_calendar':     ['earnings%'],
                'lse_flow':              ['lse%', 'uk_%', 'alt_data_lse%'],
                'fca_short_interest':    ['fca%', 'alt_data_fca%'],
                'edgar_realtime':        ['edgar_realtime%'],
                'insider_transactions':  ['regulatory_filing_sec_form4%'],
                'short_interest':        ['alt_data_finra%'],
                'sector_rotation':       ['derived_signal_sector_rotation%'],
                'economic_calendar_macro': ['macro_data_calendar%'],
            }
            for name, entry in adapter_status.items():
                patterns = _src_patterns.get(name, [])
                total = 0
                for pat in patterns:
                    row = _sc.execute(
                        "SELECT COUNT(*) FROM facts WHERE source LIKE ?", (pat,)
                    ).fetchone()
                    total += row[0] if row else 0
                entry['kb_atoms'] = total
        finally:
            _sc.close()
    except Exception:
        pass
    return jsonify({
        'scheduler': 'running',
        'adapters': adapter_status,
    })


@bp.route('/ingest/run-all', methods=['POST'])
@ext.require_auth
def ingest_run_all():
    """Trigger an immediate out-of-schedule run of ALL registered ingest adapters."""
    if not ext.ingest_scheduler:
        return jsonify({'error': 'scheduler not running'}), 503

    data = request.get_json(force=True, silent=True) or {}
    requested = data.get('adapters')

    status = ext.ingest_scheduler.get_status()
    dispatched = []
    skipped    = []

    for name in status:
        if requested and name not in requested:
            continue
        ok = ext.ingest_scheduler.run_now(name)
        if ok:
            dispatched.append(name)
        else:
            skipped.append(name)

    return jsonify({
        'dispatched': dispatched,
        'skipped':    skipped,
        'note':       'runs are async — poll /ingest/status to track progress',
    })


@bp.route('/ingest/historical', methods=['POST'])
@ext.require_auth
def ingest_historical():
    """Trigger a one-shot historical summary backfill for the watchlist."""
    if not ext.HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    from ingest.historical_adapter import HistoricalBackfillAdapter

    data    = request.get_json(force=True, silent=True) or {}
    tickers = data.get('tickers')

    try:
        adapter = HistoricalBackfillAdapter(tickers=tickers)
        result  = adapter.run_and_push(ext.kg)
        return jsonify({
            'ingested': result.get('ingested', 0),
            'skipped':  result.get('skipped',  0),
            'tickers':  len(adapter.tickers),
        })
    except Exception as e:
        _logger.error('historical backfill failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/calibrate/historical', methods=['POST'])
@ext.require_auth
def calibrate_historical():
    """Back-populate signal_calibration with historical pattern outcome statistics."""
    if not ext.HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    try:
        from analytics.historical_calibration import HistoricalCalibrator
    except ImportError as e:
        return jsonify({'error': f'historical_calibration not available: {e}'}), 503

    data           = request.get_json(force=True, silent=True) or {}
    tickers        = data.get('tickers') or None
    lookback_years = max(1, min(int(data.get('lookback_years', 3)), 10))

    try:
        cal     = HistoricalCalibrator(db_path=ext.DB_PATH)
        results = cal.calibrate_watchlist(tickers=tickers, lookback_years=lookback_years)

        total_patterns = sum(r.get('patterns_detected', 0) for r in results.values())
        total_rows     = sum(r.get('calibration_rows_written', 0) for r in results.values())

        return jsonify({
            'tickers_calibrated':      len(results),
            'total_patterns_detected': total_patterns,
            'total_rows_written':      total_rows,
            'lookback_years':          lookback_years,
            'per_ticker': {
                t: {
                    'patterns_detected':       r.get('patterns_detected', 0),
                    'calibration_rows_written': r.get('calibration_rows_written', 0),
                    'error':                   r.get('error'),
                }
                for t, r in results.items()
            },
        })
    except Exception as e:
        _logger.error('historical calibration failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/calibrate/regime-history', methods=['POST'])
@ext.require_auth
def calibrate_regime_history():
    """Classify historical months into macro regimes and write regime-conditional atoms."""
    if not ext.HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    try:
        from analytics.regime_history import RegimeHistoryClassifier
    except ImportError as e:
        return jsonify({'error': f'regime_history not available: {e}'}), 503

    data           = request.get_json(force=True, silent=True) or {}
    lookback_years = max(1, min(int(data.get('lookback_years', 5)), 10))
    tickers        = data.get('tickers') or None

    try:
        clf    = RegimeHistoryClassifier(db_path=ext.DB_PATH)
        result = clf.run(tickers=tickers, lookback_years=lookback_years)
        return jsonify({**result, 'lookback_years': lookback_years})
    except Exception as e:
        _logger.error('regime history failed: %s', e)
        return jsonify({'error': str(e)}), 500


@bp.route('/ingest/patterns', methods=['POST'])
@ext.require_auth
def ingest_patterns():
    """Trigger pattern detection across all KB tickers that have last_price atoms."""
    if not ext.HAS_INGEST:
        return jsonify({'error': 'ingest not available'}), 503

    try:
        from analytics.pattern_detector import detect_all_patterns, OHLCV as _OHLCV
        import yfinance as _yf
    except ImportError as e:
        return jsonify({'error': f'pattern detection not available: {e}'}), 503

    _YF_MAP = {
        'xauusd': 'GC=F',  'xagusd': 'SI=F',  'xptusd': 'PL=F',
        'cl': 'CL=F',      'bz': 'BZ=F',       'ng': 'NG=F',
        'gbpusd': 'GBPUSD=X', 'eurusd': 'EURUSD=X', 'usdjpy': 'JPY=X',
        'dxy': 'DX-Y.NYB',
        'spx': '^GSPC',    'ndx': '^NDX',       'dji': '^DJI',
        'ftse': '^FTSE',   'dax': '^GDAXI',     'vix': '^VIX',
    }

    data    = request.get_json(force=True, silent=True) or {}
    filter_tickers = [t.lower() for t in data.get('tickers', [])]

    conn = sqlite3.connect(ext.DB_PATH, timeout=15)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL, pattern_type TEXT NOT NULL,
            direction TEXT NOT NULL, zone_high REAL NOT NULL,
            zone_low REAL NOT NULL, zone_size_pct REAL,
            timeframe TEXT NOT NULL, formed_at TEXT,
            status TEXT NOT NULL DEFAULT 'open', filled_at TEXT,
            quality_score REAL, kb_conviction TEXT DEFAULT '',
            kb_regime TEXT DEFAULT '', kb_signal_dir TEXT DEFAULT '',
            alerted_users TEXT DEFAULT '[]', detected_at TEXT
        )
    """)
    conn.commit()

    rows = conn.execute(
        "SELECT DISTINCT subject FROM facts WHERE predicate = 'last_price'"
    ).fetchall()
    tickers = [r[0] for r in rows]
    if filter_tickers:
        tickers = [t for t in tickers if t.lower() in filter_tickers]

    now_iso = datetime.now(timezone.utc).isoformat()
    total_inserted = 0
    total_tickers  = 0

    for ticker in tickers:
        yf_sym = _YF_MAP.get(ticker.lower(), ticker.upper())
        atoms_rows = conn.execute(
            "SELECT predicate, object FROM facts WHERE subject = ?", (ticker,)
        ).fetchall()
        atoms_map = {r[0]: r[1] for r in atoms_rows}

        try:
            hist = _yf.Ticker(yf_sym).history(period='6mo', interval='1d', auto_adjust=True)
            if hist.empty or len(hist) < 10:
                continue
            candles = [
                _OHLCV(
                    timestamp=ts.isoformat(),
                    open=float(row['Open']), high=float(row['High']),
                    low=float(row['Low']),   close=float(row['Close']),
                    volume=float(row.get('Volume', 0) or 0),
                )
                for ts, row in hist.iterrows()
            ]
            signals = detect_all_patterns(
                candles, ticker=ticker.upper(), timeframe='1d',
                kb_conviction=atoms_map.get('conviction_tier', ''),
                kb_regime=atoms_map.get('price_regime', ''),
                kb_signal_dir=atoms_map.get('signal_direction', ''),
            )
            inserted = 0
            for sig in signals:
                exists = conn.execute(
                    """SELECT 1 FROM pattern_signals
                       WHERE ticker=? AND pattern_type=? AND formed_at=? AND timeframe=?
                       LIMIT 1""",
                    (sig.ticker, sig.pattern_type, sig.formed_at, sig.timeframe),
                ).fetchone()
                if exists:
                    continue
                conn.execute(
                    """INSERT INTO pattern_signals
                       (ticker, pattern_type, direction, zone_high, zone_low,
                        zone_size_pct, timeframe, formed_at, status,
                        quality_score, kb_conviction, kb_regime, kb_signal_dir,
                        alerted_users, detected_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'[]',?)""",
                    (sig.ticker, sig.pattern_type, sig.direction,
                     sig.zone_high, sig.zone_low, sig.zone_size_pct,
                     sig.timeframe, sig.formed_at, sig.status,
                     sig.quality_score, sig.kb_conviction,
                     sig.kb_regime, sig.kb_signal_dir, now_iso),
                )
                inserted += 1
            conn.commit()
            total_inserted += inserted
            total_tickers  += 1
        except Exception as _e:
            _logger.warning('pattern detection failed for %s: %s', ticker, _e)

    conn.close()
    total_now = sqlite3.connect(ext.DB_PATH).execute(
        "SELECT COUNT(*) FROM pattern_signals"
    ).fetchone()[0]
    return jsonify({
        'tickers_processed': total_tickers,
        'patterns_inserted': total_inserted,
        'pattern_signals_total': total_now,
    })


@bp.route('/discover/<ticker>', methods=['POST'])
@ext.require_auth
def discover_ticker(ticker: str):
    """Trigger the universal discovery pipeline for a single ticker."""
    if ext.discovery_pipeline is None:
        return jsonify({'error': 'discovery pipeline not available'}), 503

    ticker = ticker.upper().strip()
    if not ticker:
        return jsonify({'error': 'ticker is required'}), 400

    data  = request.get_json(force=True, silent=True) or {}
    force = bool(data.get('force', False))

    staleness = ext.discovery_pipeline.assess_staleness(ticker)

    if not staleness and not force:
        return jsonify({
            'ticker':         ticker,
            'status':         'fresh',
            'stages_run':     [],
            'stages_skipped': [],
            'atoms_written':  0,
            'duration_ms':    0,
            'staleness':      {},
            'message':        'All atoms are fresh — use {"force": true} to re-run anyway',
        })

    if force and not staleness:
        from ingest.discovery_pipeline import STALENESS_THRESHOLDS
        staleness = {p: float('inf') for p in STALENESS_THRESHOLDS}

    try:
        user_id = getattr(g, 'user_id', None)
        result  = ext.discovery_pipeline.discover(ticker, trigger='manual', user_id=user_id)
        return jsonify({
            'ticker':         result.ticker,
            'status':         result.status,
            'stages_run':     result.stages_run,
            'stages_skipped': result.stages_skipped,
            'atoms_written':  result.atoms_written,
            'duration_ms':    result.duration_ms,
            'staleness':      {k: round(v, 1) for k, v in staleness.items()
                               if v != float('inf')},
        })
    except Exception as e:
        _logger.error('discovery failed for %s: %s', ticker, e)
        return jsonify({'error': str(e), 'ticker': ticker}), 500

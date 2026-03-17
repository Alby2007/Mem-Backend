"""routes_v2/analytics_.py — Phase 6: analytics endpoints."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user

router = APIRouter()
_logger = logging.getLogger(__name__)


class MarkSeenRequest(BaseModel):
    ids: list[int] = []


class StressTestRequest(BaseModel):
    scenarios: Optional[list[str]] = None


class CounterfactualRequest(BaseModel):
    scenario: dict = {}


class SignalStressRequest(BaseModel):
    ticker: str
    pattern_id: Optional[int] = None


@router.get("/alerts")
async def alerts_list(
    all: str = "false",
    since: Optional[str] = None,
    limit: int = 200,
):
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        unseen_only = all.lower() != "true"
        rows = ext.get_alerts(ext.DB_PATH, unseen_only=unseen_only,
                              since_iso=since, limit=limit)
        return {"alerts": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/alerts/mark-seen")
async def alerts_mark_seen(data: MarkSeenRequest):
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        updated = ext.mark_alerts_seen(ext.DB_PATH, data.ids)
        return {"updated": updated}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/analytics/snapshot")
async def analytics_snapshot():
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        result = ext.take_snapshot(ext.DB_PATH)
        snaps  = ext.list_snapshots(ext.DB_PATH)
        result["snapshot_count"] = len(snaps)
        result["snapshots"]      = snaps
        return result
    except Exception as e:
        _logger.error("snapshot failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.get("/analytics/snapshot")
async def analytics_snapshot_list():
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        snaps = ext.list_snapshots(ext.DB_PATH)
        return {"snapshot_count": len(snaps), "snapshots": snaps}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/analytics/backtest")
async def analytics_backtest(window: str = "1m"):
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    if window not in ("1w", "1m", "3m"):
        raise HTTPException(400, detail="window must be '1w', '1m', or '3m'")
    try:
        return ext.run_backtest(ext.DB_PATH, window=window)
    except Exception as e:
        _logger.error("backtest failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.get("/analytics/backtest/regime")
async def analytics_backtest_regime():
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        return ext.run_regime_backtest(ext.DB_PATH)
    except Exception as e:
        _logger.error("regime backtest failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.post("/analytics/stress-test")
async def analytics_stress_test(data: StressTestRequest):
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        return ext.run_stress_test(ext.DB_PATH, scenarios=data.scenarios)
    except Exception as e:
        _logger.error("stress test failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.post("/analytics/counterfactual")
async def analytics_counterfactual(data: CounterfactualRequest):
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    if not data.scenario:
        raise HTTPException(400, detail="scenario is required and must not be empty")
    try:
        return ext.run_counterfactual(ext.DB_PATH, scenario=data.scenario)
    except Exception as e:
        _logger.error("counterfactual failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.get("/portfolio/summary")
async def portfolio_summary():
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        return ext.build_portfolio_summary(ext.DB_PATH)
    except Exception as e:
        _logger.error("portfolio summary failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.get("/ledger/track-record")
async def ledger_track_record():
    """Public aggregate track record — no auth required."""
    import sqlite3 as _sq
    from datetime import datetime, timezone, timedelta
    try:
        conn = _sq.connect(ext.DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        try:
            # Resolved predictions in last 30d
            cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
            row = conn.execute("""
                SELECT COUNT(*) as n,
                       AVG(CASE WHEN outcome IN ('hit_t1','hit_t2','t1_hit') THEN 1.0 ELSE 0.0 END) as hit_rate,
                       AVG(brier_t1) as avg_brier,
                       MIN(issued_at) as earliest
                FROM prediction_ledger
                WHERE outcome IS NOT NULL AND outcome != 'expired'
                  AND issued_at >= ?
            """, (cutoff,)).fetchone()
            resolved = row['n'] or 0
            hit_rate = round(row['hit_rate'], 3) if row['hit_rate'] is not None else None
            avg_brier = round(row['avg_brier'], 3) if row['avg_brier'] is not None else None
            period_days = 30

            if avg_brier is None:
                brier_label = 'no_data'
            elif avg_brier < 0.10:
                brier_label = 'excellent'
            elif avg_brier < 0.15:
                brier_label = 'good'
            elif avg_brier < 0.25:
                brier_label = 'calibrating'
            else:
                brier_label = 'poor'

            # Best / worst pattern by hit rate (min 5 predictions)
            pat_rows = conn.execute("""
                SELECT pattern_type,
                       COUNT(*) as n,
                       AVG(CASE WHEN outcome IN ('hit_t1','hit_t2','t1_hit') THEN 1.0 ELSE 0.0 END) as hr
                FROM prediction_ledger
                WHERE outcome IS NOT NULL AND outcome != 'expired'
                  AND issued_at >= ?
                GROUP BY pattern_type
                HAVING n >= 5
                ORDER BY hr DESC
            """, (cutoff,)).fetchall()
            best_pattern  = pat_rows[0]['pattern_type']  if pat_rows else None
            worst_pattern = pat_rows[-1]['pattern_type'] if pat_rows else None

            # Current regime from facts table
            regime_row = conn.execute("""
                SELECT object FROM facts
                WHERE subject = 'market' AND predicate = 'market_regime'
                ORDER BY timestamp DESC LIMIT 1
            """).fetchone()
            current_regime = regime_row['object'] if regime_row else None

            # Top pattern in current regime
            top_regime_pat = None
            top_regime_hr  = None
            if current_regime:
                rp = conn.execute("""
                    SELECT pattern_type,
                           AVG(CASE WHEN outcome IN ('hit_t1','hit_t2','t1_hit') THEN 1.0 ELSE 0.0 END) as hr,
                           COUNT(*) as n
                    FROM prediction_ledger
                    WHERE outcome IS NOT NULL AND outcome != 'expired'
                      AND market_regime = ?
                    GROUP BY pattern_type
                    HAVING n >= 3
                    ORDER BY hr DESC LIMIT 1
                """, (current_regime,)).fetchone()
                if rp:
                    top_regime_pat = rp['pattern_type']
                    top_regime_hr  = round(rp['hr'], 3)

            return {
                'resolved_predictions': resolved,
                'hit_rate_t1':          hit_rate,
                'avg_brier':            avg_brier,
                'brier_label':          brier_label,
                'period_days':          period_days,
                'best_pattern':         best_pattern,
                'worst_pattern':        worst_pattern,
                'regime_edge': {
                    'current_regime': current_regime,
                    'top_pattern':    top_regime_pat,
                    'top_hit_rate':   top_regime_hr,
                },
                'last_updated': datetime.now(timezone.utc).isoformat(),
            }
        finally:
            conn.close()
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/analytics/forward-backtest")
async def forward_backtest_status():
    """Forward backtest readiness — shows result once 2+ snapshots exist."""
    if not ext.HAS_ANALYTICS:
        raise HTTPException(503, detail="analytics module not available")
    try:
        from analytics.backtest import list_snapshots, run_backtest
        dates = list_snapshots(ext.DB_PATH)
        if len(dates) < 2:
            return {
                'ready':            False,
                'snapshots_taken':  len(dates),
                'snapshots_needed': 2,
                'next_snapshot':    'tomorrow 00:00 UTC',
                'message':          f'{2 - len(dates)} more snapshot(s) needed before first result',
            }
        result = run_backtest(ext.DB_PATH)
        return {'ready': True, 'snapshots': len(dates), **result}
    except Exception as e:
        _logger.error("forward-backtest failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.get("/ledger/performance")
async def ledger_performance():
    if ext.prediction_ledger is None:
        raise HTTPException(503, detail="prediction_ledger_not_initialised")
    try:
        return ext.prediction_ledger.get_performance_report()
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/ledger/open")
async def ledger_open(_: str = Depends(get_current_user)):
    if ext.prediction_ledger is None:
        raise HTTPException(503, detail="prediction_ledger_not_initialised")
    try:
        predictions = ext.prediction_ledger.get_open_predictions()
        return {"predictions": predictions, "count": len(predictions)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/forecast/{ticker}/{pattern_type}")
async def forecast_signal(
    ticker: str,
    pattern_type: str,
    timeframe: str = "1d",
    account_size: float = 10000.0,
    risk_pct: float = 1.0,
    _: str = Depends(get_current_user),
):
    try:
        from analytics.signal_forecaster import SignalForecaster
        forecaster = SignalForecaster(ext.DB_PATH)
        result     = forecaster.forecast(
            ticker=ticker, pattern_type=pattern_type,
            timeframe=timeframe, account_size=account_size,
            risk_pct=risk_pct, seed=None,
        )
        return {
            "ticker":                result.ticker,
            "pattern_type":          result.pattern_type,
            "timeframe":             result.timeframe,
            "market_regime":         result.market_regime,
            "p_hit_t1":              result.p_hit_t1,
            "p_hit_t2":              result.p_hit_t2,
            "p_stopped_out":         result.p_stopped_out,
            "p_expired":             result.p_expired,
            "expected_value_gbp":    result.expected_value_gbp,
            "ci_90_low":             result.ci_90_low,
            "ci_90_high":            result.ci_90_high,
            "days_to_target_median": result.days_to_target_median,
            "regime_adjustment_pct": result.regime_adjustment_pct,
            "iv_adjustment_pct":     result.iv_adjustment_pct,
            "macro_adjustment_pct":  result.macro_adjustment_pct,
            "short_adjustment_pct":  result.short_adjustment_pct,
            "calibration_samples":   result.calibration_samples,
            "used_prior":            result.used_prior,
            "generated_at":          result.generated_at,
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/causal/shocks")
async def causal_shocks(
    n: int = 50,
    _: str = Depends(get_current_user),
):
    if ext.shock_engine is None:
        return {"shocks": [], "note": "shock_engine_not_initialised"}
    try:
        n = min(n, 200)
        shocks = ext.shock_engine.get_recent_shocks(n=n)
        return {"shocks": shocks, "count": len(shocks)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/signals/stress-test")
async def signals_stress_test(
    data: SignalStressRequest,
    _: str = Depends(get_current_user),
):
    try:
        from analytics.adversarial_tester import AdversarialTester
        if not data.ticker:
            raise HTTPException(400, detail="ticker required")

        patterns = ext.get_open_patterns(ext.DB_PATH, min_quality=0.0, limit=500)
        pattern  = None
        for p in patterns:
            if p["ticker"].upper() == data.ticker.upper():
                if data.pattern_id is None or p["id"] == data.pattern_id:
                    pattern = p
                    break
        if pattern is None:
            raise HTTPException(404, detail="no open pattern found for ticker")

        tester = AdversarialTester(ext.DB_PATH)
        result = tester.stress_test_signal(data.ticker, pattern)
        return {
            "ticker":                 data.ticker.upper(),
            "pattern_type":           pattern.get("pattern_type"),
            "survival_rate":          result.survival_rate,
            "robustness_label":       result.robustness_label,
            "invalidating_scenarios": result.invalidating_scenarios,
            "earnings_warning":       result.earnings_proximity_warning,
            "scenarios_tested":       result.scenarios_tested,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))

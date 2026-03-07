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

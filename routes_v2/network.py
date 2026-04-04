"""routes_v2/network.py — Phase 6: network effect endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

import extensions as ext
from middleware.fastapi_auth import get_current_user

router = APIRouter()


@router.get("/network/health")
async def network_health():
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        report = ext.compute_network_health(ext.DB_PATH)
        return {
            "total_tickers":         report.total_tickers,
            "total_users":           report.total_users,
            "tickers_by_tier":       report.tickers_by_tier,
            "coverage_distribution": report.coverage_distribution,
            "flywheel_velocity":     report.flywheel_velocity,
            "cohort_signals_active": report.cohort_signals_active,
            "generated_at":          report.generated_at,
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/network/calibration/{ticker}")
async def network_calibration(
    ticker: str,
    pattern_type: str = "fvg",
    timeframe: str = "1h",
):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        cal = ext.get_calibration(ticker, pattern_type, timeframe, ext.DB_PATH)
        if cal is None:
            return {"calibration": None, "reason": "insufficient_samples (< 10)"}
        return {
            "ticker":                 cal.ticker,
            "pattern_type":           cal.pattern_type,
            "timeframe":              cal.timeframe,
            "market_regime":          cal.market_regime,
            "sample_size":            cal.sample_size,
            "hit_rate_t1":            cal.hit_rate_t1,
            "hit_rate_t2":            cal.hit_rate_t2,
            "hit_rate_t3":            cal.hit_rate_t3,
            "stopped_out_rate":       cal.stopped_out_rate,
            "calibration_confidence": cal.calibration_confidence,
            "confidence_label":       cal.confidence_label,
            "last_updated":           cal.last_updated,
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/network/cohort/{ticker}")
async def network_cohort(ticker: str):
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        signal = ext.detect_cohort_consensus(ticker, ext.DB_PATH)
        if signal is None:
            return {"cohort_signal": None, "reason": "insufficient_cohort (< 10 users)"}
        return {
            "ticker":              signal.ticker,
            "cohort_size":         signal.cohort_size,
            "consensus_direction": signal.consensus_direction,
            "consensus_strength":  signal.consensus_strength,
            "stop_cluster":        signal.stop_cluster,
            "contrarian_flag":     signal.contrarian_flag,
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/network/convergence")
async def network_convergence(
    lookback_hours: int = 24,
    _: str = Depends(get_current_user),
):
    try:
        from analytics.network_effect_engine import NetworkEffectEngine
        engine  = NetworkEffectEngine(ext.DB_PATH)
        signals = engine.detect_convergence(lookback_hours=lookback_hours)
        return {
            "convergence_signals": [
                {
                    "ticker":         s.ticker,
                    "distinct_users": s.distinct_users,
                    "lookback_hours": s.lookback_hours,
                    "kb_signal":      s.kb_signal_direction,
                    "organic":        s.is_organic,
                    "detected_at":    s.detected_at,
                }
                for s in signals
            ],
            "count": len(signals),
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/universe/trending")
async def universe_trending():
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        trending = ext.compute_trending_markets(ext.DB_PATH)
        return {
            "trending": [
                {
                    "ticker":          t.ticker,
                    "coverage_count":  t.coverage_count,
                    "coverage_7d_ago": t.coverage_7d_ago,
                    "growth_rate":     t.growth_rate,
                    "sector_label":    t.sector_label,
                }
                for t in trending
            ]
        }
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/universe/coverage")
async def universe_coverage():
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        rows = ext.get_universe_tickers(ext.DB_PATH)
        return {"tickers": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/universe/staging/global")
async def universe_staging_global():
    if not ext.HAS_HYBRID:
        raise HTTPException(503, detail="hybrid layer not available")
    try:
        rows = ext.get_staged_tickers(ext.DB_PATH)
        return {"staging": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

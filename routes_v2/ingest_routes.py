"""routes_v2/ingest_routes.py — Phase 6: ingest/calibration/discovery endpoints."""

from __future__ import annotations

import logging
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user

router = APIRouter()
_logger = logging.getLogger(__name__)

_SRC_PATTERNS = {
    "yfinance":               ["exchange_feed_yahoo%", "yfinance%", "exchange_feed_on_demand_yf%", "broker_research_on_demand_yf%"],
    "signal_enrichment":      ["derived_signal%", "signal_enrichment%"],
    "rss_news":               ["news_wire%", "rss_%"],
    "llm_extraction":         ["llm_extract%"],
    "fred":                   ["macro_data_fred%", "fred%"],
    "edgar":                  ["regulatory_filing_sec%", "edgar%"],
    "bne":                    ["bne%"],
    "options":                ["options%"],
    "earnings_calendar":      ["earnings%"],
    "lse_flow":               ["lse%", "uk_%", "alt_data_lse%"],
    "fca_short_interest":     ["fca%", "alt_data_fca%"],
    "edgar_realtime":         ["edgar_realtime%"],
    "insider_transactions":   ["regulatory_filing_sec_form4%"],
    "short_interest":         ["alt_data_finra%"],
    "sector_rotation":        ["derived_signal_sector_rotation%"],
    "economic_calendar_macro": ["macro_data_calendar%"],
}


class RunAllRequest(BaseModel):
    adapters: Optional[list[str]] = None


class HistoricalRequest(BaseModel):
    tickers: Optional[list[str]] = None


class RegimeHistoryRequest(BaseModel):
    tickers: Optional[list[str]] = None
    lookback_years: int = 5


class DiscoverRequest(BaseModel):
    force: bool = False


@router.get("/ingest/status")
async def ingest_status():
    if not ext.ingest_scheduler:
        # Build a static adapter map directly from the facts table source prefixes
        adapters: dict = {}
        try:
            conn = sqlite3.connect(ext.DB_PATH, timeout=5)
            try:
                for name, patterns in _SRC_PATTERNS.items():
                    total = 0
                    last_ts = None
                    for pat in patterns:
                        row = conn.execute(
                            "SELECT COUNT(*) FROM facts WHERE source LIKE ?", (pat,)
                        ).fetchone()
                        total += row[0] if row else 0
                        ts_row = conn.execute(
                            "SELECT MAX(timestamp) FROM facts WHERE source LIKE ?", (pat,)
                        ).fetchone()
                        if ts_row and ts_row[0]:
                            if last_ts is None or ts_row[0] > last_ts:
                                last_ts = ts_row[0]
                    if total > 0:
                        adapters[name] = {
                            "name": name,
                            "kb_atoms": total,
                            "total_atoms": total,
                            "last_run_at": last_ts,
                            "is_running": False,
                            "last_error": None,
                        }
            finally:
                conn.close()
        except Exception:
            pass
        return {"scheduler": "static", "adapters": adapters}

    adapter_status = ext.ingest_scheduler.get_status()
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        try:
            for name, entry in adapter_status.items():
                patterns = _SRC_PATTERNS.get(name, [])
                total = 0
                for pat in patterns:
                    row = conn.execute("SELECT COUNT(*) FROM facts WHERE source LIKE ?", (pat,)).fetchone()
                    total += row[0] if row else 0
                entry["kb_atoms"] = total
        finally:
            conn.close()
    except Exception:
        pass
    return {"scheduler": "running", "adapters": adapter_status}


@router.get("/ingest/scheduler/status")
async def ingest_scheduler_status(request: Request):
    from fastapi import Request as _Req
    scheduler = getattr(getattr(request, 'app', None), 'state', None)
    scheduler = getattr(scheduler, 'scheduler', None)

    adapter_status: dict = {}
    if scheduler is not None:
        try:
            adapter_status = scheduler.get_status()
        except Exception:
            pass

    queue_pending = 0
    queue_total = 0
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM extraction_queue WHERE processed = 0"
            ).fetchone()
            queue_pending = row[0] if row else 0
            row2 = conn.execute("SELECT COUNT(*) FROM extraction_queue").fetchone()
            queue_total = row2[0] if row2 else 0
        finally:
            conn.close()
    except Exception:
        pass

    return {
        "scheduler": "running" if scheduler is not None else "not_running",
        "adapters": adapter_status,
        "extraction_queue": {
            "pending": queue_pending,
            "total": queue_total,
            "processed": queue_total - queue_pending,
        },
    }


@router.post("/ingest/run-all")
async def ingest_run_all(data: RunAllRequest, _: str = Depends(get_current_user)):
    if not ext.ingest_scheduler:
        raise HTTPException(503, detail="scheduler not running")

    status     = ext.ingest_scheduler.get_status()
    dispatched = []
    skipped    = []
    for name in status:
        if data.adapters and name not in data.adapters:
            skipped.append(name)
            continue
        try:
            ext.ingest_scheduler.run_now(name)
            dispatched.append(name)
        except Exception as e:
            _logger.warning("run_now(%s) failed: %s", name, e)
            skipped.append(name)

    return {"dispatched": dispatched, "skipped": skipped,
            "note": "runs are async — poll /ingest/status to track progress"}


@router.post("/ingest/historical")
async def ingest_historical(data: HistoricalRequest, _: str = Depends(get_current_user)):
    if not ext.HAS_INGEST:
        raise HTTPException(503, detail="ingest not available")
    try:
        from ingest.historical_adapter import HistoricalBackfillAdapter
        adapter = HistoricalBackfillAdapter(db_path=ext.DB_PATH,
                                            tickers=data.tickers or None)
        result  = adapter.run()
        return {"status": "ok", "ingested": result.get("ingested", 0),
                "skipped": result.get("skipped", 0), "tickers": len(adapter.tickers)}
    except Exception as e:
        _logger.error("historical backfill failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.post("/calibrate/historical")
async def calibrate_historical(_: str = Depends(get_current_user)):
    if not ext.HAS_INGEST:
        raise HTTPException(503, detail="ingest not available")
    try:
        from analytics.historical_calibration import HistoricalCalibrator
    except ImportError as e:
        raise HTTPException(503, detail=f"historical_calibration not available: {e}")
    try:
        cal     = HistoricalCalibrator(db_path=ext.DB_PATH)
        results = cal.run()
        return {
            "status": "ok",
            "tickers_calibrated": len(results),
            "results": {t: {"status": r.get("status"), "samples": r.get("samples")}
                        for t, r in results.items()},
        }
    except Exception as e:
        _logger.error("historical calibration failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.post("/calibrate/regime-history")
async def calibrate_regime_history(
    data: RegimeHistoryRequest,
    _: str = Depends(get_current_user),
):
    if not ext.HAS_INGEST:
        raise HTTPException(503, detail="ingest not available")
    try:
        from analytics.regime_history import RegimeHistoryClassifier
    except ImportError as e:
        raise HTTPException(503, detail=f"regime_history not available: {e}")
    try:
        clf    = RegimeHistoryClassifier(db_path=ext.DB_PATH)
        result = clf.run(tickers=data.tickers, lookback_years=data.lookback_years)
        return {**result, "lookback_years": data.lookback_years}
    except Exception as e:
        _logger.error("regime history failed: %s", e)
        raise HTTPException(500, detail=str(e))


@router.post("/ingest/patterns")
async def ingest_patterns(_: str = Depends(get_current_user)):
    if not ext.HAS_INGEST:
        raise HTTPException(503, detail="ingest not available")
    try:
        from analytics.pattern_detector import detect_all_patterns, OHLCV as _OHLCV
        import yfinance as _yf
    except ImportError as e:
        raise HTTPException(503, detail=f"pattern detection not available: {e}")
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        tickers = [r[0] for r in conn.execute(
            "SELECT DISTINCT subject FROM facts WHERE predicate='last_price'"
        ).fetchall()]
        conn.close()

        total_inserted = 0
        total_tickers  = 0
        for ticker in tickers:
            try:
                df = _yf.download(ticker, period="60d", interval="1d", progress=False, auto_adjust=True)
                if df.empty:
                    continue
                ohlcv_list = [
                    _OHLCV(date=str(idx.date()), open=float(row["Open"]),
                            high=float(row["High"]), low=float(row["Low"]),
                            close=float(row["Close"]), volume=float(row["Volume"]))
                    for idx, row in df.iterrows()
                ]
                result = detect_all_patterns(ticker, ohlcv_list, ext.DB_PATH)
                total_inserted += result.get("inserted", 0)
                total_tickers  += 1
            except Exception:
                continue

        conn2 = sqlite3.connect(ext.DB_PATH, timeout=5)
        total_now = conn2.execute("SELECT COUNT(*) FROM pattern_signals").fetchone()[0]
        conn2.close()
        return {"tickers_processed": total_tickers, "patterns_inserted": total_inserted,
                "pattern_signals_total": total_now}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/discover/{ticker}")
async def discover_ticker(
    ticker: str,
    data: DiscoverRequest = DiscoverRequest(),
    _: str = Depends(get_current_user),
):
    if ext.discovery_pipeline is None:
        raise HTTPException(503, detail="discovery pipeline not available")
    ticker = ticker.upper().strip()
    if not ticker:
        raise HTTPException(400, detail="ticker is required")
    try:
        result = ext.discovery_pipeline.run(ticker=ticker, force=data.force)
        return {"ticker": ticker, "status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

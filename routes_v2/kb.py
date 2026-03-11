"""routes_v2/kb.py — Phase 6: knowledge base endpoints."""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user

router = APIRouter()

_log = logging.getLogger(__name__)


class IngestAtom(BaseModel):
    subject: str
    predicate: str
    object: str
    confidence: float = 0.5
    source: str = "unverified_api"
    metadata: Optional[dict] = None


class IngestRequest(BaseModel):
    atoms: Optional[list[IngestAtom]] = None
    # single-atom fields (when no "atoms" key)
    subject: Optional[str] = None
    predicate: Optional[str] = None
    object: Optional[str] = None
    confidence: float = 0.5
    source: str = "unverified_api"
    metadata: Optional[dict] = None


class RetrieveRequest(BaseModel):
    message: str
    session_id: str = "default"
    goal: Optional[str] = None
    topic: Optional[str] = None
    turn_count: int = 1
    limit: int = 30


@router.post("/ingest")
async def ingest(request: Request, user_id: str = Depends(get_current_user)):
    _admin_ids = {u.strip() for u in os.environ.get("ADMIN_USER_IDS", "").split(",") if u.strip()}
    _ingest_key = os.environ.get("INGEST_API_KEY", "")
    _provided_key = request.headers.get("X-Ingest-Key", "")
    if user_id not in _admin_ids and not (_ingest_key and _provided_key == _ingest_key):
        raise HTTPException(403, detail="forbidden — admin or valid X-Ingest-Key required")

    body = await request.json()
    if not body:
        raise HTTPException(400, detail="invalid JSON")

    atoms: list = body.get("atoms") or [body]
    ingested = 0
    skipped  = 0
    is_single = "atoms" not in body
    for atom in atoms:
        subject   = atom.get("subject")
        predicate = atom.get("predicate")
        obj       = atom.get("object")
        if not (subject and predicate and obj):
            skipped += 1
            if is_single:
                raise HTTPException(400, detail="subject, predicate and object are all required")
            continue
        ok = ext.kg.add_fact(
            subject=subject, predicate=predicate, object=obj,
            confidence=float(atom.get("confidence", 0.5)),
            source=atom.get("source", "unverified_api"),
            metadata=atom.get("metadata"),
        )
        if ok:
            ingested += 1
        else:
            skipped += 1
    return {"ingested": ingested, "skipped": skipped}


@router.get("/query")
async def query(
    subject: Optional[str] = None,
    predicate: Optional[str] = None,
    object: Optional[str] = None,
    limit: int = 50,
    _: str = Depends(get_current_user),
):
    results = ext.kg.query(subject=subject, predicate=predicate, object=object, limit=limit)
    return {"results": results, "count": len(results)}


@router.post("/retrieve")
async def retrieve_endpoint(data: RetrieveRequest):
    conn = ext.kg.thread_local_conn()

    prior_context = None
    if ext.HAS_WORKING_STATE:
        try:
            ws = ext.get_working_state_store(ext.DB_PATH)
            if data.turn_count == 0:
                prior_context = ws.format_prior_context(data.session_id) or None
            ws.maybe_persist(
                data.session_id, data.turn_count,
                goal=data.goal, topic=data.topic,
                force=(data.turn_count == 1),
            )
        except Exception:
            pass

    nudges = None
    if ext.HAS_ADAPTATION and ext.HAS_STRESS:
        try:
            from knowledge.epistemic_adaptation import ensure_adaptation_tables
            ensure_adaptation_tables(conn)
            engine = ext.get_adaptation_engine(data.session_id, db_path=ext.DB_PATH)
            engine._session_id = data.session_id
            sess = ext.sessions.get_streak(data.session_id)

            class _StateStub:
                pass
            state_stub = _StateStub()
            state_stub.epistemic_stress_streak = sess["streak"]
            state_stub._session_id = data.session_id

            class _StressStub:
                composite_stress    = sess["last_stress"]
                decay_pressure      = 0.0
                authority_conflict  = 0.0
                supersession_density = 0.0
                conflict_cluster    = 0.0
                domain_entropy      = 1.0

            nudges = engine.compute(state_stub, _StressStub(), topic=data.topic, key_terms=[])
        except Exception:
            nudges = None

    snippet, atoms = ext.retrieve(data.message, conn, limit=data.limit, nudges=nudges)
    response: dict = {"snippet": snippet, "atoms": atoms}
    if prior_context:
        response["prior_context"] = prior_context

    stress_report = None
    if ext.HAS_STRESS and atoms:
        try:
            words = re.findall(r"\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b", data.message)
            key_terms = list({w.lower() for w in words if len(w) > 2})[:10]
            stress_report = ext.compute_stress(atoms, key_terms, conn)
            response["stress"] = {
                "composite_stress":       stress_report.composite_stress,
                "decay_pressure":         stress_report.decay_pressure,
                "authority_conflict":     stress_report.authority_conflict,
                "supersession_density":   stress_report.supersession_density,
                "conflict_cluster":       stress_report.conflict_cluster,
                "domain_entropy":         stress_report.domain_entropy,
            }
        except Exception:
            pass

    if ext.HAS_ADAPTATION and stress_report:
        try:
            from knowledge.epistemic_adaptation import _STRESS_STREAK_THRESHOLD
            sess = ext.sessions.get_streak(data.session_id)
            if stress_report.composite_stress >= _STRESS_STREAK_THRESHOLD:
                sess["streak"] = sess.get("streak", 0) + 1
            else:
                sess["streak"] = max(0, sess.get("streak", 0) - 1)
            sess["last_stress"] = stress_report.composite_stress
            ext.sessions.set_streak(data.session_id, sess)
        except Exception:
            pass

    if nudges is not None and nudges.is_active():
        response["adaptation"] = {
            "streak":                    nudges.streak,
            "consolidation_mode":        nudges.consolidation_mode,
            "retrieval_scope_broadened": nudges.retrieval_scope_broadened,
            "prefer_high_authority":     nudges.prefer_high_authority,
            "prefer_recent":             nudges.prefer_recent,
            "refresh_domain_queued":     nudges.refresh_domain_queued,
            "conflict_synthesis_queued": nudges.conflict_synthesis_queued,
            "kb_insufficient":           nudges.kb_insufficient,
        }
        if nudges.refresh_domain_queued and ext.ingest_scheduler and data.topic:
            try:
                ext.ingest_scheduler.run_now("yfinance")
            except Exception:
                pass

    if ext.HAS_CLASSIFIER and stress_report and atoms:
        try:
            _tickers = [t for t in re.findall(r"\b[A-Z]{2,5}\b", data.message)
                        if t not in {"THE","IS","AT","ON","AN","AND","OR","FOR","IN","OF",
                                     "TO","THAT","THIS","WITH","FROM","BY","ARE","WAS","BE",
                                     "HAS","HAVE","HAD","ITS","DO","DID","WHAT","HOW","WHY",
                                     "WHEN","WHERE","WHO","CAN","WILL","NOT","BUT","ALL"}]
            composite  = getattr(stress_report, "composite_stress", 0.0)
            atom_count = len(atoms)
            if composite > 0.35 or atom_count < 8:
                topic_hint = (data.topic or (_tickers[0] if _tickers else None)
                              or data.message[:40])
                diagnosis = ext.classify_insufficiency(topic_hint, stress_report, conn)
                response["kb_diagnosis"] = {
                    "topic":         diagnosis.topic,
                    "types":         [t.value for t in diagnosis.types],
                    "primary_type":  diagnosis.primary_type().value,
                    "confidence":    diagnosis.confidence,
                    "matched_rules": diagnosis.matched_rules,
                    "signals":       diagnosis.signals,
                }
        except Exception:
            pass

    return response


@router.get("/search")
async def search(q: str, category: Optional[str] = None, limit: int = 20):
    if not q:
        raise HTTPException(400, detail="q is required")
    results = ext.kg.search(q, limit=limit, category=category)
    return {"results": results, "count": len(results)}


@router.get("/context/{entity}")
async def context(entity: str):
    facts = ext.kg.get_context(entity)
    return {"entity": entity, "facts": facts, "count": len(facts)}


@router.get("/stats")
async def stats():
    base = ext.kg.get_stats()
    conn = ext.kg.thread_local_conn()
    c    = conn.cursor()
    extras: dict = {}

    for key, sql in [
        ("total_conflicts_detected", "SELECT COUNT(*) FROM fact_conflicts"),
        ("pending_repair_proposals",  "SELECT COUNT(*) FROM repair_proposals WHERE status = 'pending'"),
        ("domain_refresh_queue_depth","SELECT COUNT(*) FROM domain_refresh_queue WHERE processed = 0"),
        ("open_patterns",             "SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken')"),
    ]:
        try:
            extras[key] = c.execute(sql).fetchone()[0]
        except Exception:
            extras[key] = 0

    try:
        extras["top_retrieved_atoms"] = [
            {"subject": r[0], "predicate": r[1], "hits": r[2]}
            for r in c.execute(
                "SELECT subject, predicate, SUM(hit_count) as hits FROM facts "
                "WHERE hit_count > 0 GROUP BY subject, predicate ORDER BY hits DESC LIMIT 5"
            ).fetchall()
        ]
    except Exception:
        extras["top_retrieved_atoms"] = []

    extras["adaptation_sessions_active"] = ext.sessions.active_streak_count()
    extras["adaptation_sessions_total"]  = ext.sessions.total_streak_count()

    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        extras["kb_insufficient_events_7d"] = c.execute(
            "SELECT COUNT(*) FROM kb_insufficient_log WHERE detected_at >= ?", (cutoff,)
        ).fetchone()[0]
    except Exception:
        extras["kb_insufficient_events_7d"] = 0

    for regime_key, sql in [
        ("market_regime",
         "SELECT object FROM facts WHERE subject='market' AND predicate='market_regime' ORDER BY timestamp DESC LIMIT 1"),
        ("regime_volatility",
         "SELECT object FROM facts WHERE predicate IN ('volatility_regime','market_volatility','vix_regime') ORDER BY timestamp DESC LIMIT 1"),
    ]:
        try:
            row = c.execute(sql).fetchone()
            extras[regime_key] = row[0] if row else None
        except Exception:
            extras[regime_key] = None

    return {**base, **extras}


@router.get("/kb/transition-forecast")
async def transition_forecast(
    ticker: Optional[str] = None,
    _user: str = Depends(get_current_user),
):
    """Get the current market state transition forecast."""
    from analytics.state_transitions import TransitionEngine
    engine = TransitionEngine(ext.DB_PATH)
    try:
        if ticker:
            forecast = engine.get_current_state_forecast(scope='ticker', subject=ticker.upper())
        else:
            forecast = engine.get_current_state_forecast(scope='global', subject='market')
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    if not forecast:
        return {"transitions": [], "message": "Insufficient snapshot data. Accumulating — check back in a few days."}

    cs = forecast.current_state
    return {
        "current_state_id": forecast.current_state_id,
        "current_state": {
            "regime":      cs.regime,
            "volatility":  cs.volatility,
            "fed_stance":  cs.fed_stance,
            "sector":      cs.dominant_sector,
            "tension":     cs.tension,
            "signal_bias": cs.signal_bias,
            "label":       cs.label(),
        },
        "total_observations":    forecast.total_observations,
        "avg_persistence_hours": round(forecast.avg_persistence_hours, 1),
        "self_transition_rate":  round(forecast.self_transition_rate, 3),
        "confidence":            forecast.confidence,
        "transitions": [
            {
                "to_state_id": t.to_state_id,
                "to_state": {
                    "regime":     t.to_state.regime,
                    "volatility": t.to_state.volatility,
                    "fed_stance": t.to_state.fed_stance,
                    "sector":     t.to_state.dominant_sector,
                    "label":      t.to_state.label(),
                },
                "probability":   round(t.probability, 3),
                "observations":  t.observation_count,
                "avg_hours":     round(t.avg_hours_to_transition, 1),
                "avg_return_1w": round(t.avg_forward_return_1w, 4) if t.avg_forward_return_1w is not None else None,
                "avg_return_1m": round(t.avg_forward_return_1m, 4) if t.avg_forward_return_1m is not None else None,
                "confidence":    t.confidence,
            }
            for t in forecast.transitions[:5]
        ],
    }


@router.get("/kb/transition-stats")
async def transition_stats(
    scope: str = "global",
    _user: str = Depends(get_current_user),
):
    """Return full transition graph statistics."""
    from analytics.state_transitions import TransitionEngine
    engine = TransitionEngine(ext.DB_PATH)
    try:
        stats = engine.get_state_statistics(scope=scope)
    except Exception as e:
        raise HTTPException(500, detail=str(e))
    return stats


@router.get("/kb/temporal-search")
async def temporal_search(
    q: str,
    ticker: Optional[str] = None,
    limit: int = 50,
    _user: str = Depends(get_current_user),
):
    """Search for historical market states matching a natural language query."""
    from analytics.temporal_search import TemporalStateSearch
    searcher = TemporalStateSearch(ext.DB_PATH)

    try:
        if ticker:
            result = searcher.search_for_ticker(ticker.upper(), q)
        else:
            result = searcher.search_by_natural_language(q)
    except Exception as e:
        raise HTTPException(500, detail=str(e))

    if not result:
        return {"match_count": 0, "message": "No matching historical states found. Snapshots accumulate every 6h — check back after the next cycle."}

    return {
        "match_count":          result.match_count,
        "query_state":          result.query_state,
        "avg_similarity":       result.avg_similarity,
        "avg_outcome_1w":       result.avg_outcome_1w,
        "avg_outcome_1m":       result.avg_outcome_1m,
        "outcome_distribution": result.outcome_distribution,
        "regime_breakdown":     result.regime_breakdown,
        "best_period":          result.best_period,
        "worst_outcome_period": result.worst_outcome_period,
        "top_matches": [
            {
                "date":       m.snapshot_at[:10],
                "subject":    m.subject,
                "similarity": m.similarity,
                "outcome_1w": m.outcome_1w,
                "outcome_1m": m.outcome_1m,
                "state":      m.state,
            }
            for m in result.top_matches[:int(limit)]
        ],
    }


@router.get("/kb/calibration-dashboard")
async def calibration_dashboard():
    """Public calibration statistics — no auth required. Used by landing page social proof."""
    conn = sqlite3.connect(ext.DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        # Ensure tables exist before querying
        from analytics.signal_calibration import _ensure_table as _sc_ensure
        _sc_ensure(conn)

        # Total observations by source
        obs_row = conn.execute(
            """SELECT
                 SUM(CASE WHEN source='paper_bot' THEN 1 ELSE 0 END) AS bot_obs,
                 SUM(CASE WHEN source IN ('user','user_feedback') THEN 1 ELSE 0 END) AS user_obs
               FROM calibration_observations"""
        ).fetchone()
        total_bot  = obs_row['bot_obs']  or 0 if obs_row else 0
        total_user = obs_row['user_obs'] or 0 if obs_row else 0

        # Calibration cells with ≥10 samples
        established = conn.execute(
            "SELECT COUNT(*) FROM signal_calibration WHERE sample_size >= 10"
        ).fetchone()[0]

        # Top performing cells by T1 hit rate
        top_cells = conn.execute(
            """SELECT ticker, pattern_type, timeframe, market_regime,
                      sample_size, hit_rate_t1, calibration_confidence
               FROM signal_calibration
               WHERE sample_size >= 10 AND hit_rate_t1 IS NOT NULL
               ORDER BY hit_rate_t1 DESC LIMIT 10"""
        ).fetchall()

        # Global correction factor
        correction = 1.0
        try:
            from analytics.calibration_correction import get_global_correction
            correction = get_global_correction(ext.DB_PATH)
        except Exception:
            pass

        return {
            "total_observations": total_bot + total_user,
            "bot_observations":   total_bot,
            "user_observations":  total_user,
            "established_cells":  established,
            "correction_factor":  round(correction, 3),
            "top_cells": [
                {
                    "ticker":       r["ticker"],
                    "pattern_type": r["pattern_type"],
                    "timeframe":    r["timeframe"],
                    "regime":       r["market_regime"],
                    "samples":      r["sample_size"],
                    "hit_rate":     round(r["hit_rate_t1"], 3) if r["hit_rate_t1"] else None,
                    "confidence":   round(r["calibration_confidence"], 3) if r["calibration_confidence"] else None,
                }
                for r in top_cells
            ],
        }
    except Exception as e:
        _log.warning("calibration_dashboard failed: %s", e)
        return {"total_observations": 0, "bot_observations": 0, "user_observations": 0,
                "established_cells": 0, "correction_factor": 1.0, "top_cells": [], "error": str(e)}
    finally:
        conn.close()

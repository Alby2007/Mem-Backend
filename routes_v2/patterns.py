"""routes_v2/patterns.py — Phase 6: pattern endpoints."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

import extensions as ext
from middleware.fastapi_auth import get_current_user

router = APIRouter()

_VALID_FEEDBACK_ACTIONS = {"taking_it", "tell_me_more", "not_for_me", "skip", "like", "dislike"}
_VALID_OUTCOMES = {"hit_t1", "hit_t2", "hit_t3", "stopped_out", "pending", "skipped"}


class PatternFeedbackRequest(BaseModel):
    action: str
    comment: str = ""


class FeedbackRequest(BaseModel):
    user_id: Optional[str] = None
    tip_id: Optional[int] = None
    pattern_id: Optional[int] = None
    outcome: str


class TipFeedbackRequest(BaseModel):
    user_id: Optional[str] = None
    action: str
    rejection_reason: str = "no_reason"
    pattern_id: Optional[int] = None


class PositionUpdateRequest(BaseModel):
    user_id: Optional[str] = None
    action: str
    exit_price: Optional[float] = None
    shares_closed: Optional[float] = None
    close_method: str = "manual"


@router.get("/patterns")
async def patterns_list(
    limit: int = 50,
    sort: str = "detected_at",
    order: str = "desc",
    status: Optional[str] = None,
    ticker: Optional[str] = None,
    min_quality: float = 0.0,
):
    _ALLOWED_SORT = {"detected_at", "formed_at", "quality_score", "id"}
    sort_col = sort if sort in _ALLOWED_SORT else "detected_at"
    order_dir = "DESC" if order.lower() != "asc" else "ASC"
    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        clauses = ["quality_score >= ?"]
        params: list = [min_quality]
        if status:
            clauses.append("status = ?")
            params.append(status)
        if ticker:
            clauses.append("UPPER(ticker) = ?")
            params.append(ticker.upper())
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        rows = conn.execute(
            f"""SELECT id, ticker, pattern_type, direction, zone_high, zone_low,
                       zone_size_pct, timeframe, formed_at, status,
                       quality_score, kb_conviction, kb_regime, kb_signal_dir, detected_at
                FROM pattern_signals
                {where}
                ORDER BY {sort_col} {order_dir}
                LIMIT ?""",
            params + [min(limit, 200)],
        ).fetchall()
        conn.close()
        patterns = [dict(r) for r in rows]
        return {"patterns": patterns, "count": len(patterns)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/patterns/open")
async def patterns_open(
    ticker: str = "",
    min_quality: float = 0.0,
    limit: int = 50,
):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    try:
        patterns = ext.get_open_patterns(
            ext.DB_PATH,
            ticker=ticker.upper() or None,
            min_quality=min_quality,
            limit=limit,
        )
        return {"patterns": patterns, "count": len(patterns)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/patterns/{pattern_id}/feedback")
async def pattern_feedback(
    pattern_id: int,
    data: PatternFeedbackRequest,
    user_id: str = Depends(get_current_user),
):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    action  = data.action.strip()
    comment = data.comment[:500]
    if action not in _VALID_FEEDBACK_ACTIONS:
        raise HTTPException(400, detail=f"action must be one of: {sorted(_VALID_FEEDBACK_ACTIONS)}")
    try:
        ext.ensure_tip_feedback_table(ext.DB_PATH)
        ext.log_tip_feedback(ext.DB_PATH, user_id=user_id,
                             pattern_signal_id=pattern_id, action=action, comment=comment)
        if ext.HAS_HYBRID:
            try:
                conn = sqlite3.connect(ext.DB_PATH, timeout=5)
                row = conn.execute(
                    "SELECT ticker, pattern_type FROM pattern_signals WHERE id=?", (pattern_id,)
                ).fetchone()
                conn.close()
                if row:
                    ext.log_engagement_event(ext.DB_PATH, user_id, f"tip_{action}",
                                             ticker=row[0], pattern_type=row[1])
                    ext.update_from_feedback(user_id, ext.DB_PATH)
            except Exception:
                pass
        return {"ok": True, "pattern_id": pattern_id, "action": action}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/tip/performance")
async def tip_performance(user_id: str = Depends(get_current_user)):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    try:
        return ext.get_tip_performance(ext.DB_PATH, user_id)
    except Exception as e:
        raise HTTPException(500, detail=str(e))


_TF_MAX_AGE_DAYS: dict[str, int] = {
    "1m": 1, "5m": 1, "15m": 2, "30m": 2,
    "1h": 3, "2h": 3, "4h": 5,
    "1d": 14, "1w": 30,
}
_DEFAULT_MAX_AGE_DAYS = 14


@router.get("/patterns/live")
async def patterns_live(
    ticker: Optional[str] = None,
    pattern_type: Optional[str] = None,
    direction: Optional[str] = None,
    timeframe: Optional[str] = None,
    min_quality: float = 0.0,
    limit: int = 50,
):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
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
        # Filter out stale patterns whose age exceeds their timeframe's expiry window.
        # This handles the case where yfinance is blocked and status updates haven't run.
        now = datetime.now(timezone.utc)
        def _is_fresh(p: dict) -> bool:
            ts = p.get("detected_at") or p.get("formed_at")
            if not ts:
                return True
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age_days = (now - dt).total_seconds() / 86400
                tf = (p.get("timeframe") or "").lower()
                max_age = _TF_MAX_AGE_DAYS.get(tf, _DEFAULT_MAX_AGE_DAYS)
                return age_days <= max_age
            except Exception:
                return True
        patterns = [p for p in patterns if _is_fresh(p)]
        return {"patterns": patterns, "count": len(patterns)}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.get("/patterns/{pattern_id}/context")
async def pattern_context(pattern_id: int, _user: str = Depends(get_current_user)):
    """Return pattern + top KB atoms for the pattern detail modal."""
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
    import json as _json
    # ── 1. Load pattern row ──────────────────────────────────────────────────
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
        raise HTTPException(500, detail=str(e))
    if row is None:
        raise HTTPException(404, detail="pattern not found")
    cols = ["id","ticker","pattern_type","direction","zone_high","zone_low",
            "zone_size_pct","timeframe","formed_at","status","filled_at",
            "quality_score","kb_conviction","kb_regime","kb_signal_dir","alerted_users","detected_at"]
    pattern = dict(zip(cols, row))
    try:
        pattern["alerted_users"] = _json.loads(pattern["alerted_users"] or "[]")
    except Exception:
        pattern["alerted_users"] = []
    ticker = pattern["ticker"]

    # ── 2. Load KB atoms for this ticker ────────────────────────────────────
    atoms: list = []
    now_utc = datetime.now(timezone.utc)
    try:
        conn2 = sqlite3.connect(ext.DB_PATH, timeout=10)
        fact_rows = conn2.execute(
            """SELECT predicate, object, confidence, source, timestamp
               FROM facts
               WHERE LOWER(subject) = LOWER(?)
               ORDER BY confidence DESC
               LIMIT 60""",
            (ticker,),
        ).fetchall()
        conn2.close()
        # Deduplicate by predicate — keep highest confidence per predicate
        seen_pred: dict = {}
        for fr in fact_rows:
            pred, obj, conf, src, ts = fr
            if pred not in seen_pred or conf > seen_pred[pred]["confidence"]:
                # Compute human-readable age
                age_str = "—"
                if ts:
                    try:
                        dt = datetime.fromisoformat(ts[:19])
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        diff_h = (now_utc - dt).total_seconds() / 3600
                        if diff_h < 1:
                            age_str = f"{int(diff_h * 60)}m ago"
                        elif diff_h < 24:
                            age_str = f"{int(diff_h)}h ago"
                        elif diff_h < 48:
                            age_str = "Yesterday"
                        else:
                            age_str = f"{int(diff_h / 24)}d ago"
                    except Exception:
                        age_str = ts[:10] if ts else "—"
                seen_pred[pred] = {
                    "predicate": pred,
                    "value": obj,
                    "confidence": round(float(conf), 3),
                    "source": src or "",
                    "age": age_str,
                }
        atoms = sorted(seen_pred.values(), key=lambda x: x["confidence"], reverse=True)[:30]
    except Exception:
        atoms = []

    return {
        "pattern": pattern,
        "atoms": atoms,
        "atom_count": len(atoms),
    }


@router.get("/patterns/{pattern_id}")
async def pattern_detail(pattern_id: int, user_id: Optional[str] = None):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")
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
        raise HTTPException(500, detail=str(e))

    if row is None:
        raise HTTPException(404, detail="pattern not found")

    cols = ["id","ticker","pattern_type","direction","zone_high","zone_low",
            "zone_size_pct","timeframe","formed_at","status","filled_at",
            "quality_score","kb_conviction","kb_regime","kb_signal_dir","alerted_users","detected_at"]
    pattern = dict(zip(cols, row))
    try:
        pattern["alerted_users"] = _json.loads(pattern["alerted_users"] or "[]")
    except Exception:
        pattern["alerted_users"] = []

    position = None
    if user_id:
        try:
            from analytics.pattern_detector import PatternSignal
            c = sqlite3.connect(ext.DB_PATH, timeout=5)
            pref_row = c.execute(
                "SELECT account_size, max_risk_per_trade_pct, account_currency "
                "FROM user_preferences WHERE user_id = ?", (user_id,)
            ).fetchone()
            c.close()
            if pref_row:
                prefs = dict(zip(["account_size","max_risk_per_trade_pct","account_currency"], pref_row))
                sig = PatternSignal(
                    pattern_type=pattern["pattern_type"], ticker=pattern["ticker"],
                    direction=pattern["direction"], zone_high=pattern["zone_high"],
                    zone_low=pattern["zone_low"], zone_size_pct=pattern["zone_size_pct"],
                    timeframe=pattern["timeframe"], formed_at=pattern["formed_at"],
                    quality_score=pattern["quality_score"] or 0.0, status=pattern["status"],
                    kb_conviction=pattern.get("kb_conviction",""),
                    kb_regime=pattern.get("kb_regime",""),
                    kb_signal_dir=pattern.get("kb_signal_dir",""),
                )
                pos = ext.calculate_position(sig, prefs)
                if pos is not None:
                    from dataclasses import asdict
                    position = asdict(pos)
        except Exception:
            position = None

    return {"pattern": pattern, "position": position}


@router.post("/feedback")
async def submit_feedback(request: Request, data: FeedbackRequest):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")

    # user_id: from JWT if authenticated, else body
    req_user = None
    try:
        from middleware.fastapi_auth import get_current_user_optional
        req_user = await get_current_user_optional(request)
    except Exception:
        pass
    user_id = req_user or (data.user_id or "").strip()
    if not user_id:
        raise HTTPException(400, detail="user_id is required")

    outcome = data.outcome.strip()
    if outcome not in _VALID_OUTCOMES:
        raise HTTPException(400, detail=f"outcome must be one of: {', '.join(sorted(_VALID_OUTCOMES))}")

    try:
        row = ext.log_tip_feedback(ext.DB_PATH, user_id, outcome,
                                   tip_id=data.tip_id, pattern_id=data.pattern_id)
        if ext.HAS_HYBRID and data.pattern_id is not None:
            try:
                conn = sqlite3.connect(ext.DB_PATH, timeout=5)
                try:
                    prow = conn.execute(
                        "SELECT ticker, pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                        (data.pattern_id,),
                    ).fetchone()
                finally:
                    conn.close()
                if prow:
                    ext.update_calibration(ticker=prow[0], pattern_type=prow[1],
                                           timeframe=prow[2], market_regime=prow[3] or None,
                                           outcome=outcome, db_path=ext.DB_PATH)
                    ext.update_from_feedback(user_id, {"pattern_type": prow[1], "outcome": outcome}, ext.DB_PATH)
            except Exception:
                pass
        return {"id": row["id"], "recorded": True}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/tips/{tip_id}/feedback")
async def tip_feedback_action(tip_id: int, request: Request, data: TipFeedbackRequest):
    if not ext.HAS_PATTERN_LAYER:
        raise HTTPException(503, detail="pattern layer not available")

    req_user = None
    try:
        from middleware.fastapi_auth import get_current_user_optional
        req_user = await get_current_user_optional(request)
    except Exception:
        pass
    user_id = req_user or (data.user_id or "").strip()
    if not user_id:
        raise HTTPException(400, detail="user_id required")

    action = data.action.strip()
    if action not in ("taking_it", "tell_me_more", "not_for_me"):
        raise HTTPException(400, detail="action must be taking_it|tell_me_more|not_for_me")

    pattern_id = data.pattern_id
    try:
        pattern_row = None
        if pattern_id:
            conn = sqlite3.connect(ext.DB_PATH, timeout=5)
            try:
                r = conn.execute(
                    "SELECT id, ticker, pattern_type, direction, timeframe, zone_low, zone_high, "
                    "quality_score, status, kb_conviction, kb_regime, kb_signal_dir "
                    "FROM pattern_signals WHERE id=?", (int(pattern_id),)
                ).fetchone()
                if r:
                    cols = ["id","ticker","pattern_type","direction","timeframe","zone_low","zone_high",
                            "quality_score","status","kb_conviction","kb_regime","kb_signal_dir"]
                    pattern_row = dict(zip(cols, r))
            finally:
                conn.close()

        if action == "taking_it":
            from users.user_store import create_tip_followup
            from analytics.pattern_detector import PatternSignal
            from analytics.position_calculator import calculate_position
            if not pattern_row:
                raise HTTPException(400, detail="pattern_id required for taking_it")
            conn2 = sqlite3.connect(ext.DB_PATH, timeout=5)
            try:
                prefs_row = conn2.execute(
                    "SELECT account_size, max_risk_per_trade_pct, account_currency, tier "
                    "FROM user_preferences WHERE user_id=?", (user_id,)
                ).fetchone()
            finally:
                conn2.close()
            prefs = {}
            if prefs_row:
                prefs = {"account_size": prefs_row[0] or 10000,
                         "max_risk_per_trade_pct": prefs_row[1] or 1.0,
                         "account_currency": prefs_row[2] or "GBP",
                         "tier": prefs_row[3] or "basic"}

            price_at_generation = (pattern_row["zone_low"] + pattern_row["zone_high"]) / 2.0
            price_at_feedback   = None
            sig = PatternSignal(
                pattern_type=pattern_row["pattern_type"], ticker=pattern_row["ticker"],
                direction=pattern_row["direction"], zone_high=pattern_row["zone_high"],
                zone_low=pattern_row["zone_low"], zone_size_pct=0.0,
                timeframe=pattern_row["timeframe"], formed_at="",
                quality_score=pattern_row["quality_score"] or 0.0, status=pattern_row["status"],
                kb_conviction=pattern_row.get("kb_conviction",""),
                kb_regime=pattern_row.get("kb_regime",""),
                kb_signal_dir=pattern_row.get("kb_signal_dir",""),
            )
            try:
                conn3 = sqlite3.connect(ext.DB_PATH, timeout=5)
                pr = conn3.execute(
                    "SELECT object FROM facts WHERE LOWER(subject)=? AND predicate='last_price' "
                    "ORDER BY created_at DESC LIMIT 1", (pattern_row["ticker"].lower(),)
                ).fetchone()
                conn3.close()
                if pr:
                    price_at_feedback = float(pr[0])
                    zh = (pattern_row["zone_high"] - pattern_row["zone_low"]) / 2.0
                    sig = PatternSignal(
                        pattern_type=pattern_row["pattern_type"], ticker=pattern_row["ticker"],
                        direction=pattern_row["direction"],
                        zone_high=price_at_feedback + zh, zone_low=price_at_feedback - zh,
                        zone_size_pct=0.0, timeframe=pattern_row["timeframe"], formed_at="",
                        quality_score=pattern_row["quality_score"] or 0.0, status=pattern_row["status"],
                        kb_conviction=pattern_row.get("kb_conviction",""),
                        kb_regime=pattern_row.get("kb_regime",""),
                        kb_signal_dir=pattern_row.get("kb_signal_dir",""),
                    )
            except Exception:
                pass

            pos = calculate_position(sig, prefs) if prefs else None

            cash_result = None
            try:
                from users.user_store import deduct_from_cash
                pos_val = getattr(pos, "position_value", None) or (
                    (pos.position_size_units * (price_at_feedback or price_at_generation))
                    if pos and pos.position_size_units else 0.0
                )
                if pos_val:
                    cash_result = deduct_from_cash(ext.DB_PATH, user_id, pos_val, tip_id=tip_id)
            except Exception:
                pass

            followup_id, thesis_candidates = create_tip_followup(
                ext.DB_PATH, user_id=user_id, ticker=pattern_row["ticker"],
                tip_id=tip_id, pattern_id=pattern_row["id"], direction=pattern_row["direction"],
                entry_price=pos.suggested_entry if pos else pattern_row["zone_low"],
                stop_loss=pos.stop_loss if pos else None,
                target_1=pos.target_1 if pos else None, target_2=pos.target_2 if pos else None,
                target_3=pos.target_3 if pos else None,
                position_size=pos.position_size_units if pos else None,
                regime_at_entry=pattern_row.get("kb_regime"),
                conviction_at_entry=pattern_row.get("kb_conviction"),
                pattern_type=pattern_row.get("pattern_type"),
                timeframe=pattern_row.get("timeframe"),
                zone_low=pattern_row.get("zone_low"), zone_high=pattern_row.get("zone_high"),
            )
            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pattern_row["ticker"], "user_action", "opened_position", ext.DB_PATH)
                except Exception:
                    pass
            return {
                "action": "taking_it", "followup_id": followup_id, "ticker": pattern_row["ticker"],
                "entry_price": pos.suggested_entry if pos else None,
                "stop_loss": pos.stop_loss if pos else None,
                "target_1": pos.target_1 if pos else None, "target_2": pos.target_2 if pos else None,
                "position_size": int(pos.position_size_units) if pos else None,
                "price_at_generation": round(price_at_generation, 4),
                "price_at_feedback": round(price_at_feedback, 4) if price_at_feedback else None,
                "cash_after": cash_result.get("new_balance") if cash_result else None,
                "cash_is_negative": cash_result.get("is_negative", False) if cash_result else False,
                "cash_deduction_skipped": cash_result.get("skipped", False) if cash_result else False,
                "thesis_candidates": thesis_candidates,
                "message": (f"{pattern_row['ticker']} added to monitoring — "
                            f"position monitor activated. You'll be alerted when action is needed."),
            }

        if action == "tell_me_more":
            return {"action": "tell_me_more", "tip_id": tip_id, "pattern": pattern_row,
                    "message": "Tip context loaded. Ask me anything about this setup.",
                    "suggested_questions": [
                        "What is the risk if it breaks below the zone?",
                        "How has this pattern performed in the current regime?",
                        "Does this conflict with my existing positions?",
                    ]}

        if action == "not_for_me":
            reason = data.rejection_reason.strip()
            _VALID_REASONS = {"too_risky","wrong_setup","wrong_timing","dont_know_stock","prefer_uk","no_reason"}
            if reason not in _VALID_REASONS:
                reason = "no_reason"
            ext.log_tip_feedback(ext.DB_PATH, user_id, "skipped", tip_id=tip_id, pattern_id=pattern_id)
            if ext.HAS_HYBRID and pattern_row:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pattern_row["ticker"], "user_rejection_reason", reason, ext.DB_PATH)
                    ext.update_from_feedback(user_id, {"pattern_type": pattern_row["pattern_type"],
                                                        "outcome": "skipped", "rejection_reason": reason},
                                             ext.DB_PATH)
                except Exception:
                    pass
            return {"action": "not_for_me", "rejection_reason": reason, "recorded": True,
                    "message": "Thanks — this helps improve future tips for you."}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@router.post("/tips/{followup_id}/position-update")
async def tip_position_update(followup_id: int, request: Request, data: PositionUpdateRequest):
    from users.user_store import (get_user_followups, update_followup_status,
                                   ensure_tip_followups_table)

    req_user = None
    try:
        from middleware.fastapi_auth import get_current_user_optional
        req_user = await get_current_user_optional(request)
    except Exception:
        pass
    user_id = req_user or (data.user_id or "").strip()
    if not user_id:
        raise HTTPException(400, detail="user_id required")
    action = data.action.strip()
    if action not in ("closed","hold_t2","partial","override"):
        raise HTTPException(400, detail="action must be closed|hold_t2|partial|override")

    conn = sqlite3.connect(ext.DB_PATH, timeout=5)
    try:
        ensure_tip_followups_table(conn)
        row = conn.execute(
            "SELECT id, user_id, tip_id, pattern_id, ticker, direction, entry_price, stop_loss, "
            "target_1, target_2, target_3, position_size, tracking_target, status, "
            "regime_at_entry, conviction_at_entry FROM tip_followups WHERE id=? AND user_id=?",
            (followup_id, user_id),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, detail="followup not found")
    cols = ["id","user_id","tip_id","pattern_id","ticker","direction","entry_price","stop_loss",
            "target_1","target_2","target_3","position_size","tracking_target","status",
            "regime_at_entry","conviction_at_entry"]
    pos = dict(zip(cols, row))

    try:
        if action == "closed":
            exit_price   = float(data.exit_price or pos["entry_price"] or 0)
            close_method = data.close_method
            entry        = pos["entry_price"] or exit_price
            position_size = pos["position_size"] or 1
            bullish = pos["direction"] != "bearish"
            pnl_raw = (exit_price - entry) * position_size * (1 if bullish else -1)
            pnl_pct = ((exit_price - entry) / entry * 100) if entry else 0.0
            update_followup_status(ext.DB_PATH, followup_id, status="closed")
            if ext.HAS_PATTERN_LAYER and pos.get("pattern_id"):
                try:
                    from analytics.prediction_ledger import PredictionLedger
                    PredictionLedger(ext.DB_PATH).on_price_written(pos["ticker"], exit_price)
                except Exception:
                    pass
            outcome_map = {"hit_t1":"hit_t1","hit_t2":"hit_t2","hit_t3":"hit_t3",
                           "stopped_out":"stopped_out","manual":"manual"}
            cal_outcome = outcome_map.get(close_method, "manual")
            if ext.HAS_HYBRID and pos.get("pattern_id"):
                try:
                    c2 = sqlite3.connect(ext.DB_PATH, timeout=5)
                    prow = c2.execute(
                        "SELECT ticker, pattern_type, timeframe, kb_regime FROM pattern_signals WHERE id=?",
                        (pos["pattern_id"],),
                    ).fetchone()
                    c2.close()
                    if prow:
                        ext.update_calibration(ticker=prow[0], pattern_type=prow[1],
                                               timeframe=prow[2], market_regime=prow[3] or None,
                                               outcome=cal_outcome, db_path=ext.DB_PATH)
                        ext.update_from_feedback(user_id, {"pattern_type": prow[1], "outcome": cal_outcome}, ext.DB_PATH)
                except Exception:
                    pass
            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos["ticker"], "trade_outcome", cal_outcome, ext.DB_PATH)
                    write_atom(user_id, pos["ticker"], "realised_pnl_pct", f"{pnl_pct:+.1f}%", ext.DB_PATH)
                except Exception:
                    pass
            ext.log_tip_feedback(ext.DB_PATH, user_id, cal_outcome,
                                 tip_id=pos.get("tip_id"), pattern_id=pos.get("pattern_id"))
            return {"action":"closed","ticker":pos["ticker"],"exit_price":exit_price,
                    "entry_price":entry,"pnl_gbp":round(pnl_raw,2),"pnl_pct":round(pnl_pct,2),
                    "outcome":cal_outcome,
                    "message":f"Trade closed — {pos['ticker']}: {'+' if pnl_pct>=0 else ''}{pnl_pct:.1f}%. Calibration updated."}

        elif action == "hold_t2":
            new_stop = pos["entry_price"]
            update_followup_status(ext.DB_PATH, followup_id, status="watching",
                                   tracking_target="T2", stop_loss=new_stop)
            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos["ticker"], "user_position_intent", "holding_for_t2", ext.DB_PATH)
                except Exception:
                    pass
            return {"action":"hold_t2","ticker":pos["ticker"],"tracking_target":"T2","new_stop":new_stop,
                    "message":f"Stop moved to breakeven ({new_stop}) — risk-free position. Watching for T2."}

        elif action == "partial":
            shares_closed = float(data.shares_closed or 0)
            exit_price    = float(data.exit_price or pos["entry_price"] or 0)
            orig_size     = pos["position_size"] or 0
            remainder     = max(0, orig_size - shares_closed)
            partial_pnl   = (exit_price - (pos["entry_price"] or exit_price)) * shares_closed
            c3 = sqlite3.connect(ext.DB_PATH, timeout=5)
            try:
                ensure_tip_followups_table(c3)
                c3.execute(
                    "UPDATE tip_followups SET position_size=?, status='partial', updated_at=? WHERE id=?",
                    (remainder, datetime.now(timezone.utc).isoformat(), followup_id),
                )
                c3.commit()
            finally:
                c3.close()
            return {"action":"partial","ticker":pos["ticker"],"shares_closed":shares_closed,
                    "remainder":remainder,"partial_pnl":round(partial_pnl,2),"exit_price":exit_price,
                    "message":f"Partial exit recorded — {int(shares_closed)} shares closed at {exit_price}. "
                               f"{int(remainder)} shares remaining. Monitor continues."}

        elif action == "override":
            update_followup_status(ext.DB_PATH, followup_id, status="watching", alert_level="OVERRIDE")
            if ext.HAS_HYBRID:
                try:
                    from users.personal_kb import write_atom
                    write_atom(user_id, pos["ticker"], "user_override", "held_past_stop_zone", ext.DB_PATH)
                except Exception:
                    pass
            return {"action":"override","ticker":pos["ticker"],
                    "message":"Override noted — monitoring every 15 minutes. If stop is breached a CRITICAL alert will fire."}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))

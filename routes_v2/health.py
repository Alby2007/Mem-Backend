"""routes_v2/health.py — Phase 1: health endpoints.

Gate: curl http://localhost:8001/health returns 200.
"""

from __future__ import annotations

import pathlib
import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter

import extensions as ext

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok", "db": ext.DB_PATH}


@router.get("/seed/status")
async def seed_status():
    tag_file = pathlib.Path(".seed_tag")
    last_tag = tag_file.read_text().strip() if tag_file.exists() else None

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        total_facts = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        conn.close()
    except Exception:
        total_facts = None

    now_utc = datetime.now(timezone.utc)
    push_hours = [9, 13, 17]
    next_pushes = []
    for h in push_hours:
        candidate = now_utc.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate <= now_utc:
            candidate += timedelta(days=1)
        next_pushes.append(candidate.strftime("%Y-%m-%dT%H:%M:%SZ"))

    return {
        "last_tag":    last_tag,
        "total_facts": total_facts,
        "next_pushes": next_pushes,
        "db_path":     ext.DB_PATH,
        "server_time": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }



@router.get("/health/detailed")
async def health_detailed():
    result: dict = {"status": "ok", "db": ext.DB_PATH}

    try:
        conn = sqlite3.connect(ext.DB_PATH, timeout=5)
        row = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT subject), COUNT(DISTINCT predicate) FROM facts"
        ).fetchone()
        conn.close()
        result["kb_stats"] = {
            "total_facts":       row[0],
            "unique_subjects":   row[1],
            "unique_predicates": row[2],
        }
    except Exception:
        result["kb_stats"] = None

    if ext.HAS_STRESS:
        try:
            conn2 = sqlite3.connect(ext.DB_PATH, timeout=5)
            sample_atoms = conn2.execute(
                "SELECT subject, predicate, object, confidence, source, timestamp "
                "FROM facts ORDER BY confidence DESC LIMIT 50"
            ).fetchall()
            conn2.close()
            cols = ["subject", "predicate", "object", "confidence", "source", "timestamp"]
            atoms = [dict(zip(cols, r)) for r in sample_atoms]
            sr = ext.compute_stress(atoms, [], None)
            result["kb_stress"] = sr.composite_stress
        except Exception:
            result["kb_stress"] = None

    if ext.HAS_INGEST and ext.ingest_scheduler:
        try:
            result["adapters"] = ext.ingest_scheduler.get_status()
        except Exception:
            result["adapters"] = None

    result["tip_scheduler"] = (
        "running"
        if (ext.tip_scheduler
            and getattr(ext.tip_scheduler, "_thread", None)
            and ext.tip_scheduler._thread.is_alive())
        else "stopped"
    )
    result["delivery_scheduler"] = (
        "running"
        if (ext.delivery_scheduler
            and getattr(ext.delivery_scheduler, "_thread", None)
            and ext.delivery_scheduler._thread.is_alive())
        else "stopped"
    )
    result["position_monitor"] = (
        "running"
        if (ext.position_monitor
            and getattr(ext.position_monitor, "_thread", None)
            and ext.position_monitor._thread.is_alive())
        else "stopped"
    )

    return result

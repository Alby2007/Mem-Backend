import os
import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

import extensions as ext

router = APIRouter()

STATUS_KEY = os.getenv("STATUS_KEY", "")


@router.get("/internal/status")
async def platform_status(key: str = ""):
    if not STATUS_KEY or key != STATUS_KEY:
        raise HTTPException(403, "forbidden")

    conn = sqlite3.connect(ext.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        # KB
        facts         = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        open_patterns = conn.execute("SELECT COUNT(*) FROM pattern_signals WHERE status='open'").fetchone()[0]

        # Fleet
        total_bots = conn.execute(
            "SELECT COUNT(*) FROM paper_bot_configs WHERE active=1 AND killed_at IS NULL AND role != 'discovery'"
        ).fetchone()[0]
        disc_bots = conn.execute(
            "SELECT COUNT(*) FROM paper_bot_configs WHERE active=1 AND killed_at IS NULL AND role = 'discovery'"
        ).fetchone()[0]
        open_pos = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='open'").fetchone()[0]
        disc_open = conn.execute("""
            SELECT COUNT(*) FROM paper_positions pp
            JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
            WHERE pbc.role='discovery' AND pp.status='open'
        """).fetchone()[0]
        closed_total = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE status='closed'").fetchone()[0]
        wins = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE status='closed' AND pnl_r > 0"
        ).fetchone()[0]
        closes_1h = conn.execute("""
            SELECT COUNT(*) FROM paper_positions
            WHERE status='closed' AND closed_at >= datetime('now', '-1 hour')
        """).fetchone()[0]
        entries_1h = conn.execute("""
            SELECT COUNT(*) FROM paper_positions
            WHERE opened_at >= datetime('now', '-1 hour')
        """).fetchone()[0]

        # Calibration
        cal_obs = conn.execute("SELECT COUNT(*) FROM calibration_observations").fetchone()[0]
        disc_closed = conn.execute("""
            SELECT COUNT(*) FROM paper_positions pp
            JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
            WHERE pbc.role='discovery' AND pp.status='closed'
        """).fetchone()[0]

        # Top discovery pattern (most entries in last 24h)
        top_pat_row = conn.execute("""
            SELECT pbc.pattern_types, COUNT(*) as n
            FROM paper_positions pp
            JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
            WHERE pbc.role='discovery' AND pp.opened_at >= datetime('now', '-24 hours')
            GROUP BY pbc.pattern_types ORDER BY n DESC
        """).fetchone()
        top_pattern = top_pat_row[0] if top_pat_row else None

        win_rate = round(wins / closed_total * 100, 1) if closed_total > 0 else None

        # Alerts
        alerts = []
        if closed_total >= 20 and win_rate is not None and win_rate < 30:
            alerts.append(f"⚠️ Win rate {win_rate}% after {closed_total} trades")
        if closes_1h == 0 and entries_1h == 0 and total_bots > 0:
            alerts.append("⚠️ No activity in last hour — check bots")
        if cal_obs > 0 and disc_closed >= 10:
            alerts.append(f"✅ {cal_obs} calibration obs — signals forming")

    finally:
        conn.close()

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "kb": {"facts": facts, "open_patterns": open_patterns},
        "fleet": {
            "user_bots": total_bots,
            "open_positions": open_pos - disc_open,
            "closed_total": closed_total - disc_closed,
            "wins": wins,
            "win_rate_pct": win_rate,
            "entries_last_1h": entries_1h,
            "closes_last_1h": closes_1h,
        },
        "discovery": {
            "active_bots": disc_bots,
            "open_positions": disc_open,
            "closed_positions": disc_closed,
            "calibration_obs": cal_obs,
            "top_pattern_24h": top_pattern,
        },
        "alerts": alerts,
    }

import os
import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

import extensions as ext

router = APIRouter()

STATUS_KEY = os.getenv("STATUS_KEY", "")


@router.post("/internal/replay-alerts")
async def replay_unsurfaced_alerts(key: str = ""):
    """
    Drain the backlog of unsurfaced CRITICAL/HIGH position alerts.
    Sends only the most-recent alert per (followup_id, alert_type) to avoid spam.
    Marks sent rows surfaced_tg=1.
    Protected by STATUS_KEY query param.
    """
    if not STATUS_KEY or key != STATUS_KEY:
        raise HTTPException(403, "forbidden")

    conn = sqlite3.connect(ext.DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        # Most-recent alert per (followup_id, alert_type) — CRITICAL/HIGH only
        rows = conn.execute(
            """SELECT pa.id, pa.followup_id, pa.user_id, pa.ticker,
                      pa.alert_type, pa.priority, pa.current_price,
                      pa.entry_price, pa.pnl_pct, pa.created_at,
                      tf.direction, tf.entry_price as tf_entry,
                      tf.stop_loss, tf.target_1, tf.target_2, tf.target_3,
                      tf.pattern_type, tf.zone_low, tf.zone_high,
                      tf.regime_at_entry, tf.conviction_at_entry,
                      up.telegram_chat_id
               FROM position_alerts pa
               JOIN tip_followups tf ON tf.id = pa.followup_id
               JOIN user_preferences up ON up.user_id = pa.user_id
               WHERE pa.surfaced_tg = 0
                 AND pa.priority IN ('CRITICAL','HIGH')
                 AND pa.id IN (
                     SELECT MAX(id) FROM position_alerts
                     WHERE surfaced_tg=0 AND priority IN ('CRITICAL','HIGH')
                     GROUP BY followup_id, alert_type
                 )
               ORDER BY pa.priority DESC, pa.created_at DESC""",
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"sent": 0, "skipped_no_tg": 0, "detail": "no unsurfaced alerts"}

    from notifications.telegram_notifier import TelegramNotifier

    notifier = TelegramNotifier()
    if not notifier.is_configured:
        raise HTTPException(503, "TELEGRAM_BOT_TOKEN not configured")

    sent = 0
    skipped = 0
    for r in rows:
        chat_id = r["telegram_chat_id"]
        if not chat_id:
            skipped += 1
            continue
        pos = {
            "id": r["followup_id"], "user_id": r["user_id"],
            "ticker": r["ticker"],
            "direction": r["direction"],
            "entry_price": r["tf_entry"] or r["entry_price"],
            "stop_loss": r["stop_loss"], "target_1": r["target_1"],
            "target_2": r["target_2"], "target_3": r["target_3"],
            "pattern_type": r["pattern_type"],
            "zone_low": r["zone_low"], "zone_high": r["zone_high"],
            "regime_at_entry": r["regime_at_entry"],
            "conviction_at_entry": r["conviction_at_entry"],
        }
        try:
            from notifications.tip_formatter import format_emergency_alert_with_confidence
            msg = format_emergency_alert_with_confidence(
                r["alert_type"], pos, r["current_price"] or 0.0, None
            )
            ok = notifier.send(chat_id, msg)
            if ok:
                c2 = sqlite3.connect(ext.DB_PATH, timeout=10)
                c2.execute("UPDATE position_alerts SET surfaced_tg=1 WHERE id=?", (r["id"],))
                c2.commit()
                c2.close()
                sent += 1
        except Exception as e:
            skipped += 1

    return {"sent": sent, "skipped_no_tg": skipped, "total_candidates": len(rows)}


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
        # Exclude force-closed (mcp_*) positions with null pnl_r from analytics
        # status='closed' = force/manual close; t2_hit/t1_hit/stopped_out = normal terminal states
        closed_total = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE status IN ('t2_hit','t1_hit','stopped_out','closed') AND pnl_r IS NOT NULL"
        ).fetchone()[0]
        wins         = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE status IN ('t2_hit','t1_hit','stopped_out','closed') AND pnl_r > 0"
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
        disc_closed  = conn.execute("""
            SELECT COUNT(*) FROM paper_positions pp
            JOIN paper_bot_configs pbc ON pbc.bot_id = pp.bot_id
            WHERE pbc.role='discovery' AND pp.status IN ('t2_hit','t1_hit','stopped_out','closed') AND pp.pnl_r IS NOT NULL
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

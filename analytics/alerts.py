"""
analytics/alerts.py — Alert Monitor

Monitors the KB for significant signal changes and writes structured alert
rows to the `alerts` table in trading_knowledge.db.

ALERT TYPES
===========
  conviction_change   — conviction_tier changed for a ticker since the last
                        signal_snapshot (compares current KB atom vs most recent
                        snapshot row for that ticker).
  new_high_conviction — ticker first appears with conviction_tier=high (no prior
                        snapshot OR prior snapshot had a lower tier).
  edgar_event         — new 8-K / 10-Q / 10-K atom ingested within the last
                        _EDGAR_LOOKBACK_MINUTES minutes (from edgar_realtime or
                        edgar_adapter sources).
  options_sweep       — smart_money_signal atom != 'none' for any equity ticker.

ALERTS TABLE
============
    alerts (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker       TEXT,
        alert_type   TEXT NOT NULL,
        detail       TEXT,
        triggered_at TEXT NOT NULL,   -- ISO-8601 UTC
        seen         INTEGER DEFAULT 0
    )

USAGE
=====
    from analytics.alerts import AlertMonitor, _ensure_alerts_table

    monitor = AlertMonitor(db_path)
    new_alerts = monitor.check()   # returns list of newly inserted alert dicts

    # Via API:
    #   POST /ingest/run-all  (triggers check as part of scheduler)
    #   GET  /alerts          (returns unseen or all alerts)
    #   POST /alerts/mark-seen  {"ids": [1, 2, 3]}
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional


# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_ALERTS = """
CREATE TABLE IF NOT EXISTS alerts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT,
    alert_type   TEXT NOT NULL,
    detail       TEXT,
    triggered_at TEXT NOT NULL,
    seen         INTEGER DEFAULT 0
)
"""

_EDGAR_LOOKBACK_MINUTES = 30   # look back this many minutes for new EDGAR atoms
_EDGAR_SOURCES = frozenset({
    'edgar_realtime', 'edgar_filing',
})
_EDGAR_PREDICATES = frozenset({
    'catalyst', 'risk_factor', 'material_event', 'earnings_date',
    'sec_filing', '8k', '10k', '10q',
})


def _ensure_alerts_table(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_ALERTS)
    conn.commit()


# ── Alert detection helpers ───────────────────────────────────────────────────

def _load_current_kb(conn: sqlite3.Connection) -> Dict[str, Dict[str, str]]:
    """
    Read current KB atoms relevant for alert detection.
    Returns { ticker_lower: { predicate: object } }.
    """
    c = conn.cursor()
    c.execute("""
        SELECT subject, predicate, object
        FROM facts
        WHERE predicate IN (
            'conviction_tier', 'smart_money_signal',
            'catalyst', 'risk_factor', 'material_event', 'sec_filing'
        )
        ORDER BY subject, predicate, confidence DESC
    """)
    result: Dict[str, Dict[str, str]] = {}
    for subj, pred, obj in c.fetchall():
        subj = subj.lower().strip()
        if subj not in result:
            result[subj] = {}
        if pred not in result[subj]:
            result[subj][pred] = obj
    return result


def _load_latest_snapshots(conn: sqlite3.Connection) -> Dict[str, Dict[str, str]]:
    """
    Load the most recent signal_snapshot row per ticker.
    Returns { ticker_upper: { 'conviction_tier': ..., 'snapshot_date': ... } }.
    """
    result: Dict[str, Dict[str, str]] = {}
    try:
        c = conn.cursor()
        c.execute("""
            SELECT ticker, conviction_tier, snapshot_date
            FROM signal_snapshots
            WHERE (ticker, snapshot_date) IN (
                SELECT ticker, MAX(snapshot_date)
                FROM signal_snapshots
                GROUP BY ticker
            )
        """)
        for ticker, ct, snap_date in c.fetchall():
            result[ticker.upper()] = {
                'conviction_tier': ct,
                'snapshot_date': snap_date,
            }
    except sqlite3.OperationalError:
        pass  # signal_snapshots table doesn't exist yet
    return result


def _load_recent_edgar_tickers(
    conn: sqlite3.Connection,
    lookback_minutes: int,
) -> List[str]:
    """
    Return list of tickers with new EDGAR atoms within the lookback window.
    Matches on source prefix containing 'edgar' and recent timestamp.
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    ).isoformat()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT DISTINCT subject
            FROM facts
            WHERE (source LIKE 'edgar_%' OR source LIKE '%edgar%')
              AND timestamp >= ?
        """, (cutoff,))
        return [row[0].upper() for row in c.fetchall()]
    except sqlite3.OperationalError:
        return []


def _already_alerted(
    conn: sqlite3.Connection,
    ticker: Optional[str],
    alert_type: str,
    since_iso: str,
) -> bool:
    """Return True if an identical alert was already recorded since since_iso."""
    c = conn.cursor()
    if ticker:
        c.execute("""
            SELECT 1 FROM alerts
            WHERE ticker = ? AND alert_type = ? AND triggered_at >= ?
            LIMIT 1
        """, (ticker, alert_type, since_iso))
    else:
        c.execute("""
            SELECT 1 FROM alerts
            WHERE ticker IS NULL AND alert_type = ? AND triggered_at >= ?
            LIMIT 1
        """, (alert_type, since_iso))
    return c.fetchone() is not None


def _insert_alert(
    conn: sqlite3.Connection,
    ticker: Optional[str],
    alert_type: str,
    detail: str,
    now_iso: str,
) -> dict:
    """Insert one alert row and return it as a dict."""
    cur = conn.execute(
        """INSERT INTO alerts (ticker, alert_type, detail, triggered_at, seen)
           VALUES (?, ?, ?, ?, 0)""",
        (ticker, alert_type, detail, now_iso),
    )
    return {
        'id':           cur.lastrowid,
        'ticker':       ticker,
        'alert_type':   alert_type,
        'detail':       detail,
        'triggered_at': now_iso,
        'seen':         0,
    }


# ── AlertMonitor ──────────────────────────────────────────────────────────────

class AlertMonitor:
    """
    Checks the KB for notable signal changes and writes alerts to the DB.

    Call monitor.check() from a scheduler (recommended: every 60s) or
    from the POST /ingest/run-all endpoint.

    All alert types are deduplicated within a rolling window to avoid
    repeated identical alerts from successive check() calls.
    """

    # Deduplication window: don't re-alert on the same condition within N minutes
    _DEDUP_WINDOW_MINUTES = 60

    def __init__(self, db_path: str = 'trading_knowledge.db'):
        self._db_path = db_path

    def check(self) -> List[dict]:
        """
        Run all alert checks.  Returns list of newly inserted alert dicts.
        """
        now_iso  = datetime.now(timezone.utc).isoformat()
        dedup_cutoff = (
            datetime.now(timezone.utc)
            - timedelta(minutes=self._DEDUP_WINDOW_MINUTES)
        ).isoformat()

        conn = sqlite3.connect(self._db_path, timeout=30)
        new_alerts: List[dict] = []
        try:
            _ensure_alerts_table(conn)

            kb_atoms    = _load_current_kb(conn)
            snapshots   = _load_latest_snapshots(conn)
            edgar_ticks = _load_recent_edgar_tickers(conn, _EDGAR_LOOKBACK_MINUTES)

            # ── conviction_change + new_high_conviction ────────────────────
            for ticker_lower, preds in kb_atoms.items():
                ct_now = preds.get('conviction_tier', '')
                if not ct_now:
                    continue

                ticker_upper = ticker_lower.upper()
                snap = snapshots.get(ticker_upper)

                if snap:
                    ct_prev = snap.get('conviction_tier', '')
                    if ct_prev and ct_prev != ct_now:
                        # Conviction tier changed since last snapshot
                        alert_type = 'conviction_change'
                        detail = f'{ct_prev} → {ct_now}'
                        if not _already_alerted(conn, ticker_upper, alert_type, dedup_cutoff):
                            new_alerts.append(_insert_alert(
                                conn, ticker_upper, alert_type, detail, now_iso,
                            ))
                else:
                    # No prior snapshot — check if this is a new high conviction name
                    if ct_now == 'high':
                        alert_type = 'new_high_conviction'
                        detail = f'first high-conviction signal for {ticker_upper}'
                        if not _already_alerted(conn, ticker_upper, alert_type, dedup_cutoff):
                            new_alerts.append(_insert_alert(
                                conn, ticker_upper, alert_type, detail, now_iso,
                            ))

                # ── options_sweep ──────────────────────────────────────────
                sweep = preds.get('smart_money_signal', 'none')
                if sweep and sweep != 'none':
                    alert_type = 'options_sweep'
                    detail = sweep
                    if not _already_alerted(conn, ticker_upper, alert_type, dedup_cutoff):
                        new_alerts.append(_insert_alert(
                            conn, ticker_upper, alert_type, detail, now_iso,
                        ))

            # ── edgar_event ────────────────────────────────────────────────
            for ticker_upper in edgar_ticks:
                alert_type = 'edgar_event'
                detail = f'new EDGAR filing atom for {ticker_upper}'
                if not _already_alerted(conn, ticker_upper, alert_type, dedup_cutoff):
                    new_alerts.append(_insert_alert(
                        conn, ticker_upper, alert_type, detail, now_iso,
                    ))

            if new_alerts:
                conn.commit()

        finally:
            conn.close()

        return new_alerts


# ── Query helpers (used by API endpoints) ────────────────────────────────────

def get_alerts(
    db_path: str,
    unseen_only: bool = True,
    since_iso: Optional[str] = None,
    limit: int = 200,
) -> List[dict]:
    """
    Return alerts from the DB.

    Parameters
    ----------
    unseen_only : if True, return only alerts with seen=0.
    since_iso   : ISO-8601 string — only return alerts triggered after this time.
    limit       : max rows returned (default 200).
    """
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_alerts_table(conn)
        c = conn.cursor()

        clauses = []
        params: list = []

        if unseen_only:
            clauses.append('seen = 0')
        if since_iso:
            clauses.append('triggered_at >= ?')
            params.append(since_iso)

        where = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''
        c.execute(
            f"""SELECT id, ticker, alert_type, detail, triggered_at, seen
                FROM alerts {where}
                ORDER BY triggered_at DESC
                LIMIT ?""",
            params + [limit],
        )
        cols = ['id', 'ticker', 'alert_type', 'detail', 'triggered_at', 'seen']
        return [dict(zip(cols, row)) for row in c.fetchall()]
    finally:
        conn.close()


def mark_alerts_seen(db_path: str, ids: List[int]) -> int:
    """
    Mark the given alert IDs as seen.  Returns the count of rows updated.
    """
    if not ids:
        return 0
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        _ensure_alerts_table(conn)
        placeholders = ','.join('?' for _ in ids)
        cur = conn.execute(
            f"UPDATE alerts SET seen = 1 WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()

"""
analytics/observatory_budget.py — Daily token budget for observatory API calls.

Budget limit: 50,000 tokens/day by default (configurable in observatory_budget table).
All observatory LLM calls check this before firing.
If over budget: queue for operator approval rather than auto-executing.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def get_today_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def get_budget_row(db_path: str) -> dict:
    today = get_today_str()
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute(
            '''INSERT OR IGNORE INTO observatory_budget
               (date, tokens_used, calls_made, budget_limit)
               VALUES (?, 0, 0, 50000)''',
            (today,),
        )
        conn.commit()
        row = conn.execute(
            'SELECT tokens_used, calls_made, budget_limit FROM observatory_budget WHERE date = ?',
            (today,),
        ).fetchone()
        return {'tokens_used': row[0], 'calls_made': row[1], 'budget_limit': row[2]}
    finally:
        conn.close()


def check_and_spend(
    db_path: str,
    estimated_tokens: int,
    trigger: str,
) -> tuple:
    """
    Check if estimated_tokens fits within today's budget.

    Returns (ok: bool, queue_id: Optional[int]).
    If ok=True: caller should proceed and then call record_spend().
    If ok=False: call has been queued, queue_id is the row ID.
    """
    from notifications.operator_bot import notify_budget, notify_budget_exceeded

    budget = get_budget_row(db_path)
    tokens_used  = budget['tokens_used']
    budget_limit = budget['budget_limit']

    if tokens_used + estimated_tokens <= budget_limit:
        notify_budget(estimated_tokens, tokens_used, budget_limit, trigger)
        return True, None
    else:
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            cur = conn.execute(
                '''INSERT INTO observatory_budget_queue
                   (trigger, payload, status, queued_at)
                   VALUES (?, ?, 'pending', ?)''',
                (trigger, f'estimated_tokens={estimated_tokens}',
                 datetime.now(timezone.utc).isoformat()),
            )
            queue_id = cur.lastrowid
            conn.commit()
        finally:
            conn.close()
        notify_budget_exceeded(tokens_used, budget_limit, trigger, queue_id)
        return False, queue_id


def record_spend(db_path: str, actual_tokens: int) -> None:
    today = get_today_str()
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute(
            '''UPDATE observatory_budget
               SET tokens_used = tokens_used + ?,
                   calls_made  = calls_made + 1
               WHERE date = ?''',
            (actual_tokens, today),
        )
        conn.commit()
    finally:
        conn.close()

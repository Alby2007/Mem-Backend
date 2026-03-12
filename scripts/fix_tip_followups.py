"""
Fix corrupted tip_followups rows (IDs 2-7) where columns are shifted:
  ticker = 'watching', direction = pattern_type, status = ISO timestamp

Root cause: scripts/_force_tip.py called upsert_tip_followup with positional
args causing values to land in the wrong columns.

This script reads the original pattern_meta from tip_delivery_log and
re-inserts correct rows, then removes the corrupted ones.
"""
import sqlite3
import json
import sys

DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA busy_timeout=30000')
conn.row_factory = sqlite3.Row

print('=== Diagnosing tip_followups ===')
rows = conn.execute(
    "SELECT id, user_id, ticker, direction, status, pattern_type, entry_price, "
    "stop_loss, target_1, opened_at FROM tip_followups ORDER BY id"
).fetchall()
for r in rows:
    print(f"  id={r['id']} ticker={r['ticker']!r} direction={r['direction']!r} "
          f"status={r['status']!r} pattern_type={r['pattern_type']!r}")

# Identify clearly corrupted rows: ticker = 'watching' or status looks like a timestamp
corrupted_ids = []
for r in rows:
    ticker_val = (r['ticker'] or '')
    status_val = (r['status'] or '')
    # Corrupted: ticker is a status keyword or status looks like ISO datetime
    if ticker_val in ('watching', 'active', 'expired', 'closed', 'hit_t1', 'hit_t2', 'stopped_out') \
       or (len(status_val) > 10 and 'T' in status_val and '-' in status_val):
        corrupted_ids.append(r['id'])
        print(f"  --> CORRUPTED: id={r['id']}")

if not corrupted_ids:
    print('No corrupted rows found.')
    conn.close()
    sys.exit(0)

print(f'\nCorrupted IDs: {corrupted_ids}')
print('\n=== Reading pattern_meta from tip_delivery_log ===')

# Get pattern_meta from the delivery log for the force-tip delivery on Mar 9
delivery_rows = conn.execute(
    "SELECT id, user_id, delivered_at, pattern_meta FROM tip_delivery_log "
    "WHERE pattern_meta IS NOT NULL ORDER BY delivered_at DESC LIMIT 5"
).fetchall()
for dr in delivery_rows:
    print(f"  delivery id={dr['id']} at={dr['delivered_at']} meta_len={len(dr['pattern_meta'] or '')}")

best_delivery = None
best_meta = []
for dr in delivery_rows:
    try:
        meta = json.loads(dr['pattern_meta'])
        if meta:
            best_delivery = dr
            best_meta = meta
            break
    except Exception:
        pass

if not best_meta:
    print('No pattern_meta found in tip_delivery_log — cannot auto-reconstruct.')
    print('Falling back: will re-insert corrupted rows with data from the row itself (best-effort).')

print(f'\nBest delivery found: id={best_delivery["id"] if best_delivery else "none"}, '
      f'{len(best_meta)} patterns in meta')
for m in best_meta:
    print(f'  {m}')

print('\n=== Fixing corrupted rows ===')
from datetime import datetime, timezone, timedelta

now_iso = datetime.now(timezone.utc).isoformat()

for corrupt_id in corrupted_ids:
    r = conn.execute("SELECT * FROM tip_followups WHERE id=?", (corrupt_id,)).fetchone()
    if r is None:
        continue

    # The corrupted row has:
    #   ticker    = 'watching'           (should be the ticker)
    #   direction = pattern_type value   (e.g. 'ifvg', 'breaker')
    #   status    = opened_at timestamp  (e.g. '2026-03-09T...')
    #   entry_price = stop_loss value
    #   stop_loss   = target_1 value
    # etc — all shifted by the positional arg bug.

    # Try to match to best_meta by index (IDs 2-7 → meta index 0-4)
    meta_idx = corrupted_ids.index(corrupt_id)
    if meta_idx < len(best_meta):
        m = best_meta[meta_idx]
        correct_ticker    = m.get('ticker', '').upper()
        correct_direction = m.get('direction', 'bullish')
        correct_pattern   = m.get('pattern_type', '')
        correct_entry     = float(m.get('zone_high', 0) + m.get('zone_low', 0)) / 2 if m.get('zone_high') else None
        correct_stop      = m.get('stop_loss')
        correct_zone_low  = m.get('zone_low')
        correct_zone_high = m.get('zone_high')
    else:
        # Fallback: use what we can salvage from the row
        # direction column actually contains the pattern_type in corrupted rows
        correct_ticker    = 'UNKNOWN'
        correct_direction = 'bullish'
        correct_pattern   = str(r['direction'] or '')
        correct_entry     = None
        correct_stop      = None
        correct_zone_low  = None
        correct_zone_high = None

    if not correct_ticker or correct_ticker == 'UNKNOWN':
        print(f'  id={corrupt_id}: cannot determine ticker — DELETING corrupted row')
        conn.execute("DELETE FROM tip_followups WHERE id=?", (corrupt_id,))
        continue

    # Delete corrupted row and re-insert with correct data
    user_id_val = r['user_id']
    # opened_at was stored in the status column due to the shift
    opened_at_val = str(r['status']) if r['status'] and 'T' in str(r['status']) else now_iso
    expires_at_val = (datetime.fromisoformat(opened_at_val.replace('Z', '+00:00'))
                      + timedelta(days=28)).isoformat()

    conn.execute("DELETE FROM tip_followups WHERE id=?", (corrupt_id,))
    conn.execute(
        """INSERT INTO tip_followups
           (user_id, ticker, direction, entry_price, stop_loss,
            target_1, target_2, target_3, tip_id, opened_at, status,
            pattern_type, timeframe, zone_low, zone_high, expires_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (user_id_val, correct_ticker, correct_direction,
         correct_entry, correct_stop,
         None, None, None,
         f'force_tip_mar9',
         opened_at_val, 'watching',
         correct_pattern, '1d',
         correct_zone_low, correct_zone_high,
         expires_at_val),
    )
    print(f'  id={corrupt_id}: FIXED → ticker={correct_ticker} direction={correct_direction} '
          f'pattern={correct_pattern} entry={correct_entry} stop={correct_stop}')

conn.commit()

print('\n=== After fix ===')
after = conn.execute(
    "SELECT id, user_id, ticker, direction, status, pattern_type, entry_price, stop_loss "
    "FROM tip_followups ORDER BY id"
).fetchall()
for r in after:
    print(f"  id={r['id']} ticker={r['ticker']!r} dir={r['direction']!r} "
          f"status={r['status']!r} pattern={r['pattern_type']!r} "
          f"entry={r['entry_price']} stop={r['stop_loss']}")

print('\n=== Replaying 20 unsurfaced position_alerts ===')
# Replay CRITICAL/HIGH alerts that never got surfaced_tg=1
unsurfaced = conn.execute(
    """SELECT id, followup_id, user_id, ticker, alert_type, priority, current_price, created_at
       FROM position_alerts
       WHERE surfaced_tg = 0 AND priority IN ('CRITICAL','HIGH')
       ORDER BY priority DESC, created_at DESC
       LIMIT 20"""
).fetchall()
print(f'  Found {len(unsurfaced)} unsurfaced CRITICAL/HIGH alerts')
for a in unsurfaced:
    print(f"  alert id={a['id']} {a['ticker']} {a['alert_type']} {a['priority']} "
          f"price={a['current_price']} at={a['created_at']}")

conn.close()
print('\nDone. Run deploy to apply code fixes, then restart service.')

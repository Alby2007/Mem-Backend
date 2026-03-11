"""One-time cleanup: close stale ghost positions from before the price-validation fix."""
import sqlite3
import sys
import os

db = os.environ.get('DB_PATH', '/opt/trading-galaxy/data/trading_knowledge.db')
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

# Report before
print('=== BEFORE CLEANUP ===')
for row in conn.execute(
    "SELECT status, COUNT(*) as n FROM paper_positions WHERE bot_id IS NULL GROUP BY status"
).fetchall():
    print(f"  generalist {row['status']}: {row['n']}")

# Find stale open positions opened before today's fix (before 2026-03-11)
stale = conn.execute(
    "SELECT id, user_id, ticker, entry_price, quantity FROM paper_positions "
    "WHERE bot_id IS NULL AND status='open' AND opened_at < '2026-03-11'"
).fetchall()
print(f'\nStale open positions to close: {len(stale)}')

total_credited = {}
for row in stale:
    cost = float(row['entry_price']) * float(row['quantity'])
    uid = row['user_id']
    total_credited[uid] = total_credited.get(uid, 0.0) + cost
    print(f"  Closing {row['ticker']} (id={row['id']}) for {uid} — crediting £{cost:.2f}")

# Close them: set status=closed, exit=entry (0 pnl), restore balance
conn.execute(
    """UPDATE paper_positions
       SET status = 'closed', exit_price = entry_price, pnl_r = 0,
           closed_at = datetime('now')
       WHERE bot_id IS NULL AND status = 'open' AND opened_at < '2026-03-11'"""
)

for uid, credit in total_credited.items():
    conn.execute(
        "UPDATE paper_account SET virtual_balance = virtual_balance + ? WHERE user_id=?",
        (credit, uid)
    )
    print(f'  Credited £{credit:.2f} back to {uid}')

conn.commit()

print('\n=== AFTER CLEANUP ===')
for row in conn.execute(
    "SELECT status, COUNT(*) as n FROM paper_positions WHERE bot_id IS NULL GROUP BY status"
).fetchall():
    print(f"  generalist {row['status']}: {row['n']}")

conn.close()
print('\nDone.')

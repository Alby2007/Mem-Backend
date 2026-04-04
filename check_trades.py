import sqlite3
conn = sqlite3.connect('/opt/trading-galaxy/data/trading_knowledge.db')
conn.row_factory = sqlite3.Row

bots = conn.execute("SELECT bot_id, strategy_name, virtual_balance, initial_balance FROM paper_bot_configs WHERE active=1").fetchall()
for b in bots:
    trades = conn.execute("""
        SELECT ticker, direction, entry_price, exit_price, quantity, pnl_r, status, opened_at, closed_at
        FROM paper_positions WHERE bot_id=? AND status!='open'
        ORDER BY closed_at DESC LIMIT 5
    """, (b['bot_id'],)).fetchall()
    if not trades:
        continue
    print(f"\n=== {b['strategy_name']} (cash={b['virtual_balance']:.2f}, initial={b['initial_balance']:.2f}) ===")
    for t in trades:
        cost  = t['entry_price'] * t['quantity']
        recvd = (t['exit_price'] or 0) * t['quantity']
        cash_chg = recvd - cost
        print(f"  {t['ticker']:6} {t['direction']:5} entry={t['entry_price']:8.2f} exit={t['exit_price'] or 0:8.2f} qty={t['quantity']:.4f} cost={cost:.2f} rcvd={recvd:.2f} cash_chg={cash_chg:+.2f} pnl_r={t['pnl_r']}")

conn.close()

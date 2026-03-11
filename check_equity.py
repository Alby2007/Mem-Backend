import sqlite3
conn = sqlite3.connect('/opt/trading-galaxy/data/trading_knowledge.db')
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT b.strategy_name, b.virtual_balance, b.initial_balance,
           COALESCE(SUM(p.entry_price * p.quantity),0) as open_val,
           b.virtual_balance + COALESCE(SUM(p.entry_price * p.quantity),0) as equity
    FROM paper_bot_configs b
    LEFT JOIN paper_positions p ON p.bot_id=b.bot_id AND p.status='open'
    WHERE b.active=1
    GROUP BY b.bot_id
""").fetchall()
for r in rows:
    ret = (r['equity'] - r['initial_balance']) / r['initial_balance'] * 100 if r['initial_balance'] else 0
    print(f"{r['strategy_name'][:22]:22} cash={r['virtual_balance']:8.2f} open={r['open_val']:8.2f} equity={r['equity']:8.2f} initial={r['initial_balance']:8.2f} ret={ret:+.1f}%")
conn.close()

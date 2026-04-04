#!/usr/bin/env python3
"""
Check live DB, seed patterns if empty, run paper agent, show reasoning.
Run with: TRADING_KB_DB=/opt/trading-galaxy/data/trading_knowledge.db python3 scripts/seed_and_test.py
"""
import sys, os, sqlite3, json
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

DB = os.environ.get('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')
print(f'Using DB: {DB}')
print(f'DB size: {os.path.getsize(DB)//1024}KB')

c = sqlite3.connect(DB)
facts = c.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
last_price_subjects = c.execute("SELECT COUNT(DISTINCT subject) FROM facts WHERE predicate='last_price'").fetchone()[0]
patterns = c.execute('SELECT COUNT(*) FROM pattern_signals').fetchone()[0]
active_patterns = c.execute("SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken')").fetchone()[0]
c.close()

print(f'Facts: {facts}')
print(f'Subjects with last_price: {last_price_subjects}')
print(f'Pattern signals total: {patterns}, active: {active_patterns}')

# --- Seed patterns if none active ---
if active_patterns == 0 and last_price_subjects > 0:
    print('\nNo active patterns — running seed_patterns...')
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'seed_patterns', '/home/ubuntu/trading-galaxy/scripts/seed_patterns.py'
    )
    mod = importlib.util.load_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()
elif last_price_subjects == 0:
    print('\nWARNING: No last_price atoms in DB — KB may not be seeded.')
    # Show sample facts to understand what's there
    c2 = sqlite3.connect(DB)
    sample = c2.execute('SELECT subject, predicate, object FROM facts LIMIT 10').fetchall()
    print(f'Sample facts: {sample}')
    top_pred = c2.execute('SELECT predicate, COUNT(*) n FROM facts GROUP BY predicate ORDER BY n DESC LIMIT 10').fetchall()
    print(f'Top predicates: {top_pred}')
    c2.close()
    sys.exit(1)
else:
    print(f'\nAlready have {active_patterns} active patterns — skipping seed.')

# --- Run paper agent ---
print('\n--- Running paper agent ---')
from services.paper_trading import ai_run as _paper_ai_run
result = _paper_ai_run('albertjemmettwaite_uggwq')
print('Result:', json.dumps(result, indent=2))

# --- Show last 15 log entries ---
c3 = sqlite3.connect(DB)
c3.row_factory = sqlite3.Row
rows = c3.execute(
    "SELECT event_type, ticker, detail, created_at FROM paper_agent_log "
    "WHERE user_id='albertjemmettwaite_uggwq' ORDER BY id DESC LIMIT 15",
).fetchall()
c3.close()

print('\n--- Last 15 agent log entries ---')
for r in reversed(rows):
    print(f"[{r['created_at'][:19]}] {r['event_type']:12s} {(r['ticker'] or ''):8s} {r['detail']}")

"""Run the tip preview logic directly for the real user."""
import os, sys, json, sqlite3
DB = '/opt/trading-galaxy/data/trading_knowledge.db'
USER = 'albertjemmettwaite_uggwq'
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, '/home/ubuntu/trading-galaxy')

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
row = conn.execute(
    "SELECT tier, tip_timeframes, tip_pattern_types, tip_markets, "
    "account_size, max_risk_per_trade_pct, account_currency "
    "FROM user_preferences WHERE user_id=?", (USER,)
).fetchone()
prefs = dict(row)
conn.close()
tier = prefs.get('tier') or 'basic'
for jcol in ('tip_timeframes', 'tip_pattern_types', 'tip_markets'):
    try:
        prefs[jcol] = json.loads(prefs[jcol]) if prefs[jcol] else None
    except Exception:
        prefs[jcol] = None

print('Tier:', tier)
print('Timeframes:', prefs['tip_timeframes'])
print('Markets:', prefs['tip_markets'])

from core.tiers import TIER_CONFIG as TIER_LIMITS
limits = TIER_LIMITS.get(tier, TIER_LIMITS['basic'])
tip_timeframes = prefs.get('tip_timeframes') or limits.get('timeframes', ['1h'])
tip_pattern_tys = prefs.get('tip_pattern_types')
tip_markets = prefs.get('tip_markets')
delivery_days = limits.get('delivery_days', 'daily')
print('Delivery days:', delivery_days)
print('Limits:', {k: v for k, v in limits.items() if k in ('delivery_days', 'timeframes', 'batch_size')})

from notifications.tip_scheduler import _pick_best_pattern, _pick_batch
is_weekly = delivery_days != 'daily'
print('Is weekly:', is_weekly)

if is_weekly:
    batch_size = limits.get('batch_size', 3)
    batch, tip_source = _pick_batch(DB, USER, tier, tip_timeframes, tip_pattern_tys, tip_markets, batch_size)
    print(f'Batch size: {len(batch)}, source: {tip_source}')
    for r in batch[:3]:
        print(' -', r.get('ticker'), r.get('pattern_type'), r.get('quality_score'))
else:
    pr = _pick_best_pattern(DB, USER, tier, tip_timeframes, tip_pattern_tys, tip_markets)
    if pr is None:
        print('No eligible patterns from _pick_best_pattern')
        # Show raw count in pattern_signals
        conn2 = sqlite3.connect(DB)
        total = conn2.execute("SELECT COUNT(*) FROM pattern_signals WHERE status='open'").fetchone()[0]
        print('Total open signals:', total)
        # Check timeframe filter
        for tf in (tip_timeframes or ['1h']):
            n = conn2.execute(
                "SELECT COUNT(*) FROM pattern_signals WHERE status='open' AND timeframe=?", (tf,)
            ).fetchone()[0]
            print(f'  timeframe={tf}: {n}')
        # Check alerted
        alerted = conn2.execute(
            "SELECT COUNT(*) FROM pattern_signals WHERE status='open' AND alerted_users LIKE ?",
            (f'%{USER}%',)
        ).fetchone()[0]
        print(f'  already alerted to this user: {alerted}')
        conn2.close()
    else:
        print('Best pattern:', pr.get('ticker'), pr.get('pattern_type'), pr.get('quality_score'))

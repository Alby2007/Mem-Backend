"""Diagnose Risk-Off Shorts conviction gap."""
import sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
DB = '/opt/trading-galaxy/data/trading_knowledge.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

cfg = conn.execute("SELECT * FROM paper_bot_configs WHERE strategy_name='Risk-Off Shorts'").fetchone()
print('--- Bot Config ---')
print(f'min_quality={cfg["min_quality"]} direction_bias={cfg["direction_bias"]} pattern_types={cfg["pattern_types"]}')

qual_floor = max(cfg['min_quality'] - 0.05, 0.55)
print(f'quality_floor used by bot_runner: max({cfg["min_quality"]}-0.05, 0.55) = {qual_floor}')

n = conn.execute(
    "SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken','expired')"
    " AND quality_score >= ? AND direction='bearish'", (qual_floor,)
).fetchone()[0]
print(f'Bearish candidates at q>={qual_floor}: {n}')

n2 = conn.execute(
    "SELECT COUNT(*) FROM pattern_signals WHERE status NOT IN ('filled','broken','expired')"
    " AND quality_score >= ? AND direction='bearish'"
    " AND kb_conviction IS NOT NULL AND kb_conviction != ''", (qual_floor,)
).fetchone()[0]
print(f'  ...with kb_conviction set: {n2}')

rows3 = conn.execute(
    "SELECT ticker, quality_score, kb_conviction, pattern_type FROM pattern_signals"
    " WHERE status NOT IN ('filled','broken','expired')"
    " AND quality_score >= 0.50 AND direction='bearish'"
    " AND (kb_conviction IS NULL OR kb_conviction='')"
    " ORDER BY quality_score DESC LIMIT 10"
).fetchall()
print('\nTop unenriched bearish patterns:')
for r in rows3:
    print(f'  {r[0]:15s} q={r[1]:.3f} type={r[3]}')

tickers_to_check = list({r[0] for r in rows3[:6]})
print('\nFacts for unenriched tickers:')
for t in tickers_to_check:
    facts = conn.execute(
        "SELECT predicate, object FROM facts WHERE LOWER(subject)=?"
        " AND predicate IN ('conviction_tier','signal_direction','signal_quality','price_regime')"
        " ORDER BY timestamp DESC LIMIT 4",
        (t.lower(),)
    ).fetchall()
    print(f'  {t}: {[(r[0], r[1]) for r in facts]}')

conn.close()

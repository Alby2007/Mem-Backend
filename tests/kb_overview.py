"""Quick KB health snapshot — run directly, no deps beyond stdlib."""
import sqlite3, sys
sys.path.insert(0, '.')

conn = sqlite3.connect('trading_knowledge.db')
c = conn.cursor()

c.execute('SELECT COUNT(*) FROM facts')
total = c.fetchone()[0]

c.execute('SELECT COUNT(DISTINCT subject) FROM facts')
subjects = c.fetchone()[0]

c.execute('SELECT COUNT(DISTINCT predicate) FROM facts')
predicates = c.fetchone()[0]

c.execute('SELECT COUNT(*) FROM fact_conflicts')
conflicts = c.fetchone()[0]

c.execute("SELECT COUNT(*) FROM facts WHERE source LIKE 'llm_extracted_%'")
llm_atoms = c.fetchone()[0]

c.execute('SELECT COUNT(*) FROM facts WHERE confidence_effective IS NOT NULL AND confidence_effective < 0.1')
stale = c.fetchone()[0]

c.execute("""SELECT
  SUM(CASE WHEN confidence >= 0.85 THEN 1 ELSE 0 END),
  SUM(CASE WHEN confidence >= 0.70 AND confidence < 0.85 THEN 1 ELSE 0 END),
  SUM(CASE WHEN confidence >= 0.55 AND confidence < 0.70 THEN 1 ELSE 0 END),
  SUM(CASE WHEN confidence < 0.55 THEN 1 ELSE 0 END)
FROM facts""")
conf = c.fetchone()

c.execute('SELECT predicate, COUNT(*) as n FROM facts GROUP BY predicate ORDER BY n DESC LIMIT 25')
by_pred = c.fetchall()

c.execute("""
    SELECT
        CASE
            WHEN source LIKE 'llm_extracted_%' THEN 'llm_extracted_'
            WHEN source LIKE 'exchange_feed%'  THEN 'exchange_feed'
            WHEN source LIKE 'regulatory_filing%' THEN 'regulatory_filing'
            WHEN source LIKE 'news_wire_%'     THEN 'news_wire_'
            WHEN source LIKE 'derived_signal_%' THEN 'derived_signal_'
            WHEN source LIKE 'model_signal_%'  THEN 'model_signal_'
            WHEN source LIKE 'macro_data%'     THEN 'macro_data'
            WHEN source LIKE 'broker_research%' THEN 'broker_research'
            WHEN source LIKE 'earnings_%'      THEN 'earnings_'
            WHEN source LIKE 'curated_%'       THEN 'curated_'
            ELSE source
        END as src_prefix,
        COUNT(*) as n
    FROM facts
    GROUP BY src_prefix
    ORDER BY n DESC
""")
by_src = c.fetchall()

try:
    c.execute('SELECT processed, COUNT(*) FROM extraction_queue GROUP BY processed')
    queue = dict(c.fetchall())
except Exception:
    queue = {}

try:
    c.execute('SELECT COUNT(*) FROM extraction_queue WHERE failed_attempts >= 3')
    queue_dead = c.fetchone()[0]
except Exception:
    queue_dead = 0

# Derived signal coverage: how many unique tickers have conviction_tier
c.execute("SELECT COUNT(DISTINCT subject) FROM facts WHERE predicate = 'conviction_tier'")
ct_coverage = c.fetchone()[0]

c.execute("SELECT COUNT(DISTINCT subject) FROM facts WHERE predicate = 'thesis_risk_level'")
trl_coverage = c.fetchone()[0]

c.execute("SELECT COUNT(DISTINCT subject) FROM facts WHERE predicate = 'signal_quality'")
sq_coverage = c.fetchone()[0]

# conviction_tier breakdown
c.execute("SELECT object, COUNT(*) FROM facts WHERE predicate='conviction_tier' GROUP BY object ORDER BY COUNT(*) DESC")
ct_dist = c.fetchall()

# thesis_risk_level breakdown
c.execute("SELECT object, COUNT(*) FROM facts WHERE predicate='thesis_risk_level' GROUP BY object ORDER BY COUNT(*) DESC")
trl_dist = c.fetchall()

# signal_quality breakdown
c.execute("SELECT object, COUNT(*) FROM facts WHERE predicate='signal_quality' GROUP BY object ORDER BY COUNT(*) DESC")
sq_dist = c.fetchall()

conn.close()

print("=" * 55)
print("  TRADING KB — HEALTH SNAPSHOT")
print("=" * 55)
print(f"  Total facts:          {total:>6,}")
print(f"  Unique subjects:      {subjects:>6,}")
print(f"  Unique predicates:    {predicates:>6,}")
print(f"  Conflict events:      {conflicts:>6,}")
print(f"  Stale atoms (<0.1):   {stale:>6,}")
print(f"  LLM-extracted atoms:  {llm_atoms:>6,}")
print()
print("  Confidence distribution")
print(f"    high   >=0.85       {conf[0]:>6,}  ({conf[0]/total*100:.0f}%)")
print(f"    medium 0.70–0.85    {conf[1]:>6,}  ({conf[1]/total*100:.0f}%)")
print(f"    low    0.55–0.70    {conf[2]:>6,}  ({conf[2]/total*100:.0f}%)")
print(f"    vlow   <0.55        {conf[3]:>6,}  ({conf[3]/total*100:.0f}%)")
print()
print("  Extraction queue")
print(f"    pending:   {queue.get(0,0):>6,}")
print(f"    processed: {queue.get(1,0):>6,}")
print(f"    dead (>=3 fails): {queue_dead:>3,}")
print()
print("  Derived signal coverage (unique tickers)")
print(f"    signal_quality:     {sq_coverage:>4,}  {[f'{v}={n}' for v,n in sq_dist]}")
print(f"    thesis_risk_level:  {trl_coverage:>4,}  {[f'{v}={n}' for v,n in trl_dist]}")
print(f"    conviction_tier:    {ct_coverage:>4,}  {[f'{v}={n}' for v,n in ct_dist]}")
print()
print("  Top predicates")
for pred, n in by_pred:
    bar = '#' * (n * 30 // (by_pred[0][1] or 1))
    print(f"    {n:5,}  {pred:<35} {bar}")
print()
print("  Atoms by source prefix")
for src, n in by_src:
    print(f"    {n:5,}  {src}")

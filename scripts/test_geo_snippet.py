"""
Run a geo retrieval for 'tell me about the war in russia' and print the snippet.
Usage: python3 scripts/test_geo_snippet.py
"""
import os, sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.environ.setdefault('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')

db = os.environ['TRADING_KB_DB']

# ── Direct DB checks BEFORE calling retrieve ───────────────────────────────
conn_raw = sqlite3.connect(db)
conn_raw.row_factory = sqlite3.Row

print("=== DIRECT DB CHECKS ===")

# 1. UCDP predicate=iso check
for iso in ('rus', 'ukr'):
    rows = conn_raw.execute(
        "SELECT subject,predicate,object FROM facts WHERE subject='ucdp_conflict' AND LOWER(predicate)=? LIMIT 5",
        (iso,)
    ).fetchall()
    print(f"  ucdp_conflict|{iso}: {[tuple(r) for r in rows]}")

# 2. GDELT predicate LIKE russia
rows = conn_raw.execute(
    "SELECT subject,predicate,object FROM facts WHERE LOWER(predicate) LIKE '%russia%' LIMIT 5"
).fetchall()
print(f"  predicate LIKE russia: {[tuple(r) for r in rows]}")

# 3. Object LIKE russia (the key_finding/catalyst atoms)
rows = conn_raw.execute(
    "SELECT subject,predicate,SUBSTR(object,1,60) FROM facts WHERE LOWER(object) LIKE '%russia%' "
    "AND predicate IN ('key_finding','headline','summary','catalyst','risk_factor') "
    "ORDER BY timestamp DESC LIMIT 5"
).fetchall()
print(f"  object LIKE russia (news): {[tuple(r) for r in rows]}")

# 4. Check _asked_entities detection
msg_lower = 'tell me about the war in russia'
GEO_KEYWORDS = ('war','conflict','attack','strike','military','iran','russia','ukraine')
GEO_ENTITY_MAP = {
    'russia': ['%russia%','%russian%','%kremlin%','%moscow%'],
    'ukraine': ['%ukraine%','%ukrainian%','%kyiv%'],
    'iran': ['%iran%','%iranian%','%tehran%'],
}
is_geo = any(kw in msg_lower for kw in GEO_KEYWORDS)
asked = [e for e in GEO_ENTITY_MAP if e in msg_lower]
print(f"  is_geo_query: {is_geo}, asked_entities: {asked}")

conn_raw.close()

# ── Step-by-step geo retrieval inline ────────────────────────────────────────
print("\n=== INLINE GEO RETRIEVAL ===")
conn_step = sqlite3.connect(db)

# 1a: object LIKE russia in key predicates
rows = conn_step.execute(
    "SELECT subject,predicate,object,source,confidence FROM facts "
    "WHERE LOWER(object) LIKE ? "
    "AND predicate IN ('key_finding','headline','summary','event','catalyst','risk_factor',"
    "'conflict_status','parties_involved','location','severity','escalation') "
    "ORDER BY timestamp DESC, confidence DESC LIMIT 15",
    ('%russia%',)
).fetchall()
print(f"  Step 1a (object LIKE %russia%): {len(rows)} rows")
for r in rows[:5]: print(f"    subj={r[0]} pred={r[1]} src={r[3]} conf={r[4]} obj={str(r[2])[:40]}")

# 1b: UCDP ISO predicate
rows = conn_step.execute(
    "SELECT subject,predicate,object,source,confidence FROM facts "
    "WHERE subject='ucdp_conflict' AND LOWER(predicate)=?",
    ('rus',)
).fetchall()
print(f"  Step 1b (ucdp_conflict|rus): {len(rows)} rows")
for r in rows: print(f"    {r[0]}|{r[1]}|{r[2]}")

# 1c: predicate LIKE russia
rows = conn_step.execute(
    "SELECT subject,predicate,object,source,confidence FROM facts "
    "WHERE LOWER(predicate) LIKE ? ORDER BY confidence DESC LIMIT 10",
    ('%russia%',)
).fetchall()
print(f"  Step 1c (predicate LIKE %russia%): {len(rows)} rows")
for r in rows[:5]: print(f"    {r[0]}|{r[1]}|{r[2]}")

# 1d: subject LIKE russia
rows = conn_step.execute(
    "SELECT subject,predicate,object,source,confidence FROM facts "
    "WHERE LOWER(subject) LIKE ? ORDER BY confidence DESC LIMIT 10",
    ('%russia%',)
).fetchall()
print(f"  Step 1d (subject LIKE %russia%): {len(rows)} rows")
for r in rows[:5]: print(f"    {r[0]}|{r[1]}|{str(r[2])[:50]}")

# news_wire filtered
rows = conn_step.execute(
    "SELECT subject,predicate,object,source,confidence FROM facts "
    "WHERE (source LIKE 'news_wire_%' OR source IN ('geopolitical_data_gdelt','geopolitical_data_acled','geopolitical_data_ucdp')) "
    "AND predicate IN ('key_finding','headline','summary','event','catalyst','risk_factor') "
    "AND LOWER(object) LIKE ? "
    "ORDER BY timestamp DESC LIMIT 20",
    ('%russia%',)
).fetchall()
print(f"  Step 3 (news_wire LIKE %russia%): {len(rows)} rows")
for r in rows[:5]: print(f"    {r[0]}|{r[1]}|{str(r[2])[:50]}")

conn_step.close()

# ── Full retrieve call ────────────────────────────────────────────────────────
from retrieval import retrieve
from knowledge import KnowledgeGraph

kg = KnowledgeGraph(db_path=db)
conn = kg.thread_local_conn()

snippet, atoms = retrieve('tell me about the war in russia', conn, limit=30)

print(f"\n=== ATOMS RETURNED: {len(atoms)} ===")
geo_atoms = [a for a in atoms if any(x in (a.get('source','') + a.get('subject','') + a.get('object','').lower()) for x in ('russia','ukraine','gdelt','ucdp','news_wire'))]
print(f"  Geo-related atoms: {len(geo_atoms)}")
for a in geo_atoms:
    print(f"  {a['subject']} | {a['predicate']} | {str(a['object'])[:60]}")
print(f"\n  Non-geo atoms: {len(atoms) - len(geo_atoms)}")
print("\n=== SNIPPET (geo section only) ===")
lines = snippet.split('\n')
in_geo = False
for line in lines:
    if '# geopolitical' in line: in_geo = True
    if in_geo: print(line)
    if in_geo and line.startswith('#') and '# geopolitical' not in line: break

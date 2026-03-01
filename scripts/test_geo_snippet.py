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

# ── Now call retrieve ───────────────────────────────────────────────────────
from retrieval import retrieve
from knowledge import KnowledgeGraph

kg = KnowledgeGraph(db_path=db)
conn = kg.thread_local_conn()

snippet, atoms = retrieve('tell me about the war in russia', conn, limit=30)

print(f"\n=== ATOMS RETURNED: {len(atoms)} ===")
print("Atom subjects/predicates:")
for a in atoms:
    print(f"  {a['subject']} | {a['predicate']} | {str(a['object'])[:50]}")
print()
print("=== SNIPPET ===")
print(snippet[:4000])

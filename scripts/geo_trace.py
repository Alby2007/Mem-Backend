"""
Trace exactly what happens inside retrieve() for a Russia geo query.
Patches _add_geo to print every call and result.
"""
import os, sys, sqlite3
sys.path.insert(0, '/home/ubuntu/trading-galaxy')
os.environ['TRADING_KB_DB'] = '/opt/trading-galaxy/data/trading_knowledge.db'

db = os.environ['TRADING_KB_DB']

# Monkey-patch retrieval to trace _add_geo
import retrieval as _ret

_orig_retrieve = _ret.retrieve

def _traced_retrieve(message, conn, limit=30, nudges=None):
    # Patch _add_geo by wrapping retrieve internals via a subclass trick
    # Instead: run retrieve normally but add logging via _logger
    import logging
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
                        format='%(name)s %(levelname)s %(message)s')
    return _orig_retrieve(message, conn, limit=limit, nudges=nudges)

from knowledge import KnowledgeGraph
kg = KnowledgeGraph(db_path=db)
conn = kg.thread_local_conn()

# Direct check: what does cursor return for step 1a?
import sqlite3 as _sq
c2 = conn.cursor()
c2.execute(
    "SELECT subject, predicate, object, source, confidence "
    "FROM facts WHERE LOWER(object) LIKE ? "
    "AND predicate IN ('key_finding','headline','summary','event',"
    "'catalyst','risk_factor','conflict_status','parties_involved',"
    "'location','severity','escalation') "
    "ORDER BY timestamp DESC, confidence DESC LIMIT 15",
    ('%russia%',)
)
rows = c2.fetchall()
print(f"Direct cursor step1a: {len(rows)} rows", file=sys.stderr)
for r in rows[:3]:
    print(f"  type={type(r).__name__} r={tuple(r)[:3]}", file=sys.stderr)

# Now trace _add_geo manually
seen = set()
geo_results = []
_geo_seen = set()

_NOISE_PREDICATES = {'source_code', 'has_title', 'has_section', 'has_content'}

def _normalise(r):
    try:
        if hasattr(r, 'keys'):
            return {
                'subject':    str(r['subject'] or '').strip(),
                'predicate':  str(r['predicate'] or '').strip(),
                'object':     str(r['object'] or '')[:300].strip(),
                'source':     str(r['source'] if 'source' in r.keys() else '').strip(),
                'confidence': float(r['confidence']) if 'confidence' in r.keys() else 0.5,
            }
        return {
            'subject': str(r[0]).strip(), 'predicate': str(r[1]).strip(),
            'object': str(r[2])[:300].strip(),
            'source': str(r[3]).strip() if len(r) > 3 else '',
            'confidence': float(r[4]) if len(r) > 4 else 0.5,
        }
    except Exception as e:
        print(f"  _normalise EXCEPTION: {e} r={r}", file=sys.stderr)
        return None

def _add_geo_trace(rows):
    rlist = list(rows)
    print(f"  _add_geo_trace: {len(rlist)} rows", file=sys.stderr)
    for r in rlist:
        atom = _normalise(r)
        if not atom:
            print(f"    SKIP: _normalise returned None for {r}", file=sys.stderr)
            continue
        key = (atom['subject'][:60], atom['predicate'], atom['object'][:60])
        if atom['predicate'] in _NOISE_PREDICATES:
            print(f"    SKIP noise pred: {atom['predicate']}", file=sys.stderr)
            continue
        if not atom['subject']:
            print(f"    SKIP empty subject", file=sys.stderr)
            continue
        if not atom['object']:
            print(f"    SKIP empty object: subj={atom['subject']} pred={atom['predicate']}", file=sys.stderr)
            continue
        print(f"    PASS: {atom['subject']}|{atom['predicate']}|{str(atom['object'])[:40]}", file=sys.stderr)
        if key not in seen:
            seen.add(key)
        if key not in _geo_seen:
            _geo_seen.add(key)
            geo_results.append(atom)
    print(f"  geo_results now: {len(geo_results)}", file=sys.stderr)

print("=== Tracing step 1a ===", file=sys.stderr)
_add_geo_trace(rows)

print(f"\n=== geo_results after step1a: {len(geo_results)} ===", file=sys.stderr)
for a in geo_results:
    print(f"  {a['subject']}|{a['predicate']}|{str(a['object'])[:50]}", file=sys.stderr)

# Now call actual retrieve
print("\n=== Full retrieve() call ===")
snip, atoms = _orig_retrieve('tell me about the war in russia', conn, limit=30)
print(f"Total atoms: {len(atoms)}")
geo = [a for a in atoms if any(k in (a.get('source','') + a.get('subject','') + a.get('object','').lower()) for k in ('russia','ukraine','gdelt','ucdp','news_wire'))]
print(f"Geo atoms in result: {len(geo)}")
for a in geo:
    print(f"  {a['subject']}|{a['predicate']}|{str(a['object'])[:60]}")
if not geo:
    print("  (none - geo atoms not in final 30)")
    print("First 5 atoms by conf:")
    for a in sorted(atoms, key=lambda x: -x.get('confidence',0))[:5]:
        print(f"  {a['subject']}|{a['predicate']}|conf={a['confidence']}|{str(a['object'])[:40]}")

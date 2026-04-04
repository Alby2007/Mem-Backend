"""
scripts/_bench.py — Performance benchmark for docs/benchmarks.md
Run on OCI: cd ~/trading-galaxy && python3 scripts/_bench.py
"""
import sys, os, time, sqlite3, json
sys.path.insert(0, os.path.expanduser('~/trading-galaxy'))

DB = '/opt/trading-galaxy/data/trading_knowledge.db'

results = {}

# ── 1. retrieval.retrieve() ───────────────────────────────────────────────────
print('=== retrieve() ===')
from retrieval import retrieve
conn = sqlite3.connect(DB, timeout=10)
conn.row_factory = sqlite3.Row

QUERIES = [
    'what is the signal for BP.L?',
    'explain the impact of a Fed rate hike',
    'what do you know about NVDA?',
    'compare SHEL.L and BP.L',
    'geopolitical risk in Middle East',
]
retrieve_times = []
for q in QUERIES:
    t0 = time.perf_counter()
    snippet, atoms = retrieve(q, conn)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    retrieve_times.append(ms)
    print(f'  {ms:6.1f}ms  atoms={len(atoms):3d}  {q[:50]}')
conn.close()
results['retrieve_p50_ms'] = round(sorted(retrieve_times)[len(retrieve_times)//2], 1)
results['retrieve_p95_ms'] = round(sorted(retrieve_times)[int(len(retrieve_times)*0.95)], 1)
results['retrieve_max_ms'] = round(max(retrieve_times), 1)
print(f'  p50={results["retrieve_p50_ms"]}ms  max={results["retrieve_max_ms"]}ms')

# ── 2. causal_graph.traverse_causal() ────────────────────────────────────────
print()
print('=== traverse_causal() ===')
from knowledge.causal_graph import traverse_causal
conn2 = sqlite3.connect(DB, timeout=10)
SEEDS = ['fed_rate_cut', 'fed_rate_hike', 'energy_prices_rise',
         'risk_off_rotation', 'dollar_strengthens']
causal_times = []
for seed in SEEDS:
    t0 = time.perf_counter()
    r = traverse_causal(conn2, seed, max_depth=4, min_confidence=0.5)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    causal_times.append(ms)
    hops = len(r.get('chain', []))
    print(f'  {ms:6.1f}ms  hops={hops:2d}  seed={seed}')
conn2.close()
results['causal_p50_ms'] = round(sorted(causal_times)[len(causal_times)//2], 1)
results['causal_max_ms'] = round(max(causal_times), 1)
print(f'  p50={results["causal_p50_ms"]}ms  max={results["causal_max_ms"]}ms')

# ── 3. scenario_engine.run_scenario() ────────────────────────────────────────
print()
print('=== run_scenario() (narrative=False) ===')
from services.scenario_engine import run_scenario
SHOCKS = ['fed rate cut', 'oil spike', 'risk off', 'dollar strengthens', 'boe cut']
scenario_times = []
for shock in SHOCKS:
    t0 = time.perf_counter()
    res = run_scenario(shock, DB, narrative=False)
    ms = round((time.perf_counter() - t0) * 1000, 1)
    scenario_times.append(ms)
    print(f'  {ms:6.1f}ms  resolved={res.resolved}  chain={len(res.chain):2d}  '
          f'conf={res.chain_confidence:.3f}  shock={shock}')
results['scenario_p50_ms'] = round(sorted(scenario_times)[len(scenario_times)//2], 1)
results['scenario_max_ms'] = round(max(scenario_times), 1)
print(f'  p50={results["scenario_p50_ms"]}ms  max={results["scenario_max_ms"]}ms')

# ── 4. graph_retrieval.build_graph_context() ─────────────────────────────────
print()
print('=== build_graph_context() ===')
try:
    from knowledge.graph_retrieval import build_graph_context
    conn3 = sqlite3.connect(DB, timeout=10)
    c = conn3.cursor()
    c.execute("""
        SELECT subject, predicate, object, source, confidence, confidence_effective, timestamp
        FROM facts
        WHERE predicate NOT IN ('source_code','has_title','has_section','has_content')
        ORDER BY confidence DESC LIMIT 200
    """)
    rows = c.fetchall()
    conn3.close()
    atoms = [{'subject': r[0], 'predicate': r[1], 'object': str(r[2])[:200],
              'source': r[3] or '', 'confidence': float(r[4] or 0.5),
              'confidence_effective': float(r[5]) if r[5] else None,
              'timestamp': str(r[6])[:10] if r[6] else ''} for r in rows]

    t0 = time.perf_counter()
    ctx = build_graph_context(atoms, 'what drives NVDA?')
    ms = round((time.perf_counter() - t0) * 1000, 1)
    results['graph_context_ms'] = ms
    print(f'  {ms:.1f}ms  output_chars={len(ctx)}  nodes_in_input={len(atoms)}')
except Exception as e:
    print(f'  SKIP: {e}')
    results['graph_context_ms'] = None

# ── 5. KB size ────────────────────────────────────────────────────────────────
print()
print('=== KB stats ===')
conn4 = sqlite3.connect(DB, timeout=10)
total_facts = conn4.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
total_subjects = conn4.execute('SELECT COUNT(DISTINCT subject) FROM facts').fetchone()[0]
conn4.close()
results['total_facts'] = total_facts
results['total_subjects'] = total_subjects
print(f'  total_facts={total_facts}  total_subjects={total_subjects}')

print()
print('JSON results:')
print(json.dumps(results, indent=2))

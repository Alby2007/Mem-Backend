"""
Smoke test: verify position sizing layer atoms (conviction_tier, volatility_scalar,
position_size_pct) for key tickers against expected CT rules and math.

Run: python tests/smoke_position_sizing.py
Expected: PASS on all assertions.
"""
import sys, sqlite3; sys.path.insert(0, '.')
from ingest.signal_enrichment_adapter import (
    _compute_position_sizing_atoms,
    _VOL_REF, _VOL_SCALAR_FLOOR, _VOL_SCALAR_CAP, _CT_BASE_ALLOC,
)

ERRORS = []

def check(label, got, expected, tol=0.01):
    try:
        if abs(float(got) - float(expected)) > tol:
            ERRORS.append(f"FAIL [{label}] got={got} expected≈{expected}")
        else:
            print(f"  OK  [{label}] {got}")
    except (ValueError, TypeError):
        if str(got).lower() != str(expected).lower():
            ERRORS.append(f"FAIL [{label}] got={got!r} expected={expected!r}")
        else:
            print(f"  OK  [{label}] {got}")

print("\n=== Position Sizing Smoke Test ===\n")

# ── 1. Unit tests of _compute_position_sizing_atoms() directly ─────────────

# NVDA: confirmed + wide + partial → CT-M1 → medium, vol=37.41
# scalar = min(1.0, 20/37.41) = 0.5346; size = 3.0 * 0.5346 = 1.60
atoms = _compute_position_sizing_atoms('nvda', 'confirmed', 'wide', 'partial', 37.41,
                                        'derived_signal_nvda', {})
a = {x.predicate: x.object for x in atoms}
print("[NVDA — CT-M1 confirmed+wide+partial]")
check('nvda.conviction_tier',   a.get('conviction_tier'), 'medium')
check('nvda.volatility_scalar', a.get('volatility_scalar'), 0.5346, tol=0.001)
check('nvda.position_size_pct', a.get('position_size_pct'), 1.60, tol=0.02)

# META: strong + tight + partial → CT-M2 → medium, vol=44.75
# scalar = min(1.0, 20/44.75) = 0.4469; size = 3.0 * 0.4469 = 1.34
atoms = _compute_position_sizing_atoms('meta', 'strong', 'tight', 'partial', 44.75,
                                        'derived_signal_meta', {})
a = {x.predicate: x.object for x in atoms}
print("\n[META — CT-M2 strong+tight+partial]")
check('meta.conviction_tier',   a.get('conviction_tier'), 'medium')
check('meta.volatility_scalar', a.get('volatility_scalar'), 0.4469, tol=0.001)
check('meta.position_size_pct', a.get('position_size_pct'), 1.34, tol=0.02)

# INTC: weak + wide + unconfirmed → CT-L1 (weak fires first) → low, vol=87.24
# scalar = max(0.2, 20/87.24) = max(0.2, 0.2293) = 0.2293; size = 1.5 * 0.2293 = 0.34
atoms = _compute_position_sizing_atoms('intc', 'weak', 'wide', 'unconfirmed', 87.24,
                                        'derived_signal_intc', {})
a = {x.predicate: x.object for x in atoms}
print("\n[INTC — CT-L1 weak+wide+unconfirmed]")
check('intc.conviction_tier',   a.get('conviction_tier'), 'low')
check('intc.volatility_scalar', a.get('volatility_scalar'), 0.2293, tol=0.001)
check('intc.position_size_pct', a.get('position_size_pct'), 0.34, tol=0.02)

# AVOID case: weak + tight → CT-A1 → avoid, size=0.0 regardless of vol
atoms = _compute_position_sizing_atoms('xyz', 'weak', 'tight', 'partial', 30.0,
                                        'derived_signal_xyz', {})
a = {x.predicate: x.object for x in atoms}
print("\n[XYZ — CT-A1 weak+tight → avoid]")
check('xyz.conviction_tier',   a.get('conviction_tier'), 'avoid')
check('xyz.position_size_pct', a.get('position_size_pct'), 0.0)

# conflicted → CT-A2 → avoid regardless of risk level
atoms = _compute_position_sizing_atoms('con', 'conflicted', 'wide', 'confirmed', 20.0,
                                        'derived_signal_con', {})
a = {x.predicate: x.object for x in atoms}
print("\n[CON — CT-A2 conflicted → avoid]")
check('con.conviction_tier',   a.get('conviction_tier'), 'avoid')
check('con.position_size_pct', a.get('position_size_pct'), 0.0)

# CT-HIGH: strong + wide + confirmed → CT-H1 → high
# vol=28 → scalar = min(1.0, 20/28) = 0.7143; size = 5.0 * 0.7143 = 3.57
atoms = _compute_position_sizing_atoms('hi', 'strong', 'wide', 'confirmed', 28.0,
                                        'derived_signal_hi', {})
a = {x.predicate: x.object for x in atoms}
print("\n[HI — CT-H1 strong+wide+confirmed]")
check('hi.conviction_tier',   a.get('conviction_tier'), 'high')
check('hi.volatility_scalar', a.get('volatility_scalar'), 0.7143, tol=0.001)
check('hi.position_size_pct', a.get('position_size_pct'), 3.57, tol=0.02)

# Vol floor test: extreme vol=200 → scalar=max(0.2, 20/200)=0.2
atoms = _compute_position_sizing_atoms('hi2', 'strong', 'wide', 'confirmed', 200.0,
                                        'derived_signal_hi2', {})
a = {x.predicate: x.object for x in atoms}
print("\n[HI2 — vol floor test vol=200]")
check('hi2.volatility_scalar', a.get('volatility_scalar'), 0.2)
check('hi2.position_size_pct', a.get('position_size_pct'), 1.0)  # 5.0 * 0.2

# Low-vol cap: vol=10 → scalar=min(1.0, 20/10)=1.0 (capped)
atoms = _compute_position_sizing_atoms('hi3', 'strong', 'wide', 'confirmed', 10.0,
                                        'derived_signal_hi3', {})
a = {x.predicate: x.object for x in atoms}
print("\n[HI3 — vol cap test vol=10]")
check('hi3.volatility_scalar', a.get('volatility_scalar'), 1.0)
check('hi3.position_size_pct', a.get('position_size_pct'), 5.0)

# missing vol → only conviction_tier emitted, no scalar/size
atoms = _compute_position_sizing_atoms('nov', 'strong', 'wide', 'confirmed', None,
                                        'derived_signal_nov', {})
a = {x.predicate: x.object for x in atoms}
print("\n[NOV — missing vol → conviction only]")
check('nov.conviction_tier', a.get('conviction_tier'), 'high')
assert 'volatility_scalar' not in a, f"FAIL: volatility_scalar should not be emitted when vol missing, got {a}"
assert 'position_size_pct' not in a, f"FAIL: position_size_pct should not be emitted when vol missing, got {a}"
print("  OK  [nov.no_scalar_when_vol_missing]")

# missing sq → skip all (no_data guard)
atoms = _compute_position_sizing_atoms('mis', '', 'wide', 'confirmed', 20.0,
                                        'derived_signal_mis', {})
print("\n[MIS — missing signal_quality → no atoms]")
assert len(atoms) == 0, f"FAIL: should emit 0 atoms when signal_quality missing, got {len(atoms)}"
print("  OK  [mis.no_atoms_when_sq_missing]")

# ── 2. DB verification for key tickers ────────────────────────────────────────
print("\n=== DB verification ===\n")
c = sqlite3.connect('trading_knowledge.db').cursor()
DB_EXPECTED = {
    'nvda': {'conviction_tier': 'medium'},
    'intc': {'conviction_tier': 'low'},
    'meta': {'conviction_tier': 'medium'},
    'msft': {'conviction_tier': 'medium'},
}
for ticker, exp in DB_EXPECTED.items():
    c.execute(
        "SELECT predicate, object FROM facts WHERE subject=? "
        "AND predicate IN ('conviction_tier','volatility_scalar','position_size_pct')",
        (ticker,)
    )
    rows = {r[0]: r[1] for r in c.fetchall()}
    for pred, expected_val in exp.items():
        got_val = rows.get(pred, 'MISSING')
        check(f'db.{ticker}.{pred}', got_val, expected_val)
    # Verify position_size_pct is present and numeric
    ps = rows.get('position_size_pct', None)
    if ps is None:
        ERRORS.append(f"FAIL [db.{ticker}.position_size_pct] MISSING")
    else:
        try:
            assert float(ps) >= 0.0
            print(f"  OK  [db.{ticker}.position_size_pct] = {ps}%")
        except (ValueError, AssertionError):
            ERRORS.append(f"FAIL [db.{ticker}.position_size_pct] non-numeric or negative: {ps}")

# ── Summary ────────────────────────────────────────────────────────────────────
print()
if ERRORS:
    print(f"{'='*60}")
    print(f"FAILED — {len(ERRORS)} error(s):")
    for e in ERRORS:
        print(f"  {e}")
    sys.exit(1)
else:
    print(f"{'='*60}")
    print("ALL CHECKS PASSED")

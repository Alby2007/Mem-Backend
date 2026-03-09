#!/usr/bin/env python3
"""
Quick smoke-test for the new adapters.
Run on OCI: python3 scripts/_test_new_adapters.py
"""
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format='%(name)s %(levelname)s %(message)s')

_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _repo_root)

# Load .env from repo root so API keys are available
_env_file = os.path.join(_repo_root, '.env')
if os.path.exists(_env_file):
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _, _v = _line.partition('=')
                os.environ.setdefault(_k.strip(), _v.strip())

os.environ.setdefault('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')

# Pass --skip-gdelt to avoid the 3-minute GDELT sleep during quick tests
_SKIP_GDELT = '--skip-gdelt' in sys.argv

ADAPTERS = [
    ('GPR',          'ingest.gpr_adapter',           'GPRAdapter',           {}),
    ('Polymarket',   'ingest.polymarket_adapter',     'PolymarketAdapter',    {}),
    ('AlphaVantage', 'ingest.alpha_vantage_adapter',  'AlphaVantageAdapter',  {'db_path': os.environ['TRADING_KB_DB']}),
] + ([] if _SKIP_GDELT else [
    ('GDELT',        'ingest.gdelt_adapter',          'GDELTAdapter',         {}),
]) + [
    ('ACLED',        'ingest.acled_adapter',          'ACLEDAdapter',         {}),
]

results = {}

for name, module_path, cls_name, kwargs in ADAPTERS:
    print(f'\n{"="*50}')
    print(f'  {name}')
    print(f'{"="*50}')
    try:
        import importlib
        mod = importlib.import_module(module_path)
        cls = getattr(mod, cls_name)
        adapter = cls(**kwargs)
        atoms = adapter.fetch()
        results[name] = len(atoms)
        print(f'  PASS — {len(atoms)} atoms')
        for a in atoms[:5]:
            print(f'    {a.subject} | {a.predicate} | {a.object}')
        if len(atoms) > 5:
            print(f'    ... ({len(atoms) - 5} more)')
    except Exception as exc:
        results[name] = f'ERROR: {exc}'
        print(f'  FAIL — {exc}')

print(f'\n{"="*50}')
print('  SUMMARY')
print(f'{"="*50}')
for name, result in results.items():
    status = 'OK' if isinstance(result, int) else 'FAIL'
    print(f'  [{status}] {name}: {result}')

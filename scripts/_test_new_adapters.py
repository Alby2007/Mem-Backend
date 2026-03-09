#!/usr/bin/env python3
"""
Quick smoke-test for the new adapters.
Run on OCI: python3 scripts/_test_new_adapters.py
"""
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format='%(name)s %(levelname)s %(message)s')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('TRADING_KB_DB', '/opt/trading-galaxy/data/trading_knowledge.db')

ADAPTERS = [
    ('GPR',             'ingest.gpr_adapter',           'GPRAdapter',           {}),
    ('Polymarket',      'ingest.polymarket_adapter',     'PolymarketAdapter',    {}),
    ('AlphaVantage',    'ingest.alpha_vantage_adapter',  'AlphaVantageAdapter',  {'db_path': os.environ['TRADING_KB_DB']}),
    ('GDELT',           'ingest.gdelt_adapter',          'GDELTAdapter',         {}),
    ('ACLED',           'ingest.acled_adapter',          'ACLEDAdapter',         {}),
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

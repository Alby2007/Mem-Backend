import json, sys, os

# Find latest valid result (skip the 0-response run)
results_dir = os.path.join(os.path.dirname(__file__), 'results')
files = sorted(
    [f for f in os.listdir(results_dir) if f.endswith('.json')],
    reverse=True
)

for fname in files:
    with open(os.path.join(results_dir, fname)) as f:
        results = json.load(f)
    # skip empty runs
    if not results or results[0].get('response_preview', '') == '':
        print(f"skip {fname} (empty responses)")
        continue
    print(f"using {fname}  ({len(results)} records)")
    break

nd_all  = [r for r in results if r['intent'] == 'no_data']
nd_fail = [r for r in nd_all if not r['pass']]
nd_pass = [r for r in nd_all if r['pass']]

print(f"\nno_data: {len(nd_pass)} pass / {len(nd_all)} total ({100*len(nd_pass)//max(1,len(nd_all))}%)")

print("\n=== FAILURES ===")
for r in nd_fail:
    print(f"\nQ: {r['query']}")
    print(f"  gives_no_data_response : {r['scores'].get('gives_no_data_response')}")
    print(f"  no_invented_data       : {r['scores'].get('no_invented_data')}")
    print(f"  not_empty              : {r['scores'].get('not_empty')}")
    preview = r.get('response_preview', '')
    print(f"  preview: {preview[:350]}")

print("\n=== PASSES (sample 3) ===")
for r in nd_pass[:3]:
    print(f"\nQ: {r['query']}")
    preview = r.get('response_preview', '')
    print(f"  preview: {preview[:200]}")

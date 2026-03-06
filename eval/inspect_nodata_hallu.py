"""Check what KB atoms are returned for MADEUPTICKER queries and how the model uses them."""
import json, os

results_dir = os.path.join(os.path.dirname(__file__), 'results')
files = sorted([f for f in os.listdir(results_dir) if f.endswith('.json')], reverse=True)

for fname in files:
    with open(os.path.join(results_dir, fname)) as f:
        results = json.load(f)
    if not results or results[0].get('response_preview', '') == '':
        continue
    print(f"using {fname}  ({len(results)} records)\n")
    break

nd_fail = [r for r in results if r['intent'] == 'no_data' and not r['pass']]
nd_pass = [r for r in results if r['intent'] == 'no_data' and r['pass']]

print(f"Failures: {len(nd_fail)} / {len([r for r in results if r['intent']=='no_data'])}\n")

# Categorise failures
scorer_gap   = [r for r in nd_fail if r['scores'].get('gives_no_data_response') == False
                and r['scores'].get('no_invented_data') == True]
invented     = [r for r in nd_fail if r['scores'].get('no_invented_data') == False]
empty_resp   = [r for r in nd_fail if not r['scores'].get('not_empty')]

print(f"Scorer gap (correct refusal, wrong phrasing): {len(scorer_gap)}")
print(f"Genuine hallucination (invented data):         {len(invented)}")
print(f"Empty response:                                {len(empty_resp)}")

print("\n--- GENUINE HALLUCINATIONS ---")
for r in invented[:5]:
    print(f"Q: {r['query']}")
    print(f"   preview: {r.get('response_preview','')[:250]}\n")

print("\n--- SCORER GAP (check if new phrases fix) ---")
for r in scorer_gap[:5]:
    print(f"Q: {r['query']}")
    print(f"   preview: {r.get('response_preview','')[:250]}\n")

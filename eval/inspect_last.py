"""eval/inspect_last.py — inspect most recent eval result."""
import json, glob, os, collections

files = sorted(glob.glob(os.path.join(os.path.dirname(__file__), "results", "*.json")))
if not files:
    print("No result files found")
    raise SystemExit(1)

path = files[-1]
print(f"File: {os.path.basename(path)}")
d = json.load(open(path))

errs = [r for r in d if r.get("error")]
passes = [r for r in d if r.get("pass")]
no_token = [r for r in d if r.get("error") == "no auth token"]
other_errs = [r for r in errs if r.get("error") != "no auth token"]

print(f"total={len(d)}  pass={len(passes)}  errors={len(errs)}")
print(f"  no_auth_token={len(no_token)}  other_errors={len(other_errs)}")

if other_errs:
    ctr = collections.Counter(r.get("error","")[:80] for r in other_errs)
    print("Other error types:")
    for msg, cnt in ctr.most_common(10):
        print(f"  {cnt:4d}x  {msg}")

if no_token:
    print(f"\nSample no-token item: {json.dumps(no_token[0], indent=2)[:400]}")

pr_fails = [r for r in d if r.get("intent") == "portfolio_review" and not r.get("pass") and not r.get("error")]
if pr_fails:
    print(f"\nportfolio_review failures: {len(pr_fails)}")
    sample = pr_fails[0]
    print(f"  query: {sample.get('query','')[:120]}")
    print(f"  scores: {sample.get('scores',{})}")
    print(f"  response_preview: {sample.get('response_preview','')[:300]}")

if passes:
    print(f"\nSample passing item intent={passes[0].get('intent')} response_length={passes[0].get('response_length')}")
    print(f"  response_preview: {passes[0].get('response_preview','')[:200]}")

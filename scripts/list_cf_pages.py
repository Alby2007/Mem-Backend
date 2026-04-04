import urllib.request, json

TOKEN = 'RndYKnbKvMEoXZlyJirmwBRVe3Yc1_DMLuEFO55U'
ACCOUNT = '920cc1658ac62ded159441bc7300e8a8'

req = urllib.request.Request(
    f'https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/pages/projects?per_page=20',
    headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'})
try:
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read())
    for p in d.get('result', []):
        dep = p.get('latest_deployment') or {}
        trigger = dep.get('deployment_trigger', {}).get('metadata', {})
        print(f"name={p['name']} | branch={p.get('production_branch','?')} | subdomain={p.get('subdomain','?')}")
        print(f"  latest_deploy_commit: {trigger.get('commit_hash','?')[:8]} | url={dep.get('url','?')}")
except Exception as e:
    print('ERROR:', e)

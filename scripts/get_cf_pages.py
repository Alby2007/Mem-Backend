import urllib.request, json

TOKEN = 'RndYKnbKvMEoXZlyJirmwBRVe3Yc1_DMLuEFO55U'

# Get accounts
req = urllib.request.Request('https://api.cloudflare.com/client/v4/accounts?per_page=5',
    headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'})
with urllib.request.urlopen(req, timeout=10) as r:
    accounts = json.loads(r.read())

for acct in accounts.get('result', []):
    acct_id = acct['id']
    print(f"Account: {acct['name']} ({acct_id})")

    # List Pages projects
    req2 = urllib.request.Request(
        f'https://api.cloudflare.com/client/v4/accounts/{acct_id}/pages/projects',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req2, timeout=10) as r2:
            projects = json.loads(r2.read())
        for p in projects.get('result', []):
            dep = p.get('latest_deployment', {}) or {}
            print(f"  Project: {p['name']}")
            print(f"    production_branch: {p.get('production_branch','?')}")
            print(f"    build_config.root_dir: {p.get('build_config',{}).get('root_dir','/')}")
            print(f"    latest_deploy: {dep.get('id','?')} | env={dep.get('environment','?')} | commit={dep.get('deployment_trigger',{}).get('metadata',{}).get('commit_hash','?')[:8]}")
            print(f"    deploy_url: {dep.get('url','?')}")
    except Exception as e:
        print(f"  Pages error: {e}")

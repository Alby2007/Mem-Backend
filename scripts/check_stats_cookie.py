import urllib.request, json

# Hit /stats without auth cookie (same as browser would if cookie missing)
req = urllib.request.Request('http://localhost:5050/stats')
try:
    r = urllib.request.urlopen(req, timeout=5)
    d = json.loads(r.read())
    print('market_regime:', repr(d.get('market_regime')))
    print('total_facts:', d.get('total_facts'))
    print('open_patterns:', d.get('open_patterns'))
except Exception as e:
    print('ERROR:', e)

import urllib.request, json

req = urllib.request.Request(
    'https://api.trading-galaxy.uk/stats',
    headers={'Origin': 'https://trading-galaxy.uk'}
)
try:
    r = urllib.request.urlopen(req, timeout=10)
    d = json.loads(r.read())
    print('market_regime:', repr(d.get('market_regime')))
    print('total_facts:',   d.get('total_facts'))
    print('open_patterns:', d.get('open_patterns'))
except Exception as e:
    print('ERROR:', e)

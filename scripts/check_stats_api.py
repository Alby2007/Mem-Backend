import urllib.request, json
r = urllib.request.urlopen('http://localhost:5050/stats', timeout=5)
d = json.loads(r.read())
print('market_regime:', d.get('market_regime'))
print('total_facts:', d.get('total_facts'))

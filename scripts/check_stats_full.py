import urllib.request, json
r = urllib.request.urlopen('http://localhost:5050/stats', timeout=5)
d = json.loads(r.read())
print(json.dumps({k: v for k, v in d.items() if 'regime' in k or k in ('total_facts','open_patterns')}, indent=2))

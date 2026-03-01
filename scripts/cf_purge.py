import urllib.request, json, os

token = os.environ.get('CF_API_TOKEN', 'RndYKnbKvMEoXZlyJirmwBRVe3Yc1_DMLuEFO55U')
zone  = os.environ.get('CF_ZONE_ID',   'ac2db04fcb7d12e3f9747f4b736c93e3')

url  = f'https://api.cloudflare.com/client/v4/zones/{zone}/purge_cache'
body = json.dumps({'purge_everything': True}).encode()
req  = urllib.request.Request(url, data=body, method='POST')
req.add_header('Authorization', f'Bearer {token}')
req.add_header('Content-Type',  'application/json')

try:
    resp = urllib.request.urlopen(req, timeout=10)
    d = json.loads(resp.read())
except urllib.error.HTTPError as e:
    d = json.loads(e.read())

print('success:', d.get('success'))
if not d.get('success'):
    print('errors:', d.get('errors'))

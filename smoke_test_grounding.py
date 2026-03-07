import urllib.request, json

import time
req = urllib.request.Request(
    'http://localhost:5050/chat',
    data=json.dumps({'message':'what is the signal on COIN?','session_id':f'smoke_{int(time.time())}'}).encode(),
    headers={'Content-Type':'application/json'},
    method='POST'
)
with urllib.request.urlopen(req) as r:
    d = json.loads(r.read())

answer = d.get('answer') or ''
has_block = '[KB_GROUNDING]' in answer
calibration = d.get('calibration')

print('atoms_used:', d.get('atoms_used'))
print('stress.composite:', (d.get('stress') or {}).get('composite_stress'))
print('KB_GROUNDING block present:', has_block)
print('calibration:', calibration)
print('grounding_atoms:', d.get('grounding_atoms'))
if has_block:
    start = answer.index('[KB_GROUNDING]')
    end   = answer.index('[/KB_GROUNDING]') + len('[/KB_GROUNDING]')
    print('\n--- BLOCK ---')
    print(answer[start:end])
    print('--- END ---')

snippet = d.get('snippet') or ''
print('\n--- SNIPPET predicates for COIN ---')
for line in snippet.split('\n'):
    l = line.lower()
    if any(p in l for p in ['conviction_tier','volatility_regime','implied_volatility','put_call_oi_ratio','put_call_ratio']):
        print(line[:120])

import sqlite3
c = sqlite3.connect('/opt/trading-galaxy/data/trading_knowledge.db')
missing = ['conviction_tier','volatility_regime','implied_volatility','put_call_oi_ratio','put_call_ratio']
print('\n--- DB facts for coin (missing predicates) ---')
for pred in missing:
    rows = c.execute(
        "SELECT subject, predicate, object, confidence FROM facts WHERE subject=? AND predicate=? LIMIT 3",
        ('coin', pred)
    ).fetchall()
    if rows:
        for r in rows:
            print(f"  {r[0]} | {r[1]} | {r[2]} | conf={r[3]}")
    else:
        print(f"  (no rows for predicate={pred})")

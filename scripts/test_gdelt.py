import urllib.request, urllib.parse, json

base = 'http://api.gdeltproject.org/api/v2/doc/doc'

# Test tonechart mode (current approach)
params = {'query': 'United States Iran', 'mode': 'tonechart', 'format': 'json', 'timespan': '1d'}
url = base + '?' + urllib.parse.urlencode(params)
print("Testing tonechart:", url)
try:
    req = urllib.request.Request(url, headers={'User-Agent': 'TradingKB/1.0'})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read().decode('utf-8', errors='replace'))
    print("tonechart keys:", list(data.keys()))
    chart = data.get('tonechart', [])
    print("tonechart entries:", len(chart))
    if chart:
        print("sample:", chart[0])
    else:
        print("EMPTY tonechart — this is the bug")
except Exception as e:
    print("tonechart ERROR:", e)

print()

# Test artlist mode (alternative — returns actual articles with tone)
params2 = {'query': 'United States Iran war', 'mode': 'artlist', 'format': 'json', 'timespan': '1d', 'maxrecords': '5'}
url2 = base + '?' + urllib.parse.urlencode(params2)
print("Testing artlist:", url2)
try:
    req2 = urllib.request.Request(url2, headers={'User-Agent': 'TradingKB/1.0'})
    with urllib.request.urlopen(req2, timeout=20) as r2:
        data2 = json.loads(r2.read().decode('utf-8', errors='replace'))
    arts = data2.get('articles', [])
    print("artlist count:", len(arts))
    for a in arts[:2]:
        print(" title:", a.get('title','?'), '| tone:', a.get('tone','?'))
except Exception as e:
    print("artlist ERROR:", e)

print()

# Test timelinevol mode (volume over time — useful for tension)
params3 = {'query': 'war OR conflict OR military', 'mode': 'timelinevol', 'format': 'json', 'timespan': '1d'}
url3 = base + '?' + urllib.parse.urlencode(params3)
print("Testing timelinevol:", url3)
try:
    req3 = urllib.request.Request(url3, headers={'User-Agent': 'TradingKB/1.0'})
    with urllib.request.urlopen(req3, timeout=20) as r3:
        data3 = json.loads(r3.read().decode('utf-8', errors='replace'))
    print("timelinevol keys:", list(data3.keys()))
    tl = data3.get('timeline', [{}])
    print("timeline entries:", len(tl[0].get('data',[])) if tl else 0)
except Exception as e:
    print("timelinevol ERROR:", e)

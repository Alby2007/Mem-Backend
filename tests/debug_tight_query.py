import sys; sys.path.insert(0, '.')
from retrieval import _KEYWORD_PREDICATE_BOOST, _STOPWORDS
import re

q = "Which tickers in the KB have thesis_risk_level of tight?"

_UPPERCASE_STOPWORDS = {
    'THE', 'IS', 'AT', 'ON', 'AN', 'AND', 'OR', 'FOR', 'IN', 'OF',
    'TO', 'THAT', 'THIS', 'WITH', 'FROM', 'BY', 'ARE', 'WAS', 'BE',
    'HAS', 'HAVE', 'HAD', 'ITS', 'DO', 'DID', 'WHAT', 'HOW', 'WHY',
    'WHEN', 'WHERE', 'WHO', 'CAN', 'WILL', 'THERE', 'THEIR', 'THEY',
    'YOU', 'NOT', 'BUT', 'ALL', 'GET', 'GOT', 'NEW', 'NOW', 'OUT',
    'USE', 'WAY', 'USED', 'ALSO', 'JUST', 'INTO', 'OVER', 'COULD',
    'WOULD', 'SHOULD', 'THAN', 'THEN', 'WHICH', 'SOME', 'MORE',
}
tickers = [t for t in re.findall(r'\b[A-Z]{2,5}\b', q) if t not in _UPPERCASE_STOPWORDS]
words   = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', q)
terms   = [w.lower() for w in words if w.lower() not in _STOPWORDS and len(w) > 2]
print(f"tickers: {tickers}")
print(f"terms:   {terms}")

boosted = set()
for term in terms:
    for kw, preds in _KEYWORD_PREDICATE_BOOST.items():
        if kw in term or term in kw:
            boosted.update(preds)

print(f"boosted: {sorted(boosted)}")

_RANKING = frozenset({
    'return_vs_spy_1m', 'return_vs_spy_3m', 'return_1m', 'return_3m',
    'return_6m', 'return_1y', 'return_1w', 'drawdown_from_52w_high',
    'volatility_30d', 'volatility_90d', 'upside_pct', 'invalidation_distance',
})
ranking_boost = boosted & _RANKING
cat_boost = boosted - _RANKING
print(f"ranking_boost: {sorted(ranking_boost)}")
print(f"cat_boost:     {sorted(cat_boost)}")
print(f"not tickers:   {not tickers}")
print(f"cat branch fires: {bool(cat_boost) and not tickers}")

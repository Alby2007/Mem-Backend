import sys; sys.path.insert(0, '.')
import re

_STOPWORDS = {
    'the', 'is', 'at', 'which', 'on', 'a', 'an', 'and', 'or', 'for',
    'in', 'of', 'to', 'that', 'this', 'with', 'from', 'by', 'are',
    'was', 'be', 'has', 'have', 'had', 'its', 'it', 'do', 'did',
    'what', 'how', 'why', 'when', 'where', 'who', 'can', 'will',
    'there', 'their', 'they', 'we', 'you', 'me', 'my', 'our',
}

def _extract_key_terms(message):
    words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_/-]{1,}\b', message)
    terms = [w.lower() for w in words if w.lower() not in _STOPWORDS and len(w) > 2]
    seen = set()
    return [t for t in terms if not (t in seen or seen.add(t))]

def _extract_tickers(message):
    _UPPERCASE_STOPWORDS = {
        'THE', 'IS', 'AT', 'ON', 'AN', 'AND', 'OR', 'FOR', 'IN', 'OF',
        'TO', 'THAT', 'THIS', 'WITH', 'FROM', 'BY', 'ARE', 'WAS', 'BE',
        'HAS', 'HAVE', 'HAD', 'ITS', 'DO', 'DID', 'WHAT', 'HOW', 'WHY',
        'WHEN', 'WHERE', 'WHO', 'CAN', 'WILL', 'THERE', 'THEIR', 'THEY',
        'YOU', 'NOT', 'BUT', 'ALL', 'GET', 'GOT', 'NEW', 'NOW', 'OUT',
        'USE', 'WAY', 'USED', 'ALSO', 'JUST', 'INTO', 'OVER', 'COULD',
        'WOULD', 'SHOULD', 'THAN', 'THEN', 'WHICH', 'SOME', 'MORE',
    }
    candidates = re.findall(r'\b[A-Z]{2,5}\b', message)
    return [t for t in candidates if t not in _UPPERCASE_STOPWORDS]

from retrieval import _KEYWORD_PREDICATE_BOOST

q = "Which tickers have outperformed SPY over the last month? Rank by return_vs_spy_1m."
terms = _extract_key_terms(q)
tickers = _extract_tickers(q)
print(f"terms:   {terms}")
print(f"tickers: {tickers}")

boosted = set()
for term in terms:
    for kw, preds in _KEYWORD_PREDICATE_BOOST.items():
        if kw in term or term in kw:
            boosted.update(preds)
            print(f"  match: term={term!r} kw={kw!r} -> {preds}")

print(f"boosted_predicates: {sorted(boosted)}")

_RANKING = frozenset({
    'return_vs_spy_1m', 'return_vs_spy_3m',
    'return_1m', 'return_3m', 'return_6m', 'return_1y',
    'return_1w', 'drawdown_from_52w_high',
    'volatility_30d', 'volatility_90d', 'upside_pct',
})
ranking_boost = boosted & _RANKING
print(f"ranking_boost (will use numeric sort): {sorted(ranking_boost)}")
print(f"no tickers: {not tickers}  -> 3b fires: {not tickers and bool(ranking_boost)}")

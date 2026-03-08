"""Quick yfinance diagnostic — run on OCI: python3 scripts/_yf_test.py"""
import time, sys
import yfinance as yf

print(f"yfinance version: {yf.__version__}")

# Test 1: single ticker download
print("\n--- yf.download(['AAPL','MSFT'], period=2d) ---")
t0 = time.time()
try:
    d = yf.download(['AAPL', 'MSFT'], period='2d', progress=False, threads=True)
    print(f"OK in {time.time()-t0:.1f}s, shape={d.shape}")
    print(d.tail(2))
except Exception as e:
    print(f"FAILED in {time.time()-t0:.1f}s: {e}")

# Test 2: single Ticker.info
print("\n--- yf.Ticker('AAPL').info ---")
t0 = time.time()
try:
    info = yf.Ticker('AAPL').info
    price = info.get('currentPrice') or info.get('regularMarketPrice')
    print(f"OK in {time.time()-t0:.1f}s, price={price}")
except Exception as e:
    print(f"FAILED in {time.time()-t0:.1f}s: {e}")

# Test 3: FTSE ticker
print("\n--- yf.Ticker('BP.L').fast_info ---")
t0 = time.time()
try:
    fi = yf.Ticker('BP.L').fast_info
    print(f"OK in {time.time()-t0:.1f}s, last_price={fi.get('lastPrice')}")
except Exception as e:
    print(f"FAILED in {time.time()-t0:.1f}s: {e}")

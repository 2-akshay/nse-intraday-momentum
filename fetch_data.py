# fetch_data.py (robust, stores full raw JSON for each symbol and extracts price+volume)
import requests, json, time
from pathlib import Path

OUT_PATH = Path("quotes.json")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/"
}

def get_nifty50_list(session):
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    # warm session
    session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
    r = session.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    j = r.json()
    symbols = [item.get("symbol") for item in j.get("data", []) if item.get("symbol")]
    return symbols

def safe_get(d, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict): return None
        cur = cur.get(k)
    return cur

def extract_price_volume(j):
    # Try common paths from NSE API JSON
    candidates_price = [
        ("priceInfo", "lastPrice"),
        ("priceInfo", "lastPriceRaw"),
        ("priceInfo", "last"),
        ("data", "lastPrice"),
        ("lastPrice",)
    ]
    candidates_vol = [
        ("securityInfo", "totalTradedVolume"),
        ("securityInfo", "totalTradedQuantity"),
        ("tradedQuantity",),
        ("tradeInfo", "totalTradedVolume"),
        ("data", "totalTradedVolume")
    ]
    p = None
    v = None
    for path in candidates_price:
        val = safe_get(j, *path) if isinstance(path, tuple) else j.get(path)
        if val is not None:
            p = val
            break
    for path in candidates_vol:
        val = safe_get(j, *path) if isinstance(path, tuple) else j.get(path)
        if val is not None:
            v = val
            break
    # normalize numeric strings
    def norm(x):
        if x is None: return None
        try:
            if isinstance(x, str):
                return float(x.replace(",", "").strip())
            return float(x)
        except:
            return None
    return norm(p), norm(v)

def fetch_quote(session, symbol):
    url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
    try:
        r = session.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        j = r.json()
        price, volume = extract_price_volume(j)
        return {"symbol": symbol, "price": price, "volume": volume, "raw": j}
    except Exception as e:
        return {"symbol": symbol, "price": None, "volume": None, "error": str(e), "raw": None}

def main():
    with requests.Session() as s:
        try:
            symbols = get_nifty50_list(s)
        except Exception as e:
            print("Could not get NIFTY50 list, falling back to sample. Err:", e)
            symbols = ["TCS","INFY","RELIANCE","HDFCBANK","ICICIBANK"]
        print("Symbols count:", len(symbols))
        sample = symbols[:50]

        results = []
        # warm the session once more
        s.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
        for i, sym in enumerate(sample, start=1):
            print(f"Fetching {i}/{len(sample)}: {sym}")
            r = fetch_quote(s, sym)
            results.append(r)
            time.sleep(0.35)

    out = {"fetchedAt": time.strftime("%Y-%m-%d %H:%M:%S"), "quotes": results}
    # write full raw data included
    with open(OUT_PATH, "w", encoding="utf8") as f:
        json.dump(out, f, indent=2)
    print("Saved:", OUT_PATH)

if __name__ == "__main__":
    main()

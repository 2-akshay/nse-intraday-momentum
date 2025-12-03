# analyze_momentum.py (estimates missing volumes so pipeline runs)
import json, math, statistics, time
IN_PATH = "quotes.json"
OUT_PATH = "momentum_ranked.json"

# Assumptions (change if you want)
PRICE_930_FACTOR = 0.01   # Price at 9:30 = current - 1%
VOLUME_930_FACTOR = 0.5   # Volume at 9:30 = currentVolume * 0.5
FALLBACK_VOLUME_IF_NONE = 500000  # used only if no real volumes found

def load_quotes():
    with open(IN_PATH, "r", encoding="utf8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "quotes" in data:
        return data["quotes"]
    if isinstance(data, list):
        return data
    raise RuntimeError("Unexpected quotes.json structure")

def get_price_volume(item):
    # common field names
    price = item.get("price") or item.get("currentPrice") or item.get("lastPrice") or None
    vol = item.get("volume") or item.get("currentVolume") or item.get("totalTradedVolume") or None

    # Try nested raw if present and values missing
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else None
    if (price is None or vol is None) and raw:
        # try some typical nests
        def sg(o, *path):
            cur = o
            for p in path:
                if not isinstance(cur, dict): return None
                cur = cur.get(p)
            return cur
        if price is None:
            for path in (("priceInfo","lastPrice"), ("priceInfo","lastPriceRaw"), ("info","lastPrice"), ("lastPrice",)):
                val = sg(raw, *path) if isinstance(path, tuple) else raw.get(path)
                if val is not None:
                    price = val; break
        if vol is None:
            for path in (("securityInfo","totalTradedVolume"), ("securityInfo","totalTradedQuantity"), ("tradeInfo","totalTradedVolume"), ("tradedQuantity",)):
                val = sg(raw, *path) if isinstance(path, tuple) else raw.get(path)
                if val is not None:
                    vol = val; break

    # normalize
    def norm(x):
        if x is None: return None
        try:
            if isinstance(x, str):
                return float(x.replace(",","").strip())
            return float(x)
        except:
            return None

    return norm(price), norm(vol)

def main():
    quotes = load_quotes()
    # gather available volumes
    volumes = []
    for item in quotes:
        if not isinstance(item, dict): continue
        _, v = get_price_volume(item)
        if v is not None and v > 0:
            volumes.append(v)

    if volumes:
        # use median to avoid outliers
        default_estimate = int(statistics.median(volumes))
    else:
        default_estimate = FALLBACK_VOLUME_IF_NONE

    print(f"Found {len(volumes)} real volumes. Using default estimate = {default_estimate} for missing volumes.")

    trending = []
    estimated_symbols = []
    skipped = 0

    for item in quotes:
        if not isinstance(item, dict):
            skipped += 1
            continue
        symbol = item.get("symbol") or item.get("shortSymbol") or item.get("ticker") or "UNKNOWN"
        price, vol = get_price_volume(item)
        if price is None:
            print(f"SKIP {symbol}: missing price -> {price}")
            skipped += 1
            continue
        if vol is None:
            # estimate
            vol = default_estimate
            estimated_symbols.append(symbol)

        # compute assumed 9:30 values per task
        price930 = price - (price * PRICE_930_FACTOR)
        vol930 = max(1, vol * VOLUME_930_FACTOR)

        price_change_pct = ((price - price930) / price930) * 100
        vol_change_pct = ((vol - vol930) / vol930) * 100

        # positive trend definition
        if price_change_pct > 0 and vol_change_pct > 0:
            momentum = price_change_pct * vol_change_pct
            trending.append({
                "symbol": symbol,
                "price": round(price, 4),
                "volume": int(round(vol)),
                "price_pct": round(price_change_pct, 6),
                "vol_pct": round(vol_change_pct, 6),
                "momentum": round(momentum, 6),
                "volume_estimated": symbol in estimated_symbols
            })
        else:
            # not trending under our definition
            pass

    trending.sort(key=lambda x: x["momentum"], reverse=True)

    out = {"analyzedAt": time.strftime("%Y-%m-%d %H:%M:%S"), "count": len(trending), "ranked": trending, "estimated_volume_default": default_estimate, "estimated_symbols_count": len(estimated_symbols)}
    with open(OUT_PATH, "w", encoding="utf8") as f:
        json.dump(out, f, indent=2)

    # print summary
    print("\n===== Momentum Ranking (top 30) =====\n")
    if not trending:
        print("No trending stocks found with current assumptions.")
    else:
        for i, r in enumerate(trending[:30], start=1):
            est_flag = "(est)" if r.get("volume_estimated") else ""
            print(f"{i:2d}. {r['symbol']:12s} Price={r['price']:8} Price%={r['price_pct']:8} Vol%={r['vol_pct']:8} Momentum={r['momentum']} {est_flag}")
    print(f"\nSkipped entries: {skipped}")
    print(f"Symbols with estimated volume: {len(estimated_symbols)} (list shown below)")
    print(estimated_symbols[:100])
    print("\nSaved:", OUT_PATH)

if __name__ == "__main__":
    main()

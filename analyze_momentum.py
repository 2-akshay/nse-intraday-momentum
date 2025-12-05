# analyze_momentum.py

import json
import time
from pathlib import Path

IN_PATH = Path("intraday_snapshot.json")
OUT_PATH = Path("momentum_ranked.json")


def load_snapshot():
    with open(IN_PATH, "r", encoding="utf8") as f:
        data = json.load(f)
    return data["data"]


def main():
    rows = load_snapshot()
    trending = []

    for item in rows:
        sym = item["symbol"]
        p930 = item["price_930"]
        p1030 = item["price_1030"]
        v930 = item["vol_930"]
        v1030 = item["vol_1030"]

        if p930 <= 0 or v930 <= 0:
            continue

        price_change_pct = ((p1030 - p930) / p930) * 100
        vol_change_pct = ((v1030 - v930) / v930) * 100

        if price_change_pct > 0 and vol_change_pct > 0:
            momentum = price_change_pct * vol_change_pct

            trending.append({
                "symbol": sym,
                "price_930": round(p930, 2),
                "price_1030": round(p1030, 2),
                "vol_930": int(v930),
                "vol_1030": int(v1030),
                "price_change_pct": round(price_change_pct, 3),
                "volume_change_pct": round(vol_change_pct, 3),
                "momentum_score": round(momentum, 3),
            })

    trending.sort(key=lambda x: x["momentum_score"], reverse=True)

    out = {
        "analyzed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(trending),
        "ranked": trending,
    }

    OUT_PATH.write_text(json.dumps(out, indent=2))

    print("\n===== TOP TRENDING STOCKS =====\n")
    for i, r in enumerate(trending[:30], 1):
        print(
            f"{i}. {r['symbol']} | Price%: {r['price_change_pct']} "
            f"| Vol%: {r['volume_change_pct']} | Momentum: {r['momentum_score']}"
        )

    print("\nSaved →", OUT_PATH)


if __name__ == "__main__":
    main()

# fetch_data.py
"""
Fetch intraday (1-min) data for NIFTY 50 stocks using yfinance,
pick 9:30 AM and 10:30 AM prices/volumes, and save snapshot to intraday_snapshot.json
"""

import json
from pathlib import Path
import datetime as dt
import pandas as pd
import requests
import yfinance as yf

OUT_PATH = Path("intraday_snapshot.json")


def get_nifty50_symbols_from_wiki():
    url = "https://en.wikipedia.org/wiki/NIFTY_50"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()

    tables = pd.read_html(resp.text)

    for tbl in tables:
        cols = [c.strip() for c in tbl.columns.astype(str)]
        if "Symbol" in cols:
            col_name = tbl.columns[cols.index("Symbol")]
        elif "Ticker" in cols:
            col_name = tbl.columns[cols.index("Ticker")]
        else:
            continue

        syms = (
            tbl[col_name]
            .dropna()
            .astype(str)
            .str.upper()
            .unique()
            .tolist()
        )
        if syms:
            return syms

    raise RuntimeError("NIFTY 50 symbols table not found on Wikipedia")


def get_row_at_or_before_time(df, target_time):
    if df.empty:
        return None

    df = df.sort_index()
    times = df.index.time
    valid_idx = [i for i, t in enumerate(times) if t <= target_time]

    if not valid_idx:
        return df.iloc[0]

    return df.iloc[valid_idx[-1]]


def fetch_intraday_for_nifty50():
    print("Fetching NIFTY 50 symbol list from Wikipedia...")
    symbols = get_nifty50_symbols_from_wiki()
    print(f"Found {len(symbols)} NIFTY 50 symbols.")

    ticker_map = {sym: f"{sym}.NS" for sym in symbols}

    print("Downloading intraday data from yFinance...")
    data = yf.download(
        tickers=" ".join(ticker_map.values()),
        period="1d",
        interval="1m",
        group_by="ticker",
        threads=True,
    )

    snapshot = []
    skipped = []
    multi = isinstance(data.columns, pd.MultiIndex)

    t930 = dt.time(9, 30)
    t1030 = dt.time(10, 30)

    for sym, yf_ticker in ticker_map.items():
        try:
            if multi:
                if yf_ticker not in data.columns.levels[0]:
                    skipped.append(sym)
                    continue
                df = data[yf_ticker].copy()
            else:
                df = data.copy()

            df = df.dropna(subset=["Close", "Volume"])
            df["CumVolume"] = df["Volume"].cumsum()

            row_930 = get_row_at_or_before_time(df, t930)
            row_1030 = get_row_at_or_before_time(df, t1030)

            if row_930 is None or row_1030 is None:
                skipped.append(sym)
                continue

            p930 = float(row_930["Close"])
            p1030 = float(row_1030["Close"])
            v930 = float(row_930["CumVolume"])
            v1030 = float(row_1030["CumVolume"])

            snapshot.append({
                "symbol": sym,
                "yf_symbol": yf_ticker,
                "t_930": row_930.name.isoformat(),
                "t_1030": row_1030.name.isoformat(),
                "price_930": p930,
                "price_1030": p1030,
                "vol_930": int(v930),
                "vol_1030": int(v1030),
            })

        except Exception as e:
            print("Error for", sym, e)
            skipped.append(sym)

    out = {
        "fetched_at": dt.datetime.now().isoformat(),
        "count": len(snapshot),
        "skipped": skipped,
        "data": snapshot,
    }

    OUT_PATH.write_text(json.dumps(out, indent=2))
    print("\nSaved →", OUT_PATH)
    print("Skipped:", skipped)


if __name__ == "__main__":
    fetch_intraday_for_nifty50()

import requests
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
from datetime import date, timedelta
import time
import os
from dotenv import load_dotenv

load_dotenv()

TWELVE_API_KEY = os.getenv("TWELVE_API_KEY")

# ======================
# CONFIG
# ======================
START_DATE = date(2026, 4, 4)
END_DATE = date.today()

# ======================
# FETCH GOLD RANGE
# ======================
def fetch_gold_range():
    url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=1day&start_date={START_DATE}&end_date={END_DATE}&apikey={TWELVE_API_KEY}"
    res = requests.get(url).json()

    if "values" not in res:
        raise ValueError(res)

    return list(reversed(res["values"]))  # chronological

# ======================
# FETCH MARKET
# ======================
def fetch_yf_range(symbol):
    df = yf.download(symbol, start=START_DATE, end=END_DATE, interval="1d", progress=False)
    df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df[["Date", "Close"]].rename(columns={"Close": symbol})

# ======================
# FETCH FRED
# ======================
def fetch_fred_range(series):
    df = pdr.DataReader(series, "fred", START_DATE, END_DATE)
    df = df.reset_index()
    return df

# ======================
# MAIN EXPORT
# ======================
def main():
    print("Fetching data...")

    gold = fetch_gold_range()
    dxy = fetch_yf_range("DX=F")
    sp500 = fetch_yf_range("^GSPC")
    oil = fetch_yf_range("CL=F")
    rate = fetch_fred_range("FEDFUNDS")
    cpi = fetch_fred_range("CPIAUCSL")

    # merge all
    df = pd.DataFrame([{
        "date": g["datetime"],
        "open": float(g["open"]),
        "high": float(g["high"]),
        "low": float(g["low"]),
        "close": float(g["close"]),
    } for g in gold])

    df["date"] = pd.to_datetime(df["date"])

    df = df.merge(dxy, left_on="date", right_on="Date", how="left")
    df = df.merge(sp500, left_on="date", right_on="Date", how="left", suffixes=("", "_sp"))
    df = df.merge(oil, left_on="date", right_on="Date", how="left", suffixes=("", "_oil"))

    df = df.merge(rate, left_on="date", right_on="DATE", how="left")
    df = df.merge(cpi, left_on="date", right_on="DATE", how="left", suffixes=("", "_cpi"))

    df = df.fillna(method="ffill")  # fill missing

    # ======================
    # BUILD SQL
    # ======================
    sql = []

    # DIM_DATE
    for _, r in df.iterrows():
        d = r["date"].date()
        sql.append(f"""INSERT INTO predict_gold_price.dim_date (date, day_of_week, month, quarter, year)
VALUES ('{d}', {d.weekday()}, {d.month}, {(d.month-1)//3+1}, {d.year})
ON CONFLICT (date) DO NOTHING;""")

    # GOLD + FEATURE
    for _, r in df.iterrows():
        d = r["date"].date()

        sql.append(f"""
INSERT INTO predict_gold_price.gold_price (date_id, open, high, low, close)
SELECT id, {r['open']}, {r['high']}, {r['low']}, {r['close']}
FROM predict_gold_price.dim_date WHERE date = '{d}'
ON CONFLICT (date_id) DO NOTHING;
""")

        sql.append(f"""
INSERT INTO predict_gold_price.feature (date_id, dxy, sp500, oil, interest_rate, cpi)
SELECT id, {r['DX=F']}, {r['^GSPC']}, {r['CL=F']}, {r['FEDFUNDS']}, {r['CPIAUCSL']}
FROM predict_gold_price.dim_date WHERE date = '{d}'
ON CONFLICT (date_id) DO NOTHING;
""")

    # save file
    with open("backfill.sql", "w", encoding="utf-8") as f:
        f.write("\n".join(sql))

    print("DONE → backfill.sql")

# ======================
if __name__ == "__main__":
    main()
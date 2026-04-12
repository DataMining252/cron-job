import requests
import psycopg2
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
from datetime import datetime
import os
import time
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")
TWELVE_API_KEY = os.getenv("TWELVE_API_KEY")

# ======================
# DB
# ======================
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# ======================
# UTIL
# ======================
def safe_float(x):
    try:
        return float(x)
    except:
        return None

# ======================
# DIM DATE
# ======================
def upsert_dim_date(date):
    cursor.execute("""
        INSERT INTO predict_gold_price.dim_date (date, day_of_week, month, quarter, year)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING
        RETURNING id;
    """, (
        date,
        date.weekday(),
        date.month,
        (date.month - 1)//3 + 1,
        date.year
    ))

    result = cursor.fetchone()

    if result:
        return result[0]

    cursor.execute("""
        SELECT id FROM predict_gold_price.dim_date WHERE date = %s
    """, (date,))
    return cursor.fetchone()[0]

# ======================
# GOLD (Twelve Data)
# ======================
def fetch_gold():
    print("Fetching GOLD...")
    url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=1day&outputsize=1&apikey={TWELVE_API_KEY}"

    res = requests.get(url).json()

    if "values" not in res:
        raise ValueError(f"TwelveData error: {res}")

    d = res["values"][0]

    return {
        "date": pd.to_datetime(d["datetime"]).date(),
        "open": safe_float(d["open"]),
        "high": safe_float(d["high"]),
        "low": safe_float(d["low"]),
        "close": safe_float(d["close"]),
    }

# ======================
# YFINANCE (ROBUST)
# ======================
def fetch_yfinance(symbol, retries=3):
    for i in range(retries):
        try:
            print(f"Fetching {symbol} (try {i+1})...")

            df = yf.download(symbol, period="5d", interval="1d", progress=False)

            if df.empty:
                raise ValueError("Empty dataframe")

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            value = float(df.tail(1)["Close"].values[0])

            print(f"{symbol}: {value}")
            return value

        except Exception as e:
            print(f"Error {symbol}: {e}")
            time.sleep(2)

    print(f"FAILED {symbol}")
    return None  # không crash

# ======================
# FRED (ROBUST)
# ======================
def fetch_fred(series):
    try:
        print(f"Fetching FRED {series}...")
        df = pdr.DataReader(series, "fred")
        return float(df.tail(1).values[0][0])
    except Exception as e:
        print(f"FRED error {series}: {e}")
        return None

# ======================
# INSERT GOLD
# ======================
def insert_gold(date_id, g):
    cursor.execute("""
        INSERT INTO predict_gold_price.gold_price
        (date_id, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING;
    """, (
        date_id,
        g["open"],
        g["high"],
        g["low"],
        g["close"],
    ))

# ======================
# INSERT FEATURE
# ======================
def insert_feature(date_id, dxy, sp500, oil, rate, cpi):
    cursor.execute("""
        INSERT INTO predict_gold_price.feature
        (date_id, dxy, sp500, oil, interest_rate, cpi)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING;
    """, (
        date_id,
        dxy,
        sp500,
        oil,
        rate,
        cpi
    ))

# ======================
# MAIN
# ======================
def main():
    try:
        print("========== START JOB ==========")

        gold = fetch_gold()
        date_id = upsert_dim_date(gold["date"])

        print("Fetching MARKET...")

        # fallback symbol cho DXY
        dxy = fetch_yfinance("DX=F")      # ổn định hơn DX-Y.NYB
        sp500 = fetch_yfinance("^GSPC")
        oil = fetch_yfinance("CL=F")

        print("Fetching MACRO...")

        rate = fetch_fred("FEDFUNDS")
        cpi = fetch_fred("CPIAUCSL")

        print("Inserting DB...")

        insert_gold(date_id, gold)
        insert_feature(date_id, dxy, sp500, oil, rate, cpi)

        conn.commit()

        print("DONE:", datetime.now())

    except Exception as e:
        conn.rollback()
        print("FATAL ERROR:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
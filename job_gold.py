import requests
import psycopg2
import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr
from datetime import datetime
import os
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
    url = f"https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=1day&outputsize=1&apikey={TWELVE_API_KEY}"
    res = requests.get(url).json()

    d = res["values"][0]

    return {
        "date": pd.to_datetime(d["datetime"]).date(),
        "open": float(d["open"]),
        "high": float(d["high"]),
        "low": float(d["low"]),
        "close": float(d["close"]),
        "volume": int(d["volume"])
    }

# ======================
# YFINANCE
# ======================
def fetch_yfinance(symbol):
    df = yf.download(symbol, period="5d", interval="1d")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.tail(1)

    return float(df["Close"].values[0])

# ======================
# FRED
# ======================
def fetch_fred(series):
    df = pdr.DataReader(series, "fred")
    return float(df.tail(1).values[0][0])

# ======================
# INSERT GOLD
# ======================
def insert_gold(date_id, g):
    cursor.execute("""
        INSERT INTO predict_gold_price.gold_price
        (date_id, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING;
    """, (
        date_id,
        g["open"],
        g["high"],
        g["low"],
        g["close"],
        g["volume"]
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
        print("Fetching...")

        gold = fetch_gold()
        date_id = upsert_dim_date(gold["date"])

        # market
        dxy = fetch_yfinance("DX-Y.NYB")
        sp500 = fetch_yfinance("^GSPC")
        oil = fetch_yfinance("CL=F")

        # macro
        rate = fetch_fred("FEDFUNDS")
        cpi = fetch_fred("CPIAUCSL")

        insert_gold(date_id, gold)
        insert_feature(date_id, dxy, sp500, oil, rate, cpi)

        conn.commit()

        print("Done:", datetime.now())

    except Exception as e:
        conn.rollback()
        print("Error:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
scripts/seed_data.py
Seeds local PostgreSQL with realistic economic data for development.
Run AFTER sql/schema/star_schema.sql
"""

import os, sys, random, math
import psycopg2
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

DB = dict(
    host     = os.getenv("DB_HOST",     "localhost"),
    port     = int(os.getenv("DB_PORT", "5432")),
    dbname   = os.getenv("DB_NAME",     "economicdb"),
    user     = os.getenv("DB_USER",     "postgres"),
    password = os.getenv("DB_PASSWORD", "postgres"),
)

# ── Real-ish economic data (World Bank approximations) ────
GDP_DATA = {
    "USA": [18.7,19.5,20.6,21.4,21.0,23.3,25.5,27.4,27.9],
    "CHN": [11.1,12.2,13.6,14.3,14.7,17.7,18.1,17.9,18.5],
    "IND": [2.1, 2.3, 2.7, 2.9, 2.7, 3.2, 3.5, 3.7, 4.0],
    "DEU": [3.5, 3.7, 3.9, 4.0, 3.9, 4.3, 4.1, 4.4, 4.5],
    "GBR": [2.7, 2.6, 2.8, 2.8, 2.7, 3.1, 3.1, 3.0, 3.1],
    "JPN": [4.4, 4.9, 4.9, 5.1, 5.0, 5.0, 4.2, 4.2, 4.3],
    "FRA": [2.4, 2.5, 2.6, 2.7, 2.6, 2.9, 2.8, 2.9, 3.0],
    "BRA": [1.8, 1.8, 2.1, 1.9, 1.4, 1.9, 2.1, 2.1, 2.2],
    "CAN": [1.6, 1.6, 1.7, 1.8, 1.7, 2.0, 2.1, 2.1, 2.2],
    "AUS": [1.3, 1.3, 1.4, 1.5, 1.3, 1.6, 1.7, 1.7, 1.8],
}
INF_DATA = {
    "USA": [0.1, 1.3, 2.1, 2.4, 1.2, 4.7, 8.0, 3.1, 2.5],
    "CHN": [1.4, 2.0, 1.8, 2.1, 2.5, 0.9, 2.0, 0.7, 1.2],
    "IND": [4.9, 4.5, 3.6, 3.4, 6.2, 5.5, 6.7, 5.4, 4.8],
    "DEU": [0.7, 0.5, 1.7, 1.9, 1.4, 3.1, 8.7, 3.0, 2.2],
    "GBR": [0.0, 0.7, 2.7, 2.5, 0.9, 2.5, 9.0, 4.0, 2.8],
    "JPN": [0.8, -0.1,0.5, 1.0, 0.0, -0.2,2.5, 2.8, 2.0],
    "FRA": [0.1, 0.3, 1.2, 2.1, 0.5, 2.1, 5.9, 4.9, 2.5],
    "BRA": [9.0, 8.7, 3.4, 3.7, 4.5, 8.3,11.9, 5.1, 4.2],
    "CAN": [1.1, 1.4, 1.6, 2.3, 0.7, 3.4, 6.8, 2.7, 2.1],
    "AUS": [1.5, 1.3, 2.0, 1.9, 0.9, 2.8, 6.6, 3.4, 2.6],
}
UNP_DATA = {
    "USA": [5.3, 4.9, 4.4, 3.9, 8.1, 5.4, 3.6, 3.9, 4.1],
    "CHN": [4.1, 4.0, 3.9, 3.8, 5.6, 5.1, 5.5, 5.2, 5.0],
    "IND": [8.8, 8.5, 7.9, 7.6,10.8, 7.9, 7.8, 7.9, 7.5],
    "DEU": [4.6, 4.2, 3.8, 3.4, 5.9, 5.7, 5.3, 3.0, 3.2],
    "GBR": [5.4, 5.0, 4.4, 4.0, 4.5, 4.5, 3.7, 4.2, 4.4],
    "JPN": [3.4, 3.1, 2.8, 2.4, 2.8, 2.8, 2.6, 2.6, 2.5],
    "FRA": [10.4,10.1,9.4, 8.8, 8.0, 7.9, 7.3, 7.3, 7.1],
    "BRA": [8.5, 11.3,12.7,12.3,14.7,14.4,11.8, 7.9, 7.0],
    "CAN": [6.9, 7.0, 6.4, 5.8, 9.6, 7.5, 5.2, 5.8, 6.1],
    "AUS": [6.1, 5.7, 5.6, 5.3, 6.5, 5.1, 3.5, 3.7, 3.9],
}

YEARS = list(range(2016, 2025))

INDICATOR_CODES = {
    "NY.GDP.MKTP.CD":    1,
    "FP.CPI.TOTL.ZG":   3,
    "SL.UEM.TOTL.ZS":   4,
    "NY.GDP.MKTP.KD.ZG": 2,
}

COUNTRY_KEYS = {"USA":1,"CHN":2,"IND":3,"DEU":4,"GBR":5,"JPN":6,"FRA":7,"BRA":8,"CAN":9,"AUS":10}

def seed(conn):
    cur = conn.cursor()

    # ── Insert dim_source ──────────────────────────
    cur.execute("""
        INSERT INTO dim_source (source_name, source_url, reliability)
        VALUES ('World Bank','https://data.worldbank.org',9),
               ('FRED','https://fred.stlouisfed.org',10),
               ('Yahoo Finance','https://finance.yahoo.com',7),
               ('UN Comtrade','https://comtradeplus.un.org',8)
        ON CONFLICT DO NOTHING
    """)

    # ── Insert fact_economic_indicators ───────────
    rows = []
    for code, c_key in COUNTRY_KEYS.items():
        gdp_vals = GDP_DATA.get(code, [None]*9)
        inf_vals = INF_DATA.get(code, [None]*9)
        unp_vals = UNP_DATA.get(code, [None]*9)

        for i, year in enumerate(YEARS):
            date_key = int(f"{year}1231")
            # GDP
            if i < len(gdp_vals) and gdp_vals[i]:
                v = gdp_vals[i] * 1e9
                prev = gdp_vals[i-1]*1e9 if i > 0 else None
                yoy  = round((v - prev)/prev*100, 2) if prev else None
                rows.append((date_key, c_key, 1, 1, v, yoy, False, f"SEED_{year}"))
            # Inflation
            if i < len(inf_vals) and inf_vals[i] is not None:
                rows.append((date_key, c_key, 3, 1, inf_vals[i], None, False, f"SEED_{year}"))
            # Unemployment
            if i < len(unp_vals) and unp_vals[i] is not None:
                rows.append((date_key, c_key, 4, 1, unp_vals[i], None, False, f"SEED_{year}"))

    cur.executemany("""
        INSERT INTO fact_economic_indicators
            (date_key, country_key, indicator_key, source_key, value, yoy_change, is_estimated, pipeline_run_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    # ── Insert sample market data ──────────────────
    market_rows = []
    for year in YEARS:
        for quarter in range(1, 5):
            for sector_key in range(1, 12):
                for c_key in list(COUNTRY_KEYS.values())[:5]:
                    base  = 100 + random.random() * 200
                    chg   = random.gauss(0.5, 3)
                    close = round(base * (1 + chg/100), 2)
                    month = quarter * 3
                    d_key = int(f"{year}{month:02d}{'30' if month in [6,9] else '31'}")
                    market_rows.append((d_key, c_key, sector_key,
                                        f"IDX_{c_key}_{sector_key}",
                                        round(base,2), close,
                                        round(max(base,close)*1.005,2),
                                        round(min(base,close)*0.995,2),
                                        random.randint(100000,5000000),
                                        round(close * random.randint(1000,50000) * 1e6, 2),
                                        round(random.uniform(10,45),2),
                                        f"SEED_{year}"))
    cur.executemany("""
        INSERT INTO fact_market_data
            (date_key,country_key,sector_key,index_name,open_price,close_price,
             high_price,low_price,volume,market_cap_usd,pe_ratio,load_timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
    """, market_rows)

    conn.commit()
    print(f"✅  Seeded {len(rows)} indicator rows + {len(market_rows)} market rows")
    cur.close()


if __name__ == "__main__":
    print(f"Connecting to PostgreSQL at {DB['host']}:{DB['port']}/{DB['dbname']} ...")
    try:
        conn = psycopg2.connect(**DB)
        seed(conn)
        conn.close()
        print("✅  Database seeding complete!")
    except Exception as e:
        print(f"❌  Error: {e}")
        print("\nMake sure PostgreSQL is running and you've run:")
        print("  psql -U postgres -c 'CREATE DATABASE economicdb;'")
        print("  psql -U postgres -d economicdb -f sql/schema/star_schema.sql")
        sys.exit(1)

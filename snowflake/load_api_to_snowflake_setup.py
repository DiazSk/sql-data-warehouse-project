"""
================================================================================
Script: load_api_to_snowflake_setup.py
Description: Fetch external API data and load into Snowflake Bronze layer
Author: Zaid Shaikh
================================================================================

PURPOSE:
  Fetches data from 3 external APIs and loads directly into Snowflake Bronze tables.
  Migrated from PostgreSQL (psycopg2) → Snowflake (snowflake-connector-python).

  APIs:
    1. Nager.Date     → RAW_BRAZIL_HOLIDAYS     (~42 records)
    2. Frankfurter    → RAW_CURRENCY_RATES       (~550 records)
    3. Open-Meteo     → RAW_WEATHER_HISTORY      (~21K records)

PREREQUISITES:
  pip install snowflake-connector-python requests

USAGE:
  Set environment variables:
    export SNOWFLAKE_ACCOUNT='your_account'    # e.g., 'abc12345.us-west-2.aws'
    export SNOWFLAKE_USER='your_username'
    export SNOWFLAKE_PASSWORD='your_password'

  Run:
    python load_api_to_snowflake_setup.py

================================================================================
"""

import os
import sys
import time
import requests
from datetime import datetime, timedelta

try:
    import snowflake.connector
except ImportError:
    print("ERROR: snowflake-connector-python not installed.")
    print("Run: pip install snowflake-connector-python")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Snowflake connection (from environment variables)
SNOWFLAKE_CONFIG = {
    "account":   os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "user":      os.environ.get("SNOWFLAKE_USER", ""),
    "password":  os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "database":  "OLIST_DWH",
    "schema":    "BRONZE",
    "warehouse": "OLIST_WH",
}

# Date range matching Olist dataset (Sep 2016 – Oct 2018)
START_DATE = "2016-09-01"
END_DATE = "2018-10-31"

# Brazilian state capitals for weather data
BRAZIL_STATE_CAPITALS = {
    "AC": (-9.9754, -67.8098),  "AL": (-9.6658, -35.7353),
    "AP": (0.0349, -51.0694),   "AM": (-3.1190, -60.0217),
    "BA": (-12.9714, -38.5124), "CE": (-3.7172, -38.5433),
    "DF": (-15.8267, -47.9218), "ES": (-20.3155, -40.3128),
    "GO": (-16.6869, -49.2648), "MA": (-2.5297, -44.2825),
    "MT": (-15.6010, -56.0974), "MS": (-20.4697, -54.6201),
    "MG": (-19.9167, -43.9345), "PA": (-1.4558, -48.5024),
    "PB": (-7.1195, -34.8450),  "PR": (-25.4284, -49.2733),
    "PE": (-8.0476, -34.8770),  "PI": (-5.0892, -42.8019),
    "RJ": (-22.9068, -43.1729), "RN": (-5.7945, -35.2110),
    "RS": (-30.0346, -51.2177), "RO": (-8.7612, -63.9004),
    "RR": (2.8195, -60.6714),   "SC": (-27.5954, -48.5480),
    "SP": (-23.5505, -46.6333), "SE": (-10.9091, -37.0677),
    "TO": (-10.1689, -48.3317),
}


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_connection():
    """
    Create Snowflake connection.
    
    SNOWFLAKE vs POSTGRESQL:
    - PostgreSQL: psycopg2.connect(host=, port=, dbname=, user=, password=)
    - Snowflake:  snowflake.connector.connect(account=, user=, password=,
                      database=, schema=, warehouse=)
    
    Key difference: Snowflake uses 'account' identifier instead of host/port.
    The account ID is in your Snowflake URL: https://<account>.snowflakecomputing.com
    """
    if not SNOWFLAKE_CONFIG["account"]:
        print("ERROR: SNOWFLAKE_ACCOUNT environment variable not set.")
        print("Find your account ID in Snowsight URL or run: SELECT CURRENT_ACCOUNT();")
        sys.exit(1)

    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


# =============================================================================
# API 1: BRAZILIAN HOLIDAYS (Nager.Date)
# =============================================================================

def fetch_holidays():
    """Fetch Brazilian public holidays for 2016-2018 from Nager.Date API."""
    print("\n" + "=" * 60)
    print("1/3  FETCHING: Brazilian Holidays (Nager.Date)")
    print("=" * 60)

    all_records = []
    base_url = "https://date.nager.at/api/v3/PublicHolidays"

    for year in [2016, 2017, 2018]:
        url = f"{base_url}/{year}/BR"
        print(f"  Fetching {year}...")

        response = requests.get(url, timeout=30)
        response.raise_for_status()
        holidays = response.json()

        for h in holidays:
            all_records.append((
                h["date"],
                h.get("localName", ""),
                h.get("name", ""),
                "BR",
                str(h.get("fixed", False)),
                str(h.get("global", True)),
                ",".join(h.get("types", [])),
            ))

        print(f"    → {len(holidays)} holidays")
        time.sleep(0.5)

    print(f"  Total: {len(all_records)} holidays")
    return all_records


def load_holidays(conn, records):
    """Load holidays into Snowflake Bronze table."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BRONZE.RAW_BRAZIL_HOLIDAYS")

    cursor.executemany(
        """INSERT INTO BRONZE.RAW_BRAZIL_HOLIDAYS
           (holiday_date, local_name, holiday_name, country_code,
            is_fixed, is_global, holiday_types)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        records,
    )

    # Update metadata
    cursor.execute("""
        UPDATE BRONZE.RAW_BRAZIL_HOLIDAYS
        SET dwh_source_file = 'api:nager.date'
        WHERE dwh_source_file IS NULL
    """)

    cursor.execute("SELECT COUNT(*) FROM BRONZE.RAW_BRAZIL_HOLIDAYS")
    count = cursor.fetchone()[0]
    print(f"  ✓ Loaded {count} holidays into BRONZE.RAW_BRAZIL_HOLIDAYS")
    cursor.close()


# =============================================================================
# API 2: CURRENCY RATES (Frankfurter)
# =============================================================================

def fetch_currency_rates():
    """Fetch BRL→USD exchange rates from Frankfurter API."""
    print("\n" + "=" * 60)
    print("2/3  FETCHING: Currency Rates (Frankfurter)")
    print("=" * 60)

    url = f"https://api.frankfurter.app/{START_DATE}..{END_DATE}"
    params = {"from": "BRL", "to": "USD"}

    print(f"  Fetching BRL→USD rates: {START_DATE} to {END_DATE}...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    records = []
    for date_str, rates in data["rates"].items():
        records.append((
            date_str,
            "BRL",
            "USD",
            str(rates["USD"]),
        ))

    records.sort(key=lambda x: x[0])
    print(f"  Total: {len(records)} daily rates")
    return records


def load_currency_rates(conn, records):
    """Load currency rates into Snowflake Bronze table."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BRONZE.RAW_CURRENCY_RATES")

    cursor.executemany(
        """INSERT INTO BRONZE.RAW_CURRENCY_RATES
           (rate_date, base_currency, target_currency, exchange_rate)
           VALUES (%s, %s, %s, %s)""",
        records,
    )

    cursor.execute("""
        UPDATE BRONZE.RAW_CURRENCY_RATES
        SET dwh_source_file = 'api:frankfurter'
        WHERE dwh_source_file IS NULL
    """)

    cursor.execute("SELECT COUNT(*) FROM BRONZE.RAW_CURRENCY_RATES")
    count = cursor.fetchone()[0]
    print(f"  ✓ Loaded {count} rates into BRONZE.RAW_CURRENCY_RATES")
    cursor.close()


# =============================================================================
# API 3: WEATHER HISTORY (Open-Meteo)
# =============================================================================

def fetch_weather():
    """Fetch daily weather history for all Brazilian state capitals."""
    print("\n" + "=" * 60)
    print("3/3  FETCHING: Weather History (Open-Meteo)")
    print("=" * 60)
    print(f"  Period: {START_DATE} to {END_DATE}")
    print(f"  Locations: {len(BRAZIL_STATE_CAPITALS)} state capitals")

    base_url = "https://archive-api.open-meteo.com/v1/archive"
    daily_vars = "temperature_2m_mean,temperature_2m_max,precipitation_sum,weather_code"
    all_records = []

    for state, (lat, lng) in BRAZIL_STATE_CAPITALS.items():
        params = {
            "latitude": lat,
            "longitude": lng,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "daily": daily_vars,
            "timezone": "America/Sao_Paulo",
        }

        print(f"  Fetching {state} ({lat}, {lng})...", end=" ")

        try:
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            daily = data.get("daily", {})
            dates = daily.get("time", [])

            for i, date_str in enumerate(dates):
                all_records.append((
                    str(lat),
                    str(lng),
                    state,
                    date_str,
                    str(daily.get("temperature_2m_mean", [None])[i] or ""),
                    str(daily.get("temperature_2m_max", [None])[i] or ""),
                    str(daily.get("precipitation_sum", [None])[i] or ""),
                    str(daily.get("weather_code", [None])[i] or ""),
                ))

            print(f"→ {len(dates)} days")

        except Exception as e:
            print(f"→ FAILED: {e}")

        time.sleep(0.3)  # Rate limiting

    print(f"  Total: {len(all_records)} daily weather records")
    return all_records


def load_weather(conn, records):
    """Load weather data into Snowflake Bronze table."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BRONZE.RAW_WEATHER_HISTORY")

    # Batch insert
    batch_size = 5000
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        cursor.executemany(
            """INSERT INTO BRONZE.RAW_WEATHER_HISTORY
               (latitude, longitude, state_code, weather_date,
                temperature_2m_mean, temperature_2m_max,
                precipitation_sum, weather_code)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            batch,
        )
        print(f"    Inserted batch {i // batch_size + 1} ({len(batch)} rows)")

    cursor.execute("""
        UPDATE BRONZE.RAW_WEATHER_HISTORY
        SET dwh_source_file = 'api:open-meteo'
        WHERE dwh_source_file IS NULL
    """)

    cursor.execute("SELECT COUNT(*) FROM BRONZE.RAW_WEATHER_HISTORY")
    count = cursor.fetchone()[0]
    print(f"  ✓ Loaded {count} records into BRONZE.RAW_WEATHER_HISTORY")
    cursor.close()


# =============================================================================
# MAIN
# =============================================================================

def main():
    start = datetime.now()
    print("=" * 60)
    print("OLIST DATA WAREHOUSE — API Data Load to Snowflake")
    print("=" * 60)
    print(f"Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target:  {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}")

    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    conn = get_connection()
    print("  ✓ Connected")

    try:
        # 1. Holidays
        holidays = fetch_holidays()
        load_holidays(conn, holidays)

        # 2. Currency Rates
        rates = fetch_currency_rates()
        load_currency_rates(conn, rates)

        # 3. Weather
        weather = fetch_weather()
        load_weather(conn, weather)

        conn.commit()

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()

    # Summary
    elapsed = datetime.now() - start
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  ✓ Holidays:  {len(holidays)} records")
    print(f"  ✓ Currency:  {len(rates)} records")
    print(f"  ✓ Weather:   {len(weather)} records")
    print(f"  Duration:    {elapsed}")
    print("=" * 60)
    print("\nNEXT: Re-run silver_layer_setup.sql to transform API data")
    print("      (API tables in Silver will now populate)")


if __name__ == "__main__":
    main()
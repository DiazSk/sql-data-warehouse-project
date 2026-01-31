"""
================================================================================
Description: Fetch historical BRL to USD exchange rates from Frankfurter API
================================================================================

PURPOSE:
--------
Fetches historical daily exchange rates (BRL to USD) for the Olist dataset
period (Sep 2016 - Oct 2018) and loads them into bronze.api_currency_rates.

API DETAILS:
------------
- Provider: Frankfurter (https://www.frankfurter.app) - Free, open-source
- Endpoint: https://api.frankfurter.app/{start_date}..{end_date}
- Rate Limit: None for reasonable usage
- Authentication: None required
- Data Source: European Central Bank

USAGE:
------
python fetch_currency_rates.py

PREREQUISITES:
--------------
pip install requests psycopg2-binary

NOTE:
-----
The API returns rates in chunks, so we fetch in yearly batches to avoid
timeouts and manage data efficiently.

================================================================================
"""

import requests
import psycopg2
import time
from dotenv import load_dotenv
import os

load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

# Database connection settings - loaded from .env file
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_DATABASE", "olist_dwh"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# API settings
API_BASE_URL = "https://api.frankfurter.app"
BASE_CURRENCY = "BRL"
TARGET_CURRENCY = "USD"

# Date range matching Olist dataset (Sep 2016 - Oct 2018)
DATE_RANGES = [
    ("2016-09-01", "2016-12-31"),
    ("2017-01-01", "2017-12-31"),
    ("2018-01-01", "2018-10-31"),
]

# =============================================================================
# API FUNCTIONS
# =============================================================================


def fetch_rates_for_range(start_date: str, end_date: str) -> dict:
    """
    Fetch exchange rates for a date range from Frankfurter API.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Dictionary with dates as keys and rates as values
    """
    url = f"{API_BASE_URL}/{start_date}..{end_date}"
    params = {"from": BASE_CURRENCY, "to": TARGET_CURRENCY}

    print(f"  Fetching {start_date} to {end_date}...")

    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()
        rates = data.get("rates", {})

        print(f"  ✓ Found {len(rates)} daily rates")

        return rates

    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error fetching rates: {e}")
        return {}


def fetch_all_rates() -> list:
    """
    Fetch rates for all configured date ranges.

    Returns:
        List of (date, rate) tuples
    """
    all_rates = []

    for start_date, end_date in DATE_RANGES:
        rates_dict = fetch_rates_for_range(start_date, end_date)

        for rate_date, currencies in rates_dict.items():
            # Extract USD rate
            usd_rate = currencies.get(TARGET_CURRENCY)
            if usd_rate:
                all_rates.append((rate_date, usd_rate))

        time.sleep(1)  # Be nice to the API

    # Sort by date
    all_rates.sort(key=lambda x: x[0])

    return all_rates


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def truncate_table(cursor):
    """Truncate the target table before loading."""
    cursor.execute("TRUNCATE TABLE bronze.api_currency_rates;")
    print("  ✓ Table truncated")


def insert_rates(cursor, rates: list):
    """
    Insert exchange rates into the Bronze table.

    Args:
        cursor: Database cursor
        rates: List of (date, rate) tuples
    """
    insert_query = """
        INSERT INTO bronze.api_currency_rates (
            rate_date,
            base_currency,
            target_currency,
            exchange_rate,
            dwh_load_date,
            dwh_source_file
        ) VALUES (
            %s, %s, %s, %s, CURRENT_TIMESTAMP, %s
        );
    """

    source_file = "api_frankfurter"

    for rate_date, rate_value in rates:
        cursor.execute(
            insert_query,
            (
                rate_date,
                BASE_CURRENCY,
                TARGET_CURRENCY,
                str(rate_value),
                source_file,
            ),
        )


def load_to_database(rates: list):
    """
    Load rates into the Bronze layer table.

    Args:
        rates: List of (date, rate) tuples
    """
    print("\nLoading to database...")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Truncate and load
        truncate_table(cursor)
        insert_rates(cursor, rates)

        conn.commit()
        print(f"  ✓ Inserted {len(rates)} exchange rate records")

    except psycopg2.Error as e:
        print(f"  ✗ Database error: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()


# =============================================================================
# VERIFICATION
# =============================================================================


def verify_load():
    """Verify the data was loaded correctly."""
    print("\nVerifying load...")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Count and date range
    cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            MIN(rate_date) as min_date,
            MAX(rate_date) as max_date,
            MIN(exchange_rate::numeric) as min_rate,
            MAX(exchange_rate::numeric) as max_rate,
            AVG(exchange_rate::numeric) as avg_rate
        FROM bronze.api_currency_rates;
    """)

    row = cursor.fetchone()
    print("\n  Summary:")
    print("  " + "-" * 40)
    print(f"  Total records: {row[0]}")
    print(f"  Date range: {row[1]} to {row[2]}")
    print(f"  Rate range: {row[3]:.4f} to {row[4]:.4f}")
    print(f"  Average rate: {row[5]:.4f}")

    # Monthly averages
    cursor.execute("""
        SELECT 
            TO_CHAR(rate_date::date, 'YYYY-MM') as month,
            ROUND(AVG(exchange_rate::numeric), 4) as avg_rate
        FROM bronze.api_currency_rates
        GROUP BY TO_CHAR(rate_date::date, 'YYYY-MM')
        ORDER BY month
        LIMIT 6;
    """)

    print("\n  Monthly average rates (first 6 months):")
    print("  " + "-" * 30)
    for row in cursor.fetchall():
        print(f"  {row[0]}: 1 BRL = {row[1]} USD")

    conn.close()


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main execution function."""
    print("=" * 60)
    print("FETCH CURRENCY EXCHANGE RATES")
    print("=" * 60)
    print("Source: Frankfurter API (European Central Bank data)")
    print(f"Conversion: {BASE_CURRENCY} → {TARGET_CURRENCY}")
    print(f"Period: {DATE_RANGES[0][0]} to {DATE_RANGES[-1][1]}")
    print("=" * 60)

    # Fetch from API
    print("\nFetching exchange rates from API...")
    rates = fetch_all_rates()

    if not rates:
        print("\n✗ No rates fetched. Exiting.")
        return

    print(f"\nTotal daily rates fetched: {len(rates)}")

    # Load to database
    load_to_database(rates)

    # Verify
    verify_load()

    print("\n" + "=" * 60)
    print("✓ Currency rates load complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

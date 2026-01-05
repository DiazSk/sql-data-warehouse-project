"""
================================================================================
Description: Fetch Brazilian public holidays from Nager.Date API
================================================================================

PURPOSE:
--------
Fetches Brazilian public holidays for years 2016, 2017, 2018 and loads them
into the bronze.api_brazil_holidays table.

API DETAILS:
------------
- Provider: Nager.Date (https://date.nager.at)
- Endpoint: https://date.nager.at/api/v3/PublicHolidays/{year}/{countryCode}
- Rate Limit: None (free and unlimited)
- Authentication: None required

USAGE:
------
python fetch_holidays.py

PREREQUISITES:
--------------
pip install requests psycopg2-binary

================================================================================
"""

import requests
import psycopg2
from datetime import datetime
import time
import os
from dotenv import load_dotenv

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
API_BASE_URL = "https://date.nager.at/api/v3/PublicHolidays"
COUNTRY_CODE = "BR"  # Brazil
YEARS = [2016, 2017, 2018]  # Years matching Olist dataset

# =============================================================================
# API FUNCTIONS
# =============================================================================


def fetch_holidays_for_year(year: int) -> list:
    """
    Fetch public holidays for a specific year from Nager.Date API.

    Args:
        year: The year to fetch holidays for

    Returns:
        List of holiday dictionaries
    """
    url = f"{API_BASE_URL}/{year}/{COUNTRY_CODE}"

    print(f"  Fetching holidays for {year}...")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        holidays = response.json()
        print(f"  ✓ Found {len(holidays)} holidays for {year}")

        return holidays

    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error fetching {year}: {e}")
        return []


def fetch_all_holidays() -> list:
    """
    Fetch holidays for all configured years.

    Returns:
        Combined list of all holidays
    """
    all_holidays = []

    for year in YEARS:
        holidays = fetch_holidays_for_year(year)
        all_holidays.extend(holidays)
        time.sleep(0.5)  # Be nice to the API

    return all_holidays


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def truncate_table(cursor):
    """Truncate the target table before loading."""
    cursor.execute("TRUNCATE TABLE bronze.api_brazil_holidays;")
    print("  ✓ Table truncated")


def insert_holidays(cursor, holidays: list):
    """
    Insert holidays into the Bronze table.

    Args:
        cursor: Database cursor
        holidays: List of holiday dictionaries from API
    """
    insert_query = """
        INSERT INTO bronze.api_brazil_holidays (
            holiday_date,
            local_name,
            holiday_name,
            country_code,
            is_fixed,
            is_global,
            holiday_types
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        );
    """

    for holiday in holidays:
        # Extract and transform data
        holiday_date = holiday.get("date", "")
        local_name = holiday.get("localName", "")
        holiday_name = holiday.get("name", "")
        country_code = holiday.get("countryCode", "")
        is_fixed = str(holiday.get("fixed", "")).lower()
        is_global = str(holiday.get("global", "")).lower()

        # Types is a list, convert to comma-separated string
        types = holiday.get("types", [])
        holiday_types = ",".join(types) if types else ""

        cursor.execute(
            insert_query,
            (
                holiday_date,
                local_name,
                holiday_name,
                country_code,
                is_fixed,
                is_global,
                holiday_types,
            ),
        )


def load_to_database(holidays: list):
    """
    Load holidays into the Bronze layer table.

    Args:
        holidays: List of holiday dictionaries
    """
    print("\nLoading to database...")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Truncate and load
        truncate_table(cursor)
        insert_holidays(cursor, holidays)

        conn.commit()
        print(f"  ✓ Inserted {len(holidays)} holiday records")

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

    # Count by year
    cursor.execute("""
        SELECT 
            EXTRACT(YEAR FROM holiday_date::date) as year,
            COUNT(*) as holiday_count
        FROM bronze.api_brazil_holidays
        GROUP BY EXTRACT(YEAR FROM holiday_date::date)
        ORDER BY year;
    """)

    print("\n  Holidays by year:")
    print("  " + "-" * 25)
    for row in cursor.fetchall():
        print(f"  {int(row[0])}: {row[1]} holidays")

    # Sample data
    cursor.execute("""
        SELECT holiday_date, holiday_name, local_name
        FROM bronze.api_brazil_holidays
        ORDER BY holiday_date
        LIMIT 5;
    """)

    print("\n  Sample records:")
    print("  " + "-" * 50)
    for row in cursor.fetchall():
        print(f"  {row[0]} | {row[1]} | {row[2]}")

    conn.close()


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main execution function."""
    print("=" * 60)
    print("FETCH BRAZILIAN HOLIDAYS")
    print("=" * 60)
    print("Source: Nager.Date API")
    print(f"Country: Brazil ({COUNTRY_CODE})")
    print(f"Years: {YEARS}")
    print("=" * 60)

    # Fetch from API
    print("\nFetching holidays from API...")
    holidays = fetch_all_holidays()

    if not holidays:
        print("\n✗ No holidays fetched. Exiting.")
        return

    print(f"\nTotal holidays fetched: {len(holidays)}")

    # Load to database
    load_to_database(holidays)

    # Verify
    verify_load()

    print("\n" + "=" * 60)
    print("✓ Holiday data load complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

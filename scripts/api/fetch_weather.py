"""
================================================================================
Description: Fetch historical weather data from Open-Meteo API
================================================================================

PURPOSE:
--------
Fetches historical daily weather data for all 27 Brazilian state capitals
for the Olist dataset period (Sep 2016 - Oct 2018) and loads it into
bronze.api_weather_history.

API DETAILS:
------------
- Provider: Open-Meteo (https://open-meteo.com)
- Endpoint: https://archive-api.open-meteo.com/v1/archive
- Rate Limit: 10,000 requests/day (free tier)
- Authentication: None required
- Data Source: ERA5 reanalysis data

USAGE:
------
python fetch_weather.py

PREREQUISITES:
--------------
pip install requests psycopg2-binary

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
API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Date range matching Olist dataset
START_DATE = "2016-09-01"
END_DATE = "2018-10-31"

# Weather variables to fetch (minimal set for business analysis)
DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_max",
    "precipitation_sum",
]

# All 27 Brazilian state capitals with coordinates
BRAZIL_STATE_CAPITALS = [
    # North Region
    {"state": "AC", "city": "Rio Branco", "lat": -9.9754, "lon": -67.8249},
    {"state": "AP", "city": "Macapá", "lat": 0.0356, "lon": -51.0705},
    {"state": "AM", "city": "Manaus", "lat": -3.1190, "lon": -60.0217},
    {"state": "PA", "city": "Belém", "lat": -1.4558, "lon": -48.4902},
    {"state": "RO", "city": "Porto Velho", "lat": -8.7612, "lon": -63.9004},
    {"state": "RR", "city": "Boa Vista", "lat": 2.8235, "lon": -60.6758},
    {"state": "TO", "city": "Palmas", "lat": -10.2491, "lon": -48.3243},
    # Northeast Region
    {"state": "AL", "city": "Maceió", "lat": -9.6498, "lon": -35.7089},
    {"state": "BA", "city": "Salvador", "lat": -12.9714, "lon": -38.5014},
    {"state": "CE", "city": "Fortaleza", "lat": -3.7172, "lon": -38.5433},
    {"state": "MA", "city": "São Luís", "lat": -2.5307, "lon": -44.3068},
    {"state": "PB", "city": "João Pessoa", "lat": -7.1195, "lon": -34.8450},
    {"state": "PE", "city": "Recife", "lat": -8.0476, "lon": -34.8770},
    {"state": "PI", "city": "Teresina", "lat": -5.0892, "lon": -42.8019},
    {"state": "RN", "city": "Natal", "lat": -5.7945, "lon": -35.2110},
    {"state": "SE", "city": "Aracaju", "lat": -10.9472, "lon": -37.0731},
    # Central-West Region
    {"state": "DF", "city": "Brasília", "lat": -15.7975, "lon": -47.8919},
    {"state": "GO", "city": "Goiânia", "lat": -16.6869, "lon": -49.2648},
    {"state": "MT", "city": "Cuiabá", "lat": -15.6014, "lon": -56.0979},
    {"state": "MS", "city": "Campo Grande", "lat": -20.4697, "lon": -54.6201},
    # Southeast Region
    {"state": "ES", "city": "Vitória", "lat": -20.3155, "lon": -40.3128},
    {"state": "MG", "city": "Belo Horizonte", "lat": -19.9167, "lon": -43.9345},
    {"state": "RJ", "city": "Rio de Janeiro", "lat": -22.9068, "lon": -43.1729},
    {"state": "SP", "city": "São Paulo", "lat": -23.5505, "lon": -46.6333},
    # South Region
    {"state": "PR", "city": "Curitiba", "lat": -25.4284, "lon": -49.2733},
    {"state": "RS", "city": "Porto Alegre", "lat": -30.0346, "lon": -51.2177},
    {"state": "SC", "city": "Florianópolis", "lat": -27.5954, "lon": -48.5480},
]

# =============================================================================
# API FUNCTIONS
# =============================================================================


def fetch_weather_for_location(state: str, lat: float, lon: float) -> list:
    """
    Fetch weather data for a specific location from Open-Meteo API.

    Args:
        state: Brazilian state code (e.g., 'SP')
        lat: Latitude
        lon: Longitude

    Returns:
        List of daily weather records
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "America/Sao_Paulo",
    }

    try:
        response = requests.get(API_BASE_URL, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()
        daily = data.get("daily", {})

        # Extract arrays
        dates = daily.get("time", [])
        weather_codes = daily.get("weather_code", [])
        temp_means = daily.get("temperature_2m_mean", [])
        temp_maxs = daily.get("temperature_2m_max", [])
        precip_sums = daily.get("precipitation_sum", [])

        # Combine into records
        records = []
        for i in range(len(dates)):
            records.append(
                {
                    "latitude": str(lat),
                    "longitude": str(lon),
                    "state_code": state,
                    "weather_date": dates[i],
                    "temperature_2m_mean": str(temp_means[i])
                    if temp_means[i] is not None
                    else None,
                    "temperature_2m_max": str(temp_maxs[i])
                    if temp_maxs[i] is not None
                    else None,
                    "precipitation_sum": str(precip_sums[i])
                    if precip_sums[i] is not None
                    else None,
                    "weather_code": str(weather_codes[i])
                    if weather_codes[i] is not None
                    else None,
                }
            )

        return records

    except requests.exceptions.RequestException as e:
        print(f"    ✗ Error: {e}")
        return []


def fetch_all_weather() -> list:
    """
    Fetch weather for all Brazilian state capitals.

    Returns:
        Combined list of all weather records
    """
    all_records = []
    total_locations = len(BRAZIL_STATE_CAPITALS)

    for idx, location in enumerate(BRAZIL_STATE_CAPITALS, 1):
        state = location["state"]
        city = location["city"]
        lat = location["lat"]
        lon = location["lon"]

        print(f"  [{idx}/{total_locations}] {state} - {city}...", end=" ")

        records = fetch_weather_for_location(state, lat, lon)

        if records:
            print(f"✓ {len(records)} days")
            all_records.extend(records)
        else:
            print("✗ Failed")

        # Rate limiting - be nice to the free API
        time.sleep(0.5)

    return all_records


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def truncate_table(cursor):
    """Truncate the target table before loading."""
    cursor.execute("TRUNCATE TABLE bronze.api_weather_history;")
    print("  ✓ Table truncated")


def insert_weather_batch(cursor, records: list, batch_size: int = 1000):
    """
    Insert weather records into the Bronze table in batches.

    Args:
        cursor: Database cursor
        records: List of weather record dictionaries
        batch_size: Number of records per batch
    """
    insert_query = """
        INSERT INTO bronze.api_weather_history (
            latitude,
            longitude,
            state_code,
            weather_date,
            temperature_2m_mean,
            temperature_2m_max,
            precipitation_sum,
            weather_code
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    total = len(records)
    inserted = 0

    for record in records:
        cursor.execute(
            insert_query,
            (
                record["latitude"],
                record["longitude"],
                record["state_code"],
                record["weather_date"],
                record["temperature_2m_mean"],
                record["temperature_2m_max"],
                record["precipitation_sum"],
                record["weather_code"],
            ),
        )

        inserted += 1

        # Progress indicator
        if inserted % 5000 == 0:
            print(f"    Inserted {inserted}/{total} records...")


def load_to_database(records: list):
    """
    Load weather records into the Bronze layer table.

    Args:
        records: List of weather record dictionaries
    """
    print("\nLoading to database...")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Truncate and load
        truncate_table(cursor)
        insert_weather_batch(cursor, records)

        conn.commit()
        print(f"  ✓ Inserted {len(records)} weather records")

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

    # Overall summary
    cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT state_code) as states,
            MIN(weather_date) as min_date,
            MAX(weather_date) as max_date
        FROM bronze.api_weather_history;
    """)

    row = cursor.fetchone()
    print("\n  Summary:")
    print("  " + "-" * 40)
    print(f"  Total records: {row[0]:,}")
    print(f"  States covered: {row[1]}")
    print(f"  Date range: {row[2]} to {row[3]}")

    # Records by state
    cursor.execute("""
        SELECT 
            state_code,
            COUNT(*) as days,
            ROUND(AVG(temperature_2m_mean::numeric), 1) as avg_temp,
            ROUND(SUM(precipitation_sum::numeric), 1) as total_precip
        FROM bronze.api_weather_history
        GROUP BY state_code
        ORDER BY state_code
        LIMIT 10;
    """)

    print("\n  Sample by state (first 10):")
    print("  " + "-" * 50)
    print(f"  {'State':<8} {'Days':<8} {'Avg Temp':<12} {'Total Precip'}")
    print("  " + "-" * 50)
    for row in cursor.fetchall():
        print(f"  {row[0]:<8} {row[1]:<8} {row[2]}°C{'':<6} {row[3]} mm")

    # Weather code distribution
    cursor.execute("""
        SELECT 
            weather_code,
            COUNT(*) as days,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
        FROM bronze.api_weather_history
        WHERE weather_code IS NOT NULL
        GROUP BY weather_code
        ORDER BY COUNT(*) DESC
        LIMIT 5;
    """)

    print("\n  Top 5 weather conditions:")
    print("  " + "-" * 40)

    # Weather code descriptions
    weather_codes = {
        "0": "Clear sky",
        "1": "Mainly clear",
        "2": "Partly cloudy",
        "3": "Overcast",
        "45": "Fog",
        "51": "Light drizzle",
        "53": "Moderate drizzle",
        "61": "Slight rain",
        "63": "Moderate rain",
        "65": "Heavy rain",
        "80": "Slight showers",
        "81": "Moderate showers",
        "95": "Thunderstorm",
    }

    for row in cursor.fetchall():
        code = row[0]
        desc = weather_codes.get(code, "Other")
        print(f"  Code {code}: {row[1]:,} days ({row[2]}%) - {desc}")

    conn.close()


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main execution function."""
    print("=" * 60)
    print("FETCH HISTORICAL WEATHER DATA")
    print("=" * 60)
    print("Source: Open-Meteo Archive API")
    print(f"Period: {START_DATE} to {END_DATE}")
    print(f"Locations: {len(BRAZIL_STATE_CAPITALS)} Brazilian state capitals")
    print(f"Variables: {', '.join(DAILY_VARIABLES)}")
    print("=" * 60)

    # Fetch from API
    print("\nFetching weather data from API...")
    records = fetch_all_weather()

    if not records:
        print("\n✗ No weather data fetched. Exiting.")
        return

    print(f"\nTotal daily records fetched: {len(records):,}")

    # Load to database
    load_to_database(records)

    # Verify
    verify_load()

    print("\n" + "=" * 60)
    print("✓ Weather data load complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

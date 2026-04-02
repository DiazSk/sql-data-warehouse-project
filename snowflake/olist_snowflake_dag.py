"""
=============================================================================
OLIST DATA WAREHOUSE - Snowflake ETL Pipeline DAG
=============================================================================
Orchestrates the complete ETL pipeline on Snowflake:
    Bronze (Stage → COPY INTO) → API Fetch → Silver (Transform) → Gold (Star Schema)

Migrated from PostgreSQL (psql BashOperator) → Snowflake (SnowflakeOperator)

Author: Zaid
Schedule: Manual trigger

POSTGRESQL → SNOWFLAKE DAG CHANGES:
  - BashOperator + psql         → SnowflakeOperator (executes SQL on Snowflake)
  - psycopg2 in PythonOperator  → snowflake.connector in PythonOperator
  - PGPASSWORD env var          → Airflow Connection 'snowflake_default'
  - Local file paths (/scripts) → SQL strings or file paths in container

SETUP:
  1. pip install apache-airflow-providers-snowflake snowflake-connector-python
  2. In Airflow UI → Admin → Connections → Add:
     - Connection Id: snowflake_default
     - Connection Type: Snowflake
     - Account: your_account (e.g., abc12345.us-west-2.aws)
     - Login: your_username
     - Password: your_password
     - Schema: BRONZE (default, overridden per task)
     - Database: OLIST_DWH
     - Warehouse: OLIST_WH
     - Role: (leave blank for default)

=============================================================================
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Snowflake provider — replaces BashOperator + psql
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import os
import time
import requests
import snowflake.connector


# =============================================================================
# Configuration
# =============================================================================

SNOWFLAKE_CONN_ID = "snowflake_default"  # Airflow Connection ID

# Base path to SQL scripts inside the Airflow container
SQL_BASE_PATH = Path("/opt/airflow/scripts/snowflake")

default_args = {
    "owner": "zaid",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# =============================================================================
# Helper: Read SQL file
# =============================================================================

def read_sql_file(filename: str) -> str:
    """Read SQL file and return contents as string."""
    filepath = SQL_BASE_PATH / filename
    with open(filepath, "r") as f:
        return f.read()


# =============================================================================
# Helper: Get Snowflake connection from Airflow
# =============================================================================

def get_snowflake_connection():
    """
    Get Snowflake connection using Airflow's connection manager.
    
    POSTGRESQL vs SNOWFLAKE:
    - PostgreSQL: psycopg2.connect(host=, port=, dbname=, user=, password=)
    - Snowflake:  snowflake.connector.connect(account=, user=, password=,
                      database=, schema=, warehouse=)
    """
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)

    return snowflake.connector.connect(
        account=conn.extra_dejson.get("account", ""),
        user=conn.login,
        password=conn.password,
        database=conn.extra_dejson.get("database", "OLIST_DWH"),
        schema=conn.schema or "BRONZE",
        warehouse=conn.extra_dejson.get("warehouse", "OLIST_WH"),
    )


# =============================================================================
# API Fetch Functions (psycopg2 → snowflake.connector)
# =============================================================================

def fetch_currency_rates():
    """Fetch BRL→USD exchange rates from Frankfurter API → Snowflake Bronze."""
    print("=" * 60)
    print("FETCHING CURRENCY RATES → Snowflake")
    print("=" * 60)

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    start_date = "2016-09-01"
    end_date = "2018-10-31"
    url = f"https://api.frankfurter.app/{start_date}..{end_date}?from=BRL&to=USD"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()

        cursor.execute("TRUNCATE TABLE BRONZE.RAW_CURRENCY_RATES")

        rates = data.get("rates", {})
        records = []
        for rate_date, rate_values in sorted(rates.items()):
            records.append((rate_date, "BRL", "USD", str(rate_values.get("USD", 0))))

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

        conn.commit()
        print(f"✓ Loaded {len(records)} currency rates")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def fetch_brazil_holidays():
    """Fetch Brazilian holidays from Nager.Date API → Snowflake Bronze."""
    print("=" * 60)
    print("FETCHING BRAZILIAN HOLIDAYS → Snowflake")
    print("=" * 60)

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE BRONZE.RAW_BRAZIL_HOLIDAYS")

        all_records = []
        for year in [2016, 2017, 2018]:
            url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/BR"
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

            print(f"  {year}: {len(holidays)} holidays")
            time.sleep(0.5)

        cursor.executemany(
            """INSERT INTO BRONZE.RAW_BRAZIL_HOLIDAYS
               (holiday_date, local_name, holiday_name, country_code,
                is_fixed, is_global, holiday_types)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            all_records,
        )

        cursor.execute("""
            UPDATE BRONZE.RAW_BRAZIL_HOLIDAYS
            SET dwh_source_file = 'api:nager.date'
            WHERE dwh_source_file IS NULL
        """)

        conn.commit()
        print(f"✓ Loaded {len(all_records)} holidays")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def fetch_weather_data():
    """Fetch historical weather from Open-Meteo API → Snowflake Bronze."""
    print("=" * 60)
    print("FETCHING WEATHER DATA → Snowflake")
    print("=" * 60)

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    states = {
        "AC": (-9.97, -67.81), "AL": (-9.67, -35.74), "AP": (0.03, -51.05),
        "AM": (-3.10, -60.02), "BA": (-12.97, -38.50), "CE": (-3.72, -38.54),
        "DF": (-15.78, -47.93), "ES": (-20.32, -40.34), "GO": (-16.68, -49.25),
        "MA": (-2.53, -44.27), "MT": (-15.60, -56.10), "MS": (-20.44, -54.65),
        "MG": (-19.92, -43.94), "PA": (-1.46, -48.50), "PB": (-7.12, -34.86),
        "PR": (-25.43, -49.27), "PE": (-8.05, -34.88), "PI": (-5.09, -42.80),
        "RJ": (-22.91, -43.17), "RN": (-5.79, -35.21), "RS": (-30.03, -51.23),
        "RO": (-8.76, -63.90), "RR": (2.82, -60.67), "SC": (-27.59, -48.55),
        "SP": (-23.55, -46.63), "SE": (-10.91, -37.07), "TO": (-10.18, -48.33),
    }

    start_date = "2016-09-01"
    end_date = "2018-10-31"

    try:
        cursor.execute("TRUNCATE TABLE BRONZE.RAW_WEATHER_HISTORY")
        conn.commit()

        total = 0
        for state, (lat, lon) in states.items():
            url = (
                f"https://archive-api.open-meteo.com/v1/archive?"
                f"latitude={lat}&longitude={lon}"
                f"&start_date={start_date}&end_date={end_date}"
                f"&daily=temperature_2m_mean,temperature_2m_max,precipitation_sum,weather_code"
                f"&timezone=America/Sao_Paulo"
            )

            try:
                response = requests.get(url, timeout=120)
                response.raise_for_status()
                data = response.json()

                daily = data.get("daily", {})
                dates = daily.get("time", [])
                records = []

                for i, date_str in enumerate(dates):
                    records.append((
                        str(lat), str(lon), state, date_str,
                        str(daily.get("temperature_2m_mean", [None])[i] or ""),
                        str(daily.get("temperature_2m_max", [None])[i] or ""),
                        str(daily.get("precipitation_sum", [None])[i] or ""),
                        str(daily.get("weather_code", [None])[i] or ""),
                    ))

                cursor.executemany(
                    """INSERT INTO BRONZE.RAW_WEATHER_HISTORY
                       (latitude, longitude, state_code, weather_date,
                        temperature_2m_mean, temperature_2m_max,
                        precipitation_sum, weather_code)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    records,
                )

                conn.commit()
                total += len(dates)
                print(f"  {state}: {len(dates)} days ✓")

            except Exception as e:
                conn.rollback()
                print(f"  {state}: FAILED - {e}")
                continue

            time.sleep(0.3)

        cursor.execute("""
            UPDATE BRONZE.RAW_WEATHER_HISTORY
            SET dwh_source_file = 'api:open-meteo'
            WHERE dwh_source_file IS NULL
        """)
        conn.commit()

        print(f"✓ Loaded {total} weather records")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="olist_snowflake_etl_pipeline",
    default_args=default_args,
    description="Olist Data Warehouse - Snowflake ETL Pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "etl", "data-warehouse", "snowflake"],
    doc_md="""
    ## Olist Snowflake ETL Pipeline

    Migrated from PostgreSQL → Snowflake.

    **Architecture:** Bronze → API Fetch → Silver → Gold
    **Compute:** Snowflake Virtual Warehouse (OLIST_WH, X-Small)
    **Key Changes from PostgreSQL version:**
    - BashOperator + psql → SnowflakeOperator
    - psycopg2 → snowflake-connector-python
    - COPY FROM → COPY INTO from @STAGE
    - SERIAL → AUTOINCREMENT
    - generate_series() → GENERATOR + DATEADD
    - DISTINCT ON → ROW_NUMBER()

    **Setup:** Configure 'snowflake_default' Airflow Connection
    """,
) as dag:

    # =========================================================================
    # BRONZE LAYER
    # CHANGE: BashOperator + psql → SnowflakeOperator
    # SnowflakeOperator sends SQL directly to Snowflake via the connection.
    # No need for psql, PGPASSWORD, or local file mounts.
    # =========================================================================
    with TaskGroup(group_id="bronze_layer", tooltip="Load raw data into Snowflake") as bronze:
        create_and_load_bronze = SnowflakeOperator(
            task_id="create_and_load",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=read_sql_file("02_bronze_setup.sql"),
            warehouse="OLIST_WH",
            database="OLIST_DWH",
            schema="BRONZE",
        )

    # =========================================================================
    # API FETCH (Parallel)
    # CHANGE: psycopg2.connect() → snowflake.connector.connect()
    # Same PythonOperator pattern, different database connector.
    # =========================================================================
    with TaskGroup(group_id="api_fetch", tooltip="Fetch external API data") as api:
        currency = PythonOperator(
            task_id="currency_rates",
            python_callable=fetch_currency_rates,
        )

        holidays = PythonOperator(
            task_id="brazil_holidays",
            python_callable=fetch_brazil_holidays,
        )

        weather = PythonOperator(
            task_id="weather_data",
            python_callable=fetch_weather_data,
            execution_timeout=timedelta(minutes=30),
        )

    # =========================================================================
    # SILVER LAYER
    # =========================================================================
    with TaskGroup(group_id="silver_layer", tooltip="Clean & transform data") as silver:
        transform_silver = SnowflakeOperator(
            task_id="create_and_transform",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=read_sql_file("03_silver_setup.sql"),
            warehouse="OLIST_WH",
            database="OLIST_DWH",
            schema="SILVER",
        )

    # =========================================================================
    # GOLD LAYER
    # =========================================================================
    with TaskGroup(group_id="gold_layer", tooltip="Build star schema") as gold:
        build_gold = SnowflakeOperator(
            task_id="create_and_load",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=read_sql_file("04_gold_setup.sql"),
            warehouse="OLIST_WH",
            database="OLIST_DWH",
            schema="GOLD",
        )

    # =========================================================================
    # Pipeline Flow: Bronze → API (parallel) → Silver → Gold
    # Same DAG structure as PostgreSQL version — only operators changed
    # =========================================================================
    bronze >> api >> silver >> gold
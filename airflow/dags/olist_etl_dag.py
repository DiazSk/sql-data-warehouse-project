"""
=============================================================================
OLIST DATA WAREHOUSE - ETL Pipeline DAG
=============================================================================
Orchestrates the complete ETL pipeline:
    Bronze (CSV Load) → API Fetch → Silver (Transform) → Gold (Star Schema)

Author: Zaid
Schedule: Manual trigger (can be changed to daily)
=============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import os
import requests
import psycopg2


# =============================================================================
# Configuration
# =============================================================================

default_args = {
    "owner": "zaid",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_db_connection():
    """Get connection to Olist DWH"""
    return psycopg2.connect(
        host=os.environ.get("DWH_HOST", "postgres"),
        port=os.environ.get("DWH_PORT", "5432"),
        database=os.environ.get("DWH_DATABASE", "olist_dwh"),
        user=os.environ.get("DWH_USER", "olist"),
        password=os.environ.get("DWH_PASSWORD", "olist123"),
    )


# =============================================================================
# API Fetch Functions
# =============================================================================


def fetch_currency_rates():
    """Fetch BRL→USD exchange rates from Frankfurter API"""
    print("=" * 60)
    print("FETCHING CURRENCY RATES")
    print("=" * 60)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Olist data date range
    start_date = "2016-09-01"
    end_date = "2018-10-31"

    url = f"https://api.frankfurter.app/{start_date}..{end_date}?from=BRL&to=USD"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()

        # Clear existing data
        cursor.execute("TRUNCATE TABLE bronze.api_currency_rates;")

        # Insert rates
        rates = data.get("rates", {})
        count = 0
        for rate_date, rate_values in rates.items():
            usd_rate = rate_values.get("USD", 0)
            cursor.execute(
                """
                INSERT INTO bronze.api_currency_rates 
                (rate_date, base_currency, target_currency, exchange_rate, dwh_load_date)
                VALUES (%s, 'BRL', 'USD', %s, CURRENT_TIMESTAMP)
            """,
                (rate_date, usd_rate),
            )
            count += 1

        conn.commit()
        print(f"✓ Loaded {count} currency rates")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def fetch_brazil_holidays():
    """Fetch Brazilian holidays from Nager.Date API"""
    print("=" * 60)
    print("FETCHING BRAZILIAN HOLIDAYS")
    print("=" * 60)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Clear existing
        cursor.execute("TRUNCATE TABLE bronze.api_brazil_holidays;")

        total = 0
        for year in [2016, 2017, 2018]:
            url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/BR"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            holidays = response.json()

            for h in holidays:
                cursor.execute(
                    """
                    INSERT INTO bronze.api_brazil_holidays 
                    (holiday_date, holiday_name, local_name, country_code, is_fixed, is_global, dwh_load_date)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                    (
                        h.get("date"),
                        h.get("name"),
                        h.get("localName"),
                        h.get("countryCode", "BR"),
                        h.get("fixed", False),
                        h.get("global", True),
                    ),
                )
                total += 1

            print(f"  {year}: {len(holidays)} holidays")

        conn.commit()
        print(f"✓ Loaded {total} total holidays")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def fetch_weather_data():
    """Fetch historical weather from Open-Meteo API"""
    print("=" * 60)
    print("FETCHING WEATHER DATA")
    print("=" * 60)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Brazilian state capitals
    states = {
        "SP": (-23.55, -46.63),
        "RJ": (-22.91, -43.17),
        "MG": (-19.92, -43.94),
        "BA": (-12.97, -38.50),
        "PR": (-25.43, -49.27),
        "RS": (-30.03, -51.23),
        "PE": (-8.05, -34.88),
        "CE": (-3.72, -38.54),
        "SC": (-27.59, -48.55),
        "GO": (-16.68, -49.25),
        "PA": (-1.46, -48.50),
        "MA": (-2.53, -44.27),
        "PB": (-7.12, -34.86),
        "AM": (-3.10, -60.02),
        "ES": (-20.32, -40.34),
        "RN": (-5.79, -35.21),
        "AL": (-9.67, -35.74),
        "PI": (-5.09, -42.80),
        "MT": (-15.60, -56.10),
        "DF": (-15.78, -47.93),
        "MS": (-20.44, -54.65),
        "SE": (-10.91, -37.07),
        "RO": (-8.76, -63.90),
        "TO": (-10.18, -48.33),
        "AC": (-9.97, -67.81),
        "AP": (0.03, -51.05),
        "RR": (2.82, -60.67),
    }

    try:
        cursor.execute("TRUNCATE TABLE bronze.api_weather_history;")
        conn.commit()  # Commit the truncate first

        start_date = "2016-09-01"
        end_date = "2018-10-31"
        total = 0

        for state, (lat, lon) in states.items():
            url = (
                f"https://archive-api.open-meteo.com/v1/archive?"
                f"latitude={lat}&longitude={lon}"
                f"&start_date={start_date}&end_date={end_date}"
                f"&daily=temperature_2m_mean,temperature_2m_max,precipitation_sum,weathercode"
                f"&timezone=America/Sao_Paulo"
            )

            try:
                response = requests.get(url, timeout=120)
                response.raise_for_status()
                data = response.json()

                daily = data.get("daily", {})
                dates = daily.get("time", [])
                temps_mean = daily.get("temperature_2m_mean", [])
                temps_max = daily.get("temperature_2m_max", [])
                precip = daily.get("precipitation_sum", [])
                codes = daily.get("weathercode", [])

                for i, date in enumerate(dates):
                    # FIXED: Use correct column names (temperature_2m_mean, temperature_2m_max)
                    cursor.execute(
                        """
                        INSERT INTO bronze.api_weather_history 
                        (state_code, weather_date, latitude, longitude,
                         temperature_2m_mean, temperature_2m_max, precipitation_sum, weather_code, dwh_load_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                        (
                            state,
                            date,
                            lat,
                            lon,
                            temps_mean[i] if i < len(temps_mean) else None,
                            temps_max[i] if i < len(temps_max) else None,
                            precip[i] if i < len(precip) else None,
                            codes[i] if i < len(codes) else None,
                        ),
                    )
                    total += 1

                conn.commit()  # Commit after each state
                print(f"  {state}: {len(dates)} days ✓")

            except Exception as e:
                conn.rollback()
                print(f"  {state}: FAILED - {e}")
                continue

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
    dag_id="olist_etl_pipeline",
    default_args=default_args,
    description="Olist Data Warehouse - Full ETL Pipeline",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "etl", "data-warehouse"],
    doc_md="""
    ## Olist ETL Pipeline
    
    **Layers:**
    - Bronze: Raw CSV data load
    - API: Currency, Holidays, Weather
    - Silver: Cleaned & transformed
    - Gold: Star schema (dims + facts)
    
    **Trigger:** Manual or schedule daily
    """,
) as dag:
    # =========================================================================
    # BRONZE LAYER
    # =========================================================================
    with TaskGroup(group_id="bronze_layer", tooltip="Load raw CSV data") as bronze:
        create_bronze = BashOperator(
            task_id="create_tables",
            bash_command="""
                PGPASSWORD=$DWH_PASSWORD psql -h $DWH_HOST -U $DWH_USER -d $DWH_DATABASE \
                -f /scripts/bronze/create_bronze_tables.sql
            """,
        )

        load_bronze = BashOperator(
            task_id="load_data",
            bash_command="""
                PGPASSWORD=$DWH_PASSWORD psql -h $DWH_HOST -U $DWH_USER -d $DWH_DATABASE \
                -f /scripts/bronze/load_bronze_data.sql
            """,
        )

        create_bronze >> load_bronze

    # =========================================================================
    # API FETCH (Parallel)
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

        # These run in parallel - no dependencies between them

    # =========================================================================
    # SILVER LAYER
    # =========================================================================
    with TaskGroup(group_id="silver_layer", tooltip="Clean & transform data") as silver:
        create_silver = BashOperator(
            task_id="create_tables",
            bash_command="""
                PGPASSWORD=$DWH_PASSWORD psql -h $DWH_HOST -U $DWH_USER -d $DWH_DATABASE \
                -f /scripts/silver/create_silver_tables.sql
            """,
        )

        load_silver = BashOperator(
            task_id="load_data",
            bash_command="""
                PGPASSWORD=$DWH_PASSWORD psql -h $DWH_HOST -U $DWH_USER -d $DWH_DATABASE \
                -f /scripts/silver/load_silver_data.sql
            """,
        )

        create_silver >> load_silver

    # =========================================================================
    # GOLD LAYER
    # =========================================================================
    with TaskGroup(group_id="gold_layer", tooltip="Build star schema") as gold:
        create_gold = BashOperator(
            task_id="create_tables",
            bash_command="""
                PGPASSWORD=$DWH_PASSWORD psql -h $DWH_HOST -U $DWH_USER -d $DWH_DATABASE \
                -f /scripts/gold/create_gold_tables.sql
            """,
        )

        load_gold = BashOperator(
            task_id="load_data",
            bash_command="""
                PGPASSWORD=$DWH_PASSWORD psql -h $DWH_HOST -U $DWH_USER -d $DWH_DATABASE \
                -f /scripts/gold/load_gold_data.sql
            """,
        )

        create_gold >> load_gold

    # =========================================================================
    # Pipeline Flow
    # =========================================================================
    # Bronze → API (parallel) → Silver → Gold
    bronze >> api >> silver >> gold

# =============================================================================
# OLIST DATA WAREHOUSE - Airflow Dockerfile
# =============================================================================
# Custom Airflow image with:
#   - PostgreSQL client (for psql commands)
#   - Python packages for API scripts
# =============================================================================

FROM apache/airflow:2.7.3-python3.10

# Switch to root to install system packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
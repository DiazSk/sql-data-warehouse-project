# Olist Analytics Dashboard (marimo)

Interactive analytics dashboard built with **marimo** - the reactive Python notebook.

## Quick Start

### Demo Mode (No Database Required)

```bash
# Activate virtual environment
source .venv/bin/activate

# Run in edit mode (interactive notebook)
marimo edit olist_demo_dashboard.py

# Or run as web app
marimo run olist_demo_dashboard.py --port 8501
```

### Production Mode (Requires PostgreSQL)

```bash
# Ensure Docker is running
docker-compose up -d

# Activate virtual environment
source .venv/bin/activate

# Run dashboard
marimo edit olist_analytics_dashboard.py
```

## Features

- **Reactive UI:** Dropdowns auto-update all charts
- **DuckDB SQL:** Query DataFrames with SQL
- **Star Schema:** Mirrors Gold layer structure
- **6 Visualizations:** Revenue trends, regional analysis, category breakdown
- **Data Explorer:** Interactive table with search/sort
- **Architecture Diagram:** Embedded data warehouse architecture image

## Dashboard Contents

| Section | Description |
|---------|-------------|
| KPI Metrics | Orders, Revenue, Avg Order Value, Review Score, Delivery Days, Late % |
| Monthly Trend | Revenue trend over time (area chart) |
| Revenue by Region | Brazilian regions with color-coded reviews |
| Top Categories | Top 10 product categories by revenue |
| Payment Distribution | Credit card, boleto, debit, voucher breakdown |
| Review Distribution | 1-5 star rating distribution |
| Delivery Performance | Avg delivery days vs late % by region |
| Data Explorer | Filterable data table |

## Dependencies

Installed in `.venv/`:
- marimo
- duckdb
- pandas
- plotly
- numpy
- sqlglot
- psycopg2-binary (production only)

## Files

| File | Description |
|------|-------------|
| `olist_demo_dashboard.py` | Self-contained demo with 25K simulated orders |
| `olist_analytics_dashboard.py` | Production version connecting to PostgreSQL |
| `.venv/` | Python virtual environment (gitignored) |

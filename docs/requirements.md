## Olist E-Commerce Data Warehouse

**Project Name:** Brazilian E-Commerce Analytics Data Warehouse

**Author:** Zaid Shaikh

**Version:** 2.0

**Last Updated:** December 30, 2025

**Status:** 🟡 In Progress

---

## 1. Project Requirements

### 1.1 Objective

### Business Objective

Develop a modern data warehouse using PostgreSQL to consolidate Brazilian e-commerce sales data AND marketing funnel data from multiple sources, enabling **full-funnel analytics** from lead generation through order fulfillment.

### Technical Objective

Build an end-to-end data pipeline following **Medallion Architecture** (Bronze → Silver → Gold) that demonstrates industry best practices in:

- Data Engineering & ETL Development
- Data Modeling (Star Schema)
- Data Quality Management
- Multi-source Integration (CSV + APIs)
- SQL-based Analytics & Reporting

### Analytics Objective

Deliver SQL-based analytics to generate actionable insights into:

- **Marketing Funnel**: Lead sources, conversion rates, SDR/SR performance
- **Customer Behavior**: Purchase patterns, geographic distribution, repeat customers
- **Product Performance**: Category trends, revenue by product line, seller analysis
- **Sales Trends**: Seasonality, holiday impact, weather correlation, revenue forecasting
- **Operational Metrics**: Delivery performance, payment methods, review analysis

### Learning Objective

Build a portfolio-ready project that demonstrates genuine understanding of:

- Medallion Architecture implementation
- Star schema design with surrogate keys
- Multi-source data integration (B2B + B2C)
- Data quality handling and transformation logic
- External API integration for data enrichment
- Professional documentation practices

---

### 1.2 Specifications

### 1.2.1 Data Sources

### Primary Source 1: Olist Brazilian E-Commerce Dataset

| Attribute       | Details                                                                                               |
|-----------------|-------------------------------------------------------------------------------------------------------|
| **Source**      | [Kaggle - Brazilian E-Commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |
| **Format**      | CSV Files (9 files)                                                                                   |
| **Volume**      | ~100,000 orders, 112,000+ order items                                                                 |
| **Time Period** | September 2016 - October 2018                                                                         |
| **Data Owner**  | Olist (Brazilian E-Commerce Platform)                                                                 |
| **License**     | CC BY-NC-SA 4.0                                                                                       |

### Primary Source 2: Olist Marketing Funnel Dataset

| Attribute       | Details                                                                                              |
|-----------------|------------------------------------------------------------------------------------------------------|
| **Source**      | [Kaggle - Marketing Funnel by Olist](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist) |
| **Format**      | CSV Files (2 files)                                                                                  |
| **Volume**      | ~8,000 MQLs, ~841 Closed Deals                                                                       |
| **Time Period** | June 2017 - June 2018                                                                                |
| **Data Owner**  | Olist (Brazilian E-Commerce Platform)                                                                |
| **License**     | CC BY-NC-SA 4.0                                                                                      |
| **Join Key**    | `seller_id` connects to E-Commerce dataset                                                           |

### Source Files Inventory

**E-Commerce Dataset (9 files)**

| File Name                                     | Description                  | Record Count | Key Fields                                            |
|-----------------------------------------------|------------------------------|--------------|-------------------------------------------------------|
| `olist_orders_dataset.csv`                    | Core order information       | ~99,441      | order_id, customer_id, order_status, timestamps       |
| `olist_order_items_dataset.csv`               | Line items within orders     | ~112,650     | order_id, product_id, seller_id, price, freight       |
| `olist_customers_dataset.csv`                 | Customer information         | ~99,441      | customer_id, customer_unique_id, city, state, zip     |
| `olist_products_dataset.csv`                  | Product catalog              | ~32,951      | product_id, category_name, dimensions, weight         |
| `olist_sellers_dataset.csv`                   | Seller information           | ~3,095       | seller_id, city, state, zip                           |
| `olist_order_payments_dataset.csv`            | Payment details              | ~103,886     | order_id, payment_type, installments, value           |
| `olist_order_reviews_dataset.csv`             | Customer reviews             | ~100,000     | order_id, review_score, comment, timestamps           |
| `olist_geolocation_dataset.csv`               | Geographic coordinates       | ~1,000,163   | zip_code, lat, lng, city, state                       |
| `product_category_name_translation.csv`       | Portuguese → English         | ~71          | category_name_portuguese, category_name_english       |

**Marketing Funnel Dataset (2 files)**

| File Name                                     | Description                | Record Count | Key Fields                                                                                            |
|-----------------------------------------------|----------------------------|--------------|-------------------------------------------------------------------------------------------------------|
| `olist_marketing_qualified_leads_dataset.csv` | Marketing Qualified Leads  | ~8,000       | mql_id, first_contact_date, landing_page_id, origin                                                   |
| `olist_closed_deals_dataset.csv`              | Leads converted to sellers | ~841         | mql_id, **seller_id**, sdr_id, sr_id, won_date, business_segment, lead_type, declared_monthly_revenue |

### Secondary Sources: External APIs

| API                  | Purpose                       | Free Tier       | Authentication  | Joins With                    |
|----------------------|-------------------------------|-----------------|-----------------|-------------------------------|
| **ExchangeRate-API** | BRL → USD currency conversion | 1,500 req/month | No key required | Order dates                   |
| **Nager.Date API**   | Brazilian public holidays     | Unlimited       | No key required | Order dates                   |
| **Open-Meteo API**   | Historical weather data       | Unlimited       | No key required | Geolocation (lat/lng) + dates |

### Source System Interview Summary

| Category                 | Details                                           |
|--------------------------|---------------------------------------------------|
| **Data Owner**           | Olist (Public Dataset for Educational Use)        |
| **Business Process**     | Full Funnel: Marketing → Sales → Order Management |
| **System Documentation** | Kaggle dataset descriptions + ERD provided        |
| **Data Model**           | Normalized transactional data across 11 tables    |
| **Integration Method**   | CSV File Extract (one-time historical load)       |
| **API Integration**      | REST API calls for enrichment data                |
| **Load Strategy**        | Full Load (Truncate & Insert) - No incremental    |
| **Data Volume**          | ~600 MB total across all files                    |
| **Authentication**       | None required for CSV; None for free API tiers    |

### Data Lineage: Marketing → E-Commerce Connection

```
MARKETING FUNNEL                              E-COMMERCE
────────────────                              ──────────

┌───────────────────┐
│ Marketing Lead    │
│ (olist_mql)       │
│ - mql_id (PK)     │
│ - first_contact   │
│ - landing_page    │
│ - origin          │
└────────┬──────────┘
         │ mql_id
         ▼
┌───────────────────┐
│ Closed Deal       │
│ (olist_closed)    │
│ - mql_id (FK)     │
│ - seller_id (FK) ─┼─────────────────────►┌───────────────────┐
│ - sdr_id          │                      │ Sellers           │
│ - sr_id           │                      │ (olist_sellers)   │
│ - won_date        │                      │ - seller_id (PK)  │
│ - business_segment│                      │ - seller_city     │
│ - lead_type       │                      │ - seller_state    │
│ - declared_revenue│                      └────────┬──────────┘
└───────────────────┘                               │
                                                    │ seller_id
                                                    ▼
                                          ┌───────────────────┐
                                          │ Order Items       │
                                          │ - order_id        │
                                          │ - seller_id (FK)  │
                                          │ - product_id      │
                                          │ - price           │
                                          └────────┬──────────┘
                                                   │
                                                   ▼
                                             100K+ Orders
                                             99K+ Customers

```

---

### 1.2.2 Data Quality

### Known Data Quality Issues to Address

| Issue Category            | Specific Problem                                           | Affected Table(s)                      | Resolution Strategy           |
|---------------------------|------------------------------------------------------------|----------------------------------------|-------------------------------|
| **Missing Values**        | NULL product category names                                | `olist_products_dataset`               | Replace with 'Unknown'        |
| **Missing Values**        | NULL business_segment in closed deals                      | `olist_closed_deals_dataset`           | Replace with 'Unknown'        |
| **Language**              | Category names in Portuguese                               | `olist_products_dataset`               | Join with translation table   |
| **Date Formats**          | Timestamps as strings                                      | All tables with dates                  | Cast to proper DATE/TIMESTAMP |
| **Duplicate Keys**        | Same customer with different IDs                           | `olist_customers_dataset`              | Use `customer_unique_id`      |
| **Calculated Fields**     | Missing total order amounts                                | `olist_order_items_dataset`            | Calculate: price + freight    |
| **Invalid Records**       | Orders with status 'canceled'                              | `olist_orders_dataset`                 | Flag for separate analysis    |
| **Outliers**              | Extreme delivery times                                     | `olist_orders_dataset`                 | Handle orders > 60 days       |
| **Referential Integrity** | Orphan order_items                                         | `olist_order_items_dataset`            | Validate all order_ids exist  |
| **Partial Match**         | Not all closed deals → sellers exist                       | `olist_closed_deals` → `olist_sellers` | ~45% match rate (379/841)     |
| **Time Gaps**             | MQL data (Jun 2017-Jun 2018) vs Orders (Sep 2016-Oct 2018) | Cross-dataset                          | Filter to overlapping period  |

### Data Quality Rules

| Rule ID | Rule Description                          | Validation Query                                                      | Severity |
|---------|-------------------------------------------|-----------------------------------------------------------------------|----------|
| DQ-001  | Order date must be before shipping date   | `order_purchase_timestamp < order_delivered_timestamp`                | Critical |
| DQ-002  | Price and freight must be positive        | `price > 0 AND freight_value >= 0`                                    | Critical |
| DQ-003  | Review score between 1-5                  | `review_score BETWEEN 1 AND 5`                                        | High     |
| DQ-004  | Valid payment types only                  | `payment_type IN ('credit_card','boleto','voucher','debit_card')`     | Medium   |
| DQ-005  | Order status is valid                     | `order_status IN ('delivered','shipped','canceled','processing',...)` | High     |
| DQ-006  | Customer state is valid Brazilian state   | `customer_state IN ('SP','RJ','MG',...)`                              | Medium   |
| DQ-007  | Won date must be after first contact date | `won_date > first_contact_date`                                       | Critical |
| DQ-008  | Valid lead origins                        | `origin IN ('organic_search','paid_search','social',...)`             | Medium   |

### Data Cleaning Transformations (Silver Layer)

| Transformation         | Source Column                        | Target Column               | Logic                           |
|------------------------|--------------------------------------|-----------------------------|---------------------------------|
| Date Casting           | `order_purchase_timestamp` (VARCHAR) | `order_date` (DATE)         | `CAST(... AS DATE)`             |
| Date Casting           | `first_contact_date` (VARCHAR)       | `first_contact_date` (DATE) | `CAST(... AS DATE)`             |
| Date Casting           | `won_date` (VARCHAR)                 | `won_date` (DATE)           | `CAST(... AS DATE)`             |
| Category Translation   | `product_category_name` (PT)         | `category_english` (EN)     | JOIN with translation           |
| NULL Handling          | `product_category_name`              | `product_category_name`     | `COALESCE(category, 'Unknown')` |
| NULL Handling          | `business_segment`                   | `business_segment`          | `COALESCE(segment, 'Unknown')`  |
| Price Calculation      | `price`, `freight_value`             | `total_item_value`          | `price + freight_value`         |
| Days Calculation       | `won_date`, `first_contact_date`     | `days_to_close`             | `won_date - first_contact_date` |
| Region Derivation      | `customer_state`                     | `region`                    | Map state codes to regions      |
| Customer Deduplication | `customer_id`                        | `customer_unique_id`        | Use unique_id for dimension     |

---

### 1.2.3 Integration

### Data Integration Strategy

| Integration Type           | Description                        | Implementation                                                     |
|----------------------------|------------------------------------|--------------------------------------------------------------------|
| **CSV + CSV**              | Combine all Olist E-Commerce files | JOIN on common keys (order_id, customer_id, product_id, seller_id) |
| **CSV + CSV**              | Marketing Funnel files             | JOIN MQL → Closed Deals on mql_id                                  |
| **Marketing + E-Commerce** | Connect funnel to orders           | JOIN Closed Deals → Sellers on seller_id                           |
| **CSV + API**              | Enrich with currency rates         | JOIN on date field                                                 |
| **CSV + API**              | Enrich with holiday flags          | JOIN on date field                                                 |
| **CSV + API**              | Enrich with weather data           | JOIN on location (lat/lng) + date                                  |

### Entity Relationships (Source Data)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           COMPLETE DATA MODEL                                    │
└─────────────────────────────────────────────────────────────────────────────────┘

MARKETING FUNNEL                           E-COMMERCE OPERATIONS
────────────────                           ─────────────────────

┌───────────────┐                          ┌─────────────────┐
│     MQL       │                          │    customers    │
│───────────────│                          │─────────────────│
│ mql_id     PK │                          │ customer_id  PK │◄────┐
│ first_contact │                          │ customer_unique │     │
│ landing_page  │                          │ customer_city   │     │
│ origin        │                          │ customer_state  │     │
└───────┬───────┘                          └─────────────────┘     │
        │                                                          │
        │ mql_id                                                   │
        ▼                                                          │
┌───────────────┐         seller_id        ┌─────────────────┐     │
│ closed_deals  │─────────────────────────►│    sellers      │     │
│───────────────│                          │─────────────────│     │
│ mql_id     FK │                          │ seller_id    PK │◄┐   │
│ seller_id  FK │                          │ seller_city     │ │   │
│ sdr_id        │                          │ seller_state    │ │   │
│ sr_id         │                          └─────────────────┘ │   │
│ won_date      │                                              │   │
│ business_seg  │                                              │   │
│ lead_type     │                                              │   │
│ declared_rev  │                          ┌─────────────────┐ │   │
└───────────────┘                          │     orders      │ │   │
                                           │─────────────────│ │   │
                                           │ order_id     PK │ │   │
                                           │ customer_id  FK─┼─┼───┘
                                           │ order_status    │ │
                                           │ order_purchase  │ │
                                           │ order_delivered │ │
                                           └────────┬────────┘ │
                                                    │          │
                               ┌────────────────────┼──────────┤
                               │                    │          │
                               ▼                    ▼          │
                      ┌────────────┐       ┌────────────┐      │
                      │order_items │       │  payments  │      │
                      │────────────│       │────────────│      │
                      │ order_id FK│       │ order_id FK│      │
                      │ product_id │       │ payment_typ│      │
                      │ seller_id ─┼───────┼────────────┼──────┘
                      │ price      │       │ value      │
                      │ freight    │       │ installmnt │
                      └─────┬──────┘       └────────────┘
                            │
                            ▼
                      ┌────────────┐     ┌────────────────────┐
                      │  products  │     │ category_translation│
                      │────────────│     │────────────────────│
                      │ product_id │────►│ category_name_pt   │
                      │ category_pt│     │ category_name_en   │
                      │ weight_g   │     └────────────────────┘
                      └────────────┘

                      ┌────────────┐     ┌────────────────────┐
                      │  reviews   │     │    geolocation     │
                      │────────────│     │────────────────────│
                      │ order_id FK│     │ zip_code_prefix    │
                      │ review_scor│     │ latitude           │
                      │ comment    │     │ longitude          │
                      └────────────┘     │ city, state        │
                                         └────────────────────┘

```

### Target Star Schema

```
                              ┌─────────────────┐
                              │    dim_date     │
                              │─────────────────│
                              │ date_key     PK │
                              │ full_date       │
                              │ day_of_week     │
                              │ is_weekend      │
                              │ is_holiday      │ ◄── Nager.Date API
                              │ holiday_name    │
                              └────────┬────────┘
                                       │
       ┌───────────────────────────────┼───────────────────────────────┐
       │                               │                               │
       ▼                               ▼                               ▼
┌─────────────────┐           ┌─────────────────┐           ┌─────────────────┐
│  dim_customer   │           │   fact_sales    │           │   dim_seller    │
│─────────────────│           │─────────────────│           │─────────────────│
│ customer_key PK │◄──────────│ customer_key FK │           │ seller_key   PK │◄──┐
│ customer_id     │           │ product_key  FK │──────────►│ seller_id       │   │
│ customer_unique │           │ seller_key   FK │           │ seller_city     │   │
│ customer_city   │           │ date_key     FK │           │ seller_state    │   │
│ customer_state  │           │ location_key FK │           │ seller_region   │   │
│ customer_region │           │─────────────────│           └─────────────────┘   │
└─────────────────┘           │ order_id        │                                 │
                              │ price_brl       │                                 │
┌─────────────────┐           │ price_usd       │ ◄── ExchangeRate API            │
│  dim_product    │           │ freight_value   │                                 │
│─────────────────│           │ total_value     │                                 │
│ product_key  PK │◄──────────│ payment_type    │                                 │
│ product_id      │           │ review_score    │                                 │
│ category_pt     │           │ days_to_deliver │                                 │
│ category_en     │           └─────────────────┘                                 │
│ weight_g        │                                                               │
└─────────────────┘                                                               │
                                                                                  │
┌─────────────────┐           ┌─────────────────────────┐                         │
│  dim_location   │           │  fact_marketing_funnel  │                         │
│─────────────────│           │─────────────────────────│                         │
│ location_key PK │◄──────────│ mql_key           FK    │                         │
│ zip_code_prefix │           │ seller_key        FK    │─────────────────────────┘
│ city            │           │ first_contact_key FK    │
│ state           │           │ won_date_key      FK    │
│ region          │           │─────────────────────────│
│ latitude        │           │ mql_id                  │
│ longitude       │           │ sdr_id                  │
│ avg_temp        │ ◄── Open-Meteo API                  │
│ precipitation   │           │ sr_id                   │
└─────────────────┘           │ origin                  │
                              │ landing_page_id         │
┌─────────────────┐           │ business_segment        │
│    dim_mql      │           │ lead_type               │
│─────────────────│           │ declared_monthly_revenue│
│ mql_key      PK │◄──────────│ days_to_close           │
│ mql_id          │           │ is_converted (1/0)      │
│ landing_page_id │           └─────────────────────────┘
│ origin          │
│ origin_category │
└─────────────────┘

```

---

### 1.2.4 Scope

### In Scope

| Area                  | Details                                         |
|-----------------------|-------------------------------------------------|
| **E-Commerce Orders** | All orders from Sept 2016 - Oct 2018            |
| **Marketing Funnel**  | All MQLs and Closed Deals (Jun 2017 - Jun 2018) |
| **Customers**         | All customers who placed orders                 |
| **Products**          | All products that appear in orders              |
| **Sellers**           | All sellers with at least one sale              |
| **Geography**         | Brazil only (all 27 states)                     |
| **Time**              | Sept 2016 - Oct 2018 (full date spine)          |
| **Currency**          | BRL (Brazilian Real) + USD conversion           |
| **Weather**           | Historical weather by location                  |
| **Holidays**          | Brazilian public holidays                       |

### Out of Scope

| Area               | Reason                    |
|--------------------|---------------------------|
| Real-time data     | Static historical dataset |
| Incremental loads  | One-time full load only   |
| Non-Brazilian data | Dataset is Brazil-only    |
| Customer PII       | Data is anonymized        |
| Competitor data    | Not available             |

### Deliverables Checklist

| #  | Deliverable                                    | Status        |
|----|------------------------------------------------|---------------|
| 1  | Bronze Layer - Raw data tables (14 tables)     | ⬜ Not Started |
| 2  | Silver Layer - Cleaned data tables (14 tables) | ⬜ Not Started |
| 3  | Gold Layer - Star schema views (8 views)       | ⬜ Not Started |
| 4  | Data quality test scripts                      | ⬜ Not Started |
| 5  | Analytics SQL queries                          | ⬜ Not Started |
| 6  | Data architecture diagram                      | ⬜ Not Started |
| 7  | Data flow diagram                              | ⬜ Not Started |
| 8  | Data model (ERD)                               | ⬜ Not Started |
| 9  | Data catalog                                   | ⬜ Not Started |
| 10 | Naming conventions document                    | ⬜ Not Started |
| 11 | README documentation                           | ⬜ Not Started |
| 12 | GitHub repository                              | ⬜ Not Started |

---

### 1.2.5 Layer Specifications

### Bronze Layer (14 Tables)

| #  | Table Name                        | Source | Records | Load Method       |
|----|-----------------------------------|--------|---------|-------------------|
| 1  | bronze.olist_orders               | CSV    | ~99K    | Truncate & Insert |
| 2  | bronze.olist_order_items          | CSV    | ~113K   | Truncate & Insert |
| 3  | bronze.olist_order_payments       | CSV    | ~104K   | Truncate & Insert |
| 4  | bronze.olist_order_reviews        | CSV    | ~100K   | Truncate & Insert |
| 5  | bronze.olist_customers            | CSV    | ~99K    | Truncate & Insert |
| 6  | bronze.olist_geolocation          | CSV    | ~1M     | Truncate & Insert |
| 7  | bronze.olist_products             | CSV    | ~33K    | Truncate & Insert |
| 8  | bronze.olist_category_translation | CSV    | ~71     | Truncate & Insert |
| 9  | bronze.olist_sellers              | CSV    | ~3K     | Truncate & Insert |
| 10 | bronze.olist_mql                  | CSV    | ~8K     | Truncate & Insert |
| 11 | bronze.olist_closed_deals         | CSV    | ~841    | Truncate & Insert |
| 12 | bronze.api_currency_rates         | API    | ~730    | Truncate & Insert |
| 13 | bronze.api_brazil_holidays        | API    | ~50     | Truncate & Insert |
| 14 | bronze.api_weather_history        | API    | TBD     | Truncate & Insert |

### Silver Layer (14 Tables)

| #  | Table Name                        | Key Transformations                                  |
|----|-----------------------------------|------------------------------------------------------|
| 1  | silver.olist_orders               | Date casting, status standardization                 |
| 2  | silver.olist_order_items          | Calculate total_value = price + freight              |
| 3  | silver.olist_order_payments       | Payment type standardization                         |
| 4  | silver.olist_order_reviews        | NULL handling for comments                           |
| 5  | silver.olist_customers            | Region derivation from state                         |
| 6  | silver.olist_geolocation          | Deduplicate by zip_code                              |
| 7  | silver.olist_products             | Join with translation, NULL → 'Unknown'              |
| 8  | silver.olist_category_translation | Clean names                                          |
| 9  | silver.olist_sellers              | Region derivation from state                         |
| 10 | silver.olist_mql                  | Date casting, origin standardization                 |
| 11 | silver.olist_closed_deals         | Date casting, calculate days_to_close, NULL handling |
| 12 | silver.api_currency_rates         | Date casting                                         |
| 13 | silver.api_brazil_holidays        | Date casting, holiday type flag                      |
| 14 | silver.api_weather_history        | Date casting, aggregations                           |

### Gold Layer (8 Views)

| # | View Name                  | Type      | Description                       | Grain                       |
|---|----------------------------|-----------|-----------------------------------|-----------------------------|
| 1 | gold.dim_date              | Dimension | Date spine with holiday flags     | One row per day             |
| 2 | gold.dim_customer          | Dimension | Customer attributes, deduplicated | One row per unique customer |
| 3 | gold.dim_product           | Dimension | Product with English categories   | One row per product         |
| 4 | gold.dim_seller            | Dimension | Seller attributes + region        | One row per seller          |
| 5 | gold.dim_location          | Dimension | Geography with weather            | One row per zip_prefix      |
| 6 | gold.dim_mql               | Dimension | Marketing lead attributes         | One row per MQL             |
| 7 | gold.fact_sales            | Fact      | E-commerce transactions           | One row per order item      |
| 8 | gold.fact_marketing_funnel | Fact      | Lead → Deal conversion            | One row per MQL             |

---

### 1.2.6 Documentation

### Required Documentation

| Document               | Purpose                              | Location                      | Format         |
|------------------------|--------------------------------------|-------------------------------|----------------|
| **README.md**          | Project overview, setup instructions | `/README.md`                  | Markdown       |
| **Data Catalog**       | Field descriptions for Gold layer    | `/docs/data_catalog.md`       | Markdown       |
| **Naming Conventions** | Standards for objects/columns        | `/docs/naming_conventions.md` | Markdown       |
| **Data Architecture**  | Medallion layers diagram             | `/docs/diagrams/`             | PlantUML + PNG |
| **Data Flow**          | Source to target mapping             | `/docs/diagrams/`             | PlantUML + PNG |
| **Data Model (ERD)**   | Star schema diagram                  | `/docs/diagrams/`             | PlantUML + PNG |
| **Requirements**       | This document                        | `/docs/requirements.md`       | Markdown       |

### Repository Structure

```
olist-data-warehouse/
│
├── datasets/                           # Raw CSV files from Kaggle
│   ├── ecommerce/                      # E-Commerce dataset (9 files)
│   │   ├── olist_orders_dataset.csv
│   │   ├── olist_order_items_dataset.csv
│   │   ├── olist_customers_dataset.csv
│   │   ├── olist_products_dataset.csv
│   │   ├── olist_sellers_dataset.csv
│   │   ├── olist_order_payments_dataset.csv
│   │   ├── olist_order_reviews_dataset.csv
│   │   ├── olist_geolocation_dataset.csv
│   │   └── product_category_name_translation.csv
│   │
│   └── marketing_funnel/               # Marketing Funnel dataset (2 files)
│       ├── olist_marketing_qualified_leads_dataset.csv
│       └── olist_closed_deals_dataset.csv
│
├── docs/                               # Project documentation
│   ├── requirements.md                 # This document
│   ├── data_catalog.md                 # Gold layer field descriptions
│   ├── naming_conventions.md           # Naming standards
│   └── diagrams/                       # Architecture diagrams
│       ├── architecture.puml           # PlantUML source
│       ├── architecture.png            # Exported image
│       ├── data_flow.puml
│       ├── data_flow.png
│       ├── star_schema.puml
│       └── star_schema.png
│
├── scripts/                            # SQL scripts
│   ├── init/                           # Database setup
│   │   └── 01_create_database.sql
│   │
│   ├── bronze/                         # Raw data loading
│   │   ├── 01_create_bronze_tables.sql
│   │   └── 02_load_bronze_data.sql
│   │
│   ├── silver/                         # Data cleansing
│   │   ├── 01_create_silver_tables.sql
│   │   └── 02_transform_silver_data.sql
│   │
│   ├── gold/                           # Star schema
│   │   ├── 01_create_dim_date.sql
│   │   ├── 02_create_dim_customer.sql
│   │   ├── 03_create_dim_product.sql
│   │   ├── 04_create_dim_seller.sql
│   │   ├── 05_create_dim_location.sql
│   │   ├── 06_create_dim_mql.sql
│   │   ├── 07_create_fact_sales.sql
│   │   └── 08_create_fact_marketing_funnel.sql
│   │
│   └── analytics/                      # Business queries
│       ├── customer_analysis.sql
│       ├── product_analysis.sql
│       ├── sales_analysis.sql
│       └── marketing_funnel_analysis.sql
│
├── tests/                              # Data quality tests
│   ├── bronze_tests.sql
│   ├── silver_tests.sql
│   └── gold_tests.sql
│
├── api/                                # API integration scripts
│   ├── fetch_currency_rates.py
│   ├── fetch_holidays.py
│   └── fetch_weather.py
│
├── .gitignore
├── README.md
└── LICENSE

```

---

## 2. Technology Stack

| Component           | Technology            | Justification                                    |
|---------------------|-----------------------|--------------------------------------------------|
| **Database**        | PostgreSQL 16         | Industry standard, Redshift-compatible SQL, free |
| **SQL Client**      | DBeaver / pgAdmin     | Free, cross-platform GUI                         |
| **API Integration** | Python 3.x + requests | Simple REST API calls                            |
| **Diagrams**        | PlantUML + Draw.io    | Free, version-controllable                       |
| **Version Control** | Git + GitHub          | Industry standard                                |
| **Documentation**   | Markdown + Notion     | Easy to maintain and share                       |

---

## 3. Timeline & Milestones

| Week       | Phase          | Deliverables                                           |
|------------|----------------|--------------------------------------------------------|
| **Week 1** | Setup & Bronze | Database setup, All 14 Bronze tables created & loaded  |
| **Week 2** | Silver Layer   | Data profiling, cleaning scripts, All 14 Silver tables |
| **Week 3** | Gold Layer     | 6 Dimension views, 2 Fact views, Star schema complete  |
| **Week 4** | Testing & Docs | Quality tests, Analytics queries, Full documentation   |

---

## 4. Success Criteria

| Criteria              | Measure                                                       |
|-----------------------|---------------------------------------------------------------|
| **Data Completeness** | 100% of source records loaded to Bronze (all 11 CSV + 3 APIs) |
| **Data Quality**      | All DQ rules pass in Silver/Gold layers                       |
| **Model Accuracy**    | Fact table totals match source totals                         |
| **Integration**       | Marketing funnel connected to E-commerce via seller_id        |
| **Documentation**     | All required docs completed and reviewed                      |
| **Version Control**   | All code committed with meaningful messages                   |
| **Portfolio Ready**   | README explains full-funnel project to recruiters             |

---

## 5. Risks & Mitigations

| Risk                                          | Impact | Mitigation                                               |
|-----------------------------------------------|--------|----------------------------------------------------------|
| Data quality issues not identified upfront    | High   | Thorough data profiling before Silver transformations    |
| API rate limits exceeded                      | Medium | Cache API responses, minimize calls                      |
| Scope creep (adding features)                 | High   | Stick to defined scope, track "nice-to-haves" separately |
| PostgreSQL syntax differs from SQL Server     | Low    | Reference PostgreSQL documentation                       |
| Marketing → E-Commerce join incomplete (~45%) | Medium | Document known limitation, analyze matched subset        |
| Time period mismatch between datasets         | Medium | Document overlapping period (Jun 2017 - Jun 2018)        |

---

## 6. Business Questions Enabled

### E-Commerce Analytics

- What are the top-selling product categories?
- Which regions generate the most revenue?
- How does delivery time affect review scores?
- What is the seasonal sales pattern?
- Do holidays impact order volume?
- Does weather affect delivery times?

### Marketing Funnel Analytics

- Which marketing channels (origin) bring the highest-quality leads?
- What is the average days-to-close by business segment?
- Which SDRs/SRs have the best conversion rates?
- Do leads with higher declared_monthly_revenue actually perform better?
- What is the conversion rate from MQL to closed deal?
- Which landing pages generate the most closed deals?

### Full-Funnel Analytics (Connected)

- Which marketing channels produce the highest-revenue sellers?
- What is the time from first contact to first sale?
- How does declared revenue compare to actual revenue?
- Which business segments have the best customer reviews?

---

## 7. References

- [Kaggle - Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [Kaggle - Marketing Funnel Dataset](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist)
- [Medallion Architecture - Databricks](https://www.databricks.com/glossary/medallion-architecture)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

---

**Document Control**

| Version | Date       | Author      | Changes                                                               |
|---------|------------|-------------|-----------------------------------------------------------------------|
| 1.0     | 2025-12-30 | Zaid Shaikh | Initial document creation                                             |
| 2.0     | 2025-12-30 | Zaid Shaikh | Added Marketing Funnel dataset, updated counts to 14 tables / 8 views |
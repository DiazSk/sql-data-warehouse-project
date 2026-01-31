# Data Catalog - Gold Layer

## Overview
The Gold Layer contains business-ready data in a star schema for the Olist Brazilian E-Commerce Data Warehouse.

**Data Period:** September 2016 - October 2018  
**Total Orders:** 99,441 | **Total Revenue:** R$ 15.8M (~$4.9M USD)

---

## Dimension Tables (5)

### 1. gold.dim_date
**Purpose:** Calendar dimension with holidays and currency rates  
**Grain:** One row per date | **Rows:** 1,096

| Column Name       | Data Type     | Description                                              |
|-------------------|---------------|----------------------------------------------------------|
| date_key          | INTEGER       | PK - YYYYMMDD format (e.g., 20170115)                    |
| full_date         | DATE          | Calendar date                                            |
| year              | INTEGER       | Year (2016-2018)                                         |
| quarter           | INTEGER       | Quarter (1-4)                                            |
| quarter_name      | VARCHAR(2)    | 'Q1', 'Q2', 'Q3', 'Q4'                                   |
| month             | INTEGER       | Month (1-12)                                             |
| month_name        | VARCHAR(10)   | Full month name                                          |
| week_of_year      | INTEGER       | ISO week (1-53)                                          |
| day_of_month      | INTEGER       | Day (1-31)                                               |
| day_of_week       | INTEGER       | Day of week (0=Sunday)                                   |
| day_name          | VARCHAR(10)   | Full day name                                            |
| is_weekend        | BOOLEAN       | TRUE for Saturday/Sunday                                 |
| is_holiday        | BOOLEAN       | TRUE for Brazilian holidays *(from Holidays API)*        |
| holiday_name      | VARCHAR(100)  | Holiday name if applicable *(from Holidays API)*         |
| usd_exchange_rate | DECIMAL(10,4) | BRL to USD rate (1 BRL = X USD) *(from Currency API)*    |

---

### 2. gold.dim_geography
**Purpose:** Geographic locations with coordinates  
**Grain:** One row per zip code prefix | **Rows:** 19,015

| Column Name     | Data Type    | Description                                    |
|-----------------|--------------|------------------------------------------------|
| geography_key   | SERIAL       | PK - Surrogate key                             |
| zip_code_prefix | VARCHAR(5)   | UK - Brazilian CEP (5 digits)                  |
| city            | VARCHAR(100) | City name                                      |
| state           | VARCHAR(2)   | State abbreviation (SP, RJ, MG, etc.)          |
| state_name      | VARCHAR(50)  | Full state name                                |
| region          | VARCHAR(20)  | Region (Southeast, Northeast, South, etc.)     |
| latitude        | DECIMAL(9,6) | Latitude coordinate                            |
| longitude       | DECIMAL(9,6) | Longitude coordinate                           |

---

### 3. gold.dim_customer
**Purpose:** Customer information with geography link  
**Grain:** One row per customer_id | **Rows:** 99,441

| Column Name        | Data Type    | Description                              |
|--------------------|--------------|------------------------------------------|
| customer_key       | SERIAL       | PK - Surrogate key                       |
| customer_id        | VARCHAR(32)  | UK - Order-level customer identifier     |
| customer_unique_id | VARCHAR(32)  | Actual customer ID (for repeat analysis) |
| customer_zip_code  | VARCHAR(5)   | Zip code prefix                          |
| customer_city      | VARCHAR(100) | City name                                |
| customer_state     | VARCHAR(2)   | State abbreviation                       |
| customer_region    | VARCHAR(20)  | Region name                              |
| geography_key      | INTEGER      | FK → dim_geography                       |

---

### 4. gold.dim_seller
**Purpose:** Seller information with marketing funnel data  
**Grain:** One row per seller_id | **Rows:** 3,095

| Column Name       | Data Type    | Description                                |
|-------------------|--------------|--------------------------------------------|
| seller_key        | SERIAL       | PK - Surrogate key                         |
| seller_id         | VARCHAR(32)  | UK - Seller identifier                     |
| seller_zip_code   | VARCHAR(5)   | Zip code prefix                            |
| seller_city       | VARCHAR(100) | City name                                  |
| seller_state      | VARCHAR(2)   | State abbreviation                         |
| seller_region     | VARCHAR(20)  | Region name                                |
| geography_key     | INTEGER      | FK → dim_geography                         |
| is_from_marketing | BOOLEAN      | TRUE if acquired via MQL funnel            |
| lead_origin       | VARCHAR(50)  | Marketing channel (organic, paid, social)  |
| lead_won_date     | DATE         | Deal closure date                          |

---

### 5. gold.dim_product
**Purpose:** Product catalog with categories  
**Grain:** One row per product_id | **Rows:** 32,951

| Column Name      | Data Type     | Description                              |
|------------------|---------------|------------------------------------------|
| product_key      | SERIAL        | PK - Surrogate key                       |
| product_id       | VARCHAR(32)   | UK - Product identifier                  |
| category_name_pt | VARCHAR(100)  | Category in Portuguese                   |
| category_name_en | VARCHAR(100)  | Category in English                      |
| weight_g         | DECIMAL(10,2) | Weight in grams                          |
| volume_cm3       | DECIMAL(12,2) | Volume in cubic centimeters              |
| weight_category  | VARCHAR(10)   | Light / Medium / Heavy                   |
| size_category    | VARCHAR(10)   | Small / Medium / Large                   |

---

## Fact Tables (2)

### 6. gold.fact_orders
**Purpose:** Order-level transactions with revenue, delivery, and satisfaction metrics  
**Grain:** One row per order | **Rows:** 99,441

| Column Name           | Data Type     | Description                                         |
|-----------------------|---------------|-----------------------------------------------------|
| order_key             | SERIAL        | PK - Surrogate key                                  |
| order_id              | VARCHAR(32)   | UK - Order identifier                               |
| customer_key          | INTEGER       | FK → dim_customer                                   |
| order_date_key        | INTEGER       | FK → dim_date                                       |
| order_status          | VARCHAR(20)   | delivered, shipped, canceled, etc.                  |
| total_items           | INTEGER       | Number of line items                                |
| total_product_value   | DECIMAL(12,2) | Product total (BRL)                                 |
| total_freight_value   | DECIMAL(12,2) | Freight total (BRL)                                 |
| total_order_value     | DECIMAL(12,2) | Order total (BRL)                                   |
| total_order_value_usd | DECIMAL(12,2) | Order total (USD) *(from Currency API)*             |
| payment_type          | VARCHAR(20)   | credit_card, boleto, voucher, debit_card            |
| payment_installments  | INTEGER       | Number of installments (1-24)                       |
| delivery_days         | INTEGER       | Actual delivery days                                |
| is_late               | BOOLEAN       | TRUE if delivered after estimate                    |
| review_score          | INTEGER       | Customer rating (1-5 stars)                         |
| weather_category      | VARCHAR(20)   | clear, cloudy, drizzle, rain *(from Weather API)*   |
| temperature_max       | DECIMAL(5,2)  | Max temperature on order date *(from Weather API)*  |
| is_rainy              | BOOLEAN       | TRUE if rainy day *(from Weather API)*              |

---

### 7. gold.fact_order_items
**Purpose:** Line item-level transactions for product/seller analysis  
**Grain:** One row per line item | **Rows:** 112,650

| Column Name    | Data Type     | Description                |
|----------------|---------------|----------------------------|
| item_key       | SERIAL        | PK - Surrogate key         |
| order_id       | VARCHAR(32)   | Order identifier           |
| order_item_id  | INTEGER       | Item sequence (1, 2, 3...) |
| order_key      | INTEGER       | FK → fact_orders           |
| customer_key   | INTEGER       | FK → dim_customer          |
| seller_key     | INTEGER       | FK → dim_seller            |
| product_key    | INTEGER       | FK → dim_product           |
| order_date_key | INTEGER       | FK → dim_date              |
| price          | DECIMAL(10,2) | Item price (BRL)           |
| freight_value  | DECIMAL(10,2) | Item freight (BRL)         |
| item_total     | DECIMAL(10,2) | price + freight (BRL)      |

---

## Bridge Table (1)

### 8. gold.bridge_marketing_funnel
**Purpose:** Marketing funnel tracking (MQL → Seller → Revenue)  
**Grain:** One row per MQL | **Rows:** ~8,000

| Column Name              | Data Type     | Description                          |
|--------------------------|---------------|--------------------------------------|
| funnel_key               | SERIAL        | PK - Surrogate key                   |
| mql_id                   | VARCHAR(32)   | UK - Lead identifier                 |
| first_contact_date       | DATE          | First contact date                   |
| origin                   | VARCHAR(50)   | Lead source channel                  |
| is_converted             | BOOLEAN       | TRUE if converted to seller          |
| won_date                 | DATE          | Deal closure date                    |
| days_to_conversion       | INTEGER       | Days from contact to close           |
| business_segment         | VARCHAR(50)   | Business type                        |
| lead_type                | VARCHAR(20)   | Lead classification                  |
| declared_monthly_revenue | DECIMAL(12,2) | Declared revenue (BRL)               |
| seller_key               | INTEGER       | FK → dim_seller                      |
| seller_id                | VARCHAR(32)   | Seller identifier                    |
| total_orders             | INTEGER       | Orders on platform                   |
| total_revenue            | DECIMAL(14,2) | Revenue on platform (BRL)            |
| first_order_date         | DATE          | First order date                     |

---

## API Integration Summary

| API          | Data Added To         | Columns                                     |
|--------------|-----------------------|---------------------------------------------|
| Holidays API | dim_date              | is_holiday, holiday_name                    |
| Currency API | dim_date, fact_orders | usd_exchange_rate, total_order_value_usd    |
| Weather API  | fact_orders           | weather_category, temperature_max, is_rainy |

---

## Key Metrics

| Metric              | Value         |
|---------------------|---------------|
| Total Orders        | 99,441        |
| Total Revenue (BRL) | R$ 15,843,553 |
| Total Revenue (USD) | $4,856,789    |
| Avg Order Value     | R$ 159.35     |
| Late Delivery Rate  | 8.11%         |
| Avg Review Score    | 4.09 ⭐        |
| MQL Conversion      | 10.53%        |
| Repeat Customer     | 3.12%         |
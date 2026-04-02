/*
================================================================================
SNOWFLAKE BRONZE LAYER: Create Tables & Load Data from Stage
================================================================================

PURPOSE:
  Migrated from PostgreSQL → Snowflake.
  Creates ALL 14 Bronze layer tables and loads 11 CSV files from internal stage.
  API tables (currency, holidays, weather) are created empty — loaded separately.

  This is a COMPLETE, idempotent script — safe to re-run from scratch.

PREREQUISITES:
  1. Database OLIST_DWH created
  2. Schemas BRONZE, SILVER, GOLD created
  3. Warehouse OLIST_WH created
  4. Stage OLIST_STAGE created with FILE_FORMAT CSV_FORMAT
  5. All 11 CSV files uploaded to OLIST_STAGE

SNOWFLAKE vs POSTGRESQL DIFFERENCES:
  - COPY INTO (Snowflake) instead of COPY FROM (PostgreSQL)
  - No \echo — using inline comments for progress tracking
  - No SET search_path — using fully qualified names
  - TEXT type works but VARCHAR is preferred in Snowflake
  - COMMENT ON syntax is identical
  - DEFAULT CURRENT_TIMESTAMP() uses parentheses in Snowflake
  - ALTER TABLE ADD COLUMN does NOT support function-based defaults
    (must use DEFAULT CURRENT_TIMESTAMP() only in CREATE TABLE)

================================================================================
*/

USE DATABASE OLIST_DWH;
USE SCHEMA BRONZE;
USE WAREHOUSE OLIST_WH;

-- ============================================================================
-- SECTION 1: E-COMMERCE DATASET TABLES (9 tables)
-- ============================================================================

-- Table 1: Orders (~99,441 rows) — Core table, everything joins to this
CREATE OR REPLACE TABLE RAW_ORDERS (
    order_id                      VARCHAR(50),
    customer_id                   VARCHAR(50),
    order_status                  VARCHAR(50),
    order_purchase_timestamp      VARCHAR(50),
    order_approved_at             VARCHAR(50),
    order_delivered_carrier_date  VARCHAR(50),
    order_delivered_customer_date VARCHAR(50),
    order_estimated_delivery_date VARCHAR(50),
    dwh_load_date                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file               VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_ORDERS IS 'Raw order data - one row per customer order';

-- Table 2: Order Items (~112,650 rows)
CREATE OR REPLACE TABLE RAW_ORDER_ITEMS (
    order_id                VARCHAR(50),
    order_item_id           VARCHAR(50),
    product_id              VARCHAR(50),
    seller_id               VARCHAR(50),
    shipping_limit_date     VARCHAR(50),
    price                   VARCHAR(50),
    freight_value           VARCHAR(50),
    dwh_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file         VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_ORDER_ITEMS IS 'Raw order line items - one row per product in each order';

-- Table 3: Order Payments (~103,886 rows)
CREATE OR REPLACE TABLE RAW_ORDER_PAYMENTS (
    order_id                VARCHAR(50),
    payment_sequential      VARCHAR(50),
    payment_type            VARCHAR(50),
    payment_installments    VARCHAR(50),
    payment_value           VARCHAR(50),
    dwh_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file         VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_ORDER_PAYMENTS IS 'Raw payment data - orders can have multiple payments';

-- Table 4: Order Reviews (~100,000 rows)
CREATE OR REPLACE TABLE RAW_ORDER_REVIEWS (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            VARCHAR(50),
    review_comment_title    VARCHAR(500),
    review_comment_message  VARCHAR(65535),
    review_creation_date    VARCHAR(50),
    review_answer_timestamp VARCHAR(50),
    dwh_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file         VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_ORDER_REVIEWS IS 'Raw customer review data - one review per order';

-- Table 5: Customers (~99,441 rows)
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id             VARCHAR(50),
    customer_unique_id      VARCHAR(50),
    customer_zip_code_prefix VARCHAR(50),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(50),
    dwh_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file         VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_CUSTOMERS IS 'Raw customer data - customer_id is per-order, customer_unique_id is truly unique';

-- Table 6: Geolocation (~1,000,163 rows) — LARGEST TABLE
CREATE OR REPLACE TABLE RAW_GEOLOCATION (
    geolocation_zip_code_prefix VARCHAR(50),
    geolocation_lat             VARCHAR(50),
    geolocation_lng             VARCHAR(50),
    geolocation_city            VARCHAR(100),
    geolocation_state           VARCHAR(50),
    dwh_load_date               TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file             VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_GEOLOCATION IS 'Raw geolocation data - multiple lat/lng points per zip code (privacy fuzzing)';

-- Table 7: Products (~32,951 rows)
CREATE OR REPLACE TABLE RAW_PRODUCTS (
    product_id                  VARCHAR(50),
    product_category_name       VARCHAR(255),
    product_name_lenght         VARCHAR(50),
    product_description_lenght  VARCHAR(50),
    product_photos_qty          VARCHAR(50),
    product_weight_g            VARCHAR(50),
    product_length_cm           VARCHAR(50),
    product_height_cm           VARCHAR(50),
    product_width_cm            VARCHAR(50),
    dwh_load_date               TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file             VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_PRODUCTS IS 'Raw product catalog - category names in Portuguese, some NULLs';

-- Table 8: Category Translation (~71 rows)
CREATE OR REPLACE TABLE RAW_CATEGORY_TRANSLATION (
    product_category_name           VARCHAR(255),
    product_category_name_english   VARCHAR(255),
    dwh_load_date                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file                 VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_CATEGORY_TRANSLATION IS 'Category name translation lookup - Portuguese to English';

-- Table 9: Sellers (~3,095 rows)
CREATE OR REPLACE TABLE RAW_SELLERS (
    seller_id               VARCHAR(50),
    seller_zip_code_prefix  VARCHAR(50),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(50),
    dwh_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file         VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_SELLERS IS 'Raw seller data - links to closed_deals via seller_id';

-- ============================================================================
-- SECTION 2: MARKETING FUNNEL TABLES (2 tables)
-- ============================================================================

-- Table 10: Marketing Qualified Leads (~8,000 rows)
CREATE OR REPLACE TABLE RAW_MARKETING_QUALIFIED_LEADS (
    mql_id              VARCHAR(50),
    first_contact_date  VARCHAR(50),
    landing_page_id     VARCHAR(50),
    origin              VARCHAR(100),
    dwh_load_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file     VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_MARKETING_QUALIFIED_LEADS IS 'Raw MQLs - leads who requested contact to become sellers';

-- Table 11: Closed Deals (~841 rows)
CREATE OR REPLACE TABLE RAW_CLOSED_DEALS (
    mql_id                          VARCHAR(50),
    seller_id                       VARCHAR(50),
    sdr_id                          VARCHAR(50),
    sr_id                           VARCHAR(50),
    won_date                        VARCHAR(50),
    business_segment                VARCHAR(100),
    lead_type                       VARCHAR(50),
    lead_behaviour_profile          VARCHAR(50),
    has_company                     VARCHAR(10),
    has_gtin                        VARCHAR(10),
    average_stock                   VARCHAR(50),
    business_type                   VARCHAR(50),
    declared_product_catalog_size   VARCHAR(20),
    declared_monthly_revenue        VARCHAR(20),
    dwh_load_date                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file                 VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_CLOSED_DEALS IS 'Raw closed deals - MQLs converted to sellers. Links to sellers via seller_id';

-- ============================================================================
-- SECTION 3: CREATE API TABLES (3 tables — loaded separately via Python)
-- ============================================================================

-- Table 12: Currency Rates
CREATE OR REPLACE TABLE RAW_CURRENCY_RATES (
    rate_date           VARCHAR(20),
    base_currency       VARCHAR(5),
    target_currency     VARCHAR(5),
    exchange_rate       VARCHAR(20),
    dwh_load_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file     VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_CURRENCY_RATES IS 'Raw BRL→USD exchange rates from Frankfurter API';

-- Table 13: Brazil Holidays
CREATE OR REPLACE TABLE RAW_BRAZIL_HOLIDAYS (
    holiday_date    VARCHAR(20),
    local_name      VARCHAR(100),
    holiday_name    VARCHAR(100),
    country_code    VARCHAR(5),
    is_fixed        VARCHAR(10),
    is_global       VARCHAR(10),
    holiday_types   VARCHAR(100),
    dwh_load_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_BRAZIL_HOLIDAYS IS 'Raw Brazilian public holidays from Nager.Date API';

-- Table 14: Weather History
CREATE OR REPLACE TABLE RAW_WEATHER_HISTORY (
    latitude            VARCHAR(20),
    longitude           VARCHAR(20),
    state_code          VARCHAR(5),
    weather_date        VARCHAR(20),
    temperature_2m_mean VARCHAR(20),
    temperature_2m_max  VARCHAR(20),
    precipitation_sum   VARCHAR(20),
    weather_code        VARCHAR(10),
    dwh_load_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_source_file     VARCHAR(255) DEFAULT NULL
);
COMMENT ON TABLE RAW_WEATHER_HISTORY IS 'Raw daily weather data from Open-Meteo Archive API';

-- ============================================================================
-- SECTION 4: LOAD DATA FROM STAGE (11 CSV files)
-- ============================================================================

-- Note: Each COPY INTO excludes the dwh_load_date and dwh_source_file columns
-- because those have DEFAULT values and aren't in the CSV files.
-- We explicitly list the CSV columns to load into.

-- Orders
COPY INTO RAW_ORDERS (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
FROM @OLIST_STAGE/olist_orders_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Order Items
COPY INTO RAW_ORDER_ITEMS (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
FROM @OLIST_STAGE/olist_order_items_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Order Payments
COPY INTO RAW_ORDER_PAYMENTS (order_id, payment_sequential, payment_type, payment_installments, payment_value)
FROM @OLIST_STAGE/olist_order_payments_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Order Reviews
COPY INTO RAW_ORDER_REVIEWS (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
FROM @OLIST_STAGE/olist_order_reviews_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Customers
COPY INTO RAW_CUSTOMERS (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
FROM @OLIST_STAGE/olist_customers_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Geolocation (largest — ~1M rows)
COPY INTO RAW_GEOLOCATION (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
FROM @OLIST_STAGE/olist_geolocation_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Products
COPY INTO RAW_PRODUCTS (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
FROM @OLIST_STAGE/olist_products_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Category Translation
COPY INTO RAW_CATEGORY_TRANSLATION (product_category_name, product_category_name_english)
FROM @OLIST_STAGE/product_category_name_translation.csv
FILE_FORMAT = CSV_FORMAT;

-- Sellers
COPY INTO RAW_SELLERS (seller_id, seller_zip_code_prefix, seller_city, seller_state)
FROM @OLIST_STAGE/olist_sellers_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Marketing Qualified Leads
COPY INTO RAW_MARKETING_QUALIFIED_LEADS (mql_id, first_contact_date, landing_page_id, origin)
FROM @OLIST_STAGE/olist_marketing_qualified_leads_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- Closed Deals
COPY INTO RAW_CLOSED_DEALS (mql_id, seller_id, sdr_id, sr_id, won_date, business_segment, lead_type, lead_behaviour_profile, has_company, has_gtin, average_stock, business_type, declared_product_catalog_size, declared_monthly_revenue)
FROM @OLIST_STAGE/olist_closed_deals_dataset.csv
FILE_FORMAT = CSV_FORMAT;

-- ============================================================================
-- SECTION 5: UPDATE METADATA COLUMNS
-- ============================================================================

UPDATE RAW_ORDERS SET dwh_source_file = 'olist_orders_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_ORDER_ITEMS SET dwh_source_file = 'olist_order_items_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_ORDER_PAYMENTS SET dwh_source_file = 'olist_order_payments_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_ORDER_REVIEWS SET dwh_source_file = 'olist_order_reviews_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_CUSTOMERS SET dwh_source_file = 'olist_customers_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_GEOLOCATION SET dwh_source_file = 'olist_geolocation_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_PRODUCTS SET dwh_source_file = 'olist_products_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_CATEGORY_TRANSLATION SET dwh_source_file = 'product_category_name_translation.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_SELLERS SET dwh_source_file = 'olist_sellers_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_MARKETING_QUALIFIED_LEADS SET dwh_source_file = 'olist_marketing_qualified_leads_dataset.csv' WHERE dwh_source_file IS NULL;
UPDATE RAW_CLOSED_DEALS SET dwh_source_file = 'olist_closed_deals_dataset.csv' WHERE dwh_source_file IS NULL;

-- ============================================================================
-- SECTION 6: VERIFICATION — Row Counts
-- ============================================================================

SELECT 'RAW_ORDERS' AS table_name, COUNT(*) AS row_count FROM RAW_ORDERS
UNION ALL SELECT 'RAW_ORDER_ITEMS', COUNT(*) FROM RAW_ORDER_ITEMS
UNION ALL SELECT 'RAW_ORDER_PAYMENTS', COUNT(*) FROM RAW_ORDER_PAYMENTS
UNION ALL SELECT 'RAW_ORDER_REVIEWS', COUNT(*) FROM RAW_ORDER_REVIEWS
UNION ALL SELECT 'RAW_CUSTOMERS', COUNT(*) FROM RAW_CUSTOMERS
UNION ALL SELECT 'RAW_GEOLOCATION', COUNT(*) FROM RAW_GEOLOCATION
UNION ALL SELECT 'RAW_PRODUCTS', COUNT(*) FROM RAW_PRODUCTS
UNION ALL SELECT 'RAW_CATEGORY_TRANSLATION', COUNT(*) FROM RAW_CATEGORY_TRANSLATION
UNION ALL SELECT 'RAW_SELLERS', COUNT(*) FROM RAW_SELLERS
UNION ALL SELECT 'RAW_MARKETING_QUALIFIED_LEADS', COUNT(*) FROM RAW_MARKETING_QUALIFIED_LEADS
UNION ALL SELECT 'RAW_CLOSED_DEALS', COUNT(*) FROM RAW_CLOSED_DEALS
ORDER BY table_name;
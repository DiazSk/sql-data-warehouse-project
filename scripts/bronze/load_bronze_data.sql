/*
================================================================================
BRONZE LAYER: Load Raw Data into Bronze Tables
================================================================================

PURPOSE:
  Load data from CSV files into Bronze layer tables using PostgreSQL COPY command.
  API data is loaded separately via Python scripts.

PREREQUISITES:
  1. Docker environment running (docker-compose up -d)
  2. Bronze tables created (create_bronze_tables.sql)
  3. Datasets in /datasets/ folder (mounted via docker-compose)

USAGE:
  docker exec -it olist_postgres psql -U olist -d olist_dwh -f /scripts/bronze/load_bronze_data.sql

================================================================================
*/

SET search_path TO bronze, public;

\echo '============================================================'
\echo 'BRONZE LAYER - Loading Raw Data'
\echo '============================================================'

-- ============================================================================
-- SECTION 1: E-COMMERCE DATASET (9 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 1: olist_orders (~99,441 rows)
-- ----------------------------------------------------------------------------
\echo ''
\echo 'Loading bronze.olist_orders...'
TRUNCATE TABLE bronze.olist_orders;

COPY bronze.olist_orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
FROM '/datasets/e-commerce/olist_orders_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_orders SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_orders SET dwh_source_file = 'olist_orders_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_orders loaded'

-- ----------------------------------------------------------------------------
-- Table 2: olist_order_items (~112,650 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_order_items...'
TRUNCATE TABLE bronze.olist_order_items;

COPY bronze.olist_order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
FROM '/datasets/e-commerce/olist_order_items_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_order_items SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_order_items SET dwh_source_file = 'olist_order_items_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_order_items loaded'

-- ----------------------------------------------------------------------------
-- Table 3: olist_order_payments (~103,886 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_order_payments...'
TRUNCATE TABLE bronze.olist_order_payments;

COPY bronze.olist_order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
FROM '/datasets/e-commerce/olist_order_payments_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_order_payments SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_order_payments SET dwh_source_file = 'olist_order_payments_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_order_payments loaded'

-- ----------------------------------------------------------------------------
-- Table 4: olist_order_reviews (~100,000 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_order_reviews...'
TRUNCATE TABLE bronze.olist_order_reviews;

COPY bronze.olist_order_reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
FROM '/datasets/e-commerce/olist_order_reviews_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', QUOTE '"', ESCAPE '"');

UPDATE bronze.olist_order_reviews SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_order_reviews SET dwh_source_file = 'olist_order_reviews_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_order_reviews loaded'

-- ----------------------------------------------------------------------------
-- Table 5: olist_customers (~99,441 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_customers...'
TRUNCATE TABLE bronze.olist_customers;

COPY bronze.olist_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
FROM '/datasets/e-commerce/olist_customers_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_customers SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_customers SET dwh_source_file = 'olist_customers_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_customers loaded'

-- ----------------------------------------------------------------------------
-- Table 6: olist_geolocation (~1,000,163 rows) - LARGEST TABLE
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_geolocation... (largest table, may take 30+ seconds)'
TRUNCATE TABLE bronze.olist_geolocation;

COPY bronze.olist_geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
FROM '/datasets/e-commerce/olist_geolocation_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_geolocation SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_geolocation SET dwh_source_file = 'olist_geolocation_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_geolocation loaded'

-- ----------------------------------------------------------------------------
-- Table 7: olist_products (~32,951 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_products...'
TRUNCATE TABLE bronze.olist_products;

COPY bronze.olist_products (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
FROM '/datasets/e-commerce/olist_products_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_products SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_products SET dwh_source_file = 'olist_products_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_products loaded'

-- ----------------------------------------------------------------------------
-- Table 8: product_category_name_translation (~71 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.product_category_name_translation...'
TRUNCATE TABLE bronze.product_category_name_translation;

COPY bronze.product_category_name_translation (
    product_category_name,
    product_category_name_english
)
FROM '/datasets/e-commerce/product_category_name_translation.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.product_category_name_translation SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.product_category_name_translation SET dwh_source_file = 'product_category_name_translation.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ product_category_name_translation loaded'

-- ----------------------------------------------------------------------------
-- Table 9: olist_sellers (~3,095 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_sellers...'
TRUNCATE TABLE bronze.olist_sellers;

COPY bronze.olist_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
FROM '/datasets/e-commerce/olist_sellers_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_sellers SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_sellers SET dwh_source_file = 'olist_sellers_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_sellers loaded'

-- ============================================================================
-- SECTION 2: MARKETING FUNNEL DATASET (2 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 10: olist_marketing_qualified_leads (~8,000 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_marketing_qualified_leads...'
TRUNCATE TABLE bronze.olist_marketing_qualified_leads;

COPY bronze.olist_marketing_qualified_leads (
    mql_id,
    first_contact_date,
    landing_page_id,
    origin
)
FROM '/datasets/marketing_funnel/olist_marketing_qualified_leads_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_marketing_qualified_leads SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_marketing_qualified_leads SET dwh_source_file = 'olist_marketing_qualified_leads_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_marketing_qualified_leads loaded'

-- ----------------------------------------------------------------------------
-- Table 11: olist_closed_deals (~841 rows)
-- ----------------------------------------------------------------------------
\echo 'Loading bronze.olist_closed_deals...'
TRUNCATE TABLE bronze.olist_closed_deals;

COPY bronze.olist_closed_deals (
    mql_id,
    seller_id,
    sdr_id,
    sr_id,
    won_date,
    business_segment,
    lead_type,
    lead_behaviour_profile,
    has_company,
    has_gtin,
    average_stock,
    business_type,
    declared_product_catalog_size,
    declared_monthly_revenue
)
FROM '/datasets/marketing_funnel/olist_closed_deals_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

UPDATE bronze.olist_closed_deals SET dwh_load_date = CURRENT_TIMESTAMP WHERE dwh_load_date IS NULL;
UPDATE bronze.olist_closed_deals SET dwh_source_file = 'olist_closed_deals_dataset.csv' WHERE dwh_source_file IS NULL;
\echo '  ✓ olist_closed_deals loaded'

-- ============================================================================
-- SECTION 3: API DATA (Loaded via Python scripts)
-- ============================================================================
-- API tables are populated by Python scripts in /api folder:
--   - api/fetch_currency_rates.py  → bronze.api_currency_rates
--   - api/fetch_holidays.py        → bronze.api_brazil_holidays
--   - api/fetch_weather.py         → bronze.api_weather_history
-- ============================================================================

-- ============================================================================
-- SECTION 4: VERIFICATION
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'BRONZE LAYER - Record Counts'
\echo '============================================================'

SELECT 'olist_orders' AS table_name, COUNT(*) AS rows FROM bronze.olist_orders
UNION ALL SELECT 'olist_order_items', COUNT(*) FROM bronze.olist_order_items
UNION ALL SELECT 'olist_order_payments', COUNT(*) FROM bronze.olist_order_payments
UNION ALL SELECT 'olist_order_reviews', COUNT(*) FROM bronze.olist_order_reviews
UNION ALL SELECT 'olist_customers', COUNT(*) FROM bronze.olist_customers
UNION ALL SELECT 'olist_geolocation', COUNT(*) FROM bronze.olist_geolocation
UNION ALL SELECT 'olist_products', COUNT(*) FROM bronze.olist_products
UNION ALL SELECT 'product_category_translation', COUNT(*) FROM bronze.product_category_name_translation
UNION ALL SELECT 'olist_sellers', COUNT(*) FROM bronze.olist_sellers
UNION ALL SELECT 'olist_mql', COUNT(*) FROM bronze.olist_marketing_qualified_leads
UNION ALL SELECT 'olist_closed_deals', COUNT(*) FROM bronze.olist_closed_deals
ORDER BY table_name;

-- ============================================================================
-- SECTION 5: DATA QUALITY CHECKS
-- ============================================================================

\echo ''
\echo 'Data Quality Checks:'

-- Check for NULL primary keys
SELECT 'NULL order_id' AS check_name, COUNT(*) AS count FROM bronze.olist_orders WHERE order_id IS NULL
UNION ALL SELECT 'NULL customer_id', COUNT(*) FROM bronze.olist_customers WHERE customer_id IS NULL
UNION ALL SELECT 'NULL product_id', COUNT(*) FROM bronze.olist_products WHERE product_id IS NULL
UNION ALL SELECT 'NULL seller_id', COUNT(*) FROM bronze.olist_sellers WHERE seller_id IS NULL;

-- Order date range
\echo ''
\echo 'Order Date Range:'
SELECT MIN(order_purchase_timestamp) AS min_date, MAX(order_purchase_timestamp) AS max_date FROM bronze.olist_orders;

\echo ''
\echo '============================================================'
\echo 'BRONZE LAYER LOAD COMPLETE!'
\echo 'Next: Run Silver layer scripts'
\echo '============================================================'
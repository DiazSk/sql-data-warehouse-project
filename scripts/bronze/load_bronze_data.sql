/*
================================================================================
Description: Load raw data into Bronze layer tables
================================================================================

PURPOSE:
--------
Load data from CSV files into Bronze layer tables using PostgreSQL COPY command.
API data is loaded separately via Python scripts.

LOAD STRATEGY:
--------------
- Full Load: TRUNCATE table, then INSERT all records
- No transformations: Data loaded exactly as-is from source files
- All columns as VARCHAR to prevent data type errors during load

PREREQUISITES:
--------------
1. Run init_database.sql (create database and schemas)
2. Run create_bronze_tables.sql (create table structures)
3. Download datasets from Kaggle:
   - E-Commerce: kaggle.com/datasets/olistbr/brazilian-ecommerce
   - Marketing: kaggle.com/datasets/olistbr/marketing-funnel-olist
4. Place CSV files in the datasets folder

FILE PATHS:
-----------
Update the file paths below to match your local setup.
Default assumes: /path/to/project/datasets/

IMPORTANT:
----------
PostgreSQL COPY requires either:
  A) Superuser privileges, OR
  B) Use \copy in psql (client-side), OR
  C) Use pg_read_server_files role

For local development, use \copy in psql or import via DBeaver/pgAdmin GUI.

================================================================================
*/

-- ============================================================================
-- CONFIGURATION: Update these paths to match your local setup
-- ============================================================================

-- Option 1: Using psql variables (uncomment and set your path)
\set data_path 'C:\sql-data-warehouse-project\datasets'

-- Option 2: Direct paths in COPY commands (update paths below)

-- ============================================================================
-- SECTION 1: LOAD E-COMMERCE DATASET (9 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 1: olist_orders (~99,441 rows)
-- ----------------------------------------------------------------------------
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
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_orders_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

-- Verify load
SELECT 'olist_orders' as table_name, COUNT(*) as row_count FROM bronze.olist_orders;

-- ----------------------------------------------------------------------------
-- Table 2: olist_order_items (~112,650 rows)
-- ----------------------------------------------------------------------------
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
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_order_items_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_order_items' as table_name, COUNT(*) as row_count FROM bronze.olist_order_items;

-- ----------------------------------------------------------------------------
-- Table 3: olist_order_payments (~103,886 rows)
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze.olist_order_payments;

COPY bronze.olist_order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_order_payments_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_order_payments' as table_name, COUNT(*) as row_count FROM bronze.olist_order_payments;

-- ----------------------------------------------------------------------------
-- Table 4: olist_order_reviews (~100,000 rows)
-- NOTE: This file may have embedded commas in review text - handle carefully
-- ----------------------------------------------------------------------------
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
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_order_reviews_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', QUOTE '"', ESCAPE '"');

SELECT 'olist_order_reviews' as table_name, COUNT(*) as row_count FROM bronze.olist_order_reviews;

-- ----------------------------------------------------------------------------
-- Table 5: olist_customers (~99,441 rows)
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze.olist_order_customer;

COPY bronze.olist_order_customer (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_customers_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_customers' as table_name, COUNT(*) as row_count FROM bronze.olist_order_customer;

-- ----------------------------------------------------------------------------
-- Table 6: olist_geolocation (~1,000,163 rows) - LARGEST TABLE
-- NOTE: This is the largest file, may take 30+ seconds to load
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze.olist_geolocation;

COPY bronze.olist_geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_geolocation_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_geolocation' as table_name, COUNT(*) as row_count FROM bronze.olist_geolocation;

-- ----------------------------------------------------------------------------
-- Table 7: olist_products (~32,951 rows)
-- ----------------------------------------------------------------------------
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
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_products_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_products' as table_name, COUNT(*) as row_count FROM bronze.olist_products;

-- ----------------------------------------------------------------------------
-- Table 8: olist_category_translation (~71 rows)
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze.olist_category_translation;

COPY bronze.olist_category_translation (
    product_category_name,
    product_category_name_english
)
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\product_category_name_translation.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_category_translation' as table_name, COUNT(*) as row_count FROM bronze.olist_category_translation;

-- ----------------------------------------------------------------------------
-- Table 9: olist_sellers (~3,095 rows)
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze.olist_sellers;

COPY bronze.olist_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
FROM 'C:\sql-data-warehouse-project\datasets\e-commerce\olist_sellers_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_sellers' as table_name, COUNT(*) as row_count FROM bronze.olist_sellers;

-- ============================================================================
-- SECTION 2: LOAD MARKETING FUNNEL DATASET (2 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 10: olist_marketing_qualified_leads (~8,000 rows)
-- ----------------------------------------------------------------------------
TRUNCATE TABLE bronze.olist_marketing_qualified_leads;

COPY bronze.olist_marketing_qualified_leads (
    mql_id,
    first_contact_date,
    landing_page_id,
    origin
)
FROM 'C:\sql-data-warehouse-project\datasets\marketing_funnel\olist_marketing_qualified_leads_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_marketing_qualified_leads' as table_name, COUNT(*) as row_count FROM bronze.olist_marketing_qualified_leads;

-- ----------------------------------------------------------------------------
-- Table 11: olist_closed_deals (~841 rows)
-- ----------------------------------------------------------------------------
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
FROM 'C:\sql-data-warehouse-project\datasets\marketing_funnel\olist_closed_deals_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

SELECT 'olist_closed_deals' as table_name, COUNT(*) as row_count FROM bronze.olist_closed_deals;

-- ============================================================================
-- SECTION 3: API DATA TABLES (3 tables)
-- NOTE: These are loaded via Python scripts, not COPY commands
-- See: api/fetch_currency_rates.py, api/fetch_holidays.py, api/fetch_weather.py
-- ============================================================================

-- Placeholder verification queries (data loaded by Python)
SELECT 'api_currency_rates' as table_name, COUNT(*) as row_count FROM bronze.api_currency_rates;
SELECT 'api_brazil_holidays' as table_name, COUNT(*) as row_count FROM bronze.api_brazil_holidays;
SELECT 'api_weather_history' as table_name, COUNT(*) as row_count FROM bronze.api_weather_history;

-- ============================================================================
-- SECTION 4: FINAL VERIFICATION
-- ============================================================================

-- Summary of all Bronze tables
SELECT
    'bronze' as schema_name,
    table_name,
    (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
FROM (
    SELECT
        table_name,
        query_to_xml(format('SELECT COUNT(*) as cnt FROM bronze.%I', table_name), false, true, '') as xml_count
    FROM information_schema.tables
    WHERE table_schema = 'bronze'
) t
ORDER BY table_name;

-- Alternative: Simple count queries for each table
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Bronze Layer Load Summary';
    RAISE NOTICE '========================================';

    SELECT COUNT(*) INTO v_count FROM bronze.olist_orders;
    RAISE NOTICE 'olist_orders: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_order_items;
    RAISE NOTICE 'olist_order_items: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_order_payments;
    RAISE NOTICE 'olist_order_payments: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_order_reviews;
    RAISE NOTICE 'olist_order_reviews: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_order_customer;
    RAISE NOTICE 'olist_customers: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_geolocation;
    RAISE NOTICE 'olist_geolocation: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_products;
    RAISE NOTICE 'olist_products: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_category_translation;
    RAISE NOTICE 'olist_category_translation: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_sellers;
    RAISE NOTICE 'olist_sellers: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_marketing_qualified_leads;
    RAISE NOTICE 'olist_marketing_qualified_leads: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.olist_closed_deals;
    RAISE NOTICE 'olist_closed_deals: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.api_currency_rates;
    RAISE NOTICE 'api_currency_rates: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.api_brazil_holidays;
    RAISE NOTICE 'api_brazil_holidays: % rows', v_count;

    SELECT COUNT(*) INTO v_count FROM bronze.api_weather_history;
    RAISE NOTICE 'api_weather_history: % rows', v_count;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Bronze layer load complete!';
    RAISE NOTICE 'Next step: Run Silver layer scripts';
    RAISE NOTICE '========================================';
END $$;

-- ============================================================================
-- SECTION 5: DATA QUALITY SPOT CHECKS
-- ============================================================================

-- Check for NULL primary keys (should be 0)
SELECT 'Orders with NULL order_id' as check_name, COUNT(*) as count
FROM bronze.olist_orders WHERE order_id IS NULL;

SELECT 'Customers with NULL customer_id' as check_name, COUNT(*) as count
FROM bronze.olist_order_customer WHERE customer_id IS NULL;

SELECT 'Products with NULL product_id' as check_name, COUNT(*) as count
FROM bronze.olist_products WHERE product_id IS NULL;

-- Check for known data quality issues
SELECT 'Products with NULL category' as check_name, COUNT(*) as count
FROM bronze.olist_products WHERE product_category_name IS NULL OR product_category_name = '';

-- Check date range in orders
SELECT
    'Order date range' as check_name,
    MIN(order_purchase_timestamp) as min_date,
    MAX(order_purchase_timestamp) as max_date
FROM bronze.olist_orders;

-- Check marketing-to-ecommerce link
SELECT
    'Closed deals with matching seller_id' as check_name,
    COUNT(*) as matched_count,
    (SELECT COUNT(*) FROM bronze.olist_closed_deals) as total_deals,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_closed_deals), 2) as match_percentage
FROM bronze.olist_closed_deals cd
WHERE EXISTS (SELECT 1 FROM bronze.olist_sellers s WHERE s.seller_id = cd.seller_id);
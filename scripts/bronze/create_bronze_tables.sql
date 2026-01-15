/*
================================================================================
Description: Create all Bronze layer tables for Olist Data Warehouse
================================================================================

PURPOSE:
--------
Creates 14 Bronze layer tables to store raw data from:
- Olist E-Commerce Dataset (9 tables)
- Olist Marketing Funnel Dataset (2 tables)
- External APIs (3 tables)

BRONZE LAYER PRINCIPLES:
------------------------
1. Store data EXACTLY as received from source (no transformations)
2. All columns are VARCHAR to prevent load failures from data type issues
3. Include technical metadata columns (dwh_load_date, dwh_source_file)
4. Use TRUNCATE & INSERT for full loads (no incremental)

NAMING CONVENTION:
------------------
- Pattern: <sourcesystem>_<entity>
- Source systems: olist (CSV), api (External APIs)
- Example: olist_orders, api_currency_rates

EXECUTION ORDER:
----------------
1. Run init_database.sql first
2. Run this script to create tables
3. Run load_bronze_data.sql to load data

================================================================================
*/

-- ============================================================================
-- PRE-EXECUTION: Set search path and verify connection
-- ============================================================================

-- Ensure we're working in the correct schema
SET search_path TO bronze, public;

-- ============================================================================
-- SECTION 1: E-COMMERCE DATASET TABLES (9 tables)
-- Source: Kaggle - Brazilian E-Commerce by Olist
-- ============================================================================

-- ----------------------------------------------------------------------------
-- TABLE 1: olist_orders
-- Description: Core order information - one row per order
-- Source File: olist_orders_dataset.csv
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_orders;

CREATE TABLE bronze.olist_orders (
    order_id    VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp VARCHAR(50),
    order_approved_at VARCHAR(50),
    order_delivered_carrier_date VARCHAR(50),
    order_delivered_customer_date VARCHAR(50),
    order_estimated_delivery_date VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_orders IS 'Raw order data - one row per customer order';
COMMENT ON COLUMN bronze.olist_orders.order_id IS 'Unique order identifier (PK in source)';
COMMENT ON COLUMN bronze.olist_orders.customer_id IS 'Foreign key to customers table';
COMMENT ON COLUMN bronze.olist_orders.order_status IS 'Order status: delivered, shipped, canceled, etc.';
COMMENT ON COLUMN bronze.olist_orders.dwh_load_date IS 'Timestamp when record was loaded into DWH';

-- ----------------------------------------------------------------------------
-- Table 2: olist_order_items
-- Description: Line items within orders - one row per product in an order
-- Source File: olist_order_items_dataset.csv
-- Record Count: ~112,650
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_order_items;

CREATE TABLE bronze.olist_order_items (
    order_id VARCHAR(50),
    order_item_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date VARCHAR(50),
    price VARCHAR(50),
    freight_value VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_order_items IS 'Raw order line items - one row per product in each order';
COMMENT ON COLUMN bronze.olist_order_items.order_item_id IS 'Sequential item number within order (1, 2, 3...)';
COMMENT ON COLUMN bronze.olist_order_items.price IS 'Item price in BRL (stored as string)';
COMMENT ON COLUMN bronze.olist_order_items.freight_value IS 'Freight cost in BRL (stored as string)';

-- ----------------------------------------------------------------------------
-- Table 3: olist_order_payments
-- Description: Payment information for orders - can have multiple payments per order
-- Source File: olist_order_payments_dataset.csv
-- Record Count: ~103,886
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_order_payments;

CREATE TABLE bronze.olist_order_payments (
    order_id VARCHAR(50),
    payment_sequential VARCHAR(50),
    payment_type VARCHAR(50),
    payment_installments VARCHAR(50),
    payment_value VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_order_payments IS 'Raw payment data - one row per payment (orders can have multiple payments)';
COMMENT ON COLUMN bronze.olist_order_payments.payment_type IS 'Payment method: credit_card, boleto, voucher, debit_card';
COMMENT ON COLUMN bronze.olist_order_payments.payment_installments IS 'Number of installments chosen';

-- ----------------------------------------------------------------------------
-- Table 4: olist_order_reviews
-- Description: Customer reviews for orders - one review per order
-- Source File: olist_order_reviews_dataset.csv
-- Record Count: ~100,000
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_order_reviews;

CREATE TABLE bronze.olist_order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score VARCHAR(50),
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date VARCHAR(50),
    review_answer_timestamp VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_order_reviews IS 'Raw customer review data - one review per order';
COMMENT ON COLUMN bronze.olist_order_reviews.review_score IS 'Rating from 1 (worst) to 5 (best)';
COMMENT ON COLUMN bronze.olist_order_reviews.review_comment_message IS 'Optional review text (often NULL)';

-- ----------------------------------------------------------------------------
-- Table 5: olist_order_customers
-- Description: Customer information - one row per order-customer combination
-- Source File: olist_order_customers_dataset.csv
-- Record Count: ~99,441
-- NOTE: customer_id is per-order; use customer_unique_id for true unique customers
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_order_customers;

CREATE TABLE bronze.olist_order_customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_order_customers IS 'Raw customer data - customer_id is per-order, customer_unique_id is truly unique';
COMMENT ON COLUMN bronze.olist_order_customers.customer_id IS 'Order-specific customer ID (FK from orders)';
COMMENT ON COLUMN bronze.olist_order_customers.customer_unique_id IS 'True unique customer identifier for deduplication';

-- ----------------------------------------------------------------------------
-- Table 6: olist_geolocation
-- Description: Geographic coordinates by zip code prefix
-- Source File: olist_geolocation_dataset.csv
-- Record Count: ~1,000,163 (multiple points per zip for privacy)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_geolocation;

CREATE TABLE bronze.olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(50),
    geolocation_lat VARCHAR(50),
    geolocation_lng VARCHAR(50),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_geolocation IS 'Raw geolocation data - multiple lat/lng points per zip code (privacy fuzzing)';
COMMENT ON COLUMN bronze.olist_geolocation.geolocation_zip_code_prefix IS '5-digit Brazilian zip code prefix';

-- ----------------------------------------------------------------------------
-- Table 7: olist_products
-- Description: Product catalog information
-- Source File: olist_products_dataset.csv
-- Record Count: ~32,951
-- NOTE: Category names are in Portuguese, ~610 have NULL category
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_products;

CREATE TABLE bronze.olist_products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(255),
    product_name_lenght VARCHAR(50),
    product_description_lenght VARCHAR(50),
    product_photos_qty VARCHAR(50),
    product_weight_g VARCHAR(50),
    product_length_cm VARCHAR(50),
    product_height_cm VARCHAR(50),
    product_width_cm VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_products IS 'Raw product catalog - category names in Portuguese, some NULLs';
COMMENT ON COLUMN bronze.olist_products.product_category_name IS 'Product category in Portuguese (~610 NULLs)';
COMMENT ON COLUMN bronze.olist_products.product_name_lenght IS 'Length of product name (typo preserved from source)';

-- ----------------------------------------------------------------------------
-- Table 8: olist_category_translation
-- Description: Portuguese to English category name translations
-- Source File: product_category_name_translation.csv
-- Record Count: ~71
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_category_translation;

CREATE TABLE bronze.olist_category_translation (
    product_category_name VARCHAR(255),
    product_category_name_english VARCHAR(255),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_category_translation IS 'Category name translation lookup - Portuguese to English';

-- ----------------------------------------------------------------------------
-- Table 9: olist_sellers
-- Description: Seller information
-- Source File: olist_sellers_dataset.csv
-- Record Count: ~3,095
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_sellers;

CREATE TABLE bronze.olist_sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(50),
    seller_city VARCHAR(100),
    seller_state VARCHAR(50),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_sellers IS 'Raw seller data - links to closed_deals via seller_id';

-- ============================================================================
-- SECTION 2: MARKETING FUNNEL DATASET TABLES (2 tables)
-- Source: Kaggle - Marketing Funnel by Olist
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 10: olist_mql (Marketing Qualified Leads)
-- Description: Leads who requested contact to sell on Olist
-- Source File: olist_marketing_qualified_leads_dataset.csv
-- Record Count: ~8,000
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_marketing_qualified_leads;

CREATE TABLE bronze.olist_marketing_qualified_leads (
    mql_id VARCHAR(50),
    first_contact_date VARCHAR(50),
    landing_page_id VARCHAR(50),
    origin VARCHAR(100),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_marketing_qualified_leads IS 'Raw Marketing Qualified Leads - leads who requested contact to become sellers';
COMMENT ON COLUMN bronze.olist_marketing_qualified_leads.origin IS 'Lead source: organic_search, paid_search, social, direct_traffic, email, referral';

-- ----------------------------------------------------------------------------
-- Table 11: olist_closed_deals
-- Description: MQLs that converted to sellers
-- Source File: olist_closed_deals_dataset.csv
-- Record Count: ~841
-- KEY: seller_id links to olist_sellers (E-Commerce dataset)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.olist_closed_deals;

CREATE TABLE bronze.olist_closed_deals (
    mql_id VARCHAR(50),
    seller_id VARCHAR(50),
    sdr_id VARCHAR(50),
    sr_id VARCHAR(50),
    won_date VARCHAR(50),
    business_segment VARCHAR(100),
    lead_type VARCHAR(50),
    lead_behaviour_profile VARCHAR(50),
    has_company VARCHAR(10),
    has_gtin VARCHAR(10),
    average_stock VARCHAR(50),
    business_type VARCHAR(50),
    declared_product_catalog_size VARCHAR(20),
    declared_monthly_revenue VARCHAR(20),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.olist_closed_deals IS 'Raw closed deals - MQLs converted to sellers. Links to olist_sellers via seller_id';
COMMENT ON COLUMN bronze.olist_closed_deals.seller_id IS 'CRITICAL: Foreign key linking Marketing Funnel to E-Commerce dataset';
COMMENT ON COLUMN bronze.olist_closed_deals.declared_monthly_revenue IS 'Self-reported monthly revenue by seller';

-- ============================================================================
-- SECTION 3: EXTERNAL API TABLES (3 tables)
-- Source: REST APIs for data enrichment
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 12: api_currency_rates
-- Description: Daily BRL to USD exchange rates
-- Source: Frankfurter API (api.frankfurter.app) - European Central Bank data
-- API Docs: https://www.frankfurter.app/docs/
-- Record Count: ~550 (business days from Sep 2016 - Oct 2018)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.api_currency_rates;

CREATE TABLE bronze.api_currency_rates (
    rate_date VARCHAR(20),
    base_currency VARCHAR(5),
    target_currency VARCHAR(5),
    exchange_rate VARCHAR(20),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.api_currency_rates IS 'Raw currency exchange rates from Frankfurter API (European Central Bank data)';
COMMENT ON COLUMN bronze.api_currency_rates.exchange_rate IS 'Exchange rate: 1 BRL = X USD';

-- ----------------------------------------------------------------------------
-- Table 13: api_brazil_holidays
-- Description: Brazilian public holidays
-- Source: Nager.Date API (date.nager.at)
-- Record Count: ~50 (holidays for 2016, 2017, 2018)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.api_brazil_holidays;

CREATE TABLE bronze.api_brazil_holidays (
    holiday_date VARCHAR(20),
    local_name VARCHAR(100),
    holiday_name VARCHAR(100),
    country_code VARCHAR(5),
    is_fixed VARCHAR(10),
    is_global VARCHAR(10),
    holiday_types VARCHAR(100),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.api_brazil_holidays IS 'Raw Brazilian public holidays from Nager.Date API';
COMMENT ON COLUMN bronze.api_brazil_holidays.local_name IS 'Holiday name in Portuguese';
COMMENT ON COLUMN bronze.api_brazil_holidays.holiday_name IS 'Holiday name in English';

-- ----------------------------------------------------------------------------
-- Table 14: api_weather_history
-- Description: Historical weather data by location (Daily aggregates - Minimal)
-- Source: Open-Meteo Historical Weather API (archive-api.open-meteo.com)
-- API Docs: https://open-meteo.com/en/docs/historical-weather-api
-- Record Count: ~21K (27 state capitals × ~790 days)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS bronze.api_weather_history;

CREATE TABLE bronze.api_weather_history (
    latitude VARCHAR(20),
    longitude VARCHAR(20),
    state_code VARCHAR(5),
    weather_date VARCHAR(20),
    temperature_2m_mean VARCHAR(20),
    temperature_2m_max  VARCHAR(20),
    precipitation_sum VARCHAR(20),
    weather_code VARCHAR(10),
    dwh_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_file VARCHAR(255) DEFAULT NULL
);

COMMENT ON TABLE bronze.api_weather_history IS 'Raw historical daily weather data from Open-Meteo Archive API (minimal columns for business analysis)';
COMMENT ON COLUMN bronze.api_weather_history.weather_code IS 'WMO weather code: 0=Clear, 1-3=Cloudy, 45-48=Fog, 51-55=Drizzle, 61-65=Rain, 71-77=Snow, 80-82=Showers, 95-99=Thunderstorm';
COMMENT ON COLUMN bronze.api_weather_history.precipitation_sum IS 'Total daily precipitation (rain + showers + snowfall) in millimeters';
COMMENT ON COLUMN bronze.api_weather_history.temperature_2m_mean IS 'Mean daily air temperature at 2 meters above ground in Celsius';

-- ============================================================================
-- SECTION 4: VERIFICATION QUERIES
-- ============================================================================

-- List all Bronze tables created
SELECT
    table_schema,
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns c
     WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'bronze'
ORDER BY table_name;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Bronze layer tables created successfully!';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE 'E-Commerce Tables (9):';
    RAISE NOTICE '  1. bronze.olist_orders';
    RAISE NOTICE '  2. bronze.olist_order_items';
    RAISE NOTICE '  3. bronze.olist_order_payments';
    RAISE NOTICE '  4. bronze.olist_order_reviews';
    RAISE NOTICE '  5. bronze.olist_order_customers';
    RAISE NOTICE '  6. bronze.olist_geolocation';
    RAISE NOTICE '  7. bronze.olist_products';
    RAISE NOTICE '  8. bronze.olist_category_translation';
    RAISE NOTICE '  9. bronze.olist_sellers';
    RAISE NOTICE '';
    RAISE NOTICE 'Marketing Funnel Tables (2):';
    RAISE NOTICE '  10. bronze.olist_mql';
    RAISE NOTICE '  11. bronze.olist_closed_deals';
    RAISE NOTICE '';
    RAISE NOTICE 'API Tables (3):';
    RAISE NOTICE '  12. bronze.api_currency_rates';
    RAISE NOTICE '  13. bronze.api_brazil_holidays';
    RAISE NOTICE '  14. bronze.api_weather_history';
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total: 14 tables created';
    RAISE NOTICE '========================================';
END $$;
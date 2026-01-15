-- ============================================================================
-- Description: Create Silver layer tables with proper data types and constraints
-- ============================================================================
--
-- PURPOSE:
-- --------
-- Creates Silver layer tables that will hold CLEANED and TRANSFORMED data.
-- Unlike Bronze (all VARCHAR), Silver has:
--   ✓ Proper data types (INTEGER, DATE, DECIMAL, BOOLEAN, TIMESTAMP)
--   ✓ Primary Key constraints
--   ✓ Fixed column names (typos corrected)
--   ✓ Derived/calculated columns
--   ✓ Transformation metadata columns
--
-- PREREQUISITES:
-- --------------
-- 1. Database 'olist_dwh' exists
-- 2. Schema 'silver' exists (created in create_database.sql)
-- 3. Bronze layer tables exist and are loaded
--
-- USAGE:
-- ------
-- psql -d olist_dwh -f create_silver_tables.sql
--
-- ============================================================================

-- Set search path
SET search_path TO silver, public;

-- ============================================================================
-- SECTION 1: E-COMMERCE TABLES (9 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 1: olist_orders
-- Description: Cleaned order transactions with derived delivery metrics
-- Source: bronze.olist_orders
-- Records: ~99,441
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_orders CASCADE;

CREATE TABLE silver.olist_orders (
    -- Primary Key
    order_id                        VARCHAR(32) PRIMARY KEY,

    -- Foreign Key
    customer_id                     VARCHAR(32) NOT NULL,

    -- Order details (proper types)
    order_status                    VARCHAR(20) NOT NULL,
    order_purchase_timestamp        TIMESTAMP NOT NULL,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,

    -- DERIVED COLUMNS (calculated during transformation)
    order_purchase_date             DATE NOT NULL,
    is_delivered                    BOOLEAN DEFAULT FALSE,
    is_late_delivery                BOOLEAN,
    delivery_days_actual            INTEGER,
    delivery_days_estimated         INTEGER,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_orders',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_orders IS 'Cleaned orders with proper types and delivery metrics';
COMMENT ON COLUMN silver.olist_orders.is_late_delivery IS 'TRUE if delivered after estimated date';
COMMENT ON COLUMN silver.olist_orders.delivery_days_actual IS 'Days from purchase to actual delivery';

-- ----------------------------------------------------------------------------
-- Table 2: olist_order_items
-- Description: Cleaned line items with calculated totals
-- Source: bronze.olist_order_items
-- Records: ~112,650
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_order_items CASCADE;

CREATE TABLE silver.olist_order_items (
    -- Composite Primary Key
    order_id                        VARCHAR(32) NOT NULL,
    order_item_id                   INTEGER NOT NULL,

    -- Foreign Keys
    product_id                      VARCHAR(32) NOT NULL,
    seller_id                       VARCHAR(32) NOT NULL,

    -- Item details (proper types)
    shipping_limit_date             TIMESTAMP,
    price                           DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    freight_value                   DECIMAL(10,2) NOT NULL DEFAULT 0.00,

    -- DERIVED COLUMNS
    item_total                      DECIMAL(10,2) NOT NULL DEFAULT 0.00,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_order_items',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT,

    -- Composite PK constraint
    PRIMARY KEY (order_id, order_item_id)
);

COMMENT ON TABLE silver.olist_order_items IS 'Cleaned order line items with item_total calculated';
COMMENT ON COLUMN silver.olist_order_items.item_total IS 'price + freight_value';

-- ----------------------------------------------------------------------------
-- Table 3: olist_order_payments
-- Description: Cleaned payment records with installment flag
-- Source: bronze.olist_order_payments
-- Records: ~103,886
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_order_payments CASCADE;

CREATE TABLE silver.olist_order_payments (
    -- Composite Primary Key
    order_id                        VARCHAR(32) NOT NULL,
    payment_sequential              INTEGER NOT NULL,

    -- Payment details (proper types)
    payment_type                    VARCHAR(20) NOT NULL,
    payment_installments            INTEGER NOT NULL DEFAULT 1,
    payment_value                   DECIMAL(10,2) NOT NULL DEFAULT 0.00,

    -- DERIVED COLUMNS
    is_single_payment               BOOLEAN DEFAULT TRUE,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_order_payments',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT,

    -- Composite PK constraint
    PRIMARY KEY (order_id, payment_sequential)
);

COMMENT ON TABLE silver.olist_order_payments IS 'Cleaned payment records';
COMMENT ON COLUMN silver.olist_order_payments.is_single_payment IS 'TRUE if payment_installments = 1';

-- ----------------------------------------------------------------------------
-- Table 4: olist_order_reviews
-- Description: Cleaned reviews with sentiment flags
-- Source: bronze.olist_order_reviews
-- Records: ~100,000
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_order_reviews CASCADE;

CREATE TABLE silver.olist_order_reviews (
    -- Primary Key
    review_id                       VARCHAR(36) PRIMARY KEY,

    -- Foreign Key
    order_id                        VARCHAR(32) NOT NULL,

    -- Review details (proper types)
    review_score                    INTEGER,
    review_comment_title            VARCHAR(100),
    review_comment_message          TEXT,
    review_creation_date            TIMESTAMP,
    review_answer_timestamp         TIMESTAMP,

    -- DERIVED COLUMNS
    has_comment                     BOOLEAN DEFAULT FALSE,
    is_positive                     BOOLEAN,
    is_negative                     BOOLEAN,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_order_reviews',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_order_reviews IS 'Cleaned reviews with sentiment classification';
COMMENT ON COLUMN silver.olist_order_reviews.is_positive IS 'TRUE if review_score >= 4';
COMMENT ON COLUMN silver.olist_order_reviews.is_negative IS 'TRUE if review_score <= 2';

-- ----------------------------------------------------------------------------
-- Table 5: olist_order_customers
-- Description: Cleaned customer records with standardized location
-- Source: bronze.olist_order_customers
-- Records: ~99,441
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_order_customers CASCADE;

CREATE TABLE silver.olist_order_customers (
    -- Primary Key
    customer_id                     VARCHAR(32) PRIMARY KEY,

    -- Customer identifiers
    customer_unique_id              VARCHAR(32) NOT NULL,

    -- Location (cleaned and standardized)
    customer_zip_code_prefix        VARCHAR(5) NOT NULL,
    customer_city                   VARCHAR(100),
    customer_state                  VARCHAR(2) NOT NULL,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_order_customers',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_order_customers IS 'Cleaned customers with standardized location';
COMMENT ON COLUMN silver.olist_order_customers.customer_unique_id IS 'True unique customer ID (use for repeat analysis)';

-- ----------------------------------------------------------------------------
-- Table 6: olist_sellers
-- Description: Cleaned seller records with standardized location
-- Source: bronze.olist_sellers
-- Records: ~3,095
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_sellers CASCADE;

CREATE TABLE silver.olist_sellers (
    -- Primary Key
    seller_id                       VARCHAR(32) PRIMARY KEY,

    -- Location (cleaned and standardized)
    seller_zip_code_prefix          VARCHAR(5) NOT NULL,
    seller_city                     VARCHAR(100),
    seller_state                    VARCHAR(2) NOT NULL,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_sellers',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_sellers IS 'Cleaned sellers with standardized location';

-- ----------------------------------------------------------------------------
-- Table 7: olist_products
-- Description: Cleaned products with FIXED TYPOS and calculated volume
-- Source: bronze.olist_products
-- Records: ~32,951
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_products CASCADE;

CREATE TABLE silver.olist_products (
    -- Primary Key
    product_id                      VARCHAR(32) PRIMARY KEY,

    -- Product details
    product_category_name           VARCHAR(100),

    -- Dimensions (TYPOS FIXED!)
    product_name_length             INTEGER,          -- Was: product_name_lenght
    product_description_length      INTEGER,          -- Was: product_description_lenght
    product_photos_qty              INTEGER,

    -- Physical attributes
    product_weight_g                DECIMAL(10,2),
    product_length_cm               DECIMAL(10,2),
    product_height_cm               DECIMAL(10,2),
    product_width_cm                DECIMAL(10,2),

    -- DERIVED COLUMNS
    product_volume_cm3              DECIMAL(12,2),
    has_dimensions                  BOOLEAN DEFAULT FALSE,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_products',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_products IS 'Cleaned products with fixed typos and calculated volume';
COMMENT ON COLUMN silver.olist_products.product_name_length IS 'RENAMED from product_name_lenght (typo fixed)';
COMMENT ON COLUMN silver.olist_products.product_description_length IS 'RENAMED from product_description_lenght (typo fixed)';
COMMENT ON COLUMN silver.olist_products.product_volume_cm3 IS 'length * height * width';

-- ----------------------------------------------------------------------------
-- Table 8: olist_category_translation
-- Description: Category name lookup (Portuguese to English)
-- Source: bronze.olist_category_translation
-- Records: ~71
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_category_translation CASCADE;

CREATE TABLE silver.olist_category_translation (
    -- Primary Key (Portuguese name)
    product_category_name           VARCHAR(100) PRIMARY KEY,

    -- English translation
    product_category_name_english   VARCHAR(100) NOT NULL,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_category_translation',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_category_translation IS 'Category lookup: Portuguese to English';

-- ----------------------------------------------------------------------------
-- Table 9: olist_geolocation
-- Description: DEDUPLICATED geolocation with averaged coordinates
-- Source: bronze.olist_geolocation
-- Records: ~19,015 (deduplicated from ~1M)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_geolocation CASCADE;

CREATE TABLE silver.olist_geolocation (
    -- Primary Key (RENAMED from geolocation_zip_code_prefix)
    zip_code_prefix                 VARCHAR(5) PRIMARY KEY,

    -- Coordinates (RENAMED and averaged)
    latitude                        DECIMAL(9,6) NOT NULL,      -- Was: geolocation_lat
    longitude                       DECIMAL(9,6) NOT NULL,      -- Was: geolocation_lng

    -- Location (RENAMED and cleaned)
    city                            VARCHAR(100),                -- Was: geolocation_city
    state                           VARCHAR(2) NOT NULL,         -- Was: geolocation_state

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_geolocation',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_geolocation IS 'Deduplicated geolocation with averaged coordinates per zip';
COMMENT ON COLUMN silver.olist_geolocation.latitude IS 'Average latitude for zip code (deduplicated)';
COMMENT ON COLUMN silver.olist_geolocation.longitude IS 'Average longitude for zip code (deduplicated)';

-- ============================================================================
-- SECTION 2: MARKETING FUNNEL TABLES (2 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 10: olist_marketing_qualified_leads
-- Description: Cleaned Marketing Qualified Leads
-- Source: bronze.olist_marketing_qualified_leads
-- Records: ~8,000
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_marketing_qualified_leads CASCADE;

CREATE TABLE silver.olist_marketing_qualified_leads (
    -- Primary Key
    mql_id                          VARCHAR(32) PRIMARY KEY,

    -- Lead details (proper types)
    first_contact_date              DATE NOT NULL,
    landing_page_id                 VARCHAR(32),
    origin                          VARCHAR(50),

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_marketing_qualified_leads',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_marketing_qualified_leads IS 'Cleaned Marketing Qualified Leads';

-- ----------------------------------------------------------------------------
-- Table 11: olist_closed_deals
-- Description: Cleaned closed deals with cross-system link flag
-- Source: bronze.olist_closed_deals
-- Records: ~841
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.olist_closed_deals CASCADE;

CREATE TABLE silver.olist_closed_deals (
    -- Primary Key (also FK to mql)
    mql_id                          VARCHAR(32) PRIMARY KEY,

    -- CRITICAL: Cross-system link to E-Commerce
    seller_id                       VARCHAR(32),

    -- Sales team
    sdr_id                          VARCHAR(32),
    sr_id                           VARCHAR(32),

    -- Deal details (proper types)
    won_date                        DATE NOT NULL,
    business_segment                VARCHAR(50),
    lead_type                       VARCHAR(20),
    lead_behaviour_profile          VARCHAR(20),
    has_company                     BOOLEAN,
    has_gtin                        BOOLEAN,
    average_stock                   VARCHAR(20),
    business_type                   VARCHAR(20),
    declared_product_catalog_size   DECIMAL(10,2),
    declared_monthly_revenue        DECIMAL(12,2),

    -- DERIVED COLUMNS
    has_seller_id                   BOOLEAN DEFAULT FALSE,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.olist_closed_deals',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.olist_closed_deals IS 'Cleaned closed deals with cross-system link';
COMMENT ON COLUMN silver.olist_closed_deals.seller_id IS 'CRITICAL: Links Marketing Funnel to E-Commerce (only ~45% match)';
COMMENT ON COLUMN silver.olist_closed_deals.has_seller_id IS 'TRUE if seller_id exists (can link to E-Commerce)';

-- ============================================================================
-- SECTION 3: API TABLES (3 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 12: api_currency_rates
-- Description: Cleaned currency rates with inverse rate
-- Source: bronze.api_currency_rates
-- Records: ~550
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.api_currency_rates CASCADE;

CREATE TABLE silver.api_currency_rates (
    -- Primary Key
    rate_date                       DATE PRIMARY KEY,

    -- Currency pair
    base_currency                   VARCHAR(3) NOT NULL DEFAULT 'BRL',
    target_currency                 VARCHAR(3) NOT NULL DEFAULT 'USD',

    -- Rate (proper type)
    exchange_rate                   DECIMAL(10,6) NOT NULL,

    -- DERIVED COLUMNS
    rate_inverse                    DECIMAL(10,6),

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.api_currency_rates',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.api_currency_rates IS 'Cleaned currency rates with inverse calculation';
COMMENT ON COLUMN silver.api_currency_rates.exchange_rate IS '1 BRL = X USD';
COMMENT ON COLUMN silver.api_currency_rates.rate_inverse IS '1 USD = X BRL (calculated)';

-- ----------------------------------------------------------------------------
-- Table 13: api_brazil_holidays
-- Description: Cleaned holidays with date components
-- Source: bronze.api_brazil_holidays
-- Records: ~50
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.api_brazil_holidays CASCADE;

CREATE TABLE silver.api_brazil_holidays (
    -- Primary Key
    holiday_date                    DATE PRIMARY KEY,

    -- Holiday details
    local_name                      VARCHAR(100) NOT NULL,
    holiday_name                    VARCHAR(100) NOT NULL,
    country_code                    VARCHAR(2) NOT NULL DEFAULT 'BR',
    is_fixed                        BOOLEAN DEFAULT FALSE,
    is_global                       BOOLEAN DEFAULT TRUE,
    holiday_types                   VARCHAR(100),

    -- DERIVED COLUMNS
    holiday_year                    INTEGER NOT NULL,
    holiday_month                   INTEGER NOT NULL,
    day_of_week                     INTEGER NOT NULL,
    is_weekend                      BOOLEAN DEFAULT FALSE,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.api_brazil_holidays',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT
);

COMMENT ON TABLE silver.api_brazil_holidays IS 'Cleaned Brazilian holidays with date components';
COMMENT ON COLUMN silver.api_brazil_holidays.day_of_week IS '0=Sunday, 6=Saturday';
COMMENT ON COLUMN silver.api_brazil_holidays.is_weekend IS 'TRUE if holiday falls on Saturday or Sunday';

-- ----------------------------------------------------------------------------
-- Table 14: api_weather_history
-- Description: Cleaned weather data with categorization
-- Source: bronze.api_weather_history
-- Records: ~21,330
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.api_weather_history CASCADE;

CREATE TABLE silver.api_weather_history (
    -- Location
    latitude                        DECIMAL(9,6) NOT NULL,
    longitude                       DECIMAL(9,6) NOT NULL,

    -- Composite Primary Key
    state_code                      VARCHAR(2) NOT NULL,
    weather_date                    DATE NOT NULL,

    -- Weather metrics (RENAMED for clarity)
    temperature_mean                DECIMAL(5,2),           -- Was: temperature_2m_mean
    temperature_max                 DECIMAL(5,2),           -- Was: temperature_2m_max
    precipitation_mm                DECIMAL(8,2) DEFAULT 0, -- Was: precipitation_sum
    weather_code                    INTEGER,

    -- DERIVED COLUMNS
    weather_category                VARCHAR(20),
    is_rainy                        BOOLEAN DEFAULT FALSE,
    is_extreme_heat                 BOOLEAN DEFAULT FALSE,

    -- Silver metadata
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.api_weather_history',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           TEXT,

    -- Composite PK constraint
    PRIMARY KEY (state_code, weather_date)
);

COMMENT ON TABLE silver.api_weather_history IS 'Cleaned weather data with category classification';
COMMENT ON COLUMN silver.api_weather_history.weather_category IS 'Derived from WMO code: clear, cloudy, rain, etc.';
COMMENT ON COLUMN silver.api_weather_history.is_extreme_heat IS 'TRUE if temperature_max > 35°C';

-- ============================================================================
-- SECTION 4: CREATE INDEXES FOR PERFORMANCE
-- ============================================================================

-- Orders indexes
CREATE INDEX idx_silver_orders_customer ON silver.olist_orders(customer_id);
CREATE INDEX idx_silver_orders_status ON silver.olist_orders(order_status);
CREATE INDEX idx_silver_orders_purchase_date ON silver.olist_orders(order_purchase_date);

-- Order items indexes
CREATE INDEX idx_silver_items_product ON silver.olist_order_items(product_id);
CREATE INDEX idx_silver_items_seller ON silver.olist_order_items(seller_id);

-- Reviews indexes
CREATE INDEX idx_silver_reviews_order ON silver.olist_order_reviews(order_id);
CREATE INDEX idx_silver_reviews_score ON silver.olist_order_reviews(review_score);

-- Customer indexes
CREATE INDEX idx_silver_customers_unique ON silver.olist_order_customers(customer_unique_id);
CREATE INDEX idx_silver_customers_state ON silver.olist_order_customers(customer_state);

-- Seller indexes
CREATE INDEX idx_silver_sellers_state ON silver.olist_sellers(seller_state);

-- Product indexes
CREATE INDEX idx_silver_products_category ON silver.olist_products(product_category_name);

-- Geolocation indexes
CREATE INDEX idx_silver_geo_state ON silver.olist_geolocation(state);

-- Closed deals indexes
CREATE INDEX idx_silver_deals_seller ON silver.olist_closed_deals(seller_id);

-- Weather indexes
CREATE INDEX idx_silver_weather_date ON silver.api_weather_history(weather_date);

-- ============================================================================
-- SECTION 5: VERIFICATION
-- ============================================================================

-- List all Silver tables created
SELECT
    schemaname,
    tablename,
    (SELECT COUNT(*) FROM information_schema.columns c
     WHERE c.table_schema = t.schemaname AND c.table_name = t.tablename) as column_count
FROM pg_tables t
WHERE schemaname = 'silver'
ORDER BY tablename;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'SILVER LAYER TABLES CREATED SUCCESSFULLY!';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Tables created (14 total):';
    RAISE NOTICE '';
    RAISE NOTICE 'E-Commerce (9):';
    RAISE NOTICE '  1.  silver.olist_orders                         - with delivery metrics';
    RAISE NOTICE '  2.  silver.olist_order_items                    - with item_total';
    RAISE NOTICE '  3.  silver.olist_order_payments                 - with single_payment flag';
    RAISE NOTICE '  4.  silver.olist_order_reviews                  - with sentiment flags';
    RAISE NOTICE '  5.  silver.olist_order_customers                - standardized location';
    RAISE NOTICE '  6.  silver.olist_sellers                        - standardized location';
    RAISE NOTICE '  7.  silver.olist_products                       - TYPOS FIXED + volume';
    RAISE NOTICE '  8.  silver.olist_category_translation';
    RAISE NOTICE '  9.  silver.olist_geolocation                    - DEDUPLICATED structure';
    RAISE NOTICE '';
    RAISE NOTICE 'Marketing Funnel (2):';
    RAISE NOTICE '  10. silver.olist_marketing_qualified_leads';
    RAISE NOTICE '  11. silver.olist_closed_deals                   - with has_seller_id flag';
    RAISE NOTICE '';
    RAISE NOTICE 'External APIs (3):';
    RAISE NOTICE '  12. silver.api_currency_rates                   - with inverse rate';
    RAISE NOTICE '  13. silver.api_brazil_holidays                  - with date components';
    RAISE NOTICE '  14. silver.api_weather_history                  - with weather category';
    RAISE NOTICE '';
    RAISE NOTICE 'Key differences from Bronze:';
    RAISE NOTICE '  ✓ Proper data types (not all VARCHAR)';
    RAISE NOTICE '  ✓ Primary Key constraints';
    RAISE NOTICE '  ✓ Fixed column names (typos corrected)';
    RAISE NOTICE '  ✓ Derived/calculated columns added';
    RAISE NOTICE '  ✓ Performance indexes created';
    RAISE NOTICE '';
    RAISE NOTICE 'NEXT STEP: Run load_silver_data.sql';
    RAISE NOTICE '============================================================';
END $$;
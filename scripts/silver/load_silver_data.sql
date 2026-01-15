-- ============================================================================
-- Description: Transform and load data from Bronze to Silver layer
-- ============================================================================
--
-- PURPOSE:
-- --------
-- Transforms Bronze layer data and loads into Silver layer with:
--   ✓ Type casting (VARCHAR → proper types)
--   ✓ NULL handling (empty strings → NULL)
--   ✓ Data cleaning (TRIM, LOWER, UPPER, INITCAP)
--   ✓ Column renaming (typo fixes)
--   ✓ Derived column calculations
--   ✓ Deduplication (geolocation)
--   ✓ Validation flags
--
-- PREREQUISITES:
-- --------------
-- 1. Bronze layer tables exist and are loaded
-- 2. Silver layer tables exist (run create_silver_tables.sql first)
--
-- USAGE:
-- ------
-- psql -d olist_dwh -f load_silver_data.sql
--
-- LOAD STRATEGY:
-- --------------
-- TRUNCATE then INSERT (full reload each time)
--
-- ============================================================================

\echo '============================================================'
\echo 'SILVER LAYER DATA TRANSFORMATION'
\echo '============================================================'
\echo ''

-- Set search path
SET search_path TO silver, bronze, public;

-- ============================================================================
-- SECTION 1: E-COMMERCE TABLES (9 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 1: olist_orders
-- Transformations: Type casts, derived delivery metrics
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_orders...'

TRUNCATE TABLE silver.olist_orders;

INSERT INTO silver.olist_orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_purchase_date,
    is_delivered,
    is_late_delivery,
    delivery_days_actual,
    delivery_days_estimated,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key
    order_id::VARCHAR(32),

    -- Foreign Key
    customer_id::VARCHAR(32),

    -- Status (cleaned)
    LOWER(TRIM(order_status)),

    -- Timestamps (cast with NULL handling)
    order_purchase_timestamp::TIMESTAMP,
    NULLIF(TRIM(order_approved_at), '')::TIMESTAMP,
    NULLIF(TRIM(order_delivered_carrier_date), '')::TIMESTAMP,
    NULLIF(TRIM(order_delivered_customer_date), '')::TIMESTAMP,
    NULLIF(TRIM(order_estimated_delivery_date), '')::TIMESTAMP,

    -- DERIVED: Purchase date (just the date part)
    DATE(order_purchase_timestamp::TIMESTAMP),

    -- DERIVED: Is delivered flag
    (LOWER(TRIM(order_status)) = 'delivered'),

    -- DERIVED: Is late delivery (delivered after estimated)
    CASE
        WHEN NULLIF(TRIM(order_delivered_customer_date), '') IS NOT NULL
         AND NULLIF(TRIM(order_estimated_delivery_date), '') IS NOT NULL
        THEN (NULLIF(TRIM(order_delivered_customer_date), '')::TIMESTAMP >
              NULLIF(TRIM(order_estimated_delivery_date), '')::TIMESTAMP)
    END,

    -- DERIVED: Actual delivery days
    CASE
        WHEN NULLIF(TRIM(order_delivered_customer_date), '') IS NOT NULL
        THEN EXTRACT(DAY FROM (
            NULLIF(TRIM(order_delivered_customer_date), '')::TIMESTAMP -
            order_purchase_timestamp::TIMESTAMP
        ))::INTEGER
    END,

    -- DERIVED: Estimated delivery days
    CASE
        WHEN NULLIF(TRIM(order_estimated_delivery_date), '') IS NOT NULL
        THEN EXTRACT(DAY FROM (
            NULLIF(TRIM(order_estimated_delivery_date), '')::TIMESTAMP -
            order_purchase_timestamp::TIMESTAMP
        ))::INTEGER
    END,

    -- Metadata
    'bronze.olist_orders',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL
FROM bronze.olist_orders
WHERE order_id IS NOT NULL
  AND TRIM(order_id) != '';

\echo '  ✓ olist_orders loaded'

-- Verify load
SELECT 'olist_orders' AS table_name, COUNT(*) AS row_count FROM silver.olist_orders;

-- ----------------------------------------------------------------------------
-- Table 2: olist_order_items
-- Transformations: Type casts, calculated item_total
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_order_items...'

TRUNCATE TABLE silver.olist_order_items;
INSERT INTO silver.olist_order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    item_total,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Composite Primary Key
    order_id::VARCHAR(32),
    order_item_id::INTEGER,

    -- Foreign Keys
    product_id::VARCHAR(32),
    seller_id::VARCHAR(32),

    -- Timestamp
    NULLIF(TRIM(shipping_limit_date), '')::TIMESTAMP,

    -- Money columns (handle empty strings, default to 0)
    COALESCE(NULLIF(TRIM(price), '')::DECIMAL(10,2), 0.00),
    COALESCE(NULLIF(TRIM(freight_value), '')::DECIMAL(10,2), 0.00),

    -- DERIVED: Item total
    COALESCE(NULLIF(TRIM(price), '')::DECIMAL(10,2), 0.00) +
    COALESCE(NULLIF(TRIM(freight_value), '')::DECIMAL(10,2), 0.00),

    -- Metadata
    'bronze.olist_order_items',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_order_items
WHERE order_id IS NOT NULL
  AND TRIM(order_id) != '';

\echo '  ✓ olist_order_items loaded'

-- Verify load
SELECT 'olist_order_items' AS table_name, COUNT(*) AS row_count FROM silver.olist_order_items;

-- ----------------------------------------------------------------------------
-- Table 3: olist_order_payments
-- Transformations: Type casts, single payment flag
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_order_payments...'

TRUNCATE TABLE silver.olist_order_payments;

INSERT INTO silver.olist_order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    is_single_payment,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Composite Primary Key
    order_id::VARCHAR(32),
    payment_sequential::INTEGER,

    -- Payment type (cleaned)
    LOWER(TRIM(payment_type)),
    COALESCE(NULLIF(TRIM(payment_installments),'')::INTEGER,1),
    COALESCE(NULLIF(TRIM(payment_value), '')::DECIMAL(10,2), 0.00),

    -- DERIVED: Is single payment flag
    COALESCE(NULLIF(TRIM(payment_installments), '')::INTEGER, 1) = 1

    -- Metadata
    , 'bronze.olist_order_payments',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_order_payments
WHERE order_id IS NOT NULL
  AND TRIM(order_id) != '';

\echo '  ✓ olist_order_payments loaded'

-- Verify load
SELECT 'olist_order_payments' AS table_name, COUNT(*) AS row_count FROM silver.olist_order_payments;

-- ----------------------------------------------------------------------------
-- Table 4: olist_order_reviews
-- Transformations: Type casts, sentiment flags, validation
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_order_reviews...'

TRUNCATE TABLE silver.olist_order_reviews;

INSERT INTO silver.olist_order_reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    has_comment,
    is_positive,
    is_negative,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT DISTINCT ON (review_id)
    -- Primary Key
    review_id::VARCHAR(36),

    -- Foreign Key
    order_id::VARCHAR(32),

    -- Review score (validated 1-5)
    CASE
        WHEN NULLIF(TRIM(review_score), '')::INTEGER BETWEEN 1 AND 5
        THEN NULLIF(TRIM(review_score), '')::INTEGER
    END,

    -- Text fields (NULL handling)
    NULLIF(TRIM(review_comment_title), ''),
    NULLIF(TRIM(review_comment_message), ''),

    -- Timestamps
    NULLIF(TRIM(review_creation_date), '')::TIMESTAMP,
    NULLIF(TRIM(review_answer_timestamp), '')::TIMESTAMP,

    -- DERIVED: Has comment
    (NULLIF(TRIM(review_comment_message), '') IS NOT NULL),

    -- DERIVED: Is positive (score >= 4)
    CASE
        WHEN NULLIF(TRIM(review_score), '')::INTEGER >= 4 THEN TRUE
        WHEN NULLIF(TRIM(review_score), '')::INTEGER < 4 THEN FALSE
    END,

    -- DERIVED: Is negative (score <= 2)
    CASE
        WHEN NULLIF(TRIM(review_score), '')::INTEGER <= 2 THEN TRUE
        WHEN NULLIF(TRIM(review_score), '')::INTEGER > 2 THEN FALSE
    END,

    -- Metadata
    'bronze.olist_order_reviews',
    CURRENT_TIMESTAMP,
    -- Validation: score must be 1-5
    (NULLIF(TRIM(review_score), '')::INTEGER BETWEEN 1 AND 5),
    CASE
        WHEN NOT (NULLIF(TRIM(review_score), '')::INTEGER BETWEEN 1 AND 5)
        THEN 'Invalid review_score: must be 1-5'
    END

FROM bronze.olist_order_reviews
WHERE review_id IS NOT NULL
  AND TRIM(review_id) != ''
ORDER BY review_id, NULLIF(TRIM(review_answer_timestamp), '')::TIMESTAMP DESC NULLS LAST;

\echo '  ✓ olist_order_reviews loaded'

-- ----------------------------------------------------------------------------
-- Table 5: olist_customers
-- Transformations: Type casts, location standardization
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_customers...'

TRUNCATE TABLE silver.olist_customers;

INSERT INTO silver.olist_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key
    customer_id::VARCHAR(32),

    -- Unique customer ID
    customer_unique_id::VARCHAR(32),

    -- Location (cleaned and standardized)
    LPAD(TRIM(customer_zip_code_prefix), 5, '0'),
    INITCAP(TRIM(customer_city)),
    UPPER(TRIM(customer_state)),

    -- Metadata
    'bronze.olist_customers',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_order_customer
WHERE customer_id IS NOT NULL
  AND TRIM(customer_id) != '';

\echo '  ✓ olist_customers loaded'

-- ----------------------------------------------------------------------------
-- Table 6: olist_sellers
-- Transformations: Type casts, location standardization
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_sellers...'

TRUNCATE TABLE silver.olist_sellers;

INSERT INTO silver.olist_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key
    seller_id::VARCHAR(32),

    -- Location (cleaned and standardized)
    LPAD(TRIM(seller_zip_code_prefix), 5, '0'),
    INITCAP(TRIM(seller_city)),
    UPPER(TRIM(seller_state)),

    -- Metadata
    'bronze.olist_sellers',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_sellers
WHERE seller_id IS NOT NULL
  AND TRIM(seller_id) != '';

\echo '  ✓ olist_sellers loaded'

-- ----------------------------------------------------------------------------
-- Table 7: olist_products
-- Transformations: Type casts, TYPO FIXES, calculated volume
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_products...'

TRUNCATE TABLE silver.olist_products;

INSERT INTO silver.olist_products (
    product_id,
    product_category_name,
    product_name_length,           -- RENAMED from product_name_lenght
    product_description_length,    -- RENAMED from product_description_lenght
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    has_dimensions,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key
    product_id::VARCHAR(32),

    -- Category (NULL handling - keep NULL, handle in Gold)
    NULLIF(TRIM(product_category_name), ''),

    -- Dimensions (TYPO FIXED: lenght → length)
    NULLIF(TRIM(product_name_lenght), '')::INTEGER,
    NULLIF(TRIM(product_description_lenght), '')::INTEGER,
    NULLIF(TRIM(product_photos_qty), '')::INTEGER,

    -- Physical measurements
    NULLIF(TRIM(product_weight_g), '')::DECIMAL(10,2),
    NULLIF(TRIM(product_length_cm), '')::DECIMAL(10,2),
    NULLIF(TRIM(product_height_cm), '')::DECIMAL(10,2),
    NULLIF(TRIM(product_width_cm), '')::DECIMAL(10,2),

    -- DERIVED: Volume (L × H × W)
    (NULLIF(TRIM(product_length_cm), '')::DECIMAL(10,2) *
     NULLIF(TRIM(product_height_cm), '')::DECIMAL(10,2) *
     NULLIF(TRIM(product_width_cm), '')::DECIMAL(10,2)),

    -- DERIVED: Has all dimensions
    (NULLIF(TRIM(product_length_cm), '') IS NOT NULL AND
     NULLIF(TRIM(product_height_cm), '') IS NOT NULL AND
     NULLIF(TRIM(product_width_cm), '') IS NOT NULL),

    -- Metadata
    'bronze.olist_products',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_products
WHERE product_id IS NOT NULL
  AND TRIM(product_id) != '';

\echo '  ✓ olist_products loaded (typos fixed!)'

-- ----------------------------------------------------------------------------
-- Table 8: olist_category_translation
-- Transformations: Type casts, lowercase standardization
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_category_translation...'

TRUNCATE TABLE silver.olist_category_translation;

INSERT INTO silver.olist_category_translation (
    product_category_name,
    product_category_name_english,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key (Portuguese - lowercase for consistent matching)
    LOWER(TRIM(product_category_name)),

    -- English translation (lowercase for consistency)
    LOWER(TRIM(product_category_name_english)),

    -- Metadata
    'bronze.olist_category_translation',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_category_translation
WHERE product_category_name IS NOT NULL
  AND TRIM(product_category_name) != '';

\echo '  ✓ olist_category_translation loaded'

-- ----------------------------------------------------------------------------
-- Table 9: olist_geolocation
-- Transformations: DEDUPLICATION, averaged coordinates, renamed columns
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_geolocation (deduplicating ~1M to ~19K)...'

TRUNCATE TABLE silver.olist_geolocation;

INSERT INTO silver.olist_geolocation (
    zip_code_prefix,
    latitude,
    longitude,
    city,
    state,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key (renamed, padded)
    LPAD(TRIM(geolocation_zip_code_prefix), 5, '0'),

    -- Averaged coordinates (deduplicated)
    ROUND(AVG(geolocation_lat::DECIMAL(9,6)), 6),
    ROUND(AVG(geolocation_lng::DECIMAL(9,6)), 6),

    -- City (most common value using MODE)
    MODE() WITHIN GROUP (ORDER BY INITCAP(TRIM(geolocation_city))),

    -- State (most common value using MODE)
    MODE() WITHIN GROUP (ORDER BY UPPER(TRIM(geolocation_state))),

    -- Metadata
    'bronze.olist_geolocation',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_geolocation
WHERE geolocation_zip_code_prefix IS NOT NULL
  AND TRIM(geolocation_zip_code_prefix) != ''
GROUP BY LPAD(TRIM(geolocation_zip_code_prefix), 5, '0');

\echo '  ✓ olist_geolocation loaded (deduplicated!)'

-- ============================================================================
-- SECTION 2: MARKETING FUNNEL TABLES (2 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 10: olist_marketing_qualified_leads
-- Transformations: Type casts, cleaned origin
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_marketing_qualified_leads...'

TRUNCATE TABLE silver.olist_marketing_qualified_leads;

INSERT INTO silver.olist_marketing_qualified_leads (
    mql_id,
    first_contact_date,
    landing_page_id,
    origin,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key
    mql_id::VARCHAR(32),

    -- Date
    first_contact_date::DATE,

    -- Landing page (NULL handling)
    NULLIF(TRIM(landing_page_id), ''),

    -- Origin (cleaned)
    LOWER(TRIM(origin)),

    -- Metadata
    'bronze.olist_marketing_qualified_leads',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_marketing_qualified_leads
WHERE mql_id IS NOT NULL
  AND TRIM(mql_id) != '';

\echo '  ✓ olist_marketing_qualified_leads loaded'

-- ----------------------------------------------------------------------------
-- Table 11: olist_closed_deals
-- Transformations: Type casts, boolean handling, cross-system flag
-- ----------------------------------------------------------------------------
\echo 'Loading silver.olist_closed_deals...'

TRUNCATE TABLE silver.olist_closed_deals;

INSERT INTO silver.olist_closed_deals (
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
    declared_monthly_revenue,
    has_seller_id,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key
    mql_id::VARCHAR(32),

    -- CRITICAL: Cross-system link to E-Commerce
    NULLIF(TRIM(seller_id), ''),

    -- Sales team IDs
    NULLIF(TRIM(sdr_id), ''),
    NULLIF(TRIM(sr_id), ''),

    -- Date
    won_date::DATE,

    -- Business info (cleaned)
    NULLIF(LOWER(TRIM(business_segment)), ''),
    LOWER(TRIM(lead_type)),
    NULLIF(LOWER(TRIM(lead_behaviour_profile)), ''),

    -- Boolean flags (handle various representations)
    CASE
        WHEN LOWER(TRIM(has_company)) IN ('true', '1', 'yes', 't') THEN TRUE
        WHEN LOWER(TRIM(has_company)) IN ('false', '0', 'no', 'f') THEN FALSE
    END,
    CASE
        WHEN LOWER(TRIM(has_gtin)) IN ('true', '1', 'yes', 't') THEN TRUE
        WHEN LOWER(TRIM(has_gtin)) IN ('false', '0', 'no', 'f') THEN FALSE
    END,

    -- Business details
    LOWER(TRIM(average_stock)),
    LOWER(TRIM(business_type)),

    -- Numeric fields (NULL handling)
    NULLIF(TRIM(declared_product_catalog_size), '')::DECIMAL(10,2),
    NULLIF(TRIM(declared_monthly_revenue), '')::DECIMAL(12,2),

    -- DERIVED: Does this deal link to E-Commerce?
    (NULLIF(TRIM(seller_id), '') IS NOT NULL),

    -- Metadata
    'bronze.olist_closed_deals',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.olist_closed_deals
WHERE mql_id IS NOT NULL
  AND TRIM(mql_id) != '';

\echo '  ✓ olist_closed_deals loaded'

-- ============================================================================
-- SECTION 3: API TABLES (3 tables)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 12: api_currency_rates
-- Transformations: Type casts, inverse rate calculation
-- ----------------------------------------------------------------------------
\echo 'Loading silver.api_currency_rates...'

TRUNCATE TABLE silver.api_currency_rates;

INSERT INTO silver.api_currency_rates (
    rate_date,
    base_currency,
    target_currency,
    exchange_rate,
    rate_inverse,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT DISTINCT ON (rate_date::DATE)
    -- Primary Key
    rate_date::DATE,

    -- Currency codes
    UPPER(TRIM(base_currency)),
    UPPER(TRIM(target_currency)),

    -- Exchange rate
    exchange_rate::DECIMAL(10,6),

    -- DERIVED: Inverse rate (1 USD = X BRL)
    ROUND(1.0 / exchange_rate::DECIMAL(10,6), 6),

    -- Metadata
    'bronze.api_currency_rates',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.api_currency_rates
WHERE rate_date IS NOT NULL
  AND TRIM(rate_date) != ''
ORDER BY rate_date::DATE;

\echo '  ✓ api_currency_rates loaded'

-- ----------------------------------------------------------------------------
-- Table 13: api_brazil_holidays
-- Transformations: Type casts, boolean handling, date components
-- ----------------------------------------------------------------------------
\echo 'Loading silver.api_brazil_holidays...'

TRUNCATE TABLE silver.api_brazil_holidays;

INSERT INTO silver.api_brazil_holidays (
    holiday_date,
    local_name,
    holiday_name,
    country_code,
    is_fixed,
    is_global,
    holiday_types,
    holiday_year,
    holiday_month,
    day_of_week,
    is_weekend,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Primary Key
    holiday_date::DATE,

    -- Names
    TRIM(local_name),
    TRIM(holiday_name),

    -- Country
    UPPER(TRIM(country_code)),

    -- Boolean flags
    CASE
        WHEN LOWER(TRIM(is_fixed)) IN ('true', '1', 'yes', 't') THEN TRUE
        ELSE FALSE
    END,
    CASE
        WHEN LOWER(TRIM(is_global)) IN ('true', '1', 'yes', 't') THEN TRUE
        ELSE FALSE
    END,

    -- Types
    TRIM(holiday_types),

    -- DERIVED: Date components
    EXTRACT(YEAR FROM holiday_date::DATE)::INTEGER,
    EXTRACT(MONTH FROM holiday_date::DATE)::INTEGER,
    EXTRACT(DOW FROM holiday_date::DATE)::INTEGER,  -- 0=Sunday, 6=Saturday
    (EXTRACT(DOW FROM holiday_date::DATE) IN (0, 6)),  -- Is weekend

    -- Metadata
    'bronze.api_brazil_holidays',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.api_brazil_holidays
WHERE holiday_date IS NOT NULL
  AND TRIM(holiday_date) != '';

\echo '  ✓ api_brazil_holidays loaded'

-- ----------------------------------------------------------------------------
-- Table 14: api_weather_history
-- Transformations: Type casts, renamed columns, weather category
-- ----------------------------------------------------------------------------
\echo 'Loading silver.api_weather_history...'

TRUNCATE TABLE silver.api_weather_history;

INSERT INTO silver.api_weather_history (
    latitude,
    longitude,
    state_code,
    weather_date,
    temperature_mean,
    temperature_max,
    precipitation_mm,
    weather_code,
    weather_category,
    is_rainy,
    is_extreme_heat,
    dwh_record_source,
    dwh_transformed_at,
    dwh_is_valid,
    dwh_validation_errors
)
SELECT
    -- Location
    latitude::DECIMAL(9,6),
    longitude::DECIMAL(9,6),

    -- Composite Primary Key
    UPPER(TRIM(state_code)),
    weather_date::DATE,

    -- Weather metrics (RENAMED)
    NULLIF(TRIM(temperature_2m_mean), '')::DECIMAL(5,2),
    NULLIF(TRIM(temperature_2m_max), '')::DECIMAL(5,2),
    COALESCE(NULLIF(TRIM(precipitation_sum), '')::DECIMAL(8,2), 0.00),
    NULLIF(TRIM(weather_code), '')::INTEGER,

    -- DERIVED: Weather category from WMO code
    CASE
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER = 0 THEN 'clear'
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER BETWEEN 1 AND 3 THEN 'cloudy'
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER BETWEEN 45 AND 48 THEN 'fog'
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER BETWEEN 51 AND 55 THEN 'drizzle'
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER BETWEEN 61 AND 65 THEN 'rain'
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER BETWEEN 71 AND 77 THEN 'snow'
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER BETWEEN 80 AND 82 THEN 'showers'
        WHEN NULLIF(TRIM(weather_code), '')::INTEGER BETWEEN 95 AND 99 THEN 'thunderstorm'
        ELSE 'unknown'
    END,

    -- DERIVED: Is rainy day
    (COALESCE(NULLIF(TRIM(precipitation_sum), '')::DECIMAL(8,2), 0.00) > 0),

    -- DERIVED: Extreme heat (> 35°C)
    (NULLIF(TRIM(temperature_2m_max), '')::DECIMAL(5,2) > 35),

    -- Metadata
    'bronze.api_weather_history',
    CURRENT_TIMESTAMP,
    TRUE,
    NULL

FROM bronze.api_weather_history
WHERE state_code IS NOT NULL
  AND TRIM(state_code) != ''
  AND weather_date IS NOT NULL
  AND TRIM(weather_date) != '';

\echo '  ✓ api_weather_history loaded'

-- ============================================================================
-- SECTION 4: VERIFICATION QUERIES
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'VERIFICATION: Record Counts'
\echo '============================================================'

-- Compare Bronze vs Silver counts
SELECT
    'olist_orders' as table_name,
    (SELECT COUNT(*) FROM bronze.olist_orders) as bronze_count,
    (SELECT COUNT(*) FROM silver.olist_orders) as silver_count
UNION ALL
SELECT 'olist_order_items',
    (SELECT COUNT(*) FROM bronze.olist_order_items),
    (SELECT COUNT(*) FROM silver.olist_order_items)
UNION ALL
SELECT 'olist_order_payments',
    (SELECT COUNT(*) FROM bronze.olist_order_payments),
    (SELECT COUNT(*) FROM silver.olist_order_payments)
UNION ALL
SELECT 'olist_order_reviews',
    (SELECT COUNT(*) FROM bronze.olist_order_reviews),
    (SELECT COUNT(*) FROM silver.olist_order_reviews)
UNION ALL
SELECT 'olist_customers',
    (SELECT COUNT(*) FROM bronze.olist_order_customer),
    (SELECT COUNT(*) FROM silver.olist_customers)
UNION ALL
SELECT 'olist_sellers',
    (SELECT COUNT(*) FROM bronze.olist_sellers),
    (SELECT COUNT(*) FROM silver.olist_sellers)
UNION ALL
SELECT 'olist_products',
    (SELECT COUNT(*) FROM bronze.olist_products),
    (SELECT COUNT(*) FROM silver.olist_products)
UNION ALL
SELECT 'olist_category_translation',
    (SELECT COUNT(*) FROM bronze.olist_category_translation),
    (SELECT COUNT(*) FROM silver.olist_category_translation)
UNION ALL
SELECT 'olist_geolocation (DEDUPLICATED!)',
    (SELECT COUNT(*) FROM bronze.olist_geolocation),
    (SELECT COUNT(*) FROM silver.olist_geolocation)
UNION ALL
SELECT 'olist_marketing_qualified_leads',
    (SELECT COUNT(*) FROM bronze.olist_marketing_qualified_leads),
    (SELECT COUNT(*) FROM silver.olist_marketing_qualified_leads)
UNION ALL
SELECT 'olist_closed_deals',
    (SELECT COUNT(*) FROM bronze.olist_closed_deals),
    (SELECT COUNT(*) FROM silver.olist_closed_deals)
UNION ALL
SELECT 'api_currency_rates',
    (SELECT COUNT(*) FROM bronze.api_currency_rates),
    (SELECT COUNT(*) FROM silver.api_currency_rates)
UNION ALL
SELECT 'api_brazil_holidays',
    (SELECT COUNT(*) FROM bronze.api_brazil_holidays),
    (SELECT COUNT(*) FROM silver.api_brazil_holidays)
UNION ALL
SELECT 'api_weather_history',
    (SELECT COUNT(*) FROM bronze.api_weather_history),
    (SELECT COUNT(*) FROM silver.api_weather_history)
ORDER BY table_name;

\echo ''
\echo '============================================================'
\echo 'VERIFICATION: Derived Columns Sample'
\echo '============================================================'

-- Sample derived columns in orders
\echo ''
\echo 'Sample: olist_orders derived columns'
SELECT
    order_id,
    order_status,
    order_purchase_date,
    is_delivered,
    is_late_delivery,
    delivery_days_actual,
    delivery_days_estimated
FROM silver.olist_orders
WHERE is_delivered = TRUE
LIMIT 5;

-- Sample weather categories
\echo ''
\echo 'Sample: Weather categories distribution'
SELECT
    weather_category,
    COUNT(*) as days,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
FROM silver.api_weather_history
GROUP BY weather_category
ORDER BY days DESC;

-- Geolocation deduplication result
\echo ''
\echo 'Geolocation deduplication result:'
SELECT
    'bronze.olist_geolocation' as layer,
    COUNT(*) as record_count
FROM bronze.olist_geolocation
UNION ALL
SELECT
    'silver.olist_geolocation (deduplicated)',
    COUNT(*)
FROM silver.olist_geolocation;

-- ============================================================================
-- SECTION 5: DATA QUALITY CHECKS
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'DATA QUALITY: Validation Failures'
\echo '============================================================'

-- Check for invalid records
SELECT
    'olist_order_reviews' as table_name,
    SUM(CASE WHEN dwh_is_valid THEN 1 ELSE 0 END) as valid_count,
    SUM(CASE WHEN NOT dwh_is_valid THEN 1 ELSE 0 END) as invalid_count
FROM silver.olist_order_reviews;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SILVER LAYER LOAD COMPLETE!'
\echo '============================================================'
\echo ''
\echo 'Transformations applied:'
\echo '  ✓ Type casting (VARCHAR → proper types)'
\echo '  ✓ NULL handling (empty strings → NULL)'
\echo '  ✓ Data cleaning (TRIM, LOWER, UPPER, INITCAP)'
\echo '  ✓ Column renaming (typos fixed in products)'
\echo '  ✓ Derived columns calculated'
\echo '  ✓ Geolocation deduplicated (~1M → ~19K)'
\echo '  ✓ Validation flags set'
\echo ''
\echo 'NEXT STEP: Create Gold layer dimensional model'
\echo '============================================================'
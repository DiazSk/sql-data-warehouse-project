/*
================================================================================
SNOWFLAKE SILVER LAYER: Create Tables & Transform Data from Bronze
================================================================================

PURPOSE:
  Migrated from PostgreSQL → Snowflake.
  Creates Silver layer tables with proper data types, then transforms and loads
  data from Bronze layer.

POSTGRESQL → SNOWFLAKE CHANGES:
  - DISTINCT ON (col)        → ROW_NUMBER() window function
  - MODE() WITHIN GROUP      → Subquery with ROW_NUMBER()
  - EXTRACT(DOW FROM date)   → DAYOFWEEK(date) — returns 0=Sun..6=Sat in Snowflake
  - EXTRACT(DAY FROM (t1-t2))→ DATEDIFF(DAY, t1, t2)
  - DATE(timestamp)          → timestamp::DATE (both work, latter is explicit)
  - CREATE INDEX             → Removed (Snowflake auto-manages micro-partitions)
  - BOOLEAN expressions      → Explicit CASE WHEN (Snowflake doesn't allow bare
                                comparison expressions in SELECT for booleans)
  - TEXT type                → VARCHAR
  - SET search_path          → Fully qualified schema.table names
  - \echo, DO $$ $$          → Removed (psql-specific)

================================================================================
*/

USE DATABASE OLIST_DWH;
USE SCHEMA SILVER;
USE WAREHOUSE OLIST_WH;

-- ============================================================================
-- SECTION 1: CREATE SILVER TABLES
-- ============================================================================

-- Table 1: Orders
CREATE OR REPLACE TABLE OLIST_ORDERS (
    order_id                        VARCHAR(32) PRIMARY KEY,
    customer_id                     VARCHAR(32) NOT NULL,
    order_status                    VARCHAR(20) NOT NULL,
    order_purchase_timestamp        TIMESTAMP NOT NULL,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    order_purchase_date             DATE NOT NULL,
    is_delivered                    BOOLEAN DEFAULT FALSE,
    is_late_delivery                BOOLEAN,
    delivery_days_actual            INTEGER,
    delivery_days_estimated         INTEGER,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_orders',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_ORDERS IS 'Cleaned orders with proper types and delivery metrics';

-- Table 2: Order Items
CREATE OR REPLACE TABLE OLIST_ORDER_ITEMS (
    order_id                        VARCHAR(32) NOT NULL,
    order_item_id                   INTEGER NOT NULL,
    product_id                      VARCHAR(32) NOT NULL,
    seller_id                       VARCHAR(32) NOT NULL,
    shipping_limit_date             TIMESTAMP,
    price                           DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    freight_value                   DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    item_total                      DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_order_items',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR,
    PRIMARY KEY (order_id, order_item_id)
);
COMMENT ON TABLE OLIST_ORDER_ITEMS IS 'Cleaned order line items with item_total calculated';

-- Table 3: Order Payments
CREATE OR REPLACE TABLE OLIST_ORDER_PAYMENTS (
    order_id                        VARCHAR(32) NOT NULL,
    payment_sequential              INTEGER NOT NULL,
    payment_type                    VARCHAR(20) NOT NULL,
    payment_installments            INTEGER NOT NULL DEFAULT 1,
    payment_value                   DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    is_single_payment               BOOLEAN DEFAULT TRUE,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_order_payments',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR,
    PRIMARY KEY (order_id, payment_sequential)
);
COMMENT ON TABLE OLIST_ORDER_PAYMENTS IS 'Cleaned payment records';

-- Table 4: Order Reviews
CREATE OR REPLACE TABLE OLIST_ORDER_REVIEWS (
    review_id                       VARCHAR(36) PRIMARY KEY,
    order_id                        VARCHAR(32) NOT NULL,
    review_score                    INTEGER,
    review_comment_title            VARCHAR(500),
    review_comment_message          VARCHAR,
    review_creation_date            TIMESTAMP,
    review_answer_timestamp         TIMESTAMP,
    has_comment                     BOOLEAN DEFAULT FALSE,
    is_positive                     BOOLEAN,
    is_negative                     BOOLEAN,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_order_reviews',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_ORDER_REVIEWS IS 'Cleaned reviews with sentiment classification';

-- Table 5: Customers
CREATE OR REPLACE TABLE OLIST_CUSTOMERS (
    customer_id                     VARCHAR(32) PRIMARY KEY,
    customer_unique_id              VARCHAR(32) NOT NULL,
    customer_zip_code_prefix        VARCHAR(5) NOT NULL,
    customer_city                   VARCHAR(100),
    customer_state                  VARCHAR(2) NOT NULL,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_customers',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_CUSTOMERS IS 'Cleaned customers with standardized location';

-- Table 6: Sellers
CREATE OR REPLACE TABLE OLIST_SELLERS (
    seller_id                       VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix          VARCHAR(5) NOT NULL,
    seller_city                     VARCHAR(100),
    seller_state                    VARCHAR(2) NOT NULL,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_sellers',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_SELLERS IS 'Cleaned sellers with standardized location';

-- Table 7: Products
CREATE OR REPLACE TABLE OLIST_PRODUCTS (
    product_id                      VARCHAR(32) PRIMARY KEY,
    product_category_name           VARCHAR(100),
    product_name_length             INTEGER,
    product_description_length      INTEGER,
    product_photos_qty              INTEGER,
    product_weight_g                DECIMAL(10,2),
    product_length_cm               DECIMAL(10,2),
    product_height_cm               DECIMAL(10,2),
    product_width_cm                DECIMAL(10,2),
    product_volume_cm3              DECIMAL(12,2),
    has_dimensions                  BOOLEAN DEFAULT FALSE,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_products',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_PRODUCTS IS 'Cleaned products with fixed typos and calculated volume';

-- Table 8: Category Translation
CREATE OR REPLACE TABLE OLIST_CATEGORY_TRANSLATION (
    product_category_name           VARCHAR(100) PRIMARY KEY,
    product_category_name_english   VARCHAR(100) NOT NULL,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_category_translation',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_CATEGORY_TRANSLATION IS 'Category lookup: Portuguese to English';

-- Table 9: Geolocation (deduplicated)
CREATE OR REPLACE TABLE OLIST_GEOLOCATION (
    zip_code_prefix                 VARCHAR(5) PRIMARY KEY,
    latitude                        DECIMAL(9,6) NOT NULL,
    longitude                       DECIMAL(9,6) NOT NULL,
    city                            VARCHAR(100),
    state                           VARCHAR(2) NOT NULL,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_geolocation',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_GEOLOCATION IS 'Deduplicated geolocation with averaged coordinates per zip';

-- Table 10: Marketing Qualified Leads
CREATE OR REPLACE TABLE OLIST_MQL (
    mql_id                          VARCHAR(32) PRIMARY KEY,
    first_contact_date              DATE NOT NULL,
    landing_page_id                 VARCHAR(32),
    origin                          VARCHAR(50),
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_marketing_qualified_leads',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_MQL IS 'Cleaned Marketing Qualified Leads';

-- Table 11: Closed Deals
CREATE OR REPLACE TABLE OLIST_CLOSED_DEALS (
    mql_id                          VARCHAR(32) PRIMARY KEY,
    seller_id                       VARCHAR(32),
    sdr_id                          VARCHAR(32),
    sr_id                           VARCHAR(32),
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
    has_seller_id                   BOOLEAN DEFAULT FALSE,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_closed_deals',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE OLIST_CLOSED_DEALS IS 'Cleaned closed deals with cross-system link';

-- Table 12: Currency Rates
CREATE OR REPLACE TABLE API_CURRENCY_RATES (
    rate_date                       DATE PRIMARY KEY,
    base_currency                   VARCHAR(3) NOT NULL DEFAULT 'BRL',
    target_currency                 VARCHAR(3) NOT NULL DEFAULT 'USD',
    exchange_rate                   DECIMAL(10,6) NOT NULL,
    rate_inverse                    DECIMAL(10,6),
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_currency_rates',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE API_CURRENCY_RATES IS 'Cleaned currency rates with inverse calculation';

-- Table 13: Brazil Holidays
CREATE OR REPLACE TABLE API_BRAZIL_HOLIDAYS (
    holiday_date                    DATE PRIMARY KEY,
    local_name                      VARCHAR(100) NOT NULL,
    holiday_name                    VARCHAR(100) NOT NULL,
    country_code                    VARCHAR(2) NOT NULL DEFAULT 'BR',
    is_fixed                        BOOLEAN DEFAULT FALSE,
    is_global                       BOOLEAN DEFAULT TRUE,
    holiday_types                   VARCHAR(100),
    holiday_year                    INTEGER NOT NULL,
    holiday_month                   INTEGER NOT NULL,
    day_of_week                     INTEGER NOT NULL,
    is_weekend                      BOOLEAN DEFAULT FALSE,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_brazil_holidays',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR
);
COMMENT ON TABLE API_BRAZIL_HOLIDAYS IS 'Cleaned Brazilian holidays with date components';

-- Table 14: Weather History
CREATE OR REPLACE TABLE API_WEATHER_HISTORY (
    latitude                        DECIMAL(9,6) NOT NULL,
    longitude                       DECIMAL(9,6) NOT NULL,
    state_code                      VARCHAR(2) NOT NULL,
    weather_date                    DATE NOT NULL,
    temperature_mean                DECIMAL(5,2),
    temperature_max                 DECIMAL(5,2),
    precipitation_mm                DECIMAL(8,2) DEFAULT 0,
    weather_code                    INTEGER,
    weather_category                VARCHAR(20),
    is_rainy                        BOOLEAN DEFAULT FALSE,
    is_extreme_heat                 BOOLEAN DEFAULT FALSE,
    dwh_record_source               VARCHAR(100) DEFAULT 'bronze.raw_weather_history',
    dwh_transformed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    dwh_is_valid                    BOOLEAN DEFAULT TRUE,
    dwh_validation_errors           VARCHAR,
    PRIMARY KEY (state_code, weather_date)
);
COMMENT ON TABLE API_WEATHER_HISTORY IS 'Cleaned weather data with category classification';

-- ============================================================================
-- SECTION 2: TRANSFORM & LOAD — E-COMMERCE TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 1: Orders
-- SNOWFLAKE CHANGE: DATEDIFF() instead of EXTRACT(DAY FROM (ts1-ts2))
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_ORDERS (
    order_id, customer_id, order_status,
    order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date, order_delivered_customer_date,
    order_estimated_delivery_date, order_purchase_date,
    is_delivered, is_late_delivery, delivery_days_actual, delivery_days_estimated,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    order_id::VARCHAR(32),
    customer_id::VARCHAR(32),
    LOWER(TRIM(order_status)),

    -- Timestamps
    order_purchase_timestamp::TIMESTAMP,
    NULLIF(TRIM(order_approved_at), '')::TIMESTAMP,
    NULLIF(TRIM(order_delivered_carrier_date), '')::TIMESTAMP,
    NULLIF(TRIM(order_delivered_customer_date), '')::TIMESTAMP,
    NULLIF(TRIM(order_estimated_delivery_date), '')::TIMESTAMP,

    -- DERIVED: Purchase date
    order_purchase_timestamp::TIMESTAMP::DATE,

    -- DERIVED: Is delivered
    CASE WHEN LOWER(TRIM(order_status)) = 'delivered' THEN TRUE ELSE FALSE END,

    -- DERIVED: Is late delivery
    -- SNOWFLAKE CHANGE: direct timestamp comparison (same as PG)
    CASE
        WHEN NULLIF(TRIM(order_delivered_customer_date), '') IS NOT NULL
         AND NULLIF(TRIM(order_estimated_delivery_date), '') IS NOT NULL
        THEN NULLIF(TRIM(order_delivered_customer_date), '')::TIMESTAMP >
             NULLIF(TRIM(order_estimated_delivery_date), '')::TIMESTAMP
    END,

    -- DERIVED: Actual delivery days
    -- SNOWFLAKE CHANGE: DATEDIFF replaces EXTRACT(DAY FROM (ts1 - ts2))
    CASE
        WHEN NULLIF(TRIM(order_delivered_customer_date), '') IS NOT NULL
        THEN DATEDIFF(DAY,
            order_purchase_timestamp::TIMESTAMP,
            NULLIF(TRIM(order_delivered_customer_date), '')::TIMESTAMP
        )
    END,

    -- DERIVED: Estimated delivery days
    CASE
        WHEN NULLIF(TRIM(order_estimated_delivery_date), '') IS NOT NULL
        THEN DATEDIFF(DAY,
            order_purchase_timestamp::TIMESTAMP,
            NULLIF(TRIM(order_estimated_delivery_date), '')::TIMESTAMP
        )
    END,

    -- Metadata
    'bronze.raw_orders',
    CURRENT_TIMESTAMP(),
    TRUE,
    NULL

FROM BRONZE.RAW_ORDERS
WHERE order_id IS NOT NULL
  AND TRIM(order_id) != '';

-- ----------------------------------------------------------------------------
-- Table 2: Order Items
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_ORDER_ITEMS (
    order_id, order_item_id, product_id, seller_id,
    shipping_limit_date, price, freight_value, item_total,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    order_id::VARCHAR(32),
    order_item_id::INTEGER,
    product_id::VARCHAR(32),
    seller_id::VARCHAR(32),
    NULLIF(TRIM(shipping_limit_date), '')::TIMESTAMP,
    COALESCE(NULLIF(TRIM(price), '')::DECIMAL(10,2), 0.00),
    COALESCE(NULLIF(TRIM(freight_value), '')::DECIMAL(10,2), 0.00),
    COALESCE(NULLIF(TRIM(price), '')::DECIMAL(10,2), 0.00) +
    COALESCE(NULLIF(TRIM(freight_value), '')::DECIMAL(10,2), 0.00),
    'bronze.raw_order_items', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_ORDER_ITEMS
WHERE order_id IS NOT NULL AND TRIM(order_id) != '';

-- ----------------------------------------------------------------------------
-- Table 3: Order Payments
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_ORDER_PAYMENTS (
    order_id, payment_sequential, payment_type,
    payment_installments, payment_value, is_single_payment,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    order_id::VARCHAR(32),
    payment_sequential::INTEGER,
    LOWER(TRIM(payment_type)),
    COALESCE(NULLIF(TRIM(payment_installments), '')::INTEGER, 1),
    COALESCE(NULLIF(TRIM(payment_value), '')::DECIMAL(10,2), 0.00),
    CASE WHEN COALESCE(NULLIF(TRIM(payment_installments), '')::INTEGER, 1) = 1 THEN TRUE ELSE FALSE END,
    'bronze.raw_order_payments', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_ORDER_PAYMENTS
WHERE order_id IS NOT NULL AND TRIM(order_id) != '';

-- ----------------------------------------------------------------------------
-- Table 4: Order Reviews
-- SNOWFLAKE CHANGE: DISTINCT ON → ROW_NUMBER() window function
-- This is a KEY difference. PostgreSQL's DISTINCT ON picks one row per group
-- based on ORDER BY. Snowflake requires a window function approach.
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_ORDER_REVIEWS (
    review_id, order_id, review_score,
    review_comment_title, review_comment_message,
    review_creation_date, review_answer_timestamp,
    has_comment, is_positive, is_negative,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    review_id, order_id, review_score,
    review_comment_title, review_comment_message,
    review_creation_date, review_answer_timestamp,
    has_comment, is_positive, is_negative,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
FROM (
    SELECT
        review_id::VARCHAR(36) AS review_id,
        order_id::VARCHAR(32) AS order_id,
        CASE
            WHEN NULLIF(TRIM(review_score), '')::INTEGER BETWEEN 1 AND 5
            THEN NULLIF(TRIM(review_score), '')::INTEGER
        END AS review_score,
        NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
        NULLIF(TRIM(review_comment_message), '') AS review_comment_message,
        NULLIF(TRIM(review_creation_date), '')::TIMESTAMP AS review_creation_date,
        NULLIF(TRIM(review_answer_timestamp), '')::TIMESTAMP AS review_answer_timestamp,
        -- DERIVED: Has comment
        CASE WHEN NULLIF(TRIM(review_comment_message), '') IS NOT NULL THEN TRUE ELSE FALSE END AS has_comment,
        -- DERIVED: Is positive (score >= 4)
        CASE
            WHEN NULLIF(TRIM(review_score), '')::INTEGER >= 4 THEN TRUE
            WHEN NULLIF(TRIM(review_score), '')::INTEGER < 4 THEN FALSE
        END AS is_positive,
        -- DERIVED: Is negative (score <= 2)
        CASE
            WHEN NULLIF(TRIM(review_score), '')::INTEGER <= 2 THEN TRUE
            WHEN NULLIF(TRIM(review_score), '')::INTEGER > 2 THEN FALSE
        END AS is_negative,
        'bronze.raw_order_reviews' AS dwh_record_source,
        CURRENT_TIMESTAMP() AS dwh_transformed_at,
        (NULLIF(TRIM(review_score), '')::INTEGER BETWEEN 1 AND 5) AS dwh_is_valid,
        CASE
            WHEN NOT (NULLIF(TRIM(review_score), '')::INTEGER BETWEEN 1 AND 5)
            THEN 'Invalid review_score: must be 1-5'
        END AS dwh_validation_errors,
        -- SNOWFLAKE CHANGE: ROW_NUMBER replaces DISTINCT ON
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY NULLIF(TRIM(review_answer_timestamp), '')::TIMESTAMP DESC NULLS LAST
        ) AS rn
    FROM BRONZE.RAW_ORDER_REVIEWS
    WHERE review_id IS NOT NULL AND TRIM(review_id) != ''
) deduped
WHERE rn = 1;

-- ----------------------------------------------------------------------------
-- Table 5: Customers
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_CUSTOMERS (
    customer_id, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    customer_id::VARCHAR(32),
    customer_unique_id::VARCHAR(32),
    LPAD(TRIM(customer_zip_code_prefix), 5, '0'),
    INITCAP(TRIM(customer_city)),
    UPPER(TRIM(customer_state)),
    'bronze.raw_customers', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_CUSTOMERS
WHERE customer_id IS NOT NULL AND TRIM(customer_id) != '';

-- ----------------------------------------------------------------------------
-- Table 6: Sellers
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_SELLERS (
    seller_id, seller_zip_code_prefix, seller_city, seller_state,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    seller_id::VARCHAR(32),
    LPAD(TRIM(seller_zip_code_prefix), 5, '0'),
    INITCAP(TRIM(seller_city)),
    UPPER(TRIM(seller_state)),
    'bronze.raw_sellers', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_SELLERS
WHERE seller_id IS NOT NULL AND TRIM(seller_id) != '';

-- ----------------------------------------------------------------------------
-- Table 7: Products (TYPO FIXES: lenght → length)
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_PRODUCTS (
    product_id, product_category_name,
    product_name_length, product_description_length, product_photos_qty,
    product_weight_g, product_length_cm, product_height_cm, product_width_cm,
    product_volume_cm3, has_dimensions,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    product_id::VARCHAR(32),
    NULLIF(TRIM(product_category_name), ''),
    NULLIF(TRIM(product_name_lenght), '')::INTEGER,
    NULLIF(TRIM(product_description_lenght), '')::INTEGER,
    NULLIF(TRIM(product_photos_qty), '')::INTEGER,
    NULLIF(TRIM(product_weight_g), '')::DECIMAL(10,2),
    NULLIF(TRIM(product_length_cm), '')::DECIMAL(10,2),
    NULLIF(TRIM(product_height_cm), '')::DECIMAL(10,2),
    NULLIF(TRIM(product_width_cm), '')::DECIMAL(10,2),
    -- DERIVED: Volume
    (NULLIF(TRIM(product_length_cm), '')::DECIMAL(10,2) *
     NULLIF(TRIM(product_height_cm), '')::DECIMAL(10,2) *
     NULLIF(TRIM(product_width_cm), '')::DECIMAL(10,2)),
    -- DERIVED: Has dimensions
    CASE
        WHEN NULLIF(TRIM(product_length_cm), '') IS NOT NULL
         AND NULLIF(TRIM(product_height_cm), '') IS NOT NULL
         AND NULLIF(TRIM(product_width_cm), '') IS NOT NULL
        THEN TRUE ELSE FALSE
    END,
    'bronze.raw_products', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_PRODUCTS
WHERE product_id IS NOT NULL AND TRIM(product_id) != '';

-- ----------------------------------------------------------------------------
-- Table 8: Category Translation
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_CATEGORY_TRANSLATION (
    product_category_name, product_category_name_english,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    LOWER(TRIM(product_category_name)),
    LOWER(TRIM(product_category_name_english)),
    'bronze.raw_category_translation', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_CATEGORY_TRANSLATION
WHERE product_category_name IS NOT NULL AND TRIM(product_category_name) != '';

-- ----------------------------------------------------------------------------
-- Table 9: Geolocation (DEDUPLICATION)
-- SNOWFLAKE CHANGE: MODE() WITHIN GROUP → subquery with ROW_NUMBER()
-- PostgreSQL's MODE() aggregate finds the most frequent value.
-- Snowflake doesn't have MODE(), so we use a ranked subquery instead.
-- ----------------------------------------------------------------------------
INSERT INTO SILVER.OLIST_GEOLOCATION (
    zip_code_prefix, latitude, longitude, city, state,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    g.zip_code_prefix,
    g.latitude,
    g.longitude,
    city_mode.city,
    state_mode.state,
    'bronze.raw_geolocation', CURRENT_TIMESTAMP(), TRUE, NULL
FROM (
    -- Average coordinates per zip
    SELECT
        LPAD(TRIM(geolocation_zip_code_prefix), 5, '0') AS zip_code_prefix,
        ROUND(AVG(geolocation_lat::DECIMAL(9,6)), 6) AS latitude,
        ROUND(AVG(geolocation_lng::DECIMAL(9,6)), 6) AS longitude
    FROM BRONZE.RAW_GEOLOCATION
    WHERE geolocation_zip_code_prefix IS NOT NULL
      AND TRIM(geolocation_zip_code_prefix) != ''
    GROUP BY LPAD(TRIM(geolocation_zip_code_prefix), 5, '0')
) g
LEFT JOIN (
    -- Most common city per zip (replaces MODE())
    SELECT zip_code_prefix, city
    FROM (
        SELECT
            LPAD(TRIM(geolocation_zip_code_prefix), 5, '0') AS zip_code_prefix,
            INITCAP(TRIM(geolocation_city)) AS city,
            ROW_NUMBER() OVER (
                PARTITION BY LPAD(TRIM(geolocation_zip_code_prefix), 5, '0')
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM BRONZE.RAW_GEOLOCATION
        WHERE geolocation_zip_code_prefix IS NOT NULL
          AND TRIM(geolocation_zip_code_prefix) != ''
        GROUP BY LPAD(TRIM(geolocation_zip_code_prefix), 5, '0'),
                 INITCAP(TRIM(geolocation_city))
    )
    WHERE rn = 1
) city_mode ON g.zip_code_prefix = city_mode.zip_code_prefix
LEFT JOIN (
    -- Most common state per zip (replaces MODE())
    SELECT zip_code_prefix, state
    FROM (
        SELECT
            LPAD(TRIM(geolocation_zip_code_prefix), 5, '0') AS zip_code_prefix,
            UPPER(TRIM(geolocation_state)) AS state,
            ROW_NUMBER() OVER (
                PARTITION BY LPAD(TRIM(geolocation_zip_code_prefix), 5, '0')
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM BRONZE.RAW_GEOLOCATION
        WHERE geolocation_zip_code_prefix IS NOT NULL
          AND TRIM(geolocation_zip_code_prefix) != ''
        GROUP BY LPAD(TRIM(geolocation_zip_code_prefix), 5, '0'),
                 UPPER(TRIM(geolocation_state))
    )
    WHERE rn = 1
) state_mode ON g.zip_code_prefix = state_mode.zip_code_prefix;

-- ============================================================================
-- SECTION 3: TRANSFORM & LOAD — MARKETING FUNNEL
-- ============================================================================

-- Table 10: MQL
INSERT INTO SILVER.OLIST_MQL (
    mql_id, first_contact_date, landing_page_id, origin,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    mql_id::VARCHAR(32),
    first_contact_date::DATE,
    NULLIF(TRIM(landing_page_id), ''),
    LOWER(TRIM(origin)),
    'bronze.raw_marketing_qualified_leads', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_MARKETING_QUALIFIED_LEADS
WHERE mql_id IS NOT NULL AND TRIM(mql_id) != '';

-- Table 11: Closed Deals
INSERT INTO SILVER.OLIST_CLOSED_DEALS (
    mql_id, seller_id, sdr_id, sr_id, won_date,
    business_segment, lead_type, lead_behaviour_profile,
    has_company, has_gtin, average_stock, business_type,
    declared_product_catalog_size, declared_monthly_revenue, has_seller_id,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    mql_id::VARCHAR(32),
    NULLIF(TRIM(seller_id), ''),
    NULLIF(TRIM(sdr_id), ''),
    NULLIF(TRIM(sr_id), ''),
    won_date::DATE,
    NULLIF(LOWER(TRIM(business_segment)), ''),
    LOWER(TRIM(lead_type)),
    NULLIF(LOWER(TRIM(lead_behaviour_profile)), ''),
    CASE WHEN LOWER(TRIM(has_company)) IN ('true', '1', 'yes', 't') THEN TRUE
         WHEN LOWER(TRIM(has_company)) IN ('false', '0', 'no', 'f') THEN FALSE END,
    CASE WHEN LOWER(TRIM(has_gtin)) IN ('true', '1', 'yes', 't') THEN TRUE
         WHEN LOWER(TRIM(has_gtin)) IN ('false', '0', 'no', 'f') THEN FALSE END,
    LOWER(TRIM(average_stock)),
    LOWER(TRIM(business_type)),
    NULLIF(TRIM(declared_product_catalog_size), '')::DECIMAL(10,2),
    NULLIF(TRIM(declared_monthly_revenue), '')::DECIMAL(12,2),
    CASE WHEN NULLIF(TRIM(seller_id), '') IS NOT NULL THEN TRUE ELSE FALSE END,
    'bronze.raw_closed_deals', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_CLOSED_DEALS
WHERE mql_id IS NOT NULL AND TRIM(mql_id) != '';

-- ============================================================================
-- SECTION 4: TRANSFORM & LOAD — API TABLES
-- (These will have 0 rows since API data isn't loaded yet in Bronze.
--  The INSERT statements are here for completeness — they'll work once
--  Bronze API tables are populated.)
-- ============================================================================

-- Table 12: Currency Rates
-- SNOWFLAKE CHANGE: DISTINCT ON → ROW_NUMBER()
INSERT INTO SILVER.API_CURRENCY_RATES (
    rate_date, base_currency, target_currency, exchange_rate, rate_inverse,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT rate_date, base_currency, target_currency, exchange_rate, rate_inverse,
       dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
FROM (
    SELECT
        rate_date::DATE AS rate_date,
        UPPER(TRIM(base_currency)) AS base_currency,
        UPPER(TRIM(target_currency)) AS target_currency,
        exchange_rate::DECIMAL(10,6) AS exchange_rate,
        ROUND(1.0 / exchange_rate::DECIMAL(10,6), 6) AS rate_inverse,
        'bronze.raw_currency_rates' AS dwh_record_source,
        CURRENT_TIMESTAMP() AS dwh_transformed_at,
        TRUE AS dwh_is_valid,
        NULL AS dwh_validation_errors,
        ROW_NUMBER() OVER (PARTITION BY rate_date::DATE ORDER BY rate_date::DATE) AS rn
    FROM BRONZE.RAW_CURRENCY_RATES
    WHERE rate_date IS NOT NULL AND TRIM(rate_date) != ''
)
WHERE rn = 1;

-- Table 13: Brazil Holidays
-- SNOWFLAKE CHANGE: DAYOFWEEK() replaces EXTRACT(DOW FROM date)
-- Snowflake DAYOFWEEK: 0=Sun, 1=Mon, ..., 6=Sat (matches PG DOW)
INSERT INTO SILVER.API_BRAZIL_HOLIDAYS (
    holiday_date, local_name, holiday_name, country_code,
    is_fixed, is_global, holiday_types,
    holiday_year, holiday_month, day_of_week, is_weekend,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    holiday_date::DATE,
    TRIM(local_name),
    TRIM(holiday_name),
    UPPER(TRIM(country_code)),
    CASE WHEN LOWER(TRIM(is_fixed)) IN ('true', '1', 'yes', 't') THEN TRUE ELSE FALSE END,
    CASE WHEN LOWER(TRIM(is_global)) IN ('true', '1', 'yes', 't') THEN TRUE ELSE FALSE END,
    TRIM(holiday_types),
    -- DERIVED: Date components
    EXTRACT(YEAR FROM holiday_date::DATE)::INTEGER,
    EXTRACT(MONTH FROM holiday_date::DATE)::INTEGER,
    DAYOFWEEK(holiday_date::DATE),
    CASE WHEN DAYOFWEEK(holiday_date::DATE) IN (0, 6) THEN TRUE ELSE FALSE END,
    'bronze.raw_brazil_holidays', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_BRAZIL_HOLIDAYS
WHERE holiday_date IS NOT NULL AND TRIM(holiday_date) != '';

-- Table 14: Weather History
INSERT INTO SILVER.API_WEATHER_HISTORY (
    latitude, longitude, state_code, weather_date,
    temperature_mean, temperature_max, precipitation_mm, weather_code,
    weather_category, is_rainy, is_extreme_heat,
    dwh_record_source, dwh_transformed_at, dwh_is_valid, dwh_validation_errors
)
SELECT
    latitude::DECIMAL(9,6),
    longitude::DECIMAL(9,6),
    UPPER(TRIM(state_code)),
    weather_date::DATE,
    NULLIF(TRIM(temperature_2m_mean), '')::DECIMAL(5,2),
    NULLIF(TRIM(temperature_2m_max), '')::DECIMAL(5,2),
    COALESCE(NULLIF(TRIM(precipitation_sum), '')::DECIMAL(8,2), 0.00),
    NULLIF(TRIM(weather_code), '')::INTEGER,
    -- DERIVED: Weather category
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
    -- DERIVED: Is rainy
    CASE WHEN COALESCE(NULLIF(TRIM(precipitation_sum), '')::DECIMAL(8,2), 0.00) > 0 THEN TRUE ELSE FALSE END,
    -- DERIVED: Extreme heat
    CASE WHEN NULLIF(TRIM(temperature_2m_max), '')::DECIMAL(5,2) > 35 THEN TRUE ELSE FALSE END,
    'bronze.raw_weather_history', CURRENT_TIMESTAMP(), TRUE, NULL
FROM BRONZE.RAW_WEATHER_HISTORY
WHERE state_code IS NOT NULL AND TRIM(state_code) != ''
  AND weather_date IS NOT NULL AND TRIM(weather_date) != '';

-- ============================================================================
-- SECTION 5: VERIFICATION — Bronze vs Silver Counts
-- ============================================================================

SELECT 'OLIST_ORDERS' AS table_name,
    (SELECT COUNT(*) FROM BRONZE.RAW_ORDERS) AS bronze_count,
    (SELECT COUNT(*) FROM SILVER.OLIST_ORDERS) AS silver_count
UNION ALL SELECT 'OLIST_ORDER_ITEMS',
    (SELECT COUNT(*) FROM BRONZE.RAW_ORDER_ITEMS),
    (SELECT COUNT(*) FROM SILVER.OLIST_ORDER_ITEMS)
UNION ALL SELECT 'OLIST_ORDER_PAYMENTS',
    (SELECT COUNT(*) FROM BRONZE.RAW_ORDER_PAYMENTS),
    (SELECT COUNT(*) FROM SILVER.OLIST_ORDER_PAYMENTS)
UNION ALL SELECT 'OLIST_ORDER_REVIEWS',
    (SELECT COUNT(*) FROM BRONZE.RAW_ORDER_REVIEWS),
    (SELECT COUNT(*) FROM SILVER.OLIST_ORDER_REVIEWS)
UNION ALL SELECT 'OLIST_CUSTOMERS',
    (SELECT COUNT(*) FROM BRONZE.RAW_CUSTOMERS),
    (SELECT COUNT(*) FROM SILVER.OLIST_CUSTOMERS)
UNION ALL SELECT 'OLIST_GEOLOCATION (DEDUPLICATED)',
    (SELECT COUNT(*) FROM BRONZE.RAW_GEOLOCATION),
    (SELECT COUNT(*) FROM SILVER.OLIST_GEOLOCATION)
UNION ALL SELECT 'OLIST_PRODUCTS',
    (SELECT COUNT(*) FROM BRONZE.RAW_PRODUCTS),
    (SELECT COUNT(*) FROM SILVER.OLIST_PRODUCTS)
UNION ALL SELECT 'OLIST_CATEGORY_TRANSLATION',
    (SELECT COUNT(*) FROM BRONZE.RAW_CATEGORY_TRANSLATION),
    (SELECT COUNT(*) FROM SILVER.OLIST_CATEGORY_TRANSLATION)
UNION ALL SELECT 'OLIST_SELLERS',
    (SELECT COUNT(*) FROM BRONZE.RAW_SELLERS),
    (SELECT COUNT(*) FROM SILVER.OLIST_SELLERS)
UNION ALL SELECT 'OLIST_MQL',
    (SELECT COUNT(*) FROM BRONZE.RAW_MARKETING_QUALIFIED_LEADS),
    (SELECT COUNT(*) FROM SILVER.OLIST_MQL)
UNION ALL SELECT 'OLIST_CLOSED_DEALS',
    (SELECT COUNT(*) FROM BRONZE.RAW_CLOSED_DEALS),
    (SELECT COUNT(*) FROM SILVER.OLIST_CLOSED_DEALS)
UNION ALL SELECT 'API_CURRENCY_RATES',
    (SELECT COUNT(*) FROM BRONZE.RAW_CURRENCY_RATES),
    (SELECT COUNT(*) FROM SILVER.API_CURRENCY_RATES)
UNION ALL SELECT 'API_BRAZIL_HOLIDAYS',
    (SELECT COUNT(*) FROM BRONZE.RAW_BRAZIL_HOLIDAYS),
    (SELECT COUNT(*) FROM SILVER.API_BRAZIL_HOLIDAYS)
UNION ALL SELECT 'API_WEATHER_HISTORY',
    (SELECT COUNT(*) FROM BRONZE.RAW_WEATHER_HISTORY),
    (SELECT COUNT(*) FROM SILVER.API_WEATHER_HISTORY)
ORDER BY table_name;
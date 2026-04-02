/*
================================================================================
SNOWFLAKE GOLD LAYER: Create Star Schema & Load from Silver
================================================================================

PURPOSE:
  Migrated from PostgreSQL → Snowflake.
  Creates dimensional model (star schema) and loads from Silver layer.

  Schema: 5 Dimensions, 2 Facts, 1 Bridge

POSTGRESQL → SNOWFLAKE CHANGES:
  - SERIAL                   → AUTOINCREMENT
  - generate_series()        → Snowflake table generator (GENERATOR + ROW_NUMBER)
  - DISTINCT ON              → ROW_NUMBER() window function
  - TO_CHAR(d, 'Month')      → MONTHNAME(d)
  - TO_CHAR(d, 'Day')        → DAYNAME(d)
  - EXTRACT(DOW FROM d)      → DAYOFWEEK(d)
  - (date1 - date2)          → DATEDIFF(DAY, date2, date1)
  - TRUNCATE ... CASCADE     → TRUNCATE (no CASCADE in Snowflake)
  - CREATE INDEX             → Removed (Snowflake auto-manages via micro-partitions)
  - REFERENCES (FK)          → Kept for documentation; Snowflake parses but doesn't enforce
  - ::NUMERIC                → ::DECIMAL

================================================================================
*/

USE DATABASE OLIST_DWH;
USE SCHEMA GOLD;
USE WAREHOUSE OLIST_WH;

-- ============================================================================
-- SECTION 1: DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_date (Calendar Dimension)
-- SNOWFLAKE CHANGE: AUTOINCREMENT replaces SERIAL; no indexes needed
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_DATE (
    date_key                INTEGER PRIMARY KEY,
    full_date               DATE NOT NULL UNIQUE,
    year                    INTEGER NOT NULL,
    quarter                 INTEGER NOT NULL,
    quarter_name            VARCHAR(2) NOT NULL,
    month                   INTEGER NOT NULL,
    month_name              VARCHAR(10) NOT NULL,
    week_of_year            INTEGER NOT NULL,
    day_of_month            INTEGER NOT NULL,
    day_of_week             INTEGER NOT NULL,
    day_name                VARCHAR(10) NOT NULL,
    is_weekend              BOOLEAN NOT NULL,
    is_holiday              BOOLEAN DEFAULT FALSE,
    holiday_name            VARCHAR(100),
    usd_exchange_rate       DECIMAL(10,4)
);
COMMENT ON TABLE DIM_DATE IS 'Calendar dimension for time-based analysis';

-- ----------------------------------------------------------------------------
-- dim_geography
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_GEOGRAPHY (
    geography_key           INTEGER AUTOINCREMENT PRIMARY KEY,
    zip_code_prefix         VARCHAR(5) NOT NULL UNIQUE,
    city                    VARCHAR(100),
    state                   VARCHAR(2) NOT NULL,
    state_name              VARCHAR(50),
    region                  VARCHAR(20) NOT NULL,
    latitude                DECIMAL(9,6),
    longitude               DECIMAL(9,6)
);
COMMENT ON TABLE DIM_GEOGRAPHY IS 'Geography dimension with Brazilian regions';

-- ----------------------------------------------------------------------------
-- dim_customer
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_key            INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id             VARCHAR(32) NOT NULL UNIQUE,
    customer_unique_id      VARCHAR(32) NOT NULL,
    customer_zip_code       VARCHAR(5),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(2) NOT NULL,
    customer_region         VARCHAR(20) NOT NULL,
    geography_key           INTEGER
);
COMMENT ON TABLE DIM_CUSTOMER IS 'Customer dimension with geography link';

-- ----------------------------------------------------------------------------
-- dim_seller
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_SELLER (
    seller_key              INTEGER AUTOINCREMENT PRIMARY KEY,
    seller_id               VARCHAR(32) NOT NULL UNIQUE,
    seller_zip_code         VARCHAR(5),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(2) NOT NULL,
    seller_region           VARCHAR(20) NOT NULL,
    geography_key           INTEGER,
    is_from_marketing       BOOLEAN DEFAULT FALSE,
    lead_origin             VARCHAR(50),
    lead_won_date           DATE
);
COMMENT ON TABLE DIM_SELLER IS 'Seller dimension with geography and marketing funnel link';

-- ----------------------------------------------------------------------------
-- dim_product
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_key             INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id              VARCHAR(32) NOT NULL UNIQUE,
    category_name_pt        VARCHAR(100),
    category_name_en        VARCHAR(100),
    weight_g                DECIMAL(10,2),
    volume_cm3              DECIMAL(12,2),
    weight_category         VARCHAR(10),
    size_category           VARCHAR(10)
);
COMMENT ON TABLE DIM_PRODUCT IS 'Product dimension with English category names';

-- ============================================================================
-- SECTION 2: FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- fact_orders (Order-level fact)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_ORDERS (
    order_key               INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id                VARCHAR(32) NOT NULL UNIQUE,
    customer_key            INTEGER,
    order_date_key          INTEGER,
    order_status            VARCHAR(20) NOT NULL,
    total_items             INTEGER NOT NULL DEFAULT 0,
    total_product_value     DECIMAL(12,2) NOT NULL DEFAULT 0,
    total_freight_value     DECIMAL(12,2) NOT NULL DEFAULT 0,
    total_order_value       DECIMAL(12,2) NOT NULL DEFAULT 0,
    total_order_value_usd   DECIMAL(12,2),
    payment_type            VARCHAR(20),
    payment_installments    INTEGER DEFAULT 1,
    delivery_days           INTEGER,
    is_late                 BOOLEAN DEFAULT FALSE,
    review_score            INTEGER,
    weather_category        VARCHAR(20),
    temperature_max         DECIMAL(5,2),
    is_rainy                BOOLEAN DEFAULT FALSE
);
COMMENT ON TABLE FACT_ORDERS IS 'Order-level fact table (1 row per order)';

-- ----------------------------------------------------------------------------
-- fact_order_items (Line item fact)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_ORDER_ITEMS (
    item_key                INTEGER AUTOINCREMENT PRIMARY KEY,
    order_id                VARCHAR(32) NOT NULL,
    order_item_id           INTEGER NOT NULL,
    order_key               INTEGER,
    customer_key            INTEGER,
    seller_key              INTEGER,
    product_key             INTEGER,
    order_date_key          INTEGER,
    price                   DECIMAL(10,2) NOT NULL DEFAULT 0,
    freight_value           DECIMAL(10,2) NOT NULL DEFAULT 0,
    item_total              DECIMAL(10,2) NOT NULL DEFAULT 0,
    UNIQUE (order_id, order_item_id)
);
COMMENT ON TABLE FACT_ORDER_ITEMS IS 'Line item fact table (1 row per item in order)';

-- ============================================================================
-- SECTION 3: BRIDGE TABLE
-- ============================================================================

CREATE OR REPLACE TABLE BRIDGE_MARKETING_FUNNEL (
    funnel_key              INTEGER AUTOINCREMENT PRIMARY KEY,
    mql_id                  VARCHAR(32) NOT NULL UNIQUE,
    first_contact_date      DATE NOT NULL,
    origin                  VARCHAR(50),
    is_converted            BOOLEAN DEFAULT FALSE,
    won_date                DATE,
    days_to_conversion      INTEGER,
    business_segment        VARCHAR(50),
    lead_type               VARCHAR(20),
    declared_monthly_revenue DECIMAL(12,2),
    seller_key              INTEGER,
    seller_id               VARCHAR(32),
    total_orders            INTEGER DEFAULT 0,
    total_revenue           DECIMAL(14,2) DEFAULT 0,
    first_order_date        DATE
);
COMMENT ON TABLE BRIDGE_MARKETING_FUNNEL IS 'Marketing funnel: MQL to seller performance';

-- ============================================================================
-- SECTION 4: LOAD DIMENSIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 4.1 dim_date
-- SNOWFLAKE CHANGE: generate_series() → GENERATOR + DATEADD
-- PostgreSQL uses generate_series('2016-01-01', '2018-12-31', '1 day')
-- Snowflake uses a row generator with DATEADD to create the date sequence.
-- This is one of the biggest syntax differences in the entire migration.
-- ----------------------------------------------------------------------------
INSERT INTO GOLD.DIM_DATE (
    date_key, full_date, year, quarter, quarter_name,
    month, month_name, week_of_year, day_of_month,
    day_of_week, day_name, is_weekend, is_holiday, holiday_name
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(QUARTER FROM d)::INTEGER,
    'Q' || EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    -- SNOWFLAKE CHANGE: MONTHNAME() replaces TO_CHAR(d, 'Month')
    MONTHNAME(d),
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(DAY FROM d)::INTEGER,
    -- SNOWFLAKE CHANGE: DAYOFWEEK() replaces EXTRACT(DOW FROM d)
    DAYOFWEEK(d),
    -- SNOWFLAKE CHANGE: DAYNAME() replaces TO_CHAR(d, 'Day')
    DAYNAME(d),
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END,
    FALSE,
    NULL
FROM (
    -- SNOWFLAKE CHANGE: Table generator replaces generate_series()
    -- Generates 1096 rows (2016-01-01 to 2018-12-31 = 3 years)
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2016-01-01'::DATE) AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 1096))
) dates;

-- ----------------------------------------------------------------------------
-- 4.2 dim_geography
-- ----------------------------------------------------------------------------
INSERT INTO GOLD.DIM_GEOGRAPHY (
    zip_code_prefix, city, state, state_name, region, latitude, longitude
)
SELECT
    zip_code_prefix,
    city,
    state,
    CASE state
        WHEN 'AC' THEN 'Acre' WHEN 'AL' THEN 'Alagoas' WHEN 'AP' THEN 'Amapá'
        WHEN 'AM' THEN 'Amazonas' WHEN 'BA' THEN 'Bahia' WHEN 'CE' THEN 'Ceará'
        WHEN 'DF' THEN 'Distrito Federal' WHEN 'ES' THEN 'Espírito Santo'
        WHEN 'GO' THEN 'Goiás' WHEN 'MA' THEN 'Maranhão' WHEN 'MT' THEN 'Mato Grosso'
        WHEN 'MS' THEN 'Mato Grosso do Sul' WHEN 'MG' THEN 'Minas Gerais'
        WHEN 'PA' THEN 'Pará' WHEN 'PB' THEN 'Paraíba' WHEN 'PR' THEN 'Paraná'
        WHEN 'PE' THEN 'Pernambuco' WHEN 'PI' THEN 'Piauí' WHEN 'RJ' THEN 'Rio de Janeiro'
        WHEN 'RN' THEN 'Rio Grande do Norte' WHEN 'RS' THEN 'Rio Grande do Sul'
        WHEN 'RO' THEN 'Rondônia' WHEN 'RR' THEN 'Roraima' WHEN 'SC' THEN 'Santa Catarina'
        WHEN 'SP' THEN 'São Paulo' WHEN 'SE' THEN 'Sergipe' WHEN 'TO' THEN 'Tocantins'
        ELSE 'Unknown'
    END,
    CASE state
        WHEN 'AC' THEN 'North' WHEN 'AP' THEN 'North' WHEN 'AM' THEN 'North'
        WHEN 'PA' THEN 'North' WHEN 'RO' THEN 'North' WHEN 'RR' THEN 'North' WHEN 'TO' THEN 'North'
        WHEN 'AL' THEN 'Northeast' WHEN 'BA' THEN 'Northeast' WHEN 'CE' THEN 'Northeast'
        WHEN 'MA' THEN 'Northeast' WHEN 'PB' THEN 'Northeast' WHEN 'PE' THEN 'Northeast'
        WHEN 'PI' THEN 'Northeast' WHEN 'RN' THEN 'Northeast' WHEN 'SE' THEN 'Northeast'
        WHEN 'DF' THEN 'Central-West' WHEN 'GO' THEN 'Central-West'
        WHEN 'MT' THEN 'Central-West' WHEN 'MS' THEN 'Central-West'
        WHEN 'ES' THEN 'Southeast' WHEN 'MG' THEN 'Southeast'
        WHEN 'RJ' THEN 'Southeast' WHEN 'SP' THEN 'Southeast'
        WHEN 'PR' THEN 'South' WHEN 'RS' THEN 'South' WHEN 'SC' THEN 'South'
        ELSE 'Unknown'
    END,
    latitude,
    longitude
FROM SILVER.OLIST_GEOLOCATION;

-- ----------------------------------------------------------------------------
-- 4.3 dim_customer
-- ----------------------------------------------------------------------------
INSERT INTO GOLD.DIM_CUSTOMER (
    customer_id, customer_unique_id, customer_zip_code,
    customer_city, customer_state, customer_region, geography_key
)
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    CASE c.customer_state
        WHEN 'AC' THEN 'North' WHEN 'AP' THEN 'North' WHEN 'AM' THEN 'North'
        WHEN 'PA' THEN 'North' WHEN 'RO' THEN 'North' WHEN 'RR' THEN 'North' WHEN 'TO' THEN 'North'
        WHEN 'AL' THEN 'Northeast' WHEN 'BA' THEN 'Northeast' WHEN 'CE' THEN 'Northeast'
        WHEN 'MA' THEN 'Northeast' WHEN 'PB' THEN 'Northeast' WHEN 'PE' THEN 'Northeast'
        WHEN 'PI' THEN 'Northeast' WHEN 'RN' THEN 'Northeast' WHEN 'SE' THEN 'Northeast'
        WHEN 'DF' THEN 'Central-West' WHEN 'GO' THEN 'Central-West'
        WHEN 'MT' THEN 'Central-West' WHEN 'MS' THEN 'Central-West'
        WHEN 'ES' THEN 'Southeast' WHEN 'MG' THEN 'Southeast'
        WHEN 'RJ' THEN 'Southeast' WHEN 'SP' THEN 'Southeast'
        WHEN 'PR' THEN 'South' WHEN 'RS' THEN 'South' WHEN 'SC' THEN 'South'
        ELSE 'Unknown'
    END,
    g.geography_key
FROM SILVER.OLIST_CUSTOMERS c
LEFT JOIN GOLD.DIM_GEOGRAPHY g ON c.customer_zip_code_prefix = g.zip_code_prefix;

-- ----------------------------------------------------------------------------
-- 4.4 dim_seller
-- ----------------------------------------------------------------------------
INSERT INTO GOLD.DIM_SELLER (
    seller_id, seller_zip_code, seller_city, seller_state,
    seller_region, geography_key, is_from_marketing, lead_origin, lead_won_date
)
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    CASE s.seller_state
        WHEN 'AC' THEN 'North' WHEN 'AP' THEN 'North' WHEN 'AM' THEN 'North'
        WHEN 'PA' THEN 'North' WHEN 'RO' THEN 'North' WHEN 'RR' THEN 'North' WHEN 'TO' THEN 'North'
        WHEN 'AL' THEN 'Northeast' WHEN 'BA' THEN 'Northeast' WHEN 'CE' THEN 'Northeast'
        WHEN 'MA' THEN 'Northeast' WHEN 'PB' THEN 'Northeast' WHEN 'PE' THEN 'Northeast'
        WHEN 'PI' THEN 'Northeast' WHEN 'RN' THEN 'Northeast' WHEN 'SE' THEN 'Northeast'
        WHEN 'DF' THEN 'Central-West' WHEN 'GO' THEN 'Central-West'
        WHEN 'MT' THEN 'Central-West' WHEN 'MS' THEN 'Central-West'
        WHEN 'ES' THEN 'Southeast' WHEN 'MG' THEN 'Southeast'
        WHEN 'RJ' THEN 'Southeast' WHEN 'SP' THEN 'Southeast'
        WHEN 'PR' THEN 'South' WHEN 'RS' THEN 'South' WHEN 'SC' THEN 'South'
        ELSE 'Unknown'
    END,
    g.geography_key,
    CASE WHEN cd.seller_id IS NOT NULL THEN TRUE ELSE FALSE END,
    m.origin,
    cd.won_date
FROM SILVER.OLIST_SELLERS s
LEFT JOIN GOLD.DIM_GEOGRAPHY g ON s.seller_zip_code_prefix = g.zip_code_prefix
LEFT JOIN SILVER.OLIST_CLOSED_DEALS cd ON s.seller_id = cd.seller_id
LEFT JOIN SILVER.OLIST_MQL m ON cd.mql_id = m.mql_id;

-- ----------------------------------------------------------------------------
-- 4.5 dim_product
-- ----------------------------------------------------------------------------
INSERT INTO GOLD.DIM_PRODUCT (
    product_id, category_name_pt, category_name_en,
    weight_g, volume_cm3, weight_category, size_category
)
SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english, 'unknown'),
    p.product_weight_g,
    p.product_volume_cm3,
    CASE
        WHEN p.product_weight_g IS NULL THEN 'Unknown'
        WHEN p.product_weight_g < 500 THEN 'Light'
        WHEN p.product_weight_g < 2000 THEN 'Medium'
        ELSE 'Heavy'
    END,
    CASE
        WHEN p.product_volume_cm3 IS NULL THEN 'Unknown'
        WHEN p.product_volume_cm3 < 1000 THEN 'Small'
        WHEN p.product_volume_cm3 < 10000 THEN 'Medium'
        ELSE 'Large'
    END
FROM SILVER.OLIST_PRODUCTS p
LEFT JOIN SILVER.OLIST_CATEGORY_TRANSLATION t ON p.product_category_name = t.product_category_name;

-- ============================================================================
-- SECTION 5: LOAD FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 5.1 fact_orders
-- SNOWFLAKE CHANGE: DISTINCT ON → ROW_NUMBER() for payment and review dedup
-- ----------------------------------------------------------------------------
INSERT INTO GOLD.FACT_ORDERS (
    order_id, customer_key, order_date_key, order_status,
    total_items, total_product_value, total_freight_value, total_order_value,
    total_order_value_usd, payment_type, payment_installments,
    delivery_days, is_late, review_score,
    weather_category, temperature_max, is_rainy
)
SELECT
    o.order_id,
    c.customer_key,
    TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INTEGER,
    o.order_status,
    COALESCE(item_agg.total_items, 0),
    COALESCE(item_agg.total_product_value, 0),
    COALESCE(item_agg.total_freight_value, 0),
    COALESCE(item_agg.total_product_value, 0) + COALESCE(item_agg.total_freight_value, 0),
    CASE
        WHEN d.usd_exchange_rate IS NOT NULL AND d.usd_exchange_rate > 0
        THEN ROUND(
            (COALESCE(item_agg.total_product_value, 0) + COALESCE(item_agg.total_freight_value, 0))
            * d.usd_exchange_rate, 2
        )
        ELSE NULL
    END,
    pay.payment_type,
    COALESCE(pay.payment_installments, 1),
    o.delivery_days_actual,
    COALESCE(o.is_late_delivery, FALSE),
    r.review_score,
    w.weather_category,
    w.temperature_max,
    COALESCE(w.is_rainy, FALSE)
FROM SILVER.OLIST_ORDERS o
LEFT JOIN GOLD.DIM_CUSTOMER c ON o.customer_id = c.customer_id
LEFT JOIN GOLD.DIM_DATE d ON TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INTEGER = d.date_key
LEFT JOIN SILVER.API_WEATHER_HISTORY w
    ON c.customer_state = w.state_code
    AND o.order_purchase_timestamp::DATE = w.weather_date
LEFT JOIN (
    SELECT order_id, COUNT(*) AS total_items,
           SUM(price) AS total_product_value, SUM(freight_value) AS total_freight_value
    FROM SILVER.OLIST_ORDER_ITEMS GROUP BY order_id
) item_agg ON o.order_id = item_agg.order_id
-- SNOWFLAKE CHANGE: DISTINCT ON → ROW_NUMBER() for primary payment
LEFT JOIN (
    SELECT order_id, payment_type, payment_installments
    FROM (
        SELECT order_id, payment_type, payment_installments,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_value DESC) AS rn
        FROM SILVER.OLIST_ORDER_PAYMENTS
    )
    WHERE rn = 1
) pay ON o.order_id = pay.order_id
-- SNOWFLAKE CHANGE: DISTINCT ON → ROW_NUMBER() for latest review
LEFT JOIN (
    SELECT order_id, review_score
    FROM (
        SELECT order_id, review_score,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY review_creation_date DESC) AS rn
        FROM SILVER.OLIST_ORDER_REVIEWS
    )
    WHERE rn = 1
) r ON o.order_id = r.order_id;

-- ----------------------------------------------------------------------------
-- 5.2 fact_order_items
-- ----------------------------------------------------------------------------
INSERT INTO GOLD.FACT_ORDER_ITEMS (
    order_id, order_item_id, order_key, customer_key,
    seller_key, product_key, order_date_key,
    price, freight_value, item_total
)
SELECT
    i.order_id,
    i.order_item_id,
    fo.order_key,
    fo.customer_key,
    s.seller_key,
    p.product_key,
    fo.order_date_key,
    i.price,
    i.freight_value,
    i.price + i.freight_value
FROM SILVER.OLIST_ORDER_ITEMS i
LEFT JOIN GOLD.FACT_ORDERS fo ON i.order_id = fo.order_id
LEFT JOIN GOLD.DIM_SELLER s ON i.seller_id = s.seller_id
LEFT JOIN GOLD.DIM_PRODUCT p ON i.product_id = p.product_id;

-- ============================================================================
-- SECTION 6: LOAD BRIDGE TABLE
-- ============================================================================

INSERT INTO GOLD.BRIDGE_MARKETING_FUNNEL (
    mql_id, first_contact_date, origin, is_converted, won_date,
    days_to_conversion, business_segment, lead_type, declared_monthly_revenue,
    seller_key, seller_id, total_orders, total_revenue, first_order_date
)
SELECT
    m.mql_id,
    m.first_contact_date,
    m.origin,
    CASE WHEN cd.mql_id IS NOT NULL THEN TRUE ELSE FALSE END,
    cd.won_date,
    -- SNOWFLAKE CHANGE: DATEDIFF replaces (date1 - date2)
    DATEDIFF(DAY, m.first_contact_date, cd.won_date),
    cd.business_segment,
    cd.lead_type,
    cd.declared_monthly_revenue,
    s.seller_key,
    cd.seller_id,
    COALESCE(perf.total_orders, 0),
    COALESCE(perf.total_revenue, 0),
    perf.first_order_date
FROM SILVER.OLIST_MQL m
LEFT JOIN SILVER.OLIST_CLOSED_DEALS cd ON m.mql_id = cd.mql_id
LEFT JOIN GOLD.DIM_SELLER s ON cd.seller_id = s.seller_id
LEFT JOIN (
    SELECT fi.seller_key, COUNT(DISTINCT fi.order_id) AS total_orders,
           SUM(fi.item_total) AS total_revenue, MIN(d.full_date) AS first_order_date
    FROM GOLD.FACT_ORDER_ITEMS fi
    JOIN GOLD.DIM_DATE d ON fi.order_date_key = d.date_key
    WHERE fi.seller_key IS NOT NULL
    GROUP BY fi.seller_key
) perf ON s.seller_key = perf.seller_key;

-- ============================================================================
-- SECTION 7: VERIFICATION
-- ============================================================================

SELECT 'DIM_DATE' AS table_name, COUNT(*) AS row_count FROM GOLD.DIM_DATE
UNION ALL SELECT 'DIM_GEOGRAPHY', COUNT(*) FROM GOLD.DIM_GEOGRAPHY
UNION ALL SELECT 'DIM_CUSTOMER', COUNT(*) FROM GOLD.DIM_CUSTOMER
UNION ALL SELECT 'DIM_SELLER', COUNT(*) FROM GOLD.DIM_SELLER
UNION ALL SELECT 'DIM_PRODUCT', COUNT(*) FROM GOLD.DIM_PRODUCT
UNION ALL SELECT 'FACT_ORDERS', COUNT(*) FROM GOLD.FACT_ORDERS
UNION ALL SELECT 'FACT_ORDER_ITEMS', COUNT(*) FROM GOLD.FACT_ORDER_ITEMS
UNION ALL SELECT 'BRIDGE_MARKETING_FUNNEL', COUNT(*) FROM GOLD.BRIDGE_MARKETING_FUNNEL
ORDER BY table_name;

-- Business metrics sanity check
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(total_order_value), 2) AS total_revenue_brl,
    ROUND(AVG(review_score), 2) AS avg_review,
    ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 1) AS late_pct
FROM GOLD.FACT_ORDERS;
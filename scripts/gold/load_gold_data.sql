-- ============================================================================
-- GOLD LAYER: LOAD DATA (ETL from Silver to Gold)
-- ============================================================================

SET search_path TO gold, silver, public;

\echo '============================================================'
\echo 'GOLD LAYER ETL - Loading Data'
\echo '============================================================'

-- ============================================================================
-- SECTION 1: LOAD DIMENSIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1.1 dim_date
-- ----------------------------------------------------------------------------

\echo 'Loading gold.dim_date...'

TRUNCATE TABLE gold.dim_date CASCADE;

INSERT INTO gold.dim_date (
    date_key, full_date, year, quarter, quarter_name,
    month, month_name, week_of_year, day_of_month,
    day_of_week, day_name, is_weekend, is_holiday, holiday_name
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(QUARTER FROM d)::INTEGER,
    'Q' || EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    TRIM(TO_CHAR(d, 'Month')),
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(DAY FROM d)::INTEGER,
    EXTRACT(DOW FROM d)::INTEGER,
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(DOW FROM d) IN (0, 6),
    FALSE,
    NULL
FROM generate_series('2016-01-01'::DATE, '2018-12-31'::DATE, '1 day'::INTERVAL) AS d;

\echo '  ✓ dim_date loaded'
SELECT COUNT(*) AS dim_date_rows FROM gold.dim_date;

-- ----------------------------------------------------------------------------
-- 1.2 dim_geography (LOAD FIRST - Referenced by customer & seller)
-- ----------------------------------------------------------------------------

\echo 'Loading gold.dim_geography...'

TRUNCATE TABLE gold.dim_geography CASCADE;

INSERT INTO gold.dim_geography (
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
FROM silver.olist_geolocation;

\echo '  ✓ dim_geography loaded'
SELECT COUNT(*) AS dim_geography_rows FROM gold.dim_geography;

-- ----------------------------------------------------------------------------
-- 1.3 dim_customer (With geography_key FK)
-- ----------------------------------------------------------------------------

\echo 'Loading gold.dim_customer...'

TRUNCATE TABLE gold.dim_customer CASCADE;

INSERT INTO gold.dim_customer (
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
FROM silver.olist_customers c
LEFT JOIN gold.dim_geography g ON c.customer_zip_code_prefix = g.zip_code_prefix;

\echo '  ✓ dim_customer loaded'
SELECT COUNT(*) AS dim_customer_rows FROM gold.dim_customer;

-- ----------------------------------------------------------------------------
-- 1.4 dim_seller (With geography_key FK)
-- ----------------------------------------------------------------------------

\echo 'Loading gold.dim_seller...'

TRUNCATE TABLE gold.dim_seller CASCADE;

INSERT INTO gold.dim_seller (
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
    (cd.seller_id IS NOT NULL),
    m.origin,
    cd.won_date
FROM silver.olist_sellers s
LEFT JOIN gold.dim_geography g ON s.seller_zip_code_prefix = g.zip_code_prefix
LEFT JOIN silver.olist_closed_deals cd ON s.seller_id = cd.seller_id
LEFT JOIN silver.olist_mql m ON cd.mql_id = m.mql_id;

\echo '  ✓ dim_seller loaded'
SELECT COUNT(*) AS dim_seller_rows FROM gold.dim_seller;

-- ----------------------------------------------------------------------------
-- 1.5 dim_product
-- ----------------------------------------------------------------------------

\echo 'Loading gold.dim_product...'

TRUNCATE TABLE gold.dim_product CASCADE;

INSERT INTO gold.dim_product (
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
FROM silver.olist_products p
LEFT JOIN silver.olist_category_translation t
    ON LOWER(p.product_category_name) = LOWER(t.product_category_name);

\echo '  ✓ dim_product loaded'
SELECT COUNT(*) AS dim_product_rows FROM gold.dim_product;

-- ============================================================================
-- SECTION 2: LOAD FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 2.1 fact_orders
-- ----------------------------------------------------------------------------

\echo 'Loading gold.fact_orders...'

TRUNCATE TABLE gold.fact_orders CASCADE;

INSERT INTO gold.fact_orders (
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
    END AS total_order_value_usd,
    pay.payment_type,
    COALESCE(pay.payment_installments, 1),
    o.delivery_days_actual,
    COALESCE(o.is_late_delivery, FALSE),
    r.review_score,
    -- Weather columns (NEW)
    w.weather_category,
    w.temperature_max,
    COALESCE(w.is_rainy, FALSE)
FROM silver.olist_orders o
LEFT JOIN gold.dim_customer c ON o.customer_id = c.customer_id
LEFT JOIN gold.dim_date d ON TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INTEGER = d.date_key
LEFT JOIN silver.api_weather_history w
    ON c.customer_state = w.state_code
    AND DATE(o.order_purchase_timestamp) = w.weather_date
LEFT JOIN (
    SELECT order_id, COUNT(*) AS total_items,
           SUM(price) AS total_product_value, SUM(freight_value) AS total_freight_value
    FROM silver.olist_order_items GROUP BY order_id
) item_agg ON o.order_id = item_agg.order_id
LEFT JOIN (
    SELECT DISTINCT ON (order_id) order_id, payment_type, payment_installments
    FROM silver.olist_order_payments ORDER BY order_id, payment_value DESC
) pay ON o.order_id = pay.order_id
LEFT JOIN (
    SELECT DISTINCT ON (order_id) order_id, review_score
    FROM silver.olist_order_reviews ORDER BY order_id, review_creation_date DESC
) r ON o.order_id = r.order_id;

\echo '  ✓ fact_orders loaded'
SELECT COUNT(*) AS fact_orders_rows FROM gold.fact_orders;

-- Verify weather integration

\echo 'Weather Integration Check:'
SELECT
    weather_category,
    COUNT(*) AS orders,
    ROUND(AVG(total_order_value), 2) AS avg_order_value,
    ROUND(AVG(review_score), 2) AS avg_review
FROM gold.fact_orders
WHERE weather_category IS NOT NULL
GROUP BY weather_category
ORDER BY orders DESC;

-- ----------------------------------------------------------------------------
-- 2.2 fact_order_items
-- ----------------------------------------------------------------------------

\echo 'Loading gold.fact_order_items...'

TRUNCATE TABLE gold.fact_order_items CASCADE;

INSERT INTO gold.fact_order_items (
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
FROM silver.olist_order_items i
LEFT JOIN gold.fact_orders fo ON i.order_id = fo.order_id
LEFT JOIN gold.dim_seller s ON i.seller_id = s.seller_id
LEFT JOIN gold.dim_product p ON i.product_id = p.product_id;

\echo '  ✓ fact_order_items loaded'
SELECT COUNT(*) AS fact_order_items_rows FROM gold.fact_order_items;

-- ============================================================================
-- SECTION 3: LOAD BRIDGE TABLE
-- ============================================================================


\echo 'Loading gold.bridge_marketing_funnel...'

TRUNCATE TABLE gold.bridge_marketing_funnel CASCADE;

INSERT INTO gold.bridge_marketing_funnel (
    mql_id, first_contact_date, origin, is_converted, won_date,
    days_to_conversion, business_segment, lead_type, declared_monthly_revenue,
    seller_key, seller_id, total_orders, total_revenue, first_order_date
)
SELECT
    m.mql_id,
    m.first_contact_date,
    m.origin,
    (cd.mql_id IS NOT NULL),
    cd.won_date,
    (cd.won_date - m.first_contact_date),
    cd.business_segment,
    cd.lead_type,
    cd.declared_monthly_revenue,
    s.seller_key,
    cd.seller_id,
    COALESCE(perf.total_orders, 0),
    COALESCE(perf.total_revenue, 0),
    perf.first_order_date
FROM silver.olist_mql m
LEFT JOIN silver.olist_closed_deals cd ON m.mql_id = cd.mql_id
LEFT JOIN gold.dim_seller s ON cd.seller_id = s.seller_id
LEFT JOIN (
    SELECT fi.seller_key, COUNT(DISTINCT fi.order_id) AS total_orders,
           SUM(fi.item_total) AS total_revenue, MIN(d.full_date) AS first_order_date
    FROM gold.fact_order_items fi
    JOIN gold.dim_date d ON fi.order_date_key = d.date_key
    WHERE fi.seller_key IS NOT NULL
    GROUP BY fi.seller_key
) perf ON s.seller_key = perf.seller_key;

\echo '  ✓ bridge_marketing_funnel loaded'
SELECT COUNT(*) AS bridge_funnel_rows FROM gold.bridge_marketing_funnel;

-- ============================================================================
-- SECTION 4: VERIFICATION
-- ============================================================================


\echo '============================================================'
\echo 'VERIFICATION: Record Counts'
\echo '============================================================'

SELECT 'dim_date' AS table_name, COUNT(*) AS rows FROM gold.dim_date
UNION ALL SELECT 'dim_geography', COUNT(*) FROM gold.dim_geography
UNION ALL SELECT 'dim_customer', COUNT(*) FROM gold.dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM gold.dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM gold.dim_product
UNION ALL SELECT 'fact_orders', COUNT(*) FROM gold.fact_orders
UNION ALL SELECT 'fact_order_items', COUNT(*) FROM gold.fact_order_items
UNION ALL SELECT 'bridge_marketing_funnel', COUNT(*) FROM gold.bridge_marketing_funnel
ORDER BY table_name;

-- Check geography connections
\echo 'Geography Connection Check:'
SELECT
    'Customers with geography_key' AS check_name,
    COUNT(*) AS total,
    SUM(CASE WHEN geography_key IS NOT NULL THEN 1 ELSE 0 END) AS linked,
    ROUND(100.0 * SUM(CASE WHEN geography_key IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
FROM gold.dim_customer;

SELECT
    'Sellers with geography_key' AS check_name,
    COUNT(*) AS total,
    SUM(CASE WHEN geography_key IS NOT NULL THEN 1 ELSE 0 END) AS linked,
    ROUND(100.0 * SUM(CASE WHEN geography_key IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct
FROM gold.dim_seller;

-- Business metrics
\echo 'Business Metrics:'
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(total_order_value)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(review_score)::NUMERIC, 2) AS avg_review,
    ROUND(100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*), 1) AS late_pct
FROM gold.fact_orders;

\echo '============================================================'
\echo 'GOLD LAYER ETL COMPLETE!'
\echo '============================================================'
-- ============================================================================
-- GOLD LAYER: CREATE TABLES (Optimized for BI & Analytics)
-- ============================================================================
--
-- SCHEMA:
--   • 5 Dimensions: date, customer, seller, product, geography
--   • 2 Facts: orders, order_items
--   • 1 Bridge: marketing_funnel
--
-- ============================================================================

SET search_path TO gold, silver, public;

-- ============================================================================
-- SECTION 1: DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_date (Calendar Dimension)
-- ----------------------------------------------------------------------------
\echo 'Creating gold.dim_date...'

DROP TABLE IF EXISTS gold.dim_date CASCADE;

CREATE TABLE gold.dim_date (
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
    usd_exchange_rate      DECIMAL(10,4)
);

COMMENT ON TABLE gold.dim_date IS 'Calendar dimension for time-based analysis';

-- ----------------------------------------------------------------------------
-- dim_geography (Geography Dimension) - MUST BE CREATED FIRST (Referenced by others)
-- ----------------------------------------------------------------------------
\echo 'Creating gold.dim_geography...'

DROP TABLE IF EXISTS gold.dim_geography CASCADE;

CREATE TABLE gold.dim_geography (
    geography_key           SERIAL PRIMARY KEY,
    zip_code_prefix         VARCHAR(5) NOT NULL UNIQUE,
    city                    VARCHAR(100),
    state                   VARCHAR(2) NOT NULL,
    state_name              VARCHAR(50),
    region                  VARCHAR(20) NOT NULL,
    latitude                DECIMAL(9,6),
    longitude               DECIMAL(9,6)
);

COMMENT ON TABLE gold.dim_geography IS 'Geography dimension with Brazilian regions';

CREATE INDEX idx_dim_geography_state ON gold.dim_geography(state);
CREATE INDEX idx_dim_geography_region ON gold.dim_geography(region);

-- ----------------------------------------------------------------------------
-- dim_customer (Customer Dimension) - Now with geography_key FK
-- ----------------------------------------------------------------------------
\echo 'Creating gold.dim_customer...'

DROP TABLE IF EXISTS gold.dim_customer CASCADE;

CREATE TABLE gold.dim_customer (
    customer_key            SERIAL PRIMARY KEY,
    customer_id             VARCHAR(32) NOT NULL UNIQUE,
    customer_unique_id      VARCHAR(32) NOT NULL,
    customer_zip_code       VARCHAR(5),
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(2) NOT NULL,
    customer_region         VARCHAR(20) NOT NULL,
    geography_key           INTEGER REFERENCES gold.dim_geography(geography_key)
);

COMMENT ON TABLE gold.dim_customer IS 'Customer dimension with geography link';

CREATE INDEX idx_dim_customer_state ON gold.dim_customer(customer_state);
CREATE INDEX idx_dim_customer_region ON gold.dim_customer(customer_region);
CREATE INDEX idx_dim_customer_geography ON gold.dim_customer(geography_key);

-- ----------------------------------------------------------------------------
-- dim_seller (Seller Dimension) - Now with geography_key FK
-- ----------------------------------------------------------------------------
\echo 'Creating gold.dim_seller...'

DROP TABLE IF EXISTS gold.dim_seller CASCADE;

CREATE TABLE gold.dim_seller (
    seller_key              SERIAL PRIMARY KEY,
    seller_id               VARCHAR(32) NOT NULL UNIQUE,
    seller_zip_code         VARCHAR(5),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(2) NOT NULL,
    seller_region           VARCHAR(20) NOT NULL,
    geography_key           INTEGER REFERENCES gold.dim_geography(geography_key),
    is_from_marketing       BOOLEAN DEFAULT FALSE,
    lead_origin             VARCHAR(50),
    lead_won_date           DATE
);

COMMENT ON TABLE gold.dim_seller IS 'Seller dimension with geography and marketing funnel link';

CREATE INDEX idx_dim_seller_state ON gold.dim_seller(seller_state);
CREATE INDEX idx_dim_seller_region ON gold.dim_seller(seller_region);
CREATE INDEX idx_dim_seller_geography ON gold.dim_seller(geography_key);

-- ----------------------------------------------------------------------------
-- dim_product (Product Dimension)
-- ----------------------------------------------------------------------------
\echo 'Creating gold.dim_product...'

DROP TABLE IF EXISTS gold.dim_product CASCADE;

CREATE TABLE gold.dim_product (
    product_key             SERIAL PRIMARY KEY,
    product_id              VARCHAR(32) NOT NULL UNIQUE,
    category_name_pt        VARCHAR(100),
    category_name_en        VARCHAR(100),
    weight_g                DECIMAL(10,2),
    volume_cm3              DECIMAL(12,2),
    weight_category         VARCHAR(10),
    size_category           VARCHAR(10)
);

COMMENT ON TABLE gold.dim_product IS 'Product dimension with English category names';

CREATE INDEX idx_dim_product_category ON gold.dim_product(category_name_en);

-- ============================================================================
-- SECTION 2: FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- fact_orders (Order Fact - Aggregated)
-- ----------------------------------------------------------------------------
\echo 'Creating gold.fact_orders...'

DROP TABLE IF EXISTS gold.fact_orders CASCADE;

CREATE TABLE gold.fact_orders (
    order_key               SERIAL PRIMARY KEY,
    order_id                VARCHAR(32) NOT NULL UNIQUE,
    customer_key            INTEGER REFERENCES gold.dim_customer(customer_key),
    order_date_key          INTEGER REFERENCES gold.dim_date(date_key),
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

COMMENT ON TABLE gold.fact_orders IS 'Order-level fact table (1 row per order)';

CREATE INDEX idx_fact_orders_customer ON gold.fact_orders(customer_key);
CREATE INDEX idx_fact_orders_date ON gold.fact_orders(order_date_key);
CREATE INDEX idx_fact_orders_status ON gold.fact_orders(order_status);

-- ----------------------------------------------------------------------------
-- fact_order_items (Line Item Fact - Detail)
-- ----------------------------------------------------------------------------
\echo 'Creating gold.fact_order_items...'

DROP TABLE IF EXISTS gold.fact_order_items CASCADE;

CREATE TABLE gold.fact_order_items (
    item_key                SERIAL PRIMARY KEY,
    order_id                VARCHAR(32) NOT NULL,
    order_item_id           INTEGER NOT NULL,
    order_key               INTEGER REFERENCES gold.fact_orders(order_key),
    customer_key            INTEGER REFERENCES gold.dim_customer(customer_key),
    seller_key              INTEGER REFERENCES gold.dim_seller(seller_key),
    product_key             INTEGER REFERENCES gold.dim_product(product_key),
    order_date_key          INTEGER REFERENCES gold.dim_date(date_key),
    price                   DECIMAL(10,2) NOT NULL DEFAULT 0,
    freight_value           DECIMAL(10,2) NOT NULL DEFAULT 0,
    item_total              DECIMAL(10,2) NOT NULL DEFAULT 0,
    UNIQUE (order_id, order_item_id)
);

COMMENT ON TABLE gold.fact_order_items IS 'Line item fact table (1 row per item in order)';

CREATE INDEX idx_fact_items_order ON gold.fact_order_items(order_key);
CREATE INDEX idx_fact_items_seller ON gold.fact_order_items(seller_key);
CREATE INDEX idx_fact_items_product ON gold.fact_order_items(product_key);
CREATE INDEX idx_fact_items_date ON gold.fact_order_items(order_date_key);

-- ============================================================================
-- SECTION 3: BRIDGE TABLE
-- ============================================================================

\echo 'Creating gold.bridge_marketing_funnel...'

DROP TABLE IF EXISTS gold.bridge_marketing_funnel CASCADE;

CREATE TABLE gold.bridge_marketing_funnel (
    funnel_key              SERIAL PRIMARY KEY,
    mql_id                  VARCHAR(32) NOT NULL UNIQUE,
    first_contact_date      DATE NOT NULL,
    origin                  VARCHAR(50),
    is_converted            BOOLEAN DEFAULT FALSE,
    won_date                DATE,
    days_to_conversion      INTEGER,
    business_segment        VARCHAR(50),
    lead_type               VARCHAR(20),
    declared_monthly_revenue DECIMAL(12,2),
    seller_key              INTEGER REFERENCES gold.dim_seller(seller_key),
    seller_id               VARCHAR(32),
    total_orders            INTEGER DEFAULT 0,
    total_revenue           DECIMAL(14,2) DEFAULT 0,
    first_order_date        DATE
);

COMMENT ON TABLE gold.bridge_marketing_funnel IS 'Marketing funnel: MQL to seller performance';

CREATE INDEX idx_bridge_funnel_seller ON gold.bridge_marketing_funnel(seller_key);
CREATE INDEX idx_bridge_funnel_origin ON gold.bridge_marketing_funnel(origin);

-- ============================================================================
-- SECTION 4: VERIFICATION
-- ============================================================================

\echo '============================================================'
\echo 'GOLD LAYER TABLES CREATED'
\echo '============================================================'

SELECT
    table_name,
    (SELECT COUNT(*)
     FROM information_schema.columns c
     WHERE c.table_schema = 'gold'
     AND c.table_name = t.table_name) as columns
FROM information_schema.tables t
WHERE table_schema = 'gold'
ORDER BY table_name;

\echo 'Schema ready! Run load_gold_data_FINAL.sql to populate.'
\echo '============================================================'
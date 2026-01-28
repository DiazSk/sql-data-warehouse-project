-- ============================================================================
-- Script: silver_layer_findings.sql
-- Description: Comprehensive data exploration queries for Silver layer
-- ============================================================================

SET search_path TO silver, public;

-- ============================================================================
-- SECTION 1: ORDERS & DELIVERY PERFORMANCE
-- ============================================================================

\echo '============================================================'
\echo 'SECTION 1: ORDERS & DELIVERY PERFORMANCE'
\echo '============================================================'

-- 1.1 Order status distribution
\echo ''
\echo '1.1 Order Status Distribution:'
SELECT
    order_status,
    COUNT(*) as order_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM silver.olist_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- 1.2 Late delivery percentage
\echo ''
\echo '1.2 Late Delivery Analysis:'
SELECT
    COUNT(*) as total_delivered,
    SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) as late_count,
    ROUND(100.0 * SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) / COUNT(*), 2) as late_pct,
    ROUND(AVG(delivery_days_actual), 2) as avg_delivery_days,
    ROUND(AVG(delivery_days_estimated), 2) as avg_estimated_days
FROM silver.olist_orders
WHERE is_delivered = TRUE;

-- 1.3 Average delivery days vs estimated
\echo ''
\echo '1.3 Delivery Days (Actual vs Estimated):'
SELECT
    ROUND(AVG(delivery_days_actual), 2) as avg_actual_days,
    ROUND(AVG(delivery_days_estimated), 2) as avg_estimated_days,
    ROUND(AVG(delivery_days_actual) - AVG(delivery_days_estimated), 2) as avg_difference
FROM silver.olist_orders
WHERE is_delivered = TRUE;

-- 1.4 Late delivery by state (worst performers)
\echo ''
\echo '1.4 Late Delivery by State (Top 10 Worst):'
SELECT
    c.customer_state,
    COUNT(*) as total_deliveries,
    SUM(CASE WHEN o.is_late_delivery THEN 1 ELSE 0 END) as late_deliveries,
    ROUND(100.0 * SUM(CASE WHEN o.is_late_delivery THEN 1 ELSE 0 END) / COUNT(*), 2) as late_pct
FROM silver.olist_orders o
JOIN silver.olist_customers c ON o.customer_id = c.customer_id
WHERE o.is_delivered = TRUE
GROUP BY c.customer_state
ORDER BY late_pct DESC
LIMIT 10;

-- ============================================================================
-- SECTION 2: REVENUE & PRODUCT ANALYSIS
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 2: REVENUE & PRODUCT ANALYSIS'
\echo '============================================================'

-- 2.1 Top 10 categories by revenue (with English names)
\echo ''
\echo '2.1 Top 10 Product Categories by Revenue:'
SELECT
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') as category_english,
    p.product_category_name as category_portuguese,
    COUNT(*) as items_sold,
    ROUND(SUM(i.item_total), 2) as total_revenue
FROM silver.olist_order_items i
JOIN silver.olist_products p ON i.product_id = p.product_id
LEFT JOIN silver.olist_category_translation t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english, p.product_category_name
ORDER BY total_revenue DESC
LIMIT 10;

-- 2.2 Order value distribution
\echo ''
\echo '2.2 Order Value Distribution:'
SELECT
    CASE
        WHEN total_value < 50 THEN '1. < R$50'
        WHEN total_value < 100 THEN '2. R$50-100'
        WHEN total_value < 200 THEN '3. R$100-200'
        WHEN total_value < 500 THEN '4. R$200-500'
        WHEN total_value < 1000 THEN '5. R$500-1000'
        ELSE '6. > R$1000'
    END as value_bucket,
    COUNT(*) as order_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM (
    SELECT order_id, SUM(item_total) as total_value
    FROM silver.olist_order_items
    GROUP BY order_id
) order_totals
GROUP BY 1
ORDER BY 1;

-- 2.3 Monthly average order value
\echo ''
\echo '2.3 Monthly Average Order Value:'
SELECT
    DATE_TRUNC('month', o.order_purchase_date) as order_month,
    COUNT(DISTINCT o.order_id) as orders,
    ROUND(SUM(i.item_total) / COUNT(DISTINCT o.order_id), 2) as avg_order_value
FROM silver.olist_orders o
JOIN silver.olist_order_items i ON o.order_id = i.order_id
GROUP BY order_month
ORDER BY order_month;

-- 2.4 Orders with most items
\echo ''
\echo '2.4 Orders with Most Items (Top 10):'
SELECT
    o.order_id,
    COUNT(i.order_item_id) as item_count,
    ROUND(SUM(i.item_total), 2) as order_total
FROM silver.olist_orders o
JOIN silver.olist_order_items i ON o.order_id = i.order_id
GROUP BY o.order_id
ORDER BY item_count DESC
LIMIT 10;

-- ============================================================================
-- SECTION 3: CUSTOMER ANALYSIS
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 3: CUSTOMER ANALYSIS'
\echo '============================================================'

-- 3.1 Customer distribution by state
\echo ''
\echo '3.1 Customer Distribution by State (Top 10):'
SELECT
    customer_state,
    COUNT(*) as customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM silver.olist_customers
GROUP BY customer_state
ORDER BY customer_count DESC
LIMIT 10;

-- 3.2 Repeat customer analysis
\echo ''
\echo '3.2 Repeat Customer Analysis:'
SELECT
    COUNT(DISTINCT customer_unique_id) as total_unique_customers,
    SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) as repeat_customers,
    ROUND(100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT customer_unique_id), 2) as repeat_rate_pct
FROM (
    SELECT customer_unique_id, COUNT(*) as order_count
    FROM silver.olist_customers c
    JOIN silver.olist_orders o ON c.customer_id = o.customer_id
    GROUP BY customer_unique_id
) customer_orders;

-- ============================================================================
-- SECTION 4: PAYMENT ANALYSIS
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 4: PAYMENT ANALYSIS'
\echo '============================================================'

-- 4.1 Payment method distribution
\echo ''
\echo '4.1 Payment Method Distribution:'
SELECT
    payment_type,
    COUNT(*) as transaction_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct,
    ROUND(AVG(payment_value), 2) as avg_value
FROM silver.olist_order_payments
GROUP BY payment_type
ORDER BY transaction_count DESC;

-- 4.2 Credit card installment behavior
\echo ''
\echo '4.2 Credit Card Installment Behavior:'
SELECT
    payment_installments,
    COUNT(*) as order_count,
    ROUND(AVG(payment_value), 2) as avg_payment_value
FROM silver.olist_order_payments
WHERE payment_type = 'credit_card'
GROUP BY payment_installments
ORDER BY payment_installments;

-- 4.3 Boleto vs Credit Card comparison
\echo ''
\echo '4.3 Boleto vs Credit Card Comparison:'
SELECT
    payment_type,
    COUNT(DISTINCT order_id) as orders,
    ROUND(AVG(payment_value), 2) as avg_order_value,
    ROUND(SUM(payment_value), 2) as total_revenue
FROM silver.olist_order_payments
WHERE payment_type IN ('credit_card', 'boleto')
GROUP BY payment_type;

-- ============================================================================
-- SECTION 5: REVIEW & SATISFACTION
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 5: REVIEW & SATISFACTION'
\echo '============================================================'

-- 5.1 Review score distribution
\echo ''
\echo '5.1 Review Score Distribution:'
SELECT
    review_score,
    COUNT(*) as review_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM silver.olist_order_reviews
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score;

-- 5.2 Average review score over time
\echo ''
\echo '5.2 Average Review Score Over Time:'
SELECT
    DATE_TRUNC('month', review_creation_date) as review_month,
    ROUND(AVG(review_score), 2) as avg_score,
    COUNT(*) as review_count
FROM silver.olist_order_reviews
WHERE review_score IS NOT NULL
GROUP BY review_month
ORDER BY review_month;

-- 5.3 CRITICAL: Late delivery impact on reviews
\echo ''
\echo '5.3 ⚠️ CRITICAL: Late Delivery Impact on Reviews:'
SELECT
    CASE WHEN o.is_late_delivery THEN 'Late Delivery' ELSE 'On-Time Delivery' END as delivery_status,
    COUNT(*) as orders,
    ROUND(AVG(r.review_score), 2) as avg_review_score
FROM silver.olist_orders o
JOIN silver.olist_order_reviews r ON o.order_id = r.order_id
WHERE o.is_delivered = TRUE AND r.review_score IS NOT NULL
GROUP BY o.is_late_delivery
ORDER BY o.is_late_delivery;

-- 5.4 Review score by category (top 10 best rated)
\echo ''
\echo '5.4 Best Rated Product Categories (min 100 reviews):'
SELECT
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') as category,
    COUNT(*) as reviews,
    ROUND(AVG(r.review_score), 2) as avg_score
FROM silver.olist_order_reviews r
JOIN silver.olist_order_items i ON r.order_id = i.order_id
JOIN silver.olist_products p ON i.product_id = p.product_id
LEFT JOIN silver.olist_category_translation t ON p.product_category_name = t.product_category_name
WHERE r.review_score IS NOT NULL
GROUP BY COALESCE(t.product_category_name_english, p.product_category_name, 'unknown')
HAVING COUNT(*) >= 100
ORDER BY avg_score DESC
LIMIT 10;

-- ============================================================================
-- SECTION 6: SELLER PERFORMANCE
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 6: SELLER PERFORMANCE'
\echo '============================================================'

-- 6.1 Top 10 sellers by revenue
\echo ''
\echo '6.1 Top 10 Sellers by Revenue:'
SELECT
    seller_id,
    COUNT(DISTINCT order_id) as orders,
    ROUND(SUM(item_total), 2) as total_revenue,
    ROUND(AVG(price), 2) as avg_item_price
FROM silver.olist_order_items
GROUP BY seller_id
ORDER BY total_revenue DESC
LIMIT 10;

-- 6.2 Seller revenue concentration
\echo ''
\echo '6.2 Seller Revenue Concentration:'
SELECT
    CASE
        WHEN rank <= 10 THEN '1. Top 10'
        WHEN rank <= 100 THEN '2. Top 11-100'
        WHEN rank <= 500 THEN '3. Top 101-500'
        ELSE '4. Rest'
    END as seller_tier,
    COUNT(*) as seller_count,
    ROUND(SUM(total_revenue), 2) as tier_revenue,
    ROUND(100.0 * SUM(total_revenue) / (SELECT SUM(item_total) FROM silver.olist_order_items), 2) as pct_of_total
FROM (
    SELECT
        seller_id,
        SUM(item_total) as total_revenue,
        RANK() OVER (ORDER BY SUM(item_total) DESC) as rank
    FROM silver.olist_order_items
    GROUP BY seller_id
) ranked
GROUP BY 1
ORDER BY 1;

-- 6.3 Seller distribution by state
\echo ''
\echo '6.3 Seller Distribution by State (Top 10):'
SELECT
    seller_state,
    COUNT(*) as seller_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM silver.olist_sellers
GROUP BY seller_state
ORDER BY seller_count DESC
LIMIT 10;

-- ============================================================================
-- SECTION 7: TIME-BASED TRENDS
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 7: TIME-BASED TRENDS'
\echo '============================================================'

-- 7.1 Monthly order and revenue trend
\echo ''
\echo '7.1 Monthly Order & Revenue Trend:'
SELECT
    DATE_TRUNC('month', order_purchase_date) as month,
    COUNT(*) as orders,
    ROUND(SUM(items.total_value), 2) as revenue
FROM silver.olist_orders o
JOIN (
    SELECT order_id, SUM(item_total) as total_value
    FROM silver.olist_order_items
    GROUP BY order_id
) items ON o.order_id = items.order_id
GROUP BY month
ORDER BY month;

-- 7.2 Day of week pattern
\echo ''
\echo '7.2 Day of Week Pattern:'
SELECT
    EXTRACT(DOW FROM order_purchase_timestamp)::INTEGER as day_num,
    CASE EXTRACT(DOW FROM order_purchase_timestamp)::INTEGER
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    COUNT(*) as orders,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM silver.olist_orders
GROUP BY 1, 2
ORDER BY 1;

-- 7.3 Hour of day pattern
\echo ''
\echo '7.3 Hour of Day Pattern:'
SELECT
    EXTRACT(HOUR FROM order_purchase_timestamp)::INTEGER as hour,
    COUNT(*) as orders
FROM silver.olist_orders
GROUP BY hour
ORDER BY hour;

-- 7.4 Date range of data
\echo ''
\echo '7.4 Data Date Range:'
SELECT
    MIN(order_purchase_date) as first_order,
    MAX(order_purchase_date) as last_order,
    MAX(order_purchase_date) - MIN(order_purchase_date) as total_days
FROM silver.olist_orders;

-- ============================================================================
-- SECTION 8: FREIGHT & LOGISTICS
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 8: FREIGHT & LOGISTICS'
\echo '============================================================'

-- 8.1 Freight as percentage of order
\echo ''
\echo '8.1 Freight as Percentage of Order Value:'
SELECT
    ROUND(AVG(100.0 * freight_value / NULLIF(price, 0))::NUMERIC, 2) as avg_freight_pct,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 100.0 * freight_value / NULLIF(price, 0))::NUMERIC, 2) as median_freight_pct
FROM silver.olist_order_items
WHERE price > 0;

-- 8.2 Freight by customer state
\echo ''
\echo '8.2 Freight by Customer State (Top 10 Highest):'
SELECT
    c.customer_state,
    COUNT(*) as orders,
    ROUND(AVG(i.freight_value), 2) as avg_freight
FROM silver.olist_orders o
JOIN silver.olist_customers c ON o.customer_id = c.customer_id
JOIN silver.olist_order_items i ON o.order_id = i.order_id
GROUP BY c.customer_state
ORDER BY avg_freight DESC
LIMIT 10;

-- 8.3 Product weight impact on delivery and freight
\echo ''
\echo '8.3 Product Weight Impact:'
SELECT
    CASE
        WHEN p.product_weight_g < 500 THEN '1. Light (<500g)'
        WHEN p.product_weight_g < 2000 THEN '2. Medium (500g-2kg)'
        WHEN p.product_weight_g < 10000 THEN '3. Heavy (2-10kg)'
        ELSE '4. Very Heavy (>10kg)'
    END as weight_category,
    COUNT(*) as items,
    ROUND(AVG(o.delivery_days_actual), 2) as avg_delivery_days,
    ROUND(AVG(i.freight_value), 2) as avg_freight
FROM silver.olist_orders o
JOIN silver.olist_order_items i ON o.order_id = i.order_id
JOIN silver.olist_products p ON i.product_id = p.product_id
WHERE o.is_delivered = TRUE AND p.product_weight_g IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ============================================================================
-- SECTION 9: MARKETING FUNNEL
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 9: MARKETING FUNNEL'
\echo '============================================================'

-- 9.1 MQL to Closed Deal conversion (FIXED TABLE NAME!)
\echo ''
\echo '9.1 Marketing Funnel Conversion:'
SELECT
    (SELECT COUNT(*) FROM silver.olist_marketing_qualified_leads) as total_mqls,
    (SELECT COUNT(*) FROM silver.olist_closed_deals) as closed_deals,
    ROUND(100.0 * (SELECT COUNT(*) FROM silver.olist_closed_deals) /
        NULLIF((SELECT COUNT(*) FROM silver.olist_marketing_qualified_leads), 0), 2) as conversion_rate_pct;

-- 9.2 Closed deals link to e-commerce
\echo ''
\echo '9.2 Closed Deals Link to E-Commerce:'
SELECT
    COUNT(*) as total_deals,
    SUM(CASE WHEN has_seller_id THEN 1 ELSE 0 END) as linked_to_ecommerce,
    ROUND(100.0 * SUM(CASE WHEN has_seller_id THEN 1 ELSE 0 END) / COUNT(*), 2) as link_pct
FROM silver.olist_closed_deals;

-- 9.3 Linked vs non-linked deal comparison
\echo ''
\echo '9.3 Linked vs Non-Linked Deals:'
SELECT
    CASE WHEN has_seller_id THEN 'Linked to E-Commerce' ELSE 'Not Linked' END as status,
    COUNT(*) as deal_count,
    ROUND(AVG(declared_monthly_revenue), 2) as avg_declared_revenue,
    COUNT(DISTINCT business_segment) as unique_segments
FROM silver.olist_closed_deals
GROUP BY has_seller_id;

-- 9.4 Top lead origins (FIXED TABLE NAME!)
\echo ''
\echo '9.4 Top Lead Origins:'
SELECT
    origin,
    COUNT(*) as lead_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM silver.olist_marketing_qualified_leads
GROUP BY origin
ORDER BY lead_count DESC;

-- ============================================================================
-- SECTION 10: EXTERNAL FACTORS (Weather & Holidays)
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 10: EXTERNAL FACTORS'
\echo '============================================================'

-- 10.1 Weather impact on orders
\echo ''
\echo '10.1 Weather Impact on Orders:'
SELECT
    w.weather_category,
    COUNT(DISTINCT o.order_id) as order_count,
    ROUND(100.0 * COUNT(DISTINCT o.order_id) / SUM(COUNT(DISTINCT o.order_id)) OVER (), 2) as pct
FROM silver.olist_orders o
JOIN silver.olist_customers c ON o.customer_id = c.customer_id
JOIN silver.api_weather_history w ON c.customer_state = w.state_code
    AND o.order_purchase_date = w.weather_date
GROUP BY w.weather_category
ORDER BY order_count DESC;

-- 10.2 Holiday vs non-holiday orders
\echo ''
\echo '10.2 Holiday vs Non-Holiday Orders:'
SELECT
    CASE WHEN h.holiday_date IS NOT NULL THEN 'Holiday' ELSE 'Non-Holiday' END as day_type,
    COUNT(*) as orders,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM silver.olist_orders o
LEFT JOIN silver.api_brazil_holidays h ON o.order_purchase_date = h.holiday_date
GROUP BY 1;

-- 10.3 Monthly revenue in BRL and USD
\echo ''
\echo '10.3 Monthly Revenue (BRL and USD):'
SELECT
    DATE_TRUNC('month', o.order_purchase_date) as month,
    ROUND(SUM(i.item_total), 2) as revenue_brl,
    ROUND(SUM(i.item_total * COALESCE(cr.exchange_rate, 0.25)), 2) as revenue_usd_approx
FROM silver.olist_orders o
JOIN silver.olist_order_items i ON o.order_id = i.order_id
LEFT JOIN silver.api_currency_rates cr ON o.order_purchase_date = cr.rate_date
GROUP BY month
ORDER BY month;

-- ============================================================================
-- SECTION 11: CATEGORY TRANSLATION TABLE (ADDED)
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 11: CATEGORY TRANSLATION ANALYSIS'
\echo '============================================================'

-- 11.1 Categories with translations
\echo ''
\echo '11.1 Product Categories (Portuguese → English):'
SELECT
    t.product_category_name as portuguese,
    t.product_category_name_english as english,
    COUNT(DISTINCT p.product_id) as product_count,
    COALESCE(SUM(i.item_total), 0) as total_revenue
FROM silver.olist_category_translation t
LEFT JOIN silver.olist_products p ON t.product_category_name = p.product_category_name
LEFT JOIN silver.olist_order_items i ON p.product_id = i.product_id
GROUP BY t.product_category_name, t.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 15;

-- 11.2 Products without category translation
\echo ''
\echo '11.2 Products Missing Category Translation:'
SELECT
    p.product_category_name,
    COUNT(*) as product_count
FROM silver.olist_products p
LEFT JOIN silver.olist_category_translation t ON p.product_category_name = t.product_category_name
WHERE t.product_category_name IS NULL AND p.product_category_name IS NOT NULL
GROUP BY p.product_category_name
ORDER BY product_count DESC
LIMIT 10;

-- ============================================================================
-- SECTION 12: GEOLOCATION ANALYSIS (ADDED)
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 12: GEOLOCATION ANALYSIS'
\echo '============================================================'

-- 12.1 Zip codes by state
\echo ''
\echo '12.1 Zip Code Coverage by State:'
SELECT
    state,
    COUNT(*) as zip_codes,
    ROUND(AVG(latitude), 4) as avg_latitude,
    ROUND(AVG(longitude), 4) as avg_longitude
FROM silver.olist_geolocation
GROUP BY state
ORDER BY zip_codes DESC;

-- 12.2 Customer to seller ratio by state
\echo ''
\echo '12.2 Customer-to-Seller Ratio by State:'
SELECT
    COALESCE(c.state, s.state) as state,
    COALESCE(c.customer_count, 0) as customers,
    COALESCE(s.seller_count, 0) as sellers,
    CASE
        WHEN COALESCE(s.seller_count, 0) > 0
        THEN ROUND(COALESCE(c.customer_count, 0)::NUMERIC / s.seller_count, 1)
        ELSE 0
    END as customers_per_seller
FROM (
    SELECT customer_state as state, COUNT(*) as customer_count
    FROM silver.olist_customers
    GROUP BY customer_state
) c
FULL OUTER JOIN (
    SELECT seller_state as state, COUNT(*) as seller_count
    FROM silver.olist_sellers
    GROUP BY seller_state
) s ON c.state = s.state
ORDER BY customers DESC;

-- ============================================================================
-- SECTION 13: SUMMARY STATISTICS
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'SECTION 13: SUMMARY STATISTICS'
\echo '============================================================'

\echo ''
\echo '13.1 Overall Business Metrics:'
SELECT
    (SELECT COUNT(*) FROM silver.olist_orders) as total_orders,
    (SELECT COUNT(*) FROM silver.olist_orders WHERE is_delivered = TRUE) as delivered_orders,
    (SELECT ROUND(SUM(item_total), 2) FROM silver.olist_order_items) as total_revenue_brl,
    (SELECT COUNT(DISTINCT customer_unique_id) FROM silver.olist_customers) as unique_customers,
    (SELECT COUNT(*) FROM silver.olist_sellers) as total_sellers,
    (SELECT COUNT(*) FROM silver.olist_products) as total_products;

\echo ''
\echo '13.2 Table Record Counts:'
SELECT 'olist_orders' as table_name, COUNT(*) as records FROM silver.olist_orders
UNION ALL SELECT 'olist_order_items', COUNT(*) FROM silver.olist_order_items
UNION ALL SELECT 'olist_order_payments', COUNT(*) FROM silver.olist_order_payments
UNION ALL SELECT 'olist_order_reviews', COUNT(*) FROM silver.olist_order_reviews
UNION ALL SELECT 'olist_customers', COUNT(*) FROM silver.olist_customers
UNION ALL SELECT 'olist_sellers', COUNT(*) FROM silver.olist_sellers
UNION ALL SELECT 'olist_products', COUNT(*) FROM silver.olist_products
UNION ALL SELECT 'olist_category_translation', COUNT(*) FROM silver.olist_category_translation
UNION ALL SELECT 'olist_geolocation', COUNT(*) FROM silver.olist_geolocation
UNION ALL SELECT 'olist_marketing_qualified_leads', COUNT(*) FROM silver.olist_marketing_qualified_leads
UNION ALL SELECT 'olist_closed_deals', COUNT(*) FROM silver.olist_closed_deals
UNION ALL SELECT 'api_currency_rates', COUNT(*) FROM silver.api_currency_rates
UNION ALL SELECT 'api_brazil_holidays', COUNT(*) FROM silver.api_brazil_holidays
UNION ALL SELECT 'api_weather_history', COUNT(*) FROM silver.api_weather_history
ORDER BY table_name;

-- ============================================================================
-- COMPLETION
-- ============================================================================

\echo ''
\echo '============================================================'
\echo 'ANALYSIS COMPLETE!'
\echo '============================================================'
\echo ''
\echo 'All 14 Silver layer tables analyzed.'
\echo 'Export results and fill in silver_layer_findings_template.md'
\echo '============================================================'
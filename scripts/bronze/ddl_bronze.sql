/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Drop all tables first (in reverse dependency order for safety)
DROP TABLE IF EXISTS bronze.olist_order_reviews;
DROP TABLE IF EXISTS bronze.olist_order_payments;
DROP TABLE IF EXISTS bronze.olist_order_items;
DROP TABLE IF EXISTS bronze.olist_orders;
DROP TABLE IF EXISTS bronze.olist_customers;
DROP TABLE IF EXISTS bronze.olist_products;
DROP TABLE IF EXISTS bronze.olist_product_category_name_translation;
DROP TABLE IF EXISTS bronze.olist_sellers;
DROP TABLE IF EXISTS bronze.olist_geolocation;

/*
===============================================================================
CUSTOMERS TABLE
===============================================================================
*/
CREATE TABLE bronze.olist_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) UNIQUE NOT NULL,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

/*
===============================================================================
ORDERS TABLE
===============================================================================
*/
CREATE TABLE bronze.olist_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE bronze.olist_order_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE bronze.olist_order_payments (
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE bronze.olist_order_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

/*
===============================================================================
PRODUCTS TABLE
===============================================================================
*/
CREATE TABLE bronze.olist_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE bronze.olist_product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

/*
===============================================================================
SELLERS TABLE
===============================================================================
*/
CREATE TABLE bronze.olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state CHAR(2)
);

/*
===============================================================================
GEOLOCATION TABLE
===============================================================================
*/
CREATE TABLE bronze.olist_geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(100),
    geolocation_state CHAR(2)
);
/*
================================================================================
Description: Initialize Olist Data Warehouse database and schemas
================================================================================

PURPOSE:
--------
This script creates the foundational database structure for the Olist E-Commerce
Data Warehouse following Medallion Architecture (Bronze, Silver, Gold layers).

PREREQUISITES:
--------------
- PostgreSQL 16+ installed
- Connected as superuser or user with CREATE DATABASE privileges
- Run from psql or DBeaver/pgAdmin

NOTES:
------
- If database already exists, drop it first (WARNING: destroys all data)
- Each schema represents a Medallion Architecture layer
================================================================================
*/

-- ============================================================================
-- STEP 1: Create Database (Run this part separately if needed)
-- ============================================================================
-- NOTE: You cannot run CREATE DATABASE inside a transaction block
-- Run this command separately in psql or pgAdmin:

-- DROP DATABASE IF EXISTS olist_dwh;
-- CREATE DATABASE olist_dwh;

-- After creating the database, connect to it:
-- \c olist_dwh

-- ============================================================================
-- STEP 2: Create Schemas (Run after connecting to olist_dwh database)
-- ============================================================================

-- Drop schemas if they exist (for clean rebuild)
-- WARNING: This will delete ALL data in these schemas!
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

-- Create Bronze Schema
-- Purpose: Raw data layer - stores data exactly as received from source systems
-- Load Method: Full load (truncate and insert)
-- Transformations: None - data is stored as-is
CREATE SCHEMA bronze;
COMMENT ON SCHEMA bronze IS 'Raw data layer - stores data exactly as received from source systems without any transformations';

-- Create Silver Schema
-- Purpose: Cleaned data layer - data cleansing, standardization, normalization
-- Load Method: Full load (truncate and insert)
-- Transformations: Data type casting, NULL handling, deduplication, derived columns
CREATE SCHEMA silver;
COMMENT ON SCHEMA silver IS 'Cleaned data layer - standardized, deduplicated, and enriched data ready for modeling';

-- Create Gold Schema
-- Purpose: Business-ready layer - dimensional model (star schema)
-- Object Type: Views (not tables)
-- Transformations: Data integration, aggregations, business logic
CREATE SCHEMA gold;
COMMENT ON SCHEMA gold IS 'Business-ready layer - star schema dimensional model for analytics and reporting';

-- ============================================================================
-- STEP 3: Verify Schema Creation
-- ============================================================================

-- List all schemas in the database
SELECT
    schema_name,
    schema_owner
FROM information_schema.schemata
WHERE schema_name IN ('bronze', 'silver', 'gold')
ORDER BY schema_name;

-- ============================================================================
-- STEP 4: Grant Permissions (Optional - for multi-user environments)
-- ============================================================================

-- Uncomment and modify these if you need to grant access to other users:

-- GRANT USAGE ON SCHEMA bronze TO your_username;
-- GRANT USAGE ON SCHEMA silver TO your_username;
-- GRANT USAGE ON SCHEMA gold TO your_username;

-- GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO your_username;
-- GRANT SELECT ON ALL TABLES IN SCHEMA silver TO your_username;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO your_username;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Database initialization complete!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Schemas created:';
    RAISE NOTICE '  - bronze (raw data)';
    RAISE NOTICE '  - silver (cleaned data)';
    RAISE NOTICE '  - gold (business-ready)';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Next step: Run 02_create_bronze_tables.sql';
    RAISE NOTICE '========================================';
END $$;


-- ============================================================================
-- STEP 1: Create Database (Run this part separately if needed)
-- ============================================================================
-- NOTE: You cannot run CREATE DATABASE inside a transaction block
-- Run this command separately in psql or pgAdmin:

DROP DATABASE IF EXISTS olist_dwh;
CREATE DATABASE olist_dwh;

-- After creating the database, connect to it:
\c olist_dwh

-- ============================================================================
-- STEP 2: Create Schemas (Run after connecting to olist_dwh database)
-- ============================================================================

-- Drop schemas if they exist (for clean rebuild)
-- WARNING: This will delete ALL data in these schemas!
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

-- Create Bronze Schema
-- Purpose: Raw data layer - stores data exactly as received from source systems
-- Load Method: Full load (truncate and insert)
-- Transformations: None - data is stored as-is
CREATE SCHEMA bronze;
COMMENT ON SCHEMA bronze IS 'Raw data layer - stores data exactly as received from source systems without any transformations';

-- Create Silver Schema
-- Purpose: Cleaned data layer - data cleansing, standardization, normalization
-- Load Method: Full load (truncate and insert)
-- Transformations: Data type casting, NULL handling, deduplication, derived columns
CREATE SCHEMA silver;
COMMENT ON SCHEMA silver IS 'Cleaned data layer - standardized, deduplicated, and enriched data ready for modeling';

-- Create Gold Schema
-- Purpose: Business-ready layer - dimensional model (star schema)
-- Object Type: Views (not tables)
-- Transformations: Data integration, aggregations, business logic
CREATE SCHEMA gold;
COMMENT ON SCHEMA gold IS 'Business-ready layer - star schema dimensional model for analytics and reporting';

-- ============================================================================
-- STEP 3: Verify Schema Creation
-- ============================================================================

-- List all schemas in the database
SELECT
    schema_name,
    schema_owner
FROM information_schema.schemata
WHERE schema_name IN ('bronze', 'silver', 'gold')
ORDER BY schema_name;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Database initialization complete!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Schemas created:';
    RAISE NOTICE '  - bronze (raw data)';
    RAISE NOTICE '  - silver (cleaned data)';
    RAISE NOTICE '  - gold (business-ready)';
END $$;


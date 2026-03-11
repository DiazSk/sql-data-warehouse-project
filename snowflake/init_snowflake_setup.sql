/*
================================================================================
SNOWFLAKE SETUP: Database, Schemas, Warehouse, Stage & File Format
================================================================================

PURPOSE:
  One-time setup for the Olist Data Warehouse on Snowflake.
  Creates all infrastructure needed before loading data.

EXECUTION:
  Run this script FIRST, before any other scripts.
  Run in a Snowflake SQL Worksheet (Snowsight UI).

================================================================================
*/

-- ============================================================================
-- STEP 1: Create Database
-- ============================================================================

CREATE DATABASE IF NOT EXISTS OLIST_DWH;
USE DATABASE OLIST_DWH;

-- ============================================================================
-- STEP 2: Create Schemas (Medallion Architecture)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS BRONZE;   -- Raw data, no transformations
CREATE SCHEMA IF NOT EXISTS SILVER;   -- Cleaned, typed, validated
CREATE SCHEMA IF NOT EXISTS GOLD;     -- Star schema for analytics

-- Verify
SHOW SCHEMAS IN DATABASE OLIST_DWH;

-- ============================================================================
-- STEP 3: Create Virtual Warehouse (Compute)
-- ============================================================================
-- XS is sufficient for ~1.6M records
-- AUTO_SUSPEND = 60: shuts off after 60s idle (saves credits)
-- AUTO_RESUME = TRUE: spins up automatically on next query
-- This is Snowflake's key advantage: separation of storage and compute

CREATE WAREHOUSE IF NOT EXISTS OLIST_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE OLIST_WH;

-- ============================================================================
-- STEP 4: Create File Format for CSV Ingestion
-- ============================================================================

USE SCHEMA BRONZE;

CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

-- ============================================================================
-- STEP 5: Create Internal Stage for File Uploads
-- ============================================================================
-- Upload CSV files via Snowsight UI:
--   Data → Databases → OLIST_DWH → BRONZE → Stages → OLIST_STAGE → + Files

CREATE STAGE IF NOT EXISTS OLIST_STAGE
  FILE_FORMAT = CSV_FORMAT;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Confirm everything exists
SELECT CURRENT_DATABASE() AS database_name,
       CURRENT_WAREHOUSE() AS warehouse_name;

SHOW STAGES IN SCHEMA BRONZE;
SHOW FILE FORMATS IN SCHEMA BRONZE;

/*
================================================================================
NEXT STEPS:
  1. Upload 11 CSV files to OLIST_STAGE via Snowsight UI
     - datasets/e-commerce/ (9 files)
     - datasets/marketing_funnel/ (2 files)
  2. Verify: LIST @BRONZE.OLIST_STAGE;
  3. Run bronze_layer_setup.sql
================================================================================
*/
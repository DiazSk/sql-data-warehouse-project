-- =============================================================================
-- OLIST DATA WAREHOUSE - Database Initialization
-- =============================================================================
-- This script runs automatically on first container start
-- =============================================================================

-- Create schemas for Medallion Architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Set default search path
ALTER DATABASE olist_dwh SET search_path TO gold, silver, bronze, public;

-- Grant permissions
GRANT ALL ON SCHEMA bronze TO olist;
GRANT ALL ON SCHEMA silver TO olist;
GRANT ALL ON SCHEMA gold TO olist;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'OLIST DWH INITIALIZED SUCCESSFULLY!';
    RAISE NOTICE 'Schemas created: bronze, silver, gold';
    RAISE NOTICE '============================================';
END $$;
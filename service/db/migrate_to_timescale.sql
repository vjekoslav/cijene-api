-- Migration script to convert existing prices table to TimescaleDB hypertable
-- This script can be run on an existing database to migrate to TimescaleDB

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Function to safely migrate existing prices table to hypertable
CREATE OR REPLACE FUNCTION migrate_prices_to_timescale()
RETURNS void AS $$
DECLARE
    table_exists boolean;
    is_hypertable boolean;
    row_count bigint;
BEGIN
    -- Check if prices table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'prices'
    ) INTO table_exists;
    
    IF NOT table_exists THEN
        RAISE NOTICE 'Prices table does not exist. Nothing to migrate.';
        RETURN;
    END IF;
    
    -- Check if it's already a hypertable
    SELECT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables 
        WHERE hypertable_name = 'prices'
    ) INTO is_hypertable;
    
    IF is_hypertable THEN
        RAISE NOTICE 'Prices table is already a hypertable. Migration not needed.';
        RETURN;
    END IF;
    
    -- Get current row count
    SELECT COUNT(*) FROM prices INTO row_count;
    RAISE NOTICE 'Starting migration of % rows in prices table', row_count;
    
    -- Convert to hypertable
    -- Use 7 days as chunk interval (optimal for daily price data)
    PERFORM create_hypertable('prices', 'price_date', chunk_time_interval => INTERVAL '7 days');
    
    -- Create optimized indexes for time-series queries
    CREATE INDEX IF NOT EXISTS idx_prices_time_series ON prices (price_date, chain_product_id, store_id);
    CREATE INDEX IF NOT EXISTS idx_prices_product_time ON prices (chain_product_id, price_date);
    CREATE INDEX IF NOT EXISTS idx_prices_store_time ON prices (store_id, price_date);
    CREATE INDEX IF NOT EXISTS idx_prices_date_price ON prices (price_date, regular_price);
    
    RAISE NOTICE 'Successfully migrated prices table to TimescaleDB hypertable';
    RAISE NOTICE 'Created % chunks for existing data', (SELECT COUNT(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'prices');
    
END;
$$ LANGUAGE plpgsql;

-- Run the migration
SELECT migrate_prices_to_timescale();

-- Clean up the migration function
DROP FUNCTION IF EXISTS migrate_prices_to_timescale();
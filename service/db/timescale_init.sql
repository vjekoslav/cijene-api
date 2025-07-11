-- TimescaleDB initialization script
-- This script sets up TimescaleDB extension and converts the prices table to a hypertable

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Convert prices table to hypertable
-- This should be done after the table is created in psql.sql
-- We'll use a function to handle this safely
CREATE OR REPLACE FUNCTION convert_prices_to_hypertable()
RETURNS void AS $$
BEGIN
    -- Check if prices table exists and is not already a hypertable
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'prices') THEN
        -- Check if it's already a hypertable
        IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'prices') THEN
            -- Convert to hypertable partitioned by price_date
            -- Use 7 days as chunk interval (good for daily data)
            PERFORM create_hypertable('prices', 'price_date', chunk_time_interval => INTERVAL '7 days');
            
            -- Create indexes optimized for time-series queries
            CREATE INDEX IF NOT EXISTS idx_prices_time_series ON prices (price_date, chain_product_id, store_id);
            CREATE INDEX IF NOT EXISTS idx_prices_product_time ON prices (chain_product_id, price_date);
            CREATE INDEX IF NOT EXISTS idx_prices_store_time ON prices (store_id, price_date);
            
            RAISE NOTICE 'Successfully converted prices table to hypertable';
        ELSE
            RAISE NOTICE 'Prices table is already a hypertable';
        END IF;
    ELSE
        RAISE NOTICE 'Prices table does not exist yet';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger to call the conversion function after prices table is created
-- This ensures the conversion happens even if the table is created later
CREATE OR REPLACE FUNCTION check_and_convert_prices()
RETURNS event_trigger AS $$
BEGIN
    -- Only run for CREATE TABLE events on the prices table
    IF TG_TAG = 'CREATE TABLE' THEN
        -- Small delay to ensure table is fully created
        PERFORM pg_sleep(0.1);
        PERFORM convert_prices_to_hypertable();
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create event trigger (only if it doesn't exist)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'convert_prices_trigger') THEN
        CREATE EVENT TRIGGER convert_prices_trigger
        ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE')
        EXECUTE FUNCTION check_and_convert_prices();
    END IF;
END $$;

-- Also try to convert now in case the table already exists
SELECT convert_prices_to_hypertable();
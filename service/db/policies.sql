-- TimescaleDB policies for data management
-- These policies handle compression, retention, and maintenance automatically

-- Compression policy for prices table
-- Compress data older than 30 days to save ~70-90% storage
SELECT add_compression_policy('prices', INTERVAL '30 days');

-- Retention policy for prices table
-- Keep detailed price data for 5 years, then automatically delete
-- Adjust this based on your business requirements
SELECT add_retention_policy('prices', INTERVAL '5 years');

-- Compression policies for continuous aggregates
-- Compress aggregate data older than 7 days
SELECT add_compression_policy('daily_price_stats', INTERVAL '7 days');
SELECT add_compression_policy('weekly_price_trends', INTERVAL '7 days');
SELECT add_compression_policy('daily_chain_comparison', INTERVAL '7 days');
SELECT add_compression_policy('daily_store_stats', INTERVAL '7 days');
SELECT add_compression_policy('daily_category_trends', INTERVAL '7 days');

-- Retention policies for continuous aggregates
-- Keep aggregated data for longer periods since they're smaller
SELECT add_retention_policy('daily_price_stats', INTERVAL '10 years');
SELECT add_retention_policy('weekly_price_trends', INTERVAL '10 years');
SELECT add_retention_policy('daily_chain_comparison', INTERVAL '10 years');
SELECT add_retention_policy('daily_store_stats', INTERVAL '10 years');
SELECT add_retention_policy('daily_category_trends', INTERVAL '10 years');

-- Reorder policy to optimize queries
-- This reorganizes data within chunks for better query performance
SELECT add_reorder_policy('prices', 'idx_prices_time_series');

-- Statistics update policy
-- Keep table statistics fresh for better query planning
CREATE OR REPLACE FUNCTION update_prices_statistics()
RETURNS void AS $$
BEGIN
    -- Update statistics on the prices table
    ANALYZE prices;
    
    -- Update statistics on continuous aggregates
    ANALYZE daily_price_stats;
    ANALYZE weekly_price_trends;
    ANALYZE daily_chain_comparison;
    ANALYZE daily_store_stats;
    ANALYZE daily_category_trends;
    
    -- Log the update
    RAISE NOTICE 'Statistics updated for prices and continuous aggregates at %', now();
END;
$$ LANGUAGE plpgsql;

-- Schedule statistics updates daily
SELECT cron.schedule('update-prices-stats', '0 2 * * *', 'SELECT update_prices_statistics();');

-- Create a function to show TimescaleDB policy status
CREATE OR REPLACE FUNCTION show_timescale_policies()
RETURNS TABLE (
    policy_type text,
    hypertable_name text,
    policy_name text,
    config jsonb,
    enabled boolean
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'compression'::text,
        h.hypertable_name,
        p.application_name,
        p.config,
        p.enabled
    FROM timescaledb_information.jobs p
    JOIN timescaledb_information.hypertables h ON h.hypertable_id = (p.config->>'hypertable_id')::int
    WHERE p.proc_name = 'policy_compression'
    
    UNION ALL
    
    SELECT 
        'retention'::text,
        h.hypertable_name,
        p.application_name,
        p.config,
        p.enabled
    FROM timescaledb_information.jobs p
    JOIN timescaledb_information.hypertables h ON h.hypertable_id = (p.config->>'hypertable_id')::int
    WHERE p.proc_name = 'policy_retention'
    
    UNION ALL
    
    SELECT 
        'reorder'::text,
        h.hypertable_name,
        p.application_name,
        p.config,
        p.enabled
    FROM timescaledb_information.jobs p
    JOIN timescaledb_information.hypertables h ON h.hypertable_id = (p.config->>'hypertable_id')::int
    WHERE p.proc_name = 'policy_reorder'
    
    UNION ALL
    
    SELECT 
        'continuous_aggregate'::text,
        h.hypertable_name,
        p.application_name,
        p.config,
        p.enabled
    FROM timescaledb_information.jobs p
    JOIN timescaledb_information.continuous_aggregates ca ON ca.materialized_hypertable_id = (p.config->>'mat_hypertable_id')::int
    JOIN timescaledb_information.hypertables h ON h.hypertable_id = ca.hypertable_id
    WHERE p.proc_name = 'policy_refresh_continuous_aggregate'
    
    ORDER BY policy_type, hypertable_name;
END;
$$ LANGUAGE plpgsql;

-- Create a function to show storage statistics
CREATE OR REPLACE FUNCTION show_storage_stats()
RETURNS TABLE (
    table_name text,
    total_size text,
    compressed_size text,
    uncompressed_size text,
    compression_ratio text,
    row_count bigint
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.hypertable_name::text,
        pg_size_pretty(cs.total_bytes) AS total_size,
        pg_size_pretty(cs.compressed_total_bytes) AS compressed_size,
        pg_size_pretty(cs.uncompressed_total_bytes) AS uncompressed_size,
        CASE 
            WHEN cs.uncompressed_total_bytes > 0 THEN 
                ROUND((cs.uncompressed_total_bytes::numeric - cs.compressed_total_bytes::numeric) / cs.uncompressed_total_bytes::numeric * 100, 2)::text || '%'
            ELSE 'N/A'
        END AS compression_ratio,
        cs.total_rows
    FROM timescaledb_information.hypertables t
    LEFT JOIN timescaledb_information.compression_settings cs ON t.hypertable_name = cs.hypertable_name
    ORDER BY cs.total_bytes DESC NULLS LAST;
END;
$$ LANGUAGE plpgsql;
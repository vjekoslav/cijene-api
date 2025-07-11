-- Continuous aggregates for real-time price analytics
-- These provide pre-computed statistics that update automatically as new data arrives

-- Daily price statistics per product across all stores
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_price_stats
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', price_date) AS bucket,
    chain_product_id,
    COUNT(*) AS store_count,
    AVG(regular_price) AS avg_price,
    MIN(regular_price) AS min_price,
    MAX(regular_price) AS max_price,
    STDDEV(regular_price) AS price_stddev,
    AVG(CASE WHEN special_price IS NOT NULL THEN special_price END) AS avg_special_price,
    COUNT(CASE WHEN special_price IS NOT NULL THEN 1 END) AS special_price_count,
    AVG(unit_price) AS avg_unit_price,
    AVG(CASE WHEN best_price_30 IS NOT NULL THEN best_price_30 END) AS avg_best_price_30
FROM prices
GROUP BY bucket, chain_product_id;

-- Add refresh policy to update continuously
SELECT add_continuous_aggregate_policy('daily_price_stats',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');

-- Weekly price trends per product
CREATE MATERIALIZED VIEW IF NOT EXISTS weekly_price_trends
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 week', price_date) AS bucket,
    chain_product_id,
    COUNT(*) AS total_records,
    AVG(regular_price) AS avg_price,
    MIN(regular_price) AS min_price,
    MAX(regular_price) AS max_price,
    FIRST(regular_price, price_date) AS first_price,
    LAST(regular_price, price_date) AS last_price,
    (LAST(regular_price, price_date) - FIRST(regular_price, price_date)) / FIRST(regular_price, price_date) * 100 AS price_change_pct
FROM prices
GROUP BY bucket, chain_product_id;

-- Add refresh policy for weekly trends
SELECT add_continuous_aggregate_policy('weekly_price_trends',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 week',
    schedule_interval => INTERVAL '6 hours');

-- Chain comparison statistics per day
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_chain_comparison
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', price_date) AS bucket,
    cp.chain_id,
    COUNT(DISTINCT cp.id) AS product_count,
    COUNT(*) AS price_records,
    AVG(p.regular_price) AS avg_price,
    MIN(p.regular_price) AS min_price,
    MAX(p.regular_price) AS max_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.regular_price) AS median_price,
    COUNT(CASE WHEN p.special_price IS NOT NULL THEN 1 END) AS special_offers_count
FROM prices p
JOIN chain_products cp ON p.chain_product_id = cp.id
GROUP BY bucket, cp.chain_id;

-- Add refresh policy for chain comparison
SELECT add_continuous_aggregate_policy('daily_chain_comparison',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '2 hours');

-- Store-level price statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_store_stats
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', price_date) AS bucket,
    store_id,
    COUNT(*) AS product_count,
    AVG(regular_price) AS avg_price,
    MIN(regular_price) AS min_price,
    MAX(regular_price) AS max_price,
    COUNT(CASE WHEN special_price IS NOT NULL THEN 1 END) AS special_offers_count,
    AVG(CASE WHEN special_price IS NOT NULL THEN 
        (regular_price - special_price) / regular_price * 100 
    END) AS avg_discount_pct
FROM prices
GROUP BY bucket, store_id;

-- Add refresh policy for store stats
SELECT add_continuous_aggregate_policy('daily_store_stats',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');

-- Product category trends (requires category data)
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_category_trends
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', price_date) AS bucket,
    cp.category,
    COUNT(*) AS product_count,
    AVG(p.regular_price) AS avg_price,
    MIN(p.regular_price) AS min_price,
    MAX(p.regular_price) AS max_price,
    COUNT(CASE WHEN p.special_price IS NOT NULL THEN 1 END) AS special_offers_count
FROM prices p
JOIN chain_products cp ON p.chain_product_id = cp.id
WHERE cp.category IS NOT NULL
GROUP BY bucket, cp.category;

-- Add refresh policy for category trends
SELECT add_continuous_aggregate_policy('daily_category_trends',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '2 hours');

-- Create indexes on continuous aggregates for better query performance
CREATE INDEX IF NOT EXISTS idx_daily_price_stats_bucket ON daily_price_stats (bucket);
CREATE INDEX IF NOT EXISTS idx_daily_price_stats_product ON daily_price_stats (chain_product_id);
CREATE INDEX IF NOT EXISTS idx_weekly_price_trends_bucket ON weekly_price_trends (bucket);
CREATE INDEX IF NOT EXISTS idx_daily_chain_comparison_bucket ON daily_chain_comparison (bucket);
CREATE INDEX IF NOT EXISTS idx_daily_chain_comparison_chain ON daily_chain_comparison (chain_id);
CREATE INDEX IF NOT EXISTS idx_daily_store_stats_bucket ON daily_store_stats (bucket);
CREATE INDEX IF NOT EXISTS idx_daily_store_stats_store ON daily_store_stats (store_id);
CREATE INDEX IF NOT EXISTS idx_daily_category_trends_bucket ON daily_category_trends (bucket);
CREATE INDEX IF NOT EXISTS idx_daily_category_trends_category ON daily_category_trends (category);
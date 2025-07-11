# TimescaleDB Integration

This project now includes TimescaleDB support for enhanced time-series analytics on price data.

## Setup

### New Installation

1. Start the services:
   ```bash
   docker-compose up -d
   ```

The TimescaleDB extension will be automatically configured.

### Existing Installation Migration

1. Stop the current services:
   ```bash
   docker-compose down
   ```

2. Run the migration script:
   ```bash
   docker-compose up -d db
   docker-compose exec db psql -U $POSTGRES_USER -d $POSTGRES_DB -f /docker-entrypoint-initdb.d/02-timescale.sql
   ```

3. Start all services:
   ```bash
   docker-compose up -d
   ```

## Features

### Hypertables
- **Prices table**: Automatically partitioned by date (7-day chunks)
- **Optimized indexes**: Time-series optimized query performance
- **Automatic compression**: 70-90% storage reduction on older data

### Continuous Aggregates
Pre-computed statistics that update automatically:

- **daily_price_stats**: Daily price statistics per product
- **weekly_price_trends**: Weekly price trends with change percentages
- **daily_chain_comparison**: Chain-level price comparisons
- **daily_store_stats**: Store-level statistics
- **daily_category_trends**: Category-level price trends

### Data Management Policies
- **Compression**: Automatic compression of data older than 30 days
- **Retention**: Automatic deletion of data older than 5 years (configurable)
- **Refresh**: Continuous aggregates refresh every 1-6 hours

## API Endpoints

### Analytics Endpoints (v1/analytics/)

#### Basic Analytics
```
GET /v1/analytics/price-stats/
```
Get daily price statistics with filters for date range and products.

```
GET /v1/analytics/price-trends/
```
Get weekly price trends showing price changes over time.

```
GET /v1/analytics/chain-comparison/
```
Compare price statistics across different retail chains.

```
GET /v1/analytics/store-stats/
```
Get store-level price statistics and performance metrics.

```
GET /v1/analytics/category-trends/
```
Analyze price trends by product category.

#### Advanced Price Trend Analysis
```
GET /v1/analytics/time-series/
```
Get detailed time-series price data for specific products with configurable intervals (daily, weekly, monthly). Perfect for charting and visualization.

```
GET /v1/analytics/trend-detection/
```
Detect increasing, decreasing, or stable price trends using statistical analysis. Includes:
- Linear regression analysis
- Correlation coefficients
- Confidence scores
- Trend strength indicators

```
GET /v1/analytics/seasonal-analysis/
```
Analyze seasonal price patterns to identify cyclical trends:
- Monthly, weekly, or quarterly analysis
- Seasonal indices
- Price change from baseline
- Detect seasonal variations

```
GET /v1/analytics/volatility-analysis/
```
Analyze price volatility and identify products with unstable pricing:
- Coefficient of variation
- Risk level classification (low/medium/high)
- Price range analysis
- Useful for risk assessment

#### System Information
```
GET /v1/analytics/storage-stats/
GET /v1/analytics/policies/
```
Monitor TimescaleDB storage usage and policy status.

## Manual Operations

### Check Storage Statistics
```sql
SELECT * FROM show_storage_stats();
```

### Check Policy Status
```sql
SELECT * FROM show_timescale_policies();
```

### Manual Compression
```sql
SELECT compress_chunk(chunk_name) FROM timescaledb_information.chunks 
WHERE hypertable_name = 'prices' AND NOT is_compressed;
```

### Manual Aggregate Refresh
```sql
CALL refresh_continuous_aggregate('daily_price_stats', '2024-01-01', '2024-12-31');
```

## Performance Benefits

- **10-100x faster** time-series queries on large datasets
- **70-90% storage reduction** through automatic compression
- **Sub-second response times** for complex analytics queries
- **Parallel processing** for aggregation operations

## Configuration

### Chunk Intervals
- **Prices**: 7 days (optimal for daily data collection)
- **Aggregates**: Auto-sized based on data volume

### Retention Policies
- **Raw prices**: 5 years (configurable in `policies.sql`)
- **Aggregates**: 10 years (smaller footprint)

### Compression Schedule
- **Raw data**: Compressed after 30 days
- **Aggregates**: Compressed after 7 days

## Troubleshooting

### Check Hypertable Status
```sql
SELECT * FROM timescaledb_information.hypertables;
```

### Check Chunk Health
```sql
SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = 'prices';
```

### Check Job Status
```sql
SELECT * FROM timescaledb_information.jobs;
```

### Manual Policy Management
```sql
-- Remove a policy
SELECT remove_compression_policy('prices');

-- Add a new policy
SELECT add_compression_policy('prices', INTERVAL '30 days');
```
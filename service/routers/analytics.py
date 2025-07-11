from __future__ import annotations

from decimal import Decimal
from datetime import date
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from service.config import settings
from service.routers.auth import RequireAuth

router = APIRouter(tags=["Analytics"], dependencies=[RequireAuth])
db = settings.get_db()


class PriceStatsResponse(BaseModel):
    """Price statistics response schema."""
    
    price_date: date = Field(..., description="Date of the statistics")
    chain_product_id: int = Field(..., description="Chain product ID")
    store_count: int = Field(..., description="Number of stores with this product")
    avg_price: Decimal = Field(..., description="Average price across stores")
    min_price: Decimal = Field(..., description="Minimum price across stores")
    max_price: Decimal = Field(..., description="Maximum price across stores")
    price_stddev: Optional[Decimal] = Field(None, description="Price standard deviation")
    special_offers_count: int = Field(..., description="Number of stores with special offers")
    avg_special_price: Optional[Decimal] = Field(None, description="Average special price")


class PriceTrendResponse(BaseModel):
    """Price trend response schema."""
    
    week_start: date = Field(..., description="Start of the week")
    chain_product_id: int = Field(..., description="Chain product ID")
    avg_price: Decimal = Field(..., description="Average price for the week")
    min_price: Decimal = Field(..., description="Minimum price for the week")
    max_price: Decimal = Field(..., description="Maximum price for the week")
    price_change_pct: Optional[Decimal] = Field(None, description="Price change percentage from week start to end")


class ChainComparisonResponse(BaseModel):
    """Chain comparison response schema."""
    
    price_date: date = Field(..., description="Date of comparison")
    chain_id: int = Field(..., description="Chain ID")
    product_count: int = Field(..., description="Number of products")
    avg_price: Decimal = Field(..., description="Average price across all products")
    min_price: Decimal = Field(..., description="Minimum price")
    max_price: Decimal = Field(..., description="Maximum price")
    median_price: Decimal = Field(..., description="Median price")
    special_offers_count: int = Field(..., description="Number of special offers")


class StoreStatsResponse(BaseModel):
    """Store statistics response schema."""
    
    price_date: date = Field(..., description="Date of statistics")
    store_id: int = Field(..., description="Store ID")
    product_count: int = Field(..., description="Number of products")
    avg_price: Decimal = Field(..., description="Average price")
    min_price: Decimal = Field(..., description="Minimum price")
    max_price: Decimal = Field(..., description="Maximum price")
    special_offers_count: int = Field(..., description="Number of special offers")
    avg_discount_pct: Optional[Decimal] = Field(None, description="Average discount percentage")


class CategoryTrendResponse(BaseModel):
    """Category trend response schema."""
    
    price_date: date = Field(..., description="Date of trend")
    category: str = Field(..., description="Product category")
    product_count: int = Field(..., description="Number of products in category")
    avg_price: Decimal = Field(..., description="Average price")
    min_price: Decimal = Field(..., description="Minimum price")
    max_price: Decimal = Field(..., description="Maximum price")
    special_offers_count: int = Field(..., description="Number of special offers")


class ProductTimeSeriesResponse(BaseModel):
    """Product time-series response schema."""
    
    chain_product_id: int = Field(..., description="Chain product ID")
    product_name: Optional[str] = Field(None, description="Product name")
    brand: Optional[str] = Field(None, description="Brand name")
    category: Optional[str] = Field(None, description="Product category")
    data_points: List[dict] = Field(..., description="Time-series data points")


class TrendDetectionResponse(BaseModel):
    """Trend detection response schema."""
    
    chain_product_id: int = Field(..., description="Chain product ID")
    product_name: Optional[str] = Field(None, description="Product name")
    brand: Optional[str] = Field(None, description="Brand name")
    trend_direction: str = Field(..., description="Trend direction: increasing, decreasing, stable")
    trend_strength: Decimal = Field(..., description="Trend strength (0-1)")
    price_change_pct: Decimal = Field(..., description="Total price change percentage")
    start_price: Decimal = Field(..., description="Starting price in period")
    end_price: Decimal = Field(..., description="Ending price in period")
    volatility: Decimal = Field(..., description="Price volatility (standard deviation)")
    confidence_score: Decimal = Field(..., description="Confidence in trend detection (0-1)")


class SeasonalAnalysisResponse(BaseModel):
    """Seasonal analysis response schema."""
    
    period: str = Field(..., description="Time period (month, week, quarter)")
    avg_price: Decimal = Field(..., description="Average price for period")
    price_change_from_baseline: Decimal = Field(..., description="Change from yearly average")
    seasonal_index: Decimal = Field(..., description="Seasonal index (1.0 = average)")
    product_count: int = Field(..., description="Number of products analyzed")


class PriceVolatilityResponse(BaseModel):
    """Price volatility response schema."""
    
    chain_product_id: int = Field(..., description="Chain product ID")
    product_name: Optional[str] = Field(None, description="Product name")
    avg_price: Decimal = Field(..., description="Average price")
    volatility: Decimal = Field(..., description="Price volatility (coefficient of variation)")
    min_price: Decimal = Field(..., description="Minimum price in period")
    max_price: Decimal = Field(..., description="Maximum price in period")
    price_range_pct: Decimal = Field(..., description="Price range as percentage of average")
    risk_level: str = Field(..., description="Risk level: low, medium, high")


class AnomalyDetectionResponse(BaseModel):
    """Anomaly detection response schema."""
    
    chain_product_id: int = Field(..., description="Chain product ID")
    product_name: Optional[str] = Field(None, description="Product name")
    brand: Optional[str] = Field(None, description="Brand name")
    anomaly_date: date = Field(..., description="Date when anomaly was detected")
    current_price: Decimal = Field(..., description="Current anomalous price")
    expected_price: Decimal = Field(..., description="Expected price based on historical data")
    price_deviation_pct: Decimal = Field(..., description="Percentage deviation from expected price")
    anomaly_type: str = Field(..., description="Type: 'spike', 'drop', 'outlier'")
    severity: str = Field(..., description="Severity: 'low', 'medium', 'high', 'critical'")
    anomaly_score: Decimal = Field(..., description="Anomaly score (0-1, higher = more unusual)")
    confidence: Decimal = Field(..., description="Confidence in anomaly detection (0-1)")
    historical_avg: Decimal = Field(..., description="Historical average price")
    z_score: Decimal = Field(..., description="Statistical z-score of the anomaly")


class CorrelationAnalysisResponse(BaseModel):
    """Correlation analysis response schema."""
    
    product_1_id: int = Field(..., description="First product ID")
    product_1_name: Optional[str] = Field(None, description="First product name")
    product_2_id: int = Field(..., description="Second product ID")
    product_2_name: Optional[str] = Field(None, description="Second product name")
    correlation_coefficient: Decimal = Field(..., description="Pearson correlation coefficient (-1 to 1)")
    correlation_strength: str = Field(..., description="Strength: 'weak', 'moderate', 'strong', 'very_strong'")
    correlation_type: str = Field(..., description="Type: 'positive', 'negative', 'none'")
    p_value: Optional[Decimal] = Field(None, description="Statistical significance (p-value)")
    sample_size: int = Field(..., description="Number of data points used")
    relationship_description: str = Field(..., description="Human-readable relationship description")


class MarketDynamicsResponse(BaseModel):
    """Market dynamics response schema."""
    
    category: str = Field(..., description="Product category")
    avg_correlation: Decimal = Field(..., description="Average correlation within category")
    market_cohesion: Decimal = Field(..., description="How unified the category pricing is (0-1)")
    price_leaders: List[dict] = Field(..., description="Products that lead price changes")
    price_followers: List[dict] = Field(..., description="Products that follow price changes")
    market_volatility: Decimal = Field(..., description="Overall category volatility")
    dominant_trends: List[str] = Field(..., description="Main market trends detected")


@router.get("/price-stats/", summary="Get daily price statistics")
async def get_price_stats(
    start_date: date = Query(..., description="Start date for statistics"),
    end_date: date = Query(..., description="End date for statistics"),
    chain_product_id: Optional[int] = Query(None, description="Filter by specific chain product ID"),
) -> List[PriceStatsResponse]:
    """
    Get daily price statistics from the continuous aggregate.
    
    This endpoint leverages TimescaleDB's continuous aggregates for fast queries
    over large time ranges.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    # Limit range to prevent excessive data
    if (end_date - start_date).days > 365:
        raise HTTPException(status_code=400, detail="Date range cannot exceed 365 days")
    
    query = """
    SELECT 
        bucket::date as date,
        chain_product_id,
        store_count,
        avg_price,
        min_price,
        max_price,
        price_stddev,
        special_price_count,
        avg_special_price
    FROM daily_price_stats
    WHERE bucket >= $1 AND bucket <= $2
    """
    
    params = [start_date, end_date]
    
    if chain_product_id:
        query += " AND chain_product_id = $3"
        params.append(chain_product_id)
    
    query += " ORDER BY bucket, chain_product_id"
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, *params)
    
    return [
        PriceStatsResponse(
            price_date=row["date"],
            chain_product_id=row["chain_product_id"],
            store_count=row["store_count"],
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            max_price=row["max_price"],
            price_stddev=row["price_stddev"],
            special_offers_count=row["special_price_count"],
            avg_special_price=row["avg_special_price"],
        )
        for row in results
    ]


@router.get("/price-trends/", summary="Get weekly price trends")
async def get_price_trends(
    start_date: date = Query(..., description="Start date for trends"),
    end_date: date = Query(..., description="End date for trends"),
    chain_product_id: Optional[int] = Query(None, description="Filter by specific chain product ID"),
) -> List[PriceTrendResponse]:
    """
    Get weekly price trends showing price changes over time.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    query = """
    SELECT 
        bucket::date as week_start,
        chain_product_id,
        avg_price,
        min_price,
        max_price,
        price_change_pct
    FROM weekly_price_trends
    WHERE bucket >= $1 AND bucket <= $2
    """
    
    params = [start_date, end_date]
    
    if chain_product_id:
        query += " AND chain_product_id = $3"
        params.append(chain_product_id)
    
    query += " ORDER BY bucket, chain_product_id"
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, *params)
    
    return [
        PriceTrendResponse(
            week_start=row["week_start"],
            chain_product_id=row["chain_product_id"],
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            max_price=row["max_price"],
            price_change_pct=row["price_change_pct"],
        )
        for row in results
    ]


@router.get("/chain-comparison/", summary="Compare chains by price")
async def get_chain_comparison(
    start_date: date = Query(..., description="Start date for comparison"),
    end_date: date = Query(..., description="End date for comparison"),
    chain_ids: Optional[str] = Query(None, description="Comma-separated chain IDs to compare"),
) -> List[ChainComparisonResponse]:
    """
    Compare price statistics across different chains.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    query = """
    SELECT 
        bucket::date as date,
        chain_id,
        product_count,
        avg_price,
        min_price,
        max_price,
        median_price,
        special_offers_count
    FROM daily_chain_comparison
    WHERE bucket >= $1 AND bucket <= $2
    """
    
    params = [start_date, end_date]
    
    if chain_ids:
        chain_id_list = [int(id.strip()) for id in chain_ids.split(",")]
        query += " AND chain_id = ANY($3)"
        params.append(chain_id_list)
    
    query += " ORDER BY bucket, chain_id"
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, *params)
    
    return [
        ChainComparisonResponse(
            price_date=row["date"],
            chain_id=row["chain_id"],
            product_count=row["product_count"],
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            max_price=row["max_price"],
            median_price=row["median_price"],
            special_offers_count=row["special_offers_count"],
        )
        for row in results
    ]


@router.get("/store-stats/", summary="Get store-level statistics")
async def get_store_stats(
    start_date: date = Query(..., description="Start date for statistics"),
    end_date: date = Query(..., description="End date for statistics"),
    store_ids: Optional[str] = Query(None, description="Comma-separated store IDs"),
) -> List[StoreStatsResponse]:
    """
    Get store-level price statistics.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    query = """
    SELECT 
        bucket::date as date,
        store_id,
        product_count,
        avg_price,
        min_price,
        max_price,
        special_offers_count,
        avg_discount_pct
    FROM daily_store_stats
    WHERE bucket >= $1 AND bucket <= $2
    """
    
    params = [start_date, end_date]
    
    if store_ids:
        store_id_list = [int(id.strip()) for id in store_ids.split(",")]
        query += " AND store_id = ANY($3)"
        params.append(store_id_list)
    
    query += " ORDER BY bucket, store_id"
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, *params)
    
    return [
        StoreStatsResponse(
            price_date=row["date"],
            store_id=row["store_id"],
            product_count=row["product_count"],
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            max_price=row["max_price"],
            special_offers_count=row["special_offers_count"],
            avg_discount_pct=row["avg_discount_pct"],
        )
        for row in results
    ]


@router.get("/category-trends/", summary="Get category price trends")
async def get_category_trends(
    start_date: date = Query(..., description="Start date for trends"),
    end_date: date = Query(..., description="End date for trends"),
    categories: Optional[str] = Query(None, description="Comma-separated category names"),
) -> List[CategoryTrendResponse]:
    """
    Get price trends by product category.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    query = """
    SELECT 
        bucket::date as date,
        category,
        product_count,
        avg_price,
        min_price,
        max_price,
        special_offers_count
    FROM daily_category_trends
    WHERE bucket >= $1 AND bucket <= $2
    """
    
    params = [start_date, end_date]
    
    if categories:
        category_list = [cat.strip() for cat in categories.split(",")]
        query += " AND category = ANY($3)"
        params.append(category_list)
    
    query += " ORDER BY bucket, category"
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, *params)
    
    return [
        CategoryTrendResponse(
            price_date=row["date"],
            category=row["category"],
            product_count=row["product_count"],
            avg_price=row["avg_price"],
            min_price=row["min_price"],
            max_price=row["max_price"],
            special_offers_count=row["special_offers_count"],
        )
        for row in results
    ]


@router.get("/storage-stats/", summary="Get TimescaleDB storage statistics")
async def get_storage_stats():
    """
    Get TimescaleDB storage and compression statistics.
    
    This endpoint shows how much storage is being used and compression ratios.
    """
    query = "SELECT * FROM show_storage_stats()"
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query)
    
    return [
        {
            "table_name": row["table_name"],
            "total_size": row["total_size"],
            "compressed_size": row["compressed_size"],
            "uncompressed_size": row["uncompressed_size"],
            "compression_ratio": row["compression_ratio"],
            "row_count": row["row_count"],
        }
        for row in results
    ]


@router.get("/policies/", summary="Get TimescaleDB policy status")
async def get_policies():
    """
    Get TimescaleDB policy status (compression, retention, etc.).
    """
    query = "SELECT * FROM show_timescale_policies()"
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query)
    
    return [
        {
            "policy_type": row["policy_type"],
            "hypertable_name": row["hypertable_name"],
            "policy_name": row["policy_name"],
            "config": row["config"],
            "enabled": row["enabled"],
        }
        for row in results
    ]


@router.get("/time-series/", summary="Get product price time-series data")
async def get_product_time_series(
    chain_product_ids: str = Query(..., description="Comma-separated chain product IDs"),
    start_date: date = Query(..., description="Start date for time-series"),
    end_date: date = Query(..., description="End date for time-series"),
    interval: str = Query("1 day", description="Time interval: '1 day', '1 week', '1 month'"),
) -> List[ProductTimeSeriesResponse]:
    """
    Get time-series price data for specific products.
    
    This endpoint provides detailed price evolution over time for analysis and visualization.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    # Parse product IDs
    try:
        product_ids = [int(id.strip()) for id in chain_product_ids.split(",")]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid chain product IDs")
    
    # Validate interval
    valid_intervals = ['1 day', '1 week', '1 month']
    if interval not in valid_intervals:
        raise HTTPException(status_code=400, detail=f"Invalid interval. Must be one of: {valid_intervals}")
    
    query = f"""
    WITH time_series AS (
        SELECT 
            p.chain_product_id,
            time_bucket(INTERVAL '{interval}', p.price_date) as bucket,
            AVG(p.regular_price) as avg_price,
            MIN(p.regular_price) as min_price,
            MAX(p.regular_price) as max_price,
            COUNT(*) as store_count,
            STDDEV(p.regular_price) as price_stddev,
            COUNT(CASE WHEN p.special_price IS NOT NULL THEN 1 END) as special_offers
        FROM prices p
        WHERE p.chain_product_id = ANY($1)
          AND p.price_date >= $2 
          AND p.price_date <= $3
        GROUP BY p.chain_product_id, bucket
        ORDER BY p.chain_product_id, bucket
    ),
    product_info AS (
        SELECT cp.id, cp.name, cp.brand, cp.category
        FROM chain_products cp
        WHERE cp.id = ANY($1)
    )
    SELECT 
        ts.chain_product_id,
        pi.name as product_name,
        pi.brand,
        pi.category,
        json_agg(
            json_build_object(
                'date', ts.bucket::date,
                'avg_price', ts.avg_price,
                'min_price', ts.min_price,
                'max_price', ts.max_price,
                'store_count', ts.store_count,
                'volatility', ts.price_stddev,
                'special_offers', ts.special_offers
            ) ORDER BY ts.bucket
        ) as data_points
    FROM time_series ts
    LEFT JOIN product_info pi ON ts.chain_product_id = pi.id
    GROUP BY ts.chain_product_id, pi.name, pi.brand, pi.category
    ORDER BY ts.chain_product_id
    """
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, product_ids, start_date, end_date)
    
    import json
    
    return [
        ProductTimeSeriesResponse(
            chain_product_id=row["chain_product_id"],
            product_name=row["product_name"],
            brand=row["brand"],
            category=row["category"],
            data_points=json.loads(row["data_points"]) if isinstance(row["data_points"], str) else row["data_points"],
        )
        for row in results
    ]


@router.get("/trend-detection/", summary="Detect price trends for products")
async def detect_price_trends(
    start_date: date = Query(..., description="Start date for trend analysis"),
    end_date: date = Query(..., description="End date for trend analysis"),
    min_data_points: int = Query(5, description="Minimum data points required for trend analysis"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    trend_threshold: float = Query(0.05, description="Minimum price change % to detect trend (0.05 = 5%)"),
) -> List[TrendDetectionResponse]:
    """
    Detect increasing, decreasing, or stable price trends for products.
    
    Uses statistical analysis to identify significant price movements and trend strength.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    if (end_date - start_date).days < min_data_points:
        raise HTTPException(status_code=400, detail=f"Date range must span at least {min_data_points} days")
    
    query = """
    WITH price_series AS (
        SELECT 
            p.chain_product_id,
            p.price_date,
            AVG(p.regular_price) as avg_price,
            ROW_NUMBER() OVER (PARTITION BY p.chain_product_id ORDER BY p.price_date) as day_number
        FROM prices p
        JOIN chain_products cp ON p.chain_product_id = cp.id
        WHERE p.price_date >= $1 AND p.price_date <= $2
        AND ($3::text IS NULL OR cp.category = $3)
        GROUP BY p.chain_product_id, p.price_date
        HAVING COUNT(*) >= 1  -- At least one store has data
    ),
    trend_analysis AS (
        SELECT 
            ps.chain_product_id,
            COUNT(*) as data_points,
            MIN(ps.avg_price) as min_price,
            MAX(ps.avg_price) as max_price,
            AVG(ps.avg_price) as mean_price,
            STDDEV(ps.avg_price) as price_stddev,
            -- Get first and last prices
            (array_agg(ps.avg_price ORDER BY ps.price_date))[1] as start_price,
            (array_agg(ps.avg_price ORDER BY ps.price_date DESC))[1] as end_price,
            -- Linear regression slope calculation
            (COUNT(*) * SUM(ps.day_number * ps.avg_price) - SUM(ps.day_number) * SUM(ps.avg_price)) / 
            NULLIF((COUNT(*) * SUM(ps.day_number * ps.day_number) - SUM(ps.day_number) * SUM(ps.day_number)), 0) as slope,
            -- Correlation coefficient (R)
            (COUNT(*) * SUM(ps.day_number * ps.avg_price) - SUM(ps.day_number) * SUM(ps.avg_price)) / 
            NULLIF(SQRT((COUNT(*) * SUM(ps.day_number * ps.day_number) - SUM(ps.day_number) * SUM(ps.day_number)) * 
                 (COUNT(*) * SUM(ps.avg_price * ps.avg_price) - SUM(ps.avg_price) * SUM(ps.avg_price))), 0) as correlation
        FROM price_series ps
        GROUP BY ps.chain_product_id
        HAVING COUNT(*) >= $4
    ),
    trend_classification AS (
        SELECT 
            ta.*,
            CASE 
                WHEN ABS((end_price - start_price) / start_price) < $5 THEN 'stable'
                WHEN end_price > start_price THEN 'increasing'
                ELSE 'decreasing'
            END as trend_direction,
            ABS((end_price - start_price) / start_price) as price_change_pct,
            ABS(correlation) as trend_strength,
            CASE 
                WHEN data_points >= 10 AND ABS(correlation) >= 0.7 THEN ABS(correlation)
                WHEN data_points >= 7 AND ABS(correlation) >= 0.6 THEN ABS(correlation) * 0.8
                WHEN data_points >= 5 AND ABS(correlation) >= 0.5 THEN ABS(correlation) * 0.6
                ELSE ABS(correlation) * 0.4
            END as confidence_score,
            price_stddev / NULLIF(mean_price, 0) as coefficient_of_variation
        FROM trend_analysis ta
    )
    SELECT 
        tc.chain_product_id,
        cp.name as product_name,
        cp.brand,
        tc.trend_direction,
        tc.trend_strength,
        tc.price_change_pct * 100 as price_change_pct,
        tc.start_price,
        tc.end_price,
        tc.coefficient_of_variation as volatility,
        tc.confidence_score
    FROM trend_classification tc
    LEFT JOIN chain_products cp ON tc.chain_product_id = cp.id
    WHERE tc.confidence_score >= 0.3  -- Only return trends with reasonable confidence
    ORDER BY tc.confidence_score DESC, tc.price_change_pct DESC
    """
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, start_date, end_date, category, min_data_points, trend_threshold)
    
    return [
        TrendDetectionResponse(
            chain_product_id=row["chain_product_id"],
            product_name=row["product_name"],
            brand=row["brand"],
            trend_direction=row["trend_direction"],
            trend_strength=row["trend_strength"] or Decimal("0"),
            price_change_pct=row["price_change_pct"] or Decimal("0"),
            start_price=row["start_price"],
            end_price=row["end_price"],
            volatility=row["volatility"] or Decimal("0"),
            confidence_score=row["confidence_score"] or Decimal("0"),
        )
        for row in results
    ]


@router.get("/seasonal-analysis/", summary="Analyze seasonal price patterns")
async def analyze_seasonal_patterns(
    analysis_type: str = Query("monthly", description="Analysis type: 'monthly', 'weekly', 'quarterly'"),
    year: Optional[int] = Query(None, description="Year for analysis (defaults to current year)"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    min_products: int = Query(10, description="Minimum products required per period"),
) -> List[SeasonalAnalysisResponse]:
    """
    Analyze seasonal price patterns to identify cyclical trends.
    
    Detects seasonal variations in pricing across different time periods.
    """
    # Validate analysis type
    valid_types = ['monthly', 'weekly', 'quarterly']
    if analysis_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid analysis type. Must be one of: {valid_types}")
    
    # Set default year
    if year is None:
        year = date.today().year
    
    # Configure time grouping based on analysis type
    if analysis_type == 'monthly':
        time_extract = "EXTRACT(MONTH FROM p.price_date)"
        period_format = "'Month ' || EXTRACT(MONTH FROM p.price_date)"
    elif analysis_type == 'weekly':
        time_extract = "EXTRACT(WEEK FROM p.price_date)"
        period_format = "'Week ' || EXTRACT(WEEK FROM p.price_date)"
    else:  # quarterly
        time_extract = "EXTRACT(QUARTER FROM p.price_date)"
        period_format = "'Q' || EXTRACT(QUARTER FROM p.price_date)"
    
    query = f"""
    WITH period_prices AS (
        SELECT 
            {time_extract} as period_num,
            {period_format} as period_name,
            AVG(p.regular_price) as avg_price,
            COUNT(DISTINCT p.chain_product_id) as product_count
        FROM prices p
        JOIN chain_products cp ON p.chain_product_id = cp.id
        WHERE EXTRACT(YEAR FROM p.price_date) = $1
          AND ($2::text IS NULL OR cp.category = $2)
        GROUP BY period_num, period_name
        HAVING COUNT(DISTINCT p.chain_product_id) >= $3
    ),
    baseline AS (
        SELECT AVG(avg_price) as yearly_avg
        FROM period_prices
    )
    SELECT 
        pp.period_name as period,
        pp.avg_price,
        (pp.avg_price - b.yearly_avg) as price_change_from_baseline,
        pp.avg_price / NULLIF(b.yearly_avg, 0) as seasonal_index,
        pp.product_count
    FROM period_prices pp
    CROSS JOIN baseline b
    ORDER BY pp.period_num
    """
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, year, category, min_products)
    
    return [
        SeasonalAnalysisResponse(
            period=row["period"],
            avg_price=row["avg_price"],
            price_change_from_baseline=row["price_change_from_baseline"] or Decimal("0"),
            seasonal_index=row["seasonal_index"] or Decimal("1"),
            product_count=row["product_count"],
        )
        for row in results
    ]


@router.get("/volatility-analysis/", summary="Analyze price volatility")
async def analyze_price_volatility(
    start_date: date = Query(..., description="Start date for volatility analysis"),
    end_date: date = Query(..., description="End date for volatility analysis"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    min_data_points: int = Query(5, description="Minimum data points required"),
    sort_by: str = Query("volatility", description="Sort by: 'volatility', 'price_range', 'avg_price'"),
    limit: int = Query(100, description="Maximum number of results"),
) -> List[PriceVolatilityResponse]:
    """
    Analyze price volatility to identify products with unstable pricing.
    
    Useful for risk assessment and pricing strategy analysis.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    # Validate sort option
    valid_sorts = ['volatility', 'price_range', 'avg_price']
    if sort_by not in valid_sorts:
        raise HTTPException(status_code=400, detail=f"Invalid sort option. Must be one of: {valid_sorts}")
    
    # Configure sorting
    sort_mapping = {
        'volatility': 'coefficient_of_variation DESC',
        'price_range': 'price_range_pct DESC', 
        'avg_price': 'avg_price DESC'
    }
    sort_clause = sort_mapping[sort_by]
    
    query = f"""
    WITH price_stats AS (
        SELECT 
            p.chain_product_id,
            COUNT(*) as data_points,
            AVG(p.regular_price) as avg_price,
            MIN(p.regular_price) as min_price,
            MAX(p.regular_price) as max_price,
            STDDEV(p.regular_price) as price_stddev
        FROM prices p
        JOIN chain_products cp ON p.chain_product_id = cp.id
        WHERE p.price_date >= $1 AND p.price_date <= $2
          AND ($3::text IS NULL OR cp.category = $3)
        GROUP BY p.chain_product_id
        HAVING COUNT(*) >= $4
    ),
    volatility_analysis AS (
        SELECT 
            ps.*,
            ps.price_stddev / NULLIF(ps.avg_price, 0) as coefficient_of_variation,
            (ps.max_price - ps.min_price) / NULLIF(ps.avg_price, 0) * 100 as price_range_pct,
            CASE 
                WHEN ps.price_stddev / NULLIF(ps.avg_price, 0) < 0.1 THEN 'low'
                WHEN ps.price_stddev / NULLIF(ps.avg_price, 0) < 0.25 THEN 'medium'
                ELSE 'high'
            END as risk_level
        FROM price_stats ps
        WHERE ps.avg_price > 0
    )
    SELECT 
        va.chain_product_id,
        cp.name as product_name,
        va.avg_price,
        va.coefficient_of_variation as volatility,
        va.min_price,
        va.max_price,
        va.price_range_pct,
        va.risk_level
    FROM volatility_analysis va
    LEFT JOIN chain_products cp ON va.chain_product_id = cp.id
    ORDER BY {sort_clause}
    LIMIT $5
    """
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(query, start_date, end_date, category, min_data_points, limit)
    
    return [
        PriceVolatilityResponse(
            chain_product_id=row["chain_product_id"],
            product_name=row["product_name"],
            avg_price=row["avg_price"],
            volatility=row["volatility"] or Decimal("0"),
            min_price=row["min_price"],
            max_price=row["max_price"],
            price_range_pct=row["price_range_pct"] or Decimal("0"),
            risk_level=row["risk_level"],
        )
        for row in results
    ]


@router.get("/anomaly-detection/", summary="Detect price anomalies and outliers")
async def detect_price_anomalies(
    analysis_date: date = Query(..., description="Date to analyze for anomalies"),
    lookback_days: int = Query(30, description="Number of historical days to use for baseline"),
    sensitivity: float = Query(2.0, description="Anomaly sensitivity (z-score threshold, 2.0 = 95% confidence)"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    min_historical_data: int = Query(7, description="Minimum historical data points required"),
    limit: int = Query(100, description="Maximum number of anomalies to return"),
) -> List[AnomalyDetectionResponse]:
    """
    Detect price anomalies using statistical analysis.
    
    Uses z-score analysis to identify unusual price spikes, drops, or outliers
    compared to historical pricing patterns.
    """
    if lookback_days < min_historical_data:
        raise HTTPException(
            status_code=400, 
            detail=f"Lookback days must be at least {min_historical_data}"
        )
    
    from datetime import timedelta
    baseline_start = analysis_date - timedelta(days=lookback_days)
    
    query = """
    WITH historical_prices AS (
        SELECT 
            p.chain_product_id,
            p.price_date,
            AVG(p.regular_price) as daily_avg_price
        FROM prices p
        JOIN chain_products cp ON p.chain_product_id = cp.id
        WHERE p.price_date >= $1 AND p.price_date < $2
          AND ($3::text IS NULL OR cp.category = $3)
        GROUP BY p.chain_product_id, p.price_date
        HAVING COUNT(*) >= 1
    ),
    baseline_stats AS (
        SELECT 
            chain_product_id,
            COUNT(*) as data_points,
            AVG(daily_avg_price) as historical_avg,
            STDDEV(daily_avg_price) as historical_stddev,
            MIN(daily_avg_price) as historical_min,
            MAX(daily_avg_price) as historical_max
        FROM historical_prices
        GROUP BY chain_product_id
        HAVING COUNT(*) >= $4
    ),
    current_prices AS (
        SELECT 
            p.chain_product_id,
            AVG(p.regular_price) as current_price
        FROM prices p
        WHERE p.price_date = $2
        GROUP BY p.chain_product_id
    ),
    anomaly_analysis AS (
        SELECT 
            bs.chain_product_id,
            bs.historical_avg,
            bs.historical_stddev,
            cp.current_price,
            -- Z-score calculation
            CASE 
                WHEN bs.historical_stddev > 0 THEN 
                    ABS(cp.current_price - bs.historical_avg) / bs.historical_stddev
                ELSE 0
            END as z_score,
            -- Price deviation percentage
            ((cp.current_price - bs.historical_avg) / bs.historical_avg) * 100 as price_deviation_pct,
            -- Anomaly classification
            CASE 
                WHEN cp.current_price > bs.historical_avg * 1.5 THEN 'spike'
                WHEN cp.current_price < bs.historical_avg * 0.5 THEN 'drop'
                ELSE 'outlier'
            END as anomaly_type,
            bs.data_points
        FROM baseline_stats bs
        JOIN current_prices cp ON bs.chain_product_id = cp.chain_product_id
        WHERE bs.historical_stddev > 0
          AND ABS(cp.current_price - bs.historical_avg) / bs.historical_stddev >= $5
    ),
    severity_classification AS (
        SELECT 
            aa.*,
            CASE 
                WHEN aa.z_score >= 4.0 THEN 'critical'
                WHEN aa.z_score >= 3.0 THEN 'high'
                WHEN aa.z_score >= 2.5 THEN 'medium'
                ELSE 'low'
            END as severity,
            -- Anomaly score (0-1)
            LEAST(aa.z_score / 5.0, 1.0) as anomaly_score,
            -- Confidence based on historical data quality
            CASE 
                WHEN aa.data_points >= 25 THEN 0.95
                WHEN aa.data_points >= 15 THEN 0.85
                WHEN aa.data_points >= 10 THEN 0.75
                ELSE 0.6
            END as confidence
        FROM anomaly_analysis aa
    )
    SELECT 
        sc.chain_product_id,
        cp.name as product_name,
        cp.brand,
        $2::date as anomaly_date,
        sc.current_price,
        sc.historical_avg as expected_price,
        sc.price_deviation_pct,
        sc.anomaly_type,
        sc.severity,
        sc.anomaly_score,
        sc.confidence,
        sc.historical_avg,
        sc.z_score
    FROM severity_classification sc
    LEFT JOIN chain_products cp ON sc.chain_product_id = cp.id
    ORDER BY sc.anomaly_score DESC, sc.z_score DESC
    LIMIT $6
    """
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(
            query, 
            baseline_start, 
            analysis_date, 
            category, 
            min_historical_data, 
            sensitivity, 
            limit
        )
    
    return [
        AnomalyDetectionResponse(
            chain_product_id=row["chain_product_id"],
            product_name=row["product_name"],
            brand=row["brand"],
            anomaly_date=row["anomaly_date"],
            current_price=row["current_price"],
            expected_price=row["expected_price"],
            price_deviation_pct=row["price_deviation_pct"] or Decimal("0"),
            anomaly_type=row["anomaly_type"],
            severity=row["severity"],
            anomaly_score=row["anomaly_score"] or Decimal("0"),
            confidence=row["confidence"] or Decimal("0"),
            historical_avg=row["historical_avg"],
            z_score=row["z_score"] or Decimal("0"),
        )
        for row in results
    ]


@router.get("/correlation-analysis/", summary="Analyze price correlations between products")
async def analyze_price_correlations(
    start_date: date = Query(..., description="Start date for correlation analysis"),
    end_date: date = Query(..., description="End date for correlation analysis"),
    product_ids: Optional[str] = Query(None, description="Comma-separated product IDs (if not provided, analyzes top products)"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    min_correlation: float = Query(0.3, description="Minimum correlation coefficient to report"),
    max_pairs: int = Query(50, description="Maximum number of product pairs to analyze"),
    min_data_points: int = Query(10, description="Minimum shared data points required"),
) -> List[CorrelationAnalysisResponse]:
    """
    Analyze price correlations between products to understand market relationships.
    
    Calculates Pearson correlation coefficients to identify products with similar
    price movement patterns.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    if min_correlation < 0 or min_correlation > 1:
        raise HTTPException(status_code=400, detail="Minimum correlation must be between 0 and 1")
    
    # Parse product IDs if provided
    selected_products = None
    if product_ids:
        try:
            selected_products = [int(id.strip()) for id in product_ids.split(",")]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid product IDs")
    
    query = """
    WITH daily_prices AS (
        SELECT 
            p.chain_product_id,
            p.price_date,
            AVG(p.regular_price) as avg_price
        FROM prices p
        JOIN chain_products cp ON p.chain_product_id = cp.id
        WHERE p.price_date >= $1 AND p.price_date <= $2
          AND ($3::text IS NULL OR cp.category = $3)
          AND ($4::int[] IS NULL OR p.chain_product_id = ANY($4))
        GROUP BY p.chain_product_id, p.price_date
    ),
    product_pairs AS (
        SELECT DISTINCT
            dp1.chain_product_id as product_1_id,
            dp2.chain_product_id as product_2_id
        FROM daily_prices dp1
        JOIN daily_prices dp2 ON dp1.chain_product_id < dp2.chain_product_id
        -- Limit combinations to prevent explosion
        LIMIT $5
    ),
    price_correlations AS (
        SELECT 
            pp.product_1_id,
            pp.product_2_id,
            COUNT(*) as sample_size,
            -- Pearson correlation coefficient calculation
            (COUNT(*) * SUM(dp1.avg_price * dp2.avg_price) - SUM(dp1.avg_price) * SUM(dp2.avg_price)) /
            NULLIF(
                SQRT(
                    (COUNT(*) * SUM(dp1.avg_price * dp1.avg_price) - SUM(dp1.avg_price) * SUM(dp1.avg_price)) *
                    (COUNT(*) * SUM(dp2.avg_price * dp2.avg_price) - SUM(dp2.avg_price) * SUM(dp2.avg_price))
                ), 0
            ) as correlation_coefficient
        FROM product_pairs pp
        JOIN daily_prices dp1 ON pp.product_1_id = dp1.chain_product_id
        JOIN daily_prices dp2 ON pp.product_2_id = dp2.chain_product_id 
          AND dp1.price_date = dp2.price_date
        GROUP BY pp.product_1_id, pp.product_2_id
        HAVING COUNT(*) >= $6
    ),
    correlation_classification AS (
        SELECT 
            pc.*,
            ABS(pc.correlation_coefficient) as abs_correlation,
            CASE 
                WHEN ABS(pc.correlation_coefficient) >= 0.8 THEN 'very_strong'
                WHEN ABS(pc.correlation_coefficient) >= 0.6 THEN 'strong'
                WHEN ABS(pc.correlation_coefficient) >= 0.4 THEN 'moderate'
                ELSE 'weak'
            END as correlation_strength,
            CASE 
                WHEN pc.correlation_coefficient > 0.1 THEN 'positive'
                WHEN pc.correlation_coefficient < -0.1 THEN 'negative'
                ELSE 'none'
            END as correlation_type,
            CASE 
                WHEN pc.correlation_coefficient > 0.7 THEN 'Prices move together strongly'
                WHEN pc.correlation_coefficient > 0.4 THEN 'Prices tend to move in the same direction'
                WHEN pc.correlation_coefficient < -0.7 THEN 'Prices move in opposite directions strongly'
                WHEN pc.correlation_coefficient < -0.4 THEN 'Prices tend to move in opposite directions'
                ELSE 'No clear price relationship'
            END as relationship_description
        FROM price_correlations pc
        WHERE ABS(pc.correlation_coefficient) >= $7
    )
    SELECT 
        cc.product_1_id,
        cp1.name as product_1_name,
        cc.product_2_id,
        cp2.name as product_2_name,
        cc.correlation_coefficient,
        cc.correlation_strength,
        cc.correlation_type,
        cc.sample_size,
        cc.relationship_description
    FROM correlation_classification cc
    LEFT JOIN chain_products cp1 ON cc.product_1_id = cp1.id
    LEFT JOIN chain_products cp2 ON cc.product_2_id = cp2.id
    ORDER BY cc.abs_correlation DESC
    """
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(
            query,
            start_date,
            end_date,
            category,
            selected_products,
            max_pairs,
            min_data_points,
            min_correlation
        )
    
    return [
        CorrelationAnalysisResponse(
            product_1_id=row["product_1_id"],
            product_1_name=row["product_1_name"],
            product_2_id=row["product_2_id"],
            product_2_name=row["product_2_name"],
            correlation_coefficient=row["correlation_coefficient"] or Decimal("0"),
            correlation_strength=row["correlation_strength"],
            correlation_type=row["correlation_type"],
            p_value=None,  # Would require more complex statistical calculation
            sample_size=row["sample_size"],
            relationship_description=row["relationship_description"],
        )
        for row in results
    ]


@router.get("/market-dynamics/", summary="Analyze market dynamics and relationships")
async def analyze_market_dynamics(
    start_date: date = Query(..., description="Start date for market analysis"),
    end_date: date = Query(..., description="End date for market analysis"),
    categories: Optional[str] = Query(None, description="Comma-separated categories to analyze"),
    min_products_per_category: int = Query(5, description="Minimum products required per category"),
) -> List[MarketDynamicsResponse]:
    """
    Analyze overall market dynamics and relationships within product categories.
    
    Provides insights into market cohesion, price leadership, and category trends.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="End date must be after start date")
    
    # Parse categories if provided
    category_list = None
    if categories:
        category_list = [cat.strip() for cat in categories.split(",") if cat.strip()]
    
    query = """
    WITH daily_prices AS (
        SELECT 
            cp.category,
            p.chain_product_id,
            p.price_date,
            AVG(p.regular_price) as avg_price
        FROM prices p
        JOIN chain_products cp ON p.chain_product_id = cp.id
        WHERE p.price_date >= $1 AND p.price_date <= $2
          AND cp.category IS NOT NULL
          AND ($3::text[] IS NULL OR cp.category = ANY($3))
        GROUP BY cp.category, p.chain_product_id, p.price_date
    ),
    category_stats AS (
        SELECT 
            category,
            COUNT(DISTINCT chain_product_id) as product_count,
            AVG(avg_price) as category_avg_price,
            STDDEV(avg_price) as category_volatility
        FROM daily_prices
        GROUP BY category
        HAVING COUNT(DISTINCT chain_product_id) >= $4
    ),
    price_changes AS (
        SELECT 
            dp.category,
            dp.chain_product_id,
            dp.price_date,
            dp.avg_price,
            LAG(dp.avg_price) OVER (PARTITION BY dp.chain_product_id ORDER BY dp.price_date) as prev_price,
            (dp.avg_price - LAG(dp.avg_price) OVER (PARTITION BY dp.chain_product_id ORDER BY dp.price_date)) /
            NULLIF(LAG(dp.avg_price) OVER (PARTITION BY dp.chain_product_id ORDER BY dp.price_date), 0) as price_change_pct
        FROM daily_prices dp
    ),
    category_analysis AS (
        SELECT 
            cs.category,
            cs.product_count,
            cs.category_avg_price,
            cs.category_volatility,
            -- Calculate average correlation within category (simplified)
            CASE 
                WHEN cs.category_volatility > 0 THEN 
                    GREATEST(0, 1 - (cs.category_volatility / cs.category_avg_price))
                ELSE 0.5
            END as market_cohesion,
            cs.category_volatility / NULLIF(cs.category_avg_price, 0) as normalized_volatility
        FROM category_stats cs
    ),
    price_leaders AS (
        SELECT 
            pc.category,
            pc.chain_product_id,
            AVG(ABS(pc.price_change_pct)) as avg_change_magnitude,
            COUNT(*) as change_events,
            'leader' as role
        FROM price_changes pc
        WHERE pc.price_change_pct IS NOT NULL
          AND ABS(pc.price_change_pct) > 0.05  -- Changes > 5%
        GROUP BY pc.category, pc.chain_product_id
        HAVING COUNT(*) >= 3
    )
    SELECT 
        ca.category,
        ca.market_cohesion,
        ca.market_cohesion as avg_correlation,  -- Simplified correlation approximation
        ca.normalized_volatility as market_volatility,
        COALESCE(
            json_agg(
                json_build_object('product_id', pl.chain_product_id, 'avg_change', pl.avg_change_magnitude)
                ORDER BY pl.avg_change_magnitude DESC
            ) FILTER (WHERE pl.chain_product_id IS NOT NULL),
            '[]'::json
        ) as price_leaders,
        '[]'::json as price_followers,  -- Simplified for now
        ARRAY['price_volatility', 'market_cohesion'] as dominant_trends
    FROM category_analysis ca
    LEFT JOIN price_leaders pl ON ca.category = pl.category
    GROUP BY ca.category, ca.market_cohesion, ca.normalized_volatility
    ORDER BY ca.market_cohesion DESC
    """
    
    import json
    
    # Use the PostgreSQL connection directly since we need raw SQL
    async with db._get_conn() as conn:
        results = await conn.fetch(
            query,
            start_date,
            end_date,
            category_list,
            min_products_per_category
        )
    
    return [
        MarketDynamicsResponse(
            category=row["category"],
            avg_correlation=row["avg_correlation"] or Decimal("0"),
            market_cohesion=row["market_cohesion"] or Decimal("0"),
            price_leaders=json.loads(row["price_leaders"]) if isinstance(row["price_leaders"], str) else row["price_leaders"],
            price_followers=json.loads(row["price_followers"]) if isinstance(row["price_followers"], str) else row["price_followers"],
            market_volatility=row["market_volatility"] or Decimal("0"),
            dominant_trends=row["dominant_trends"] or [],
        )
        for row in results
    ]
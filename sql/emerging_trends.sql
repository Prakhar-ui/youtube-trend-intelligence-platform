-- =============================================================================
-- Dashboard 2: Trend Intelligence
-- "Which trends are gaining momentum?"
-- Source: yt_pipeline_gold_dev.{video_trends, category_analytics, trend_opportunities}
-- =============================================================================

-- Emerging trends: all EMERGING/RISING/CROSS_MARKET_EXPANSION opportunities, most recent snapshot
SELECT
    scope,
    region,
    entity_name,
    trend_type,
    trend_score,
    velocity_score,
    engagement_score,
    persistence_score,
    market_expansion_score,
    rank,
    evidence
FROM yt_pipeline_gold_dev.trend_opportunities
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.trend_opportunities)
  AND trend_type IN ('EMERGING', 'RISING_CATEGORY', 'CROSS_MARKET_EXPANSION')
ORDER BY trend_score DESC;

-- Trend velocity: video-level growth rate distribution, most recent snapshot, by region
SELECT
    region,
    trend_stage,
    COUNT(*) AS video_count,
    ROUND(AVG(view_growth_rate), 4) AS avg_growth_rate,
    ROUND(APPROX_PERCENTILE(view_growth_rate, 0.5), 4) AS median_growth_rate
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends)
  AND view_growth_rate IS NOT NULL
GROUP BY region, trend_stage
ORDER BY region, trend_stage;

-- Trend persistence: how long currently-trending videos have been trending, by trend_stage
SELECT
    trend_stage,
    COUNT(*) AS video_count,
    ROUND(AVG(trending_days_to_date), 1) AS avg_trending_days,
    ROUND(AVG(persistence_score), 1) AS avg_persistence_score
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends)
GROUP BY trend_stage
ORDER BY avg_persistence_score DESC;

-- Rising categories: positive growth, most recent snapshot, per region
SELECT
    region,
    category_name,
    total_views,
    category_growth_pct,
    category_momentum,
    category_rank,
    rank_change
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
  AND category_growth_pct > 0
ORDER BY category_growth_pct DESC
LIMIT 20;

-- Declining categories: negative growth, most recent snapshot, per region
SELECT
    region,
    category_name,
    total_views,
    category_growth_pct,
    category_momentum,
    category_rank,
    rank_change
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
  AND category_growth_pct < 0
ORDER BY category_growth_pct ASC
LIMIT 20;

-- Trend lifecycle: how many videos are in each trend_stage right now, all regions
SELECT
    trend_stage,
    COUNT(*) AS video_count
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends)
GROUP BY trend_stage
ORDER BY video_count DESC;

-- Top opportunity categories: highest trend_score, most recent snapshot
SELECT
    region,
    entity_name AS category_name,
    trend_type,
    trend_score,
    market_expansion_score,
    evidence
FROM yt_pipeline_gold_dev.trend_opportunities
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.trend_opportunities)
  AND scope = 'CATEGORY'
ORDER BY trend_score DESC
LIMIT 15;

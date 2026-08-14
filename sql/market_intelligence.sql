-- =============================================================================
-- Dashboard 3: Market Intelligence
-- "Where is the trend happening?"
-- Source: yt_pipeline_gold_dev.{category_analytics, trending_analytics, trend_opportunities}
-- =============================================================================

-- Market comparison: totals per region, most recent snapshot
SELECT
    region,
    total_videos,
    total_views,
    total_likes,
    total_comments,
    avg_views_per_video,
    avg_engagement_rate,
    unique_channels,
    unique_categories
FROM yt_pipeline_gold_dev.trending_analytics
WHERE trending_date_parsed = (SELECT MAX(trending_date_parsed) FROM yt_pipeline_gold_dev.trending_analytics)
ORDER BY total_views DESC;

-- Category x market heatmap: view share per category per region, most recent snapshot
SELECT
    region,
    category_name,
    total_views,
    view_share_pct,
    category_rank
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
ORDER BY region, category_rank;

-- Market-specific top trends: highest trend_score opportunity per region, most recent snapshot
SELECT
    region,
    scope,
    entity_name,
    trend_type,
    trend_score,
    evidence
FROM (
    SELECT
        region, scope, entity_name, trend_type, trend_score, evidence,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY trend_score DESC) AS rn
    FROM yt_pipeline_gold_dev.trend_opportunities
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.trend_opportunities)
      AND region IS NOT NULL
)
WHERE rn <= 5
ORDER BY region, trend_score DESC;

-- Cross-market trends: categories trending in more than one region, most recent snapshot,
-- with the number of markets and the region list -- the "is this spreading" view.
SELECT
    category_name,
    COUNT(DISTINCT region) AS market_count,
    SUM(total_views) AS total_views_all_markets,
    ARRAY_AGG(DISTINCT region) AS markets
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
GROUP BY category_name
HAVING COUNT(DISTINCT region) > 1
ORDER BY market_count DESC, total_views_all_markets DESC;

-- Global vs. local trends: classify every category by how many markets it appears in
SELECT
    category_name,
    COUNT(DISTINCT region) AS market_count,
    CASE
        WHEN COUNT(DISTINCT region) >= 7 THEN 'GLOBAL'
        WHEN COUNT(DISTINCT region) >= 2 THEN 'REGIONAL'
        ELSE 'LOCAL'
    END AS reach_classification
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
GROUP BY category_name
ORDER BY market_count DESC;

-- Category growth by market: side-by-side growth rate for a category across every region
-- it's trending in on the most recent snapshot (drives the "growing in India but flat in
-- the US" style comparison -- see docs/trend_intelligence.md section 4).
SELECT
    category_name,
    region,
    total_views,
    category_growth_pct,
    category_momentum
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
ORDER BY category_name, category_growth_pct DESC;

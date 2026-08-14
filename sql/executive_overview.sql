-- =============================================================================
-- Dashboard 1: Executive Trend Overview
-- "What is trending right now?"
-- Source: yt_pipeline_gold_dev.{video_trends, category_analytics, trending_analytics, trend_opportunities}
-- Each query below feeds one KPI tile or chart (see docs/dashboard.md for the
-- full layout). Written as separate statements -- run individually, or map
-- each one to a QuickSight CustomSql dataset (see terraform/quicksight/).
-- =============================================================================

-- KPI: markets monitored on the most recent snapshot date
SELECT COUNT(DISTINCT region) AS markets_monitored
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends);

-- KPI: videos analyzed on the most recent snapshot date
SELECT COUNT(DISTINCT video_id) AS videos_analyzed
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends);

-- KPI: emerging trends identified on the most recent snapshot date (all scopes)
SELECT COUNT(*) AS emerging_trends
FROM yt_pipeline_gold_dev.trend_opportunities
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.trend_opportunities)
  AND trend_type IN ('EMERGING', 'RISING_CATEGORY', 'CROSS_MARKET_EXPANSION');

-- KPI: top category by total views on the most recent snapshot date, across all markets
SELECT
    category_name,
    SUM(total_views) AS total_views_all_markets
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
GROUP BY category_name
ORDER BY total_views_all_markets DESC
LIMIT 1;

-- KPI: single highest-velocity video on the most recent snapshot date
SELECT
    title,
    channel_title,
    region,
    view_growth_rate,
    views
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends)
  AND view_growth_rate IS NOT NULL
ORDER BY view_growth_rate DESC
LIMIT 1;

-- KPI: highest-engagement category on the most recent snapshot date (across all markets, views-weighted)
SELECT
    category_name,
    SUM(total_views * avg_engagement_rate) / NULLIF(SUM(total_views), 0) AS weighted_avg_engagement_rate
FROM yt_pipeline_gold_dev.category_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.category_analytics)
GROUP BY category_name
ORDER BY weighted_avg_engagement_rate DESC
LIMIT 1;

-- Chart: trending volume over time (last 30 snapshots), all markets combined
SELECT
    trending_date_parsed AS snapshot_date,
    SUM(total_videos) AS total_videos,
    SUM(total_views) AS total_views
FROM yt_pipeline_gold_dev.trending_analytics
WHERE trending_date_parsed >= DATE_ADD('day', -30, (SELECT MAX(trending_date_parsed) FROM yt_pipeline_gold_dev.trending_analytics))
GROUP BY trending_date_parsed
ORDER BY trending_date_parsed;

-- Chart: top emerging categories on the most recent snapshot date
SELECT
    region,
    entity_name AS category_name,
    trend_score,
    velocity_score,
    market_expansion_score,
    evidence
FROM yt_pipeline_gold_dev.trend_opportunities
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.trend_opportunities)
  AND scope = 'CATEGORY'
  AND trend_type IN ('RISING_CATEGORY', 'CROSS_MARKET_EXPANSION')
ORDER BY trend_score DESC
LIMIT 10;

-- Chart: top emerging videos on the most recent snapshot date
SELECT
    region,
    entity_name AS video_title,
    trend_score,
    velocity_score,
    engagement_score,
    evidence
FROM yt_pipeline_gold_dev.trend_opportunities
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.trend_opportunities)
  AND scope = 'VIDEO'
ORDER BY trend_score DESC
LIMIT 10;

-- Chart: market comparison -- total views and video count per region, most recent snapshot date
SELECT
    region,
    total_videos,
    total_views,
    avg_views_per_video,
    unique_channels,
    unique_categories
FROM yt_pipeline_gold_dev.trending_analytics
WHERE trending_date_parsed = (SELECT MAX(trending_date_parsed) FROM yt_pipeline_gold_dev.trending_analytics)
ORDER BY total_views DESC;

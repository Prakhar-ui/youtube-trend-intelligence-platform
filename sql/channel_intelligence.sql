-- =============================================================================
-- Dashboard 4: Channel & Content Intelligence
-- "Who is driving the trends?"
-- Source: yt_pipeline_gold_dev.{channel_analytics, video_trends, trend_opportunities}
-- Note: the Glue Catalog table name is still "channel_analytics" (kept for
-- continuity from the original job) even though its grain/columns were
-- redesigned -- see docs/decisions.md.
-- =============================================================================

-- Top channels by cumulative views, most recent snapshot, per region
SELECT
    region,
    channel_title,
    total_views_to_date,
    trending_video_count_to_date,
    rank_in_region
FROM yt_pipeline_gold_dev.channel_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.channel_analytics)
ORDER BY region, rank_in_region
LIMIT 100;

-- Fastest-growing channels: highest trend_score, most recent snapshot
SELECT
    region,
    channel_title,
    trend_score,
    views_percentile,
    trending_frequency,
    markets_present
FROM yt_pipeline_gold_dev.channel_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.channel_analytics)
ORDER BY trend_score DESC
LIMIT 20;

-- Highest-engagement channels, most recent snapshot
SELECT
    region,
    channel_title,
    avg_engagement_rate_to_date,
    total_views_to_date,
    trending_video_count_to_date
FROM yt_pipeline_gold_dev.channel_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.channel_analytics)
ORDER BY avg_engagement_rate_to_date DESC
LIMIT 20;

-- Top videos, most recent snapshot, across all regions
SELECT
    region,
    title,
    channel_title,
    views,
    view_growth_rate,
    trend_stage,
    rank_in_region
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends)
ORDER BY views DESC
LIMIT 50;

-- Trending duration distribution: how many videos have trended for how many days,
-- most recent snapshot
SELECT
    trending_days_to_date,
    COUNT(*) AS video_count
FROM yt_pipeline_gold_dev.video_trends
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends)
GROUP BY trending_days_to_date
ORDER BY trending_days_to_date;

-- Multi-market channels: channels trending in more than one region, most recent snapshot
SELECT
    channel_title,
    MAX(markets_present) AS markets_present,
    SUM(total_views_to_date) AS total_views_across_regions_shown
FROM yt_pipeline_gold_dev.channel_analytics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.channel_analytics)
GROUP BY channel_title
HAVING MAX(markets_present) > 1
ORDER BY markets_present DESC, total_views_across_regions_shown DESC
LIMIT 30;

-- Channel / category relationship: which categories each top channel's trending videos fall into
SELECT
    vt.channel_title,
    ca.category_name,
    COUNT(*) AS trending_video_count,
    SUM(vt.views) AS total_views
FROM yt_pipeline_gold_dev.video_trends vt
JOIN yt_pipeline_gold_dev.category_analytics ca
    ON vt.category_id = ca.category_id
    AND vt.region = ca.region
    AND vt.snapshot_date = ca.snapshot_date
WHERE vt.snapshot_date = (SELECT MAX(snapshot_date) FROM yt_pipeline_gold_dev.video_trends)
GROUP BY vt.channel_title, ca.category_name
ORDER BY total_views DESC
LIMIT 50;

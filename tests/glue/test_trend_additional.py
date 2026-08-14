
from datetime import date

from pyspark.sql import functions as F


def test_compute_video_trends_engagement_change_and_persistence_cap(spark, trend_intelligence_module):
    module = trend_intelligence_module
    rows = []
    for i in range(8):
        rows.append(("v1", "us", date(2024, 7, 1 + i), 100 + i * 10, 10 + i, 2 + i, 0.1 + i * 0.01, 0.01 + i * 0.001))
    df = spark.createDataFrame(
        rows,
        "video_id string, region string, trending_date_parsed date, views long, likes long, comment_count long, like_rate double, comment_rate double",
    )
    result = module.compute_video_trends(df).orderBy("trending_date_parsed").collect()
    assert result[0]["engagement_change"] is None
    assert result[1]["engagement_change"] is not None
    assert result[-1]["persistence_score"] == 100.0
    assert result[-1]["first_trending_date"] == date(2024, 7, 1)


def test_build_category_opportunities_declining_and_missing_cross_market(spark, trend_intelligence_module):
    module = trend_intelligence_module
    category_df = spark.createDataFrame(
        [(10, "Music", "us", date(2024, 7, 17), 100, -10.0, 0.1, 1)],
        "category_id long, category_name string, region string, snapshot_date date, total_views long, category_growth_pct double, avg_engagement_rate double, unique_channels long",
    )
    cross = module.compute_cross_market_expansion(category_df)
    row = module.build_category_opportunities(category_df, cross).collect()[0]
    assert row["trend_type"] == "DECLINING_CATEGORY"
    assert row["market_expansion_score"] == 10.0
    assert row["scope"] == "CATEGORY"


def test_build_channel_opportunities_ranks_and_builds_evidence(spark, trend_intelligence_module):
    module = trend_intelligence_module
    df = spark.createDataFrame(
        [("c1", "Channel 1", "us", date(2024, 7, 17), 2, 0.5, 90.0, 90.0),
         ("c2", "Channel 2", "us", date(2024, 7, 17), 1, 0.2, 80.0, 70.0)],
        "channel_id string, channel_title string, region string, snapshot_date date, trending_video_count_to_date long, trending_frequency double, views_percentile double, trend_score double",
    ).withColumn("markets_present", F.lit(2))
    rows = module.build_channel_opportunities(df).orderBy("rank").collect()
    assert rows[0]["entity_id"] == "c1"
    assert rows[0]["rank"] == 1
    assert "Trending in 2 market(s)" in rows[0]["evidence"]
    assert rows[0]["trend_type"] == "HIGH_PERFORMING_CHANNEL"


def test_build_trend_opportunities_unions_all_scopes(spark, trend_intelligence_module):
    module = trend_intelligence_module
    video_df = spark.createDataFrame(
        [("v1", "us", date(2024, 7, 17), "V1", 100, 0, 0, 0.1, 0.01)],
        "video_id string, region string, trending_date_parsed date, title string, views long, likes long, comment_count long, like_rate double, comment_rate double",
    )
    video = module.compute_video_trends(video_df)
    category = spark.createDataFrame(
        [(10, "Music", "us", date(2024, 7, 17), 100, 1.0, 1)],
        "category_id long, category_name string, region string, snapshot_date date, total_views long, avg_engagement_rate double, unique_channels long",
    ).withColumn("category_growth_pct", F.lit(10.0))
    cross = module.compute_cross_market_expansion(category)
    channel = spark.createDataFrame(
        [("c1", "Channel", "us", date(2024, 7, 17), 1, 1.0, 100.0, 100.0)],
        "channel_id string, channel_title string, region string, snapshot_date date, trending_video_count_to_date long, trending_frequency double, views_percentile double, trend_score double",
    ).withColumn("markets_present", F.lit(1))
    result = module.build_trend_opportunities(video, category, cross, channel)
    assert {r["scope"] for r in result.select("scope").distinct().collect()} == {"VIDEO", "CATEGORY", "CHANNEL"}
    assert "_aggregated_at" in result.columns

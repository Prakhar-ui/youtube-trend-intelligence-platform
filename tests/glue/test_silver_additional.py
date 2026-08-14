
from datetime import date

from pyspark.sql import functions as F


def test_build_channel_analytics_falls_back_to_channel_title(spark, silver_to_gold_module):
    df = spark.createDataFrame(
        [("v1", "us", "Channel A", None, date(2024, 7, 17), 100, 10, 5, 0.15)],
        "video_id string, region string, channel_title string, channel_id string, trending_date_parsed date, views long, likes long, comment_count long, engagement_rate double",
    )
    row = silver_to_gold_module.build_channel_analytics(df).collect()[0]
    assert row["channel_id"] == "Channel A"
    assert row["markets_present"] == 1


def test_add_channel_trend_score_computes_percentile_weighted_score(spark, silver_to_gold_module):
    df = spark.createDataFrame(
        [("a", "us", date(2024, 7, 17), 1000, 1.0), ("b", "us", date(2024, 7, 17), 500, 0.5)],
        "channel_id string, region string, snapshot_date date, total_views_to_date long, trending_frequency double",
    )
    rows = {
        r["channel_id"]: r
        for r in silver_to_gold_module.add_channel_trend_score(df).collect()
    }
    assert rows["a"]["views_percentile"] == 100.0
    assert rows["b"]["views_percentile"] == 0.0
    assert rows["a"]["trend_score"] == 100.0
    assert rows["b"]["trend_score"] == 15.0


def test_build_category_analytics_rank_change_and_momentum(spark, silver_to_gold_module):
    df = spark.createDataFrame(
        [
            ("v1", "us", "Music", 10, "c1", date(2024, 7, 16), 100, 10, 5, 0.1),
            ("v2", "us", "Sports", 20, "c2", date(2024, 7, 16), 200, 20, 10, 0.1),
            ("v1", "us", "Music", 10, "c1", date(2024, 7, 17), 300, 30, 15, 0.1),
            ("v2", "us", "Sports", 20, "c2", date(2024, 7, 17), 100, 10, 5, 0.1),
        ],
        "video_id string, region string, category_name string, category_id long, channel_title string, trending_date_parsed date, views long, likes long, comment_count long, engagement_rate double",
    )
    rows = {(r["category_name"], r["trending_date_parsed"]): r for r in silver_to_gold_module.build_category_analytics(df).collect()}
    assert rows[("Music", date(2024, 7, 17))]["category_rank"] == 1
    assert rows[("Music", date(2024, 7, 17))]["previous_rank"] == 2
    assert rows[("Music", date(2024, 7, 17))]["rank_change"] == 1
    assert rows[("Music", date(2024, 7, 17))]["category_momentum"] == 200.0

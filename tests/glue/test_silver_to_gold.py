"""Tests for silver_to_gold_analytics.py transforms."""
from pyspark.sql import types as T
import pytest


def test_add_category_name(spark, silver_to_gold_module):
    module = silver_to_gold_module
    
    stats_schema = T.StructType([
        T.StructField('category_id', T.LongType()),
        T.StructField('region', T.StringType()),
        T.StructField('video_id', T.StringType()),
        T.StructField('views', T.LongType()),
    ])
    stats_data = [(10, 'us', 'vid_1', 100), (20, 'us', 'vid_2', 200)]
    stats_df = spark.createDataFrame(stats_data, stats_schema)
    
    ref_schema = T.StructType([
        T.StructField('id', T.StringType()),
        T.StructField('snippet_title', T.StringType()),
        T.StructField('region', T.StringType()),
    ])
    ref_data = [('10', 'Music', 'us'), ('20', 'Sports', 'us')]
    ref_df = spark.createDataFrame(ref_data, ref_schema)
    
    result_df = module.add_category_name(stats_df, ref_df)
    
    assert 'category_name' in result_df.columns
    assert result_df.filter(result_df.category_name == 'Music').count() == 1
    assert result_df.filter(result_df.category_name == 'Sports').count() == 1


def test_add_category_name_falls_back_to_unknown(spark, silver_to_gold_module):
    module = silver_to_gold_module
    
    stats_schema = T.StructType([
        T.StructField('category_id', T.LongType()),
        T.StructField('region', T.StringType()),
        T.StructField('video_id', T.StringType()),
        T.StructField('views', T.LongType()),
    ])
    stats_data = [(999, 'us', 'vid_1', 100)]
    stats_df = spark.createDataFrame(stats_data, stats_schema)
    
    ref_schema = T.StructType([
        T.StructField('id', T.StringType()),
        T.StructField('snippet_title', T.StringType()),
        T.StructField('region', T.StringType()),
    ])
    ref_data = [('10', 'Music', 'us')]
    ref_df = spark.createDataFrame(ref_data, ref_schema)
    
    result_df = module.add_category_name(stats_df, ref_df)
    
    assert 'Unknown Category 999' in result_df.select('category_name').collect()[0][0]


def test_build_trending_analytics(spark, silver_to_gold_module):
    module = silver_to_gold_module
    from datetime import date
    
    schema = 'video_id string, region string, trending_date_parsed date, views long, likes long, dislikes long, comment_count long, like_ratio double, engagement_rate double, channel_title string, category_id long'
    data = [('v1', 'us', date(2024, 7, 17), 100, 10, 1, 5, 0.1, 0.16, 'Chan1', 10)]
    df = spark.createDataFrame(data, schema)
    
    result_df = module.build_trending_analytics(df)
    
    assert result_df.count() == 1
    assert 'total_videos' in result_df.columns
    assert 'total_views' in result_df.columns
    assert 'avg_like_ratio' in result_df.columns
    assert '_aggregated_at' in result_df.columns


def test_build_channel_analytics_has_ranking(spark, silver_to_gold_module):
    module = silver_to_gold_module
    from datetime import date

    schema = 'video_id string, region string, channel_title string, channel_id string, trending_date_parsed date, category_name string, views long, likes long, comment_count long, engagement_rate double'
    data = [
        ('v1', 'us', 'ChanA', 'chanA_id', date(2024, 7, 17), 'Music', 300, 30, 15, 0.15),
        ('v2', 'us', 'ChanB', 'chanB_id', date(2024, 7, 17), 'Sports', 100, 10, 5, 0.15),
    ]
    df = spark.createDataFrame(data, schema)

    result_df = module.build_channel_analytics(df)

    assert result_df.count() == 2
    assert 'rank_in_region' in result_df.columns
    row_a = result_df.filter(result_df.channel_title == 'ChanA').collect()[0]
    row_b = result_df.filter(result_df.channel_title == 'ChanB').collect()[0]
    assert row_a.rank_in_region == 1  # highest cumulative views
    assert row_b.rank_in_region == 2
    assert row_a.total_views_to_date == 300
    assert row_a.channel_id == 'chanA_id'


def test_build_channel_analytics_accumulates_across_snapshots(spark, silver_to_gold_module):
    """A channel trending on two consecutive days should show growing
    cumulative totals and trending_video_count_to_date, not a static lifetime
    number -- this is the whole point of the (channel, region, snapshot_date)
    redesign."""
    module = silver_to_gold_module
    from datetime import date

    schema = 'video_id string, region string, channel_title string, channel_id string, trending_date_parsed date, category_name string, views long, likes long, comment_count long, engagement_rate double'
    data = [
        ('v1', 'us', 'ChanA', 'chanA_id', date(2024, 7, 17), 'Music', 300, 30, 15, 0.15),
        ('v2', 'us', 'ChanA', 'chanA_id', date(2024, 7, 18), 'Music', 200, 20, 10, 0.15),
    ]
    df = spark.createDataFrame(data, schema)

    result_df = module.build_channel_analytics(df).orderBy('snapshot_date').collect()

    assert result_df[0]['total_views_to_date'] == 300
    assert result_df[0]['trending_video_count_to_date'] == 1
    assert result_df[1]['total_views_to_date'] == 500       # cumulative, not just day 2's 200
    assert result_df[1]['trending_video_count_to_date'] == 2  # two distinct videos seen to date
    assert result_df[1]['days_active_to_date'] == 2


def test_build_channel_analytics_trending_frequency_capped_at_one(spark, silver_to_gold_module):
    """A channel trending on every consecutive day since its first appearance
    should show trending_frequency == 1.0, never > 1.0 (caught a real
    off-by-one bug during development: datediff() is a difference, not a
    count of days, so the denominator must be datediff + 1)."""
    module = silver_to_gold_module
    from datetime import date

    schema = 'video_id string, region string, channel_title string, channel_id string, trending_date_parsed date, category_name string, views long, likes long, comment_count long, engagement_rate double'
    data = [
        ('v1', 'us', 'ChanA', 'chanA_id', date(2024, 7, 17), 'Music', 300, 30, 15, 0.15),
        ('v2', 'us', 'ChanA', 'chanA_id', date(2024, 7, 18), 'Music', 200, 20, 10, 0.15),
        ('v3', 'us', 'ChanA', 'chanA_id', date(2024, 7, 19), 'Music', 250, 25, 12, 0.15),
    ]
    df = spark.createDataFrame(data, schema)

    result = module.build_channel_analytics(df).orderBy('snapshot_date').collect()

    for row in result:
        assert row['trending_frequency'] == 1.0


def test_build_channel_analytics_trending_frequency_with_a_gap(spark, silver_to_gold_module):
    """Trending on day 1 and day 3 but not day 2 should show a frequency below
    1.0, not exactly 1.0 or above it."""
    module = silver_to_gold_module
    from datetime import date

    schema = 'video_id string, region string, channel_title string, channel_id string, trending_date_parsed date, category_name string, views long, likes long, comment_count long, engagement_rate double'
    data = [
        ('v1', 'us', 'ChanA', 'chanA_id', date(2024, 7, 17), 'Music', 300, 30, 15, 0.15),
        ('v2', 'us', 'ChanA', 'chanA_id', date(2024, 7, 19), 'Music', 250, 25, 12, 0.15),  # gap on 7/18
    ]
    df = spark.createDataFrame(data, schema)

    result = module.build_channel_analytics(df).orderBy('snapshot_date').collect()

    assert result[1]['trending_frequency'] == round(2 / 3, 4)  # 2 active days out of a 3-day span
    assert result[1]['trending_frequency'] < 1.0


def test_add_markets_present_counts_distinct_regions(spark, silver_to_gold_module):
    module = silver_to_gold_module
    from datetime import date

    schema = 'channel_id string, region string, snapshot_date date'
    data = [
        ('chanA', 'us', date(2024, 7, 17)),
        ('chanA', 'gb', date(2024, 7, 18)),  # entered a 2nd market the next day
    ]
    df = spark.createDataFrame(data, schema)

    result = module.add_markets_present(df).collect()

    day1 = [r for r in result if r['snapshot_date'] == date(2024, 7, 17)][0]
    day2_gb = [r for r in result if r['snapshot_date'] == date(2024, 7, 18) and r['region'] == 'gb'][0]

    assert day1['markets_present'] == 1
    assert day2_gb['markets_present'] == 2  # us (entered day 1) + gb (entered day 2)


def test_build_category_analytics_calculates_view_share(spark, silver_to_gold_module):
    module = silver_to_gold_module
    from datetime import date
    
    schema = 'video_id string, region string, category_name string, category_id long, channel_title string, trending_date_parsed date, views long, likes long, comment_count long, engagement_rate double'
    data = [
        ('v1', 'us', 'Music', 10, 'Chan1', date(2024, 7, 17), 300, 30, 15, 0.15),
        ('v2', 'us', 'Sports', 20, 'Chan2', date(2024, 7, 17), 100, 10, 5, 0.15),
    ]
    df = spark.createDataFrame(data, schema)
    
    result_df = module.build_category_analytics(df)
    
    assert result_df.count() == 2
    assert 'view_share_pct' in result_df.columns
    music_share = result_df.filter(result_df.category_name == 'Music').select('view_share_pct').collect()[0][0]
    assert music_share == 75.0  # 300 / 400 * 100


def test_build_category_analytics_growth_and_momentum(spark, silver_to_gold_module):
    module = silver_to_gold_module
    from datetime import date

    schema = 'video_id string, region string, category_name string, category_id long, channel_title string, trending_date_parsed date, views long, likes long, comment_count long, engagement_rate double'
    data = [
        ('v1', 'us', 'Music', 10, 'Chan1', date(2024, 7, 16), 200, 20, 10, 0.15),
        ('v1', 'us', 'Music', 10, 'Chan1', date(2024, 7, 17), 300, 30, 15, 0.15),  # +50%
    ]
    df = spark.createDataFrame(data, schema)

    result = module.build_category_analytics(df).orderBy('trending_date_parsed').collect()

    assert result[0]['category_growth_pct'] is None  # no prior snapshot
    assert result[1]['category_growth_pct'] == 50.0
    assert result[1]['category_rank'] == 1


def test_build_category_analytics_growth_null_on_zero_baseline(spark, silver_to_gold_module):
    """A previous snapshot with 0 total_views can't support a meaningful
    growth ratio -- must be null, never a fabricated/infinite value."""
    module = silver_to_gold_module
    from datetime import date

    schema = 'video_id string, region string, category_name string, category_id long, channel_title string, trending_date_parsed date, views long, likes long, comment_count long, engagement_rate double'
    data = [
        ('v1', 'us', 'Music', 10, 'Chan1', date(2024, 7, 16), 0, 0, 0, 0.0),
        ('v1', 'us', 'Music', 10, 'Chan1', date(2024, 7, 17), 300, 30, 15, 0.15),
    ]
    df = spark.createDataFrame(data, schema)

    result = module.build_category_analytics(df).orderBy('trending_date_parsed').collect()

    assert result[1]['category_growth_pct'] is None
"""Tests for trend_intelligence.py -- video velocity, persistence, trend-stage
classification, cross-market expansion, and the composite opportunities table."""
from datetime import date

from pyspark.sql import functions as F
import pytest


VIDEO_SCHEMA = (
    'video_id string, region string, trending_date_parsed date, title string, '
    'views long, likes long, comment_count long, like_rate double, comment_rate double'
)


def _video_row(video_id, region, d, views, likes=0, comments=0, title='Title'):
    like_rate = round(likes / views, 6) if views else 0.0
    comment_rate = round(comments / views, 6) if views else 0.0
    return (video_id, region, d, title, views, likes, comments, like_rate, comment_rate)


# ── compute_video_trends ──────────────────────────────────────────────────

def test_compute_video_trends_first_observation_is_new_entrant(spark, trend_intelligence_module):
    module = trend_intelligence_module
    df = spark.createDataFrame([_video_row('v1', 'us', date(2024, 7, 17), 1000)], VIDEO_SCHEMA)

    result = module.compute_video_trends(df).collect()[0]

    assert result['is_new_entrant'] is True
    assert result['previous_views'] is None
    assert result['view_growth_rate'] is None       # never fabricated, not 0 or inf
    assert result['rank_change'] is None
    assert result['trending_days_to_date'] == 1
    assert result['trend_stage'] == 'EMERGING'       # a video's first appearance is itself emerging


def test_compute_video_trends_growth_rate_and_deltas(spark, trend_intelligence_module):
    module = trend_intelligence_module
    df = spark.createDataFrame([
        _video_row('v1', 'us', date(2024, 7, 16), 1000, likes=50, comments=10),
        _video_row('v1', 'us', date(2024, 7, 17), 1500, likes=90, comments=20),
    ], VIDEO_SCHEMA)

    rows = {r['trending_date_parsed']: r for r in module.compute_video_trends(df).collect()}
    day2 = rows[date(2024, 7, 17)]

    assert day2['previous_views'] == 1000
    assert day2['views_delta'] == 500
    assert day2['view_growth_rate'] == 0.5           # (1500-1000)/1000
    assert day2['is_new_entrant'] is False
    assert day2['trending_days_to_date'] == 2


def test_compute_video_trends_zero_previous_views_yields_null_growth(spark, trend_intelligence_module):
    """A zero-view previous snapshot can't support a meaningful ratio -- must
    stay null, never an infinite/undefined growth rate."""
    module = trend_intelligence_module
    df = spark.createDataFrame([
        _video_row('v1', 'us', date(2024, 7, 16), 0),
        _video_row('v1', 'us', date(2024, 7, 17), 500),
    ], VIDEO_SCHEMA)

    rows = {r['trending_date_parsed']: r for r in module.compute_video_trends(df).collect()}
    assert rows[date(2024, 7, 17)]['view_growth_rate'] is None


def test_compute_video_trends_rank_change_sign_convention(spark, trend_intelligence_module):
    """rank_change is previous_rank - current_rank: positive means the video
    moved UP the leaderboard (a smaller/better rank number)."""
    module = trend_intelligence_module
    df = spark.createDataFrame([
        _video_row('v1', 'us', date(2024, 7, 16), 100),   # rank 2 on day 1 (v2 has more views)
        _video_row('v2', 'us', date(2024, 7, 16), 500),   # rank 1 on day 1
        _video_row('v1', 'us', date(2024, 7, 17), 900),   # rank 1 on day 2 (overtakes v2)
        _video_row('v2', 'us', date(2024, 7, 17), 500),   # rank 2 on day 2
    ], VIDEO_SCHEMA)

    rows = {(r['video_id'], r['trending_date_parsed']): r for r in module.compute_video_trends(df).collect()}
    v1_day2 = rows[('v1', date(2024, 7, 17))]

    assert v1_day2['previous_rank'] == 2
    assert v1_day2['rank_in_region'] == 1
    assert v1_day2['rank_change'] == 1   # moved from rank 2 to rank 1: +1 improvement


@pytest.mark.parametrize('growth,days,expected_stage', [
    (0.30, 2, 'EMERGING'),     # big growth, still early
    (0.30, 5, 'SUSTAINING'),   # big growth, but past the emerging window
    (-0.10, 5, 'FADING'),      # declining
    (0.0, 10, 'ESTABLISHED'),  # flat, long-running
])
def test_classify_trend_stage_rules(spark, trend_intelligence_module, growth, days, expected_stage):
    module = trend_intelligence_module
    # Build a history of `days` snapshots ending with the target growth rate on the last day.
    views_start = 1000
    rows = []
    for i in range(days):
        v = views_start if i < days - 1 else round(views_start * (1 + growth))
        rows.append(_video_row('v1', 'us', date(2024, 7, 1 + i), v))
        views_start = v if i == days - 2 else views_start

    df = spark.createDataFrame(rows, VIDEO_SCHEMA)
    result = module.compute_video_trends(df).orderBy('trending_date_parsed').collect()[-1]

    assert result['trend_stage'] == expected_stage


# ── compute_cross_market_expansion ────────────────────────────────────────

CATEGORY_SCHEMA = 'category_id long, category_name string, region string, snapshot_date date, total_views long'


def test_compute_cross_market_expansion_counts_markets(spark, trend_intelligence_module):
    module = trend_intelligence_module
    df = spark.createDataFrame([
        (10, 'Music', 'us', date(2024, 7, 17), 1000),
        (10, 'Music', 'gb', date(2024, 7, 17), 500),
    ], CATEGORY_SCHEMA)

    result = module.compute_cross_market_expansion(df).collect()[0]

    assert result['market_count'] == 2
    assert result['total_views_all_markets'] == 1500


def test_compute_cross_market_expansion_tracks_growth(spark, trend_intelligence_module):
    module = trend_intelligence_module
    df = spark.createDataFrame([
        (10, 'Music', 'us', date(2024, 7, 16), 1000),                      # 1 market on day 1
        (10, 'Music', 'us', date(2024, 7, 17), 1000),
        (10, 'Music', 'gb', date(2024, 7, 17), 500),                       # +1 market on day 2
    ], CATEGORY_SCHEMA)

    rows = {r['snapshot_date']: r for r in module.compute_cross_market_expansion(df).collect()}

    assert rows[date(2024, 7, 16)]['market_count'] == 1
    assert rows[date(2024, 7, 16)]['market_count_delta'] is None   # no prior snapshot
    assert rows[date(2024, 7, 17)]['market_count'] == 2
    assert rows[date(2024, 7, 17)]['market_count_delta'] == 1


# ── build_trend_opportunities ─────────────────────────────────────────────

def test_build_category_opportunities_flags_cross_market_expansion(spark, trend_intelligence_module):
    module = trend_intelligence_module

    category_df = spark.createDataFrame([
        # US: Music grows locally but does not expand into a new market
        (10, 'Music', 'us', date(2024, 7, 16), 1000, 0.10, 5),
        (10, 'Music', 'us', date(2024, 7, 17), 1300, 0.10, 4),
        # Gaming: expands from 1 market to 2 on day 2
        (20, 'Gaming', 'us', date(2024, 7, 16), 500, 0.05, 3),
        (20, 'Gaming', 'us', date(2024, 7, 17), 550, 0.05, 3),
        (20, 'Gaming', 'gb', date(2024, 7, 17), 300, 0.05, 2),
    ], 'category_id long, category_name string, region string, snapshot_date date, total_views long, avg_engagement_rate double, unique_channels long')

    # category_growth_pct would normally come from build_category_analytics;
    # supply it directly here since this test targets build_category_opportunities in isolation.
    category_df = category_df.withColumn(
        'category_growth_pct',
        F.when((F.col('category_id') == 10) & (F.col('snapshot_date') == date(2024, 7, 17)), F.lit(30.0))
         .when((F.col('category_id') == 20) & (F.col('snapshot_date') == date(2024, 7, 17)), F.lit(10.0))
         .otherwise(F.lit(None).cast('double'))
    )

    cross_market_df = module.compute_cross_market_expansion(category_df)
    opportunities = module.build_category_opportunities(category_df, cross_market_df)

    rows = {(r['entity_name'], r['snapshot_date']): r for r in opportunities.collect()}

    assert rows[('Gaming', date(2024, 7, 17))]['trend_type'] == 'CROSS_MARKET_EXPANSION'
    assert rows[('Music', date(2024, 7, 17))]['trend_type'] == 'RISING_CATEGORY'


def test_build_video_opportunities_excludes_fading_and_established(spark, trend_intelligence_module):
    module = trend_intelligence_module
    df = spark.createDataFrame([
        _video_row('v1', 'us', date(2024, 7, 16), 1000),
        _video_row('v1', 'us', date(2024, 7, 17), 5000),   # +400% -> EMERGING
        _video_row('v2', 'us', date(2024, 7, 17), 1000),
        _video_row('v2', 'us', date(2024, 7, 18), 850),    # -15% -> FADING
    ], VIDEO_SCHEMA)

    video_trends = module.compute_video_trends(df)
    opportunities = module.build_video_opportunities(video_trends)

    # v1's day-1 row is also its is_new_entrant EMERGING row -- a video's first
    # trending appearance is itself a legitimate opportunity signal.
    types = {(r['entity_id'], r['snapshot_date']): r['trend_type'] for r in opportunities.collect()}
    assert types[('v1', date(2024, 7, 16))] == 'EMERGING'
    assert types[('v1', date(2024, 7, 17))] == 'EMERGING'
    assert ('v2', date(2024, 7, 18)) not in types   # FADING is excluded from opportunities

    day2_row = opportunities.filter(
        (F.col('entity_id') == 'v1') & (F.col('snapshot_date') == date(2024, 7, 17))
    ).collect()[0]
    assert day2_row['scope'] == 'VIDEO'
    assert day2_row['rank'] == 1
    assert day2_row['evidence'] is not None

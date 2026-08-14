from datetime import date
from unittest.mock import Mock

from pyspark.sql import functions as F


def test_enforce_schema_api_format_and_missing_optional_fields(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            (
                "v1",
                "Title",
                "Channel",
                "10",
                "100",
                "5",
                "3",
                "us",
            )
        ],
        """
        id string,
        `snippet.title` string,
        `snippet.channelTitle` string,
        `snippet.categoryId` string,
        `statistics.viewCount` string,
        `statistics.likeCount` string,
        `statistics.commentCount` string,
        region string
        """,
    )

    result = bronze_to_silver_module.enforce_schema(df).collect()[0]

    assert result["video_id"] == "v1"
    assert result["title"] == "Title"
    assert result["category_id"] == 10
    assert result["views"] == 100
    assert result["dislikes"] == 0
    assert result["duration"] is None
    assert result["region"] == "us"


def test_enforce_schema_kaggle_format(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            (
                "v1",
                "24.17.07",
                "Title",
                "Channel",
                "10",
                "2024-07-17T10:00:00Z",
                "tag1,tag2",
                "100",
                "5",
                "1",
                "0",
                "url",
                "false",
                "false",
                "false",
                "desc",
                "us",
            )
        ],
        """
        video_id string,
        trending_date string,
        title string,
        channel_title string,
        category_id string,
        publish_time string,
        tags string,
        views string,
        likes string,
        dislikes string,
        comment_count string,
        thumbnail_link string,
        comments_disabled string,
        ratings_disabled string,
        video_error_or_removed string,
        description string,
        region string
        """,
    )

    result = bronze_to_silver_module.enforce_schema(df)

    row = result.collect()[0]

    assert row["video_id"] == "v1"
    assert row["category_id"] == 10
    assert row["channel_id"] is None
    assert row["publish_time"] == "2024-07-17T10:00:00Z"
    assert row["tags"] == "tag1,tag2"
    assert row["duration"] is None


def test_add_duration_seconds_parses_full_duration(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            ("PT1H2M3S",),
            ("PT4M13S",),
            (None,),
        ],
        "duration string",
    )

    rows = bronze_to_silver_module.add_duration_seconds(df).collect()

    assert [row["duration_seconds"] for row in rows] == [
        3723,
        253,
        None,
    ]


def test_add_duration_seconds_handles_missing_column(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [(1,)],
        "id int",
    )

    row = bronze_to_silver_module.add_duration_seconds(
        df
    ).collect()[0]

    assert row["duration_seconds"] is None


def test_add_rate_metrics_handles_zero_and_null_views(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            (100, 10, 5),
            (0, 10, 5),
            (None, 10, 5),
        ],
        """
        views long,
        likes long,
        comment_count long
        """,
    )

    rows = bronze_to_silver_module.add_rate_metrics(
        df
    ).collect()

    assert rows[0]["like_rate"] == 0.1
    assert rows[0]["comment_rate"] == 0.05

    assert rows[1]["like_rate"] == 0.0
    assert rows[1]["comment_rate"] == 0.0

    assert rows[2]["like_rate"] == 0.0
    assert rows[2]["comment_rate"] == 0.0


def test_add_velocity_context_computes_age_and_views_per_hour(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            (
                "2024-07-16T22:00:00Z",
                date(2024, 7, 17),
                300,
            )
        ],
        """
        publish_time string,
        trending_date_parsed date,
        views long
        """,
    )

    row = bronze_to_silver_module.add_velocity_context(
        df
    ).collect()[0]

    assert row["video_age_hours"] == 2.0
    assert row["views_per_hour"] == 150.0


def test_add_velocity_context_handles_missing_inputs(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [(1,)],
        "views int",
    )

    row = bronze_to_silver_module.add_velocity_context(
        df
    ).collect()[0]

    assert row["video_age_hours"] is None
    assert row["views_per_hour"] is None


def test_cleanse_data_filters_invalid_rows_and_normalizes_region(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            (
                None,
                " US ",
                "24.17.07",
                None,
                10,
                None,
                None,
                5,
            ),
            (
                "v1",
                " US ",
                "2024-07-17",
                None,
                100,
                10,
                None,
                5,
            ),
        ],
        """
        video_id string,
        region string,
        trending_date string,
        title string,
        views long,
        likes long,
        dislikes long,
        comment_count long
        """,
    )

    result = bronze_to_silver_module.cleanse_data(
        df,
        "job",
    ).collect()

    assert len(result) == 1
    assert result[0]["region"] == "us"
    assert result[0]["views"] == 100
    assert result[0]["likes"] == 10
    assert result[0]["dislikes"] == 0
    assert result[0]["comment_count"] == 5
    assert result[0]["like_ratio"] == 10.0
    assert result[0]["_job_name"] == "job"


def test_deduplicate_data_keeps_latest_record(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            (
                "v1",
                "us",
                date(2024, 7, 17),
                "old",
            ),
            (
                "v1",
                "us",
                date(2024, 7, 17),
                "new",
            ),
        ],
        """
        video_id string,
        region string,
        trending_date_parsed date,
        title string
        """,
    )

    df = df.withColumn(
        "_processed_at",
        F.when(
            F.col("title") == "old",
            F.lit("2024-07-17 10:00:00"),
        )
        .otherwise(
            F.lit("2024-07-17 11:00:00")
        )
        .cast("timestamp"),
    )

    row = bronze_to_silver_module.deduplicate_data(
        df
    ).collect()[0]

    assert row["title"] == "new"


def test_run_data_quality_checks_reports_nulls_and_negative_views(
    spark,
    bronze_to_silver_module,
):
    df = spark.createDataFrame(
        [
            ("v1", None, "c", -1),
            (None, "t", "c", 2),
        ],
        """
        video_id string,
        title string,
        channel_title string,
        views long
        """,
    )

    logger = Mock()

    result = bronze_to_silver_module.run_data_quality_checks(
        df,
        logger,
    )

    assert result["nulls"]["video_id"] == 1
    assert result["nulls"]["title"] == 1
    assert result["negative_views"] == 1
    assert logger.warn.call_count >= 2
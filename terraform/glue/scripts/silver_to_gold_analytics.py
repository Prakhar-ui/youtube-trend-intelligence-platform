import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Transformation Functions (Testable locally with pure PySpark) ────────────

def add_category_name(df: DataFrame, reference_df: DataFrame) -> DataFrame:
    """Joins the category reference table (ingested from the YouTube videoCategories
    API and flattened by the json_to_parquet Lambda) onto the statistics data to
    attach a real, human-readable category_name.

    Category IDs are global but the reference data is stored per-region, so we join
    on (category_id, region) and fall back to a labeled placeholder only when a
    region genuinely has no matching category (e.g. a brand-new category ID YouTube
    hasn't backfilled reference data for yet).

    The reference table is tiny (< 1k rows) so we broadcast it to avoid a full shuffle.
    """
    from pyspark.sql.functions import broadcast

    ref = broadcast(
        reference_df
        .select(
            F.col("id").cast("long").alias("category_id"),
            F.col("snippet_title").alias("category_name"),
            F.col("region").alias("ref_region"),
        )
        .dropDuplicates(["category_id", "ref_region"])
    )

    df = df.join(
        ref,
        on=(df["category_id"] == ref["category_id"]) & (df["region"] == ref["ref_region"]),
        how="left",
    ).drop(ref["category_id"]).drop("ref_region")

    df = df.withColumn(
        "category_name",
        F.coalesce(
            F.col("category_name"),
            F.concat(F.lit("Unknown Category "), F.col("category_id").cast("string")),
        )
    )

    return df


def build_trending_analytics(df: DataFrame) -> DataFrame:
    """Builds daily trending summaries per region."""
    trending = df.groupBy("region", "trending_date_parsed").agg(
        F.count("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("dislikes").alias("total_dislikes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("views").alias("avg_views_per_video"),
        F.avg("like_ratio").alias("avg_like_ratio"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("views").alias("max_views"),
        F.countDistinct("channel_title").alias("unique_channels"),
        F.countDistinct("category_id").alias("unique_categories"),
    )
    return trending.withColumn("_aggregated_at", F.current_timestamp())


def build_channel_analytics(df: DataFrame) -> DataFrame:
    """Channel performance, CUMULATIVE-TO-DATE, at (channel, region,
    snapshot_date) grain. Each row answers 'as of this date, how has this
    channel performed in this market so far' -- unlike the original single
    lifetime-aggregate design, this lets trending_frequency, trend_score, etc.
    evolve over time and be compared date to date.

    channel_id is the entity key where available (YouTube API rows);
    channel_title is the fallback for Kaggle-format rows where channel_id is
    null (see docs/data_model.md). Distinct video counts are computed via the
    standard 'first occurrence' running-count trick, since Spark window
    functions don't support a running countDistinct directly.
    """
    df = df.withColumn("channel_key", F.coalesce(F.col("channel_id"), F.col("channel_title")))

    daily = df.groupBy("channel_key", "channel_title", "region", "trending_date_parsed").agg(
        F.sum("views").alias("daily_views"),
        F.sum("likes").alias("daily_likes"),
        F.sum("comment_count").alias("daily_comments"),
        F.avg("engagement_rate").alias("daily_avg_engagement_rate"),
        F.max("views").alias("daily_peak_views"),
    )

    first_seen_window = Window.partitionBy("channel_key", "region", "video_id").orderBy("trending_date_parsed")
    first_seen = (
        df.withColumn("_rn", F.row_number().over(first_seen_window))
        .filter(F.col("_rn") == 1)
        .groupBy("channel_key", "region", "trending_date_parsed")
        .agg(F.count("video_id").alias("new_distinct_videos"))
    )

    daily = daily.join(first_seen, on=["channel_key", "region", "trending_date_parsed"], how="left")
    daily = daily.withColumn("new_distinct_videos", F.coalesce(F.col("new_distinct_videos"), F.lit(0)))

    cumulative_window = (
        Window.partitionBy("channel_key", "region")
        .orderBy("trending_date_parsed")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    channel = (
        daily.withColumn("trending_video_count_to_date", F.sum("new_distinct_videos").over(cumulative_window))
        .withColumn("total_views_to_date", F.sum("daily_views").over(cumulative_window))
        .withColumn("total_likes_to_date", F.sum("daily_likes").over(cumulative_window))
        .withColumn("total_comments_to_date", F.sum("daily_comments").over(cumulative_window))
        .withColumn("avg_engagement_rate_to_date", F.avg("daily_avg_engagement_rate").over(cumulative_window))
        .withColumn("peak_views_to_date", F.max("daily_peak_views").over(cumulative_window))
        .withColumn(
            "days_active_to_date",
            F.row_number().over(Window.partitionBy("channel_key", "region").orderBy("trending_date_parsed"))
        )
        .withColumn("first_trending_date", F.min("trending_date_parsed").over(cumulative_window))
    )

    channel = channel.withColumn(
        "avg_views_per_video_to_date",
        F.when(
            F.col("trending_video_count_to_date") > 0,
            F.round(F.col("total_views_to_date") / F.col("trending_video_count_to_date"), 2)
        ).otherwise(F.lit(0.0))
    )

    # trending_frequency: fraction of calendar days since this channel's first
    # trending appearance in this region (inclusive of both endpoints) that
    # it's had at least one trending video. 1.0 means trending every day since
    # it first appeared; capped at 1.0 by construction since a channel can't
    # be active on more days than exist in that span.
    days_span_inclusive = F.datediff(F.col("trending_date_parsed"), F.col("first_trending_date")).cast("double") + F.lit(1.0)
    channel = channel.withColumn(
        "trending_frequency",
        F.round(F.col("days_active_to_date") / days_span_inclusive, 4)
    )

    rank_window = Window.partitionBy("region", "trending_date_parsed").orderBy(F.col("total_views_to_date").desc())
    channel = channel.withColumn("rank_in_region", F.row_number().over(rank_window))

    channel = channel.select(
        F.col("channel_key").alias("channel_id"),
        F.col("channel_title"),
        F.col("region"),
        F.col("trending_date_parsed").alias("snapshot_date"),
        F.col("trending_video_count_to_date"),
        F.col("total_views_to_date"),
        F.col("total_likes_to_date"),
        F.col("total_comments_to_date"),
        F.col("avg_views_per_video_to_date"),
        F.col("avg_engagement_rate_to_date"),
        F.col("peak_views_to_date"),
        F.col("days_active_to_date"),
        F.col("first_trending_date"),
        F.col("trending_frequency"),
        F.col("rank_in_region"),
    )

    channel = add_markets_present(channel)
    channel = add_channel_trend_score(channel)

    return channel.withColumn("_aggregated_at", F.current_timestamp())


def add_markets_present(channel_df: DataFrame) -> DataFrame:
    """Adds markets_present: how many distinct regions this channel has had a
    trending video in, cumulative to date, across ALL regions -- not just the
    region of the current row. This is the cross-market signal behind
    'multi-market channels' (spec section 16)."""
    first_region_seen = Window.partitionBy("channel_id", "region").orderBy("snapshot_date")
    region_entries = (
        channel_df.withColumn("_rn", F.row_number().over(first_region_seen))
        .filter(F.col("_rn") == 1)
        .select("channel_id", "region", F.col("snapshot_date").alias("region_entry_date"))
    )

    dates = channel_df.select("channel_id", "snapshot_date").distinct()
    markets_present = (
        dates.join(region_entries, on="channel_id", how="left")
        .filter(F.col("region_entry_date") <= F.col("snapshot_date"))
        .groupBy("channel_id", "snapshot_date")
        .agg(F.countDistinct("region").alias("markets_present"))
    )

    return channel_df.join(markets_present, on=["channel_id", "snapshot_date"], how="left")


def add_channel_trend_score(channel_df: DataFrame) -> DataFrame:
    """A simple, explainable channel trend score (0-100): 70% percentile rank
    of total_views_to_date within the same region+date, 30% trending_frequency.
    Weights documented in docs/trend_intelligence.md."""
    pct_window = Window.partitionBy("region", "snapshot_date").orderBy(F.col("total_views_to_date").asc())
    channel_df = channel_df.withColumn("views_percentile", F.round(F.percent_rank().over(pct_window) * 100, 2))
    channel_df = channel_df.withColumn(
        "trend_score",
        F.round(F.col("views_percentile") * 0.7 + F.col("trending_frequency") * 100 * 0.3, 2)
    )
    return channel_df


def build_category_analytics(df: DataFrame) -> DataFrame:
    """Builds category-level trends over time including view share percentage,
    growth vs. the category's own previous snapshot in the same region,
    3-snapshot rolling momentum, and its rank within region+date."""
    category = df.groupBy("category_name", "category_id", "region", "trending_date_parsed").agg(
        F.count("video_id").alias("video_count"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.countDistinct("channel_title").alias("unique_channels"),
    )

    window_total = Window.partitionBy("region", "trending_date_parsed")
    category = category.withColumn(
        "view_share_pct",
        F.round(F.col("total_views") / F.sum("total_views").over(window_total) * 100, 2)
    )

    lag_window = Window.partitionBy("category_id", "region").orderBy("trending_date_parsed")
    category = category.withColumn("previous_total_views", F.lag("total_views").over(lag_window))
    category = category.withColumn(
        "category_growth_pct",
        F.when(F.col("previous_total_views").isNull(), F.lit(None).cast("double"))
         .when(F.col("previous_total_views") == 0, F.lit(None).cast("double"))
         .otherwise(F.round((F.col("total_views") - F.col("previous_total_views")) / F.col("previous_total_views") * 100, 2))
    )

    # 3-snapshot rolling average of growth, to smooth day-to-day noise into a
    # "momentum" signal (a single day's growth spike vs. a sustained trend).
    momentum_window = Window.partitionBy("category_id", "region").orderBy("trending_date_parsed").rowsBetween(-2, 0)
    category = category.withColumn("category_momentum", F.round(F.avg("category_growth_pct").over(momentum_window), 2))

    rank_window = Window.partitionBy("region", "trending_date_parsed").orderBy(F.col("total_views").desc())
    category = category.withColumn("category_rank", F.dense_rank().over(rank_window))
    prev_rank_window = Window.partitionBy("category_id", "region").orderBy("trending_date_parsed")
    category = category.withColumn("previous_rank", F.lag("category_rank").over(prev_rank_window))
    category = category.withColumn(
        "rank_change",
        F.when(F.col("previous_rank").isNotNull(), F.col("previous_rank") - F.col("category_rank"))
         .otherwise(F.lit(None).cast("int"))
    )

    return category.withColumn("_aggregated_at", F.current_timestamp())


# ── Main Execution (Glue I/O) ────────────────────────────────────────────────
# Wrapping the execution logic prevents Glue libraries from executing 
# when you import this file in your unit tests.

if __name__ == "__main__":
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame

    # Job Setup
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "silver_database",
        "gold_bucket",
        "gold_database",
        "reference_table",
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    logger = glueContext.get_logger()

    SILVER_DB = args["silver_database"]
    GOLD_BUCKET = args["gold_bucket"]
    GOLD_DB = args["gold_database"]

    # 1. Read Silver Table
    logger.info("Reading Silver layer tables...")
    stats_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name="clean_statistics",
        transformation_ctx="stats",
    )
    stats_df = stats_dyf.toDF()
    logger.info(f"Statistics records: {stats_df.count()}")

    logger.info(f"Reading Silver reference table: {args['reference_table']}...")
    reference_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name=args["reference_table"],
        transformation_ctx="reference",
    )
    reference_df = reference_dyf.toDF()
    logger.info(f"Reference records: {reference_df.count()}")

    # 2. Apply Transformations
    stats_df = add_category_name(stats_df, reference_df)
    
    trending_df = build_trending_analytics(stats_df)
    channel_df = build_channel_analytics(stats_df)
    category_df = build_category_analytics(stats_df)

    # 3. Write Gold Tables
    def write_to_gold(df: DataFrame, table_name: str, path: str):
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
        sink = glueContext.getSink(
            connection_type="s3",
            path=path,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["region"],
        )
        sink.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName=table_name)
        sink.setFormat("glueparquet", compression="snappy")
        sink.writeFrame(dyf)
        logger.info(f"  Written {df.count()} rows → {path}")

    logger.info("Building Gold: trending_analytics...")
    write_to_gold(trending_df, "trending_analytics", f"s3://{GOLD_BUCKET}/youtube/trending_analytics/")

    logger.info("Building Gold: channel_analytics...")
    write_to_gold(channel_df, "channel_analytics", f"s3://{GOLD_BUCKET}/youtube/channel_analytics/")

    logger.info("Building Gold: category_analytics...")
    write_to_gold(
        category_df.withColumnRenamed("trending_date_parsed", "snapshot_date"),
        "category_analytics", f"s3://{GOLD_BUCKET}/youtube/category_analytics/"
    )

    logger.info("Gold layer build complete.")
    job.commit()
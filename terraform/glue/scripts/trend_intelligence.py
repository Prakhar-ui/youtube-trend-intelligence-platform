"""
Trend Intelligence Gold layer.

Produces two Gold datasets that the original silver_to_gold_analytics.py job
did not (see docs/trend_intelligence.md for full methodology):

  gold_video_trends       grain: video_id x region x snapshot_date
  gold_trend_opportunities grain: one opportunity x scope x region x snapshot_date

Runs AFTER silver_to_gold_analytics (reads its category_analytics and
channel_analytics outputs from the Glue Catalog) -- see step_functions for
ordering. This keeps the inter-job data flow consistent with the rest of the
pipeline (catalog-based handoffs, not direct S3 path coupling).
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Tunable constants (documented in docs/trend_intelligence.md) ────────────
# A video/category is only ever classified EMERGING within its first N
# trending days -- after that, sustained growth is SUSTAINING, not EMERGING.
EMERGING_MAX_DAYS = 3
# >20% view growth vs the previous snapshot counts as "emerging" velocity.
EMERGING_GROWTH_THRESHOLD = 0.20
# <-5% view growth counts as "fading".
FADING_GROWTH_THRESHOLD = -0.05
# Needs at least this many trending days, with growth roughly flat, to be
# "established" rather than "sustaining".
ESTABLISHED_MIN_DAYS = 7
# trending_days at/above this is treated as "fully persistent" (score = 100).
PERSISTENCE_CAP_DAYS = 7
# Denominator for market_expansion_score. Keep in sync with the --regions
# argument in bronze_to_silver_statistics_job.tf.
TOTAL_MARKETS_MONITORED = 10


# ── Video Trend Velocity (gold_video_trends) ─────────────────────────────────

def compute_video_trends(df: DataFrame) -> DataFrame:
    """Grain: video_id x region x snapshot_date (snapshot_date =
    trending_date_parsed from Silver). Compares each observation only to the
    SAME video's previous snapshot in the SAME region (never across regions or
    across different videos).

    A video's first-ever observation gets null growth/rank-change values, not
    a fabricated 0% or infinite% -- it's flagged is_new_entrant instead. This
    is deliberate: a growth rate needs a real previous value to be meaningful.
    """
    w = Window.partitionBy("video_id", "region").orderBy("trending_date_parsed")

    df = df.withColumn("previous_views", F.lag("views").over(w))
    df = df.withColumn("previous_likes", F.lag("likes").over(w))
    df = df.withColumn("previous_comment_count", F.lag("comment_count").over(w))
    df = df.withColumn("is_new_entrant", F.col("previous_views").isNull())

    df = df.withColumn("views_delta", F.col("views") - F.col("previous_views"))
    df = df.withColumn("likes_delta", F.col("likes") - F.col("previous_likes"))
    df = df.withColumn("comments_delta", F.col("comment_count") - F.col("previous_comment_count"))

    df = df.withColumn(
        "view_growth_rate",
        F.when(F.col("previous_views").isNull(), F.lit(None).cast("double"))
         # a zero-view previous snapshot can't support a meaningful ratio --
         # treat as unknown rather than reporting an infinite/undefined growth rate
         .when(F.col("previous_views") == 0, F.lit(None).cast("double"))
         .otherwise(F.round((F.col("views") - F.col("previous_views")) / F.col("previous_views"), 6))
    )

    df = df.withColumn(
        "current_engagement_fraction",
        F.coalesce(F.col("like_rate"), F.lit(0.0)) + F.coalesce(F.col("comment_rate"), F.lit(0.0))
    )
    df = df.withColumn("previous_engagement_fraction", F.lag("current_engagement_fraction").over(w))
    df = df.withColumn(
        "engagement_change",
        F.when(
            F.col("previous_engagement_fraction").isNotNull(),
            F.round(F.col("current_engagement_fraction") - F.col("previous_engagement_fraction"), 6)
        ).otherwise(F.lit(None).cast("double"))
    )

    # Rank within its own region+date, then compare to the video's own previous rank.
    rank_window = Window.partitionBy("region", "trending_date_parsed").orderBy(F.col("views").desc())
    df = df.withColumn("rank_in_region", F.dense_rank().over(rank_window))
    df = df.withColumn("previous_rank", F.lag("rank_in_region").over(w))
    df = df.withColumn(
        "rank_change",
        F.when(F.col("previous_rank").isNotNull(), F.col("previous_rank") - F.col("rank_in_region"))
         .otherwise(F.lit(None).cast("int"))
        # positive rank_change = moved UP the leaderboard (e.g. rank 10 -> rank 4 is +6)
    )

    # Persistence: the grain guarantees exactly one row per video+region+date,
    # so a running row_number() IS a correct cumulative "trending days so far" count.
    df = df.withColumn("trending_days_to_date", F.row_number().over(w))
    df = df.withColumn(
        "first_trending_date",
        F.min("trending_date_parsed").over(w.rowsBetween(Window.unboundedPreceding, 0))
    )
    df = df.withColumn(
        "persistence_score",
        F.round(F.least(F.col("trending_days_to_date") / F.lit(float(PERSISTENCE_CAP_DAYS)), F.lit(1.0)) * 100, 2)
    )

    df = df.withColumn("trend_stage", _classify_trend_stage())

    return df.withColumn("_aggregated_at", F.current_timestamp())


def _classify_trend_stage():
    """Rule-based, explainable trend-stage classifier -- evaluated top to
    bottom, first match wins. Every threshold is a named constant at the top
    of this file so 'why is this EMERGING' always has a one-line answer (spec
    section 13/14 explainability requirement). Not a statistical/ML model --
    deliberately simple so a business user can audit the rule directly."""
    return (
        F.when(F.col("is_new_entrant"), F.lit("EMERGING"))
         .when(F.col("view_growth_rate") < FADING_GROWTH_THRESHOLD, F.lit("FADING"))
         .when(
             (F.col("trending_days_to_date") <= EMERGING_MAX_DAYS) &
             (F.col("view_growth_rate") > EMERGING_GROWTH_THRESHOLD),
             F.lit("EMERGING")
         )
         .when(
             (F.col("trending_days_to_date") > ESTABLISHED_MIN_DAYS) &
             (F.col("view_growth_rate") >= FADING_GROWTH_THRESHOLD) &
             (F.col("view_growth_rate") <= EMERGING_GROWTH_THRESHOLD),
             F.lit("ESTABLISHED")
         )
         .when(F.col("view_growth_rate") > 0, F.lit("SUSTAINING"))
         # covers: growth rate unknown-but-not-new (shouldn't normally occur) and
         # flat/slightly-negative-but-not-fading videos with few trending days.
         .otherwise(F.lit("ESTABLISHED"))
    )


# ── Cross-Market Signal (feeds gold_trend_opportunities; not persisted alone) ─

def compute_cross_market_expansion(category_df: DataFrame) -> DataFrame:
    """Input: gold_category_trends (category x region x snapshot_date, as
    persisted -- i.e. already using the 'snapshot_date' column name, not the
    internal 'trending_date_parsed' used while it was being computed in
    silver_to_gold_analytics.py). Rolls it up to category x snapshot_date
    ACROSS all regions to measure how many markets a category is trending in
    and whether that count is growing -- the 'is this trend spreading' signal
    from spec section 17.

    Deliberately not persisted as a standalone gold_cross_market_trends /
    gold_market_trends table: with the same (category, snapshot_date) grain as
    the per-region rollup below, a separate table would just be a groupBy of
    this one, so the aggregation is done here and feeds gold_trend_opportunities
    directly. Documented in docs/decisions.md.
    """
    market_counts = category_df.groupBy("category_id", "category_name", "snapshot_date").agg(
        F.countDistinct("region").alias("market_count"),
        F.sum("total_views").alias("total_views_all_markets"),
    )
    w = Window.partitionBy("category_id").orderBy("snapshot_date")
    market_counts = market_counts.withColumn("previous_market_count", F.lag("market_count").over(w))
    market_counts = market_counts.withColumn(
        "market_count_delta",
        F.when(F.col("previous_market_count").isNotNull(), F.col("market_count") - F.col("previous_market_count"))
         .otherwise(F.lit(None).cast("int"))
    )
    market_counts = market_counts.withColumn(
        "market_expansion_score",
        F.round(F.col("market_count") / F.lit(float(TOTAL_MARKETS_MONITORED)) * 100, 2)
    )
    return market_counts


# ── Trend Opportunities (gold_trend_opportunities) ───────────────────────────

def _percentile_score(df: DataFrame, value_col: str, partition_cols: list) -> "F.Column":
    """Percentile rank (0-100) of value_col within partition_cols, nulls
    sorted first (so nulls land near 0, not skew the scale). Using percentile
    rank rather than min-max normalization means one outlier video/category
    can't compress everyone else's score toward 0 or 100."""
    w = Window.partitionBy(*partition_cols).orderBy(F.col(value_col).asc_nulls_first())
    return F.round(F.percent_rank().over(w) * 100, 2)


def build_video_opportunities(video_trends_df: DataFrame, top_n: int = 20) -> DataFrame:
    """Top emerging/sustaining videos per region+date. trend_score = 50%
    velocity + 30% engagement + 20% persistence (weights documented in
    docs/trend_intelligence.md)."""
    df = video_trends_df.withColumn(
        "velocity_score", _percentile_score(video_trends_df, "view_growth_rate", ["region", "trending_date_parsed"])
    )
    df = df.withColumn(
        "engagement_score", _percentile_score(df, "current_engagement_fraction", ["region", "trending_date_parsed"])
    )
    df = df.withColumn(
        "trend_score",
        F.round(
            F.coalesce(F.col("velocity_score"), F.lit(50.0)) * 0.5 +
            F.coalesce(F.col("engagement_score"), F.lit(0.0)) * 0.3 +
            F.coalesce(F.col("persistence_score"), F.lit(0.0)) * 0.2,
            2
        )
    )
    df = df.filter(F.col("trend_stage").isin("EMERGING", "SUSTAINING"))

    rank_w = Window.partitionBy("region", "trending_date_parsed").orderBy(F.col("trend_score").desc())
    df = df.withColumn("rank", F.row_number().over(rank_w)).filter(F.col("rank") <= top_n)

    df = df.withColumn(
        "evidence",
        F.concat(
            F.lit("Views "), F.round(F.coalesce(F.col("view_growth_rate") * 100, F.lit(0.0)), 1),
            F.lit("% vs previous snapshot, trending "), F.col("trending_days_to_date"),
            F.lit(" day(s), engagement "), F.round(F.col("current_engagement_fraction") * 100, 2), F.lit("%.")
        )
    )

    return df.select(
        F.col("trending_date_parsed").alias("snapshot_date"),
        F.col("region"),
        F.lit("VIDEO").alias("scope"),
        F.col("video_id").alias("entity_id"),
        F.col("title").alias("entity_name"),
        F.col("trend_stage").alias("trend_type"),
        F.col("trend_score"),
        F.col("velocity_score"),
        F.col("engagement_score"),
        F.col("persistence_score"),
        F.lit(0.0).alias("market_expansion_score"),
        F.col("rank"),
        F.col("evidence"),
    )


def build_category_opportunities(category_df: DataFrame, cross_market_df: DataFrame, top_n: int = 10) -> DataFrame:
    """Top rising/expanding categories per region+date. trend_score = 40%
    local velocity + 25% engagement + 35% cross-market expansion.
    category_df uses the persisted 'snapshot_date' column name (see
    compute_cross_market_expansion's docstring)."""
    df = category_df.withColumn(
        "velocity_score", _percentile_score(category_df, "category_growth_pct", ["region", "snapshot_date"])
    )
    df = df.withColumn(
        "engagement_score", _percentile_score(df, "avg_engagement_rate", ["region", "snapshot_date"])
    )
    df = df.join(
        cross_market_df.select(
            "category_id",
            F.col("snapshot_date"),
            "market_expansion_score", "market_count", "market_count_delta",
        ),
        on=["category_id", "snapshot_date"],
        how="left",
    )
    df = df.withColumn("market_expansion_score", F.coalesce(F.col("market_expansion_score"), F.lit(0.0)))
    df = df.withColumn(
        "trend_score",
        F.round(
            F.coalesce(F.col("velocity_score"), F.lit(50.0)) * 0.4 +
            F.coalesce(F.col("engagement_score"), F.lit(0.0)) * 0.25 +
            F.col("market_expansion_score") * 0.35,
            2
        )
    )
    df = df.withColumn(
        "trend_type",
        F.when(F.coalesce(F.col("market_count_delta"), F.lit(0)) > 0, F.lit("CROSS_MARKET_EXPANSION"))
         .when(F.coalesce(F.col("category_growth_pct"), F.lit(0.0)) > 0, F.lit("RISING_CATEGORY"))
         .otherwise(F.lit("DECLINING_CATEGORY"))
    )

    rank_w = Window.partitionBy("region", "snapshot_date").orderBy(F.col("trend_score").desc())
    df = df.withColumn("rank", F.row_number().over(rank_w)).filter(F.col("rank") <= top_n)

    df = df.withColumn(
        "evidence",
        F.concat(
            F.lit("Growth "), F.round(F.coalesce(F.col("category_growth_pct"), F.lit(0.0)), 1),
            F.lit("%, trending in "), F.coalesce(F.col("market_count"), F.lit(1)),
            F.lit(" market(s), engagement "), F.round(F.col("avg_engagement_rate") * 100, 2), F.lit("%.")
        )
    )

    return df.select(
        F.col("snapshot_date"),
        F.col("region"),
        F.lit("CATEGORY").alias("scope"),
        F.col("category_id").cast("string").alias("entity_id"),
        F.col("category_name").alias("entity_name"),
        F.col("trend_type"),
        F.col("trend_score"),
        F.col("velocity_score"),
        F.col("engagement_score"),
        F.lit(None).cast("double").alias("persistence_score"),
        F.col("market_expansion_score"),
        F.col("rank"),
        F.col("evidence"),
    )


def build_channel_opportunities(channel_df: DataFrame, top_n: int = 10) -> DataFrame:
    """Top channels per region+date by trend_score (already computed upstream
    in silver_to_gold_analytics.add_channel_trend_score)."""
    rank_w = Window.partitionBy("region", "snapshot_date").orderBy(F.col("trend_score").desc())
    df = channel_df.withColumn("rank", F.row_number().over(rank_w)).filter(F.col("rank") <= top_n)

    df = df.withColumn(
        "evidence",
        F.concat(
            F.lit("Trending in "), F.coalesce(F.col("markets_present"), F.lit(1)), F.lit(" market(s), "),
            F.col("trending_video_count_to_date"), F.lit(" video(s) trending to date, frequency "),
            F.round(F.col("trending_frequency") * 100, 0), F.lit("%.")
        )
    )

    return df.select(
        F.col("snapshot_date"),
        F.col("region"),
        F.lit("CHANNEL").alias("scope"),
        F.col("channel_id").alias("entity_id"),
        F.col("channel_title").alias("entity_name"),
        F.lit("HIGH_PERFORMING_CHANNEL").alias("trend_type"),
        F.col("trend_score"),
        F.col("views_percentile").alias("velocity_score"),
        F.lit(None).cast("double").alias("engagement_score"),
        F.lit(None).cast("double").alias("persistence_score"),
        F.lit(0.0).alias("market_expansion_score"),
        F.col("rank"),
        F.col("evidence"),
    )


def build_trend_opportunities(
    video_trends_df: DataFrame, category_df: DataFrame, cross_market_df: DataFrame, channel_df: DataFrame
) -> DataFrame:
    """Unions video, category, and channel opportunities into one ranked,
    explainable Gold table. Grain: one opportunity x scope x region x
    snapshot_date. This is the dashboard-ready 'what should we pay attention
    to' table (spec sections 18/24) -- every row carries the exact numbers
    (velocity/engagement/persistence/market_expansion scores) behind its
    trend_score in the `evidence` text, so 'why did this rank #1' is always
    answerable from the row itself."""
    video_opps = build_video_opportunities(video_trends_df)
    category_opps = build_category_opportunities(category_df, cross_market_df)
    channel_opps = build_channel_opportunities(channel_df)

    combined = video_opps.unionByName(category_opps).unionByName(channel_opps)
    return combined.withColumn("_aggregated_at", F.current_timestamp())


# ── Main Execution (Glue I/O) ────────────────────────────────────────────────
# Wrapping the execution logic prevents Glue libraries from executing
# when you import this file in your unit tests.

if __name__ == "__main__":
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "silver_database",
        "gold_bucket",
        "gold_database",
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

    # 1. Read Silver (video-grain) and the Gold tables silver_to_gold_analytics
    #    already wrote in this same pipeline run (see step_functions ordering).
    logger.info("Reading Silver clean_statistics...")
    stats_df = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB, table_name="clean_statistics", transformation_ctx="stats",
    ).toDF()

    logger.info("Reading Gold category_analytics + channel_analytics...")
    category_df = glueContext.create_dynamic_frame.from_catalog(
        database=GOLD_DB, table_name="category_analytics", transformation_ctx="category",
    ).toDF()
    channel_df = glueContext.create_dynamic_frame.from_catalog(
        database=GOLD_DB, table_name="channel_analytics", transformation_ctx="channel",
    ).toDF()

    # 2. Compute
    # NOTE: internally these functions all use "trending_date_parsed" (the
    # Silver column name) for windowing/joins — it's renamed to "snapshot_date"
    # only at write time below, for a consistent, BI-friendly column name
    # across every Gold table (see docs/decisions.md).
    logger.info("Computing gold_video_trends...")
    video_trends_df = compute_video_trends(stats_df)

    logger.info("Computing cross-market expansion signal...")
    cross_market_df = compute_cross_market_expansion(category_df)

    logger.info("Computing gold_trend_opportunities...")
    opportunities_df = build_trend_opportunities(video_trends_df, category_df, cross_market_df, channel_df)

    # 3. Write
    def write_to_gold(df: DataFrame, table_name: str, path: str, partition_keys: list):
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
        sink = glueContext.getSink(
            connection_type="s3",
            path=path,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=partition_keys,
        )
        sink.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName=table_name)
        sink.setFormat("glueparquet", compression="snappy")
        sink.writeFrame(dyf)
        logger.info(f"  Written {df.count()} rows -> {path}")

    logger.info("Writing Gold: video_trends...")
    write_to_gold(
        video_trends_df.withColumnRenamed("trending_date_parsed", "snapshot_date"),
        "video_trends", f"s3://{GOLD_BUCKET}/youtube/video_trends/", ["region"]
    )

    logger.info("Writing Gold: trend_opportunities...")
    write_to_gold(opportunities_df, "trend_opportunities", f"s3://{GOLD_BUCKET}/youtube/trend_opportunities/", ["scope"])

    logger.info("Trend Intelligence layer build complete.")
    job.commit()

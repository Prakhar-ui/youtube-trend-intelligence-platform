import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, LongType, BooleanType

# ── Transformation Functions (Testable locally with pure PySpark) ────────────

def enforce_schema(df: DataFrame) -> DataFrame:
    """Standardizes columns for both Kaggle CSV and YouTube API JSON formats.

    Kaggle CSV format has no channel_id/duration/definition/caption fields —
    these are populated as null for that source. Downstream Trend Intelligence
    logic must treat them as optional (see docs/data_model.md).
    """
    columns = set(df.columns)

    def api_col(dotted_name, cast=None):
        """Reads a YouTube-API field by its literal (dot or double-underscore)
        column name, tolerating either naming convention, and returns a null
        literal if the field is absent from this batch entirely."""
        underscored = dotted_name.replace(".", "__")
        if dotted_name in columns:
            c = F.col(f"`{dotted_name}`")
        elif underscored in columns:
            c = F.col(underscored)
        else:
            c = F.lit(None)
        return c.cast(cast) if cast else c

    if "snippet.title" in columns or "snippet__title" in columns:
        # YouTube API format — flatten nested structure
        return df.select(
            F.col("id").alias("video_id"),
            F.lit(datetime.utcnow().strftime("%y.%d.%m")).alias("trending_date"),
            api_col("snippet.title", StringType()).alias("title"),
            api_col("snippet.channelTitle", StringType()).alias("channel_title"),
            api_col("snippet.channelId", StringType()).alias("channel_id"),
            api_col("snippet.categoryId", LongType()).alias("category_id"),
            api_col("snippet.publishedAt", StringType()).alias("publish_time"),
            api_col("snippet.tags", StringType()).alias("tags"),
            api_col("statistics.viewCount", LongType()).alias("views"),
            api_col("statistics.likeCount", LongType()).alias("likes"),
            F.coalesce(api_col("statistics.dislikeCount", LongType()), F.lit(0).cast(LongType())).alias("dislikes"),
            api_col("statistics.commentCount", LongType()).alias("comment_count"),
            api_col("snippet.thumbnails.default.url", StringType()).alias("thumbnail_link"),
            F.lit(False).alias("comments_disabled"),
            F.lit(False).alias("ratings_disabled"),
            F.lit(False).alias("video_error_or_removed"),
            api_col("snippet.description", StringType()).alias("description"),
            api_col("contentDetails.duration", StringType()).alias("duration"),
            api_col("contentDetails.definition", StringType()).alias("definition"),
            api_col("contentDetails.caption", StringType()).alias("caption"),
            F.col("region"),
        )
    else:
        # Kaggle CSV format — just cast types. channel_id/duration/definition/
        # caption don't exist in the classic Kaggle dataset, so they're null.
        return df.select(
            F.col("video_id").cast(StringType()),
            F.col("trending_date").cast(StringType()),
            F.col("title").cast(StringType()),
            F.col("channel_title").cast(StringType()),
            F.lit(None).cast(StringType()).alias("channel_id"),
            F.col("category_id").cast(LongType()),
            F.col("publish_time").cast(StringType()),
            F.col("tags").cast(StringType()),
            F.col("views").cast(LongType()),
            F.col("likes").cast(LongType()),
            F.col("dislikes").cast(LongType()),
            F.col("comment_count").cast(LongType()),
            F.col("thumbnail_link").cast(StringType()),
            F.col("comments_disabled").cast(BooleanType()),
            F.col("ratings_disabled").cast(BooleanType()),
            F.col("video_error_or_removed").cast(BooleanType()),
            F.col("description").cast(StringType()),
            F.lit(None).cast(StringType()).alias("duration"),
            F.lit(None).cast(StringType()).alias("definition"),
            F.lit(None).cast(StringType()).alias("caption"),
            F.col("region").cast(StringType()),
        )


def add_duration_seconds(df: DataFrame) -> DataFrame:
    """Parses ISO-8601 durations from the YouTube API (e.g. 'PT4M13S') into
    whole seconds. Rows without a duration (e.g. Kaggle-format source data,
    or a 'duration' column that isn't present at all) get a null
    duration_seconds rather than a misleading 0."""
    if "duration" not in df.columns:
        return df.withColumn("duration_seconds", F.lit(None).cast("int"))

    hours = F.coalesce(F.regexp_extract(F.col("duration"), r"(\d+)H", 1).cast("int"), F.lit(0))
    minutes = F.coalesce(F.regexp_extract(F.col("duration"), r"(\d+)M", 1).cast("int"), F.lit(0))
    seconds = F.coalesce(F.regexp_extract(F.col("duration"), r"(\d+)S", 1).cast("int"), F.lit(0))
    return df.withColumn(
        "duration_seconds",
        F.when(
            F.col("duration").isNotNull(),
            hours * 3600 + minutes * 60 + seconds
        ).otherwise(F.lit(None).cast("int"))
    )


def add_rate_metrics(df: DataFrame) -> DataFrame:
    """Adds fractional engagement rate metrics (0-1 scale), as distinct from
    the legacy percentage-based like_ratio/engagement_rate columns kept below
    for backward compatibility. Formulas documented in docs/data_model.md:
        like_rate    = likes / views
        comment_rate = comment_count / views
    Both are 0.0 when views is 0 or null, never divide-by-zero."""
    df = df.withColumn(
        "like_rate",
        F.when(F.col("views") > 0, F.round(F.col("likes") / F.col("views"), 6)).otherwise(F.lit(0.0))
    )
    df = df.withColumn(
        "comment_rate",
        F.when(F.col("views") > 0, F.round(F.col("comment_count") / F.col("views"), 6)).otherwise(F.lit(0.0))
    )
    return df


def add_velocity_context(df: DataFrame) -> DataFrame:
    """Adds video_age_hours and views_per_hour.

    Uses trending_date_parsed (the snapshot date, already parsed earlier in
    cleanse_data) rather than the Glue job's wall-clock run time as the
    observation instant, so these values stay identical on reprocessing or
    backfill regardless of when the job actually executes.

    video_age_hours is null when publish_time or trending_date_parsed can't be
    parsed. views_per_hour floors the age at 1 hour to avoid divide-by-zero /
    unrealistic spikes for videos observed within their publish hour."""
    if "publish_time" not in df.columns or "trending_date_parsed" not in df.columns:
        return df.withColumn("video_age_hours", F.lit(None).cast("double")) \
                  .withColumn("views_per_hour", F.lit(None).cast("double"))

    publish_ts = F.to_timestamp(F.col("publish_time"))
    trending_ts = F.to_timestamp(F.col("trending_date_parsed"))

    df = df.withColumn(
        "video_age_hours",
        F.when(
            publish_ts.isNotNull() & trending_ts.isNotNull(),
            F.round((F.unix_timestamp(trending_ts) - F.unix_timestamp(publish_ts)) / 3600.0, 2)
        ).otherwise(F.lit(None).cast("double"))
    )
    df = df.withColumn(
        "views_per_hour",
        F.when(
            F.col("video_age_hours").isNotNull(),
            F.round(F.col("views") / F.greatest(F.col("video_age_hours"), F.lit(1.0)), 2)
        ).otherwise(F.lit(None).cast("double"))
    )
    return df


def cleanse_data(df: DataFrame, job_name: str) -> DataFrame:
    """Filters corrupt rows, parses dates, fills nulls, and calculates derived metrics."""
    # Remove records where video_id is null (corrupt rows)
    df = df.filter(F.col("video_id").isNotNull())

    # Standardize region codes to lower
    df = df.withColumn("region", F.lower(F.trim(F.col("region"))))

    # Parse trending_date from Kaggle format (YY.DD.MM) to proper date
    df = df.withColumn(
        "trending_date_parsed",
        F.when(
            F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
            F.to_date(F.col("trending_date"), "yy.dd.MM")
        ).otherwise(F.to_date(F.col("trending_date")))
    )

    # Fill nulls for numeric columns with 0
    numeric_cols = ["views", "likes", "dislikes", "comment_count"]
    for col_name in numeric_cols:
        df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

    # Add derived columns
    df = df.withColumn("like_ratio",
        F.when(
            (F.col("views") > 0), 
            F.round(F.col("likes") / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )
    df = df.withColumn("engagement_rate",
        F.when(
            (F.col("views") > 0),
            F.round((F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.col("views") * 100, 4)
        ).otherwise(0.0)
    )

    # Spec-defined fractional rate metrics, ISO-8601 duration parsing, and
    # publish-to-trending velocity context (see docs/data_model.md)
    df = add_rate_metrics(df)
    df = add_duration_seconds(df)
    df = add_velocity_context(df)

    # Add processing metadata
    df = df.withColumn("_processed_at", F.current_timestamp())
    df = df.withColumn("_job_name", F.lit(job_name))
    
    return df


def deduplicate_data(df: DataFrame) -> DataFrame:
    """Keeps the latest record per video_id + region + trending_date."""
    window = Window.partitionBy("video_id", "region", "trending_date_parsed") \
        .orderBy(F.col("_processed_at").desc())

    return df.withColumn("_row_num", F.row_number().over(window)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")


def run_data_quality_checks(df: DataFrame, logger) -> dict:
    """Runs DQ checks and logs warnings. Returns a dict of check results for test assertions."""
    results = {"nulls": {}, "negative_views": 0}
    
    for col_name in ["video_id", "title", "channel_title", "views"]:
        null_count = df.filter(F.col(col_name).isNull()).count()
        results["nulls"][col_name] = null_count
        if null_count > 0:
            logger.warn(f"  DQ WARNING: {col_name} has {null_count} null values")

    negative_views = df.filter(F.col("views") < 0).count()
    results["negative_views"] = negative_views
    if negative_views > 0:
        logger.warn(f"  DQ WARNING: {negative_views} records with negative views")
        
    return results


# ── Main Execution (Glue I/O) ────────────────────────────────────────────────
# Wrapping the execution logic in an if __name__ block prevents Glue libraries 
# from loading and executing when you import this file in your unit tests.

if __name__ == "__main__":
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame

    # Job Setup
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "bronze_database", "bronze_table",
        "silver_bucket", "silver_database", "silver_table", "silver_path",
        "regions"
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    logger = glueContext.get_logger()

    # Config
    BRONZE_DB = args["bronze_database"]
    BRONZE_TABLE = args["bronze_table"]
    SILVER_PATH = f"s3://{args['silver_bucket']}/{args['silver_path']}"
    
    logger.info(f"Bronze: {BRONZE_DB}.{BRONZE_TABLE}")
    logger.info(f"Silver: {args['silver_database']}.{args['silver_table']} → {SILVER_PATH}")

    # 1. Read from Bronze
    # Markets are driven by the --regions job parameter (see
    # bronze_to_silver_statistics_job.tf) rather than hardcoded here, so the
    # platform can add/remove markets without a code change. NOTE: this was
    # previously hardcoded to only 4 of the ingested regions ('ca','gb','us',
    # 'in'), silently dropping the other 6 markets before they ever reached
    # Silver/Gold — fixed as part of the Trend Intelligence Platform rollout.
    regions = [r.strip().lower() for r in args["regions"].split(",") if r.strip()]
    predicate = "region in (" + ",".join(f"'{r}'" for r in regions) + ")"
    logger.info(f"Processing regions: {regions}")
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DB,
        table_name=BRONZE_TABLE,
        transformation_ctx="datasource",
        push_down_predicate=predicate,
    )

    df = datasource.toDF()
    initial_count = df.count()
    
    if initial_count == 0:
        logger.info("No new records to process. Committing empty job.")
    else:
        # Apply Modular Transformations
        df = enforce_schema(df)
        df = cleanse_data(df, args["JOB_NAME"])
        df = deduplicate_data(df)
        
        clean_count = df.count()
        logger.info(f"After cleansing & dedup: {clean_count} records (removed {initial_count - clean_count})")
        
        # Run DQ Checks
        run_data_quality_checks(df, logger)

        # Write to Silver
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "silver_statistics")
        sink = glueContext.getSink(
            connection_type="s3",
            path=SILVER_PATH,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["region"],
            transformation_ctx="silver_sink",
        )
        sink.setCatalogInfo(catalogDatabase=args["silver_database"], catalogTableName=args["silver_table"])
        sink.setFormat("glueparquet", compression="snappy")
        sink.writeFrame(dynamic_frame)
        logger.info(f"Silver write complete. {clean_count} records written.")

    job.commit()
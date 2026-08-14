import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Configuration (Loaded dynamically for testability) ───────────────────────
def get_config():
    """Loads environment variables dynamically so tests can override them."""
    return {
        "sns_topic": os.environ.get("SNS_ALERT_TOPIC_ARN", ""),
        "athena_output": os.environ.get("ATHENA_OUTPUT_LOCATION"),
        "athena_workgroup": os.environ.get("ATHENA_WORKGROUP"),
        "min_row_count": int(os.environ.get("DQ_MIN_ROW_COUNT", "10")),
        "max_null_pct": float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0")),
        "max_views": 50_000_000_000,
        "freshness_hours": 48
    }

CRITICAL_COLUMNS = {
    "clean_statistics": [
        "video_id",
        "title",
        "channel_title",
        "views",
        "region"
    ],
    "video_trends": [
        "video_id",
        "region",
        "snapshot_date",
        "views",
        "trend_stage",
    ],
    "category_analytics": [
        "category_id",
        "category_name",
        "region",
        "snapshot_date",
    ],
    "channel_analytics": [
        "channel_id",
        "channel_title",
        "region",
        "snapshot_date",
    ],
    "trend_opportunities": [
        "snapshot_date",
        "scope",
        "entity_id",
        "trend_score",
    ],
}

# Grain (natural key) for each table -- used by check_uniqueness. A duplicate
# key here means the same real-world observation/opportunity was written
# twice, which would silently double-count in every downstream aggregate and
# dashboard total.
UNIQUE_KEYS = {
    "clean_statistics": ["video_id", "region", "trending_date_parsed"],
    "video_trends": ["video_id", "region", "snapshot_date"],
    "category_analytics": ["category_id", "region", "snapshot_date"],
    "channel_analytics": ["channel_id", "region", "snapshot_date"],
    "trend_opportunities": ["scope", "region", "entity_id", "snapshot_date"],
}

# Expected value ranges for Gold-layer scored/normalized columns. Every score
# in gold_trend_opportunities is documented (docs/trend_intelligence.md) as a
# 0-100 percentile-based value -- if the computation has a bug, this is the
# cheapest place to catch it, before it reaches a dashboard.
METRIC_BOUNDS = {
    "trend_opportunities": [
        ("trend_score", 0, 100),
        ("velocity_score", 0, 100),
        ("engagement_score", 0, 100),
        ("market_expansion_score", 0, 100),
    ],
    "video_trends": [
        ("persistence_score", 0, 100),
    ],
}

# ── Core Business Logic (Pure functions, no AWS dependencies) ────────────────

def check_row_count(df: pd.DataFrame, table_name: str, min_count: int) -> dict:
    count = len(df)
    passed = count >= min_count
    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": min_count,
        "passed": passed,
        "message": f"Row count: {count} (min: {min_count})",
    }


def check_null_percentage(df: pd.DataFrame, table_name: str, max_null_pct: float) -> list:
    results = []
    cols = CRITICAL_COLUMNS.get(table_name, [])

    for col in cols:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Column '{col}' missing from table",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
        passed = bool(null_pct <= max_null_pct)
        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(null_pct, 2),
            "threshold": max_null_pct,
            "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {max_null_pct}%)",
        })

    return results


def check_schema(df: pd.DataFrame, table_name: str) -> dict:
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(df.columns)
    missing = expected - actual
    passed = len(missing) == 0
    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": list(missing),
        "passed": passed,
        "message": f"Missing columns: {missing}" if missing else "All expected columns present",
    }


def check_value_ranges(df: pd.DataFrame, table_name: str, max_views: int) -> list:
    results = []
    if table_name != "clean_statistics" or "views" not in df.columns:
        return results

    negative = int((df["views"] < 0).sum())
    extreme = int((df["views"] > max_views).sum())
    passed = bool(negative == 0 and extreme == 0)
    
    results.append({
        "check": "value_range",
        "table": table_name,
        "column": "views",
        "negative_count": negative,
        "extreme_count": extreme,
        "passed": passed,
        "message": f"Views: {negative} negative, {extreme} extreme (>{max_views})",
    })

    return results


def check_freshness(df: pd.DataFrame, table_name: str, freshness_hours: int) -> dict:
    if "_processed_at" not in df.columns and "_ingestion_timestamp" not in df.columns:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found — skipping freshness check (backfill data)",
        }

    ts_col = "_processed_at" if "_processed_at" in df.columns else "_ingestion_timestamp"
    try:
        latest = pd.to_datetime(df[ts_col]).max()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=freshness_hours)
        
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
            
        passed = latest >= cutoff
        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "passed": passed,
            "message": f"Latest: {latest}, Cutoff: {cutoff}",
        }
    except Exception as e:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True, # Fail open if unparseable
            "message": f"Could not parse timestamps: {e} — skipping",
        }


def check_uniqueness(df: pd.DataFrame, table_name: str, key_columns: list = None) -> dict:
    """Flags rows sharing a duplicate natural key -- e.g. the same video
    observed twice for the same region+snapshot_date. A duplicate here means
    every downstream aggregate/dashboard total for that row is double-counted.
    """
    key_columns = key_columns if key_columns is not None else UNIQUE_KEYS.get(table_name, [])

    if not key_columns or not all(c in df.columns for c in key_columns):
        return {
            "check": "uniqueness",
            "table": table_name,
            "passed": True,
            "message": "No key columns configured or present for this table -- skipping",
        }

    duplicate_count = int(df.duplicated(subset=key_columns, keep=False).sum())
    passed = duplicate_count == 0
    key_desc = "+".join(key_columns)
    return {
        "check": "uniqueness",
        "table": table_name,
        "key_columns": key_columns,
        "duplicate_row_count": duplicate_count,
        "passed": passed,
        "message": (
            f"{duplicate_count} row(s) share a duplicate {key_desc} key"
            if duplicate_count else f"No duplicate {key_desc} keys"
        ),
    }


def check_metric_sanity(df: pd.DataFrame, table_name: str, bounds: list = None) -> list:
    """Checks that scored/normalized Gold columns (e.g. the 0-100
    trend/velocity/engagement scores documented in
    docs/trend_intelligence.md) actually stay within their documented range.
    A value outside range means the scoring formula has a real bug -- this is
    the cheapest place to catch it, before it reaches a dashboard."""
    bounds = bounds if bounds is not None else METRIC_BOUNDS.get(table_name, [])
    results = []

    for column, lo, hi in bounds:
        if column not in df.columns:
            continue
        series = df[column].dropna()
        out_of_bounds = int(((series < lo) | (series > hi)).sum())
        passed = out_of_bounds == 0
        results.append({
            "check": "metric_sanity",
            "table": table_name,
            "column": column,
            "out_of_bounds_count": out_of_bounds,
            "expected_range": [lo, hi],
            "passed": passed,
            "message": f"{column}: {out_of_bounds} value(s) outside [{lo}, {hi}]",
        })

    return results


# ── AWS I/O Adapters (Easily mockable in tests) ──────────────────────────────

def fetch_table_data(database: str, table_name: str, athena_output: str, workgroup: str) -> pd.DataFrame:
    """Wrapper for AWS Wrangler to fetch Athena data."""
    query = f'SELECT * FROM "{table_name}" LIMIT 10000'
    return wr.athena.read_sql_query(
        sql=query,
        database=database,
        s3_output=athena_output,
        workgroup=workgroup,
    )

def send_failure_alert(sns_topic: str, failed_checks: list):
    """Wrapper for boto3 to send SNS alerts."""
    sns_client = boto3.client("sns", region_name="ap-south-1")
    sns_client.publish(
        TopicArn=sns_topic,
        Subject="[YT Pipeline] Data quality checks FAILED",
        Message=json.dumps(failed_checks, indent=2, default=str),
    )


# ── Main Handler ─────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    config = get_config()
    database = event.get("database", "yt_pipeline_silver_dev")
    tables = event.get("tables", ["clean_statistics"])

    all_results = []
    overall_passed = True

    for table_name in tables:
        logger.info(f"Running DQ checks on {database}.{table_name}...")

        try:
            df = fetch_table_data(
                database, 
                table_name, 
                config["athena_output"], 
                config["athena_workgroup"]
            )
        except Exception as e:
            logger.error(f"Could not read {table_name}: {e}")
            all_results.append({
                "check": "read_table",
                "table": table_name,
                "passed": False,
                "message": str(e),
            })
            overall_passed = False
            continue

        # Run checks and inject config parameters
        checks = []
        checks.append(check_row_count(df, table_name, config["min_row_count"]))
        checks.extend(check_null_percentage(df, table_name, config["max_null_pct"]))
        checks.append(check_schema(df, table_name))
        checks.extend(check_value_ranges(df, table_name, config["max_views"]))
        checks.append(check_freshness(df, table_name, config["freshness_hours"]))
        checks.append(check_uniqueness(df, table_name))
        checks.extend(check_metric_sanity(df, table_name))

        for check in checks:
            logger.info(f"  {check['check']}: {'PASS' if check['passed'] else 'FAIL'} — {check['message']}")
            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # Summary and Alerting
    passed_count = sum(1 for r in all_results if r["passed"])
    total_count = len(all_results)
    failed_count = total_count - passed_count
    quality_score = round((passed_count / total_count) * 100, 2) if total_count else 100.0

    logger.info(f"DQ Summary: {passed_count}/{total_count} passed ({quality_score}%). Overall: {'PASS' if overall_passed else 'FAIL'}")

    if not overall_passed and config["sns_topic"]:
        failed = [r for r in all_results if not r["passed"]]
        try:
            send_failure_alert(config["sns_topic"], failed)
        except Exception as e:
            logger.error(f"Failed to send SNS alert: {e}")

    return {
        "quality_passed": bool(overall_passed),
        "quality_score": quality_score,
        "checks_passed": int(passed_count),
        "checks_failed": int(failed_count),
        "checks_total": int(total_count),
        "details": json.loads(json.dumps(all_results, default=str)),
    }

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pandas as pd
import pytest


def test_get_config_reads_environment(data_quality_check_module, monkeypatch):
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn")
    monkeypatch.setenv("ATHENA_OUTPUT_LOCATION", "s3://out")
    monkeypatch.setenv("ATHENA_WORKGROUP", "wg")
    monkeypatch.setenv("DQ_MIN_ROW_COUNT", "25")
    monkeypatch.setenv("DQ_MAX_NULL_PERCENT", "2.5")
    config = data_quality_check_module.get_config()
    assert config["sns_topic"] == "arn"
    assert config["athena_output"] == "s3://out"
    assert config["athena_workgroup"] == "wg"
    assert config["min_row_count"] == 25
    assert config["max_null_pct"] == 2.5


def test_check_null_percentage_flags_missing_columns(data_quality_check_module):
    result = data_quality_check_module.check_null_percentage(
        pd.DataFrame({"video_id": ["a"]}), "clean_statistics", 5
    )
    missing = {r["column"] for r in result if not r["passed"]}
    assert {"title", "channel_title", "views", "region"} <= missing


@pytest.mark.parametrize(
    "column,value,expected",
    [("_processed_at", datetime.now(timezone.utc), True),
     ("_ingestion_timestamp", datetime.now(timezone.utc), True)]
)
def test_check_freshness_uses_available_timestamp(data_quality_check_module, column, value, expected):
    result = data_quality_check_module.check_freshness(
        pd.DataFrame({column: [value]}), "table", 48
    )
    assert result["passed"] is expected


def test_check_freshness_fails_for_stale_data(data_quality_check_module):
    stale = datetime.now(timezone.utc) - timedelta(hours=72)
    result = data_quality_check_module.check_freshness(pd.DataFrame({"_processed_at": [stale]}), "table", 48)
    assert result["passed"] is False


def test_check_freshness_skips_when_no_timestamp(data_quality_check_module):
    result = data_quality_check_module.check_freshness(pd.DataFrame({"id": [1]}), "table", 48)
    assert result["passed"] is True
    assert "skipping" in result["message"]


def test_check_freshness_fails_open_for_unparseable_timestamp(data_quality_check_module):
    result = data_quality_check_module.check_freshness(
        pd.DataFrame({"_processed_at": ["not-a-date"]}), "table", 48
    )
    assert result["passed"] is True
    assert "Could not parse" in result["message"]


def test_check_uniqueness_accepts_custom_keys(data_quality_check_module):
    df = pd.DataFrame({"a": [1, 1], "b": [2, 3]})
    result = data_quality_check_module.check_uniqueness(df, "custom", key_columns=["a"])
    assert result["passed"] is False
    assert result["duplicate_row_count"] == 2


def test_check_metric_sanity_skips_missing_columns(data_quality_check_module):
    result = data_quality_check_module.check_metric_sanity(pd.DataFrame({"x": [1]}), "trend_opportunities")
    assert result == []


def test_fetch_table_data_calls_athena(data_quality_check_module):
    expected = pd.DataFrame({"x": [1]})
    with patch.object(data_quality_check_module.wr.athena, "read_sql_query", return_value=expected) as read_sql:
        result = data_quality_check_module.fetch_table_data("db", "table", "s3://out", "wg")
    assert result.equals(expected)
    kwargs = read_sql.call_args.kwargs
    assert kwargs["sql"] == 'SELECT * FROM "table" LIMIT 10000'
    assert kwargs["database"] == "db"
    assert kwargs["s3_output"] == "s3://out"
    assert kwargs["workgroup"] == "wg"


def test_send_failure_alert_publishes_json(data_quality_check_module):
    client = Mock()
    with patch.object(data_quality_check_module.boto3, "client", return_value=client):
        data_quality_check_module.send_failure_alert("arn", [{"passed": False}])
    call = client.publish.call_args.kwargs
    assert call["TopicArn"] == "arn"
    assert call["Subject"] == "[YT Pipeline] Data quality checks FAILED"


def test_lambda_handler_all_checks_pass(data_quality_check_module, monkeypatch):
    monkeypatch.setenv("DQ_MIN_ROW_COUNT", "1")
    df = pd.DataFrame({
        "video_id": ["a"], "title": ["t"], "channel_title": ["c"], "views": [100],
        "region": ["us"], "trending_date_parsed": ["2026-08-14"],
    })
    with patch.object(data_quality_check_module, "fetch_table_data", return_value=df):
        result = data_quality_check_module.lambda_handler({"tables": ["clean_statistics"]}, None)
    assert result["quality_passed"] is True
    assert result["checks_failed"] == 0
    assert result["quality_score"] == 100.0


def test_lambda_handler_handles_read_failure(data_quality_check_module):
    with patch.object(data_quality_check_module, "fetch_table_data", side_effect=Exception("Athena down")):
        result = data_quality_check_module.lambda_handler({"tables": ["clean_statistics"]}, None)
    assert result["quality_passed"] is False
    assert result["checks_failed"] == 1
    assert result["details"][0]["check"] == "read_table"


def test_lambda_handler_alerts_on_failed_checks(data_quality_check_module, monkeypatch):
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn")
    df = pd.DataFrame({"video_id": ["a"]})  # intentionally fails schema/null checks
    with patch.object(data_quality_check_module, "fetch_table_data", return_value=df), \
         patch.object(data_quality_check_module, "send_failure_alert") as alert:
        result = data_quality_check_module.lambda_handler({"tables": ["clean_statistics"]}, None)
    assert result["quality_passed"] is False
    alert.assert_called_once()


def test_check_freshness_handles_naive_timestamp(data_quality_check_module):
    result = data_quality_check_module.check_freshness(
        pd.DataFrame({"_processed_at": ["2026-08-14 10:00:00"]}), "table", 48
    )
    assert result["passed"] is True


def test_lambda_handler_continues_when_alert_send_fails(data_quality_check_module, monkeypatch):
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn")
    with patch.object(data_quality_check_module, "fetch_table_data", side_effect=Exception("Athena down")), \
         patch.object(data_quality_check_module, "send_failure_alert", side_effect=Exception("SNS down")):
        result = data_quality_check_module.lambda_handler({"tables": ["clean_statistics"]}, None)
    assert result["quality_passed"] is False
    assert result["checks_total"] == 1

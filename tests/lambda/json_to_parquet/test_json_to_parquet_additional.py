
import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest


def test_get_config_reads_environment(json_to_parquet_module, monkeypatch):
    monkeypatch.setenv("S3_BUCKET_BRONZE", "bronze")
    monkeypatch.setenv("S3_BUCKET_SILVER", "silver")
    monkeypatch.setenv("GLUE_DB_SILVER", "db")
    monkeypatch.setenv("GLUE_TABLE_REFERENCE", "ref")
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn")
    assert json_to_parquet_module.get_config() == {
        "bronze_bucket": "bronze",
        "silver_bucket": "silver",
        "glue_db": "db",
        "glue_table": "ref",
        "sns_topic": "arn",
    }


def test_extract_s3_records_supports_direct_s3_event(json_to_parquet_module):
    event = {"s3": {"bucket": {"name": "b"}, "object": {"key": "x%2By.json"}}}
    assert json_to_parquet_module.extract_s3_records(event) == [("b", "x+y.json")]


def test_extract_s3_records_ignores_non_s3_records(json_to_parquet_module):
    event = {"Records": [{"foo": "bar"}, {"s3": {"bucket": {"name": "b"}, "object": {"key": "x"}}}]}
    assert json_to_parquet_module.extract_s3_records(event) == [("b", "x")]


def test_normalize_json_without_items(json_to_parquet_module):
    result = json_to_parquet_module.normalize_json_to_df({"id": "1", "snippet": {"title": "Music"}})
    assert list(result.columns) == ["id", "snippet.title"]
    assert result.iloc[0]["snippet.title"] == "Music"


def test_validate_category_data_warns_for_missing_required_columns(json_to_parquet_module, caplog):
    df = pd.DataFrame({"id": ["1", "2"]})
    with caplog.at_level("WARNING"):
        result = json_to_parquet_module.validate_category_data(df)
    assert len(result) == 2
    assert "Missing expected columns" in caplog.text


def test_validate_category_data_keeps_last_duplicate(json_to_parquet_module):
    df = pd.DataFrame({"id": ["1", "1"], "snippet_title": ["Old", "New"]})
    result = json_to_parquet_module.validate_category_data(df)
    assert len(result) == 1
    assert result.iloc[0]["snippet_title"] == "New"


def test_enrich_data_adds_metadata(json_to_parquet_module):
    df = pd.DataFrame({"id": ["1"]})
    result = json_to_parquet_module.enrich_data(df, "some/key.json", "in")
    assert result.loc[0, "region"] == "in"
    assert result.loc[0, "_source_file"] == "some/key.json"
    assert result.loc[0, "_ingestion_timestamp"]


def test_read_json_from_s3_reads_and_decodes(json_to_parquet_module):
    body = Mock()
    body.read.return_value = b'{"items":[{"id":"1"}]}'
    client = Mock()
    client.get_object.return_value = {"Body": body}
    with patch.object(json_to_parquet_module.boto3, "client", return_value=client):
        result = json_to_parquet_module.read_json_from_s3("bucket", "key")
    assert result == {"items": [{"id": "1"}]}
    client.get_object.assert_called_once_with(Bucket="bucket", Key="key")


def test_write_to_silver_delegates_to_awswrangler(json_to_parquet_module):
    df = pd.DataFrame({"id": ["1"], "region": ["us"]})
    config = {"silver_bucket": "silver", "glue_db": "db", "glue_table": "table"}
    with patch.object(json_to_parquet_module.wr.s3, "to_parquet") as to_parquet:
        path = json_to_parquet_module.write_to_silver(df, config)
    assert path == "s3://silver/youtube/reference_data/"
    kwargs = to_parquet.call_args.kwargs
    assert kwargs["database"] == "db"
    assert kwargs["table"] == "table"
    assert kwargs["partition_cols"] == ["region"]
    assert kwargs["mode"] == "overwrite_partitions"


def test_send_alert_noop_without_topic(json_to_parquet_module):
    with patch.object(json_to_parquet_module.boto3, "client") as client:
        json_to_parquet_module.send_alert("", "subject", "message")
    client.assert_not_called()


def test_send_alert_publishes_when_topic_exists(json_to_parquet_module):
    client = Mock()
    with patch.object(json_to_parquet_module.boto3, "client", return_value=client):
        json_to_parquet_module.send_alert("arn", "x" * 150, "message")
    call = client.publish.call_args.kwargs
    assert call["TopicArn"] == "arn"
    assert len(call["Subject"]) == 100


def test_validate_category_data_without_id_column_does_not_dedupe(json_to_parquet_module):
    df = pd.DataFrame({"snippet_title": ["Music", "Music"]})
    result = json_to_parquet_module.validate_category_data(df)
    assert len(result) == 2


def test_lambda_handler_processes_direct_s3_event(json_to_parquet_module, monkeypatch):
    monkeypatch.setenv("S3_BUCKET_SILVER", "silver")
    event = {"s3": {"bucket": {"name": "bronze"}, "object": {"key": "youtube/raw_statistics_reference_data/region=us/date=2026-08-14/us.json"}}}
    with patch.object(
        json_to_parquet_module, "read_json_from_s3",
        return_value={"items": [{"id": "10", "snippet": {"title": "Music"}}]},
    ), patch.object(json_to_parquet_module, "write_to_silver", return_value="s3://silver/youtube/reference_data/"):
        result = json_to_parquet_module.lambda_handler(event, None)
    assert result["statusCode"] == 200
    assert result["errors"] == []
    assert result["processed"][0]["region"] == "us"

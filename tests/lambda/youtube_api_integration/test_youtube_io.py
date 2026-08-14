
import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import Mock, patch

import pytest


def test_get_config_reads_environment(youtube_ingestion, monkeypatch):
    monkeypatch.setenv("YOUTUBE_API_KEY", "key")
    monkeypatch.setenv("S3_BUCKET_BRONZE", "bucket")
    monkeypatch.setenv("YOUTUBE_REGIONS", "US, GB")
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn:sns")
    config = youtube_ingestion.get_config()
    assert config["api_key"] == "key"
    assert config["bucket"] == "bucket"
    assert config["regions"] == ["US", " GB"]
    assert config["sns_topic"] == "arn:sns"


def test_fetch_json_from_api_parses_response(youtube_ingestion):
    response = Mock()
    response.read.return_value = b'{"items":[{"id":"1"}]}'
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=None)
    with patch.object(youtube_ingestion, "urlopen", return_value=response) as mocked:
        result = youtube_ingestion.fetch_json_from_api("https://example.test")
    assert result == {"items": [{"id": "1"}]}
    mocked.assert_called_once()
    request = mocked.call_args.args[0]
    assert request.full_url == "https://example.test"
    assert request.headers["Accept"] == "application/json"


def test_write_to_s3_serializes_utf8_json(youtube_ingestion):
    client = Mock()
    with patch.object(youtube_ingestion.boto3, "client", return_value=client):
        youtube_ingestion.write_to_s3({"title": "café"}, "bucket", "key.json")
    call = client.put_object.call_args.kwargs
    assert call["Bucket"] == "bucket"
    assert call["Key"] == "key.json"
    assert json.loads(call["Body"].decode("utf-8")) == {"title": "café"}
    assert call["ContentType"] == "application/json"
    assert call["Metadata"]["source"] == "youtube_data_api_v3"


def test_send_alert_is_noop_without_topic(youtube_ingestion):
    with patch.object(youtube_ingestion.boto3, "client") as client:
        youtube_ingestion.send_alert("", "subject", "message")
    client.assert_not_called()


def test_send_alert_publishes_with_truncated_subject(youtube_ingestion):
    client = Mock()
    with patch.object(youtube_ingestion.boto3, "client", return_value=client):
        youtube_ingestion.send_alert("arn:sns", "x" * 150, "message")
    call = client.publish.call_args.kwargs
    assert call["TopicArn"] == "arn:sns"
    assert len(call["Subject"]) == 100
    assert call["Message"] == "message"


@pytest.mark.parametrize(
    "regions,expected_success,expected_failures",
    [
        ("US", ["us"], []),
        ("US,GB", ["us"], [{"region": "gb", "type": "categories", "error": "category down"}]),
    ],
)
def test_lambda_handler_handles_category_failure(youtube_ingestion, monkeypatch, regions, expected_success, expected_failures):
    monkeypatch.setenv("YOUTUBE_REGIONS", regions)
    responses = iter([
        {"items": [{"id": "video"}]},
        {"items": [{"id": "category"}]},
        {"items": [{"id": "video"}]},
    ])

    def fetch(url):
        if "videoCategories" in url and "gb" in url:
            raise Exception("category down")
        return next(responses)

    with patch.object(youtube_ingestion, "fetch_json_from_api", side_effect=fetch), \
         patch.object(youtube_ingestion, "write_to_s3"), \
         patch.object(youtube_ingestion, "send_alert") as alert:
        result = youtube_ingestion.lambda_handler({}, None)

    assert result["results"]["success"] == expected_success
    assert result["results"]["failed"] == expected_failures
    if expected_failures:
        alert.assert_called_once()


def test_lambda_handler_skips_categories_after_trending_failure(youtube_ingestion, monkeypatch):
    monkeypatch.setenv("YOUTUBE_REGIONS", "US")
    with patch.object(youtube_ingestion, "fetch_json_from_api", side_effect=Exception("trending down")) as fetch, \
         patch.object(youtube_ingestion, "write_to_s3"), \
         patch.object(youtube_ingestion, "send_alert") as alert:
        with pytest.raises(RuntimeError, match="failed for ALL"):
            youtube_ingestion.lambda_handler({}, None)
    assert fetch.call_count == 1
    alert.assert_called_once()

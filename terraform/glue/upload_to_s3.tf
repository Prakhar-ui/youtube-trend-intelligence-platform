resource "aws_s3_object" "bronze_to_silver_statistics_glue_script" {
  bucket = format("%s-bronze-%s", local.name_prefix, local.account_id)

  key = "glue/scripts/bronze_to_silver_statistics.py"

  source = "${path.module}/scripts/bronze_to_silver_statistics.py"

  etag = filemd5("${path.module}/scripts/bronze_to_silver_statistics.py")
}

resource "aws_s3_object" "silver_to_gold_analytics_glue_script" {
  bucket = format("%s-bronze-%s", local.name_prefix, local.account_id)

  key = "glue/scripts/silver_to_gold_analytics.py"

  source = "${path.module}/scripts/silver_to_gold_analytics.py"

  etag = filemd5("${path.module}/scripts/silver_to_gold_analytics.py")
}

resource "aws_s3_object" "trend_intelligence_glue_script" {
  bucket = format("%s-bronze-%s", local.name_prefix, local.account_id)

  key = "glue/scripts/trend_intelligence.py"

  source = "${path.module}/scripts/trend_intelligence.py"

  etag = filemd5("${path.module}/scripts/trend_intelligence.py")
}

resource "aws_glue_job" "trend_intelligence_glue_job" {
  name = "trend_intelligence"

  role_arn = data.terraform_remote_state.iam.outputs.glue_iam_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  execution_class   = "STANDARD"

  timeout     = 60
  max_retries = 1

  command {
    name = "glueetl"

    script_location = format("s3://%s-bronze-%s/glue/scripts/trend_intelligence.py", local.name_prefix, local.account_id)

    python_version = "3"
  }

  default_arguments = {
    #################################################
    # Glue Configuration
    #################################################

    "--job-language" = "python"

    "--enable-continuous-cloudwatch-log" = "true"

    "--enable-metrics" = "true"

    "--enable-glue-datacatalog" = "true"

    # NOTE: job bookmarks are intentionally NOT enabled, for the same reason
    # as silver_to_gold_analytics — this job recomputes video-grain velocity
    # and persistence over the full Silver history (via window functions that
    # need every prior snapshot of a video, not just newly-bookmarked rows).

    "--TempDir" = format("s3://%s-bronze-%s/glue/temp/", local.name_prefix, local.account_id)

    #################################################
    # PySpark Performance Configuration
    #################################################

    "--conf" = "spark.sql.shuffle.partitions=4 spark.sql.adaptive.enabled=true"

    #################################################
    # Environment
    #################################################

    "--ENV" = "dev"

    #################################################
    # Silver Layer Parameters
    #################################################

    "--silver_database" = "yt_pipeline_silver_dev"

    #################################################
    # Gold Layer Parameters
    #
    # Reads category_analytics/channel_analytics (written by
    # silver_to_gold_analytics, which must run first — see
    # terraform/step_functions/main.tf) and writes video_trends
    # + trend_opportunities into the same Gold database/bucket.
    #################################################

    "--gold_bucket" = format("%s-gold-%s", local.name_prefix, local.account_id)

    "--gold_database" = "yt_pipeline_gold_dev"
  }

  tags = {
    Name        = "trend_intelligence"
    Environment = "dev"
    Project     = local.name_prefix
  }

  depends_on = [
    aws_s3_object.trend_intelligence_glue_script
  ]
}

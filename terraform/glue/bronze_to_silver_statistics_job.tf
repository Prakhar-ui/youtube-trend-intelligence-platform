resource "aws_glue_job" "bronze_to_silver_statistics_glue_job" {
  name = "bronze_to_silver_statistics"

  role_arn = data.terraform_remote_state.iam.outputs.glue_iam_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  execution_class   = "STANDARD"

  timeout     = 60
  max_retries = 1

  command {
    name = "glueetl"

    script_location = format("s3://%s-bronze-%s/glue/scripts/bronze_to_silver_statistics.py", local.name_prefix, local.account_id)

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

    "--job-bookmark-option" = "job-bookmark-enable"

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
    # Bronze Layer Parameters
    #################################################

    "--bronze_database" = "yt_pipeline_bronze_dev"

    "--bronze_table" = "raw_statistics"

    #################################################
    # Markets
    #
    # NOTE: keep this in sync with YOUTUBE_REGIONS in
    # terraform/lambda/youtube_api_integration_function.tf. Each module here
    # is a separate Terraform root/state, so there isn't a single shared
    # variable to source this from without a larger restructuring; documented
    # as a known limitation in docs/decisions.md.
    #################################################

    "--regions" = "us,gb,in,ca,au,de,fr,jp,kr,ru"

    #################################################
    # Silver Layer Parameters
    #################################################

    "--silver_bucket" = format("%s-silver-%s", local.name_prefix, local.account_id)

    "--silver_database" = "yt_pipeline_silver_dev"

    "--silver_table" = "clean_statistics"

    "--silver_path" = "youtube/clean_statistics/"

    #################################################
    # Optional Additional Buckets
    #################################################

    "--S3_BUCKET_BRONZE" = format("%s-bronze-%s", local.name_prefix, local.account_id)

    "--S3_BUCKET_SILVER" = format("%s-silver-%s", local.name_prefix, local.account_id)

    "--S3_BUCKET_GOLD" = format("%s-gold-%s", local.name_prefix, local.account_id)
  }

  tags = {
    Name        = "bronze_to_silver_statistics"
    Environment = "dev"
    Project     = local.name_prefix
  }

  depends_on = [
    aws_s3_object.bronze_to_silver_statistics_glue_script
  ]
}

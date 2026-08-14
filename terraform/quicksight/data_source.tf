#################################################
# QuickSight Athena Data Source
#
# PREREQUISITE (one-time, manual, cannot be fully automated by Terraform):
# QuickSight must already be subscribed/enabled in this AWS account, and its
# service role must be granted access to the Gold S3 bucket + the Athena
# query-results bucket via QuickSight console > Manage QuickSight > Security
# & permissions > "Add or remove" > check S3 (select the gold + query-result
# buckets) and Athena. This is an AWS account-level, one-time setup step that
# has no Terraform resource — see docs/deployment.md.
#################################################

resource "aws_quicksight_data_source" "gold_athena" {
  data_source_id = "yt-trend-intelligence-gold-athena"
  name           = "YouTube Trend Intelligence - Gold (Athena)"
  aws_account_id = local.account_id

  type = "ATHENA"

  parameters {
    athena {
      work_group = "primary"
    }
  }

  ssl_properties {
    disable_ssl = false
  }

  tags = {
    Name        = "yt-trend-intelligence-gold-athena"
    Environment = "dev"
    Project     = local.name_prefix
  }
}

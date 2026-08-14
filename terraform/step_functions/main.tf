locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  name_prefix = "yt-data-pipeline"
}

#################################################
# Fetch IAM Remote State
#################################################

data "terraform_remote_state" "iam" {
  backend = "s3"

  config = {
    bucket = "yt-terraform-state-prakhar"
    key    = "iam/terraform.tfstate"
    region = "ap-south-1"
  }
}

#################################################
# Step Function State Machine
#################################################

resource "aws_sfn_state_machine" "yt_pipeline_state_machine" {

  name = "${local.name_prefix}-orchestration-dev"

  role_arn = data.terraform_remote_state.iam.outputs.step_function_role_arn

  type = "STANDARD"

  definition = jsonencode({

    Comment = "YouTube Data Pipeline — Full orchestration via Step Functions"

    StartAt = "IngestFromYouTubeAPI"

    States = {

      #################################################
      # Ingestion Lambda
      #################################################

      IngestFromYouTubeAPI = {

        Type = "Task"

        Resource = "arn:aws:states:::lambda:invoke"

        Parameters = {
          FunctionName = format("arn:aws:lambda:%s:%s:function:%s-youtube-ingestion-dev", local.region, local.account_id, local.name_prefix)

          Payload = {
            triggered_by = "step_functions"

            "execution_id.$": "$$.Execution.Id"
          }
        }

        ResultPath = "$.ingestion_result"

        Retry = [
          {
            ErrorEquals = [
              "Lambda.ServiceException",
              "Lambda.TooManyRequestsException"
            ]

            IntervalSeconds = 30

            MaxAttempts = 3

            BackoffRate = 2
          }
        ]

        Catch = [
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyIngestionFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "WaitForS3Consistency"
      }

      #################################################
      # Wait State — S3 read-after-write consistency buffer
      #################################################

      WaitForS3Consistency = {
        Type = "Wait"

        Seconds = 10

        Next = "StartBronzeCrawler"
      }

      #################################################
      # Glue Crawler
      #
      # Discovers new Bronze partitions/schema before the
      # Bronze→Silver Glue job reads via the Data Catalog.
      # Step Functions has no native ".sync" integration for
      # crawlers, so this uses the standard
      # start -> wait -> poll -> choice loop pattern.
      #################################################

      StartBronzeCrawler = {

        Type = "Task"

        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"

        Parameters = {
          Name = "${local.name_prefix}-bronze-crawler-dev"
        }

        ResultPath = null

        Retry = [
          {
            ErrorEquals = [
              "Glue.ThrottlingException"
            ]

            IntervalSeconds = 10

            MaxAttempts = 3

            BackoffRate = 2
          }
        ]

        Catch = [
          {
            # Another execution's crawl is still in flight — that's fine,
            # just start polling instead of failing the pipeline.
            ErrorEquals = [
              "Glue.CrawlerRunningException"
            ]

            Next = "WaitForCrawler"

            ResultPath = null
          },
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyTransformFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "WaitForCrawler"
      }

      WaitForCrawler = {
        Type = "Wait"

        Seconds = 20

        Next = "GetCrawlerStatus"
      }

      GetCrawlerStatus = {

        Type = "Task"

        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"

        Parameters = {
          Name = "${local.name_prefix}-bronze-crawler-dev"
        }

        ResultPath = "$.crawler_status"

        Catch = [
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyTransformFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "EvaluateCrawlerStatus"
      }

      EvaluateCrawlerStatus = {

        Type = "Choice"

        Choices = [
          {
            Variable = "$.crawler_status.Crawler.State"

            StringEquals = "READY"

            Next = "ProcessInParallel"
          }
        ]

        # Still RUNNING or STOPPING — keep polling.
        Default = "WaitForCrawler"
      }

      #################################################
      # Parallel Processing
      #################################################

      ProcessInParallel = {

        Type = "Parallel"

        Branches = [

          #################################################
          # Reference Transform Branch
          #################################################

          {
            StartAt = "TransformReferenceData"

            States = {

              TransformReferenceData = {

                Type = "Task"

                Resource = "arn:aws:states:::lambda:invoke"

                Parameters = {
                  FunctionName = format("arn:aws:lambda:%s:%s:function:%s-json-to-parquet-dev", local.region, local.account_id, local.name_prefix)

                  Payload = {
                    triggered_by = "step_functions"

                    "date_partition.$" = "$.ingestion_result.Payload.date_partition"
                  }
                }

                ResultPath = "$.reference_result"

                Retry = [
                  {
                    ErrorEquals = [
                      "States.ALL"
                    ]

                    IntervalSeconds = 15

                    MaxAttempts = 2

                    BackoffRate = 2
                  }
                ]

                End = true
              }
            }
          },

          #################################################
          # Bronze To Silver Glue Job Branch
          #################################################

          {
            StartAt = "RunBronzeToSilverGlueJob"

            States = {

              RunBronzeToSilverGlueJob = {

                Type = "Task"

                Resource = "arn:aws:states:::glue:startJobRun.sync"

                Parameters = {

                  JobName = "bronze_to_silver_statistics"

                  Arguments = {

                    "--bronze_database" = "yt_pipeline_bronze_dev"

                    "--bronze_table" = "raw_statistics"

                    "--silver_bucket" = format("%s-silver-%s", local.name_prefix, local.account_id)

                    "--silver_database" = "yt_pipeline_silver_dev"

                    "--silver_table" = "clean_statistics"

                    "--silver_path" = "youtube/clean_statistics/"
                  }
                }

                ResultPath = "$.glue_bronze_silver_result"

                Retry = [
                  {
                    ErrorEquals = [
                      "States.ALL"
                    ]

                    IntervalSeconds = 60

                    MaxAttempts = 2

                    BackoffRate = 2
                  }
                ]

                End = true
              }
            }
          }
        ]

        ResultPath = "$.parallel_results"

        Catch = [
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyTransformFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "RunDataQualityChecks"
      }

      #################################################
      # Data Quality Lambda
      #################################################

      RunDataQualityChecks = {

        Type = "Task"

        Resource = "arn:aws:states:::lambda:invoke"

        Parameters = {
          FunctionName = format("arn:aws:lambda:%s:%s:function:%s-data-quality-check", local.region, local.account_id, local.name_prefix)

          Payload = {
            layer = "silver"

            database = "yt_pipeline_silver_dev"

            tables = [
              "clean_statistics"
            ]
          }
        }

        ResultPath = "$.dq_result"

        Catch = [
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyDQFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "EvaluateDataQuality"
      }

      #################################################
      # Quality Decision
      #################################################

      EvaluateDataQuality = {

        Type = "Choice"

        Choices = [
          {
            Variable = "$.dq_result.Payload.quality_passed"

            BooleanEquals = true

            Next = "RunSilverToGoldGlueJob"
          }
        ]

        Default = "NotifyDQFailure"
      }

      #################################################
      # Silver To Gold Glue Job
      #################################################

      RunSilverToGoldGlueJob = {

        Type = "Task"

        Resource = "arn:aws:states:::glue:startJobRun.sync"

        Parameters = {

          JobName = "silver_to_gold_analytics"

          Arguments = {

            "--silver_database" = "yt_pipeline_silver_dev"

            "--gold_bucket" = format("%s-gold-%s", local.name_prefix, local.account_id)

            "--gold_database" = "yt_pipeline_gold_dev"
          }
        }

        ResultPath = "$.glue_gold_result"

        Catch = [
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyTransformFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "RunTrendIntelligenceGlueJob"
      }

      #################################################
      # Trend Intelligence Glue Job
      #
      # Runs after silver_to_gold_analytics and reads its
      # category_analytics/channel_analytics outputs from the
      # Glue Catalog to build gold_video_trends and
      # gold_trend_opportunities (velocity, persistence, trend
      # scoring, cross-market expansion — see
      # docs/trend_intelligence.md).
      #################################################

      RunTrendIntelligenceGlueJob = {

        Type = "Task"

        Resource = "arn:aws:states:::glue:startJobRun.sync"

        Parameters = {

          JobName = "trend_intelligence"

          Arguments = {

            "--silver_database" = "yt_pipeline_silver_dev"

            "--gold_bucket" = format("%s-gold-%s", local.name_prefix, local.account_id)

            "--gold_database" = "yt_pipeline_gold_dev"
          }
        }

        ResultPath = "$.glue_trend_intelligence_result"

        Catch = [
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyTransformFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "RunGoldDataQualityChecks"
      }

      #################################################
      # Gold Layer Quality Gate
      #
      # Mirrors the Silver-layer RunDataQualityChecks /
      # EvaluateDataQuality pattern above, but checks the
      # scored/derived Gold tables (uniqueness of the
      # video/opportunity grain, and that every 0-100 score
      # in gold_trend_opportunities actually stayed in
      # range — see check_uniqueness / check_metric_sanity
      # in data_quality_check/lambda_function.py). A bad
      # trend-scoring bug is caught here instead of reaching
      # a dashboard.
      #################################################

      RunGoldDataQualityChecks = {

        Type = "Task"

        Resource = "arn:aws:states:::lambda:invoke"

        Parameters = {
          FunctionName = format("arn:aws:lambda:%s:%s:function:%s-data-quality-check", local.region, local.account_id, local.name_prefix)

          Payload = {
            layer = "gold"

            database = "yt_pipeline_gold_dev"

            tables = [
              "video_trends",
              "trend_opportunities"
            ]
          }
        }

        ResultPath = "$.gold_dq_result"

        Catch = [
          {
            ErrorEquals = [
              "States.ALL"
            ]

            Next = "NotifyDQFailure"

            ResultPath = "$.error"
          }
        ]

        Next = "EvaluateGoldDataQuality"
      }

      EvaluateGoldDataQuality = {

        Type = "Choice"

        Choices = [
          {
            Variable = "$.gold_dq_result.Payload.quality_passed"

            BooleanEquals = true

            Next = "NotifySuccess"
          }
        ]

        Default = "NotifyDQFailure"
      }

      #################################################
      # Success Notification
      #################################################

      NotifySuccess = {

        Type = "Task"

        Resource = "arn:aws:states:::sns:publish"

        Parameters = {
          TopicArn = format("arn:aws:sns:%s:%s:%s-alerts-dev", local.region, local.account_id, local.name_prefix)

          Subject = "[YT Pipeline] Pipeline completed successfully"

          "Message.$" = "States.Format('Pipeline run {} completed successfully', $.Execution.Id)"
        }

        End = true
      }

      #################################################
      # Failure Notifications
      #
      # Each of these now actually publishes to SNS before
      # terminating the execution — previously these were bare
      # Fail states, so infrastructure-level failures (Glue job
      # failures, Lambda invoke errors, crawler errors) never
      # notified anyone at the state-machine level.
      #################################################

      NotifyIngestionFailure = {

        Type = "Task"

        Resource = "arn:aws:states:::sns:publish"

        Parameters = {
          TopicArn = format("arn:aws:sns:%s:%s:%s-alerts-dev", local.region, local.account_id, local.name_prefix)

          Subject = "[YT Pipeline] Ingestion stage failed"

          "Message.$" = "States.Format('Pipeline run {} failed during ingestion. Error: {}', $.Execution.Id, States.JsonToString($.error))"
        }

        Next = "PipelineFailed"
      }

      NotifyTransformFailure = {

        Type = "Task"

        Resource = "arn:aws:states:::sns:publish"

        Parameters = {
          TopicArn = format("arn:aws:sns:%s:%s:%s-alerts-dev", local.region, local.account_id, local.name_prefix)

          Subject = "[YT Pipeline] Transform stage failed"

          "Message.$" = "States.Format('Pipeline run {} failed during crawl/transform. Error: {}', $.Execution.Id, States.JsonToString($.error))"
        }

        Next = "PipelineFailed"
      }

      NotifyDQFailure = {

        Type = "Task"

        Resource = "arn:aws:states:::sns:publish"

        Parameters = {
          TopicArn = format("arn:aws:sns:%s:%s:%s-alerts-dev", local.region, local.account_id, local.name_prefix)

          Subject = "[YT Pipeline] Data quality checks failed"

          "Message.$" = "States.Format('Pipeline run {} failed data quality checks. See the data-quality-check Lambda logs for the specific failing checks.', $.Execution.Id)"
        }

        Next = "PipelineFailed"
      }

      PipelineFailed = {
        Type = "Fail"

        Error = "PipelineExecutionFailed"

        Cause = "See the SNS alert and Step Functions execution history for details."
      }
    }
  })

  tags = {
    Name        = "${local.name_prefix}-orchestration-dev"
    Environment = "dev"
    Project     = local.name_prefix
  }
}

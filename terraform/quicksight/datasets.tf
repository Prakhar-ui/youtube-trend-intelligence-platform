#################################################
# QuickSight Datasets
#
# One dataset per Gold table, DIRECT_QUERY (not SPICE) so dashboards always
# reflect the latest pipeline run without a separate SPICE refresh schedule
# to manage. Column lists below are a curated subset -- the columns actually
# used across sql/*.sql -- not an exhaustive mirror of every Gold column;
# see docs/data_model.md for the full schema of each table if you need to
# add more fields in the QuickSight console later.
#################################################

resource "aws_quicksight_data_set" "video_trends" {
  data_set_id    = "yt-gold-video-trends"
  name           = "Gold - Video Trends"
  aws_account_id = local.account_id
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "video_trends_table"
    relational_table {
      data_source_arn = aws_quicksight_data_source.gold_athena.arn
      schema          = local.gold_db
      name            = "video_trends"

      input_columns {
        name = "video_id"
        type = "STRING"
      }
      input_columns {
        name = "region"
        type = "STRING"
      }
      input_columns {
        name = "snapshot_date"
        type = "DATETIME"
      }
      input_columns {
        name = "title"
        type = "STRING"
      }
      input_columns {
        name = "channel_title"
        type = "STRING"
      }
      input_columns {
        name = "channel_id"
        type = "STRING"
      }
      input_columns {
        name = "category_id"
        type = "INTEGER"
      }
      input_columns {
        name = "views"
        type = "INTEGER"
      }
      input_columns {
        name = "likes"
        type = "INTEGER"
      }
      input_columns {
        name = "comment_count"
        type = "INTEGER"
      }
      input_columns {
        name = "previous_views"
        type = "INTEGER"
      }
      input_columns {
        name = "views_delta"
        type = "INTEGER"
      }
      input_columns {
        name = "view_growth_rate"
        type = "DECIMAL"
      }
      input_columns {
        name = "is_new_entrant"
        type = "BOOLEAN"
      }
      input_columns {
        name = "rank_in_region"
        type = "INTEGER"
      }
      input_columns {
        name = "rank_change"
        type = "INTEGER"
      }
      input_columns {
        name = "trending_days_to_date"
        type = "INTEGER"
      }
      input_columns {
        name = "persistence_score"
        type = "DECIMAL"
      }
      input_columns {
        name = "trend_stage"
        type = "STRING"
      }
    }
  }
}

resource "aws_quicksight_data_set" "category_trends" {
  data_set_id    = "yt-gold-category-trends"
  name           = "Gold - Category Trends"
  aws_account_id = local.account_id
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "category_trends_table"
    relational_table {
      data_source_arn = aws_quicksight_data_source.gold_athena.arn
      schema          = local.gold_db
      name            = "category_analytics"

      input_columns {
        name = "category_id"
        type = "INTEGER"
      }
      input_columns {
        name = "category_name"
        type = "STRING"
      }
      input_columns {
        name = "region"
        type = "STRING"
      }
      input_columns {
        name = "snapshot_date"
        type = "DATETIME"
      }
      input_columns {
        name = "video_count"
        type = "INTEGER"
      }
      input_columns {
        name = "total_views"
        type = "INTEGER"
      }
      input_columns {
        name = "avg_engagement_rate"
        type = "DECIMAL"
      }
      input_columns {
        name = "unique_channels"
        type = "INTEGER"
      }
      input_columns {
        name = "view_share_pct"
        type = "DECIMAL"
      }
      input_columns {
        name = "category_growth_pct"
        type = "DECIMAL"
      }
      input_columns {
        name = "category_momentum"
        type = "DECIMAL"
      }
      input_columns {
        name = "category_rank"
        type = "INTEGER"
      }
      input_columns {
        name = "rank_change"
        type = "INTEGER"
      }
    }
  }
}

resource "aws_quicksight_data_set" "channel_performance" {
  data_set_id    = "yt-gold-channel-performance"
  name           = "Gold - Channel Performance"
  aws_account_id = local.account_id
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "channel_performance_table"
    relational_table {
      data_source_arn = aws_quicksight_data_source.gold_athena.arn
      schema          = local.gold_db
      # NOTE: table name is still "channel_analytics" in the Glue Catalog --
      # kept for continuity even though its grain/columns were redesigned.
      # See docs/decisions.md.
      name = "channel_analytics"

      input_columns {
        name = "channel_id"
        type = "STRING"
      }
      input_columns {
        name = "channel_title"
        type = "STRING"
      }
      input_columns {
        name = "region"
        type = "STRING"
      }
      input_columns {
        name = "snapshot_date"
        type = "DATETIME"
      }
      input_columns {
        name = "trending_video_count_to_date"
        type = "INTEGER"
      }
      input_columns {
        name = "total_views_to_date"
        type = "INTEGER"
      }
      input_columns {
        name = "avg_views_per_video_to_date"
        type = "DECIMAL"
      }
      input_columns {
        name = "avg_engagement_rate_to_date"
        type = "DECIMAL"
      }
      input_columns {
        name = "peak_views_to_date"
        type = "INTEGER"
      }
      input_columns {
        name = "days_active_to_date"
        type = "INTEGER"
      }
      input_columns {
        name = "trending_frequency"
        type = "DECIMAL"
      }
      input_columns {
        name = "rank_in_region"
        type = "INTEGER"
      }
      input_columns {
        name = "markets_present"
        type = "INTEGER"
      }
      input_columns {
        name = "trend_score"
        type = "DECIMAL"
      }
    }
  }
}

resource "aws_quicksight_data_set" "trend_opportunities" {
  data_set_id    = "yt-gold-trend-opportunities"
  name           = "Gold - Trend Opportunities"
  aws_account_id = local.account_id
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "trend_opportunities_table"
    relational_table {
      data_source_arn = aws_quicksight_data_source.gold_athena.arn
      schema          = local.gold_db
      name            = "trend_opportunities"

      input_columns {
        name = "snapshot_date"
        type = "DATETIME"
      }
      input_columns {
        name = "region"
        type = "STRING"
      }
      input_columns {
        name = "scope"
        type = "STRING"
      }
      input_columns {
        name = "entity_id"
        type = "STRING"
      }
      input_columns {
        name = "entity_name"
        type = "STRING"
      }
      input_columns {
        name = "trend_type"
        type = "STRING"
      }
      input_columns {
        name = "trend_score"
        type = "DECIMAL"
      }
      input_columns {
        name = "velocity_score"
        type = "DECIMAL"
      }
      input_columns {
        name = "engagement_score"
        type = "DECIMAL"
      }
      input_columns {
        name = "persistence_score"
        type = "DECIMAL"
      }
      input_columns {
        name = "market_expansion_score"
        type = "DECIMAL"
      }
      input_columns {
        name = "rank"
        type = "INTEGER"
      }
      input_columns {
        name = "evidence"
        type = "STRING"
      }
    }
  }
}

resource "aws_quicksight_data_set" "trending_overview" {
  data_set_id    = "yt-gold-trending-overview"
  name           = "Gold - Trending Overview (Daily)"
  aws_account_id = local.account_id
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "trending_overview_table"
    relational_table {
      data_source_arn = aws_quicksight_data_source.gold_athena.arn
      schema          = local.gold_db
      name            = "trending_analytics"

      input_columns {
        name = "region"
        type = "STRING"
      }
      input_columns {
        name = "trending_date_parsed"
        type = "DATETIME"
      }
      input_columns {
        name = "total_videos"
        type = "INTEGER"
      }
      input_columns {
        name = "total_views"
        type = "INTEGER"
      }
      input_columns {
        name = "avg_views_per_video"
        type = "DECIMAL"
      }
      input_columns {
        name = "avg_engagement_rate"
        type = "DECIMAL"
      }
      input_columns {
        name = "unique_channels"
        type = "INTEGER"
      }
      input_columns {
        name = "unique_categories"
        type = "INTEGER"
      }
    }
  }
}

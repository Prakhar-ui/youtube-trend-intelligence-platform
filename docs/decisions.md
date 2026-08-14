# Architecture & Data Decisions

A running log of decisions made while building the Trend Intelligence layer,
and why, so the reasoning isn't lost to a comment fragment somewhere.

## Markets: kept AU, did not switch to MX

The pipeline's deployed `--regions` list (`us,gb,in,ca,au,de,fr,jp,kr,ru`)
includes Australia rather than Mexico. An earlier draft of this project's
brief listed Mexico as the 10th market; we kept the platform's actual
deployed history (Australia) rather than silently switching, since switching
would orphan AU's accumulated trending history and mean MX has none. If MX
market coverage is wanted later, it's an additive change (add it to
`--regions` and `YOUTUBE_REGIONS`), not a replacement.

## Consolidated 7 suggested Gold tables into 4

Rather than building `gold_video_trends`, `gold_trend_velocity`,
`gold_category_trends`, `gold_channel_performance`, `gold_market_trends`,
`gold_cross_market_trends`, and `gold_trend_opportunities` as seven separate
physical tables:

- **`gold_trend_velocity` folded into `gold_video_trends`.** Velocity columns
  (`previous_views`, `view_growth_rate`, `rank_change`, ...) are computed at
  the exact same grain (video × region × snapshot_date) as the rest of the
  video table — a separate table would just be the same rows with fewer
  columns, requiring a join to reconstruct anything useful.
- **`gold_market_trends` folded into `gold_category_trends`.** Both were
  specified at the same grain (category × region × snapshot_date). One table,
  not two identical-grain tables with different names.
- **`gold_cross_market_trends` is not materialized at all** — it's computed
  in-memory (`compute_cross_market_expansion()`) and fed directly into
  `gold_trend_opportunities`. See `docs/trend_intelligence.md` §4 for the
  reasoning: it's a straightforward re-aggregation of `gold_category_trends`,
  and the "growing in India but flat in the US" style comparison is a
  two-row lookup across `gold_category_trends`, not something that benefits
  from precomputing every pairwise market combination.

Net result: 4 tables (`gold_video_trends`, `gold_category_trends`,
`gold_channel_performance`, `gold_trend_opportunities`) instead of 7, with no
loss of the analytical capability described in the original spec.

## channel_id vs. channel_title as the channel entity key

The YouTube API provides a stable `channel_id`; the classic Kaggle CSV
reference/demo dataset only has `channel_title` (channel names can collide or
be renamed, so this is a real but accepted limitation of demo-format data).
`channel_key = COALESCE(channel_id, channel_title)` is used as the entity key
throughout channel intelligence so both data sources work, with the
understanding that Kaggle-sourced rows are less reliable for channel identity
than live API rows.

## Bronze→Silver region predicate bug

The original `bronze_to_silver_statistics.py` hardcoded
`region in ('ca','gb','us','in')` — only 4 of the 10 ingested markets ever
reached Silver/Gold, silently. Fixed to read the market list from a new
`--regions` Glue job parameter (set in `bronze_to_silver_statistics_job.tf`,
kept in sync with `YOUTUBE_REGIONS` in the Lambda module — see the NOTE in
that Terraform file). This is a pre-existing bug fix, not a new design
choice, but it's significant enough to call out here since it silently
suppressed 60% of markets from ever reaching analytics.

## No fabricated growth rates on a zero or missing baseline

`view_growth_rate`, `category_growth_pct`, and related fields are `NULL`
— never `0%` or a fabricated infinite value — when there's no previous
snapshot to compare against, or when the previous snapshot's value was zero.
A `NULL` growth rate correctly says "we don't know yet"; a `0%` would falsely
say "this held steady," and an infinite value isn't meaningful for ranking or
display. Rows are flagged (`is_new_entrant`) rather than given a synthetic
number.

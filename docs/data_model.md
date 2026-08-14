# Data Model

## Bronze

`bronze/youtube/raw_statistics/region={region}/date={date}/hour={hour}/{ingestion_id}.json`

Immutable YouTube API responses (or `bronze/youtube/raw_statistics_reference_data/...`
for `videoCategories`), one file per ingestion Lambda invocation per region.
Never overwritten — this is what makes historical comparison in Gold
possible at all. Lifecycle: expires at 90 days (raw payloads aren't needed
once Silver/Gold have derived what they need from them).

## Silver — `clean_statistics`

Grain: one row per video observation (video_id × region × trending_date,
deduplicated keeping the latest-processed row on conflict). Partitioned by
`region` in S3; `trending_date_parsed` retained as a column for date-level
filtering/joins.

| Column | Type | Notes |
|---|---|---|
| `video_id` | string | |
| `trending_date` | string | raw `YY.DD.MM` string (legacy Kaggle format) |
| `trending_date_parsed` | date | parsed, used for all date logic downstream |
| `title` | string | |
| `channel_title` | string | |
| `channel_id` | string | **null for Kaggle-format rows** — API only |
| `category_id` | long | |
| `publish_time` | string | ISO-8601 |
| `tags` | string | |
| `views`, `likes`, `dislikes`, `comment_count` | long | `dislikes` always 0 for API rows (YouTube deprecated public dislike counts) |
| `thumbnail_link` | string | |
| `comments_disabled`, `ratings_disabled`, `video_error_or_removed` | boolean | |
| `description` | string | |
| `duration` | string | ISO-8601 (`PT4M13S`), **null for Kaggle-format rows** |
| `definition` | string | `hd`/`sd`, **null for Kaggle-format rows** |
| `caption` | string | **null for Kaggle-format rows** |
| `region` | string | lowercased |
| `like_ratio` | double | legacy: `(likes / views) * 100`, percentage |
| `engagement_rate` | double | legacy: `(likes + dislikes + comment_count) / views * 100`, percentage |
| `like_rate` | double | spec-defined: `likes / views`, **fraction** (0–1) |
| `comment_rate` | double | spec-defined: `comment_count / views`, **fraction** (0–1) |
| `duration_seconds` | int | parsed from `duration`; null if `duration` is null |
| `video_age_hours` | double | hours between `publish_time` and `trending_date_parsed`; null if either is unparseable |
| `views_per_hour` | double | `views / max(video_age_hours, 1)`; null if `video_age_hours` is null |
| `_processed_at`, `_job_name` | timestamp, string | pipeline lineage metadata |

The legacy percentage-based `like_ratio`/`engagement_rate` are kept
alongside the new fraction-based `like_rate`/`comment_rate` for backward
compatibility rather than redefined in place — see `docs/decisions.md` if
this becomes confusing enough to warrant a breaking rename later.

## Gold

### `gold_video_trends`

Grain: video_id × region × snapshot_date. Full column list and formulas in
`docs/trend_intelligence.md` §2. Partitioned by `region`.

Key columns beyond the Silver passthrough: `previous_views`,
`views_delta`, `view_growth_rate`, `is_new_entrant`, `engagement_change`,
`rank_in_region`, `rank_change`, `trending_days_to_date`,
`first_trending_date`, `persistence_score`, `trend_stage`.

### `gold_category_trends`

Grain: category_id × region × snapshot_date. Partitioned by `region`.

`category_name`, `category_id`, `region`, `snapshot_date`, `video_count`,
`total_views`, `total_likes`, `total_comments`, `avg_engagement_rate`,
`unique_channels`, `view_share_pct`, `previous_total_views`,
`category_growth_pct`, `category_momentum`, `category_rank`,
`previous_rank`, `rank_change`.

### `gold_channel_performance`

Grain: channel_id × region × snapshot_date, **cumulative-to-date**. Formulas
in `docs/trend_intelligence.md` §5. Table name in the Glue Catalog is
`channel_analytics` (kept from the original job for continuity; the *content*
is the new time-aware design — see `docs/decisions.md`).

`channel_id`, `channel_title`, `region`, `snapshot_date`,
`trending_video_count_to_date`, `total_views_to_date`, `total_likes_to_date`,
`total_comments_to_date`, `avg_views_per_video_to_date`,
`avg_engagement_rate_to_date`, `peak_views_to_date`, `days_active_to_date`,
`first_trending_date`, `trending_frequency`, `rank_in_region`,
`markets_present`, `views_percentile`, `trend_score`.

**Known gap:** `categories_present` is not yet implemented (see
`docs/trend_intelligence.md` §7).

### `gold_trend_opportunities`

Grain: one opportunity × scope × region × snapshot_date. Not partitioned by
`region` (queries typically want "top opportunities across all markets on a
date" as often as per-region) — partitioned by `scope` instead
(`VIDEO`/`CATEGORY`/`CHANNEL`).

`snapshot_date`, `region`, `scope`, `entity_id`, `entity_name`, `trend_type`,
`trend_score`, `velocity_score`, `engagement_score`, `persistence_score`,
`market_expansion_score`, `rank`, `evidence`.

`trend_type` values: `EMERGING` / `SUSTAINING` (video), `CROSS_MARKET_EXPANSION`
/ `RISING_CATEGORY` / `DECLINING_CATEGORY` (category), `HIGH_PERFORMING_CHANNEL`
(channel).

### `trending_analytics` (unchanged)

Grain: region × snapshot_date. Daily rollup — total videos/views/likes/
comments, averages, unique channels/categories. Unchanged from the original
implementation; still useful as the simplest "what happened today" table.

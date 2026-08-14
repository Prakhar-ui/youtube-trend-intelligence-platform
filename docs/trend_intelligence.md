# Trend Intelligence Methodology

This document is the single source of truth for every formula, threshold, and
scoring rule used in the Trend Intelligence Gold layer
(`terraform/glue/scripts/trend_intelligence.py` and the extensions to
`silver_to_gold_analytics.py`). Every rule here is rule-based and auditable
from the row's own columns — none of it is a statistical or ML model. That's
a deliberate choice: a business user asking "why did this rank #1?" should be
able to get a one-line answer from the data itself.

## 1. Grain

| Table | Grain |
|---|---|
| `gold_video_trends` | video_id × region × snapshot_date |
| `gold_category_trends` | category_id × region × snapshot_date |
| `gold_channel_performance` | channel_id × region × snapshot_date (cumulative-to-date) |
| `gold_trend_opportunities` | one opportunity × scope × region × snapshot_date |

Every velocity/growth/rank-change comparison is against the **same entity's
own previous snapshot** — never across regions, never across different
entities. Cross-region comparison is a distinct, separate computation (see
§4) so it's never silently mixed into a single-market growth number.

## 2. Video Trend Velocity (`gold_video_trends`)

```
view_growth_rate = (views - previous_views) / previous_views
```

- `previous_views` comes from `LAG()` over `(video_id, region)` ordered by
  `snapshot_date`.
- **Null, not fabricated**, when there is no previous snapshot (the video's
  first-ever trending appearance — flagged `is_new_entrant = true`) or when
  `previous_views == 0` (a zero baseline can't support a meaningful ratio).
- `views_delta`, `likes_delta`, `comments_delta` are simple differences; also
  null on the first observation.
- `engagement_change` = `(like_rate + comment_rate)` this snapshot minus the
  same sum on the previous snapshot.
- `rank_in_region` = `DENSE_RANK()` by `views` within `(region, snapshot_date)`.
  `rank_change = previous_rank - rank_in_region`, so **positive means the
  video moved up** (a smaller/better rank number).

### Persistence

Because the grain guarantees exactly one row per `(video_id, region,
snapshot_date)`, a running `ROW_NUMBER()` over `(video_id, region)` ordered by
date **is** a correct cumulative "trending days so far" count —
`trending_days_to_date`.

```
persistence_score = min(trending_days_to_date / 7, 1.0) * 100
```

`PERSISTENCE_CAP_DAYS = 7`: a video trending for a full week is treated as
"fully persistent" for scoring purposes. This is a tunable constant, not a
claim that trends stop mattering after 7 days.

### Trend stage classification

Evaluated top to bottom, first match wins:

1. `is_new_entrant` → **EMERGING** (a video's first trending appearance is
   itself a signal worth surfacing)
2. `view_growth_rate < -0.05` (`FADING_GROWTH_THRESHOLD`) → **FADING**
3. `trending_days_to_date <= 3` (`EMERGING_MAX_DAYS`) AND
   `view_growth_rate > 0.20` (`EMERGING_GROWTH_THRESHOLD`) → **EMERGING**
4. `trending_days_to_date > 7` (`ESTABLISHED_MIN_DAYS`) AND growth roughly
   flat (between -5% and +20%) → **ESTABLISHED**
5. `view_growth_rate > 0` → **SUSTAINING**
6. otherwise → **ESTABLISHED** (flat/slightly-negative, not yet fading, too
   few days to call "established" on persistence alone — the safest default)

All five thresholds are named constants at the top of `trend_intelligence.py`
so they can be tuned in one place without touching the classification logic.

## 3. Category Intelligence (`gold_category_trends`)

Extends the original `category_analytics` table (unchanged: `video_count`,
`total_views`, `view_share_pct`, `avg_engagement_rate`, `unique_channels`)
with:

```
category_growth_pct = (total_views - previous_total_views) / previous_total_views * 100
```

Same null-on-no-prior / null-on-zero-baseline rule as video growth.

```
category_momentum = 3-snapshot rolling average of category_growth_pct
```

A single day's growth spike doesn't make a trend — averaging the last 3
snapshots (current + 2 prior) smooths day-to-day noise into something closer
to a real momentum signal.

`category_rank` = `DENSE_RANK()` by `total_views` within `(region,
snapshot_date)`; `rank_change` follows the same sign convention as videos.

## 4. Cross-Market Expansion

**Deliberately not a standalone `gold_cross_market_trends` /
`gold_market_trends` table** — see `docs/decisions.md`. Computed in-memory in
`trend_intelligence.py::compute_cross_market_expansion()` by rolling
`gold_category_trends` up from `(category, region, date)` to `(category,
date)` across all regions:

```
market_count          = COUNT(DISTINCT region) trending this category on this date
market_count_delta     = market_count - previous market_count (same category, prior date)
market_expansion_score = market_count / TOTAL_MARKETS_MONITORED * 100
```

`TOTAL_MARKETS_MONITORED = 10` — kept in sync with the `--regions` argument
in `bronze_to_silver_statistics_job.tf`. If markets are added/removed, update
both.

This is the "is category X spreading from India into other markets" signal.
A per-region comparison like "growing in India, flat in the US" is answered
by comparing rows of `gold_category_trends` across `region` for the same
`category_id` and `snapshot_date` — a two-row lookup, not a precomputed
column, since precomputing every pairwise market comparison would be
combinatorial and mostly unused.

## 5. Channel Intelligence (`gold_channel_performance`)

Redesigned from a single lifetime aggregate (the original `channel_analytics`
had no time dimension at all) into cumulative-to-date stats at `(channel,
region, snapshot_date)` grain, so a channel's trajectory can be compared
date to date. See `docs/data_model.md` for the full column list.

```
trending_frequency = days_active_to_date / (DATEDIFF(snapshot_date, first_trending_date) + 1)
```

The `+ 1` matters: `DATEDIFF` is a difference, not a count of calendar days
in the span. Without it, a channel trending on two *consecutive* days would
score `2 / 1 = 2.0` — over 100%, which is nonsensical for a frequency. This
was caught and fixed during development (see the regression test
`test_build_channel_analytics_trending_frequency_capped_at_one`).

`trending_video_count_to_date` (a running **distinct** video count) uses the
standard "first occurrence" SQL pattern: flag each video's earliest
appearance for that channel+region, then take a cumulative sum of those
flags — Spark window functions don't support a running `COUNT(DISTINCT ...)`
directly.

`markets_present` (how many distinct regions a channel has appeared in,
cumulative to date, **across all regions** — not just the current row's
region) is computed separately in `add_markets_present()` and joined back on
`(channel_id, snapshot_date)`, since it's inherently cross-region while the
rest of the table is grouped by region.

```
channel trend_score = views_percentile * 0.7 + (trending_frequency * 100) * 0.3
```

`views_percentile` = percentile rank of `total_views_to_date` within the same
`(region, snapshot_date)` — see §6 for why percentile rank over min-max.

## 6. Trend Opportunities (`gold_trend_opportunities`)

The dashboard-ready, business-facing table. Unions three scored/ranked
subsets:

- **Video opportunities**: `trend_stage IN (EMERGING, SUSTAINING)`, top 20 per
  `(region, snapshot_date)` by `trend_score`.
- **Category opportunities**: top 10 per `(region, snapshot_date)`, typed as
  `CROSS_MARKET_EXPANSION` (market count grew), `RISING_CATEGORY` (growing but
  not expanding into new markets), or `DECLINING_CATEGORY`.
- **Channel opportunities**: top 10 per `(region, snapshot_date)` by channel
  `trend_score`.

### Component scores

All normalized 0–100 using **percentile rank within the same `(region,
snapshot_date)` group**, not min-max normalization. Percentile rank means one
extreme outlier can't compress everyone else's score toward 0 or 100 — a
video with 50x the growth of everything else still just takes the top
percentile slot, it doesn't distort the scale for the other 19.

| Score | Formula |
|---|---|
| `velocity_score` | percentile rank of `view_growth_rate` (video) / `category_growth_pct` (category) |
| `engagement_score` | percentile rank of engagement fraction / rate |
| `persistence_score` | video's own `persistence_score` (§2); null for category/channel rows |
| `market_expansion_score` | `market_expansion_score` from §4; `0` for video/channel rows (doesn't apply at that scope) |

### Composite trend_score

```
video    trend_score = velocity_score * 0.5 + engagement_score * 0.3 + persistence_score * 0.2
category trend_score = velocity_score * 0.4 + engagement_score * 0.25 + market_expansion_score * 0.35
channel  trend_score = (see §5)
```

Weights are named literals in `trend_intelligence.py`, chosen to reflect that
velocity is the primary "is this actually taking off" signal, engagement is a
quality check against pure view-count noise, and — for categories only —
market expansion carries real weight because spreading into new markets is
the platform's specific cross-market differentiator (spec §17).

### Evidence

Every `gold_trend_opportunities` row carries a plain-text `evidence` string
built from its own actual computed numbers (e.g. *"Views +412.0% vs previous
snapshot, trending 2 day(s), engagement 6.10%."*) — not a template filled
with placeholder text. Combined with the four component scores stored
alongside it, the row is self-explanatory without needing to join back to
raw Silver data to answer "why did this rank #1?"

## 7. Known limitations

- `categories_present` (distinct categories a channel has trended in) is not
  yet implemented on `gold_channel_performance` — a real gap, not silently
  dropped: it needs the same first-occurrence running-distinct-count pattern
  used for `trending_video_count_to_date`, applied to `category_id` instead
  of `video_id`. Left for a follow-up pass rather than rushed in.
- All thresholds in §2–§6 are reasonable, documented starting points, not
  values tuned against real trending-video outcomes (this platform doesn't
  yet have enough accumulated history to do that tuning meaningfully). Revisit
  once a few months of daily snapshots have accumulated.

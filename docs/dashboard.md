# Dashboard

## What's Terraform-managed vs. manual

| Layer | Status |
|---|---|
| Athena Gold tables (`video_trends`, `category_analytics`, `channel_analytics`, `trend_opportunities`, `trending_analytics`) | **Automated** — written by the pipeline |
| Dashboard SQL (`sql/*.sql`) | **Automated** — checked into the repo, run directly in Athena or copy-pasted into QuickSight's SQL editor |
| QuickSight data source (`terraform/quicksight/data_source.tf`) | **Automated** via Terraform |
| QuickSight datasets, one per Gold table (`terraform/quicksight/datasets.tf`) | **Automated** via Terraform |
| QuickSight subscription/namespace itself | **Manual, one-time** — AWS doesn't expose first-time QuickSight signup as a Terraform resource |
| QuickSight service role → S3/Athena permissions | **Manual, one-time** — console-only (Manage QuickSight → Security & permissions) |
| The 4 dashboards' actual sheets/visuals/layout | **Manual** — see below for why |

### Why the visual layer is manual, not Terraform

`aws_quicksight_dashboard` can define sheets and visuals inline via a
`definition` block, but that schema is large (per-visual field wells, sort
configuration, conditional formatting, layout — often 100+ lines per chart)
and there's no way to validate it against the live QuickSight API from this
environment (no AWS network access while building this). Hand-authoring four
full dashboards' worth of that schema with no ability to `terraform plan`
against it would risk shipping Terraform that looks plausible but fails at
`apply` in a way that's hard to debug remotely. The data source and datasets
above are ordinary, well-understood resource shapes and are a safe bet;
the dashboard visual layer isn't, so it's documented as a spec instead —
building it in the console against the datasets we've already provisioned is
a matter of minutes per chart, not a redesign.

If you want to eventually manage dashboards as code too, the standard path is
to build sheet 1 by hand in the console, then run
`aws quicksight describe-dashboard-definition` to get real, valid JSON back,
and import *that* into Terraform (`aws_quicksight_dashboard` with
`definition` sourced from the console-authored JSON) rather than writing it
from scratch.

## Dashboard 1 — Executive Trend Overview

*"What is trending right now?"*

**KPI tiles:** markets monitored · videos analyzed · emerging trends · top
category (by views) · highest-velocity video · highest-engagement category

**Charts:**
- Trending volume over time (line, last 30 snapshots) — `total_videos`,
  `total_views` from `trending_analytics`
- Top emerging categories (bar) — `trend_opportunities` filtered to
  `scope = 'CATEGORY'`, `trend_type IN ('RISING_CATEGORY',
  'CROSS_MARKET_EXPANSION')`
- Top emerging videos (bar/table) — `trend_opportunities` filtered to
  `scope = 'VIDEO'`
- Market comparison (bar, by region) — `trending_analytics`

Dataset: `Gold - Video Trends`, `Gold - Category Trends`,
`Gold - Trend Opportunities`, `Gold - Trending Overview (Daily)`.
Queries: `sql/executive_overview.sql`.

## Dashboard 2 — Trend Intelligence

*"Which trends are gaining momentum?"*

**Visuals:**
- Emerging trends table — all `EMERGING`/`RISING_CATEGORY`/
  `CROSS_MARKET_EXPANSION` rows from `trend_opportunities`, most recent
  snapshot, sorted by `trend_score`
- Trend velocity (box plot or bar) — `view_growth_rate` distribution by
  `trend_stage`, from `video_trends`
- Trend persistence (bar) — avg `trending_days_to_date` /
  `persistence_score` by `trend_stage`
- Rising / declining categories (two tables) — `category_analytics` split
  on the sign of `category_growth_pct`
- Trend lifecycle (donut) — video count by `trend_stage`
- Top opportunity categories (bar) — `trend_opportunities`, `scope =
  'CATEGORY'`, ranked by `trend_score`

Dataset: `Gold - Video Trends`, `Gold - Category Trends`,
`Gold - Trend Opportunities`. Queries: `sql/emerging_trends.sql`.

## Dashboard 3 — Market Intelligence

*"Where is the trend happening?"*

**Visuals:**
- Market comparison (bar, by region) — `trending_analytics`
- Category × market heatmap — `category_analytics`, `view_share_pct` by
  `(region, category_name)`
- Market-specific top trends (table, top 5 per region) — `trend_opportunities`
- Cross-market trends (table) — categories with `market_count > 1`,
  aggregated from `category_analytics`
- Global vs. local trends (donut) — categories classified GLOBAL (≥7
  markets) / REGIONAL (2–6) / LOCAL (1)
- Category growth by market (grouped bar) — `category_growth_pct` per
  `(category_name, region)`, the "growing in India but flat in the US" view

Dataset: `Gold - Category Trends`, `Gold - Trending Overview (Daily)`,
`Gold - Trend Opportunities`. Queries: `sql/market_intelligence.sql`.

## Dashboard 4 — Channel & Content Intelligence

*"Who is driving the trends?"*

**Visuals:**
- Top channels (table, per region) — `channel_analytics`, ranked by
  `total_views_to_date`
- Fastest-growing channels (bar) — ranked by `trend_score`
- Highest-engagement channels (bar) — ranked by `avg_engagement_rate_to_date`
- Top videos (table) — `video_trends`, ranked by `views`
- Trending duration distribution (histogram) — `trending_days_to_date`
  value counts
- Multi-market channels (table) — channels with `markets_present > 1`
- Channel/category relationship (table or Sankey) — join of `video_trends`
  and `category_analytics`

Dataset: `Gold - Channel Performance`, `Gold - Video Trends`,
`Gold - Category Trends`. Queries: `sql/channel_intelligence.sql`.

## Deploying the QuickSight layer

```bash
cd terraform/quicksight
terraform init
terraform plan   # review before applying — see the "not yet validated" note below
terraform apply
```

**Not yet validated against live AWS** (no network access while this was
built — see `docs/decisions.md`/session notes): run `terraform plan` first
and check the field names/required-vs-optional arguments against your
installed `hashicorp/aws` provider version before applying. The two most
likely adjustment points, if any, are (a) whether your provider version
requires an explicit `logical_table_map` block alongside `physical_table_map`
on `aws_quicksight_data_set`, and (b) the exact `ATHENA` data source
permission requirements for your account's QuickSight edition.

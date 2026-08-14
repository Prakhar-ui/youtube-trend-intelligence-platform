# Solution Architecture

## Overview

The YouTube Trend Intelligence Platform is an end-to-end serverless pipeline
that ingests daily YouTube trending-video snapshots across 10 markets,
transforms them through a Bronze → Silver → Gold (Medallion) architecture,
validates data quality at two gates, and computes a dedicated Trend
Intelligence layer (velocity, persistence, trend scoring, cross-market
expansion) on top of the accumulated history — see `docs/trend_intelligence.md`
for the methodology and `docs/decisions.md` for why the architecture is
shaped the way it is.

---

## High-Level Architecture

```text
YouTube Data API
        │
        ▼
AWS Lambda (Ingestion)
        │
        ▼
Bronze Layer (Amazon S3) — immutable, never overwritten
        │
        ▼
AWS Step Functions
        │
        ├──────────────┐
        ▼              ▼
AWS Glue        JSON Reference Lambda
(Bronze→Silver)  (category metadata)
        │              │
        └──────┬───────┘
               ▼
Silver Layer (clean_statistics)
               │
               ▼
Data Quality Lambda (Silver gate)
               │
        Pass / Fail
               │
       ┌───────┴────────┐
       ▼                ▼
Glue: Silver→Gold    SNS Alert
(category/channel
 analytics)
       │
       ▼
Glue: Trend Intelligence
(video_trends, trend_opportunities —
 velocity, persistence, cross-market)
       │
       ▼
Data Quality Lambda (Gold gate)
               │
        Pass / Fail
               │
       ┌───────┴────────┐
       ▼                ▼
Glue Catalog         SNS Alert
       │
       ▼
Amazon Athena
       │
       ▼
Amazon QuickSight (data source + datasets)
       │
       ▼
4 Dashboards (docs/dashboard.md)
```

---

## AWS Services

| Service | Purpose |
|----------|---------|
| Lambda | Ingestion, reference-data processing, data quality checks (Silver + Gold gates) |
| Glue | PySpark ETL — Bronze→Silver, Silver→Gold, Trend Intelligence |
| Step Functions | Workflow orchestration, retries, failure notifications |
| S3 | Data lake (Bronze/Silver/Gold) + Athena query results |
| SNS | Failure notifications |
| EventBridge | Daily scheduling |
| Athena | SQL analytics over Gold |
| QuickSight | Dashboard data source + datasets |
| Glue Catalog | Metadata / schema registry |
| CloudWatch | Monitoring, logs, alarms |

---

## Medallion Architecture

### Bronze

Raw, immutable API snapshots, partitioned by region/date/hour. Never
overwritten — this is what makes historical trend comparison possible at all.

### Silver

Schema-validated, deduplicated, one row per video observation. Adds derived
metrics (engagement rates, video age, views-per-hour). See
`docs/data_model.md`.

### Gold

Two layers of Gold datasets:
- **Analytics** (`trending_analytics`, `category_analytics`,
  `channel_analytics`) — daily/cumulative rollups.
- **Trend Intelligence** (`video_trends`, `trend_opportunities`) — velocity,
  persistence, trend-stage classification, cross-market expansion, and a
  composite, explainable trend score. See `docs/trend_intelligence.md`.

---

## Design Principles

- Serverless
- Event-driven
- Fault tolerant (retries, Catch blocks, two data-quality gates, SNS alerts
  at every failure point)
- Infrastructure as Code
- Cost optimized (see the README's Cost Considerations section)
- Modular — each Terraform module is its own state
- Explainable, not black-box — every trend score is a documented, auditable
  formula, not a statistical model

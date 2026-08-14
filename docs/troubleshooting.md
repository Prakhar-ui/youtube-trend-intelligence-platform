# Troubleshooting

## Lambda timeout

Increase timeout in Terraform.

---

## Glue job failed

Check:

- CloudWatch Logs
- IAM permissions
- S3 paths

---

## Athena table empty

Verify:

- Glue crawler
- Glue Catalog
- S3 partitions

---

## Step Functions failed

Check execution history.

---

## Terraform apply failed

Run

```bash
terraform plan
```

Review IAM permissions.

---

## GitHub Actions failed

Check

- AWS credentials
- Terraform version
- Backend configuration

---

## SNS email not received

Confirm email subscription.

---

## Gold data quality gate failed

Check the `RunGoldDataQualityChecks` step's output in the Step Functions
execution history for which specific check failed (`uniqueness`,
`metric_sanity`, etc. — see `docs/trend_intelligence.md` for what each score
should range over). A `uniqueness` failure on `video_trends` usually means
the Trend Intelligence Glue job was re-run over a Silver range it had
already processed; a `metric_sanity` failure on `trend_score`/`velocity_
score`/etc. outside `[0, 100]` means the scoring formula itself has a
regression — check recent changes to `trend_intelligence.py` first.

---

## QuickSight dataset / dashboard shows no data

Check, in order:

1. Has a pipeline run actually completed and written to the Gold table this
   dataset points at? (`SELECT COUNT(*) FROM <table>` in Athena directly.)
2. Has the QuickSight service role been granted S3 + Athena access via
   QuickSight console → Manage QuickSight → Security & permissions? This is
   a manual, one-time step Terraform cannot perform — see `docs/dashboard.md`.
3. Does the dataset's `physical_table_map` column list actually match the
   Gold table's current schema? Glue schema evolution
   (`enableUpdateCatalog`) can add columns the QuickSight dataset definition
   doesn't know about yet — re-apply `terraform/quicksight` after any Gold
   schema change.
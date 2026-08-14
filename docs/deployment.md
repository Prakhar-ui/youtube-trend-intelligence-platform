# Deployment Guide

## Prerequisites

- AWS CLI
- Terraform >= 1.5
- Python 3.12
- Git

---

## Clone Repository

```bash
git clone https://github.com/<username>/aws-serverless-data-platform.git
```

---

## Configure AWS

```bash
aws configure
```

---

## Module Layout

There is no root Terraform configuration. Each folder under `terraform/` is an
independently-stated module (its own `backend.tf`), so every command below is
run from inside a module directory, not the repo root.

Deploy in this order, since downstream modules read upstream ones via
`terraform_remote_state`:

1. **`terraform/bootstrap`** — one-time only. Creates the S3 state bucket and
   DynamoDB lock table every other module's backend points at.
2. **`terraform/s3`**, **`terraform/iam`**, **`terraform/sns`** — no
   dependencies on each other, can be applied in any order.
3. **`terraform/glue`**, **`terraform/lambda`**, **`terraform/step_functions`**
   — each reads `iam`'s remote state for its execution role ARN.
4. **`terraform/eventbridge`** — reads both `iam`'s and `step_functions`'
   remote state.
5. **`terraform/monitoring`** — reads resources created by the modules above.
6. **`terraform/quicksight`** — deploy only after a pipeline run has actually
   populated the Gold tables it points at (`video_trends`, `category_analytics`,
   `channel_analytics`, `trend_opportunities`, `trending_analytics`). Also
   requires a one-time **manual** step that has no Terraform resource:
   QuickSight must already be subscribed/enabled in this AWS account, and its
   service role needs S3 + Athena access granted via QuickSight console →
   Manage QuickSight → Security & permissions. See `docs/dashboard.md`.
7. **`terraform/budget`** — standalone, deploy last.

---

## Initialize Terraform (per module)

```bash
cd terraform/<module>
terraform init
```

---

## Validate

```bash
terraform validate
```

---

## Plan

```bash
terraform plan
```

`terraform/lambda` additionally requires the YouTube API key:

```bash
terraform plan -var="youtube_api_key=<your-key>"
```

---

## Apply

```bash
terraform apply
```

`terraform/lambda` again requires the same `-var` flag:

```bash
terraform apply -var="youtube_api_key=<your-key>"
```

---

## Verify Deployment

Check:

- S3 buckets
- Lambda functions
- Glue Jobs
- Step Functions
- SNS
- EventBridge

---

## Destroy

Tear down in the **reverse** order of deployment (dependents before their
dependencies), from inside each module directory:

```bash
cd terraform/budget         && terraform destroy
cd terraform/quicksight     && terraform destroy
cd terraform/monitoring     && terraform destroy
cd terraform/eventbridge   && terraform destroy
cd terraform/step_functions && terraform destroy
cd terraform/lambda        && terraform destroy -var="youtube_api_key=<your-key>"
cd terraform/glue          && terraform destroy
cd terraform/sns           && terraform destroy
cd terraform/iam           && terraform destroy
cd terraform/s3            && terraform destroy
```

Leave `terraform/bootstrap` in place unless you're permanently done with the
project — destroying it deletes the state bucket every other module depends on.
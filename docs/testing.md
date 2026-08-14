# Testing and CI Quality Gates

This project uses layered automated testing so application code and infrastructure are validated before deployment.

## Python and PySpark

The test suite covers Lambda and Glue/PySpark transformation logic with a minimum **95% line coverage** gate.

```bash
make test
```

The coverage threshold is configured in `pyproject.toml` and `pytest.ini` and applies to:

- `terraform/lambda/scripts`
- `terraform/glue/scripts`

AWS SDK/Wrangler calls are mocked in unit tests, so the unit-test suite does not require an AWS account.

## Terraform

Every Terraform root under `terraform/` is independently initialized with the backend disabled and validated:

```bash
make validate-terraform
```

The validation script also runs:

```bash
terraform fmt -check -recursive terraform/
terraform init -backend=false -input=false
terraform validate
```

This covers all Terraform roots, including `quicksight`.

Backend initialization is disabled during the CI validation gate so a state bucket or AWS credentials are not required just to validate configuration. The deployment job subsequently initializes the real backend before applying infrastructure.

## SQL

Repository contract tests verify that all dashboard SQL files:

- exist as expected;
- contain only `SELECT` statements;
- reference the Gold Glue Catalog;
- contain the expected query structure; and
- do not contain destructive SQL statements or unfinished placeholders.

## CI/CD order

The GitHub Actions workflow uses a mandatory `quality` job before deployment:

```text
Checkout
   ↓
Install Python dependencies
   ↓
Run all Python/PySpark tests + ≥95% coverage
   ↓
Terraform fmt + init -backend=false + validate (every module)
   ↓
Repository/SQL contract tests
   ↓
Deploy Terraform
```

The deployment job has `needs: quality`, so Terraform deployment cannot start unless the Python coverage gate and Terraform validation gate both pass.

Each deployment step also runs `terraform validate` immediately before `terraform apply` as a second defensive check.

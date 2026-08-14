#!/usr/bin/env bash
set -euo pipefail

# Validate every Terraform root independently. Backend initialization is disabled
# so CI can validate syntax/provider schemas without requiring an AWS account or
# Terraform state bucket. Deployment still runs terraform init against the real backend.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v terraform >/dev/null 2>&1; then
  echo "ERROR: terraform is required but was not found in PATH." >&2
  exit 1
fi

mapfile -t MODULES < <(find terraform -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort)

if [ "${#MODULES[@]}" -eq 0 ]; then
  echo "ERROR: no Terraform modules found under terraform/." >&2
  exit 1
fi

echo "=== Terraform formatting check ==="
terraform fmt -check -recursive terraform/

echo "=== Terraform module validation ==="
for module in "${MODULES[@]}"; do
  module_dir="terraform/${module}"
  echo "--- ${module} ---"
  terraform -chdir="${module_dir}" init -backend=false -input=false -upgrade=false
  terraform -chdir="${module_dir}" validate
  echo "PASS: ${module}"
done

echo "All Terraform modules passed fmt and validate."

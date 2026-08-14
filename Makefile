.PHONY: help init validate plan apply destroy destroy-all fmt test test-glue validate-terraform quality install precommit clean

MODULES = bootstrap s3 iam sns glue lambda step_functions eventbridge monitoring budget quicksight

help:  ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-20s %s\n", $$1, $$2}'

init: ## Initialize all Terraform modules
	@for mod in $(MODULES); do \
		if [ -d "terraform/$${mod}" ]; then \
			echo "=== $${mod} ==="; \
			cd terraform/$${mod} && terraform init && cd ../..; \
		fi; \
	done

validate: ## Validate all Terraform modules
	@for mod in $(MODULES); do \
		if [ -d "terraform/$${mod}" ]; then \
			echo "=== $${mod} ==="; \
			cd terraform/$${mod} && terraform validate && cd ../..; \
		fi; \
	done

plan: ## Plan all Terraform modules
	@for mod in $(MODULES); do \
		if [ -d "terraform/$${mod}" ]; then \
			echo "=== $${mod} ==="; \
			cd terraform/$${mod} && terraform plan && cd ../..; \
		fi; \
	done

apply: ## Apply all Terraform modules in dependency order
	@for mod in s3 iam sns glue lambda step_functions eventbridge monitoring budget; do \
		if [ -d "terraform/$${mod}" ]; then \
			echo "=== $${mod} ==="; \
			cd terraform/$${mod} && terraform apply -auto-approve && cd ../..; \
		fi; \
	done

destroy: ## Destroy all Terraform modules (dependency-safe order)
	@for mod in budget monitoring eventbridge step_functions lambda glue sns iam s3; do \
		if [ -d "terraform/$${mod}" ]; then \
			echo "=== $${mod} ==="; \
			cd terraform/$${mod} && terraform destroy -auto-approve && cd ../..; \
		fi; \
	done

destroy-all: ## Full teardown: destroy infra, then delete terraform state/locks
	@echo "=== Destroying all infrastructure ==="
	$(MAKE) destroy
	@echo ""
	@echo "=== All infrastructure destroyed ==="
	@echo "Terraform state files remain in S3 (bucket: yt-terraform-state-prakhar)"
	@echo "To remove state too, run:"
	@echo "  cd terraform/bootstrap && terraform destroy -auto-approve"
	@echo "  aws s3 rb s3://yt-terraform-state-prakhar --force"

fmt: ## Format all Terraform files
	terraform fmt -recursive terraform/

test: ## Run the full test suite with 95% minimum coverage
	python -m pip install -q -r tests/requirements-test.txt
	python -m pytest


validate-terraform: ## Validate and format-check every Terraform module without AWS
	./scripts/validate_terraform.sh

quality: ## Run Python coverage tests and Terraform validation
	python -m pytest
	./scripts/validate_terraform.sh

test-glue: ## Run Glue/PySpark tests
	python -m pip install -q -r tests/requirements-test.txt
	python -m pytest tests/glue

install: ## Install development dependencies
	python -m pip install -q -e ".[dev]"
	python -m pip install -q pre-commit ruff mypy
	pre-commit install

precommit: ## Run pre-commit hooks on all files
	pre-commit run --all-files

clean: ## Remove generated/compiled artifacts
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find terraform -type f -name "*.zip" -delete

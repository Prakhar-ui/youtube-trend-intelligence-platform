"""Repository-level Terraform contract tests.

These tests complement `terraform validate`: they catch accidental drift in the
set of modules, CI ordering, and required module metadata without needing AWS.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
TERRAFORM_ROOT = ROOT / "terraform"
WORKFLOW = ROOT / ".github" / "workflows" / "terraform-deploy.yml"
MAKEFILE = ROOT / "Makefile"

EXPECTED_MODULES = {
    "bootstrap",
    "budget",
    "eventbridge",
    "glue",
    "iam",
    "lambda",
    "monitoring",
    "quicksight",
    "s3",
    "sns",
    "step_functions",
}


def test_all_terraform_roots_have_provider_and_tf_files():
    modules = {p.name for p in TERRAFORM_ROOT.iterdir() if p.is_dir() and not p.name.startswith(".")}
    assert modules == EXPECTED_MODULES

    for module in sorted(modules):
        path = TERRAFORM_ROOT / module
        assert any(path.glob("*.tf")), f"{module} has no Terraform files"
        assert (path / "provider.tf").exists(), f"{module} is missing provider.tf"
        provider_text = (path / "provider.tf").read_text()
        assert 'source  = "hashicorp/aws"' in provider_text or 'source = "hashicorp/aws"' in provider_text
        assert 'version = "~> 5.0"' in provider_text


def test_workflow_validates_before_deployment():
    workflow = WORKFLOW.read_text()
    assert "jobs:" in workflow
    assert re.search(r"quality:\n", workflow), "CI must have a quality gate"
    assert "needs: quality" in workflow, "deployment must depend on the quality gate"
    assert "scripts/validate_terraform.sh" in workflow
    assert "python -m pytest" in workflow
    assert "terraform validate" in workflow or "validate_terraform.sh" in workflow

    quality_block = workflow.split("  deploy:", 1)[0]
    assert "Run Python tests with 95% coverage" in quality_block
    assert "Validate every Terraform module" in quality_block


def test_makefile_lists_every_non_generated_terraform_root():
    makefile = MAKEFILE.read_text()
    modules_match = re.search(r"^MODULES\s*=\s*(.+)$", makefile, re.MULTILINE)
    assert modules_match
    listed = set(modules_match.group(1).split())
    assert EXPECTED_MODULES - listed == set(), "Makefile MODULES is missing Terraform roots"

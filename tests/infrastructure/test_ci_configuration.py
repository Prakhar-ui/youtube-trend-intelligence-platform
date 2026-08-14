"""CI configuration tests that fail fast when quality gates are weakened."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "terraform-deploy.yml"


def test_ci_has_python_coverage_gate_and_artifact():
    text = WORKFLOW.read_text()
    assert "python-version: \"3.12\"" in text
    assert "python -m pytest" in text
    assert "--cov-fail-under=95" in text or "--cov-fail-under 95" in text or "95%" in text
    assert "coverage.xml" in text


def test_deploy_cannot_run_until_quality_passes():
    text = WORKFLOW.read_text()
    deploy = text.split("  deploy:", 1)[1]
    assert "needs: quality" in deploy
    assert "if: github.event_name == 'push'" in deploy


def test_terraform_validation_runs_before_apply():
    text = WORKFLOW.read_text()
    deploy = text.split("  deploy:", 1)[1]
    # The deployment remains defensive too: each applied module is validated.
    assert deploy.count("terraform validate") >= 8

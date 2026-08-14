"""Static SQL contract tests for the Athena/QuickSight query layer."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
SQL_ROOT = ROOT / "sql"
EXPECTED_FILES = {
    "channel_intelligence.sql",
    "emerging_trends.sql",
    "executive_overview.sql",
    "market_intelligence.sql",
}


def _statements(text: str):
    # SQL files contain dashboard queries separated by semicolons. Ignore comments
    # and empty statements while retaining enough structure for contract checks.
    without_comments = re.sub(r"--.*?$", "", text, flags=re.MULTILINE)
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def test_expected_dashboard_sql_files_exist():
    assert {p.name for p in SQL_ROOT.glob("*.sql")} == EXPECTED_FILES


def test_every_sql_file_contains_select_statements():
    for path in sorted(SQL_ROOT.glob("*.sql")):
        statements = _statements(path.read_text())
        assert statements, f"{path.name} contains no SQL statements"
        assert all(re.match(r"^SELECT\b", statement, flags=re.IGNORECASE | re.DOTALL) for statement in statements), (
            f"{path.name} contains a non-SELECT statement"
        )


def test_dashboard_queries_use_expected_gold_catalog():
    for path in sorted(SQL_ROOT.glob("*.sql")):
        text = path.read_text()
        assert "yt_pipeline_gold_dev." in text, f"{path.name} does not reference the Gold catalog"
        assert "SELECT" in text.upper(), f"{path.name} has no SELECT"
        assert "FROM" in text.upper(), f"{path.name} has no FROM clause"


def test_sql_has_no_placeholder_or_destructive_commands():
    combined = "\n".join(p.read_text() for p in SQL_ROOT.glob("*.sql")).upper()
    assert "TODO" not in combined
    assert not re.search(r"\b(DROP|DELETE|TRUNCATE|INSERT|UPDATE|ALTER)\b", combined)

import importlib.util
import pathlib

import pytest

# Unit tests mock AWS Wrangler I/O; keep collection lightweight when the
# production-only dependency is not installed locally.
try:
    import awswrangler  # noqa: F401
except ImportError:
    import sys
    import types
    _wr = types.ModuleType("awswrangler")
    _wr.s3 = types.SimpleNamespace(to_parquet=None)
    _wr.athena = types.SimpleNamespace(read_sql_query=None)
    sys.modules["awswrangler"] = _wr

SCRIPTS_ROOT = pathlib.Path(__file__).parent.parent / "terraform" / "lambda" / "scripts"


def _load_lambda_module(unique_name: str, function_dir: str):
    """Loads a Lambda's lambda_function.py under a unique module name.

    All three Lambdas in this project use the same filename
    (lambda_function.py), so a plain `sys.path.insert` + `import
    lambda_function` would silently reuse whichever one Python imported
    first via sys.modules. Loading each by explicit file path under a
    distinct name avoids that collision entirely.
    """
    path = SCRIPTS_ROOT / function_dir / "lambda_function.py"
    spec = importlib.util.spec_from_file_location(unique_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def youtube_ingestion():
    return _load_lambda_module("youtube_ingestion_module", "youtube_api_integration")


@pytest.fixture(scope="session")
def json_to_parquet_module():
    return _load_lambda_module("json_to_parquet_module", "json_to_parquet")


@pytest.fixture(scope="session")
def data_quality_check_module():
    return _load_lambda_module("data_quality_check_module", "data_quality_check")

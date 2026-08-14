"""PySpark test fixtures for Glue job unit tests."""
import importlib.util
import pathlib
import sys

import pytest

SCRIPTS_ROOT = pathlib.Path(__file__).parent.parent.parent / 'terraform' / 'glue' / 'scripts'


@pytest.fixture(scope='session')
def spark():
    """Creates a local SparkSession for unit testing PySpark transformations.
    
    Uses single thread and node to keep tests lightweight.
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('test') \
        .config('spark.sql.shuffle.partitions', '1') \
        .config('spark.sql.adaptive.enabled', 'false') \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def bronze_to_silver_module():
    """Loads the bronze_to_silver_statistics module."""
    path = SCRIPTS_ROOT / 'bronze_to_silver_statistics.py'
    spec = importlib.util.spec_from_file_location('bronze_to_silver_module', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def silver_to_gold_module():
    """Loads the silver_to_gold_analytics module."""
    path = SCRIPTS_ROOT / 'silver_to_gold_analytics.py'
    spec = importlib.util.spec_from_file_location('silver_to_gold_module', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def trend_intelligence_module():
    """Loads the trend_intelligence module."""
    path = SCRIPTS_ROOT / 'trend_intelligence.py'
    spec = importlib.util.spec_from_file_location('trend_intelligence_module', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
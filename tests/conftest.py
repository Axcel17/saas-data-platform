"""Shared pytest fixtures."""

from __future__ import annotations

import os
import sys

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Workers must use the same interpreter as the driver (see spark_session.py).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A local, Delta-enabled Spark session for the whole test session."""
    builder = (
        SparkSession.builder.appName("saas-pipeline-tests")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

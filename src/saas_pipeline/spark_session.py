"""Spark session construction with Delta Lake enabled.

The same builder works locally and on Databricks. Locally,
``configure_spark_with_delta_pip`` pulls the matching ``delta-spark`` jars and
registers the Delta extensions. On Databricks the runtime already provides both,
so those static configs are skipped and the existing session is reused.
"""

from __future__ import annotations

import os
import sys

from delta import configure_spark_with_delta_pip
from omegaconf import DictConfig
from pyspark.sql import SparkSession


def _pin_worker_python() -> None:
    """Ensure Spark workers use the same interpreter as the driver.

    Without this, workers can pick up a different system Python and fail to
    import the project's dependencies. Harmless on Databricks, where both are
    already aligned.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _on_databricks() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def get_spark(cfg: DictConfig) -> SparkSession:
    """Return a Delta-enabled Spark session.

    Time zone is pinned to UTC so technical timestamps are unambiguous, and
    dynamic partition overwrite is enabled so Bronze/Gold writes replace only
    the partitions present in the batch (idempotent reprocessing).
    """
    _pin_worker_python()
    builder = SparkSession.builder.appName(cfg.spark.app_name)

    if _on_databricks():
        spark = builder.getOrCreate()
    else:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Runtime configs, settable on both local and Databricks sessions.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.shuffle.partitions", int(cfg.spark.shuffle_partitions))
    return spark

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

from saas_pipeline.logging_config import get_logger

log = get_logger(__name__)


def _pin_worker_python() -> None:
    """Ensure Spark workers use the same interpreter as the driver.

    Without this, workers can pick up a different system Python and fail to
    import the project's dependencies. Harmless on Databricks, where both are
    already aligned.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _safe_set(spark: SparkSession, key: str, value: object) -> None:
    """Set a Spark conf, tolerating the ones locked on Databricks serverless."""
    try:
        spark.conf.set(key, value)
    except Exception as exc:  # noqa: BLE001 - serverless locks some configs
        log.debug("Skipping locked Spark conf %s: %s", key, exc)


def get_spark(cfg: DictConfig) -> SparkSession:
    """Return a Delta-enabled Spark session.

    Time zone is pinned to UTC so technical timestamps are unambiguous. Dynamic
    partition overwrite is applied per write (see ``storage.overwrite``) rather
    than as a session config, because serverless locks that config.
    """
    _pin_worker_python()

    # On Databricks (classic or serverless) a session already exists; reuse it
    # instead of building one, which avoids the serverless "no active session"
    # error and keeps us from stopping a session we do not own.
    active = SparkSession.getActiveSession()
    if active is not None:
        _safe_set(active, "spark.sql.session.timeZone", "UTC")
        return active

    builder = (
        SparkSession.builder.appName(cfg.spark.app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    _safe_set(spark, "spark.sql.session.timeZone", "UTC")
    _safe_set(spark, "spark.sql.shuffle.partitions", int(cfg.spark.shuffle_partitions))
    return spark

"""Bronze layer: raw CSV ingestion to Delta.

Bronze preserves the original schema (all columns as strings) and adds the
technical columns required by section 6.2. It is partitioned by ``fecha_proceso``
and ``_tenant_id`` and written idempotently: dynamic partition overwrite replaces
only the partitions present in the batch, so re-running a range never duplicates.

Rows with a null/invalid ``fecha_proceso`` cannot be range-filtered; they are
still ingested (never silently dropped) so Silver can quarantine them. See
docs/observations.md for the rationale.
"""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline import storage, transforms
from saas_pipeline.logging_config import get_logger
from saas_pipeline.paths import table_location
from saas_pipeline.schemas import DELIVERIES_RAW_SCHEMA

log = get_logger(__name__)

TABLE = "deliveries"


def _read_raw(spark: SparkSession, cfg: DictConfig) -> DataFrame:
    source = cfg.paths.raw.deliveries
    return (
        spark.read.option("header", True)
        .schema(DELIVERIES_RAW_SCHEMA)
        .csv(source)
        .withColumn("_source_file", F.lit(source))
    )


def _within_range(cfg: DictConfig):
    """Predicate: keep rows in the fecha_proceso range OR with an invalid date.

    Invalid/null dates are retained regardless of range so they reach Silver
    quarantine instead of vanishing.
    """
    fecha = transforms.parsed_fecha_column()
    predicate = F.lit(True)
    if cfg.execution.start_date is not None:
        predicate = predicate & (fecha >= F.lit(str(cfg.execution.start_date)))
    if cfg.execution.end_date is not None:
        predicate = predicate & (fecha <= F.lit(str(cfg.execution.end_date)))
    return predicate | fecha.isNull()


def build_bronze(spark: SparkSession, cfg: DictConfig, tenant: str, batch_id: str) -> DataFrame:
    """Ingest one tenant's deliveries into Bronze and return the written frame."""
    raw = _read_raw(spark, cfg)
    tenant_df = transforms.normalize_tenant(raw).where(F.col("_tenant_id") == tenant)
    scoped = tenant_df.where(_within_range(cfg))

    bronze_df = scoped.withColumn("_ingestion_timestamp", F.current_timestamp()).withColumn(
        "_batch_id", F.lit(batch_id)
    )

    location = table_location(cfg, "bronze", tenant, TABLE)
    log.info("Bronze %s/%s: writing %d rows to %s", tenant, TABLE, bronze_df.count(), location.ref)

    storage.overwrite(spark, location, bronze_df, partition_by=["fecha_proceso", "_tenant_id"])
    return bronze_df


def read_bronze(spark: SparkSession, cfg: DictConfig, tenant: str) -> DataFrame:
    """Read a tenant's Bronze table (used by the Silver layer)."""
    return storage.read(spark, table_location(cfg, "bronze", tenant, TABLE))

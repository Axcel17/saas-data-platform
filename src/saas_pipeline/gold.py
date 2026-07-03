"""Gold layer: business metrics.

Two derived tables per tenant, both recomputed from Silver (not authoritative):

* ``daily_metrics_by_delivery_type`` - one row per (tenant, fecha_proceso,
  tipo_entrega): units, revenue, distinct routes and transports.
* ``top_materials_by_tenant_month`` - the top materials per tenant and month,
  ranked by revenue.
"""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from saas_pipeline import silver, storage
from saas_pipeline.logging_config import get_logger
from saas_pipeline.paths import table_location

log = get_logger(__name__)

DAILY_METRICS_TABLE = "daily_metrics_by_delivery_type"
TOP_MATERIALS_TABLE = "top_materials_by_tenant_month"


def compute_daily_metrics(fact: DataFrame) -> DataFrame:
    """Aggregate facts to one row per (tenant, fecha_proceso, tipo_entrega).

    ``total_units`` uses the ST-normalised quantity; ``total_revenue`` uses the
    transaction price (``precio``), not the catalog ``precio_base``.
    """
    precio = F.col("precio").cast("double")
    return (
        fact.groupBy("_tenant_id", "fecha_proceso", "tipo_entrega")
        .agg(
            F.sum("cantidad_st").alias("total_units"),
            F.sum(F.col("cantidad_st") * precio).alias("total_revenue"),
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports"),
        )
        .withColumnRenamed("_tenant_id", "tenant_id")
    )


def compute_top_materials(fact: DataFrame, top_n: int) -> DataFrame:
    """Rank materials by revenue within each (tenant, month), keeping the top N.

    Month is derived from the first six digits of ``fecha_proceso`` (YYYYMM).
    """
    precio = F.col("precio").cast("double")
    per_material = (
        fact.withColumn("year_month", F.substring("fecha_proceso", 1, 6))
        .groupBy("_tenant_id", "year_month", "material")
        .agg(
            F.first("descripcion", ignorenulls=True).alias("descripcion"),
            F.first("categoria", ignorenulls=True).alias("categoria"),
            F.sum("cantidad_st").alias("total_units"),
            F.sum(F.col("cantidad_st") * precio).alias("total_revenue"),
        )
        .withColumnRenamed("_tenant_id", "tenant_id")
    )
    rank = F.row_number().over(
        Window.partitionBy("tenant_id", "year_month").orderBy(F.col("total_revenue").desc())
    )
    return per_material.withColumn("rank", rank).where(F.col("rank") <= top_n)


def build_gold(spark: SparkSession, cfg: DictConfig, tenant: str) -> None:
    """Build both Gold tables for a tenant."""
    fact = silver.read_fact(spark, cfg, tenant)

    daily = compute_daily_metrics(fact)
    daily_loc = table_location(cfg, "gold", tenant, DAILY_METRICS_TABLE)
    # Dynamic partition overwrite recomputes only the date partitions in the batch.
    storage.overwrite(spark, daily_loc, daily, partition_by="fecha_proceso")
    log.info("Gold %s/%s: %d rows", tenant, DAILY_METRICS_TABLE, daily.count())

    top = compute_top_materials(fact, int(cfg.gold.top_materials_n))
    top_loc = table_location(cfg, "gold", tenant, TOP_MATERIALS_TABLE)
    storage.overwrite(spark, top_loc, top)
    log.info("Gold %s/%s: %d rows", tenant, TOP_MATERIALS_TABLE, top.count())

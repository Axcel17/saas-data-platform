"""Refactor of ``bad_code.py``.

Same intent (compute normalised quantity and revenue per routine delivery for a
tenant), rewritten as idempotent, testable Spark-native code that fits the SAAS
architecture. The transformation is a pure function of a DataFrame; the I/O
boundaries (read, write) are thin and parameterised, with no hardcoded paths,
countries or magic numbers.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROUTINE_DELIVERY_TYPES = ("ZPRE", "ZVE1")
UNITS_PER_CASE = 20


@dataclass(frozen=True)
class DeliveryJobConfig:
    """Parameters for a single run. No values are hardcoded in the logic."""

    source_path: str
    output_path: str
    tenant_id: str
    units_per_case: int = UNITS_PER_CASE


def normalize_deliveries(deliveries: DataFrame, units_per_case: int) -> DataFrame:
    """Filter to routine deliveries, normalise units to ST and compute revenue.

    Vectorised Spark transformation - no row-by-row iteration. Returns a new
    DataFrame; the input is not mutated.
    """
    cantidad_st = F.when(
        F.col("unidad") == "CS", F.col("cantidad") * units_per_case
    ).otherwise(F.col("cantidad"))

    return (
        deliveries.where(F.col("tipo_entrega").isin(ROUTINE_DELIVERY_TYPES))
        .withColumn("cantidad_st", cantidad_st)
        .withColumn("total_revenue", F.col("cantidad_st") * F.col("precio"))
        .select(
            F.col("pais").alias("tenant_id"),
            F.col("fecha_proceso"),
            F.col("material"),
            "cantidad_st",
            "total_revenue",
        )
    )


def read_deliveries(spark: SparkSession, source_path: str, tenant_id: str) -> DataFrame:
    """Read the source with Spark and scope it to one tenant."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
        .where(F.col("pais") == tenant_id)
    )


def run(spark: SparkSession, config: DeliveryJobConfig) -> DataFrame:
    """Read, transform and write idempotently, partitioned by tenant."""
    deliveries = read_deliveries(spark, config.source_path, config.tenant_id)
    result = normalize_deliveries(deliveries, config.units_per_case)

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"tenant_id = '{config.tenant_id}'")
        .partitionBy("tenant_id")
        .save(config.output_path)
    )
    return result


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    session = configure_spark_with_delta_pip(
        SparkSession.builder.appName("deliveries-job")
    ).getOrCreate()
    job = DeliveryJobConfig(
        source_path="data/raw/global_mobility_data_entrega_productos.csv",
        output_path="data/dev/gold/deliveries_revenue",
        tenant_id="gt",
    )
    run(session, job)

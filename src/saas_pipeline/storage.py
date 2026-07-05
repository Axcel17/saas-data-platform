"""Delta read/write primitives that work for both storage modes.

Every layer goes through these helpers instead of calling ``.save(path)`` or
``.saveAsTable(name)`` directly, so switching between local paths and Unity
Catalog tables is a configuration change, not a code change. See
:mod:`saas_pipeline.paths` for how a :class:`Location` is resolved.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from saas_pipeline.paths import Location


def _ensure_schema(spark: SparkSession, ref: str) -> None:
    """Create the Unity Catalog schema for ``catalog.schema.table`` if missing.

    The catalog itself is assumed to exist (created once during tenant
    onboarding); see docs/databricks.md.
    """
    catalog, schema, _ = ref.split(".")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _exists(spark: SparkSession, loc: Location) -> bool:
    if loc.is_table:
        return spark.catalog.tableExists(loc.ref)
    return DeltaTable.isDeltaTable(spark, loc.ref)


def _save(spark: SparkSession, loc: Location, writer) -> None:
    if loc.is_table:
        _ensure_schema(spark, loc.ref)
        writer.saveAsTable(loc.ref)
    else:
        writer.save(loc.ref)


def read(spark: SparkSession, loc: Location) -> DataFrame:
    if loc.is_table:
        return spark.read.table(loc.ref)
    return spark.read.format("delta").load(loc.ref)


def overwrite(
    spark: SparkSession, loc: Location, df: DataFrame, partition_by: list[str] | str | None = None
) -> None:
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        # Dynamic partition overwrite as a write option (portable to serverless,
        # which locks the equivalent session config): only the partitions present
        # in the batch are replaced, keeping reprocessing idempotent.
        writer = writer.partitionBy(partition_by).option("partitionOverwriteMode", "dynamic")
    _save(spark, loc, writer)


def append(spark: SparkSession, loc: Location, df: DataFrame) -> None:
    _save(spark, loc, df.write.format("delta").mode("append"))


def upsert(
    spark: SparkSession,
    loc: Location,
    df: DataFrame,
    merge_condition: str,
    partition_by: list[str] | str | None = None,
) -> None:
    """MERGE ``df`` into the table, creating it on first run.

    The MERGE is what makes reprocessing idempotent: matched rows are updated,
    new rows inserted, no duplicates.
    """
    if _exists(spark, loc):
        target = (
            DeltaTable.forName(spark, loc.ref)
            if loc.is_table
            else DeltaTable.forPath(spark, loc.ref)
        )
        (
            target.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return
    writer = df.write.format("delta")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    _save(spark, loc, writer)

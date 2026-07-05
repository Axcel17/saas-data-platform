"""Pure, side-effect-free transformations.

Every function takes a DataFrame (plus scalar policy parameters) and returns a
DataFrame. Keeping the business logic free of I/O is what makes it unit-testable
without a warehouse, and keeps the layer modules (bronze/silver/gold) focused on
reading and writing Delta.

Domain columns keep their original (Spanish) source names because they are the
ubiquitous language of the dataset and are referenced verbatim in the
architecture. Engineered flags and technical columns use the English,
underscore-prefixed names mandated by the naming conventions (section 5.3).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# --- Bronze-level normalisation -------------------------------------------------

# The original transactional columns, used to detect exact duplicates.
SOURCE_COLUMNS = [
    "pais",
    "fecha_proceso",
    "transporte",
    "ruta",
    "tipo_entrega",
    "material",
    "precio",
    "cantidad",
    "unidad",
]


def normalize_tenant(df: DataFrame) -> DataFrame:
    """Lowercase the tenant code (``pais``) into the ``_tenant_id`` column.

    The CSV delivers country codes in uppercase (SV, HN, ...); paths, schemas
    and the ``_tenant_id`` column must use lowercase (section 5.3).
    """
    return df.withColumn("_tenant_id", F.lower(F.trim(F.col("pais"))))


# --- Parsing helpers ------------------------------------------------------------


def parsed_fecha_column() -> Column:
    """Parse ``fecha_proceso`` (string YYYYMMDD) into a valid ``date`` or null.

    ``try_to_timestamp`` returns null for unparseable or impossible values such
    as ``00000000``, ``20251332`` or ``20250230`` (Feb 30), the signal used to
    route date anomalies to quarantine. Unlike ``to_date`` it returns null
    instead of raising under ANSI mode (Databricks default), so the same code
    behaves identically on local Spark and on Databricks.
    """
    return F.expr("to_date(try_to_timestamp(fecha_proceso, 'yyyyMMdd'))")


def _numeric(col_name: str) -> Column:
    """Cast a raw string column to double, yielding null when not numeric."""
    return F.col(col_name).cast("double")


# --- Silver transformations -----------------------------------------------------


def deduplicate_exact(df: DataFrame) -> DataFrame:
    """Drop exact duplicates across all original source columns (section 5.6)."""
    return df.dropDuplicates(SOURCE_COLUMNS)


def split_by_delivery_type(df: DataFrame, valid_types: list[str]) -> tuple[DataFrame, DataFrame]:
    """Partition rows into (kept, discarded) by ``tipo_entrega``.

    Types outside the valid set (e.g. COBR, Z99) are a business-rule discard:
    they are counted but not persisted (section 5.6).
    """
    is_valid = F.col("tipo_entrega").isin(valid_types)
    return df.where(is_valid), df.where(~is_valid | F.col("tipo_entrega").isNull())


def with_quarantine_reason(df: DataFrame) -> DataFrame:
    """Add ``_quarantine_reason`` for value-level anomalies, else null.

    Precedence is deterministic; the first matching rule wins. Material
    integrity is evaluated separately, after the catalog is known.
    """
    cantidad = _numeric("cantidad")
    precio = _numeric("precio")
    reason = (
        F.when(parsed_fecha_column().isNull(), F.lit("invalid_fecha_proceso"))
        .when(cantidad.isNull() | (cantidad <= 0), F.lit("invalid_cantidad"))
        .when(precio.isNull(), F.lit("null_precio"))
        .otherwise(F.lit(None))
    )
    return df.withColumn("_quarantine_reason", reason)


def flag_material_integrity(df: DataFrame, catalog_materials: DataFrame) -> DataFrame:
    """Mark rows whose material is absent from the catalog for quarantine.

    Breaking referential integrity must be visible and auditable, never dropped
    silently in the join (section 5.6). Only rows that are otherwise clean are
    evaluated, so an existing reason is preserved.
    """
    known = (
        catalog_materials.select("material").distinct().withColumn("_material_known", F.lit(True))
    )
    joined = df.join(F.broadcast(known), on="material", how="left")
    reason = F.when(F.col("_quarantine_reason").isNotNull(), F.col("_quarantine_reason")).when(
        F.col("_material_known").isNull(), F.lit("material_not_in_catalog")
    )
    return joined.withColumn("_quarantine_reason", reason).drop("_material_known")


def normalize_units(df: DataFrame, units_per_case: int) -> DataFrame:
    """Convert quantities to the common ST unit (1 CS = ``units_per_case`` ST).

    Produces ``cantidad_st`` from the numeric ``cantidad``; the ST value is the
    basis for every downstream metric.
    """
    cantidad = _numeric("cantidad")
    cantidad_st = F.when(F.col("unidad") == "CS", cantidad * units_per_case).otherwise(cantidad)
    return df.withColumn("cantidad_st", cantidad_st)


def add_delivery_flags(
    df: DataFrame, routine_types: list[str], bonus_types: list[str]
) -> DataFrame:
    """Add ``is_routine_delivery`` and ``is_bonus_delivery`` boolean flags."""
    return df.withColumn(
        "is_routine_delivery", F.col("tipo_entrega").isin(routine_types)
    ).withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(bonus_types))


def enrich_with_materials(fact: DataFrame, dim: DataFrame) -> DataFrame:
    """Temporal join of facts with the SCD Type 2 material dimension.

    The version is selected by ``fecha_date BETWEEN valid_from AND valid_to``,
    i.e. the version in force at the transaction date - not ``is_current`` -
    otherwise historical metrics would use the wrong price/category.

    The catalog price is kept as the informational column ``precio_base``; the
    revenue-bearing price stays ``precio`` (the transaction price).
    """
    dim_scoped = dim.select(
        F.col("material").alias("_dim_material"),
        F.col("descripcion"),
        F.col("categoria"),
        F.col("precio_base"),
        F.col("valid_from"),
        F.col("valid_to"),
    )
    condition = (fact["material"] == dim_scoped["_dim_material"]) & (
        F.col("fecha_date").between(F.col("valid_from"), F.col("valid_to"))
    )
    enriched = fact.join(dim_scoped, on=condition, how="left")
    return enriched.drop("_dim_material", "valid_from", "valid_to")

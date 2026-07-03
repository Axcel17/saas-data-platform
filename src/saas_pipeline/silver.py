"""Silver layer: cleaning, SCD Type 2 dimension, enrichment and quarantine.

Produces two tables per tenant:

* ``dim_materials``   - SCD Type 2 catalog, upserted on (material, valid_from).
* ``fact_deliveries`` - deduplicated, unit-normalised, flagged and enriched
  facts, upserted (MERGE INTO) on the composite business key.

Anomalies are handled per section 5.6: value/integrity anomalies go to a
parallel quarantine table with ``_quarantine_reason``; out-of-scope delivery
types are discarded and only counted.
"""

from __future__ import annotations

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from saas_pipeline import bronze, storage, transforms
from saas_pipeline.logging_config import get_logger
from saas_pipeline.paths import quarantine_location, table_location
from saas_pipeline.schemas import MATERIALS_RAW_SCHEMA

log = get_logger(__name__)

FACT_TABLE = "fact_deliveries"
DIM_TABLE = "dim_materials"

VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]

# Composite business key for fact idempotency (section 5.5).
FACT_BUSINESS_KEY = [
    "_tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "material",
    "tipo_entrega",
]


# --- dim_materials (SCD Type 2) -------------------------------------------------


def build_dim_materials(spark: SparkSession, cfg: DictConfig, tenant: str) -> DataFrame:
    """Load the catalog and upsert it as an SCD Type 2 dimension.

    The source already carries valid_from/valid_to/is_current, so the version
    key is (material, valid_from): existing versions are updated, new versions
    inserted, and history preserved.
    """
    catalog = _read_materials(spark, cfg)
    location = table_location(cfg, "silver", tenant, DIM_TABLE)

    storage.upsert(
        spark, location, catalog, "t.material = s.material AND t.valid_from = s.valid_from"
    )

    dim = storage.read(spark, location)
    log.info("Silver %s/%s: %d versions", tenant, DIM_TABLE, dim.count())
    return dim


def _read_materials(spark: SparkSession, cfg: DictConfig) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(MATERIALS_RAW_SCHEMA)
        .csv(cfg.paths.raw.materials)
        .withColumn("precio_base", F.col("precio_base").cast("decimal(18,4)"))
        .withColumn("valid_from", F.to_date("valid_from", "yyyy-MM-dd"))
        .withColumn("valid_to", F.to_date("valid_to", "yyyy-MM-dd"))
        .withColumn("is_current", F.col("is_current") == F.lit("true"))
    )


# --- fact_deliveries ------------------------------------------------------------


def build_fact_deliveries(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    dim: DataFrame,
) -> tuple[DataFrame, int]:
    """Clean, enrich and upsert fact_deliveries. Returns (facts, discarded_count)."""
    src = bronze.read_bronze(spark, cfg, tenant).withColumn(
        "fecha_date", transforms.parsed_fecha_column()
    )

    deduped = transforms.deduplicate_exact(src)
    kept, discarded = transforms.split_by_delivery_type(deduped, VALID_DELIVERY_TYPES)
    discarded_count = discarded.count()

    flagged = transforms.flag_material_integrity(transforms.with_quarantine_reason(kept), dim)

    quarantine = flagged.where(F.col("_quarantine_reason").isNotNull())
    clean = flagged.where(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")

    normalized = transforms.add_delivery_flags(
        transforms.normalize_units(clean, int(cfg.business.units_per_case)),
        list(cfg.business.routine_delivery_types),
        list(cfg.business.bonus_delivery_types),
    )
    enriched = transforms.enrich_with_materials(normalized, dim)

    # A material present in the catalog but with no version covering the
    # transaction date is a temporal-integrity anomaly -> quarantine.
    no_version = enriched.where(F.col("categoria").isNull()).withColumn(
        "_quarantine_reason", F.lit("no_material_version_for_date")
    )
    facts = enriched.where(F.col("categoria").isNotNull())

    _write_quarantine(spark, cfg, tenant, quarantine, no_version)
    facts = _deduplicate_business_key(facts)
    _merge_fact(spark, cfg, tenant, facts)

    log.info(
        "Silver %s/%s: %d facts, %d quarantined, %d discarded",
        tenant,
        FACT_TABLE,
        facts.count(),
        quarantine.count() + no_version.count(),
        discarded_count,
    )
    return facts, discarded_count


def _deduplicate_business_key(facts: DataFrame) -> DataFrame:
    """Keep one row per business key before MERGE (the latest ingested).

    MERGE INTO fails if several source rows match the same target key. The
    source has no line id, so this resolves near-duplicates deterministically.
    """
    window = Window.partitionBy(*FACT_BUSINESS_KEY).orderBy(F.col("_ingestion_timestamp").desc())
    return facts.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def _merge_fact(spark: SparkSession, cfg: DictConfig, tenant: str, facts: DataFrame) -> None:
    location = table_location(cfg, "silver", tenant, FACT_TABLE)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in FACT_BUSINESS_KEY)
    storage.upsert(spark, location, facts, condition, partition_by="fecha_proceso")


def _write_quarantine(
    spark: SparkSession, cfg: DictConfig, tenant: str, *frames: DataFrame
) -> None:
    """Overwrite the quarantine table with rows carrying _quarantine_reason."""
    location = quarantine_location(cfg, "silver", tenant, FACT_TABLE)
    combined: DataFrame | None = None
    for frame in frames:
        selected = frame.select(*transforms.SOURCE_COLUMNS, "_quarantine_reason")
        combined = selected if combined is None else combined.unionByName(selected)
    if combined is not None:
        storage.overwrite(spark, location, combined)


def read_fact(spark: SparkSession, cfg: DictConfig, tenant: str) -> DataFrame:
    return storage.read(spark, table_location(cfg, "silver", tenant, FACT_TABLE))

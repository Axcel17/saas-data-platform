"""Data-quality framework.

Checks run against Silver, each declaring a severity (critical / warning / info).
Every result is persisted to the shared ``quality_logs`` Delta table using the
schema from section 5.9. When ``quality.fail_on_critical`` is set and any
critical check fails, the caller aborts before Gold.
"""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from saas_pipeline import storage
from saas_pipeline.logging_config import get_logger
from saas_pipeline.paths import quality_logs_location
from saas_pipeline.schemas import QUALITY_LOGS_SCHEMA

# quality_logs schema without executed_at, which is stamped at write time.
_QUALITY_LOGS_INPUT_SCHEMA = StructType(
    [f for f in QUALITY_LOGS_SCHEMA.fields if f.name != "executed_at"]
)

log = get_logger(__name__)

CRITICAL = "critical"
WARNING = "warning"
INFO = "info"


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    check_severity: str
    layer: str
    table_name: str
    records_checked: int
    records_failed: int

    @property
    def passed(self) -> bool:
        return self.records_failed == 0


def _count_failing(df: DataFrame, condition) -> int:
    return df.where(condition).count()


def run_silver_checks(fact: DataFrame, dim: DataFrame) -> list[CheckResult]:
    """Run the Silver data-quality suite and return one result per check."""
    fact_total = fact.count()
    dim_total = dim.count()

    results = [
        CheckResult(
            "cantidad_st_not_null",
            CRITICAL,
            "silver",
            "fact_deliveries",
            fact_total,
            _count_failing(fact, F.col("cantidad_st").isNull()),
        ),
        CheckResult(
            "cantidad_st_positive",
            CRITICAL,
            "silver",
            "fact_deliveries",
            fact_total,
            _count_failing(fact, F.col("cantidad_st") <= 0),
        ),
        CheckResult(
            "delivery_type_in_scope",
            CRITICAL,
            "silver",
            "fact_deliveries",
            fact_total,
            _count_failing(fact, ~F.col("tipo_entrega").isin(["ZPRE", "ZVE1", "Z04", "Z05"])),
        ),
        CheckResult(
            # A fact with no material version at its date slipped past enrichment;
            # a warning surfaces temporal gaps in the catalog without blocking.
            "material_version_resolved",
            WARNING,
            "silver",
            "fact_deliveries",
            fact_total,
            _count_failing(fact, F.col("categoria").isNull()),
        ),
        CheckResult(
            # SCD2 invariant: at most one current version per SKU.
            "single_current_version_per_material",
            CRITICAL,
            "silver",
            "dim_materials",
            dim_total,
            _current_version_violations(dim),
        ),
    ]

    for r in results:
        log.info(
            "DQ %-38s [%-8s] %s (%d/%d failed)",
            r.check_name,
            r.check_severity,
            "PASS" if r.passed else "FAIL",
            r.records_failed,
            r.records_checked,
        )
    return results


def _current_version_violations(dim: DataFrame) -> int:
    offenders = dim.where(F.col("is_current")).groupBy("material").count().where(F.col("count") > 1)
    return offenders.count()


def has_critical_failure(results: list[CheckResult]) -> bool:
    return any(r.check_severity == CRITICAL and not r.passed for r in results)


def persist_quality_logs(
    spark: SparkSession,
    cfg: DictConfig,
    results: list[CheckResult],
    *,
    run_id: str,
    batch_id: str,
    tenant: str,
) -> None:
    """Append check results to the shared quality-logs Delta table."""
    if not results:
        return

    rows = [
        (
            run_id,
            batch_id,
            tenant,
            r.layer,
            r.table_name,
            r.check_name,
            r.check_severity,
            r.records_checked,
            r.records_failed,
            r.passed,
        )
        for r in results
    ]
    df = spark.createDataFrame(rows, schema=_QUALITY_LOGS_INPUT_SCHEMA).withColumn(
        "executed_at", F.current_timestamp()
    )
    ordered = df.select(*[f.name for f in QUALITY_LOGS_SCHEMA.fields])

    storage.append(spark, quality_logs_location(cfg), ordered)
    log.info("Persisted %d quality-check rows for tenant %s", len(rows), tenant)

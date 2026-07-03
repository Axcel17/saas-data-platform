"""Pipeline orchestration.

Runs the requested layers for one tenant or for every configured tenant. With
``--tenant all`` the ``execution.fail_fast`` flag decides whether one tenant's
failure aborts the batch or is collected and reported at the end.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from omegaconf import DictConfig
from pyspark.sql import SparkSession

from saas_pipeline import bronze, gold, quality, silver
from saas_pipeline.config import configured_tenants
from saas_pipeline.logging_config import get_logger
from saas_pipeline.spark_session import get_spark

log = get_logger(__name__)

LAYERS = ("bronze", "silver", "gold")


class CriticalQualityError(RuntimeError):
    """Raised when a critical data-quality check fails and fail_on_critical is set."""


@dataclass
class TenantOutcome:
    tenant: str
    status: str  # "ok" | "failed"
    error: str | None = None


@dataclass
class RunReport:
    run_id: str
    outcomes: list[TenantOutcome] = field(default_factory=list)

    @property
    def failed(self) -> list[TenantOutcome]:
        return [o for o in self.outcomes if o.status == "failed"]


def run_tenant(
    spark: SparkSession,
    cfg: DictConfig,
    tenant: str,
    run_id: str,
    layers: tuple[str, ...] = LAYERS,
) -> None:
    """Execute the requested layers for a single tenant."""
    batch_id = f"{run_id[:8]}-{tenant}"
    log.info("=== tenant=%s run_id=%s layers=%s ===", tenant, run_id, ",".join(layers))

    if "bronze" in layers:
        bronze.build_bronze(spark, cfg, tenant, batch_id)

    if "silver" in layers:
        dim = silver.build_dim_materials(spark, cfg, tenant)
        facts, _ = silver.build_fact_deliveries(spark, cfg, tenant, dim)

        results = quality.run_silver_checks(facts, dim)
        quality.persist_quality_logs(
            spark, cfg, results, run_id=run_id, batch_id=batch_id, tenant=tenant
        )
        if cfg.quality.fail_on_critical and quality.has_critical_failure(results):
            raise CriticalQualityError(
                f"Critical quality check failed for tenant '{tenant}'; aborting before Gold."
            )

    if "gold" in layers:
        gold.build_gold(spark, cfg, tenant)


def run(cfg: DictConfig, layers: tuple[str, ...] = LAYERS) -> RunReport:
    """Run the pipeline for the tenant(s) selected in the configuration."""
    run_id = uuid.uuid4().hex
    report = RunReport(run_id=run_id)

    if cfg.execution.tenant == "all":
        tenants = configured_tenants(cfg)
    else:
        tenants = [str(cfg.execution.tenant)]

    spark = get_spark(cfg)
    try:
        for tenant in tenants:
            try:
                run_tenant(spark, cfg, tenant, run_id, layers)
                report.outcomes.append(TenantOutcome(tenant, "ok"))
            except Exception as exc:  # noqa: BLE001 - deliberate per-tenant isolation
                log.error("Tenant %s failed: %s", tenant, exc)
                report.outcomes.append(TenantOutcome(tenant, "failed", str(exc)))
                if cfg.execution.fail_fast:
                    raise
    finally:
        spark.stop()

    if report.failed:
        log.error(
            "Run %s finished with %d failed tenant(s): %s",
            run_id,
            len(report.failed),
            ", ".join(o.tenant for o in report.failed),
        )
    else:
        log.info("Run %s finished successfully for %d tenant(s)", run_id, len(tenants))
    return report

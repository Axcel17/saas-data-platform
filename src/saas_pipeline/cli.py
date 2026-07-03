"""Command-line entry point.

Examples:
    saas-pipeline --env dev --tenant sv --start-date 2025-03-01 --end-date 2025-03-31
    saas-pipeline --env dev --tenant all --layer silver
    saas-pipeline --env main --tenant all           # fail_fast + fail_on_critical
"""

from __future__ import annotations

import argparse
import sys

from saas_pipeline.config import load_config
from saas_pipeline.logging_config import get_logger
from saas_pipeline.pipeline import LAYERS, run

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="saas-pipeline",
        description="Multi-tenant Medallion pipeline (Bronze/Silver/Gold).",
    )
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "main"])
    parser.add_argument(
        "--tenant", default=None, help="Tenant code (e.g. sv) or 'all'. Overrides config."
    )
    parser.add_argument("--start-date", default=None, help="Lower bound fecha_proceso, YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Upper bound fecha_proceso, YYYY-MM-DD.")
    parser.add_argument(
        "--layer",
        default="all",
        choices=["bronze", "silver", "gold", "all"],
        help="Layer to run; 'all' runs the full pipeline.",
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Unity Catalog catalog name to write UC tables instead of local paths.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="With --tenant all, abort the whole run if any tenant fails.",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Abort before Gold if a critical quality check fails.",
    )
    return parser


def _overrides(args: argparse.Namespace) -> dict:
    overrides: dict = {}
    if args.tenant is not None:
        overrides["execution.tenant"] = args.tenant
    if args.start_date is not None:
        overrides["execution.start_date"] = args.start_date
    if args.end_date is not None:
        overrides["execution.end_date"] = args.end_date
    if args.fail_fast:
        overrides["execution.fail_fast"] = True
    if args.fail_on_critical:
        overrides["quality.fail_on_critical"] = True
    if args.catalog is not None:
        overrides["storage.catalog"] = args.catalog
    return overrides


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Tenant file is merged only for a concrete tenant, not for "all".
    tenant_for_config = args.tenant if args.tenant not in (None, "all") else None
    cfg = load_config(env=args.env, tenant=tenant_for_config, overrides=_overrides(args))

    layers = LAYERS if args.layer == "all" else (args.layer,)
    report = run(cfg, layers)
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())

"""Hierarchical configuration loading with OmegaConf.

Configuration is resolved by merging three layers, in increasing precedence:

1. ``config/base.yaml``            - shared defaults.
2. ``config/env/<env>.yaml``       - per-environment overrides (dev/qa/main).
3. ``config/tenants/<tenant>.yaml`` - per-tenant overrides (skipped for "all").

CLI overrides are applied last, on top of the merged result. This mirrors the
layout described in section 5.8 of the architecture and keeps environment- and
tenant-specific values out of the code.
"""

from __future__ import annotations

from pathlib import Path

from omegaconf import DictConfig, OmegaConf

# Repository root, resolved relative to this file so the loader works regardless
# of the current working directory (no hardcoded absolute paths).
REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"

VALID_ENVS = ("dev", "qa", "main")


def _read_yaml(path: Path) -> DictConfig:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    return OmegaConf.load(path)


def load_config(
    env: str = "dev",
    tenant: str | None = None,
    overrides: dict | None = None,
) -> DictConfig:
    """Build the effective configuration for a run.

    Args:
        env: Target environment (dev/qa/main).
        tenant: A single tenant code to merge its file, or None/"all" to skip
            tenant-level overrides (used when iterating over every tenant).
        overrides: Flat dotlist-style overrides applied with highest precedence,
            e.g. ``{"execution.tenant": "sv", "execution.start_date": "2025-03-01"}``.

    Returns:
        A resolved ``DictConfig``.
    """
    if env not in VALID_ENVS:
        raise ValueError(f"Unknown env '{env}'. Expected one of {VALID_ENVS}.")

    cfg = _read_yaml(CONFIG_DIR / "base.yaml")
    cfg = OmegaConf.merge(cfg, _read_yaml(CONFIG_DIR / "env" / f"{env}.yaml"))

    if tenant and tenant != "all":
        tenant_file = CONFIG_DIR / "tenants" / f"{tenant}.yaml"
        if tenant_file.exists():
            cfg = OmegaConf.merge(cfg, _read_yaml(tenant_file))

    if overrides:
        dotlist = [f"{k}={_to_yaml_scalar(v)}" for k, v in overrides.items()]
        cfg = OmegaConf.merge(cfg, OmegaConf.from_dotlist(dotlist))

    # Resolve interpolations (${data_root}) eagerly so downstream code sees
    # plain strings and fails fast on a bad reference.
    OmegaConf.resolve(cfg)
    return cfg


def _to_yaml_scalar(value: object) -> str:
    """Render a Python value for an OmegaConf dotlist entry.

    None must become the literal ``null`` so it is parsed as a missing value
    rather than the string "None".
    """
    if value is None:
        return "null"
    return str(value)


def configured_tenants(cfg: DictConfig) -> list[str]:
    """Return the list of tenant codes registered in the configuration."""
    return [str(t) for t in cfg.tenants]

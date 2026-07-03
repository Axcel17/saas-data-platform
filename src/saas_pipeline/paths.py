"""Table locations for both storage modes.

Each logical table resolves to a :class:`Location` that is either a filesystem
path (local mode) or a Unity Catalog identifier (when ``storage.catalog`` is
set). The two are 1:1: ``data/<layer>/<tenant>/<table>`` maps to
``<catalog>.<layer>_<tenant>.<table>``, exactly the migration the architecture
describes (section 5.2).
"""

from __future__ import annotations

from dataclasses import dataclass

from omegaconf import DictConfig


@dataclass(frozen=True)
class Location:
    """Where a Delta table lives: a UC identifier or a filesystem path."""

    is_table: bool
    ref: str


def _catalog(cfg: DictConfig) -> str | None:
    return cfg.storage.catalog


def table_location(cfg: DictConfig, layer: str, tenant: str, table: str) -> Location:
    catalog = _catalog(cfg)
    if catalog:
        return Location(True, f"{catalog}.{layer}_{tenant}.{table}")
    return Location(False, f"{cfg.paths[layer]}/{tenant}/{table}")


def quarantine_location(cfg: DictConfig, layer: str, tenant: str, table: str) -> Location:
    catalog = _catalog(cfg)
    if catalog:
        return Location(True, f"{catalog}.{layer}_quarantine_{tenant}.{table}")
    return Location(False, f"{cfg.paths.quarantine_root}/{layer}_quarantine/{tenant}/{table}")


def quality_logs_location(cfg: DictConfig) -> Location:
    catalog = _catalog(cfg)
    if catalog:
        return Location(True, f"{catalog}.shared.quality_logs")
    return Location(False, str(cfg.paths.quality_logs))

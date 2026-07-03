"""Configuration-loading tests (also satisfies the CI 'YAML loads' requirement)."""

from __future__ import annotations

import pytest

from saas_pipeline.config import VALID_ENVS, configured_tenants, load_config


@pytest.mark.parametrize("env", VALID_ENVS)
def test_every_env_loads_and_relocates_paths(env):
    cfg = load_config(env=env)
    assert cfg.env == env
    # Paths must resolve (no dangling ${data_root}) and be scoped to the env.
    assert cfg.paths.bronze == f"data/{env}/bronze"
    assert cfg.paths.silver == f"data/{env}/silver"
    assert cfg.paths.quality_logs == f"data/{env}/shared/quality_logs"


def test_every_configured_tenant_file_loads():
    base = load_config(env="dev")
    for tenant in configured_tenants(base):
        cfg = load_config(env="dev", tenant=tenant)
        assert cfg.tenant_profile.id == tenant


def test_cli_overrides_take_precedence():
    cfg = load_config(
        env="dev",
        tenant="sv",
        overrides={"execution.tenant": "sv", "execution.start_date": "2025-03-01"},
    )
    assert cfg.execution.tenant == "sv"
    assert cfg.execution.start_date == "2025-03-01"
    # Unset override stays null, not the string "None".
    assert cfg.execution.end_date is None


def test_unknown_env_raises():
    with pytest.raises(ValueError):
        load_config(env="staging")

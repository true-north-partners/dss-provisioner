"""YAML configuration loading and convenience plan/apply API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import SecretStr

from dss_provisioner.config.loader import ConfigError, load_config
from dss_provisioner.config.registry import default_registry
from dss_provisioner.config.schema import Config, ProviderConfig
from dss_provisioner.core.provider import ApiKeyAuth, DSSProvider
from dss_provisioner.engine.engine import DSSEngine
from dss_provisioner.resources.loader import resolve_code_files

if TYPE_CHECKING:
    from pathlib import Path

    from dss_provisioner.engine.types import ApplyResult, Plan

__all__ = [
    "Config",
    "ConfigError",
    "ProviderConfig",
    "apply",
    "load",
    "load_config",
    "plan",
    "plan_and_apply",
]


def load(path: Path | str) -> Config:
    """Load a YAML configuration file."""
    return load_config(path)


def _engine_from_config(config: Config) -> DSSEngine:
    """Build a ``DSSEngine`` from a ``Config`` instance."""
    auth = (
        ApiKeyAuth(api_key=SecretStr(config.provider.api_key)) if config.provider.api_key else None
    )
    provider = DSSProvider(host=config.provider.host, auth=auth)
    return DSSEngine(
        provider=provider,
        project_key=config.provider.project,
        state_path=config.state_path,
        registry=default_registry(),
    )


def plan(config: Config, *, destroy: bool = False, refresh: bool = True) -> Plan:
    """Plan changes for the given configuration."""
    resources = resolve_code_files(config.resources, config.config_dir)
    engine = _engine_from_config(config)
    return engine.plan(resources, destroy=destroy, refresh=refresh)


def apply(plan_obj: Plan, config: Config) -> ApplyResult:
    """Apply a previously computed plan."""
    engine = _engine_from_config(config)
    return engine.apply(plan_obj)


def plan_and_apply(config: Config, *, destroy: bool = False, refresh: bool = True) -> ApplyResult:
    """Plan and apply in one step."""
    plan_obj = plan(config, destroy=destroy, refresh=refresh)
    return apply(plan_obj, config)

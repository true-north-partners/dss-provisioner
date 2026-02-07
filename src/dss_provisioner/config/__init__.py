"""YAML configuration loading and convenience plan/apply API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import SecretStr

from dss_provisioner.config.loader import ConfigError, load_config
from dss_provisioner.config.registry import default_registry
from dss_provisioner.config.schema import Config, ProviderConfig
from dss_provisioner.core.provider import ApiKeyAuth, DSSProvider
from dss_provisioner.core.state import State
from dss_provisioner.engine.engine import DSSEngine, ProgressCallback
from dss_provisioner.engine.types import Action, ResourceChange
from dss_provisioner.resources.loader import resolve_code_files

if TYPE_CHECKING:
    from pathlib import Path

    from dss_provisioner.engine.types import ApplyResult, Plan

__all__ = [
    "Config",
    "ConfigError",
    "ProviderConfig",
    "State",
    "apply",
    "drift",
    "load",
    "load_config",
    "plan",
    "plan_and_apply",
    "refresh",
    "save_state",
]


def load(path: Path | str) -> Config:
    """Load a YAML configuration file."""
    return load_config(path)


def _engine_from_config(config: Config) -> DSSEngine:
    """Build a ``DSSEngine`` from a ``Config`` instance."""
    if not config.provider.host:
        raise ConfigError("provider.host is required (set in YAML or DSS_HOST env var)")
    if not config.provider.api_key:
        raise ConfigError("provider.api_key is required (set DSS_API_KEY env var)")
    auth = ApiKeyAuth(api_key=SecretStr(config.provider.api_key))
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


def apply(
    plan_obj: Plan, config: Config, *, progress: ProgressCallback | None = None
) -> ApplyResult:
    """Apply a previously computed plan."""
    engine = _engine_from_config(config)
    return engine.apply(plan_obj, progress=progress)


def plan_and_apply(config: Config, *, destroy: bool = False, refresh: bool = True) -> ApplyResult:
    """Plan and apply in one step."""
    plan_obj = plan(config, destroy=destroy, refresh=refresh)
    return apply(plan_obj, config)


def refresh(config: Config) -> tuple[list[ResourceChange], State]:
    """Refresh state from the live DSS instance (not persisted).

    Returns the list of drift changes and the new state. Call
    :func:`save_state` to persist the returned state to disk.
    """
    engine = _engine_from_config(config)
    old_state = State.load_or_create(config.state_path, config.provider.project)
    new_state = engine.refresh()
    return _build_drift_changes(old_state, new_state), new_state


def save_state(config: Config, state: State) -> None:
    """Persist state to disk."""
    from dss_provisioner.engine.lock import StateLock

    with StateLock(config.state_path):
        state.serial += 1
        state.save(config.state_path)


def drift(config: Config) -> list[ResourceChange]:
    """Detect drift between state file and live DSS."""
    changes, _ = refresh(config)
    return changes


def _build_drift_changes(old_state: State, new_state: State) -> list[ResourceChange]:
    """Compare old vs new state and return a list of drift changes."""
    old_attrs = {addr: inst.attributes.copy() for addr, inst in old_state.resources.items()}
    changes: list[ResourceChange] = []
    for addr, inst in new_state.resources.items():
        old = old_attrs.get(addr)
        if old is None:
            continue
        if old != inst.attributes:
            all_keys = set(old) | set(inst.attributes)
            diff = {
                k: {"from": old.get(k), "to": inst.attributes.get(k)}
                for k in all_keys
                if old.get(k) != inst.attributes.get(k)
            }
            changes.append(
                ResourceChange(
                    address=addr,
                    resource_type=inst.resource_type,
                    action=Action.UPDATE,
                    prior=old,
                    planned=dict(inst.attributes),
                    diff=diff,
                )
            )
    for addr in set(old_attrs) - set(new_state.resources):
        changes.append(
            ResourceChange(
                address=addr,
                resource_type=old_state.resources[addr].resource_type
                if addr in old_state.resources
                else "unknown",
                action=Action.DELETE,
                prior=old_attrs[addr],
            )
        )
    return changes

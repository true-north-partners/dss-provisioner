"""YAML configuration file loader."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dotenv import dotenv_values
from pydantic import ValidationError
from ruamel.yaml import YAML
from ruamel.yaml.constructor import SafeConstructor

from dss_provisioner.config.modules import ModuleExpansionError, expand_modules
from dss_provisioner.config.schema import Config

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dss_provisioner.resources.base import Resource


class ConfigError(Exception):
    """Raised for configuration loading / validation errors."""


# Field name → environment variable.
_PROVIDER_ENV_MAP: dict[str, str] = {
    "host": "DSS_HOST",
    "api_key": "DSS_API_KEY",
    "project": "DSS_PROJECT",
    "verify_ssl": "DSS_VERIFY_SSL",
}

_PROVIDER_BOOL_FIELDS: frozenset[str] = frozenset({"verify_ssl"})


def _resolve_provider(raw_provider: dict[str, Any], config_dir: Path) -> dict[str, Any]:
    """Resolve provider fields from YAML, env vars, and ``.env`` file.

    Priority (highest wins): YAML value > env var > ``.env`` file.
    """
    env_file = config_dir / ".env"
    dotenv_vals = dotenv_values(env_file, encoding="utf-8-sig") if env_file.is_file() else {}

    resolved: dict[str, Any] = {}
    for field, env_key in _PROVIDER_ENV_MAP.items():
        val = raw_provider.get(field)
        if val is None:
            val = os.environ.get(env_key)
        if val is None:
            val = dotenv_vals.get(env_key)
        if val is not None:
            if field in _PROVIDER_BOOL_FIELDS and isinstance(val, str):
                if val.lower() not in SafeConstructor.bool_values:
                    raise ConfigError(f"Invalid boolean for {env_key}: {val!r}")
                val = SafeConstructor.bool_values[val.lower()]
            resolved[field] = val

    return resolved


def _validate_unique_names(resources: list[Resource]) -> list[str]:
    """Check that no two resources share the same name within a DSS namespace."""
    groups: dict[str, dict[str, str]] = {}  # namespace → {name: first_address}
    errors: list[str] = []
    for r in resources:
        seen = groups.setdefault(r.namespace, {})
        if r.name in seen:
            errors.append(
                f"Duplicate {r.namespace} name '{r.name}': "
                f"found in both {seen[r.name]} and {r.address}"
            )
        else:
            seen[r.name] = r.address
    return errors


def load_config(path: Path | str) -> Config:
    """Load a YAML configuration file and return a ``Config`` object.

    Raises:
        ConfigError: On YAML parse errors, missing sections, or validation failures.
    """
    path = Path(path)

    try:
        raw = YAML(typ="safe").load(path)
    except Exception as exc:
        raise ConfigError(f"Failed to read {path}: {exc}") from exc

    try:
        raw["provider"] = _resolve_provider(raw.get("provider", {}), path.parent)
        config = Config.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(str(exc)) from exc

    config.config_dir = path.parent

    if config.modules:
        logger.debug("Expanding %d module(s)", len(config.modules))
        try:
            config._module_resources = expand_modules(config.modules, config.config_dir)
        except ModuleExpansionError as exc:
            raise ConfigError(str(exc)) from exc

    errors = _validate_unique_names(config.resources)
    if errors:
        raise ConfigError("\n".join(errors))

    logger.info("Loaded config from %s (%d resources)", path, len(config.resources))
    return config

"""YAML configuration file loader."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ValidationError
from ruamel.yaml import YAML

from dss_provisioner.config.modules import ModuleExpansionError, expand_modules
from dss_provisioner.config.schema import Config, ProviderConfig

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dss_provisioner.resources.base import Resource


class ConfigError(Exception):
    """Raised for configuration loading / validation errors."""


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
        env_file = path.parent / ".env"
        raw["provider"] = ProviderConfig(
            _env_file=env_file if env_file.is_file() else None,  # type: ignore[call-arg]
            **raw.get("provider", {}),
        )
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

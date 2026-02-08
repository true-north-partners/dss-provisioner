"""YAML configuration file loader."""

from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError
from ruamel.yaml import YAML

from dss_provisioner.config.modules import ModuleExpansionError, expand_modules
from dss_provisioner.config.schema import Config


class ConfigError(Exception):
    """Raised for configuration loading / validation errors."""


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
        config = Config.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(str(exc)) from exc

    config.config_dir = path.parent

    if config.modules:
        try:
            config._module_resources = expand_modules(config.modules, config.config_dir)
        except ModuleExpansionError as exc:
            raise ConfigError(str(exc)) from exc

    return config

"""Shared fixtures for unit tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dss_provisioner.config import load

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from dss_provisioner.config.schema import Config

_DSS_ENV_VARS = ("DSS_HOST", "DSS_API_KEY", "DSS_PROJECT", "DSS_VERIFY_SSL", "DSS_LOG")


@pytest.fixture(autouse=True)
def _clean_dss_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove DSS_* env vars so unit tests don't leak host config."""
    for var in _DSS_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def make_config(tmp_path: Path) -> Callable[..., Config]:
    """Factory fixture: write YAML + optional .env, return loaded Config."""

    def _make(yaml_str: str, *, dotenv: str | None = None) -> Config:
        (tmp_path / "config.yaml").write_text(yaml_str)
        if dotenv is not None:
            (tmp_path / ".env").write_text(dotenv)
        return load(tmp_path / "config.yaml")

    return _make

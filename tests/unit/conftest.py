"""Shared fixtures for unit tests."""

import pytest

_DSS_ENV_VARS = ("DSS_HOST", "DSS_API_KEY", "DSS_PROJECT", "DSS_LOG")


@pytest.fixture(autouse=True)
def _clean_dss_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove DSS_* env vars so unit tests don't leak host config."""
    for var in _DSS_ENV_VARS:
        monkeypatch.delenv(var, raising=False)

"""Shared fixtures for e2e integration tests against a live DSS instance."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

import dataikuapi
import pytest

from dss_provisioner.config.schema import Config, ProviderConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from dataikuapi.dss.project import DSSProject

logger = logging.getLogger(__name__)

_KEYRING_SERVICE = "dss-provisioner-e2e"

# ---------------------------------------------------------------------------
# DSS discovery helpers
# ---------------------------------------------------------------------------


def _resolve_host(config: pytest.Config) -> str:
    return (
        config.getoption("--e2e-host", default=None)
        or os.environ.get("DSS_HOST", "http://localhost:11200")
    ).rstrip("/")


def _find_dsscli() -> str | None:
    """Find the dsscli binary: $PATH → well-known DSS home locations."""
    found = shutil.which("dsscli")
    if found:
        return found
    candidates = [
        Path(os.environ["DIP_HOME"]) / "bin" / "dsscli" if "DIP_HOME" in os.environ else None,
        Path.home() / "Library" / "DataScienceStudio" / "dss_home" / "bin" / "dsscli",  # macOS
        Path.home() / "dss_home" / "bin" / "dsscli",  # Linux default
    ]
    for path in candidates:
        if path and path.is_file():
            return str(path)
    return None


def _provision_api_key() -> str | None:
    """Provision an admin API key via dsscli. Returns the key or None."""
    dsscli = _find_dsscli()
    if not dsscli:
        return None
    try:
        result = subprocess.run(
            [
                dsscli,
                "api-key-create",
                "--output",
                "json",
                "--label",
                "provisioner-e2e",
                "--admin",
                "true",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(result.stdout)
        # dsscli returns a JSON array with one element
        if isinstance(data, list):
            data = data[0]
        return data["key"]
    except (subprocess.CalledProcessError, KeyError, json.JSONDecodeError, IndexError):
        return None


def _keyring_get(host: str) -> str | None:
    """Read API key from keyring, or None if unavailable."""
    with contextlib.suppress(Exception):
        import keyring

        return keyring.get_password(_KEYRING_SERVICE, host)
    return None


def _keyring_set(host: str, key: str) -> None:
    """Store API key in keyring (best-effort)."""
    with contextlib.suppress(Exception):
        import keyring

        keyring.set_password(_KEYRING_SERVICE, host, key)


def _is_community_edition(host: str, api_key: str) -> bool | None:
    """Return True if community, False if enterprise, None if undetermined."""
    try:
        status = dataikuapi.DSSClient(host, api_key).get_licensing_status()
        return not status.get("ceEnterprise", False)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# pytest CLI options + hooks
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("e2e", "DSS e2e integration test options")
    group.addoption(
        "--e2e-host",
        default=None,
        help="DSS host URL (default: DSS_HOST env → http://localhost:11200)",
    )
    group.addoption(
        "--e2e-project",
        default=None,
        help="DSS project key (default: DSS_PROJECT env → TEST)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-skip @pytest.mark.enterprise tests on community edition."""
    if not any("enterprise" in item.keywords for item in items):
        return
    host = _resolve_host(config)
    api_key = os.environ.get("DSS_API_KEY") or _keyring_get(host) or _provision_api_key()
    is_community = _is_community_edition(host, api_key) if api_key else None
    if is_community is True:
        skip = pytest.mark.skip(reason="requires enterprise DSS edition")
        for item in items:
            if "enterprise" in item.keywords:
                item.add_marker(skip)


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def dss_host(request: pytest.FixtureRequest) -> str:
    return _resolve_host(request.config)


@pytest.fixture(scope="session")
def dss_api_key(dss_host: str) -> str:
    """Resolve API key: DSS_API_KEY env → keyring → dsscli provisioning."""
    # 1. Explicit env var (CI / override)
    key = os.environ.get("DSS_API_KEY")
    if key:
        return key

    # 2. Keyring lookup (validate stored key still works)
    stored = _keyring_get(dss_host)
    if stored:
        try:
            dataikuapi.DSSClient(dss_host, stored).get_auth_info()
            return stored
        except Exception:
            logger.info("Stored keyring API key is invalid, re-provisioning")

    # 3. Provision via dsscli (admin key needed for cross-resource-type operations)
    provisioned = _provision_api_key()
    if provisioned:
        _keyring_set(dss_host, provisioned)
        return provisioned

    pytest.skip("No API key available: set DSS_API_KEY env var or install dsscli")
    return ""  # unreachable; pytest.skip raises


@pytest.fixture(scope="session")
def dss_client(dss_host: str, dss_api_key: str) -> dataikuapi.DSSClient:
    client = dataikuapi.DSSClient(dss_host, dss_api_key)
    try:
        client.get_auth_info()
    except Exception as exc:
        pytest.skip(f"DSS not reachable at {dss_host}: {exc}")
    return client


@pytest.fixture(scope="session")
def dss_is_community(dss_host: str, dss_api_key: str) -> bool | None:
    """True if community, False if enterprise, None if undetermined."""
    return _is_community_edition(dss_host, dss_api_key)


@pytest.fixture(scope="session")
def dss_project_key(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--e2e-project") or os.environ.get("DSS_PROJECT", "TEST")


@pytest.fixture(scope="session")
def dss_project(dss_client: dataikuapi.DSSClient, dss_project_key: str) -> DSSProject:
    return dss_client.get_project(dss_project_key)


# ---------------------------------------------------------------------------
# Cleanup fixtures (function-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture()
def cleanup_datasets(dss_project: DSSProject) -> Generator[list[str]]:
    created: list[str] = []
    yield created
    for name in reversed(created):
        with contextlib.suppress(Exception):
            dss_project.get_dataset(name).delete()


@pytest.fixture()
def cleanup_recipes(dss_project: DSSProject) -> Generator[list[str]]:
    created: list[str] = []
    yield created
    for name in reversed(created):
        with contextlib.suppress(Exception):
            dss_project.get_recipe(name).delete()


@pytest.fixture()
def cleanup_managed_folders(dss_project: DSSProject) -> Generator[list[str]]:
    created: list[str] = []
    yield created
    for name in reversed(created):
        with contextlib.suppress(Exception):
            dss_project.get_managed_folder(name).delete()


@pytest.fixture()
def cleanup_scenarios(dss_project: DSSProject) -> Generator[list[str]]:
    created: list[str] = []
    yield created
    for name in reversed(created):
        with contextlib.suppress(Exception):
            dss_project.get_scenario(name).delete()


@pytest.fixture()
def cleanup_zones(dss_project: DSSProject) -> Generator[list[str]]:
    created: list[str] = []
    yield created
    for zone_id in reversed(created):
        with contextlib.suppress(Exception):
            dss_project.get_flow().get_zone(zone_id).delete()


# ---------------------------------------------------------------------------
# Config factory
# ---------------------------------------------------------------------------


@pytest.fixture()
def make_config(
    dss_host: str, dss_api_key: str, dss_project_key: str, tmp_path: Path
) -> Callable[..., Config]:
    def _make(
        *,
        datasets: list[Any] | None = None,
        recipes: list[Any] | None = None,
        managed_folders: list[Any] | None = None,
        scenarios: list[Any] | None = None,
        zones: list[Any] | None = None,
        variables: Any | None = None,
        state_name: str = "state",
    ) -> Config:
        state_path = tmp_path / f".{state_name}.json"
        return Config(
            provider=ProviderConfig(host=dss_host, api_key=dss_api_key, project=dss_project_key),
            state_path=state_path,
            datasets=datasets or [],
            recipes=recipes or [],
            managed_folders=managed_folders or [],
            scenarios=scenarios or [],
            zones=zones or [],
            variables=variables,
        )

    return _make


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def assert_changes(plan_obj: Any, expected: dict[str, str]) -> None:
    """Assert that a plan contains exactly the expected actions — no more, no less.

    *expected* maps resource names to Action values (e.g. ``{"my_ds": "create"}``).
    """
    from dss_provisioner.engine.types import Action

    actual = {c.address.split(".")[-1]: c.action for c in plan_obj.changes}
    normalized = {
        name: (Action(action) if isinstance(action, str) else action)
        for name, action in expected.items()
    }
    assert actual == normalized, (
        f"Plan changes mismatch.\nExpected: {normalized}\nActual:   {actual}"
    )

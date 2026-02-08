"""Shared fixtures for e2e integration tests against a live DSS instance."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import subprocess
from typing import TYPE_CHECKING, Any

import dataikuapi
import pytest

from dss_provisioner.config.schema import Config, ProviderConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

    from dataikuapi.dss.project import DSSProject

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# pytest CLI options
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


# ---------------------------------------------------------------------------
# Auto-skip enterprise tests on community edition
# ---------------------------------------------------------------------------


def _resolve_host(config: pytest.Config) -> str:
    return (
        config.getoption("--e2e-host", default=None)
        or os.environ.get("DSS_HOST", "http://localhost:11200")
    ).rstrip("/")


def _resolve_api_key_for_collection(host: str) -> str | None:
    """Best-effort API key resolution at collection time (env → keyring)."""
    key = os.environ.get("DSS_API_KEY")
    if key:
        return key
    with contextlib.suppress(Exception):
        import keyring

        return keyring.get_password("dss-provisioner-e2e", host)
    return None


def _is_community_edition(host: str, api_key: str) -> bool:
    try:
        client = dataikuapi.DSSClient(host, api_key)
        status = client.get_licensing_status()
        return not status.get("ceEnterprise", False)
    except Exception:
        return True


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not any("enterprise" in item.keywords for item in items):
        return
    host = _resolve_host(config)
    api_key = _resolve_api_key_for_collection(host)
    if api_key is None or _is_community_edition(host, api_key):
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
    # 1. Explicit env var (CI / override)
    key = os.environ.get("DSS_API_KEY")
    if key:
        return key

    # 2. Keyring lookup
    with contextlib.suppress(Exception):
        import keyring

        stored = keyring.get_password("dss-provisioner-e2e", dss_host)
        if stored:
            try:
                dataikuapi.DSSClient(dss_host, stored).get_auth_info()
                return stored
            except Exception:
                logger.info("Stored keyring API key is invalid, re-provisioning")

    # 3. Provision via dsscli
    result = subprocess.run(
        [
            "dsscli",
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
    key = json.loads(result.stdout)["key"]

    # Store in keyring for next run
    with contextlib.suppress(Exception):
        import keyring

        keyring.set_password("dss-provisioner-e2e", dss_host, key)

    return key


@pytest.fixture(scope="session")
def dss_client(dss_host: str, dss_api_key: str) -> dataikuapi.DSSClient:
    client = dataikuapi.DSSClient(dss_host, dss_api_key)
    try:
        client.get_auth_info()
    except Exception as exc:
        pytest.skip(f"DSS not reachable at {dss_host}: {exc}")
    return client


@pytest.fixture(scope="session")
def dss_is_community(dss_client: dataikuapi.DSSClient) -> bool:
    try:
        status = dss_client.get_licensing_status()
        return not status.get("ceEnterprise", False)
    except Exception:
        return True


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
    """Assert that a plan contains exactly the expected actions for the given resource names.

    *expected* maps resource names to Action values (e.g. ``{"my_ds": "create"}``).
    """
    from dss_provisioner.engine.types import Action

    actual = {c.address.split(".")[-1]: c.action for c in plan_obj.changes}
    for name, action in expected.items():
        act = Action(action) if isinstance(action, str) else action
        assert actual.get(name) == act, (
            f"Expected {name}={act}, got {actual.get(name)}. Full plan: {actual}"
        )

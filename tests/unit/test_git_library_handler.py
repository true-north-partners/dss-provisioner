"""Tests for the GitLibraryHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.git_library_handler import GitLibraryHandler
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.git_library import GitLibraryResource

if TYPE_CHECKING:
    from pathlib import Path


_LIB_DATA: dict[str, Any] = {
    "gitReferences": {
        "shared_utils": {
            "repository": "git@github.com:org/lib.git",
            "checkout": "main",
            "pathInGitRepository": "python",
        },
    },
    "pythonPath": ["shared_utils"],
    "rsrcPath": [],
    "importLibrariesFromProjects": [],
}


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def ctx(mock_client: MagicMock) -> EngineContext:
    provider = DSSProvider.from_client(mock_client)
    return EngineContext(provider=provider, project_key="PRJ")


@pytest.fixture
def handler() -> GitLibraryHandler:
    return GitLibraryHandler()


@pytest.fixture
def mock_git(mock_client: MagicMock) -> MagicMock:
    git = MagicMock()
    project = MagicMock()
    project.get_project_git.return_value = git
    mock_client.get_project.return_value = project
    return git


class TestCreate:
    def test_calls_add_library(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        future = MagicMock()
        mock_git.add_library.return_value = future
        mock_git.list_libraries.return_value = _LIB_DATA

        desired = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
            checkout="main",
            path="python",
        )
        handler.create(ctx, desired)

        mock_git.add_library.assert_called_once_with(
            repository="git@github.com:org/lib.git",
            local_target_path="shared_utils",
            checkout="main",
            path_in_git_repository="python",
            add_to_python_path=True,
        )
        future.wait_for_result.assert_called_once()

    def test_returns_read_attrs(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        future = MagicMock()
        mock_git.add_library.return_value = future
        mock_git.list_libraries.return_value = _LIB_DATA

        desired = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
            checkout="main",
            path="python",
        )
        result = handler.create(ctx, desired)

        assert result["name"] == "shared_utils"
        assert result["repository"] == "git@github.com:org/lib.git"
        assert result["checkout"] == "main"
        assert result["path"] == "python"
        assert result["add_to_python_path"] is True

    def test_raises_when_not_found_after_create(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        future = MagicMock()
        mock_git.add_library.return_value = future
        mock_git.list_libraries.return_value = {"gitReferences": {}, "pythonPath": []}

        desired = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
        )
        with pytest.raises(RuntimeError, match="not found after create"):
            handler.create(ctx, desired)


class TestRead:
    def test_returns_current_state(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        mock_git.list_libraries.return_value = _LIB_DATA

        prior = ResourceInstance(
            address="dss_git_library.shared_utils",
            resource_type="dss_git_library",
            name="shared_utils",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["name"] == "shared_utils"
        assert result["repository"] == "git@github.com:org/lib.git"
        assert result["checkout"] == "main"
        assert result["path"] == "python"
        assert result["add_to_python_path"] is True

    def test_returns_python_path_false_when_not_in_list(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        data = {
            "gitReferences": {
                "shared_utils": {
                    "repository": "git@github.com:org/lib.git",
                    "checkout": "main",
                    "pathInGitRepository": "",
                },
            },
            "pythonPath": [],  # name NOT in list
        }
        mock_git.list_libraries.return_value = data

        prior = ResourceInstance(
            address="dss_git_library.shared_utils",
            resource_type="dss_git_library",
            name="shared_utils",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["add_to_python_path"] is False

    def test_returns_none_when_missing(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        mock_git.list_libraries.return_value = {
            "gitReferences": {},
            "pythonPath": [],
        }

        prior = ResourceInstance(
            address="dss_git_library.shared_utils",
            resource_type="dss_git_library",
            name="shared_utils",
        )
        result = handler.read(ctx, prior)
        assert result is None


class TestUpdate:
    def test_calls_set_library_and_reset(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        future = MagicMock()
        mock_git.reset_library.return_value = future
        mock_git.list_libraries.return_value = _LIB_DATA

        desired = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
            checkout="v2.0",
            path="python",
        )
        prior = ResourceInstance(
            address="dss_git_library.shared_utils",
            resource_type="dss_git_library",
            name="shared_utils",
        )
        handler.update(ctx, desired, prior)

        mock_git.set_library.assert_called_once_with(
            git_reference_path="shared_utils",
            remote="git@github.com:org/lib.git",
            remotePath="python",
            checkout="v2.0",
        )
        mock_git.reset_library.assert_called_once_with("shared_utils")
        future.wait_for_result.assert_called_once()

    def test_returns_updated_attrs(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        future = MagicMock()
        mock_git.reset_library.return_value = future

        updated_data = {
            "gitReferences": {
                "shared_utils": {
                    "repository": "git@github.com:org/lib.git",
                    "checkout": "v2.0",
                    "pathInGitRepository": "python",
                },
            },
            "pythonPath": ["shared_utils"],
        }
        mock_git.list_libraries.return_value = updated_data

        desired = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
            checkout="v2.0",
            path="python",
        )
        prior = ResourceInstance(
            address="dss_git_library.shared_utils",
            resource_type="dss_git_library",
            name="shared_utils",
        )
        result = handler.update(ctx, desired, prior)

        assert result["checkout"] == "v2.0"


class TestDelete:
    def test_removes_library(
        self,
        ctx: EngineContext,
        handler: GitLibraryHandler,
        mock_git: MagicMock,
    ) -> None:
        prior = ResourceInstance(
            address="dss_git_library.shared_utils",
            resource_type="dss_git_library",
            name="shared_utils",
        )
        handler.delete(ctx, prior)

        mock_git.remove_library.assert_called_once_with("shared_utils", delete_directory=True)


class TestEngineRoundtrip:
    def _setup_engine(
        self, tmp_path: Path, lib_data: dict[str, Any]
    ) -> tuple[DSSEngine, MagicMock]:
        mock_client = MagicMock()
        provider = DSSProvider.from_client(mock_client)

        project = MagicMock()
        mock_client.get_project.return_value = project

        git = MagicMock()
        project.get_project_git.return_value = git
        git.list_libraries.return_value = lib_data

        future = MagicMock()
        git.add_library.return_value = future
        git.reset_library.return_value = future

        registry = ResourceTypeRegistry()
        registry.register(GitLibraryResource, GitLibraryHandler())

        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )
        return engine, git

    def test_create_noop_update_delete_cycle(self, tmp_path: Path) -> None:
        engine, git = self._setup_engine(tmp_path, _LIB_DATA)

        lib = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
            checkout="main",
            path="python",
        )

        # --- CREATE ---
        plan = engine.plan([lib])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_git_library.shared_utils" in state.resources
        assert state.serial == 1

        # --- NOOP ---
        plan2 = engine.plan([lib])
        assert plan2.changes[0].action == Action.NOOP
        engine.apply(plan2)
        assert State.load(engine.state_path).serial == 1

        # --- UPDATE (change checkout) ---
        updated_data = {
            "gitReferences": {
                "shared_utils": {
                    "repository": "git@github.com:org/lib.git",
                    "checkout": "v2.0",
                    "pathInGitRepository": "python",
                },
            },
            "pythonPath": ["shared_utils"],
        }
        lib_v2 = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
            checkout="v2.0",
            path="python",
        )
        plan3 = engine.plan([lib_v2])
        assert plan3.changes[0].action == Action.UPDATE

        git.list_libraries.return_value = updated_data
        engine.apply(plan3)
        assert State.load(engine.state_path).serial == 2

        # --- DELETE ---
        plan4 = engine.plan([])
        assert any(c.action == Action.DELETE for c in plan4.changes)
        engine.apply(plan4)

        state4 = State.load(engine.state_path)
        assert state4.resources == {}

    def test_libraries_applied_before_datasets(self, tmp_path: Path) -> None:
        """Verify plan_priority ordering: libraries (10) before datasets (100)."""
        from dss_provisioner.engine.dataset_handler import DatasetHandler
        from dss_provisioner.resources.dataset import DatasetResource

        mock_client = MagicMock()
        provider = DSSProvider.from_client(mock_client)
        project = MagicMock()
        mock_client.get_project.return_value = project

        git = MagicMock()
        project.get_project_git.return_value = git
        git.list_libraries.return_value = _LIB_DATA

        future = MagicMock()
        git.add_library.return_value = future

        registry = ResourceTypeRegistry()
        registry.register(GitLibraryResource, GitLibraryHandler())
        registry.register(DatasetResource, DatasetHandler())

        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )

        lib = GitLibraryResource(
            name="shared_utils",
            repository="git@github.com:org/lib.git",
        )
        ds = DatasetResource(name="my_ds", type="Filesystem")

        # Dataset listed first, but library should be planned first due to priority
        plan = engine.plan([ds, lib])
        addrs = [c.address for c in plan.changes]
        assert addrs.index("dss_git_library.shared_utils") < addrs.index("dss_dataset.my_ds")

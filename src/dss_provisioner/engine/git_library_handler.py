"""Git library handler implementing CRUD via dataikuapi project Git API."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.handlers import ResourceHandler

if TYPE_CHECKING:
    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.git_library import GitLibraryResource

logger = logging.getLogger(__name__)


class GitLibraryHandler(ResourceHandler["GitLibraryResource"]):
    """CRUD handler for DSS Git library references."""

    def _get_git(self, ctx: EngineContext) -> Any:
        return ctx.provider.client.get_project(ctx.project_key).get_project_git()

    def _get_ref(self, ctx: EngineContext, name: str) -> dict[str, Any] | None:
        """Read a single git reference by local target path."""
        git = self._get_git(ctx)
        data = git.list_libraries()
        refs = data.get("gitReferences", {})
        if name not in refs:
            return None
        ref = refs[name]
        python_path = data.get("pythonPath", [])
        return {
            "name": name,
            "description": "",
            "tags": [],
            "repository": ref.get("repository", ""),
            "checkout": ref.get("checkout", ""),
            "path": ref.get("pathInGitRepository", ""),
            "add_to_python_path": name in python_path,
        }

    def create(self, ctx: EngineContext, desired: GitLibraryResource) -> dict[str, Any]:
        git = self._get_git(ctx)
        future = git.add_library(
            repository=desired.repository,
            local_target_path=desired.name,
            checkout=desired.checkout,
            path_in_git_repository=desired.path,
            add_to_python_path=desired.add_to_python_path,
        )
        future.wait_for_result()
        result = self._get_ref(ctx, desired.name)
        if result is None:
            msg = f"Library '{desired.name}' not found after create — clone may have failed"
            raise RuntimeError(msg)
        return result

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        return self._get_ref(ctx, prior.name)

    def update(
        self, ctx: EngineContext, desired: GitLibraryResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        current_python_path = prior.attributes.get("add_to_python_path", True)
        if desired.add_to_python_path != current_python_path:
            msg = (
                f"Cannot change 'add_to_python_path' on library '{desired.name}' "
                f"(from {current_python_path} to {desired.add_to_python_path}). "
                f"Delete and recreate the library to change this setting."
            )
            raise RuntimeError(msg)
        git = self._get_git(ctx)
        git.set_library(
            git_reference_path=desired.name,
            remote=desired.repository,
            remotePath=desired.path,
            checkout=desired.checkout,
        )
        future = git.reset_library(desired.name)
        future.wait_for_result()
        result = self._get_ref(ctx, desired.name)
        if result is None:
            msg = f"Library '{desired.name}' not found after update — reset may have failed"
            raise RuntimeError(msg)
        return result

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        git = self._get_git(ctx)
        git.remove_library(prior.name, delete_directory=True)

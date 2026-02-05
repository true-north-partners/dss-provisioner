"""Project-scoped convenience wrapper.

The engine is intentionally project-scoped (one project per state file). This
wrapper is for user code that wants the same ergonomics: bind to a project once,
then call dataset/recipe/zone operations without passing `project_key` around.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataikuapi.dss.dataset import DSSDataset
    from dataikuapi.dss.project import DSSProject
    from dataikuapi.dss.recipe import DSSRecipe

    from dss_provisioner.core.provider import DSSProvider


class ProjectScopedProvider:
    def __init__(self, provider: DSSProvider, project_key: str) -> None:
        self._provider = provider
        self._project_key = project_key

    @property
    def project_key(self) -> str:
        return self._project_key

    @cached_property
    def project(self) -> DSSProject:
        return self._provider.client.get_project(self._project_key)

    @cached_property
    def datasets(self) -> ProjectDatasets:
        return ProjectDatasets(self.project)

    @cached_property
    def recipes(self) -> ProjectRecipes:
        return ProjectRecipes(self.project)

    @cached_property
    def zones(self) -> ProjectZones:
        return ProjectZones(self.project)


class ProjectDatasets:
    def __init__(self, project: DSSProject) -> None:
        self._project = project

    def list_datasets(self) -> list[dict[str, Any]]:
        return self._project.list_datasets()

    def get(self, dataset_name: str) -> DSSDataset:
        return self._project.get_dataset(dataset_name)


class ProjectRecipes:
    def __init__(self, project: DSSProject) -> None:
        self._project = project

    def list_recipes(self) -> list[dict[str, Any]]:
        return self._project.list_recipes()

    def get(self, recipe_name: str) -> DSSRecipe:
        return self._project.get_recipe(recipe_name)


class ProjectZones:
    def __init__(self, project: DSSProject) -> None:
        self._project = project

    def list_zones(self) -> list[Any]:
        flow = self._project.get_flow()
        return flow.list_zones()

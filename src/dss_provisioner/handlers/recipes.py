"""Handler for DSS recipes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import dataikuapi
    from dataikuapi.dss.recipe import DSSRecipe


class RecipeHandler:
    """Handler for DSS recipe operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list_recipes(self, project_key: str) -> list[dict[str, Any]]:
        """List all recipes in a project."""
        project = self.client.get_project(project_key)
        return project.list_recipes()

    def get(self, project_key: str, recipe_name: str) -> DSSRecipe:
        """Get a recipe."""
        project = self.client.get_project(project_key)
        return project.get_recipe(recipe_name)

    # TODO: Implement create, update, delete when we have resource definitions

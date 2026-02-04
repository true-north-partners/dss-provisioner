"""Handler for DSS recipes."""

from typing import Any

import dataikuapi


class RecipeHandler:
    """Handler for DSS recipe operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list(self, project_key: str) -> list[dict[str, Any]]:
        """List all recipes in a project."""
        project = self.client.get_project(project_key)
        return project.list_recipes()

    def get(
        self, project_key: str, recipe_name: str
    ) -> dataikuapi.dss.recipe.DSSRecipe:
        """Get a recipe."""
        project = self.client.get_project(project_key)
        return project.get_recipe(recipe_name)

    # TODO: Implement create, update, delete when we have resource definitions

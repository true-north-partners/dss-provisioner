"""Recipe handler implementing CRUD via dataikuapi."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.handlers import ResourceHandler
from dss_provisioner.resources.recipe import PythonRecipeResource, SQLQueryRecipeResource

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dataikuapi.dss.project import DSSProject
    from dataikuapi.dss.recipe import DSSRecipe

    from dss_provisioner.core.state import ResourceInstance, State
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.base import Resource
    from dss_provisioner.resources.recipe import RecipeResource

logger = logging.getLogger(__name__)

# Dataset types that expose a SQL connection.
_SQL_DATASET_TYPES: set[str] = {"PostgreSQL", "Snowflake", "Oracle", "MySQL"}


def _is_sql_dataset(ref: str, all_desired: Mapping[str, Resource], state: State) -> bool:
    """Check whether *ref* names a dataset backed by a SQL connection."""
    # Check co-planned resources.
    for r in all_desired.values():
        if r.name == ref and getattr(r, "dataset_type", None) in _SQL_DATASET_TYPES:
            return True
    # Check existing resources in state.
    return any(
        inst.name == ref and inst.attributes.get("dataset_type") in _SQL_DATASET_TYPES
        for inst in state.resources.values()
    )


class RecipeHandler(ResourceHandler["RecipeResource"]):
    """Base CRUD handler for DSS recipes.

    Subclasses override hook methods to handle type-specific behaviour
    (code payloads, code environments, validation rules).
    """

    # -- DSS helpers ----------------------------------------------------------

    def _get_project(self, ctx: EngineContext) -> DSSProject:
        return ctx.provider.client.get_project(ctx.project_key)

    def _get_recipe(self, ctx: EngineContext, name: str) -> DSSRecipe:
        return self._get_project(ctx).get_recipe(name)

    def _recipe_exists(self, recipe: DSSRecipe) -> bool:
        """Check if a recipe exists (DSSRecipe has no .exists() method)."""
        try:
            recipe.get_metadata()
        except Exception:
            return False
        return True

    def _read_zone(self, ctx: EngineContext, recipe_name: str) -> str | None:
        """Read the flow zone of a recipe, or None if in the default zone."""
        try:
            flow = self._get_project(ctx).get_flow()
            for zone in flow.list_zones():
                graph = zone.get_graph()
                for node in graph.nodes.values():
                    if getattr(node, "name", None) == recipe_name:
                        zone_id = zone.id
                        if zone_id == "default":
                            return None
                        return zone_id
        except Exception:
            # Flow zones may not be available (e.g. free edition).
            return None
        return None

    def _apply_metadata(self, recipe: DSSRecipe, desired: RecipeResource) -> None:
        """Set description and tags via metadata."""
        meta = recipe.get_metadata()
        meta["description"] = desired.description
        meta["tags"] = list(desired.tags)
        recipe.set_metadata(meta)

    def _apply_zone(self, recipe: DSSRecipe, desired: RecipeResource) -> None:
        """Move recipe to a flow zone if specified."""
        if desired.zone is None:
            return
        recipe.move_to_zone(desired.zone)

    # -- Hook methods (override in subclasses) --------------------------------

    def _read_extra_attrs(self, settings: Any, raw_def: dict[str, Any]) -> dict[str, Any]:
        """Extra attributes to merge into read output. Default: empty."""
        _ = settings, raw_def
        return {}

    def _apply_type_settings(
        self,
        settings: Any,
        raw_def: dict[str, Any],
        desired: RecipeResource,
        *,
        creating: bool,
    ) -> bool:
        """Apply type-specific settings. Return True if settings.save() needed."""
        _ = settings, raw_def, desired, creating
        return False

    # -- Read -----------------------------------------------------------------

    def _read_attrs(
        self,
        ctx: EngineContext,
        recipe: DSSRecipe,
        name: str,
    ) -> dict[str, Any]:
        """Extract recipe attributes from DSS, keyed to match model_dump output."""
        settings = recipe.get_settings()
        raw_def = settings.get_recipe_raw_definition()
        meta = recipe.get_metadata()

        attrs: dict[str, Any] = {
            "name": name,
            "description": meta.get("description", ""),
            "tags": meta.get("tags", []),
            "recipe_type": raw_def.get("type", ""),
            "inputs": settings.get_flat_input_refs(),
            "outputs": settings.get_flat_output_refs(),
            "zone": self._read_zone(ctx, name),
        }

        attrs.update(self._read_extra_attrs(settings, raw_def))
        return attrs

    # -- Validation -----------------------------------------------------------

    def validate(self, ctx: EngineContext, desired: RecipeResource) -> list[str]:
        _ = ctx
        errors: list[str] = []
        if not desired.outputs:
            errors.append(f"Recipe '{desired.name}' requires at least one output dataset")
        return errors

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: RecipeResource,
        all_desired: Mapping[str, Resource],
        state: State,
    ) -> list[str]:
        _ = ctx, desired, all_desired, state
        return []

    # -- CRUD -----------------------------------------------------------------

    def create(self, ctx: EngineContext, desired: RecipeResource) -> dict[str, Any]:
        """Create a recipe in DSS."""
        project = self._get_project(ctx)

        builder = project.new_recipe(desired.recipe_type, desired.name)
        for ref in desired.inputs:
            builder.with_input(ref)
        for ref in desired.outputs:
            builder.with_output(ref)
        recipe = builder.create()

        settings = recipe.get_settings()
        raw_def = settings.get_recipe_raw_definition()
        needs_save = self._apply_type_settings(settings, raw_def, desired, creating=True)
        if needs_save:
            settings.save()

        self._apply_metadata(recipe, desired)
        self._apply_zone(recipe, desired)

        return self._read_attrs(ctx, recipe, desired.name)

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        """Read recipe from DSS. Returns None if deleted externally."""
        recipe = self._get_recipe(ctx, prior.name)
        if not self._recipe_exists(recipe):
            return None
        return self._read_attrs(ctx, recipe, prior.name)

    def update(
        self, ctx: EngineContext, desired: RecipeResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        """Update a recipe in DSS."""
        _ = prior
        recipe = self._get_recipe(ctx, desired.name)
        settings = recipe.get_settings()
        raw_def = settings.get_recipe_raw_definition()

        # Update inputs/outputs by modifying raw definition directly.
        raw_def["inputs"] = {"main": {"items": [{"ref": r} for r in desired.inputs]}}
        raw_def["outputs"] = {
            "main": {"items": [{"ref": r, "appendMode": False} for r in desired.outputs]}
        }

        self._apply_type_settings(settings, raw_def, desired, creating=False)
        settings.save()

        self._apply_metadata(recipe, desired)
        self._apply_zone(recipe, desired)

        return self._read_attrs(ctx, recipe, desired.name)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        """Delete a recipe from DSS."""
        recipe = self._get_recipe(ctx, prior.name)
        recipe.delete()


class SyncRecipeHandler(RecipeHandler):
    """Handler for sync recipes. No type-specific overrides needed."""


class CodeRecipeHandler(RecipeHandler):
    """Base handler for recipes that carry a code payload."""

    def _read_extra_attrs(self, settings: Any, raw_def: dict[str, Any]) -> dict[str, Any]:
        _ = raw_def
        return {"code": settings.str_payload or ""}

    def _apply_type_settings(
        self,
        settings: Any,
        raw_def: dict[str, Any],
        desired: RecipeResource,
        *,
        creating: bool,
    ) -> bool:
        _ = raw_def
        if not isinstance(desired, (PythonRecipeResource, SQLQueryRecipeResource)):
            return False
        if creating and not desired.code:
            return False
        settings.set_payload(desired.code)
        return True


class PythonRecipeHandler(CodeRecipeHandler):
    """Handler for Python recipes — adds code_env support."""

    def _read_extra_attrs(self, settings: Any, raw_def: dict[str, Any]) -> dict[str, Any]:
        attrs = super()._read_extra_attrs(settings, raw_def)
        env_sel = raw_def.get("params", {}).get("envSelection", {})
        attrs["code_env"] = (
            env_sel.get("envName") if env_sel.get("envMode") == "EXPLICIT_ENV" else None
        )
        return attrs

    def _apply_type_settings(
        self,
        settings: Any,
        raw_def: dict[str, Any],
        desired: RecipeResource,
        *,
        creating: bool,
    ) -> bool:
        needs_save = super()._apply_type_settings(settings, raw_def, desired, creating=creating)

        if not isinstance(desired, PythonRecipeResource):
            return needs_save

        if desired.code_env is not None:
            raw_def.setdefault("params", {})["envSelection"] = {
                "envMode": "EXPLICIT_ENV",
                "envName": desired.code_env,
            }
            needs_save = True
        elif not creating:
            # On update, clear envSelection when code_env is None.
            raw_def.get("params", {}).pop("envSelection", None)

        return needs_save


class SQLQueryRecipeHandler(CodeRecipeHandler):
    """Handler for SQL query recipes — adds SQL input validation."""

    def validate(self, ctx: EngineContext, desired: RecipeResource) -> list[str]:
        errors = super().validate(ctx, desired)
        if not desired.inputs:
            errors.append(f"SQL query recipe '{desired.name}' requires at least one input dataset")
        return errors

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: RecipeResource,
        all_desired: Mapping[str, Resource],
        state: State,
    ) -> list[str]:
        _ = ctx
        errors: list[str] = []

        if not any(_is_sql_dataset(ref, all_desired, state) for ref in desired.inputs):
            errors.append(
                f"SQL query recipe '{desired.name}' requires at least one input "
                f"with a SQL connection (inputs: {desired.inputs})"
            )
        return errors

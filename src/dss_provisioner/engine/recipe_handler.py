"""Recipe handler implementing CRUD via dataikuapi."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar

from dss_provisioner.engine.handlers import PlanContext, ResourceHandler
from dss_provisioner.resources.dataset import DatasetResource

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dataikuapi.dss.project import DSSProject
    from dataikuapi.dss.recipe import DSSRecipe

    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.recipe import (
        PythonRecipeResource,
        RecipeResource,
        SQLQueryRecipeResource,
        SyncRecipeResource,  # noqa: F401 — used in RecipeHandler["SyncRecipeResource"] base
    )

R = TypeVar("R", bound="RecipeResource")


def _read_code_payload(settings: Any) -> str:
    """Read the code payload from recipe settings."""
    return settings.str_payload or ""


def _apply_code_payload(settings: Any, code: str, *, creating: bool) -> bool:
    """Set code payload on recipe settings. Return True if settings.save() is needed."""
    if creating and not code:
        return False
    settings.set_payload(code)
    return True


class RecipeHandler(ResourceHandler[R]):
    """Base CRUD handler for DSS recipes.

    Subclasses override hook methods to handle type-specific behavior
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
            logger.debug("Zone read unavailable for recipe %s", recipe_name, exc_info=True)
            return None
        return None

    def _apply_metadata(self, recipe: DSSRecipe, desired: R) -> None:
        """Set description and tags via metadata."""
        meta = recipe.get_metadata()
        meta["description"] = desired.description
        meta["tags"] = list(desired.tags)
        recipe.set_metadata(meta)

    def _apply_zone(self, recipe: DSSRecipe, desired: R) -> None:
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
        desired: R,
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
            "type": raw_def.get("type", ""),
            "inputs": settings.get_flat_input_refs(),
            "outputs": settings.get_flat_output_refs(),
            "zone": self._read_zone(ctx, name),
        }

        attrs.update(self._read_extra_attrs(settings, raw_def))
        return attrs

    # -- Validation -----------------------------------------------------------

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: R,
        plan_ctx: PlanContext,
    ) -> list[str]:
        _ = ctx
        errors: list[str] = []
        if desired.zone is not None and plan_ctx.get_resource_type(desired.zone) != "dss_zone":
            errors.append(f"Recipe '{desired.name}' references unknown zone '{desired.zone}'")
        return errors

    # -- CRUD -----------------------------------------------------------------

    def create(self, ctx: EngineContext, desired: R) -> dict[str, Any]:
        """Create a recipe in DSS."""
        project = self._get_project(ctx)

        builder = project.new_recipe(desired.type, desired.name)
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

    def update(self, ctx: EngineContext, desired: R, prior: ResourceInstance) -> dict[str, Any]:
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


class SyncRecipeHandler(RecipeHandler["SyncRecipeResource"]):
    """Handler for sync recipes. No type-specific overrides needed."""


class PythonRecipeHandler(RecipeHandler["PythonRecipeResource"]):
    """Handler for Python recipes — adds code payload and code_env support."""

    def __init__(self) -> None:
        self._code_env_cache: set[str] | None = None

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: PythonRecipeResource,
        plan_ctx: PlanContext,
    ) -> list[str]:
        errors = super().validate_plan(ctx, desired, plan_ctx)
        if desired.code_env is not None:
            if self._code_env_cache is None:
                envs = ctx.provider.client.list_code_envs()
                self._code_env_cache = {e["envName"] for e in envs if e.get("envLang") == "PYTHON"}
            if desired.code_env not in self._code_env_cache:
                errors.append(
                    f"Python recipe '{desired.name}' references "
                    f"unknown code env '{desired.code_env}'"
                )
        return errors

    def _read_extra_attrs(self, settings: Any, raw_def: dict[str, Any]) -> dict[str, Any]:
        env_sel = raw_def.get("params", {}).get("envSelection", {})
        return {
            "code": _read_code_payload(settings),
            "code_env": (
                env_sel.get("envName") if env_sel.get("envMode") == "EXPLICIT_ENV" else None
            ),
        }

    def _apply_type_settings(
        self,
        settings: Any,
        raw_def: dict[str, Any],
        desired: PythonRecipeResource,
        *,
        creating: bool,
    ) -> bool:
        needs_save = _apply_code_payload(settings, desired.code, creating=creating)

        if desired.code_env is not None:
            raw_def.setdefault("params", {})["envSelection"] = {
                "envMode": "EXPLICIT_ENV",
                "envName": desired.code_env,
            }
            needs_save = True
        elif not creating:
            # On update, clear envSelection when code_env is None.
            if "params" in raw_def:
                raw_def["params"].pop("envSelection", None)

        return needs_save


class SQLQueryRecipeHandler(RecipeHandler["SQLQueryRecipeResource"]):
    """Handler for SQL query recipes — adds code payload and SQL input validation."""

    def _read_extra_attrs(self, settings: Any, raw_def: dict[str, Any]) -> dict[str, Any]:
        _ = raw_def
        return {"code": _read_code_payload(settings)}

    def _apply_type_settings(
        self,
        settings: Any,
        raw_def: dict[str, Any],
        desired: SQLQueryRecipeResource,
        *,
        creating: bool,
    ) -> bool:
        _ = raw_def
        return _apply_code_payload(settings, desired.code, creating=creating)

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: SQLQueryRecipeResource,
        plan_ctx: PlanContext,
    ) -> list[str]:
        errors = super().validate_plan(ctx, desired, plan_ctx)

        if not any(
            plan_ctx.get_attr(ref, "type") in DatasetResource.sql_types for ref in desired.inputs
        ):
            errors.append(
                f"SQL query recipe '{desired.name}' requires at least one input "
                f"with a SQL connection (inputs: {desired.inputs})"
            )
        return errors

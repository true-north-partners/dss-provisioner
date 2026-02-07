"""Scenario handler implementing CRUD via dataikuapi."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, TypeVar

from dss_provisioner.engine.handlers import ResourceHandler

if TYPE_CHECKING:
    from dataikuapi.dss.project import DSSProject
    from dataikuapi.dss.scenario import DSSScenario, DSSScenarioSettings

    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.scenario import (
        PythonScenarioResource,
        ScenarioResource,
        StepBasedScenarioResource,
    )

R = TypeVar("R", bound="ScenarioResource")


class ScenarioHandler(ResourceHandler[R]):
    """Base CRUD handler for DSS scenarios.

    Subclasses override hook methods to handle type-specific behavior
    (steps vs. Python code).
    """

    # -- DSS helpers ----------------------------------------------------------

    def _get_project(self, ctx: EngineContext) -> DSSProject:
        return ctx.provider.client.get_project(ctx.project_key)

    def _get_scenario(self, ctx: EngineContext, scenario_id: str) -> DSSScenario:
        return self._get_project(ctx).get_scenario(scenario_id)

    def _apply_common_settings(self, settings: DSSScenarioSettings, desired: R) -> None:
        """Apply common scenario settings (active, triggers, description, tags)."""
        settings.data["active"] = desired.active
        settings.data["triggers"] = desired.triggers
        settings.data["shortDesc"] = desired.description
        settings.data["tags"] = list(desired.tags)

    # -- Hook methods (override in subclasses) --------------------------------

    def _scenario_type(self) -> str:
        """Return the DSS scenario type string."""
        raise NotImplementedError

    def _apply_type_settings(self, settings: Any, desired: R) -> None:
        """Apply type-specific settings (steps or code)."""
        raise NotImplementedError

    def _read_type_attrs(self, settings: Any, prior: ResourceInstance) -> dict[str, Any]:
        """Read type-specific attributes from DSS settings."""
        raise NotImplementedError

    def _write_type_attrs(self, desired: R) -> dict[str, Any]:
        """Return type-specific attributes to store in state."""
        raise NotImplementedError

    # -- CRUD -----------------------------------------------------------------

    def create(self, ctx: EngineContext, desired: R) -> dict[str, Any]:
        """Create a scenario in DSS."""
        project = self._get_project(ctx)
        scenario = project.create_scenario(desired.name, type=self._scenario_type())
        settings = scenario.get_settings()
        self._apply_common_settings(settings, desired)
        self._apply_type_settings(settings, desired)
        settings.save()
        return {
            "name": desired.name,
            "description": desired.description,
            "tags": list(desired.tags),
            "type": desired.type,
            "active": desired.active,
            "triggers": desired.triggers,
            "scenario_id": scenario.id,
            **self._write_type_attrs(desired),
        }

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        """Read scenario from DSS. Returns None if deleted externally."""
        scenario_id = prior.attributes.get("scenario_id")
        if scenario_id is None:
            return None
        try:
            scenario = self._get_scenario(ctx, scenario_id)
            settings = scenario.get_settings()
        except Exception:
            return None
        return {
            "name": prior.name,
            "description": settings.data.get("shortDesc", ""),
            "tags": settings.data.get("tags", []),
            "type": prior.attributes.get("type", self._scenario_type()),
            "active": settings.active,
            "triggers": prior.attributes.get("triggers", []),
            "scenario_id": scenario_id,
            **self._read_type_attrs(settings, prior),
        }

    def update(self, ctx: EngineContext, desired: R, prior: ResourceInstance) -> dict[str, Any]:
        """Update a scenario in DSS."""
        scenario_id = prior.attributes["scenario_id"]
        scenario = self._get_scenario(ctx, scenario_id)
        settings = scenario.get_settings()
        self._apply_common_settings(settings, desired)
        self._apply_type_settings(settings, desired)
        settings.save()
        return {
            "name": desired.name,
            "description": desired.description,
            "tags": list(desired.tags),
            "type": desired.type,
            "active": desired.active,
            "triggers": desired.triggers,
            "scenario_id": scenario_id,
            **self._write_type_attrs(desired),
        }

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        """Delete a scenario from DSS."""
        scenario_id = prior.attributes.get("scenario_id")
        if scenario_id is None:
            return
        with contextlib.suppress(Exception):
            self._get_scenario(ctx, scenario_id).delete()


class StepBasedScenarioHandler(ScenarioHandler["StepBasedScenarioResource"]):
    """Handler for step-based scenarios."""

    def _scenario_type(self) -> str:
        return "step_based"

    def _apply_type_settings(self, settings: Any, desired: StepBasedScenarioResource) -> None:
        settings.data["params"]["steps"] = desired.steps

    def _read_type_attrs(self, settings: Any, prior: ResourceInstance) -> dict[str, Any]:
        _ = settings
        return {"steps": prior.attributes.get("steps", [])}

    def _write_type_attrs(self, desired: StepBasedScenarioResource) -> dict[str, Any]:
        return {"steps": desired.steps}


class PythonScenarioHandler(ScenarioHandler["PythonScenarioResource"]):
    """Handler for custom Python scenarios."""

    def _scenario_type(self) -> str:
        return "custom_python"

    def _apply_type_settings(self, settings: Any, desired: PythonScenarioResource) -> None:
        settings.code = desired.code

    def _read_type_attrs(self, settings: Any, prior: ResourceInstance) -> dict[str, Any]:
        _ = prior
        return {"code": settings.code}

    def _write_type_attrs(self, desired: PythonScenarioResource) -> dict[str, Any]:
        return {"code": desired.code}

"""Variables handler implementing CRUD via dataikuapi project variables API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.handlers import ResourceHandler

if TYPE_CHECKING:
    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.variables import VariablesResource


class VariablesHandler(ResourceHandler["VariablesResource"]):
    """CRUD handler for DSS project variables."""

    def _read_attrs(self, ctx: EngineContext) -> dict[str, Any]:
        project = ctx.provider.client.get_project(ctx.project_key)
        project_vars = project.get_variables()
        return {
            "name": "variables",
            "description": "",
            "tags": [],
            "standard": project_vars.get("standard", {}),
            "local": project_vars.get("local", {}),
        }

    def create(self, ctx: EngineContext, desired: VariablesResource) -> dict[str, Any]:
        project = ctx.provider.client.get_project(ctx.project_key)
        current = project.get_variables()
        merged = {
            "standard": {**current.get("standard", {}), **desired.standard},
            "local": {**current.get("local", {}), **desired.local},
        }
        project.set_variables(merged)
        return self._read_attrs(ctx)

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any]:
        _ = prior
        return self._read_attrs(ctx)

    def update(
        self, ctx: EngineContext, desired: VariablesResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        _ = prior
        return self.create(ctx, desired)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        _ = prior
        project = ctx.provider.client.get_project(ctx.project_key)
        project.set_variables({"standard": {}, "local": {}})

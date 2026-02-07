"""Zone handler implementing CRUD via dataikuapi flow API."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.handlers import ResourceHandler

if TYPE_CHECKING:
    from dataikuapi.dss.flow import DSSFlowZone, DSSProjectFlow

    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.zone import ZoneResource

logger = logging.getLogger(__name__)


class ZoneHandler(ResourceHandler["ZoneResource"]):
    """CRUD handler for DSS flow zones."""

    def _get_flow(self, ctx: EngineContext) -> DSSProjectFlow:
        return ctx.provider.client.get_project(ctx.project_key).get_flow()

    def _find_zone(self, flow: DSSProjectFlow, name: str) -> DSSFlowZone | None:
        """Find a zone by name in the flow. Returns the zone object or None."""
        try:
            zones = flow.list_zones()
        except Exception:
            # Flow zones unavailable (e.g. DSS Free Edition returns 404).
            return None
        for z in zones:
            if z.id == name:
                return z
        return None

    def _read_attrs(self, zone: DSSFlowZone) -> dict[str, Any]:
        """Extract zone attributes matching ZoneResource model_dump output."""
        settings = zone.get_settings().get_raw()
        return {
            "name": zone.id,
            "color": settings.get("color", "#2ab1ac"),
            # DSS zones don't support metadata, but we must echo these back
            # so the engine's model_dump comparison doesn't see phantom drift.
            "description": "",
            "tags": [],
        }

    def create(self, ctx: EngineContext, desired: ZoneResource) -> dict[str, Any]:
        """Create a flow zone in DSS."""
        flow = self._get_flow(ctx)
        try:
            zone = flow.create_zone(desired.name, color=desired.color)
        except Exception as exc:
            msg = f"Failed to create zone '{desired.name}' — flow zones may not be available: {exc}"
            raise RuntimeError(msg) from exc
        return self._read_attrs(zone)

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        """Read a flow zone from DSS. Returns None if it no longer exists."""
        flow = self._get_flow(ctx)
        zone = self._find_zone(flow, prior.name)
        if zone is None:
            return None
        return self._read_attrs(zone)

    def update(
        self, ctx: EngineContext, desired: ZoneResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        """Update a flow zone in DSS (color change)."""
        _ = prior
        flow = self._get_flow(ctx)
        zone = self._find_zone(flow, desired.name)
        if zone is None:
            msg = f"Zone '{desired.name}' not found for update — flow zones may not be available"
            raise RuntimeError(msg)
        settings = zone.get_settings()
        raw = settings.get_raw()
        raw["color"] = desired.color
        settings.save()
        return self._read_attrs(zone)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        """Delete a flow zone from DSS."""
        flow = self._get_flow(ctx)
        zone = self._find_zone(flow, prior.name)
        if zone is None:
            # Zone already gone or flow zones unavailable — nothing to do.
            return
        zone.delete()

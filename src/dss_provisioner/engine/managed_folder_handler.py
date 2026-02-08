"""Managed folder handler implementing CRUD via dataikuapi."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.handlers import PlanContext, ResourceHandler
from dss_provisioner.engine.variables import get_variables, resolve_variables
from dss_provisioner.resources.managed_folder import (
    FilesystemManagedFolderResource,
    ManagedFolderResource,
    UploadManagedFolderResource,
)
from dss_provisioner.resources.markers import extract_dss_attrs

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dataikuapi.dss.managedfolder import DSSManagedFolder
    from dataikuapi.dss.project import DSSProject

    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext

_RESOURCE_CLASSES: dict[str, type[ManagedFolderResource]] = {
    cls.resource_type: cls
    for cls in [
        ManagedFolderResource,
        FilesystemManagedFolderResource,
        UploadManagedFolderResource,
    ]
}


class ManagedFolderHandler(ResourceHandler["ManagedFolderResource"]):
    """CRUD handler for DSS managed folders.

    Managed folders are accessed by an internal ``odb_id``, not by name.
    This handler maintains a per-project name → ID cache that is populated
    on first access and invalidated after create/delete operations.
    """

    def __init__(self) -> None:
        self._variables_cache: dict[str, dict[str, str]] = {}
        self._id_cache: dict[str, dict[str, str]] = {}

    def _get_project(self, ctx: EngineContext) -> DSSProject:
        return ctx.provider.client.get_project(ctx.project_key)

    def _get_variables(self, ctx: EngineContext) -> dict[str, str]:
        if ctx.project_key in self._variables_cache:
            return self._variables_cache[ctx.project_key]
        variables = get_variables(ctx)
        self._variables_cache[ctx.project_key] = variables
        return variables

    def _resolve_folder_id(self, ctx: EngineContext, name: str) -> str | None:
        """Resolve a managed folder name to its internal odb_id."""
        if ctx.project_key not in self._id_cache:
            folders = self._get_project(ctx).list_managed_folders()
            self._id_cache[ctx.project_key] = {f["name"]: f["id"] for f in folders}
        return self._id_cache[ctx.project_key].get(name)

    def _invalidate_id_cache(self, ctx: EngineContext) -> None:
        self._id_cache.pop(ctx.project_key, None)

    def _get_folder(self, ctx: EngineContext, name: str) -> DSSManagedFolder | None:
        """Get a managed folder handle by name, or None if not found."""
        odb_id = self._resolve_folder_id(ctx, name)
        if odb_id is None:
            return None
        return self._get_project(ctx).get_managed_folder(odb_id)

    def _apply_zone(self, folder: DSSManagedFolder, desired: ManagedFolderResource) -> None:
        if desired.zone is None:
            return
        folder.move_to_zone(desired.zone)

    def _read_zone(self, folder: DSSManagedFolder) -> str | None:
        try:
            zone = folder.get_zone()
        except Exception:
            logger.debug("Zone read unavailable for managed folder %s", folder.id, exc_info=True)
            return None
        zone_id = zone.id
        if zone_id == "default":
            return None
        return zone_id

    def _read_attrs(
        self,
        ctx: EngineContext,
        folder: DSSManagedFolder,
        resource_cls: type[ManagedFolderResource],
        name: str,
    ) -> dict[str, Any]:
        settings = folder.get_settings()
        raw = settings.get_raw()

        attrs: dict[str, Any] = {
            "name": name,
            "description": raw.get("description", ""),
            "tags": raw.get("tags", []),
            "type": raw.get("type", ""),
            "zone": self._read_zone(folder),
        }
        attrs.update(extract_dss_attrs(resource_cls, raw))
        return resolve_variables(attrs, self._get_variables(ctx))

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: ManagedFolderResource,
        plan_ctx: PlanContext,
    ) -> list[str]:
        _ = ctx
        errors: list[str] = []
        if desired.zone is not None and plan_ctx.get_resource_type(desired.zone) != "dss_zone":
            errors.append(
                f"Managed folder '{desired.name}' references unknown zone '{desired.zone}'"
            )
        return errors

    def create(self, ctx: EngineContext, desired: ManagedFolderResource) -> dict[str, Any]:
        project = self._get_project(ctx)
        folder = project.create_managed_folder(
            desired.name,
            folder_type=desired.type,
            connection_name=desired.connection,
        )

        settings = folder.get_settings()
        raw = settings.get_raw()

        # Apply params (e.g. path for filesystem folders)
        params = raw.setdefault("params", {})
        params.update(desired.to_dss_params())

        raw["description"] = desired.description
        raw["tags"] = list(desired.tags)
        settings.save()

        self._apply_zone(folder, desired)
        self._invalidate_id_cache(ctx)

        return self._read_attrs(ctx, folder, type(desired), desired.name)

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        folder = self._get_folder(ctx, prior.name)
        if folder is None:
            return None
        resource_cls = _RESOURCE_CLASSES.get(prior.resource_type, ManagedFolderResource)
        return self._read_attrs(ctx, folder, resource_cls, prior.name)

    def update(
        self, ctx: EngineContext, desired: ManagedFolderResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        _ = prior
        folder = self._get_folder(ctx, desired.name)
        if folder is None:
            msg = f"Managed folder '{desired.name}' not found during update"
            raise RuntimeError(msg)

        settings = folder.get_settings()
        raw = settings.get_raw()

        params = raw.setdefault("params", {})
        params.update(desired.to_dss_params())

        raw["description"] = desired.description
        raw["tags"] = list(desired.tags)
        settings.save()

        self._apply_zone(folder, desired)

        return self._read_attrs(ctx, folder, type(desired), desired.name)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        folder = self._get_folder(ctx, prior.name)
        if folder is not None:
            folder.delete()
            self._invalidate_id_cache(ctx)

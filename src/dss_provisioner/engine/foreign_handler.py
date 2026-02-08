"""Handlers for foreign (cross-project) dataset/folder declarations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from dss_provisioner.engine.handlers import ResourceHandler
from dss_provisioner.resources.foreign import ForeignDatasetResource, ForeignManagedFolderResource

if TYPE_CHECKING:
    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext

R = TypeVar("R", ForeignDatasetResource, ForeignManagedFolderResource)


class _ForeignHandler(ResourceHandler[R]):
    object_type: ClassVar[str]

    def _source_project(self, ctx: EngineContext, source_project: str) -> Any:
        return ctx.provider.client.get_project(source_project)

    def _check_exposed(self, ctx: EngineContext, source_project: str, source_name: str) -> bool:
        src = self._source_project(ctx, source_project)
        settings = src.get_settings().settings
        objects = settings.get("exposedObjects", {}).get("objects", [])
        for entry in objects:
            if entry.get("type") != self.object_type or entry.get("localName") != source_name:
                continue
            targets = {
                rule.get("targetProject")
                for rule in entry.get("rules", [])
                if isinstance(rule, dict)
            }
            if ctx.project_key in targets:
                return True
        return False

    def _exists_in_source(
        self,
        ctx: EngineContext,
        source_project: str,
        source_name: str,
    ) -> tuple[bool, str | None]:
        raise NotImplementedError

    def _attrs(
        self,
        desired: R,
        *,
        description: str,
        tags: list[str],
        source_type: str | None,
    ) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            "name": desired.name,
            "description": description,
            "tags": tags,
            "source_project": desired.source_project,
            "source_name": desired.source_name,
            "full_ref": desired.full_ref,
        }
        if source_type is not None:
            attrs["type"] = source_type
        return attrs

    def _read_from_decl(
        self,
        ctx: EngineContext,
        desired: R,
        *,
        description: str,
        tags: list[str],
    ) -> dict[str, Any] | None:
        exists, source_type = self._exists_in_source(
            ctx, desired.source_project, desired.source_name
        )
        if not exists:
            return None
        if not self._check_exposed(ctx, desired.source_project, desired.source_name):
            return None
        return self._attrs(desired, description=description, tags=tags, source_type=source_type)

    def _desired(self, name: str, source_project: str, source_name: str) -> R:
        raise NotImplementedError

    def validate(self, ctx: EngineContext, desired: R) -> list[str]:
        errors: list[str] = []
        if desired.source_project == ctx.project_key:
            errors.append(
                f"Foreign object '{desired.name}' must reference another project "
                f"(got source_project='{ctx.project_key}')"
            )
        return errors

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        source_project = prior.attributes.get("source_project")
        source_name = prior.attributes.get("source_name")
        if not isinstance(source_project, str) or not isinstance(source_name, str):
            return None
        desired = self._desired(prior.name, source_project, source_name)
        return self._read_from_decl(
            ctx,
            desired,
            description=prior.attributes.get("description", ""),
            tags=list(prior.attributes.get("tags", [])),
        )

    def create(self, ctx: EngineContext, desired: R) -> dict[str, Any]:
        result = self._read_from_decl(
            ctx,
            desired,
            description=desired.description,
            tags=list(desired.tags),
        )
        if result is None:
            msg = (
                f"Foreign {self.object_type.lower()} '{desired.full_ref}' is missing "
                f"or not exposed to project '{ctx.project_key}'"
            )
            raise RuntimeError(msg)
        return result

    def update(self, ctx: EngineContext, desired: R, prior: ResourceInstance) -> dict[str, Any]:
        _ = prior
        return self.create(ctx, desired)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        _ = ctx, prior


class ForeignDatasetHandler(_ForeignHandler[ForeignDatasetResource]):
    """Handler for foreign dataset declarations."""

    object_type = "DATASET"

    def _desired(self, name: str, source_project: str, source_name: str) -> ForeignDatasetResource:
        return ForeignDatasetResource(
            name=name,
            source_project=source_project,
            source_name=source_name,
        )

    def _exists_in_source(
        self,
        ctx: EngineContext,
        source_project: str,
        source_name: str,
    ) -> tuple[bool, str | None]:
        source = self._source_project(ctx, source_project)
        dataset = source.get_dataset(source_name)
        if not dataset.exists():
            return False, None
        raw = dataset.get_settings().get_raw()
        return True, raw.get("type")


class ForeignManagedFolderHandler(_ForeignHandler[ForeignManagedFolderResource]):
    """Handler for foreign managed folder declarations."""

    object_type = "MANAGED_FOLDER"

    def _desired(
        self,
        name: str,
        source_project: str,
        source_name: str,
    ) -> ForeignManagedFolderResource:
        return ForeignManagedFolderResource(
            name=name,
            source_project=source_project,
            source_name=source_name,
        )

    def _exists_in_source(
        self,
        ctx: EngineContext,
        source_project: str,
        source_name: str,
    ) -> tuple[bool, str | None]:
        source = self._source_project(ctx, source_project)
        folders = source.list_managed_folders()
        exists = any(f.get("name") == source_name for f in folders)
        return exists, None

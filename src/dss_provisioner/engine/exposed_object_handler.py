"""Handler for project exposed object settings."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from dss_provisioner.engine.handlers import ResourceHandler

if TYPE_CHECKING:
    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.exposed_object import (
        ExposedDatasetResource,  # noqa: F401
        ExposedManagedFolderResource,  # noqa: F401
        ExposedObjectResource,
    )

R = TypeVar("R", bound="ExposedObjectResource")


class _ExposedObjectHandler(ResourceHandler[R]):
    object_type: ClassVar[str]

    def _objects(self, ctx: EngineContext) -> list[dict[str, Any]]:
        project = ctx.provider.client.get_project(ctx.project_key)
        settings = project.get_settings()
        exposed = settings.settings.setdefault("exposedObjects", {})
        return exposed.setdefault("objects", [])

    def _find_entry(self, objects: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
        for entry in objects:
            if entry.get("type") == self.object_type and entry.get("localName") == name:
                return entry
        return None

    @staticmethod
    def _target_projects(entry: dict[str, Any]) -> list[str]:
        targets = [
            r.get("targetProject")
            for r in entry.get("rules", [])
            if isinstance(r, dict) and isinstance(r.get("targetProject"), str)
        ]
        return sorted(set(targets))

    def _attrs(
        self,
        name: str,
        targets: list[str],
        *,
        description: str,
        tags: list[str],
    ) -> dict[str, Any]:
        return {
            "name": name,
            "description": description,
            "tags": tags,
            "type": self.object_type,
            "target_projects": targets,
        }

    def _exists_in_project(self, ctx: EngineContext, name: str) -> bool:
        raise NotImplementedError

    def validate(self, ctx: EngineContext, desired: R) -> list[str]:
        errors: list[str] = []
        if not self._exists_in_project(ctx, desired.name):
            errors.append(
                f"Exposed object '{desired.name}' ({self.object_type}) "
                f"does not exist in project '{ctx.project_key}'"
            )
        if ctx.project_key in desired.target_projects:
            errors.append(
                f"Exposed object '{desired.name}' includes current project "
                f"'{ctx.project_key}' in target_projects"
            )
        return errors

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        objects = self._objects(ctx)
        entry = self._find_entry(objects, prior.name)
        if entry is None:
            return None
        return self._attrs(
            prior.name,
            self._target_projects(entry),
            description=prior.attributes.get("description", ""),
            tags=list(prior.attributes.get("tags", [])),
        )

    def _upsert(self, ctx: EngineContext, *, name: str, targets: list[str]) -> None:
        project = ctx.provider.client.get_project(ctx.project_key)
        settings = project.get_settings()
        exposed = settings.settings.setdefault("exposedObjects", {})
        objects = exposed.setdefault("objects", [])

        entry = self._find_entry(objects, name)
        if entry is None:
            entry = {"type": self.object_type, "localName": name, "rules": []}
            objects.append(entry)

        entry["rules"] = [{"targetProject": p} for p in targets]
        settings.save()

    def create(self, ctx: EngineContext, desired: R) -> dict[str, Any]:
        targets = sorted(set(desired.target_projects))
        self._upsert(ctx, name=desired.name, targets=targets)
        return self._attrs(
            desired.name,
            targets,
            description=desired.description,
            tags=list(desired.tags),
        )

    def update(self, ctx: EngineContext, desired: R, prior: ResourceInstance) -> dict[str, Any]:
        _ = prior
        return self.create(ctx, desired)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        project = ctx.provider.client.get_project(ctx.project_key)
        settings = project.get_settings()
        exposed = settings.settings.setdefault("exposedObjects", {})
        objects = exposed.setdefault("objects", [])
        before = len(objects)
        objects[:] = [
            entry
            for entry in objects
            if not (entry.get("type") == self.object_type and entry.get("localName") == prior.name)
        ]
        if len(objects) != before:
            settings.save()


class ExposedDatasetHandler(_ExposedObjectHandler["ExposedDatasetResource"]):
    """Expose dataset settings for a project."""

    object_type = "DATASET"

    def _exists_in_project(self, ctx: EngineContext, name: str) -> bool:
        project = ctx.provider.client.get_project(ctx.project_key)
        return project.get_dataset(name).exists()


class ExposedManagedFolderHandler(_ExposedObjectHandler["ExposedManagedFolderResource"]):
    """Expose managed folder settings for a project."""

    object_type = "MANAGED_FOLDER"

    def _exists_in_project(self, ctx: EngineContext, name: str) -> bool:
        project = ctx.provider.client.get_project(ctx.project_key)
        folders = project.list_managed_folders()
        return any(f.get("name") == name for f in folders)

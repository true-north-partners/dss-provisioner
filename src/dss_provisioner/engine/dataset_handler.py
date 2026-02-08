"""Dataset handler implementing CRUD via dataikuapi."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.handlers import PlanContext, ResourceHandler
from dss_provisioner.engine.variables import get_variables, resolve_variables
from dss_provisioner.resources.dataset import (
    DatasetResource,
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
    UploadDatasetResource,
)
from dss_provisioner.resources.markers import extract_dss_attrs

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dataikuapi.dss.dataset import DSSDataset, DSSDatasetSettings
    from dataikuapi.dss.project import DSSProject

    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext

_RESOURCE_CLASSES: dict[str, type[DatasetResource]] = {
    cls.resource_type: cls
    for cls in [
        DatasetResource,
        SnowflakeDatasetResource,
        OracleDatasetResource,
        FilesystemDatasetResource,
        UploadDatasetResource,
    ]
}


class DatasetHandler(ResourceHandler["DatasetResource"]):
    """CRUD handler for DSS datasets.

    Handles DatasetResource, SnowflakeDatasetResource, and OracleDatasetResource.
    """

    def __init__(self) -> None:
        self._variables_cache: dict[str, dict[str, str]] = {}

    def _get_project(self, ctx: EngineContext) -> DSSProject:
        return ctx.provider.client.get_project(ctx.project_key)

    def _get_variables(self, ctx: EngineContext) -> dict[str, str]:
        """Build the DSS variable substitution map from built-ins + project/instance vars.

        Results are cached per project key to avoid redundant API calls within a
        plan/apply cycle.
        """
        if ctx.project_key in self._variables_cache:
            return self._variables_cache[ctx.project_key]

        variables = get_variables(ctx)
        self._variables_cache[ctx.project_key] = variables
        return variables

    def _get_dataset(self, ctx: EngineContext, name: str) -> DSSDataset:
        return self._get_project(ctx).get_dataset(name)

    def _apply_schema(self, dataset: DSSDataset, desired: DatasetResource) -> None:
        """Set schema columns on the dataset."""
        if not desired.columns:
            return
        columns = [
            {"name": c.name, "type": c.type, "comment": c.description, "meaning": c.meaning}
            for c in desired.columns
        ]
        dataset.set_schema({"columns": columns, "userModified": True})

    def _apply_metadata(self, dataset: DSSDataset, desired: DatasetResource) -> None:
        """Set description and tags via metadata."""
        meta = dataset.get_metadata()
        meta["description"] = desired.description
        meta["tags"] = list(desired.tags)
        dataset.set_metadata(meta)

    def _apply_zone(self, dataset: DSSDataset, desired: DatasetResource) -> None:
        """Move dataset to a flow zone if specified."""
        if desired.zone is None:
            return
        dataset.move_to_zone(desired.zone)

    def _apply_format(self, settings: DSSDatasetSettings, desired: DatasetResource) -> None:
        """Set format type and params on the dataset settings."""
        if desired.format_type is None:
            return
        raw = settings.get_raw()
        raw["formatType"] = desired.format_type
        raw.setdefault("formatParams", {}).update(desired.format_params)

    def _read_zone(self, dataset: DSSDataset) -> str | None:
        """Read the current flow zone of a dataset, or None if in the default zone."""
        try:
            zone = dataset.get_zone()
        except Exception:
            logger.debug("Zone read unavailable for dataset %s", dataset.dataset_name)
            return None
        zone_id = zone.id
        # The default zone has a well-known id; treat it as "no zone".
        if zone_id == "default":
            return None
        return zone_id

    def _read_attrs(
        self,
        ctx: EngineContext,
        dataset: DSSDataset,
        resource_cls: type[DatasetResource],
        name: str,
        type_fallback: str = "",
    ) -> dict[str, Any]:
        """Extract dataset attributes from DSS, keyed to match model_dump output."""
        settings = dataset.get_settings()
        raw = settings.get_raw()
        meta = dataset.get_metadata()
        schema = dataset.get_schema()

        columns = [
            {
                "name": col.get("name", ""),
                "type": col.get("type", "string"),
                "description": col.get("comment", ""),
                "meaning": col.get("meaning"),
            }
            for col in schema.get("columns", [])
        ]

        attrs: dict[str, Any] = {
            "name": name,
            "description": meta.get("description", ""),
            "tags": meta.get("tags", []),
            "type": raw.get("type", type_fallback),
            "managed": raw.get("managed", False),
            "columns": columns,
            "zone": self._read_zone(dataset),
        }

        attrs.update(extract_dss_attrs(resource_cls, raw))

        return resolve_variables(attrs, self._get_variables(ctx))

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: DatasetResource,
        plan_ctx: PlanContext,
    ) -> list[str]:
        _ = ctx
        errors: list[str] = []
        if desired.zone is not None and plan_ctx.get_resource_type(desired.zone) != "dss_zone":
            errors.append(f"Dataset '{desired.name}' references unknown zone '{desired.zone}'")
        return errors

    def create(self, ctx: EngineContext, desired: DatasetResource) -> dict[str, Any]:
        """Create a dataset in DSS."""
        project = self._get_project(ctx)
        params = desired.to_dss_params()

        if desired.managed:
            builder = project.new_managed_dataset(desired.name)
            if desired.connection is not None:
                builder.with_store_into(desired.connection)
            dataset = builder.create()
        else:
            dataset = project.create_dataset(desired.name, desired.type, params=params)

        settings = dataset.get_settings()
        self._apply_format(settings, desired)
        settings.save()

        self._apply_schema(dataset, desired)
        self._apply_metadata(dataset, desired)
        self._apply_zone(dataset, desired)

        return self._read_attrs(ctx, dataset, type(desired), desired.name, desired.type)

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        """Read dataset from DSS. Returns None if deleted externally."""
        dataset = self._get_dataset(ctx, prior.name)
        if not dataset.exists():
            return None
        resource_cls = _RESOURCE_CLASSES.get(prior.resource_type, DatasetResource)
        return self._read_attrs(ctx, dataset, resource_cls, prior.name)

    def update(
        self, ctx: EngineContext, desired: DatasetResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        """Update a dataset in DSS."""
        _ = prior
        dataset = self._get_dataset(ctx, desired.name)
        settings = dataset.get_settings()

        raw = settings.get_raw()
        params = raw.setdefault("params", {})
        params.update(desired.to_dss_params())

        self._apply_format(settings, desired)
        settings.save()

        self._apply_schema(dataset, desired)
        self._apply_metadata(dataset, desired)
        self._apply_zone(dataset, desired)

        return self._read_attrs(ctx, dataset, type(desired), desired.name, desired.type)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        """Delete a dataset from DSS."""
        managed = prior.attributes.get("managed", False)
        dataset = self._get_dataset(ctx, prior.name)
        dataset.delete(drop_data=managed)

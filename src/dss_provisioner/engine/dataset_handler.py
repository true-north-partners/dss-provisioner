"""Dataset handler implementing CRUD via dataikuapi."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataikuapi.dss.dataset import DSSDataset, DSSDatasetSettings
    from dataikuapi.dss.project import DSSProject

    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.resources.dataset import DatasetResource

# Maps resource_type -> list of (model_field, dss_params_key, default) for
# type-specific fields that live inside raw["params"].
_EXTRA_PARAM_FIELDS: dict[str, list[tuple[str, str, Any]]] = {
    "dss_snowflake_dataset": [
        ("schema_name", "schema", ""),
        ("table", "table", ""),
        ("catalog", "catalog", None),
        ("write_mode", "writeMode", "OVERWRITE"),
    ],
    "dss_oracle_dataset": [
        ("schema_name", "schema", ""),
        ("table", "table", ""),
    ],
}


class DatasetHandler:
    """CRUD handler for DSS datasets.

    Handles DatasetResource, SnowflakeDatasetResource, and OracleDatasetResource.
    """

    def _get_project(self, ctx: EngineContext) -> DSSProject:
        return ctx.provider.client.get_project(ctx.project_key)

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

    def _apply_zone(
        self, project: DSSProject, dataset: DSSDataset, desired: DatasetResource
    ) -> None:
        """Move dataset to a flow zone if specified."""
        if desired.zone is None:
            return
        flow = project.get_flow()
        zone = flow.get_zone(desired.zone)
        zone.add_item(dataset)

    def _apply_format(self, settings: DSSDatasetSettings, desired: DatasetResource) -> None:
        """Set format type and params on the dataset settings."""
        if desired.format_type is None:
            return
        raw = settings.get_raw()
        raw["formatType"] = desired.format_type
        if desired.format_params:
            raw["formatParams"] = dict(desired.format_params)

    def _read_attrs(
        self,
        dataset: DSSDataset,
        resource_type: str,
        name: str,
        zone: str | None,
        dataset_type_fallback: str = "",
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
            "dataset_type": raw.get("type", dataset_type_fallback),
            "connection": raw.get("params", {}).get("connection"),
            "managed": raw.get("managed", False),
            "format_type": raw.get("formatType"),
            "format_params": raw.get("formatParams", {}),
            "columns": columns,
            "zone": zone,
        }

        params = raw.get("params", {})
        for model_field, dss_key, default in _EXTRA_PARAM_FIELDS.get(resource_type, []):
            attrs[model_field] = params.get(dss_key, default)

        return attrs

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
            dataset = project.create_dataset(desired.name, desired.dataset_type, params=params)

        settings = dataset.get_settings()
        self._apply_format(settings, desired)
        settings.save()

        self._apply_schema(dataset, desired)
        self._apply_metadata(dataset, desired)
        self._apply_zone(project, dataset, desired)

        return self._read_attrs(
            dataset, desired.resource_type, desired.name, desired.zone, desired.dataset_type
        )

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        """Read dataset from DSS. Returns None if deleted externally."""
        dataset = self._get_dataset(ctx, prior.name)
        if not dataset.exists():
            return None
        return self._read_attrs(
            dataset, prior.resource_type, prior.name, prior.attributes.get("zone")
        )

    def update(
        self, ctx: EngineContext, desired: DatasetResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        """Update a dataset in DSS."""
        _ = prior
        project = self._get_project(ctx)
        dataset = self._get_dataset(ctx, desired.name)
        settings = dataset.get_settings()

        raw = settings.get_raw()
        params = raw.setdefault("params", {})
        params.update(desired.to_dss_params())

        self._apply_format(settings, desired)
        settings.save()

        self._apply_schema(dataset, desired)
        self._apply_metadata(dataset, desired)
        self._apply_zone(project, dataset, desired)

        return self._read_attrs(
            dataset, desired.resource_type, desired.name, desired.zone, desired.dataset_type
        )

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        """Delete a dataset from DSS."""
        managed = prior.attributes.get("managed", False)
        dataset = self._get_dataset(ctx, prior.name)
        dataset.delete(drop_data=managed)

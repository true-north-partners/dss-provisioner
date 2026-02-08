"""Module examples used by examples/free/dss-provisioner.yaml."""

from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.dataset import FilesystemDatasetResource
from dss_provisioner.resources.recipe import SyncRecipeResource


def stage_pipeline(
    *,
    name: str,
    source_dataset: str,
    output_dataset: str,
    connection: str = "filesystem_managed",
    root_path: str = "${projectKey}/staging",
) -> list[Resource]:
    """Create one staged dataset + sync recipe."""
    return [
        FilesystemDatasetResource(
            name=output_dataset,
            connection=connection,
            path=f"{root_path}/{name}",
            managed=True,
            format_type="parquet",
            description=f"Staged features for {name}",
        ),
        SyncRecipeResource(
            name=f"sync_{name}",
            inputs=[source_dataset],
            outputs=[output_dataset],
            description=f"Syncs {source_dataset} -> {output_dataset}",
        ),
    ]

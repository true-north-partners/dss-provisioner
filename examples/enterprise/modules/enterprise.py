"""Module examples used by examples/enterprise/dss-provisioner.yaml."""

from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.dataset import FilesystemDatasetResource
from dss_provisioner.resources.recipe import SyncRecipeResource


def serving_stack(
    *,
    name: str,
    source_dataset: str,
    connection: str = "s3_managed",
    zone: str = "serving",
) -> list[Resource]:
    """Create a serving dataset + sync recipe for one model segment."""
    serving_dataset = f"{name}_serving"
    return [
        FilesystemDatasetResource(
            name=serving_dataset,
            connection=connection,
            path=f"${{projectKey}}/serving/{name}",
            managed=True,
            format_type="parquet",
            zone=zone,
            description=f"Serving table for {name}",
        ),
        SyncRecipeResource(
            name=f"publish_{name}",
            inputs=[source_dataset],
            outputs=[serving_dataset],
            zone=zone,
            description=f"Publishes {source_dataset} to {serving_dataset}",
        ),
    ]

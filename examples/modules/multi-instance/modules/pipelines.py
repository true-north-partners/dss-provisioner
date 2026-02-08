from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.dataset import FilesystemDatasetResource
from dss_provisioner.resources.recipe import SyncRecipeResource


def feature_pipeline(*, name: str, source_dataset: str, output_dataset: str) -> list[Resource]:
    return [
        FilesystemDatasetResource(
            name=output_dataset,
            connection="filesystem_managed",
            path=f"${{projectKey}}/features/{name}",
            format_type="parquet",
        ),
        SyncRecipeResource(
            name=f"build_{name}_features",
            inputs=[source_dataset],
            outputs=[output_dataset],
        ),
    ]

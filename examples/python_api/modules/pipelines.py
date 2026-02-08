from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.dataset import FilesystemDatasetResource
from dss_provisioner.resources.recipe import SyncRecipeResource


def scoring_pipeline(*, name: str, source_dataset: str) -> list[Resource]:
    scored = f"{name}_scores"
    return [
        FilesystemDatasetResource(
            name=scored,
            connection="filesystem_managed",
            path=f"${{projectKey}}/scores/{name}",
            format_type="parquet",
            managed=True,
        ),
        SyncRecipeResource(
            name=f"publish_{name}_scores",
            inputs=[source_dataset],
            outputs=[scored],
        ),
    ]

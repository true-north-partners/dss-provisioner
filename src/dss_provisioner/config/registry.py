"""Default resource type registry factory."""

from __future__ import annotations

from dss_provisioner.engine.dataset_handler import DatasetHandler
from dss_provisioner.engine.recipe_handler import (
    PythonRecipeHandler,
    SQLQueryRecipeHandler,
    SyncRecipeHandler,
)
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.variables_handler import VariablesHandler
from dss_provisioner.engine.zone_handler import ZoneHandler
from dss_provisioner.resources.dataset import (
    DatasetResource,
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
    UploadDatasetResource,
)
from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)
from dss_provisioner.resources.variables import VariablesResource
from dss_provisioner.resources.zone import ZoneResource


def default_registry() -> ResourceTypeRegistry:
    """Create a fresh registry with all built-in resource types and handlers."""
    registry = ResourceTypeRegistry()

    registry.register(VariablesResource, VariablesHandler())
    registry.register(ZoneResource, ZoneHandler())

    dataset_handler = DatasetHandler()
    registry.register(DatasetResource, dataset_handler)
    registry.register(SnowflakeDatasetResource, dataset_handler)
    registry.register(OracleDatasetResource, dataset_handler)
    registry.register(FilesystemDatasetResource, dataset_handler)
    registry.register(UploadDatasetResource, dataset_handler)

    registry.register(SyncRecipeResource, SyncRecipeHandler())
    registry.register(PythonRecipeResource, PythonRecipeHandler())
    registry.register(SQLQueryRecipeResource, SQLQueryRecipeHandler())

    return registry

"""Default resource type registry factory."""

from __future__ import annotations

from dss_provisioner.engine.code_env_handler import CodeEnvHandler
from dss_provisioner.engine.dataset_handler import DatasetHandler
from dss_provisioner.engine.exposed_object_handler import (
    ExposedDatasetHandler,
    ExposedManagedFolderHandler,
)
from dss_provisioner.engine.foreign_handler import (
    ForeignDatasetHandler,
    ForeignManagedFolderHandler,
)
from dss_provisioner.engine.git_library_handler import GitLibraryHandler
from dss_provisioner.engine.managed_folder_handler import ManagedFolderHandler
from dss_provisioner.engine.recipe_handler import (
    PythonRecipeHandler,
    SQLQueryRecipeHandler,
    SyncRecipeHandler,
)
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.scenario_handler import (
    PythonScenarioHandler,
    StepBasedScenarioHandler,
)
from dss_provisioner.engine.variables_handler import VariablesHandler
from dss_provisioner.engine.zone_handler import ZoneHandler
from dss_provisioner.resources.code_env import CodeEnvResource
from dss_provisioner.resources.dataset import (
    DatasetResource,
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
    UploadDatasetResource,
)
from dss_provisioner.resources.exposed_object import (
    ExposedDatasetResource,
    ExposedManagedFolderResource,
)
from dss_provisioner.resources.foreign import ForeignDatasetResource, ForeignManagedFolderResource
from dss_provisioner.resources.git_library import GitLibraryResource
from dss_provisioner.resources.managed_folder import (
    FilesystemManagedFolderResource,
    ManagedFolderResource,
    UploadManagedFolderResource,
)
from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)
from dss_provisioner.resources.scenario import (
    PythonScenarioResource,
    StepBasedScenarioResource,
)
from dss_provisioner.resources.variables import VariablesResource
from dss_provisioner.resources.zone import ZoneResource


def default_registry() -> ResourceTypeRegistry:
    """Create a fresh registry with all built-in resource types and handlers."""
    registry = ResourceTypeRegistry()

    registry.register(VariablesResource, VariablesHandler())
    registry.register(CodeEnvResource, CodeEnvHandler())
    registry.register(ZoneResource, ZoneHandler())
    registry.register(GitLibraryResource, GitLibraryHandler())

    managed_folder_handler = ManagedFolderHandler()
    registry.register(ManagedFolderResource, managed_folder_handler)
    registry.register(FilesystemManagedFolderResource, managed_folder_handler)
    registry.register(UploadManagedFolderResource, managed_folder_handler)

    dataset_handler = DatasetHandler()
    registry.register(DatasetResource, dataset_handler)
    registry.register(SnowflakeDatasetResource, dataset_handler)
    registry.register(OracleDatasetResource, dataset_handler)
    registry.register(FilesystemDatasetResource, dataset_handler)
    registry.register(UploadDatasetResource, dataset_handler)

    registry.register(ExposedDatasetResource, ExposedDatasetHandler())
    registry.register(ExposedManagedFolderResource, ExposedManagedFolderHandler())

    registry.register(ForeignDatasetResource, ForeignDatasetHandler())
    registry.register(ForeignManagedFolderResource, ForeignManagedFolderHandler())

    registry.register(SyncRecipeResource, SyncRecipeHandler())
    registry.register(PythonRecipeResource, PythonRecipeHandler())
    registry.register(SQLQueryRecipeResource, SQLQueryRecipeHandler())

    registry.register(StepBasedScenarioResource, StepBasedScenarioHandler())
    registry.register(PythonScenarioResource, PythonScenarioHandler())

    return registry

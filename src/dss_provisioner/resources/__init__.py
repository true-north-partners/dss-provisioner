"""DSS resource definitions."""

from dss_provisioner.resources.code_env import CodeEnvResource
from dss_provisioner.resources.dataset import (
    Column,
    DatasetResource,
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
    UploadDatasetResource,
)
from dss_provisioner.resources.exposed_object import (
    ExposedDatasetResource,
    ExposedManagedFolderResource,
    ExposedObjectResource,
)
from dss_provisioner.resources.foreign import ForeignDatasetResource, ForeignManagedFolderResource
from dss_provisioner.resources.git_library import GitLibraryResource
from dss_provisioner.resources.loader import resolve_code_files
from dss_provisioner.resources.managed_folder import (
    FilesystemManagedFolderResource,
    ManagedFolderResource,
    UploadManagedFolderResource,
)
from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    RecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)
from dss_provisioner.resources.scenario import (
    PythonScenarioResource,
    ScenarioResource,
    StepBasedScenarioResource,
)
from dss_provisioner.resources.variables import VariablesResource
from dss_provisioner.resources.zone import ZoneResource

__all__ = [
    "CodeEnvResource",
    "Column",
    "DatasetResource",
    "ExposedDatasetResource",
    "ExposedManagedFolderResource",
    "ExposedObjectResource",
    "FilesystemDatasetResource",
    "FilesystemManagedFolderResource",
    "ForeignDatasetResource",
    "ForeignManagedFolderResource",
    "GitLibraryResource",
    "ManagedFolderResource",
    "OracleDatasetResource",
    "PythonRecipeResource",
    "PythonScenarioResource",
    "RecipeResource",
    "SQLQueryRecipeResource",
    "ScenarioResource",
    "SnowflakeDatasetResource",
    "StepBasedScenarioResource",
    "SyncRecipeResource",
    "UploadDatasetResource",
    "UploadManagedFolderResource",
    "VariablesResource",
    "ZoneResource",
    "resolve_code_files",
]

"""Configuration models for YAML-based provisioning."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Discriminator, PrivateAttr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from dss_provisioner.config.modules import (
    ModuleSpec,  # noqa: TC001 — Pydantic needs this at runtime
)
from dss_provisioner.resources.base import Resource  # noqa: TC001 — Pydantic needs this at runtime
from dss_provisioner.resources.code_env import (
    CodeEnvResource,  # noqa: TC001 — Pydantic needs this at runtime
)
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
    ExposedObjectResource,
)
from dss_provisioner.resources.foreign import (
    ForeignDatasetResource,  # noqa: TC001 — Pydantic needs this at runtime
    ForeignManagedFolderResource,  # noqa: TC001 — Pydantic needs this at runtime
)
from dss_provisioner.resources.git_library import (
    GitLibraryResource,  # noqa: TC001 — Pydantic needs this at runtime
)
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
    ScenarioResource,
    StepBasedScenarioResource,
)
from dss_provisioner.resources.variables import (
    VariablesResource,  # noqa: TC001 — Pydantic needs this at runtime
)
from dss_provisioner.resources.zone import (
    ZoneResource,  # noqa: TC001 — Pydantic needs this at runtime
)


class ProviderConfig(BaseSettings):
    """DSS provider connection settings.

    Fields can be set via YAML (constructor kwargs) or environment variables
    with the ``DSS_`` prefix.  Constructor kwargs take precedence.

    ``api_key`` is typically provided via the ``DSS_API_KEY`` environment
    variable rather than YAML to avoid committing secrets to version control.
    """

    model_config = SettingsConfigDict(env_prefix="DSS_")

    host: str | None = None
    api_key: str | None = None
    project: str


def _none_to_list(v: Any) -> Any:
    return v if v is not None else []


def _type_aliases(*models: type[BaseModel]) -> dict[str, str]:
    """Collect ``yaml_alias`` → ``type`` default from model classes."""
    aliases: dict[str, str] = {}
    for m in models:
        yaml_alias = getattr(m, "yaml_alias", None)
        if yaml_alias is not None:
            aliases[yaml_alias] = m.model_fields["type"].default
    return aliases


def _type_normalizer(base: type[BaseModel]) -> Any:
    """Build a BeforeValidator callable that maps ``yaml_alias`` → DSS type value.

    Discovers aliases from direct subclasses of *base* that declare ``yaml_alias``.
    """
    aliases = _type_aliases(*base.__subclasses__())

    def _normalize(v: Any) -> Any:
        if isinstance(v, dict) and "type" in v:
            v["type"] = aliases.get(v["type"], v["type"])
        return v

    return _normalize


_ManagedFolderEntry = Annotated[
    FilesystemManagedFolderResource | UploadManagedFolderResource,
    BeforeValidator(_type_normalizer(ManagedFolderResource)),
    Discriminator("type"),
]

_DatasetEntry = Annotated[
    SnowflakeDatasetResource
    | OracleDatasetResource
    | FilesystemDatasetResource
    | UploadDatasetResource,
    BeforeValidator(_type_normalizer(DatasetResource)),
    Discriminator("type"),
]

_RecipeEntry = Annotated[
    PythonRecipeResource | SQLQueryRecipeResource | SyncRecipeResource,
    Discriminator("type"),
]

_ExposedObjectEntry = Annotated[
    ExposedDatasetResource | ExposedManagedFolderResource,
    BeforeValidator(_type_normalizer(ExposedObjectResource)),
    Discriminator("type"),
]

_ScenarioEntry = Annotated[
    StepBasedScenarioResource | PythonScenarioResource,
    BeforeValidator(_type_normalizer(ScenarioResource)),
    Discriminator("type"),
]


class Config(BaseModel):
    """Provisioning configuration — validates YAML structure directly."""

    provider: ProviderConfig
    state_path: Path = Path(".dss-state.json")
    variables: VariablesResource | None = None
    code_envs: CodeEnvResource | None = None
    zones: Annotated[list[ZoneResource], BeforeValidator(_none_to_list)] = []
    libraries: Annotated[list[GitLibraryResource], BeforeValidator(_none_to_list)] = []
    managed_folders: Annotated[list[_ManagedFolderEntry], BeforeValidator(_none_to_list)] = []
    datasets: Annotated[list[_DatasetEntry], BeforeValidator(_none_to_list)] = []
    exposed_objects: Annotated[list[_ExposedObjectEntry], BeforeValidator(_none_to_list)] = []
    foreign_datasets: Annotated[list[ForeignDatasetResource], BeforeValidator(_none_to_list)] = []
    foreign_managed_folders: Annotated[
        list[ForeignManagedFolderResource],
        BeforeValidator(_none_to_list),
    ] = []
    recipes: Annotated[list[_RecipeEntry], BeforeValidator(_none_to_list)] = []
    scenarios: Annotated[list[_ScenarioEntry], BeforeValidator(_none_to_list)] = []
    modules: Annotated[list[ModuleSpec], BeforeValidator(_none_to_list)] = []
    config_dir: Path = Path()

    _module_resources: list[Resource] = PrivateAttr(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def resources(self) -> list[Resource]:
        """All declared resources — ordering is not significant."""
        resources: list[Resource] = [
            *self.zones,
            *self.libraries,
            *self.managed_folders,
            *self.datasets,
            *self.exposed_objects,
            *self.foreign_datasets,
            *self.foreign_managed_folders,
            *self.recipes,
            *self.scenarios,
        ]
        if self.variables is not None:
            resources.append(self.variables)
        if self.code_envs is not None:
            resources.append(self.code_envs)
        resources.extend(self._module_resources)
        return resources

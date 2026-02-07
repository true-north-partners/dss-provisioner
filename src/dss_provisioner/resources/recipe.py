"""Recipe resource models for DSS recipes."""

from __future__ import annotations

from typing import Annotated, ClassVar, Literal, Self

from pydantic import BeforeValidator, Field, model_validator

from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.markers import Ref

_NonEmptyStr = Annotated[str, Field(min_length=1)]


def _coerce_str_to_list(v: str | list[_NonEmptyStr]) -> list[_NonEmptyStr]:
    if isinstance(v, str):
        return [v]
    return v


StrOrList = Annotated[list[_NonEmptyStr], BeforeValidator(_coerce_str_to_list)]


class RecipeResource(Resource):
    """Base resource for DSS recipes."""

    resource_type: ClassVar[str] = "dss_recipe"

    type: str
    inputs: Annotated[StrOrList, Ref()] = Field(default_factory=list)
    outputs: Annotated[StrOrList, Ref()] = Field(min_length=1)
    zone: Annotated[str | None, Ref("dss_zone")] = None


class SyncRecipeResource(RecipeResource):
    """Sync recipe resource."""

    resource_type: ClassVar[str] = "dss_sync_recipe"

    type: Literal["sync"] = "sync"


class PythonRecipeResource(RecipeResource):
    """Python recipe resource."""

    resource_type: ClassVar[str] = "dss_python_recipe"

    type: Literal["python"] = "python"
    code: str = ""
    code_env: str | None = None
    code_file: str | None = Field(default=None, exclude=True)
    code_wrapper: bool = Field(default=False, exclude=True)

    @model_validator(mode="after")
    def _check_code_or_file(self) -> Self:
        if self.code and self.code_file:
            msg = "Cannot set both 'code' and 'code_file'"
            raise ValueError(msg)
        return self


class SQLQueryRecipeResource(RecipeResource):
    """SQL query recipe resource."""

    resource_type: ClassVar[str] = "dss_sql_query_recipe"

    type: Literal["sql_query"] = "sql_query"
    inputs: Annotated[StrOrList, Ref()] = Field(min_length=1)
    code: str = ""
    code_file: str | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def _check_code_or_file(self) -> Self:
        if self.code and self.code_file:
            msg = "Cannot set both 'code' and 'code_file'"
            raise ValueError(msg)
        return self

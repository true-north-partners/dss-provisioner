"""Recipe resource models for DSS recipes."""

from __future__ import annotations

from typing import ClassVar, Literal, Self

from pydantic import Field, model_validator

from dss_provisioner.resources.base import Resource


class RecipeResource(Resource):
    """Base resource for DSS recipes."""

    resource_type: ClassVar[str] = "dss_recipe"

    recipe_type: str
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    zone: str | None = None


class SyncRecipeResource(RecipeResource):
    """Sync recipe resource."""

    resource_type: ClassVar[str] = "dss_sync_recipe"

    recipe_type: Literal["sync"] = "sync"


class PythonRecipeResource(RecipeResource):
    """Python recipe resource."""

    resource_type: ClassVar[str] = "dss_python_recipe"

    recipe_type: Literal["python"] = "python"
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

    recipe_type: Literal["sql_query"] = "sql_query"
    code: str = ""
    code_file: str | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def _check_code_or_file(self) -> Self:
        if self.code and self.code_file:
            msg = "Cannot set both 'code' and 'code_file'"
            raise ValueError(msg)
        return self

"""Tests for CodeEnvResource model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.code_env import CodeEnvResource


class TestCodeEnvResource:
    def test_address(self) -> None:
        r = CodeEnvResource()
        assert r.address == "dss_code_env.code_envs"

    def test_defaults(self) -> None:
        r = CodeEnvResource()
        assert r.name == "code_envs"
        assert r.default_python is None
        assert r.default_r is None
        assert r.description == ""
        assert r.tags == []
        assert r.depends_on == []

    def test_custom_values(self) -> None:
        r = CodeEnvResource(default_python="py39_ml", default_r="r_base")
        assert r.default_python == "py39_ml"
        assert r.default_r == "r_base"

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            CodeEnvResource(unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_excludes_none(self) -> None:
        r = CodeEnvResource(default_python="py39")
        dump = r.model_dump(exclude_none=True, exclude={"address"})
        assert "default_python" in dump
        assert "default_r" not in dump

    def test_plan_priority(self) -> None:
        assert CodeEnvResource.plan_priority == 5

    def test_name_literal(self) -> None:
        with pytest.raises(ValidationError, match="literal_error"):
            CodeEnvResource(name="custom")  # type: ignore[arg-type]

    def test_reference_names_empty(self) -> None:
        r = CodeEnvResource()
        assert r.reference_names() == []

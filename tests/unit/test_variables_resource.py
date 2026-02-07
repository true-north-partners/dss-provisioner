"""Tests for VariablesResource model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.variables import VariablesResource


class TestVariablesResource:
    def test_address(self) -> None:
        v = VariablesResource()
        assert v.address == "dss_variables.variables"

    def test_defaults(self) -> None:
        v = VariablesResource()
        assert v.name == "variables"
        assert v.standard == {}
        assert v.local == {}
        assert v.description == ""
        assert v.tags == []
        assert v.depends_on == []

    def test_custom_values(self) -> None:
        v = VariablesResource(
            standard={"env": "prod", "data_root": "/mnt/data"},
            local={"debug": "false"},
        )
        assert v.standard == {"env": "prod", "data_root": "/mnt/data"}
        assert v.local == {"debug": "false"}

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            VariablesResource(unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_shape(self) -> None:
        v = VariablesResource(standard={"k": "v"})
        dump = v.model_dump(exclude={"address"})
        assert dump["name"] == "variables"
        assert dump["standard"] == {"k": "v"}
        assert dump["local"] == {}
        assert "address" not in dump

    def test_reference_names_empty(self) -> None:
        v = VariablesResource()
        assert v.reference_names() == []

    def test_plan_priority(self) -> None:
        assert VariablesResource.plan_priority == 0

    def test_name_must_be_variables(self) -> None:
        with pytest.raises(ValidationError, match="literal_error"):
            VariablesResource(name="custom")  # type: ignore[arg-type]

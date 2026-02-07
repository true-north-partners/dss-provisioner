"""Tests for ZoneResource model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.zone import ZoneResource


class TestZoneResource:
    def test_address(self) -> None:
        z = ZoneResource(name="raw")
        assert z.address == "dss_zone.raw"

    def test_defaults(self) -> None:
        z = ZoneResource(name="raw")
        assert z.color == "#2ab1ac"
        assert z.description == ""
        assert z.tags == []
        assert z.depends_on == []

    def test_custom_color(self) -> None:
        z = ZoneResource(name="curated", color="#ff5733")
        assert z.color == "#ff5733"

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ZoneResource(name="raw", unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_shape(self) -> None:
        z = ZoneResource(name="raw", color="#123456")
        dump = z.model_dump(exclude={"address"})
        assert dump["name"] == "raw"
        assert dump["color"] == "#123456"
        assert "address" not in dump

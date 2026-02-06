from typing import ClassVar

import pytest

from dss_provisioner.engine.errors import UnknownResourceTypeError
from dss_provisioner.engine.handlers import ResourceHandler
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.resources.base import Resource


class DummyResource(Resource):
    resource_type: ClassVar[str] = "dummy"
    value: int


class DummyHandler(ResourceHandler["DummyResource"]):
    pass


def test_registry_register_and_get() -> None:
    registry = ResourceTypeRegistry()
    handler = DummyHandler()

    registry.register(DummyResource, handler)
    reg = registry.get("dummy")

    assert reg.resource_type == "dummy"
    assert reg.model is DummyResource
    assert reg.handler is handler


def test_registry_duplicate_registration() -> None:
    registry = ResourceTypeRegistry()
    handler = DummyHandler()

    registry.register(DummyResource, handler)
    with pytest.raises(ValueError):
        registry.register(DummyResource, handler)


def test_registry_unknown_type() -> None:
    registry = ResourceTypeRegistry()
    with pytest.raises(UnknownResourceTypeError):
        registry.get("missing")

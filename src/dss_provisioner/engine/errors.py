"""Engine error types."""

from __future__ import annotations

from typing import Any


class EngineError(Exception):
    """Base exception for engine errors."""


class UnknownResourceTypeError(EngineError):
    """Raised when a resource type has no registration/handler."""

    def __init__(self, resource_type: str) -> None:
        super().__init__(f"Unknown resource type: {resource_type}")
        self.resource_type = resource_type


class DuplicateAddressError(EngineError):
    """Raised when multiple desired resources share the same address."""

    def __init__(self, address: str) -> None:
        super().__init__(f"Duplicate resource address: {address}")
        self.address = address


class DependencyCycleError(EngineError):
    """Raised when dependencies contain a cycle."""

    def __init__(self, addresses: list[str]) -> None:
        msg = "Dependency cycle detected"
        if addresses:
            msg += f": {', '.join(addresses)}"
        super().__init__(msg)
        self.addresses = addresses


class StateProjectMismatchError(EngineError):
    """Raised when the on-disk state belongs to a different project."""

    def __init__(self, expected: str, got: str) -> None:
        super().__init__(f"State project_key mismatch: expected {expected}, got {got}")
        self.expected = expected
        self.got = got


class StalePlanError(EngineError):
    """Raised when applying a plan against a different state than planned."""


class StateLockError(EngineError):
    """Raised when the state lock cannot be acquired or released."""


class ValidationError(EngineError):
    """One or more resources failed plan validation."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        msg = "Validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(msg)


class ApplyError(EngineError):
    """Raised when an apply fails mid-way through.

    Carries the partial result (what was applied before the failure) so
    callers can inspect progress.  The original exception is chained via
    ``__cause__``.
    """

    def __init__(self, *, applied: list[Any], address: str, message: str) -> None:
        from dss_provisioner.engine.types import ApplyResult

        self.result = ApplyResult(applied=applied)
        self.address = address
        super().__init__(f"Apply failed on {address}: {message}")


class ApplyCanceled(EngineError):
    """Raised when an apply is canceled (e.g., Ctrl-C)."""

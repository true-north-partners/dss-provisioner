"""Local state locking."""

from __future__ import annotations

from typing import TYPE_CHECKING

from filelock import FileLock, Timeout

from dss_provisioner.engine.errors import StateLockError

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType


class StateLock:
    """Exclusive lock for a local state file."""

    def __init__(self, state_path: Path) -> None:
        self._lock = FileLock(str(state_path) + ".lock")

    def __enter__(self) -> StateLock:
        try:
            self._lock.acquire()
        except Timeout as e:
            raise StateLockError(str(e)) from e
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._lock.release()

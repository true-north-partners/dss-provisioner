"""Local state locking."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from dss_provisioner.engine.errors import StateLockError

if TYPE_CHECKING:
    from types import TracebackType

try:
    import fcntl  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


class StateLock:
    """Exclusive lock for a local state file."""

    def __init__(self, state_path: Path) -> None:
        self._lock_path = Path(str(state_path) + ".lock")
        self._file = None

    def __enter__(self) -> StateLock:
        # Keep fd open for lifetime of the lock.
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._lock_path.open("a+", encoding="utf-8")
        try:
            self._acquire()
        except Exception as e:
            try:
                self._file.close()
            finally:
                self._file = None
            raise StateLockError(str(e)) from e
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._file is None:
            return
        try:
            self._release()
        finally:
            self._file.close()
            self._file = None

    def _acquire(self) -> None:
        if self._file is None:
            raise StateLockError("Lock file is not open")

        if fcntl is not None:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
            return

        if sys.platform == "win32":  # pragma: no cover
            import msvcrt

            msvcrt.locking(self._file.fileno(), msvcrt.LK_LOCK, 1)
            return

        raise StateLockError("State locking is not supported on this platform")

    def _release(self) -> None:
        if self._file is None:
            return

        if fcntl is not None:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            return

        if sys.platform == "win32":  # pragma: no cover
            import msvcrt

            msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            return

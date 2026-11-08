"""The single-instance lock and the error types shared by node and backup.

Nothing here imports Reticulum, so the backup archive logic can use the
lock and raise its errors without loading RNS.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

try:  # POSIX only; the instance lock is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

log = logging.getLogger(__name__)


class NodeError(RuntimeError):
    """Raised when a node operation fails."""


class InstanceAlreadyRunning(NodeError):
    """The data directory belongs to another running node."""


class BackupError(NodeError):
    """Raised when a backup or restore fails (bad archive, bad target)."""


class InstanceLock:
    """Exclusive advisory lock marking a data directory as in use.

    A serving node holds this lock for its whole lifetime, so a second
    serving node on the same data directory fails fast instead of
    corrupting the shared peer registry, mailbox and Tan database.
    The lock file records the holder's PID for diagnostics; the
    ``flock`` itself is the guard, so a file left behind by a crashed
    node never blocks a restart.  POSIX only — without ``fcntl`` the
    lock is a no-op, mirroring the mailbox's cross-process locking.
    """

    def __init__(self, path: Path, data_dir: Path) -> None:
        self.path = Path(path)
        self.data_dir = Path(data_dir)
        self._handle: Optional[Any] = None

    def acquire(self) -> None:
        """Take the lock, raising :class:`NodeError` when it is held."""
        if self._handle is not None or fcntl is None:
            return
        handle = open(self.path, "a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            holder = self._read_holder_pid(handle)
            handle.close()
            detail = f" (pid {holder})" if holder is not None else ""
            raise InstanceAlreadyRunning(
                "another tau node is already serving on data directory "
                f"{self.data_dir}{detail}; stop it before starting a "
                "second node on the same directory"
            ) from None
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        self._handle = handle

    @staticmethod
    def _read_holder_pid(handle: Any) -> Optional[int]:
        try:
            handle.seek(0)
            text = handle.read().strip()
        except OSError:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    def release(self) -> None:
        """Release the lock; safe to call when it is not held."""
        handle, self._handle = self._handle, None
        if handle is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:  # pragma: no cover - defensive
            log.debug("Could not unlock %s", self.path, exc_info=True)
        handle.close()

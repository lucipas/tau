"""Private local IPC for fetching Tan media through the serving node."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict

from .local_ipc import LocalIPCUnavailable, LocalServer, call_local
from .node import NodeError

_MEDIA_SOCKET_NAME = "media.sock"


class MediaServer(LocalServer):
    """Single worker, stopped and joined before the owning node releases its lock."""

    def __init__(
        self, data_dir: Path, fetch: Callable[..., Dict[str, Any]]
    ) -> None:
        super().__init__(
            data_dir, _MEDIA_SOCKET_NAME, fetch, thread_name="tau-media"
        )


def fetch_running(data_dir: Path, **request: Any) -> Dict[str, Any]:
    """Ask the serving node to fetch one Tan image; returns its result."""
    try:
        return call_local(data_dir / _MEDIA_SOCKET_NAME, **request)
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "could not fetch through the running node; ensure it is running "
            "with --social on a build that serves media (restart it after "
            "upgrading)"
        ) from exc

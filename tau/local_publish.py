"""Private local IPC for publishing through the node that owns the data lock."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict

from .local_ipc import LocalIPCUnavailable, LocalServer, call_local
from .node import NodeError

_PUBLISH_SOCKET_NAME = "publish.sock"


class PublishServer(LocalServer):
    """Single worker, stopped and joined before the owning node releases its lock."""

    def __init__(
        self, data_dir: Path, publish: Callable[..., Dict[str, Any]]
    ) -> None:
        super().__init__(
            data_dir, _PUBLISH_SOCKET_NAME, publish, thread_name="tau-publish"
        )


def publish_running(data_dir: Path, **request: Any) -> Dict[str, Any]:
    """Send once: a lost reply may mean the post was already published."""
    try:
        return call_local(data_dir / _PUBLISH_SOCKET_NAME, **request)
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "could not publish through the running node; ensure it is running "
            "with --social (a lost reply may mean the post was published)"
        ) from exc

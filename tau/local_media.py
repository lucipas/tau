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


# Slack beyond the requested fetch timeout for the serving node to finish
# its own resolution and transfer bookkeeping before replying.
_REPLY_SLACK_SECONDS = 30.0


def fetch_running(data_dir: Path, **request: Any) -> Dict[str, Any]:
    """Ask the serving node to fetch one Tan image; returns its result."""
    timeout = request.get("timeout")
    reply_timeout = (
        float(timeout) + _REPLY_SLACK_SECONDS
        if isinstance(timeout, (int, float))
        else None
    )
    try:
        return call_local(
            data_dir / _MEDIA_SOCKET_NAME,
            reply_timeout=reply_timeout,
            **request,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "could not fetch through the running node; ensure it is running "
            "with --social on a build that serves media (restart it after "
            "upgrading)"
        ) from exc

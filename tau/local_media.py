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
    """Ask the serving node to fetch one Tan image; returns its result.

    The client-side reply wait is bounded by the fetch timeout the
    caller asked for, plus slack for the node's own stages — without
    it a serving node that stops responding leaves the caller hung
    on the socket long after its own timeout passed.
    """
    timeout = float(request.get("timeout") or 60.0)
    try:
        return call_local(
            data_dir / _MEDIA_SOCKET_NAME,
            reply_timeout=timeout + 30.0,
            **request,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "could not fetch through the running node; ensure it is running "
            "with --social on a build that serves media (restart it after "
            "upgrading)"
        ) from exc

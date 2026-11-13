"""Private local IPC for sending through the node that owns the data lock."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from .local_ipc import LocalServer, call_local, local_server_available

_SEND_SOCKET_NAME = "send.sock"


class SendServer(LocalServer):
    """Single worker, stopped and joined before the owning node releases its lock."""

    def __init__(self, data_dir: Path, send: Any) -> None:
        super().__init__(data_dir, _SEND_SOCKET_NAME, send, thread_name="tau-send")


def send_running(data_dir: Path, *, reply_timeout: Optional[float] = None, **request: Any) -> Dict[str, Any]:
    """Send once through the serving node: a lost reply may mean the message was sent.

    ``reply_timeout`` bounds the client's wait for the reply (see :func:`call_local`);
    without it the read waits indefinitely, as sends and retracts need.
    It is never part of the request sent over the socket; any ``timeout``
    in *request* is the operation timeout and is forwarded to the server.
    """
    return call_local(data_dir / _SEND_SOCKET_NAME, reply_timeout=reply_timeout, **request)


def send_server_available(data_dir: Path) -> bool:
    """True when a serving node is answering send requests on its local socket."""
    return local_server_available(data_dir / _SEND_SOCKET_NAME)

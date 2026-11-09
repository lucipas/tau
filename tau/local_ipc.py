"""Private local IPC between one-shot commands and the serving node.

One Unix socket per command family, owned by the process that holds the
data-directory lock.  A one-shot command probes the socket first: when a
serving node answers, the work runs on that node instead of starting a
second Reticulum instance on the same identity and data directory.  The
wire format is one JSON object per connection, newline-terminated, capped
at 64 KiB; the server replies ``{"result": ...}`` or ``{"error": ...}``.
"""

from __future__ import annotations

import json
import os
import socket
import socketserver
import threading
from pathlib import Path
from typing import Any, Callable, Dict

from .node import NodeError

_MAX_BYTES = 64 * 1024


class LocalIPCUnavailable(Exception):
    """The serving node is not answering on its local socket."""


def _read(stream: Any) -> Dict[str, Any]:
    raw = stream.readline(_MAX_BYTES + 1)
    if len(raw) > _MAX_BYTES or not raw.endswith(b"\n"):
        raise ValueError("invalid local IPC request size")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


class LocalServer:
    """Single worker, stopped and joined before the owning node releases its lock."""

    def __init__(
        self,
        data_dir: Path,
        socket_name: str,
        handler: Callable[..., Dict[str, Any]],
        *,
        thread_name: str = "tau-local",
    ) -> None:
        self.path = data_dir / socket_name

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                self.connection.settimeout(10)
                try:
                    request = _read(self.rfile)
                    result = handler(**request)
                    response = {"result": result}
                except Exception as exc:
                    response = {"error": str(exc)}
                try:
                    self.wfile.write(json.dumps(response).encode() + b"\n")
                except OSError:
                    pass  # A disconnected client must never cause a retry.

        # Only the instance-lock owner may remove a stale socket.
        self.path.unlink(missing_ok=True)
        self.server = socketserver.UnixStreamServer(
            str(self.path), Handler, bind_and_activate=False
        )
        try:
            self.server.server_bind()
            os.chmod(self.path, 0o600)
            self.server.server_activate()
            self.thread = threading.Thread(
                target=self.server.serve_forever, name=thread_name, daemon=True
            )
            self.thread.start()
        except BaseException:
            self.server.server_close()
            self.path.unlink(missing_ok=True)
            raise

    def stop(self) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()
        self.path.unlink(missing_ok=True)


def call_local(path: Path, **request: Any) -> Dict[str, Any]:
    """Send once: a lost reply may mean the request was already handled."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(10)
            client.connect(str(path))
            client.sendall(json.dumps(request).encode() + b"\n")
            # Handling can wait on network peers. Do not retry on timeout.
            client.settimeout(None)
            with client.makefile("rb") as stream:
                response = _read(stream)
    except (OSError, ValueError) as exc:
        raise LocalIPCUnavailable(str(exc)) from exc
    if "error" in response:
        raise NodeError(str(response["error"]))
    return response["result"]


def local_server_available(path: Path) -> bool:
    """True when something answers on the local socket at *path*.

    A stale socket file left by a crashed serve answers nothing, so this
    is the probe one-shot commands use before deciding whether a serving
    node is really there.
    """
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.connect(str(path))
    except OSError:
        return False
    return True

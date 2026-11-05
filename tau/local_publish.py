"""Private local IPC for publishing through the node that owns the data lock."""

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


def _read(stream: Any) -> Dict[str, Any]:
    raw = stream.readline(_MAX_BYTES + 1)
    if len(raw) > _MAX_BYTES or not raw.endswith(b"\n"):
        raise ValueError("invalid local publishing request size")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


class PublishServer:
    """Single worker, stopped and joined before the owning node releases its lock."""

    def __init__(self, data_dir: Path, publish: Callable[..., Dict[str, Any]]) -> None:
        self.path = data_dir / "publish.sock"

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                self.connection.settimeout(10)
                try:
                    request = _read(self.rfile)
                    result = publish(**request)
                    response = {"result": result}
                except Exception as exc:
                    response = {"error": str(exc)}
                try:
                    self.wfile.write(json.dumps(response).encode() + b"\n")
                except OSError:
                    pass  # A disconnected client must never cause a retry.

        # Only the instance-lock owner may remove a stale socket.
        self.path.unlink(missing_ok=True)
        self.server = socketserver.UnixStreamServer(str(self.path), Handler,
                                                    bind_and_activate=False)
        try:
            self.server.server_bind()
            os.chmod(self.path, 0o600)
            self.server.server_activate()
            self.thread = threading.Thread(target=self.server.serve_forever,
                                           name="tau-publish", daemon=True)
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


def publish_running(data_dir: Path, **request: Any) -> Dict[str, Any]:
    """Send once: a lost reply may mean the post was already published."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(10)
            client.connect(str(data_dir / "publish.sock"))
            client.sendall(json.dumps(request).encode() + b"\n")
            # Publishing can wait on several network peers. Do not retry on timeout.
            client.settimeout(None)
            with client.makefile("rb") as stream:
                response = _read(stream)
    except (OSError, ValueError) as exc:
        raise NodeError(
            "could not publish through the running node; ensure it is running "
            "with --social (a lost reply may mean the post was published)"
        ) from exc
    if "error" in response:
        raise NodeError(str(response["error"]))
    return response["result"]

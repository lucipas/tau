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
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .identity import IdentityError
from .mailbox import MailboxError
from .node import NodeError
from .peers import PeerError
from .protocol import ProtocolError

_MAX_BYTES = 64 * 1024
# A reply may carry a peer's response (up to the protocol's 64 KiB) plus
# the local message id, result fields, JSON escaping and the IPC wrapper.
_MAX_RESPONSE_BYTES = 1024 * 1024

# Error types the one-shot path lets reach the caller, re-raised as the same
# type on the client so both paths treat them identically.  ValueError is
# included: a usage error raised on the serving node (an unknown group on a
# retract) must reach the caller as that error, not be re-wrapped as a
# delivery failure.
_REMOTE_ERRORS = {
    cls.__name__: cls
    for cls in (
        NodeError,
        PeerError,
        MailboxError,
        ProtocolError,
        IdentityError,
        ValueError,
    )
}


class LocalIPCUnavailable(Exception):
    """The serving node is not answering on its local socket."""


class LocalIPCConnectFailed(LocalIPCUnavailable):
    """The connect itself failed, so no request bytes were sent.

    Raised only when the socket path is missing or no listener owns
    it — a refused connection, not a timeout.  The request was never
    sent, so the caller may retry it elsewhere (a fallback socket);
    a failure after the connect, including a reply timeout, must
    never be retried, because the first server may already have
    handled the request.
    """


def _read(stream: Any, limit: int = _MAX_BYTES) -> Dict[str, Any]:
    raw = stream.readline(limit + 1)
    if len(raw) > limit or not raw.endswith(b"\n"):
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
                    response = {"error": str(exc), "kind": type(exc).__name__}
                try:
                    self.wfile.write(json.dumps(response).encode() + b"\n")
                except OSError:
                    pass  # A disconnected client must never cause a retry.

        # Only the instance-lock owner may remove a stale socket.
        self.path.unlink(missing_ok=True)
        self.server = socketserver.UnixStreamServer(
            str(self.path), Handler, bind_and_activate=False
        )
        # A single-worker server queues every request it cannot take
        # yet in the listen backlog; the five-slot default is stingy
        # when one request can keep the worker busy for minutes, so
        # give the queue room for concurrent clients instead of
        # refusing them (issue #455, Qodo Warning on PR #456).
        self.server.request_queue_size = 32
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


def call_local(path: Path, *, reply_timeout: Optional[float] = None, **request: Any) -> Dict[str, Any]:
    """Send once: a lost reply may mean the request was already handled.

    ``reply_timeout`` bounds the wait for the reply in seconds, on the
    client side only; it is never sent over the socket.  Without it
    the read waits indefinitely — the right default for sends and
    retracts, whose handling can wait on network peers — but a
    best-effort notification (the console-copy drop) must never hang
    the caller, so it passes a bound.
    """
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(10)
            try:
                client.connect(str(path))
            except (ConnectionRefusedError, FileNotFoundError) as exc:
                # No listener owns the path: nothing was sent, so the
                # caller may retry the request elsewhere.
                raise LocalIPCConnectFailed(str(exc)) from exc
            except BlockingIOError as exc:
                # The listener exists but its backlog is full: a Unix
                # connect fails fast instead of waiting, so retry it
                # until a slot frees or the connect budget runs out.
                # The request was never sent, and it must queue behind
                # this listener rather than spill onto another worker.
                deadline = time.monotonic() + 10.0
                while True:
                    if time.monotonic() >= deadline:
                        raise LocalIPCUnavailable(str(exc)) from exc
                    time.sleep(0.05)
                    try:
                        client.connect(str(path))
                        break
                    except BlockingIOError as retry_exc:
                        exc = retry_exc
                        continue
            client.sendall(json.dumps(request).encode() + b"\n")
            # Handling can wait on network peers. Do not retry on timeout.
            # A caller that passes `reply_timeout` keeps that bound instead:
            # the reply read then fails with LocalIPCUnavailable.
            client.settimeout(reply_timeout)
            with client.makefile("rb") as stream:
                response = _read(stream, _MAX_RESPONSE_BYTES)
    except (OSError, ValueError) as exc:
        raise LocalIPCUnavailable(str(exc)) from exc
    if "error" in response:
        error_type = _REMOTE_ERRORS.get(str(response.get("kind")), NodeError)
        raise error_type(str(response["error"]))
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


def local_server_listening(path: Path, *, timeout: float = 2.0) -> bool:
    """True when a server owns the local socket at *path*, even when it is too busy to answer.

    Unlike :func:`local_server_available`, a connect that times out
    still counts: the listener exists but its backlog is full, so the
    caller should queue behind it rather than treat the server as
    absent.  Only a refused connection (a stale socket file) or a
    missing socket counts as no server.  Any other connect failure
    also counts as no server — the safe direction is to fall back,
    since the listener may simply be unusable.
    """
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(timeout)
            client.connect(str(path))
    except (ConnectionRefusedError, FileNotFoundError):
        return False
    except (socket.timeout, BlockingIOError):
        # The listener exists but its backlog is full: a Unix connect
        # fails fast with EAGAIN instead of waiting, so a failed
        # connect still proves a server owns this socket — the caller
        # queues behind it rather than treating it as absent.
        return True
    except OSError:
        return False
    return True

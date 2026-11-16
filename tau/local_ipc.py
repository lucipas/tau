"""Private local IPC between one-shot commands and the serving node.

One Unix socket per command family, owned by the process that holds the
data-directory lock.  A one-shot command probes the socket first: when a
serving node answers, the work runs on that node instead of starting a
second Reticulum instance on the same identity and data directory.  The
wire format is one JSON object per connection, newline-terminated, capped
at 64 KiB; the server replies ``{"result": ...}`` or ``{"error": ...}``.
"""

from __future__ import annotations

import errno
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

# How long a client waits for a connect to a listener whose backlog is
# full before giving up.  The listener is still a running worker, so
# the wait is bounded rather than a fallback: a group op must not be
# rerouted to the main socket just because the fan-out worker is busy.
_CONNECT_TIMEOUT_SECONDS = 10.0

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


def _is_absent_listener(exc: OSError) -> bool:
    """True when a connect failure means no listener is there at all.

    The socket file is missing, or nothing is listening on it (a stale
    file left by a crashed serve).  A timeout, or a full backlog
    (``EAGAIN``), means the opposite — a listener exists — so neither
    is absent.
    """
    return exc.errno in (errno.ENOENT, errno.ECONNREFUSED)


def _is_full_backlog(exc: OSError) -> bool:
    """True when a connect failed only because the listener's backlog is full.

    The listener exists and is running; the client just has to wait
    for a slot to free.
    """
    return exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK)


def _connect(path: Path, timeout: float = _CONNECT_TIMEOUT_SECONDS) -> socket.socket:
    """Connect to a local socket, waiting for a full backlog to drain.

    A listener whose backlog is full is still a running worker, so a
    connect that fails with ``EAGAIN`` is retried — on a fresh socket
    each time, since a failed connect leaves the socket unusable —
    until *timeout* seconds have passed.  A failure that proves the
    listener absent, or any other error, is raised at once.
    """
    deadline = time.monotonic() + timeout
    while True:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.settimeout(timeout)
            client.connect(str(path))
            return client
        except OSError as exc:
            client.close()
            if _is_full_backlog(exc) and time.monotonic() < deadline:
                time.sleep(0.05)
                continue
            raise


def call_local(path: Path, *, reply_timeout: Optional[float] = None, fallback_path: Optional[Path] = None, **request: Any) -> Dict[str, Any]:
    """Send once: a lost reply may mean the request was already handled.

    ``reply_timeout`` bounds the wait for the reply in seconds, on the
    client side only; it is never sent over the socket.  Without it
    the read waits indefinitely — the right default for sends and
    retracts, whose handling can wait on network peers — but a
    best-effort notification (the console-copy drop) must never hang
    the caller, so it passes a bound.

    ``fallback_path`` names a second socket that may carry the request
    when the first has no listener: socket selection and connection
    are one operation, so a request is never sent twice.  Only a
    connect failure that proves the listener absent (a missing socket
    file, or a stale one nothing listens on) falls back, and only
    before any request bytes are sent.  A connect that fails because
    the listener's backlog is full means the worker is running, so
    the call waits for a slot (bounded by the connect timeout) and
    then fails loudly rather than silently routing the request
    somewhere it does not belong.  Once the request is written,
    nothing is retried anywhere.
    """
    try:
        client = _connect(path)
    except OSError as exc:
        if fallback_path is not None and _is_absent_listener(exc):
            # Nothing was sent, so the fallback socket can carry the
            # request instead.  This return sits outside the try below:
            # a remote error the fallback server reports (a usage
            # error re-raised as ValueError, say) must reach the caller
            # as that error, never re-wrapped as unavailable.
            return call_local(
                fallback_path, reply_timeout=reply_timeout, **request
            )
        raise LocalIPCUnavailable(str(exc)) from exc
    try:
        with client:
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
    """True when a serving node is answering on the local socket at *path*.

    A stale socket file left by a crashed serve answers nothing, so this
    is the probe one-shot commands use before deciding whether a serving
    node is really there.  A connect that times out, or fails with a
    full backlog, is not an absence: a listener exists, only busy, so
    the serving node is there and the probe reports it present.
    """
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.connect(str(path))
    except TimeoutError:
        return True
    except OSError as exc:
        return _is_full_backlog(exc)
    return True

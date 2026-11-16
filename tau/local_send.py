"""Private local IPC for sending through the node that owns the data lock.

Two sockets, two workers: the main send socket carries one-shot
sends, edits and retractions, while a second fan-out socket carries
the group fan-out ops (``group_send``, ``retract_group``,
``edit_group``, ``typing_group``).  A group send fans one message
out to every member — and an optional ``--wait`` — so on a single
worker it monopolizes the socket while later sends, edits and
retractions wait behind it (issue #455).  The fan-out worker runs
on its own thread, so a slow fan-out cannot block the main socket.

The two workers call node methods concurrently, which is the same
concurrency the node already sees from its Reticulum callback and
mail threads: the shared subsystems serialize their own state (the
mailbox has an RLock, the group store a Lock, and ``flush_outbox``
skips a round while the mail lock is busy), so no extra lock is
needed around the dispatch itself.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from .local_ipc import LocalServer, call_local, local_server_available

_SEND_SOCKET_NAME = "send.sock"
_FANOUT_SOCKET_NAME = "send-fanout.sock"

#: Ops that fan one request out to every group member.  These are the
#: long-lived requests: each leg is bounded by its own timeout, so a
#: group with several unreachable members — or a ``--wait`` — keeps
#: the worker busy far longer than a single send.  They travel on the
#: fan-out socket; everything else stays on the main send socket.
FANOUT_OPS = frozenset({"group_send", "retract_group", "edit_group", "typing_group"})


class SendServer(LocalServer):
    """Single worker, stopped and joined before the owning node releases its lock."""

    def __init__(self, data_dir: Path, send: Any) -> None:
        super().__init__(data_dir, _SEND_SOCKET_NAME, send, thread_name="tau-send")


class FanoutServer(LocalServer):
    """Second worker for the group fan-out ops, on its own socket.

    Same handler as the main send server — the dispatch is shared —
    but its own thread, so a slow fan-out cannot block the main
    socket.  Best-effort like the main server: when it cannot bind,
    fan-out ops fall back to the main socket (see :func:`send_running`).
    """

    def __init__(self, data_dir: Path, send: Any) -> None:
        super().__init__(
            data_dir, _FANOUT_SOCKET_NAME, send, thread_name="tau-send-fanout"
        )


def send_running(data_dir: Path, *, reply_timeout: Optional[float] = None, **request: Any) -> Dict[str, Any]:
    """Send once through the serving node: a lost reply may mean the message was sent.

    ``reply_timeout`` bounds the client's wait for the reply (see :func:`call_local`);
    without it the read waits indefinitely, as sends and retracts need.
    It is never part of the request sent over the socket; any ``timeout``
    in *request* is the operation timeout and is forwarded to the server.

    A fan-out op (see :data:`FANOUT_OPS`) goes to the fan-out socket
    when a fan-out worker is answering there, so it cannot block the
    main socket; when no fan-out worker answers — an older serving
    node, or a fan-out server that could not bind — it falls back to
    the main socket instead of failing.  The probe only connects, so
    a fallback decision is made before anything is sent: a request
    is never sent twice.
    """
    if request.get("op") in FANOUT_OPS and send_fanout_server_available(data_dir):
        return call_local(
            data_dir / _FANOUT_SOCKET_NAME, reply_timeout=reply_timeout, **request
        )
    return call_local(data_dir / _SEND_SOCKET_NAME, reply_timeout=reply_timeout, **request)


def send_server_available(data_dir: Path) -> bool:
    """True when a serving node is answering send requests on its local socket."""
    return local_server_available(data_dir / _SEND_SOCKET_NAME)


def send_fanout_server_available(data_dir: Path) -> bool:
    """True when a serving node is answering fan-out requests on its local socket."""
    return local_server_available(data_dir / _FANOUT_SOCKET_NAME)

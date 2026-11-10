"""Local HTTP bridge and web console for a running tau node.

The bridge exposes a small JSON API over loopback so the browser (or any local
script) can read what arrives from the mesh and send messages back:

    GET  /api/status       node identity, interfaces and counters
    GET  /api/peers        peers from the local registry
    GET  /api/groups       known group DMs with their member lists
    GET  /api/messages     recent inbound/outbound messages
    POST /api/messages     send ``{"to": "...", "text": "..."}`` (optional ``"reply_to"`` wire id,
                           ``"expires_in"`` seconds for a disappearing message)
    POST /api/react        react ``{"target": "<wire id>", "reaction": "👍"}``
                           or retract ``{"target": "<wire id>", "clear": true}``
    GET  /api/outbox       queued mail waiting on offline peers
    POST /api/outbox/cancel cancel ``{"target": "<message id or prefix>"}``    POST /api/ping         measure reachability of a peer
    POST /api/announce     announce this node immediately
    GET  /api/events       server-sent events for live messages, peers and posts
    GET  /api/tan/timeline followee timeline (or all cached posts)
    GET  /api/tan/following followed authors
    POST /api/tan/posts    publish a Tan post
    POST /api/tan/like     like a post (signed reaction on your chain)
    POST /api/tan/unlike   remove your like from a post
    POST /api/tan/delete   delete one of your own posts
    POST /api/tan/follow   follow an author
    POST /api/tan/unfollow stop following an author
    POST /api/tan/sync     pull missing posts from peers

If a static build of the Next.js console exists (``web/out``), it is served
from the same origin.  Everything binds to loopback by default because anyone
who can reach this API can send messages as this node.
"""

from __future__ import annotations

import errno
import json
import logging
import math
import mimetypes
import queue
import socket
import sys
import threading
import time
from collections import OrderedDict, deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__
from .config import MESSAGES_FILE_NAME
from .inbox import MAX_REACTIONS, merge_reaction
from .mailbox import MailboxError
from .node import NodeError, TauNode
from .peers import Peer, PeerError
from .protocol import (
    MAX_MAIL_TTL,
    MAX_MESSAGE_CHARS,
    ProtocolError,
    clean_in_reply_to,
    clean_reaction,
    excerpt_hash,
    normalize_message,
    sanitize_name,
    short_hash,
)
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

DEFAULT_WEB_PORT = 8787
DEFAULT_WEB_HOST = "127.0.0.1"

MAX_BODY_BYTES = 32 * 1024
MAX_MESSAGE_LOG = 500
# Delivery reports buffered while their console record does not exist
# yet (the flush can deliver before send_message records the send).
MAX_EARLY_DELIVERIES = 128
MAX_EVENT_LOG = 500
MAX_SUBSCRIBERS = 32
SUBSCRIBER_QUEUE_SIZE = 256
SSE_HEARTBEAT_SECONDS = 15.0

_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
_LOCAL_ORIGIN_HOSTS = {"localhost", "127.0.0.1", "::1"}


class WebBridgeError(RuntimeError):
    """Raised when the web bridge cannot start or process a request."""


class RequestError(WebBridgeError):
    """Raised for malformed or oversized HTTP requests."""


def is_loopback_host(host: str) -> bool:
    return host.strip().lower() in _LOOPBACK_HOSTS


def parse_bind(value: str) -> Tuple[str, int]:
    """Parse ``host:port`` or a bare port into an address tuple."""
    from .config import parse_host_port

    return parse_host_port(value, default_host=DEFAULT_WEB_HOST)


def _web_console_bind_error(host: str, port: int, exc: OSError) -> WebBridgeError:
    """Name a web console bind failure instead of surfacing a bare errno."""
    if exc.errno == errno.EADDRINUSE:
        return WebBridgeError(
            f"web console port {port} on {host} is already in use; "
            "another tau node (or another process) is already "
            "listening there. Stop it, or choose a different "
            "console port with --http"
        )
    return WebBridgeError(f"cannot listen for the web console on {host}:{port}: {exc}")


def check_web_console_bind(host: str, port: int) -> None:
    """Fail fast when the web console address cannot be bound.

    Mirrors what :meth:`WebBridge.start` does when it builds its
    ``ThreadingHTTPServer`` — an ``AF_INET`` socket with ``SO_REUSEADDR``
    (the server sets ``allow_reuse_address``), so a socket merely in
    TIME_WAIT does not trip the check — and raises :class:`WebBridgeError`
    naming the console address instead of a bare errno.

    Only ``tau serve`` calls this.  Transient commands never start a
    bridge, so they skip the check.

    The probe mirrors the TCP listen probe (``check_tcp_listen_ports``)
    on POSIX: an ``AF_INET`` socket with ``SO_REUSEADDR``, so a socket
    merely in TIME_WAIT does not trip the check.  Windows gets a
    different probe (see :func:`_check_web_console_bind_windows`):
    ``SO_REUSEADDR`` there lets a socket bind a port that a live
    listener already holds, so a bind probe cannot tell a live
    listener from a recently closed connection in TIME_WAIT.
    """
    if sys.platform == "win32":
        _check_web_console_bind_windows(host, port)
        return
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # SO_REUSEADDR here means "TIME_WAIT is fine" (POSIX). Windows
        # never reaches this branch (see above).
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
    except OSError as exc:
        raise _web_console_bind_error(host, port, exc) from exc
    finally:
        sock.close()


def _check_web_console_bind_windows(host: str, port: int) -> None:
    """Windows console probe: connect, do not bind.

    A bind probe cannot work on Windows: ``SO_REUSEADDR`` there lets a
    socket bind a port that a live listener already holds, and omitting
    it makes the probe trip on a recently closed connection in
    TIME_WAIT — while the server itself binds with ``SO_REUSEADDR``
    (``_WebServer.allow_reuse_address``), so the late-bind path cannot
    catch the collision either.  Connecting instead distinguishes the
    two cases: a live listener completes the handshake, while a port
    left in TIME_WAIT refuses the connection.
    """
    try:
        with socket.create_connection((host, port), timeout=1.0):
            pass
    except ConnectionRefusedError:
        return
    except OSError as exc:
        raise _web_console_bind_error(host, port, exc) from exc
    raise WebBridgeError(
        f"web console port {port} on {host} is already in use; "
        "another tau node (or another process) is already "
        "listening there. Stop it, or choose a different "
        "console port with --http"
    )


def _is_local_origin(origin: str) -> bool:
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    return host in _LOCAL_ORIGIN_HOSTS or host.endswith(".localhost")


class EventBus:
    """Fan out events to server-sent-event subscribers with replay."""

    def __init__(self, limit: int = MAX_EVENT_LOG) -> None:
        self._lock = threading.Lock()
        self._events: Deque[Dict[str, Any]] = deque(maxlen=limit)
        self._subscribers: Set[queue.Queue] = set()
        self._next_id = 1
        self._closed = False

    def publish(self, event: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if self._closed:
                return event
            payload = dict(event)
            payload["id"] = self._next_id
            self._next_id += 1
            self._events.append(payload)
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(payload)
            except queue.Full:
                log.debug("Dropping event for a slow subscriber")
        return payload

    def replay(self, since: int = 0) -> List[Dict[str, Any]]:
        with self._lock:
            return [event for event in self._events if event["id"] > since]

    def subscribe(self) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE_SIZE)
        with self._lock:
            if len(self._subscribers) >= MAX_SUBSCRIBERS:
                raise WebBridgeError("too many event subscribers")
            self._subscribers.add(subscriber)
        return subscriber

    def subscriber_count(self) -> int:
        """Live SSE subscribers — whether anyone is watching the console."""
        with self._lock:
            return len(self._subscribers)

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            self._subscribers.discard(subscriber)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscribers = list(self._subscribers)
            self._subscribers.clear()
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(None)
            except queue.Full:
                pass


def _clean_stored_reaction(entry: Any) -> Optional[Dict[str, Any]]:
    """Validate one reaction stored on a console history entry.

    Returns a normalised copy, or ``None`` when the entry is not a
    recognisable reaction (corrupt or hand-edited history never blocks
    startup; the bad entry is simply dropped).
    """
    if not isinstance(entry, dict):
        return None
    reactor = entry.get("reactor")
    name = entry.get("name")
    reaction = entry.get("reaction")
    if (
        not isinstance(reactor, str)
        or not reactor
        or not isinstance(name, str)
        or not isinstance(reaction, str)
        or not reaction
    ):
        return None
    return {"reactor": reactor, "name": name, "reaction": reaction}


def _clean_stored_message(entry: Any) -> Optional[Dict[str, Any]]:
    """Validate one message record loaded from ``messages.json``.

    Returns a normalised copy, or ``None`` when the entry is not a
    recognisable message (corrupt or hand-edited history never blocks
    startup; the bad entry is simply dropped).
    """
    if not isinstance(entry, dict):
        return None
    if entry.get("direction") not in ("in", "out"):
        return None
    message_id = entry.get("id")
    if (
        not isinstance(message_id, int)
        or isinstance(message_id, bool)
        or message_id < 1
    ):
        return None
    peer = entry.get("peer")
    if peer is not None and not isinstance(peer, str):
        return None
    peer_name = entry.get("peer_name")
    text = entry.get("text")
    status = entry.get("status")
    if (
        not isinstance(peer_name, str)
        or not isinstance(text, str)
        or not isinstance(status, str)
    ):
        return None
    sent_at = entry.get("sent_at")
    received_at = entry.get("received_at")
    if (
        not isinstance(sent_at, (int, float))
        or isinstance(sent_at, bool)
        or not isinstance(received_at, (int, float))
        or isinstance(received_at, bool)
    ):
        return None
    try:
        sent_at = float(sent_at)
        received_at = float(received_at)
    except (OverflowError, ValueError):
        # A huge JSON integer timestamp overflows float(); drop the entry
        # instead of failing bridge startup.
        return None
    message: Dict[str, Any] = {
        "id": message_id,
        "direction": entry["direction"],
        "peer": peer,
        "peer_name": peer_name,
        "text": text,
        "sent_at": sent_at,
        "received_at": received_at,
        "status": status,
    }
    expires_at = entry.get("expires_at")
    if expires_at is not None:
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
            return None
        try:
            expires_at = float(expires_at)
        except (OverflowError, ValueError):
            return None
        if not math.isfinite(expires_at):
            return None
        message["expires_at"] = expires_at
    rtt_ms = entry.get("rtt_ms")
    if isinstance(rtt_ms, (int, float)) and not isinstance(rtt_ms, bool):
        message["rtt_ms"] = rtt_ms
    read_at = entry.get("read_at")
    if read_at is not None:
        if not isinstance(read_at, (int, float)) or isinstance(read_at, bool):
            return None
        message["read_at"] = float(read_at)
    mail_id = entry.get("mail_id")
    if isinstance(mail_id, str):
        # Kept so a message still queued across a restart can be matched
        # when its delivery is reported.
        message["mail_id"] = mail_id
    group = entry.get("group")
    if isinstance(group, str):
        message["group"] = group
        group_name = entry.get("group_name")
        if isinstance(group_name, str):
            message["group_name"] = group_name
    wire_id = entry.get("wire_id")
    if isinstance(wire_id, str) and wire_id:
        # The Reticulum message id: how replies name this message, and
        # how reactions (which name their target by wire id) attach to
        # the right history entry.
        message["wire_id"] = wire_id
    reactions = entry.get("reactions")
    if isinstance(reactions, list):
        cleaned = [_clean_stored_reaction(item) for item in reactions]
        message["reactions"] = [item for item in cleaned if item is not None]
    try:
        in_reply_to = clean_in_reply_to(entry.get("in_reply_to"))
    except ValueError:
        in_reply_to = None
    if in_reply_to is not None:
        message["in_reply_to"] = in_reply_to
    try:
        # The writer serialises with ensure_ascii=False, so a stored
        # string that is not UTF-8 encodable (e.g. a lone surrogate from a
        # hand-edited file) would poison every later save; drop it here.
        json.dumps(message, ensure_ascii=False).encode("utf-8")
    except (UnicodeEncodeError, ValueError):
        return None
    # The hash is derived from the validated text, never trusted from
    # the file: a hand-edited messages.json that changes the text but
    # keeps the old hash must not make a changed quote look verified.
    # Records written before hashes existed get one here. It is computed
    # after the UTF-8 check above, because hashing a lone surrogate
    # would raise instead of dropping the entry.
    message["excerpt_hash"] = excerpt_hash(text)
    return message


class WebBridge:
    """Expose a :class:`TauNode` over a loopback HTTP API.

    Inbound and outbound messages shown by the console are persisted to
    ``messages.json`` in the node data directory (when the node has one),
    so history survives a restart; without a data directory the bridge
    keeps messages in memory only.
    """

    def __init__(
        self,
        node: TauNode,
        *,
        host: str = DEFAULT_WEB_HOST,
        port: int = DEFAULT_WEB_PORT,
        web_dir: Optional[Path] = None,
        allow_remote: bool = False,
    ) -> None:
        if not allow_remote and not is_loopback_host(host):
            raise WebBridgeError(
                "refusing to expose the tau console on a non-loopback address "
                "without --http-allow-remote"
            )
        self.node = node
        self.host = host
        self.port = int(port)
        self.web_dir = Path(web_dir).expanduser().resolve() if web_dir else None
        self.allow_remote = bool(allow_remote)

        # The bridge owns the console message log, so it observes queued
        # deliveries itself; chain any delivery callback the node already
        # has instead of replacing it.
        previous_on_delivered = getattr(node, "on_delivered", None)

        def _on_delivered(message_id: str) -> None:
            try:
                if callable(previous_on_delivered):
                    previous_on_delivered(message_id)
            finally:
                # A failing previous callback must not lose the status
                # update; the exception propagates to the node's callback
                # guard, which logs it like any other user-callback failure.
                self.record_delivery(message_id)

        node.on_delivered = _on_delivered

        # Same chaining for read receipts: the bridge flips the console
        # record to "read" when the sender's node reports one.
        previous_on_read = getattr(node, "on_read", None)

        def _on_read(receipt: Dict[str, Any]) -> None:
            try:
                if callable(previous_on_read):
                    previous_on_read(receipt)
            finally:
                self.record_read(
                    receipt.get("reader", ""),
                    receipt.get("ids", []),
                    receipt.get("read_at", time.time()),
                )

        node.on_read = _on_read
        # Reactions arrive on the same node callback pattern: chain any
        # reaction callback the node already has, then annotate the
        # console history entry the reaction targets.
        previous_on_reaction = getattr(node, "on_reaction", None)

        def _on_reaction(record: Dict[str, Any]) -> None:
            try:
                if callable(previous_on_reaction):
                    previous_on_reaction(record)
            finally:
                self.record_reaction(record)

        node.on_reaction = _on_reaction

        self.bus = EventBus()
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._save_lock = threading.Lock()
        # Latest reaction per (target, reactor), kept even when the target
        # is not in history yet: a reaction can beat the console append of
        # its target, and the append seeds the message from this state.
        self._reaction_state: List[Dict[str, Any]] = []
        # Unread counts are stamped with an ever-increasing revision so
        # the console can drop a queued or delayed count older than one
        # it already applied.
        self._unread_lock = threading.Lock()
        self._unread_revision = 0
        # Outbox ids reported delivered before their send was recorded;
        # send_message reconciles them when it appends the record. An
        # OrderedDict used as a bounded set (its popitem(last=False)
        # evicts the oldest report; a plain dict has no such call).
        self._early_deliveries: OrderedDict[str, None] = OrderedDict()
        # Verified read receipts whose console record was not appended yet,
        # keyed by (reader, wire id); send_message reconciles them on append.
        self._early_reads: OrderedDict[Tuple[str, str], float] = OrderedDict()
        # Receipt ids currently being dispatched, so overlapping triggers
        # (SSE reconnects, re-lists) do not send the same receipt twice.
        self._receipts_in_flight: Set[str] = set()
        self._messages: Deque[Dict[str, Any]] = deque(maxlen=MAX_MESSAGE_LOG)
        self._tan_posts: Deque[Dict[str, Any]] = deque(maxlen=MAX_MESSAGE_LOG)
        self._next_message_id = 1
        self._messages_path = self._resolve_messages_path()
        self._load_messages()
        self._server: Optional[_WebServer] = None
        self._thread: Optional[threading.Thread] = None
        self._closing = threading.Event()
        self._started_at = time.time()

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> str:
        if self._server is not None:
            raise WebBridgeError("web bridge is already running")
        self._closing.clear()
        try:
            server = _WebServer((self.host, self.port), _WebHandler)
        except OSError as exc:
            raise _web_console_bind_error(self.host, self.port, exc) from exc
        server.bridge = self
        server.daemon_threads = True
        self._server = server
        self.port = int(server.server_address[1])
        self._thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.25},
            name="tau-web",
            daemon=True,
        )
        self._thread.start()
        log.info("tau web console listening on %s", self.url)
        return self.url

    def stop(self) -> None:
        self._closing.set()
        self.bus.close()
        server = self._server
        self._server = None
        if server is not None:
            server.shutdown()
            server.server_close()
        thread = self._thread
        self._thread = None
        if thread is not None:
            thread.join(timeout=3.0)

    # -- message history ---------------------------------------------------

    def _resolve_messages_path(self) -> Optional[Path]:
        """Data-dir path for persisted message history, or ``None``.

        A node without a data directory (tests, embeds) keeps messages in
        memory only.
        """
        data_dir = getattr(self.node, "data_dir", None)
        if data_dir is None:
            config = getattr(self.node, "config", None)
            data_dir = getattr(config, "data_dir", None)
        if data_dir is None:
            return None
        return Path(data_dir) / MESSAGES_FILE_NAME

    def _load_messages(self) -> None:
        """Load persisted message history into the in-memory log."""
        path = self._messages_path
        if path is None:
            return
        raw = read_json(path, default=None)
        if not isinstance(raw, dict):
            return
        entries = raw.get("messages")
        if not isinstance(entries, list):
            return
        loaded = []
        for entry in entries:
            message = _clean_stored_message(entry)
            if message is not None:
                loaded.append(message)
        loaded = loaded[-MAX_MESSAGE_LOG:]
        # Disappearing messages whose deadline passed are gone for good:
        # drop them here (and persist the purge below) so a restarted
        # console never resurrects them.
        now = time.time()
        kept = [
            message
            for message in loaded
            if message.get("expires_at") is None
            or float(message["expires_at"]) > now
        ]
        purged = len(loaded) - len(kept)
        loaded = kept
        self._messages.extend(loaded)
        saved_next = raw.get("next_message_id")
        max_id = max((message["id"] for message in loaded), default=0)
        if isinstance(saved_next, int) and not isinstance(saved_next, bool):
            self._next_message_id = max(saved_next, max_id + 1, 1)
        else:
            self._next_message_id = max(max_id + 1, 1)
        if purged:
            self._save_messages()

    def _save_messages(self) -> None:
        """Persist the current message log to ``messages.json``.

        Saves are serialised through ``_save_lock`` and the snapshot is
        taken inside it, so a slow write can never let an older snapshot
        overwrite a newer one: the last save to finish always holds the
        newest log. A failed write only logs a warning: the messages are
        already in memory, and a full disk (or an unencodable record) must
        never break sending or receiving.
        """
        path = self._messages_path
        if path is None:
            return
        with self._save_lock:
            with self._lock:
                messages = list(self._messages)
                next_id = self._next_message_id
            try:
                atomic_write_json(
                    path,
                    {"messages": messages, "next_message_id": next_id},
                    mode=0o600,
                )
            except (OSError, ValueError) as exc:
                log.warning(
                    "Could not persist message history to %s: %s", path, exc
                )

    @property
    def url(self) -> str:
        host = self.host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{self.port}/"

    @property
    def closing(self) -> bool:
        return self._closing.is_set()

    # -- node callbacks ----------------------------------------------------

    def record_peer(self, peer: Peer) -> None:
        # local_dict: the console replaces its snapshot record with this
        # event, so it must carry the same trust fields — a public_dict
        # event would silently drop the verified badge and any key-change
        # warning until the next snapshot load.
        payload = peer.local_dict()
        # Annotate like the snapshot does, so a live update does not
        # erase the frontend's blocked badge for this peer.
        blocklist = getattr(self.node, "blocklist", None)
        payload["blocked"] = bool(
            blocklist is not None and blocklist.is_blocked(payload.get("destination"))
        )
        self.bus.publish({"type": "peer", "peer": payload})

    def record_inbound(self, record: Dict[str, Any]) -> Dict[str, Any]:
        peer_hash = str(record.get("destination") or "")
        peer_name = str(record.get("from") or "")
        if not peer_name or peer_name == "unknown":
            peer_name = short_hash(peer_hash) if peer_hash else "unknown"
        group = record.get("group") if record.get("kind") == "group" else None
        expires_at = record.get("expires_at")
        if not isinstance(expires_at, (int, float)) or isinstance(expires_at, bool):
            expires_at = None
        wire_id = record.get("id")
        message = self._append_message(
            direction="in",
            peer=peer_hash or None,
            peer_name=sanitize_name(peer_name, fallback="unknown"),
            text=str(record.get("text") or ""),
            sent_at=_as_float(record.get("sent_at")),
            received_at=_as_float(record.get("received_at"), default=time.time()),
            status="received",
            group=group if isinstance(group, str) else None,
            group_name=(
                str(record.get("group_name") or "")
                if isinstance(record.get("group_name"), str)
                else None
            ),
            expires_at=float(expires_at) if expires_at is not None else None,
            wire_id=wire_id if isinstance(wire_id, str) else None,
            in_reply_to=record.get("in_reply_to"),
        )
        # The node appends to its inbox before this callback runs, so
        # the unread total already includes the new message.
        self.bus.publish(
            {"type": "message", "message": message, "unread": self.unread()}
        )
        # A live console is showing the thread view, so this message
        # counts as displayed: send its read receipt.  With nobody
        # subscribed the receipt waits for a snapshot serve instead.
        if self.bus.subscriber_count():
            self.mark_displayed_read()
        return message

    def record_delivery(self, mail_id: str) -> Optional[Dict[str, Any]]:
        """Mark a queued outbound message delivered.

        Called when the node reports that an outbox entry reached its
        recipient. The matching console record (by the outbox id stored
        at send time) flips from ``queued`` to ``delivered`` and the
        console is told live, so the feed updates in place. A report
        that arrives before the send is recorded (queueing starts a
        flush that can deliver first) is buffered and reconciled when
        the record is appended, so it is never lost; reports for mail
        this bridge never recorded — queued from the CLI, or from
        before this was tracked — simply age out of that buffer.
        """
        updated: Optional[Dict[str, Any]] = None
        with self._send_lock:
            with self._lock:
                for message in self._messages:
                    if (
                        message.get("direction") == "out"
                        and message.get("status") == "queued"
                        and message.get("mail_id") == mail_id
                    ):
                        message["status"] = "delivered"
                        updated = dict(message)
                        break
                if updated is None:
                    self._early_deliveries[mail_id] = None
                    while len(self._early_deliveries) > MAX_EARLY_DELIVERIES:
                        self._early_deliveries.popitem(last=False)
                else:
                    self._early_deliveries.pop(mail_id, None)
        if updated is None:
            return None
        # Persist the flip like any other log change, so a restart does
        # not resurrect the message as queued.
        self._save_messages()
        self.bus.publish({"type": "message_updated", "message": updated})
        return updated

    def record_read(
        self,
        reader_hash: str,
        message_ids: List[str],
        read_at: float,
    ) -> List[Dict[str, Any]]:
        """Mark outbound messages read from a verified receipt.

        Only records for this reader, still showing ``queued`` or
        ``delivered``, flip to ``read``; a record already read keeps
        the earliest read time.  Returns the updated records.
        """
        wanted = set(message_ids)
        matched: Set[str] = set()
        updated: List[Dict[str, Any]] = []
        with self._send_lock:
            with self._lock:
                for message in self._messages:
                    if message.get("direction") != "out" or (
                        message.get("peer") != reader_hash
                    ):
                        continue
                    # Queued records are named by their outbox id and
                    # every record by its wire id; a receipt names the
                    # wire id, which for queued mail is the outbox id.
                    record_id = message.get("wire_id") or message.get("mail_id")
                    if record_id not in wanted:
                        continue
                    matched.add(record_id)
                    if message.get("status") == "read":
                        continue
                    message["status"] = "read"
                    message["read_at"] = float(read_at)
                    updated.append(dict(message))
                # A receipt can beat the console record: send_message
                # registers the send before the record is appended.  Keep
                # the read so the record picks it up when it is appended.
                for mail_id in wanted - matched:
                    self._early_reads[(reader_hash, mail_id)] = float(read_at)
                    while len(self._early_reads) > MAX_EARLY_DELIVERIES:
                        self._early_reads.popitem(last=False)
        if not updated:
            return updated
        self._save_messages()
        for message in updated:
            self.bus.publish({"type": "message_updated", "message": message})
        return updated

    def mark_displayed_read(self) -> None:
        """Send read receipts for inbound messages shown in the thread view.

        Best-effort and asynchronous: receipt delivery must never
        block the console.  Only messages the inbox has not already
        stamped as receipted are covered, so a reconnect or a re-list
        never re-sends a receipt.  A node that opted out sends none.
        """
        node = self.node
        if node is None or not getattr(node, "read_receipts_enabled", True):
            return
        inbox = getattr(node, "inbox", None)
        if inbox is None:
            return
        with self._lock:
            pending = [
                dict(message)
                for message in self._messages
                if message.get("direction") == "in"
                and (message.get("wire_id") or message.get("mail_id"))
            ]
        if not pending:
            return
        try:
            # A one-shot `tau messages` may have stamped receipts on
            # disk since this node's copy was loaded.
            inbox.reload()
            wanted = {entry["id"] for entry in inbox.unreceipted()}
        except Exception:
            log.debug("Could not list unreceipted messages", exc_info=True)
            return
        by_peer: Dict[str, List[str]] = {}
        for message in pending:
            mail_id = message.get("wire_id") or message["mail_id"]
            if mail_id not in wanted:
                continue
            peer = message.get("peer")
            if not isinstance(peer, str) or not peer:
                continue
            ids = by_peer.setdefault(peer, [])
            if mail_id not in ids:
                ids.append(mail_id)
        if not by_peer:
            return
        # Stamp the displayed messages read before dispatch: a send that
        # fails then stays owed, so the serving node's sweep retries it.
        try:
            inbox.mark_read([mid for ids in by_peer.values() for mid in ids])
        except Exception:
            log.debug("Could not stamp displayed messages read", exc_info=True)
        # Claim the ids before the thread starts, so a second trigger that
        # runs while this one is still sending does not dispatch them again.
        claimed: Dict[str, List[str]] = {}
        with self._lock:
            for peer_hash, ids in by_peer.items():
                fresh = [mid for mid in ids if mid not in self._receipts_in_flight]
                if fresh:
                    self._receipts_in_flight.update(fresh)
                    claimed[peer_hash] = fresh
        if not claimed:
            return
        claimed_ids = [mid for ids in claimed.values() for mid in ids]

        def _send() -> None:
            try:
                for peer_hash, ids in claimed.items():
                    try:
                        result = node.send_read_receipts(peer_hash, ids)
                    except Exception:
                        log.debug(
                            "Could not send read receipt to %s",
                            peer_hash,
                            exc_info=True,
                        )
                        continue
                    handled = result.get("ids") or []
                    if handled:
                        try:
                            inbox.mark_receipt_sent(handled)
                        except Exception:
                            log.debug("Could not stamp read receipts", exc_info=True)
            finally:
                with self._lock:
                    self._receipts_in_flight.difference_update(claimed_ids)

        threading.Thread(
            target=_send, name="tau-read-receipts", daemon=True
        ).start()

    def record_reaction(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Attach a reaction to the console history entry it targets.

        Reactions name their target by wire id, which history entries
        now carry.  Per (message, reactor) the latest reaction wins —
        the same resolution as the inbox — and an empty token retracts.
        A reaction whose target is not in this console's history (sent
        from the CLI, or predating wire ids) is dropped: the node inbox
        still keeps it, so ``tau messages`` shows it.  The updated
        entry is re-sent as ``message_updated``, which the console
        already renders in place.
        """
        target = record.get("target")
        token = record.get("reaction")
        reactor = record.get("destination")
        name = record.get("from")
        if (
            not isinstance(target, str)
            or not target
            or not isinstance(reactor, str)
            or not reactor
            or not isinstance(token, str)
        ):
            return None
        entry = {
            "target": target,
            "destination": reactor,
            "reaction": token,
            "from": name if isinstance(name, str) else "unknown",
            "sent_at": _as_float(record.get("sent_at")),
            "id": record.get("id") if isinstance(record.get("id"), str) else None,
        }
        updated: Optional[Dict[str, Any]] = None
        with self._lock:
            if not merge_reaction(self._reaction_state, entry):
                return None
            self._reaction_state = self._reaction_state[-MAX_REACTIONS:]
            for message in self._messages:
                if message.get("wire_id") != target:
                    continue
                message["reactions"] = _with_reaction(
                    message.get("reactions") or [], reactor, entry
                )
                updated = dict(message)
                break
        if updated is None:
            return None
        self._save_messages()
        self.bus.publish({"type": "message_updated", "message": updated})
        return updated

    def record_tan_post(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Record a Tan post and fan it out to SSE subscribers."""
        with self._lock:
            self._tan_posts.append(dict(record))
        self.bus.publish({"type": "post", "post": dict(record)})
        return record

    # -- Tan social feed ---------------------------------------------------

    def _tan_app(self) -> Optional[Any]:
        app = getattr(self.node, "app", None)
        return app if getattr(app, "db_path", None) is not None else None

    def _tan_db_path(self) -> Path:
        app = self._tan_app()
        if app is not None:
            return Path(app.db_path)
        return Path(self.node.config.social_db_path)

    def _tan_conn(self) -> Any:
        from .tangents.tan import init_db

        return init_db(self._tan_db_path())

    def tan_timeline(
        self,
        *,
        limit: int = 30,
        include_all: bool = False,
        author: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        from .identity import destination_hash, load_identity
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import (
            annotate_quote_counts,
            annotate_quotes,
            annotate_reactions,
            annotate_replies,
            apply_display_rules,
            author_timeline,
            following,
            resolve_author,
            timeline,
        )

        # One snapshot for every console surface: the node's cached
        # block-list, the same list the DM gate enforces.  A list that
        # failed closed (block_all) hides every post here, matching
        # the gate, which drops every DM on the same condition.
        blocklist = getattr(self.node, "blocklist", None)
        blocked = blocklist.blocked if blocklist is not None else frozenset()
        exclude_all = bool(blocklist is not None and blocklist.block_all)
        conn = self._tan_conn()
        try:
            app = self._tan_app()
            if app is not None:
                viewer = app.author
            else:
                identity = load_identity(self.node.config.identity_path)
                viewer = (
                    destination_hash(identity).hex() if identity is not None else None
                )
            if author:
                target, _ = resolve_author(conn, author, registry=self.node.registry)
                posts = author_timeline(
                    conn,
                    target,
                    limit=limit,
                    viewer=viewer,
                    exclude_authors=blocked,
                    exclude_all=exclude_all,
                )
            elif include_all:
                posts = timeline(
                    conn,
                    include_all=True,
                    limit=limit,
                    viewer=viewer,
                    exclude_authors=blocked,
                    exclude_all=exclude_all,
                )
            else:
                authors = [record["author"] for record in following(conn)]
                if viewer is not None:
                    authors.append(viewer)
                posts = timeline(
                    conn,
                    authors,
                    limit=limit,
                    viewer=viewer,
                    exclude_authors=blocked,
                    exclude_all=exclude_all,
                )
            if app is not None:
                ruleset = app.ruleset
            else:
                try:
                    ruleset = load_ruleset(
                        getattr(self.node.config, "tan_rules_path", None)
                    )
                except RulesError:
                    ruleset = Ruleset()
            return apply_display_rules(posts, ruleset)
        finally:
            conn.close()

    def tan_top(
        self,
        *,
        limit: int = 10,
        author: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        from .identity import destination_hash, load_identity
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import apply_display_rules, resolve_author, top_posts

        # Same block-list snapshot as the timeline: the console hides
        # posts by blocked authors, and a failed-closed list hides all.
        # top_posts has no exclude parameters, so the block-list is
        # applied after the ranking — a blocked author's post can take
        # a result slot, it just never reaches the console.
        blocklist = getattr(self.node, "blocklist", None)
        blocked = blocklist.blocked if blocklist is not None else frozenset()
        exclude_all = bool(blocklist is not None and blocklist.block_all)
        conn = self._tan_conn()
        try:
            app = self._tan_app()
            if app is not None:
                viewer = app.author
            else:
                identity = load_identity(self.node.config.identity_path)
                viewer = (
                    destination_hash(identity).hex() if identity is not None else None
                )
            if app is not None:
                ruleset = app.ruleset
            else:
                try:
                    ruleset = load_ruleset(
                        getattr(self.node.config, "tan_rules_path", None)
                    )
                except RulesError:
                    ruleset = Ruleset()
            target = None
            if author:
                target, _ = resolve_author(conn, author, registry=self.node.registry)
            if exclude_all:
                return []
            posts = top_posts(
                conn,
                author=target,
                limit=limit,
                viewer=viewer,
                ruleset=ruleset,
            )
            posts = [post for post in posts if post.get("author") not in blocked]
            return apply_display_rules(posts, ruleset)
        finally:
            conn.close()

    def tan_following(self) -> List[Dict[str, Any]]:
        from .tangents.tan import author_stats, following

        conn = self._tan_conn()
        try:
            records = following(conn)
            for record in records:
                record.update(author_stats(conn, record["author"]))
            return records
        finally:
            conn.close()

    def tan_follow(self, target: str) -> Dict[str, Any]:
        from .tangents.tan import follow, resolve_author

        conn = self._tan_conn()
        try:
            author, name = resolve_author(conn, target, registry=self.node.registry)
            return follow(conn, author, name=name)
        finally:
            conn.close()

    def tan_unfollow(self, target: str) -> Dict[str, Any]:
        from .tangents.tan import resolve_author, unfollow

        conn = self._tan_conn()
        try:
            author, name = resolve_author(conn, target, registry=self.node.registry)
            return {"author": author, "name": name, "unfollowed": unfollow(conn, author)}
        finally:
            conn.close()

    def tan_posts(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._tan_posts)
        return items[-limit:] if limit >= 0 else items

    def tan_post(self, text: str, *, reply_to: Optional[str] = None) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        if reply_to is not None:
            conn = self._tan_conn()
            try:
                from .tangents.tan import resolve_post

                reply_to = resolve_post(conn, reply_to)["hash"]
            finally:
                conn.close()
        return app.create_post(text, reply_to=reply_to, publish=True)

    def tan_react(self, target: str, *, liked: bool) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        conn = self._tan_conn()
        try:
            from .tangents.tan import resolve_post

            post_hash = resolve_post(conn, target)["hash"]
        finally:
            conn.close()
        return app.react(post_hash, liked=liked, publish=True)

    def tan_delete(self, target: str) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        conn = self._tan_conn()
        try:
            from .tangents.tan import resolve_delete_target

            post_hash = resolve_delete_target(conn, target)
        finally:
            conn.close()
        return app.delete(post_hash, publish=True)

    def tan_sync(self, *, explore: bool = True) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        return app.sync(self.node, explore=explore)

    def _resolve_console_reply(
        self, peer_hash: str, reply_to: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Resolve a console ``reply_to`` wire id to an ``in_reply_to``.

        The quoted message must be in this conversation — same peer in
        the console history, matched by full id or unique prefix — so a
        reply never dangles at a message from another peer or one the
        console never saw.
        """
        if reply_to is None:
            return None
        if not isinstance(reply_to, str) or not reply_to.strip():
            raise ValueError("field 'reply_to' must be a message id")
        ref = reply_to.strip().lower()
        with self._lock:
            matches = [
                message
                for message in self._messages
                if message.get("peer") == peer_hash
                and isinstance(message.get("wire_id"), str)
                and str(message["wire_id"]).lower().startswith(ref)
            ]
        if not matches:
            raise ValueError(
                f"no message {reply_to.strip()!r} in this conversation to reply to"
            )
        if len(matches) > 1:
            raise ValueError(
                f"{len(matches)} messages in this conversation match"
                f" {reply_to.strip()!r}; use a longer id"
            )
        quoted = matches[0]
        return {
            "id": str(quoted["wire_id"]),
            "excerpt": excerpt_hash(str(quoted.get("text") or "")),
        }

    def send_message(
        self,
        target: str,
        text: str,
        *,
        timeout: float = 20.0,
        expires_at: Optional[float] = None,
        reply_to: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target peer is required")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("message text is required")
        if len(text) > MAX_MESSAGE_CHARS:
            raise ValueError(f"message is longer than {MAX_MESSAGE_CHARS} characters")
        if expires_at is not None:
            if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
                raise ValueError("expires_at must be a number")
            expires_at = float(expires_at)
            if not math.isfinite(expires_at):
                raise ValueError("expires_at must be a finite number")
        # Normalise up front so the recorded text is exactly what gets sent
        # (and is always UTF-8 encodable for the history file): the node
        # normalises again before sending, so this cannot change delivery.
        text = normalize_message(text)

        peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        in_reply_to = self._resolve_console_reply(peer.hexhash, reply_to)
        # The send lock is NOT held across the network send: queueing
        # starts a flush thread that can deliver the entry (and report
        # it via record_delivery) before the record below exists, and
        # holding the lock across network I/O would stall every other
        # delivery report behind this send. Instead, record_delivery
        # buffers a report that beats the record, and the append below
        # reconciles it.
        response = self.node.send_message(
            peer, text, timeout=timeout, in_reply_to=in_reply_to, expires_at=expires_at
        )
        queued = bool(response.get("queued"))
        # The outbox id links this record to a later delivery; only
        # present while the message is queued.
        mail_id = response.get("id") if queued else None
        wire_id = response.get("id") if isinstance(response.get("id"), str) else None
        flipped = False
        early_read: Optional[float] = None
        with self._send_lock:
            message = self._append_message(
                direction="out",
                peer=peer.hexhash,
                peer_name=peer.display_name,
                text=text,
                sent_at=time.time(),
                received_at=time.time(),
                status="queued" if queued else "delivered",
                rtt=_as_float(response.get("_rtt")),
                mail_id=mail_id,
                expires_at=expires_at,
                wire_id=wire_id,
                in_reply_to=in_reply_to,
            )
            with self._lock:
                if mail_id is not None and mail_id in self._early_deliveries:
                    del self._early_deliveries[mail_id]
                    message["status"] = "delivered"
                    flipped = True
                read_key = wire_id or mail_id
                if read_key is not None:
                    early_read = self._early_reads.pop(
                        (peer.hexhash, read_key), None
                    )
            if early_read is not None:
                # A read receipt beat the record; it is the later state.
                message["status"] = "read"
                message["read_at"] = early_read
                flipped = True
            if flipped:
                self._save_messages()
        self.bus.publish({"type": "message", "message": message})
        if flipped:
            self.bus.publish(
                {"type": "message_updated", "message": dict(message)}
            )
        return message, response

    def react(
        self,
        target: str,
        reaction: Optional[str] = None,
        *,
        clear: bool = False,
        timeout: float = 20.0,
    ) -> Dict[str, Any]:
        """React to a DM from the console, by its wire id.

        The target is resolved from this console's history first (it
        covers both directions: an inbound message reacts to its author,
        an outbound one to its recipient) and then from the node inbox,
        which keeps received messages past the console's window.  The
        outgoing reaction is recorded locally by the node, so the
        console annotates the entry through the same ``message_updated``
        event as an inbound reaction.
        """
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target message id is required")
        if clear and reaction is not None:
            raise ValueError("give a reaction, or clear it (not both)")
        if not clear and (not isinstance(reaction, str) or not reaction.strip()):
            raise ValueError("a reaction is required")
        token = clean_reaction("" if clear else reaction)
        target = target.strip()
        peer: Optional[str] = None
        with self._lock:
            for message in self._messages:
                if message.get("wire_id") == target:
                    peer = message.get("peer")
                    break
        if peer is None:
            inbox = getattr(self.node, "inbox", None)
            if inbox is None:
                raise ValueError(f"unknown message: {target!r}")
            try:
                record = inbox.resolve(target)
            except ValueError as exc:
                raise ValueError(f"unknown message: {target!r}") from exc
            peer = record.get("destination")
        if not peer:
            raise ValueError("that message has no known peer to react to")
        response = self.node.send_reaction(peer, target, token, timeout=timeout)
        return {
            "ok": True,
            "target": target,
            "reaction": token,
            "cleared": clear,
            "id": response.get("id"),
        }

    def outbox(self) -> Dict[str, Any]:
        """Queued mail waiting on offline peers, with recipient names.

        Read under the mailbox transaction so entries queued by a
        separate process (e.g. a one-shot ``tau send``) are visible
        instead of the serving process's cached snapshot.
        """
        with self.node.mailbox.transaction():
            entries = [
                self._describe_outbox_entry(entry)
                for entry in self.node.mailbox.outbox_list()
            ]
            held = self.node.mailbox.counts()["held"]
        return {"outbox": entries, "count": len(entries), "held_for_others": held}

    def cancel_outbox(self, target: str) -> Dict[str, Any]:
        """Remove one queued message from the outbox and describe it.

        Cancelling stops this node's future delivery attempts; sealed
        copies already held by other peers may still be delivered.
        Resolve and remove happen inside one mailbox transaction, so a
        cancel cannot miss an entry another process just queued, and a
        delivery that lands in between is reported instead of silently
        claimed as cancelled.
        """
        if not isinstance(target, str) or not target.strip():
            raise ValueError("message id is required")
        with self.node.mailbox.transaction():
            entry = self.node.mailbox.resolve_outbox(target.strip())
            if not self.node.mailbox.remove_outbox(entry.id):
                # Delivered or expired between resolve and remove.
                raise MailboxError(
                    f"queued message {target.strip()!r} is no longer queued"
                )
            return self._describe_outbox_entry(entry)

    def _describe_outbox_entry(self, entry: Any) -> Dict[str, Any]:
        payload = entry.to_dict()
        peer = self.node.registry.get(entry.to)
        payload["to_name"] = peer.display_name if peer is not None else entry.to_name
        payload["held_by"] = sorted(entry.held_by)
        payload["read_at"] = self.node.mailbox.read_at(entry.id)
        return payload

    def ping(self, target: str, *, timeout: float = 15.0) -> Dict[str, Any]:
        peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        response = self.node.ping(peer, timeout=timeout)
        rtt = response.get("_rtt")
        return {
            "ok": True,
            "peer": peer.public_dict(),
            "server": response.get("server"),
            "rtt_ms": round(float(rtt) * 1000.0, 2)
            if isinstance(rtt, (int, float))
            else None,
        }

    def announce(self) -> None:
        self.node.announce()
        self.bus.publish({"type": "status", "action": "announce"})

    # -- snapshots ---------------------------------------------------------

    def peers(self) -> List[Dict[str, Any]]:
        now = time.time()
        try:
            # local_dict: verification is this node's own trust state, so
            # it is shown to the console owner but never sent to peers.
            items = [peer.local_dict(now) for peer in self.node.registry.all()]
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not read peer registry", exc_info=True)
            return []
        blocklist = getattr(self.node, "blocklist", None)
        for item in items:
            # is_blocked covers the fail-closed case too: with a
            # corrupt list every peer reads as blocked, matching the
            # DM gate, which drops every message then.
            item["blocked"] = bool(
                blocklist is not None
                and blocklist.is_blocked(item.get("destination"))
            )
        return items

    def groups(self) -> List[Dict[str, Any]]:
        try:
            groups = []
            for group in self.node.groups.list_groups():
                members = []
                for member_hash in group["members"]:
                    peer = self.node.registry.get(member_hash)
                    members.append(
                        {
                            "hash": member_hash,
                            "name": (
                                peer.display_name
                                if peer is not None
                                else member_hash[:8]
                            ),
                        }
                    )
                groups.append(
                    {
                        "id": group["id"],
                        "name": group["name"],
                        "members": members,
                        "created_at": group["created_at"],
                    }
                )
            return groups
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not read group store", exc_info=True)
            return []

    def messages(self, limit: int = 200) -> List[Dict[str, Any]]:
        # Drop disappearing messages whose deadline has passed while the
        # bridge is running, not only at startup, so snapshots and the
        # live feed never show an expired entry.
        now = time.time()
        with self._lock:
            kept = [
                message
                for message in self._messages
                if message.get("expires_at") is None
                or float(message["expires_at"]) > now
            ]
            removed = len(kept) != len(self._messages)
            if removed:
                # _messages is a deque (no slice assignment): rebuild it.
                self._messages.clear()
                self._messages.extend(kept)
            items = list(self._messages)
        if removed:
            self._save_messages()
        if limit >= 0:
            items = items[-limit:]
        # Blocked senders' history stays stored but is hidden locally;
        # unblocking restores the view.  Inbound records carry the
        # sender hash as "destination" (node inbox) or "peer" (console
        # log); outbound records carry the recipient, which blocking
        # never hides.  is_blocked covers the fail-closed case too.
        blocklist = getattr(self.node, "blocklist", None)
        if blocklist is not None:
            items = [
                item
                for item in items
                if item.get("direction") != "in"
                or not blocklist.is_blocked(
                    item.get("destination") or item.get("peer")
                )
            ]
        by_target = self._inbox_reactions_by_target()
        return [
            self._with_inbox_reactions(dict(item), by_target) for item in items
        ]

    def _inbox_reactions_by_target(self) -> Dict[str, List[Dict[str, Any]]]:
        """Node inbox reactions grouped by target, reloaded once per read.

        One reload serves the whole listing: reloading per message
        would take the cross-process lock and re-read ``inbox.json``
        once per history entry.
        """
        inbox = getattr(self.node, "inbox", None)
        if inbox is None:
            return {}
        try:
            reactions = inbox.all_reactions()
        except Exception:  # pragma: no cover - defensive
            return {}
        by_target: Dict[str, List[Dict[str, Any]]] = {}
        for entry in reactions:
            target = entry.get("target")
            if isinstance(target, str) and target:
                by_target.setdefault(target, []).append(entry)
        return by_target

    def _with_inbox_reactions(
        self, message: Dict[str, Any], by_target: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Overlay the node inbox's reaction state on one history entry.

        The bridge learns reactions live through its own node's callback,
        but a reaction made from another process (``tau react`` from the
        CLI) only lands in the shared ``inbox.json``.  The inbox is the
        durable authority — per reactor its entry wins over the bridge's
        live state (the two agree for live traffic; the inbox additionally
        holds the CLI-made ones, in ``sent_at`` order) — so every read of
        the history shows the current state.  A live push to an already
        open console tab for a CLI-made reaction is still a follow-up:
        this reconciles reads, not the SSE stream.
        """
        wire_id = message.get("wire_id")
        if not wire_id:
            return message
        stored = by_target.get(wire_id)
        if not stored:
            return message
        merged: Dict[str, Dict[str, Any]] = {
            str(item.get("reactor")): dict(item)
            for item in message.get("reactions") or []
            if item.get("reactor")
        }
        for entry in stored:
            reactor = str(entry.get("destination") or "")
            if not reactor:
                continue
            if entry.get("reaction"):
                merged[reactor] = {
                    "reactor": reactor,
                    "name": str(entry.get("from") or "unknown"),
                    "reaction": entry["reaction"],
                }
            else:
                merged.pop(reactor, None)
        message["reactions"] = list(merged.values())
        return message

    def counts(self) -> Dict[str, int]:
        with self._lock:
            message_count = len(self._messages)
        return {"messages": message_count, "peers": len(self.peers())}

    def _stamp_unread(self, counts: Dict[str, Any]) -> Dict[str, Any]:
        """Attach the next revision to *counts* (caller holds ``_unread_lock``)."""
        self._unread_revision = max(time.time_ns(), self._unread_revision + 1)
        return dict(counts, revision=self._unread_revision)

    def unread(self) -> Dict[str, Any]:
        """Per-conversation unread counts from the node inbox.

        A node without an inbox (tests, embeds) simply has nothing
        unread; read state is local to this node either way.
        """
        inbox = getattr(self.node, "inbox", None)
        with self._unread_lock:
            counts: Dict[str, Any] = {"per_peer": {}, "total": 0}
            if inbox is not None:
                try:
                    counts = inbox.unread_counts()
                except Exception:  # pragma: no cover - defensive
                    log.debug("Could not read inbox unread counts", exc_info=True)
            return self._stamp_unread(counts)

    def mark_read(self, peer: Optional[str] = None) -> Dict[str, Any]:
        """Advance the inbox read cursor for one conversation, or all.

        The console calls this when a conversation is opened, so its
        unread badge clears; other tabs learn from the ``unread`` SSE
        event.  Read state never leaves this node.
        """
        inbox = getattr(self.node, "inbox", None)
        with self._unread_lock:
            counts: Dict[str, Any] = {"per_peer": {}, "total": 0}
            if inbox is not None:
                counts = inbox.mark_read(peer=peer)
            stamped = self._stamp_unread(counts)
        if inbox is not None:
            self.bus.publish({"type": "unread", "unread": stamped})
        return stamped

    def snapshot(self) -> Dict[str, Any]:
        return {
            "node": self.node.info(),
            "peers": self.peers(),
            "groups": self.groups(),
            "messages": self.messages(limit=200),
            "counts": self.counts(),
            "unread": self.unread(),
            "bridge": {
                "version": __version__,
                "web": self.web_dir is not None,
                "url": self.url,
            },
            "social": {
                "enabled": self._tan_app() is not None,
                "posts": len(self.tan_posts(limit=0)),
            },
        }

    def _append_message(
        self,
        *,
        direction: str,
        peer: Optional[str],
        peer_name: str,
        text: str,
        sent_at: float,
        received_at: float,
        status: str,
        rtt: Optional[float] = None,
        mail_id: Optional[str] = None,
        group: Optional[str] = None,
        group_name: Optional[str] = None,
        expires_at: Optional[float] = None,
        wire_id: Optional[str] = None,
        in_reply_to: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            message = {
                "id": self._next_message_id,
                "direction": direction,
                "peer": peer,
                "peer_name": peer_name,
                "text": text,
                "sent_at": sent_at,
                "received_at": received_at,
                "status": status,
                "reactions": [],
            }
            if rtt is not None:
                message["rtt_ms"] = round(rtt * 1000.0, 2)
            if mail_id is not None:
                message["mail_id"] = mail_id
            if group is not None:
                message["group"] = group
                if group_name:
                    message["group_name"] = group_name
            if expires_at is not None:
                message["expires_at"] = expires_at
            if wire_id is not None:
                message["wire_id"] = wire_id
                # A reaction may have arrived before this entry was
                # appended; apply the latest known state for it now.
                for entry in self._reaction_state:
                    if entry["target"] == wire_id and entry["reaction"]:
                        message["reactions"] = _with_reaction(
                            message["reactions"], entry["destination"], entry
                        )
            message["excerpt_hash"] = excerpt_hash(text)
            reply_to = clean_in_reply_to(in_reply_to)
            if reply_to is not None:
                message["in_reply_to"] = reply_to
            self._next_message_id += 1
            self._messages.append(message)
        self._save_messages()
        return message

    # -- static files ------------------------------------------------------

    def resolve_static(self, request_path: str) -> Optional[Path]:
        """Return a file inside ``web_dir`` for a URL path, or ``None``."""
        if self.web_dir is None:
            return None
        relative = unquote(request_path).lstrip("/")
        if not relative:
            relative = "index.html"
        base = self.web_dir
        try:
            candidate = (base / relative).resolve()
        except (OSError, ValueError):
            return None
        if candidate != base and base not in candidate.parents:
            return None
        if candidate.is_dir():
            candidate = candidate / "index.html"
        if candidate.is_file() and not _has_hidden_part(candidate, base):
            return candidate
        # Static export uses trailing slashes; fall back to the SPA entry point.
        if "." not in Path(relative).name:
            index = base / "index.html"
            if index.is_file():
                return index
        return None


def _has_hidden_part(path: Path, base: Path) -> bool:
    try:
        parts = path.relative_to(base).parts
    except ValueError:
        return True
    return any(part.startswith(".") for part in parts)


def _with_reaction(
    reactions: List[Dict[str, Any]], reactor: str, entry: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Return a message's reaction list with the reactor's choice set.

    Drops the reactor's previous entry; a retraction (empty token) leaves
    none, while a reaction appends one carrying the reactor's name.
    """
    kept = [item for item in reactions if item.get("reactor") != reactor]
    if entry["reaction"]:
        kept.append(
            {"reactor": reactor, "name": entry["from"], "reaction": entry["reaction"]}
        )
    return kept


def _as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _valid_expires_in(value: Any) -> bool:
    """True when ``value`` is a finite lifetime in (0, MAX_MAIL_TTL] seconds.

    Huge JSON integers overflow ``float()``, so the conversion is guarded
    here instead of raising past the 400 response.
    """
    try:
        seconds = float(value)
    except OverflowError:
        return False
    return math.isfinite(seconds) and 0 < seconds <= MAX_MAIL_TTL


class _WebServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    bridge: WebBridge


class _WebHandler(BaseHTTPRequestHandler):
    server: _WebServer
    server_version = f"tau-web/{__version__}"
    protocol_version = "HTTP/1.1"

    # -- plumbing ----------------------------------------------------------

    @property
    def bridge(self) -> WebBridge:
        return self.server.bridge

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        log.debug("web %s - %s", self.address_string(), fmt % args)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle(head_only=True)

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def _handle(self, *, head_only: bool = False) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path
        try:
            if path.startswith("/api/"):
                if self.command == "POST" and not self._post_is_allowed():
                    self._send_error(
                        HTTPStatus.FORBIDDEN,
                        "cross-origin requests are not allowed",
                        head_only,
                    )
                    return
                self._handle_api(path, parse_qs(parsed.query), head_only)
            elif path == "/healthz":
                self._send_json({"ok": True, "version": __version__}, head_only)
            else:
                self._serve_static(path, head_only)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except RequestError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc), head_only)
        except WebBridgeError as exc:
            self._send_error(HTTPStatus.TOO_MANY_REQUESTS, str(exc), head_only)
        except Exception:  # pragma: no cover - defensive
            log.exception("Unhandled web request error")
            self._send_error(
                HTTPStatus.INTERNAL_SERVER_ERROR, "internal error", head_only
            )

    # -- API ---------------------------------------------------------------

    def _handle_api(
        self, path: str, query: Dict[str, List[str]], head_only: bool
    ) -> None:
        if path == "/api/events" and self.command == "GET" and not head_only:
            self._serve_events(query)
            return

        if self.command == "GET":
            if path == "/api/status":
                self._send_json(self.bridge.snapshot(), head_only)
                return
            if path == "/api/peers":
                peers = self.bridge.peers()
                self._send_json({"peers": peers, "count": len(peers)}, head_only)
                return
            if path == "/api/groups":
                groups = self.bridge.groups()
                self._send_json({"groups": groups, "count": len(groups)}, head_only)
                return
            if path == "/api/messages":
                limit = _int_param(query, "limit", default=200, minimum=1, maximum=500)
                self._send_json({"messages": self.bridge.messages(limit)}, head_only)
                return
            if path == "/api/outbox":
                self._send_json(self.bridge.outbox(), head_only)
                return
            if path == "/api/tan/timeline":
                limit = _int_param(query, "limit", default=30, minimum=1, maximum=200)
                include_all = _query_flag(query, "all")
                author = (query.get("author") or [None])[0]
                try:
                    posts = self.bridge.tan_timeline(
                        limit=limit, include_all=include_all, author=author
                    )
                except Exception as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
                return
            if path == "/api/tan/top":
                limit = _int_param(query, "limit", default=10, minimum=1, maximum=100)
                author = (query.get("author") or [None])[0]
                try:
                    posts = self.bridge.tan_top(limit=limit, author=author)
                except Exception as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
                return
            if path == "/api/tan/following":
                records = self.bridge.tan_following()
                self._send_json(
                    {"following": records, "count": len(records)}, head_only
                )
                return
            if path == "/api/tan/posts":
                limit = _int_param(query, "limit", default=100, minimum=1, maximum=500)
                posts = self.bridge.tan_posts(limit=limit)
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
                return
            self._send_json({"error": {"message": "not found"}}, head_only, 404)
            return

        if self.command == "POST":
            body = self._read_body()
            if path == "/api/messages":
                self._handle_send(body, head_only)
                return
            if path == "/api/react":
                self._handle_react(body, head_only)
                return
            if path == "/api/messages/read":
                peer = body.get("peer")
                if peer is not None and (
                    not isinstance(peer, str) or not peer.strip()
                ):
                    self._send_json(
                        {"error": {"message": "field 'peer' must be a peer hash"}},
                        head_only,
                        400,
                    )
                    return
                counts = self.bridge.mark_read(
                    peer.strip() if isinstance(peer, str) else None
                )
                self._send_json({"ok": True, "unread": counts}, head_only)
                return
            if path == "/api/outbox/cancel":
                self._handle_outbox_cancel(body, head_only)
                return
            if path == "/api/ping":
                self._handle_ping(body, head_only)
                return
            if path == "/api/announce":
                self.bridge.announce()
                self._send_json({"ok": True}, head_only)
                return
            if path == "/api/tan/posts":
                text = body.get("text")
                if not isinstance(text, str) or not text.strip():
                    self._send_json(
                        {"error": {"message": "field 'text' is required"}},
                        head_only,
                        400,
                    )
                    return
                reply_to = body.get("reply_to")
                if reply_to is not None and (
                    not isinstance(reply_to, str) or not reply_to.strip()
                ):
                    self._send_json(
                        {"error": {"message": "field 'reply_to' must be a post hash"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    post = self.bridge.tan_post(text, reply_to=reply_to)
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                self._send_json({"ok": True, "post": post}, head_only)
                return
            if path in ("/api/tan/follow", "/api/tan/unfollow"):
                target = body.get("target") or body.get("author")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    if path.endswith("unfollow"):
                        result = self.bridge.tan_unfollow(target.strip())
                    else:
                        result = self.bridge.tan_follow(target.strip())
                except Exception as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "follow": result}, head_only)
                return
            if path in ("/api/tan/like", "/api/tan/unlike"):
                target = body.get("target") or body.get("hash")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    record = self.bridge.tan_react(
                        target.strip(), liked=(path == "/api/tan/like")
                    )
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "reaction": record}, head_only)
                return
            if path == "/api/tan/delete":
                target = body.get("target") or body.get("hash")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    record = self.bridge.tan_delete(target.strip())
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "delete": record}, head_only)
                return
            if path == "/api/tan/sync":
                try:
                    result = self.bridge.tan_sync(
                        explore=bool(body.get("explore", True))
                    )
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except NodeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 502)
                    return
                self._send_json({"ok": True, "sync": result}, head_only)
                return
            self._send_json({"error": {"message": "not found"}}, head_only, 404)
            return

        self._send_json({"error": {"message": "method not allowed"}}, head_only, 405)

    def _handle_send(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        text = body.get("text")
        timeout = body.get("timeout", 20.0)
        expires_in = body.get("expires_in")
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'to' is required"}}, head_only, 400
            )
            return
        if not isinstance(text, str):
            self._send_json(
                {"error": {"message": "field 'text' is required"}}, head_only, 400
            )
            return
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = 20.0
        timeout = max(1.0, min(float(timeout), 120.0))
        expires_at = None
        if expires_in is not None:
            if (
                not isinstance(expires_in, (int, float))
                or isinstance(expires_in, bool)
                or not _valid_expires_in(expires_in)
            ):
                self._send_json(
                    {
                        "error": {
                            "message": "field 'expires_in' must be a number of "
                            "seconds between 0 and 30 days"
                        }
                    },
                    head_only,
                    400,
                )
                return
            expires_at = time.time() + float(expires_in)
        reply_to = body.get("reply_to")
        if reply_to is not None and (
            not isinstance(reply_to, str) or not reply_to.strip()
        ):
            self._send_json(
                {"error": {"message": "field 'reply_to' must be a message id"}},
                head_only,
                400,
            )
            return
        try:
            message, response = self.bridge.send_message(
                target,
                text,
                timeout=timeout,
                expires_at=expires_at,
                reply_to=reply_to,
            )
        except (ValueError, ProtocolError) as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except PeerError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(
            {"ok": True, "message": message, "response": response}, head_only
        )

    def _handle_react(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("target")
        reaction = body.get("reaction")
        clear = bool(body.get("clear", False))
        timeout = body.get("timeout", 20.0)
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'target' is required"}},
                head_only,
                400,
            )
            return
        if reaction is not None and not isinstance(reaction, str):
            self._send_json(
                {"error": {"message": "field 'reaction' must be a string"}},
                head_only,
                400,
            )
            return
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = 20.0
        timeout = max(1.0, min(float(timeout), 120.0))
        try:
            result = self.bridge.react(
                target.strip(), reaction, clear=clear, timeout=timeout
            )
        except (ValueError, ProtocolError) as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except PeerError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(result, head_only)

    def _handle_outbox_cancel(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("target") or body.get("id")
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'target' is required"}},
                head_only,
                400,
            )
            return
        try:
            cancelled = self.bridge.cancel_outbox(target.strip())
        except ValueError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except MailboxError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        self._send_json({"ok": True, "cancelled": cancelled}, head_only)

    def _handle_ping(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        timeout = body.get("timeout", 15.0)
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'to' is required"}}, head_only, 400
            )
            return
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = 15.0
        timeout = max(1.0, min(float(timeout), 120.0))
        try:
            result = self.bridge.ping(target, timeout=timeout)
        except PeerError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(result, head_only)

    # -- server-sent events ------------------------------------------------

    def _serve_events(self, query: Dict[str, List[str]]) -> None:
        since = _int_param(query, "since", default=0, minimum=0, maximum=2**31 - 1)
        replay = "since" in query and since > 0
        try:
            subscriber = self.bridge.bus.subscribe()
        except WebBridgeError as exc:
            self._send_error(HTTPStatus.TOO_MANY_REQUESTS, str(exc))
            return

        self.close_connection = True
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self._cors_headers()
        self.end_headers()

        try:
            self._write_event("snapshot", {"snapshot": self.bridge.snapshot()})
            # A client just opened the console, so the thread view is
            # displaying these messages: send their read receipts.
            self.bridge.mark_displayed_read()
            if replay:
                for event in self.bridge.bus.replay(since):
                    self._write_event(
                        str(event.get("type", "message")),
                        {key: value for key, value in event.items() if key != "type"},
                        event.get("id"),
                    )
            while not self.bridge.closing:
                try:
                    event = subscriber.get(timeout=SSE_HEARTBEAT_SECONDS)
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                if event is None:
                    break
                self._write_event(
                    str(event.get("type", "message")),
                    {key: value for key, value in event.items() if key != "type"},
                    event.get("id"),
                )
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.bridge.bus.unsubscribe(subscriber)

    def _write_event(
        self,
        name: str,
        payload: Dict[str, Any],
        event_id: Optional[int] = None,
    ) -> None:
        if event_id is not None:
            self.wfile.write(f"id: {event_id}\n".encode("utf-8"))
        self.wfile.write(f"event: {name}\n".encode("utf-8"))
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
        self.wfile.flush()

    # -- static files ------------------------------------------------------

    def _serve_static(self, path: str, head_only: bool) -> None:
        resolved = self.bridge.resolve_static(path)
        if resolved is None:
            if path in ("/", "/index.html"):
                self._send_json(
                    {
                        "name": "tau",
                        "version": __version__,
                        "message": (
                            "web console not built; run `make web-build` or use "
                            "the API under /api/"
                        ),
                    },
                    head_only,
                    404,
                )
            else:
                self._send_error(HTTPStatus.NOT_FOUND, "not found", head_only)
            return

        try:
            data = resolved.read_bytes()
        except OSError:
            self._send_error(HTTPStatus.NOT_FOUND, "not found", head_only)
            return

        content_type = mimetypes.guess_type(str(resolved))[0] or (
            "application/octet-stream"
        )
        if resolved.suffix in (".js", ".mjs"):
            content_type = "text/javascript; charset=utf-8"
        elif resolved.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif resolved.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif resolved.suffix == ".svg":
            content_type = "image/svg+xml"

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self._cors_headers()
        self.send_header("X-Content-Type-Options", "nosniff")
        if "/_next/static/" in path or path.startswith("/assets/"):
            self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        else:
            self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if not head_only:
            self.wfile.write(data)

    # -- helpers -----------------------------------------------------------

    def _post_is_allowed(self) -> bool:
        origin = self.headers.get("Origin")
        return not origin or _is_local_origin(origin)

    def _read_body(self) -> Dict[str, Any]:
        content_type = (
            (self.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
        )
        if content_type != "application/json":
            raise RequestError("Content-Type must be application/json")
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or 0)
        except ValueError:
            raise RequestError("invalid Content-Length")
        if length > MAX_BODY_BYTES:
            raise RequestError("request body is too large")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RequestError("request body must be valid JSON")
        if not isinstance(payload, dict):
            raise RequestError("request body must be a JSON object")
        return payload

    def _cors_headers(self) -> None:
        origin = self.headers.get("Origin")
        if origin and _is_local_origin(origin):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Max-Age", "600")

    def _send_json(
        self,
        payload: Dict[str, Any],
        head_only: bool = False,
        status: int = HTTPStatus.OK,
    ) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        if status >= 400:
            # The request body may not have been consumed; close instead of
            # risking keep-alive desynchronisation.
            self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        if status >= 400:
            self.send_header("Connection", "close")
        self._cors_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _send_error(self, status: int, message: str, head_only: bool = False) -> None:
        self._send_json({"error": {"message": message}}, head_only, status)


def _query_flag(query: Dict[str, List[str]], name: str) -> bool:
    values = query.get(name) or []
    if not values:
        return False
    return values[0].strip().lower() in ("1", "true", "yes", "on")


def _int_param(
    query: Dict[str, List[str]],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    values = query.get(name)
    if not values:
        return default
    try:
        value = int(values[0])
    except (TypeError, ValueError):
        return default
    return max(minimum, min(value, maximum))

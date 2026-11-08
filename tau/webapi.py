"""Local HTTP bridge and web console for a running tau node.

The bridge exposes a small JSON API over loopback so the browser (or any local
script) can read what arrives from the mesh and send messages back:

    GET  /api/status       node identity, interfaces and counters
    GET  /api/peers        peers from the local registry
    GET  /api/messages     recent inbound/outbound messages
    POST /api/messages     send ``{"to": "...", "text": "..."}``
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
    GET  /api/tan/saved    saved posts, newest save first
    POST /api/tan/save     save a post to read later
    POST /api/tan/unsave   remove a post from the saved list

If a static build of the Next.js console exists (``web/out``), it is served
from the same origin.  Everything binds to loopback by default because anyone
who can reach this API can send messages as this node.
"""

from __future__ import annotations

import errno
import json
import logging
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
from .mailbox import MailboxError
from .node import NodeError, TauNode
from .peers import Peer, PeerError
from .protocol import (
    MAX_MESSAGE_CHARS,
    ProtocolError,
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
    rtt_ms = entry.get("rtt_ms")
    if isinstance(rtt_ms, (int, float)) and not isinstance(rtt_ms, bool):
        message["rtt_ms"] = rtt_ms
    mail_id = entry.get("mail_id")
    if isinstance(mail_id, str):
        # Kept so a message still queued across a restart can be matched
        # when its delivery is reported.
        message["mail_id"] = mail_id
    try:
        # The writer serialises with ensure_ascii=False, so a stored
        # string that is not UTF-8 encodable (e.g. a lone surrogate from a
        # hand-edited file) would poison every later save; drop it here.
        json.dumps(message, ensure_ascii=False).encode("utf-8")
    except (UnicodeEncodeError, ValueError):
        return None
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

        self.bus = EventBus()
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._save_lock = threading.Lock()
        # Outbox ids reported delivered before their send was recorded;
        # send_message reconciles them when it appends the record. An
        # OrderedDict used as a bounded set (its popitem(last=False)
        # evicts the oldest report; a plain dict has no such call).
        self._early_deliveries: OrderedDict[str, None] = OrderedDict()
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
        self._messages.extend(loaded)
        saved_next = raw.get("next_message_id")
        max_id = max((message["id"] for message in loaded), default=0)
        if isinstance(saved_next, int) and not isinstance(saved_next, bool):
            self._next_message_id = max(saved_next, max_id + 1, 1)
        else:
            self._next_message_id = max(max_id + 1, 1)

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
        self.bus.publish({"type": "peer", "peer": peer.public_dict()})

    def record_inbound(self, record: Dict[str, Any]) -> Dict[str, Any]:
        peer_hash = str(record.get("destination") or "")
        peer_name = str(record.get("from") or "")
        if not peer_name or peer_name == "unknown":
            peer_name = short_hash(peer_hash) if peer_hash else "unknown"
        message = self._append_message(
            direction="in",
            peer=peer_hash or None,
            peer_name=sanitize_name(peer_name, fallback="unknown"),
            text=str(record.get("text") or ""),
            sent_at=_as_float(record.get("sent_at")),
            received_at=_as_float(record.get("received_at"), default=time.time()),
            status="received",
        )
        self.bus.publish({"type": "message", "message": message})
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
            annotate_saved,
            apply_display_rules,
            author_timeline,
            following,
            resolve_author,
            timeline,
        )

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
                posts = author_timeline(conn, target, limit=limit, viewer=viewer)
            elif include_all:
                posts = timeline(conn, include_all=True, limit=limit, viewer=viewer)
            else:
                authors = [record["author"] for record in following(conn)]
                if viewer is not None:
                    authors.append(viewer)
                posts = timeline(conn, authors, limit=limit, viewer=viewer)
            annotate_saved(conn, posts)
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

    def tan_save(self, target: str) -> Dict[str, Any]:
        # Saves are local-only: no signed entry and no gossip, so the
        # social app is not needed — the same as `tau tan save`.
        from .tangents.tan import save_post

        conn = self._tan_conn()
        try:
            return save_post(conn, target)
        finally:
            conn.close()

    def tan_unsave(self, target: str) -> Dict[str, Any]:
        from .tangents.tan import unsave_post

        conn = self._tan_conn()
        try:
            return unsave_post(conn, target)
        finally:
            conn.close()

    def tan_saved(self, *, limit: int = 30, offset: int = 0) -> List[Dict[str, Any]]:
        from .identity import destination_hash, load_identity
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import annotated_saved_posts, apply_display_rules

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
            posts = annotated_saved_posts(
                conn, limit=limit, offset=offset, viewer=viewer
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

    def tan_sync(self, *, explore: bool = True) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        return app.sync(self.node, explore=explore)

    def send_message(
        self,
        target: str,
        text: str,
        *,
        timeout: float = 20.0,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target peer is required")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("message text is required")
        if len(text) > MAX_MESSAGE_CHARS:
            raise ValueError(f"message is longer than {MAX_MESSAGE_CHARS} characters")
        # Normalise up front so the recorded text is exactly what gets sent
        # (and is always UTF-8 encodable for the history file): the node
        # normalises again before sending, so this cannot change delivery.
        text = normalize_message(text)

        peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        # The send lock is NOT held across the network send: queueing
        # starts a flush thread that can deliver the entry (and report
        # it via record_delivery) before the record below exists, and
        # holding the lock across network I/O would stall every other
        # delivery report behind this send. Instead, record_delivery
        # buffers a report that beats the record, and the append below
        # reconciles it.
        response = self.node.send_message(peer, text, timeout=timeout)
        queued = bool(response.get("queued"))
        # The outbox id links this record to a later delivery; only
        # present while the message is queued.
        mail_id = response.get("id") if queued else None
        flipped = False
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
            )
            if mail_id is not None:
                with self._lock:
                    if mail_id in self._early_deliveries:
                        del self._early_deliveries[mail_id]
                        message["status"] = "delivered"
                        flipped = True
                if flipped:
                    self._save_messages()
        self.bus.publish({"type": "message", "message": message})
        if flipped:
            self.bus.publish(
                {"type": "message_updated", "message": dict(message)}
            )
        return message, response

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
            return [peer.public_dict(now) for peer in self.node.registry.all()]
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not read peer registry", exc_info=True)
            return []

    def messages(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._messages)
        if limit >= 0:
            items = items[-limit:]
        return items

    def counts(self) -> Dict[str, int]:
        with self._lock:
            message_count = len(self._messages)
        return {"messages": message_count, "peers": len(self.peers())}

    def snapshot(self) -> Dict[str, Any]:
        return {
            "node": self.node.info(),
            "peers": self.peers(),
            "messages": self.messages(limit=200),
            "counts": self.counts(),
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
            }
            if rtt is not None:
                message["rtt_ms"] = round(rtt * 1000.0, 2)
            if mail_id is not None:
                message["mail_id"] = mail_id
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


def _as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return default


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
            if path == "/api/tan/following":
                records = self.bridge.tan_following()
                self._send_json(
                    {"following": records, "count": len(records)}, head_only
                )
                return
            if path == "/api/tan/saved":
                limit = _int_param(query, "limit", default=30, minimum=1, maximum=200)
                offset = _int_param(
                    query, "offset", default=0, minimum=0, maximum=100000
                )
                try:
                    posts = self.bridge.tan_saved(limit=limit, offset=offset)
                except ValueError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                except Exception:
                    log.exception("failed to load saved Tan posts")
                    self._send_json(
                        {"error": {"message": "could not load saved posts"}},
                        head_only,
                        500,
                    )
                    return
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
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
            if path in ("/api/tan/save", "/api/tan/unsave"):
                target = body.get("target") or body.get("hash")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    if path.endswith("unsave"):
                        result = self.bridge.tan_unsave(target.strip())
                    else:
                        result = self.bridge.tan_save(target.strip())
                except ValueError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                except Exception:
                    log.exception("failed to update a saved Tan post")
                    self._send_json(
                        {"error": {"message": "could not update saved posts"}},
                        head_only,
                        500,
                    )
                    return
                self._send_json({"ok": True, "save": result}, head_only)
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
        try:
            message, response = self.bridge.send_message(target, text, timeout=timeout)
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

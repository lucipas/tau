"""Local HTTP bridge and web console for a running tau node.

The bridge exposes a small JSON API over loopback so the browser (or any local
script) can read what arrives from the mesh and send messages back:

    GET  /api/status       node identity, interfaces and counters
    GET  /api/peers        peers from the local registry
    POST /api/peers/forget  forget a peer ``{"target": "<name or hash>"}``
                           (optional ``"force": true`` strands queued mail)
    POST /api/peers/verify mark a peer verified ``{"target": "alice"}`` —
    call only after comparing the safety number out of band
    POST /api/peers/safety-number show a peer's safety number
    ``{"target": "alice"}`` without recording anything
    POST /api/peers/block   block ``{"target": "<name, hash or prefix>"}``
    POST /api/peers/unblock unblock ``{"target": "<name, hash or prefix>"}``
    GET  /api/groups       known group DMs with their member lists
    POST /api/groups       create ``{"name": "...", "members": ["<hash>", ...]}``
    POST /api/groups/delete delete ``{"group": "<id | prefix | name>"}``
    POST /api/groups/add-member add ``{"group": "...", "members": ["<hash>", ...]}``
    POST /api/groups/remove-member remove ``{"group": "...", "members": ["<hash>", ...]}``
    GET  /api/messages     recent inbound/outbound messages
    GET  /api/messages/search search stored messages by keyword
    POST /api/messages     send ``{"to": "...", "text": "..."}`` (optional ``"reply_to"`` wire id,
                           ``"expires_in"`` seconds for a disappearing message)
                           or to a group with ``{"group": "<id | prefix | name>", "text": "..."}``
                           (one outbound entry per member; replies are peer-DM-only,
                           ``expires_in`` works for DM and group sends)
    GET  /api/messages/export export stored DMs as the CLI's JSON document (loopback only)
    POST /api/messages/import import a DM export document (loopback only)
    GET  /api/files/<sender>/<name>
                           download a file received from ``<sender>``
                           (sender destination hash, sanitised file name)
    POST /api/send-file    send a file ``{"to": "...", "name": "...", "data": "<base64>"}``
                           (optional ``"timeout"`` seconds, default 300)
    POST /api/react        react ``{"target": "<wire id>", "reaction": "👍"}``
                           or retract ``{"target": "<wire id>", "clear": true}``
    POST /api/messages/delete delete ``{"id": <console message id>}`` from
                           the console history (and the node inbox, when the
                           message carries a wire id)
    GET  /api/outbox       queued mail waiting on offline peers
    POST /api/outbox/cancel cancel ``{"target": "<message id or prefix>"}``
    GET  /api/backup       download a backup archive of the data directory
    GET  /api/doctor       health verdicts for the data directory (``tau doctor``)
    POST /api/bundle/export build a signed sneakernet bundle
                           ``{"include_tan": true}`` for Tan posts too
    POST /api/bundle/import merge a bundle document (the signed JSON
                           itself) into this node's mailbox and Tan store
    POST /api/outbox/retry  retry queued mail now, ignoring backoff
                           (``{"target": "<message id or prefix>"}`` for one
                           entry; omit it to retry everything queued)
    GET  /api/outbox/held   sealed mail held for other peers
    POST /api/outbox/drop-held drop ``{"target": "<message id or prefix>"}``
    POST /api/ping         measure reachability of a peer
    POST /api/info         show a peer's reported info (version, uptime, peers)
    GET  /api/allowlist    DM allow-list: whether enforced, and who is on it
    POST /api/allowlist/add add ``{"target": "<name, hash or prefix>"}``
    POST /api/allowlist/remove remove ``{"target": "<name, hash or prefix>"}``
    POST /api/allowlist/set-enabled turn enforcement ``{"enabled": true}`` on/off
    POST /api/announce     announce this node immediately
    POST /api/id/rename    rename this node (live name + re-announce)
    GET  /api/events       server-sent events for live messages, peers and posts
    GET  /api/tan/timeline followee timeline (or all cached posts)
    GET  /api/tan/top      most-liked stored posts (``?limit=10``, ``?author=<name or hash>``)
    GET  /api/tan/search   search stored posts by keyword
    GET  /api/tan/mentions replies to your own posts
    GET  /api/tan/thread    thread rooted at a post (``?ref=<hash-or-prefix>``)
    GET  /api/tan/following followed authors
    GET  /api/tan/likes    who liked a Tan post (?target=<post hash>)
    POST /api/tan/posts    publish a Tan post
    POST /api/tan/quote    quote a post with your own comment
    POST /api/tan/like     like a post (signed reaction on your chain)
    POST /api/tan/unlike   remove your like from a post
    POST /api/tan/repost   repost a post (signed repost entry on your chain)
    POST /api/tan/delete   delete one of your own posts
    POST /api/tan/edit     edit one of your own posts
    POST /api/tan/follow   follow an author
    POST /api/tan/unfollow stop following an author
    GET  /api/tan/muted    muted authors
    POST /api/tan/mute     mute an author
    POST /api/tan/unmute   unmute an author
    GET  /api/tan/muted-keywords muted keywords
    POST /api/tan/mute-keyword mute a keyword
    POST /api/tan/unmute-keyword unmute a keyword
    POST /api/tan/sync     pull missing posts from peers
    GET  /api/tan/export   export stored posts (``?scope=mine|all``)
    POST /api/tan/import   import an export document (every entry verified)
    GET  /api/tan/saved    saved posts, newest save first
    POST /api/tan/save     save a post to read later
    POST /api/tan/unsave   remove a post from the saved list

If a static build of the Next.js console exists (``web/out``), it is served
from the same origin.  Everything binds to loopback by default because anyone
who can reach this API can send messages as this node.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import ipaddress
import json
import logging
import math
import mimetypes
import os
import queue
import shutil
import socket
import sys
import tempfile
import threading
import time
from collections import OrderedDict, deque
from contextlib import nullcontext
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, quote, unquote, urlsplit

from . import __version__
from .allowlist import AllowListError, clean_destination
from .backup import BackupError, create_backup
from .blocklist import BlockList, BlockListError, load_blocklist, save_blocklist
from .bundle import BundleError, DEFAULT_MAX_BUNDLE_BYTES
from .config import (
    MESSAGES_FILE_NAME,
    accept_files_enabled,
    send_read_receipts_enabled,
    set_node_setting,
    write_node_metadata,
)
from .inbox import MAX_REACTIONS, InboxError, merge_reaction
from .mailbox import MailboxError
from .node import NodeError, TauNode
from .peers import Peer, PeerError, safety_number
from .protocol import (
    MAX_FILE_BYTES,
    MAX_MAIL_TTL,
    MAX_MESSAGE_CHARS,
    ProtocolError,
    clean_in_reply_to,
    clean_reaction,
    excerpt_hash,
    normalize_message,
    sanitize_file_name,
    sanitize_name,
    short_hash,
)
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

DEFAULT_WEB_PORT = 8787
DEFAULT_WEB_HOST = "127.0.0.1"

MAX_BODY_BYTES = 32 * 1024
# A file send carries the bytes as base64 inside one JSON body: a 10 MiB
# file is ~13.4 MB once encoded, so this endpoint gets its own cap.  The
# bridge still enforces the 10 MiB file limit after decoding.
MAX_FILE_BODY_BYTES = 15 * 1024 * 1024
# An import carries a whole message history or a whole signed chain, so
# the 32 KiB action cap would reject any real backup. The bridge binds
# to loopback only and the document is validated entry by entry, so a
# 16 MiB cap is the bound — and the export is held to the same cap, so
# the console never hands out a backup it cannot import back (larger
# histories and caches back up with the CLI, which has no cap).
MAX_IMPORT_BYTES = 16 * 1024 * 1024
MAX_MESSAGE_LOG = 500
# Delivery reports buffered while their console record does not exist
# yet (the flush can deliver before send_message records the send).
MAX_EARLY_DELIVERIES = 128
# Wire ids removed from console history (by retraction or a console
# delete), remembered so a delivery callback already in flight when
# the removal landed cannot append the message back afterwards.
MAX_REMOVED_WIRE_IDS = 512
MAX_EVENT_LOG = 500

# Most likers one /api/tan/likes response carries. The full count is
# reported as ``total`` so the console can say when a list is truncated.
MAX_LIKERS = 100
MAX_SUBSCRIBERS = 32
# Backup archives in flight at once (being built or still streaming).  Each
# one is a full copy of the data directory in temporary storage.
MAX_CONCURRENT_BACKUPS = 2
# How long shutdown waits for an archive build to finish before cleanup.
_BACKUP_SHUTDOWN_WAIT = 3.0
SUBSCRIBER_QUEUE_SIZE = 256
SSE_HEARTBEAT_SECONDS = 15.0

_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
_LOCAL_ORIGIN_HOSTS = {"localhost", "127.0.0.1", "::1"}


class WebBridgeError(RuntimeError):
    """Raised when the web bridge cannot start or process a request."""


class PeerForgetBlocked(ValueError):
    """Forgetting a peer was refused for a reason the caller can act on.

    Carries a machine-readable ``code`` so the console can offer the
    matching next step (``queued_mail`` means the peer still has queued
    mail and the user may forget anyway) instead of parsing the message.
    """

    def __init__(self, message: str, *, code: str, queued: int = 0) -> None:
        super().__init__(message)
        self.code = code
        self.queued = queued


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


def _unlink_quietly(path: Path) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def _is_local_origin(origin: str) -> bool:
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    return host in _LOCAL_ORIGIN_HOSTS or host.endswith(".localhost")


def _is_loopback_client(client: str) -> bool:
    """True when the connected client is the local machine.

    Bulk imports are accepted only from loopback, even when the bridge
    is bound to a remote interface: an import writes many entries in
    one request, so it stays a local-operator action.
    """
    try:
        return ipaddress.ip_address(client).is_loopback
    except ValueError:
        return False


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


def _clean_stored_file(value: Any) -> Optional[Dict[str, Any]]:
    """Validate stored file metadata for a message, or ``None``.

    A file entry names the file that arrived with a message — or the
    file that was sent (``{"name", "size", "sha256"}``).  The name must
    already be sanitised — anything else is a smuggled path, not a file
    name — and the size must fit the transfer budget, so a hand-edited
    history file cannot point the console at an arbitrary path.
    Anything else — a wrong shape, an impossible size, a hash that is
    not a SHA-256 hex digest — is dropped rather than trusted, like
    every other stored field.
    """
    if not isinstance(value, dict):
        return None
    name = value.get("name")
    if not isinstance(name, str) or sanitize_file_name(name) != name:
        return None
    size = value.get("size")
    if (
        not isinstance(size, int)
        or isinstance(size, bool)
        or size < 1
        or size > MAX_FILE_BYTES
    ):
        return None
    sha256 = value.get("sha256")
    if (
        not isinstance(sha256, str)
        or len(sha256) != 64
        or any(char not in "0123456789abcdef" for char in sha256.lower())
    ):
        return None
    return {"name": name, "size": size, "sha256": sha256.lower()}


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
    stored_file = _clean_stored_file(entry.get("file"))
    if stored_file is not None:
        message["file"] = stored_file
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


def _public_peer_info(response: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only the peer-reported fields the console shows.

    The response comes from a remote node, so a field with an unexpected
    type is dropped instead of trusted.
    """
    info: Dict[str, Any] = {}
    for key in ("name", "version", "rns_version"):
        value = response.get(key)
        if isinstance(value, str):
            info[key] = value
    for key in ("protocol", "uptime", "peers"):
        value = response.get(key)
        if isinstance(value, bool):
            continue
        # NaN and infinity are not valid JSON; drop them with other bad types.
        if isinstance(value, int) or (
            isinstance(value, float) and math.isfinite(value)
        ):
            info[key] = value
    lxmf = response.get("lxmf")
    if isinstance(lxmf, dict) and isinstance(lxmf.get("enabled"), bool):
        info["lxmf_enabled"] = lxmf["enabled"]
    return info


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
        # A full-store audit verifies every signature, so only one runs
        # at a time; a second caller is told the audit is busy instead
        # of queuing behind it and stalling the console's worker threads.
        self._verify_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._save_lock = threading.Lock()
        # Serialises concurrent renames so a second rename cannot read
        # the old name, persist it, and leave disk and the live node split.
        self._rename_lock = threading.Lock()
        # One health check at a time: run_checks reads whole store files
        # and runs a SQLite integrity check, so concurrent scans would
        # contend with the serving node for threads, disk and CPU.
        self._doctor_lock = threading.Lock()
        # Serializes block/unblock read-modify-write cycles so concurrent
        # console requests cannot overwrite each other's changes.
        self._blocklist_lock = threading.Lock()
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
        # Wire ids removed from console history (by retraction or a
        # console delete).  A delivery callback already in flight when
        # the removal landed must not append the message back
        # afterwards; record_inbound drops those ids.  An OrderedDict
        # used as a bounded set, like _early_deliveries.
        self._removed_wire_ids: OrderedDict[str, None] = OrderedDict()
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
        # Backup bookkeeping, guarded by _backup_lock: archives being built,
        # archives awaiting download, and the temp paths to clean up on stop.
        self._backup_lock = threading.Lock()
        self._backup_idle = threading.Condition(self._backup_lock)
        self._backups_building = 0
        self._backup_artifacts: Set[Path] = set()
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
        with self._backup_lock:
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
        # Request threads are daemons and may outlive this call.  Let any
        # archive build finish (so its staging file is renamed or removed),
        # then delete every archive still on disk: each holds the identity.
        with self._backup_lock:
            self._backup_idle.wait_for(
                lambda: self._backups_building == 0,
                timeout=_BACKUP_SHUTDOWN_WAIT,
            )
            leftovers = list(self._backup_artifacts)
            self._backup_artifacts.clear()
        for path in leftovers:
            _unlink_quietly(path)

    def finish_backup(self, path: Path) -> None:
        """Release an archive returned by :meth:`backup` and delete it."""
        with self._backup_lock:
            self._backup_artifacts.discard(path)
        _unlink_quietly(path)

    # -- message history ---------------------------------------------------

    def _data_dir(self) -> Optional[Path]:
        """This node's data directory, or ``None`` for a dataless node.

        A node without a data directory (tests, embeds) has no
        ``node.json`` to read settings from or persist them to.
        """
        data_dir = getattr(self.node, "data_dir", None)
        if data_dir is None:
            config = getattr(self.node, "config", None)
            data_dir = getattr(config, "data_dir", None)
        if data_dir is None:
            return None
        return Path(data_dir)

    def settings(self) -> Dict[str, Any]:
        """This node's own settings, as ``tau config`` manages them.

        Read live from ``node.json`` on every call, so a change made on
        the CLI is reflected by the next console read without a restart
        — and a change made here is picked up by the running node the
        same way, since it re-reads these keys on every use.
        """
        data_dir = self._data_dir()
        if data_dir is None:
            raise WebBridgeError("this node has no data directory")
        return {
            "accept_files": accept_files_enabled(data_dir),
            "send_read_receipts": send_read_receipts_enabled(data_dir),
        }

    def update_setting(self, setting: str, value: bool) -> Dict[str, Any]:
        """Change one node setting and return the fresh settings.

        Only the keys ``tau config`` manages are accepted, and the value
        must be a real boolean — anything else is a client error, not a
        value to coerce. The write keeps the other settings (and the
        node's name) in place.
        """
        if setting not in ("accept_files", "send_read_receipts"):
            raise ValueError(f"unknown setting {setting!r}")
        if not isinstance(value, bool):
            raise ValueError(f"setting {setting!r} must be true or false")
        data_dir = self._data_dir()
        if data_dir is None:
            raise WebBridgeError("this node has no data directory")
        set_node_setting(data_dir, setting, value)
        return self.settings()

    def _resolve_messages_path(self) -> Optional[Path]:
        """Data-dir path for persisted message history, or ``None``.

        A node without a data directory (tests, embeds) keeps messages in
        memory only.
        """
        data_dir = self._data_dir()
        if data_dir is None:
            return None
        return data_dir / MESSAGES_FILE_NAME

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

    def _save_messages(self, *, strict: bool = False) -> None:
        """Persist the current message log to ``messages.json``.

        Saves are serialised through ``_save_lock`` and the snapshot is
        taken inside it, so a slow write can never let an older snapshot
        overwrite a newer one: the last save to finish always holds the
        newest log. A failed write only logs a warning: the messages are
        already in memory, and a full disk (or an unencodable record) must
        never break sending or receiving. ``strict`` callers (deletion)
        need the write to have landed, so they get the error instead.
        """
        path = self._messages_path
        if path is None:
            return
        with self._save_lock:
            self._save_messages_locked(strict=strict)

    def _save_messages_locked(self, *, strict: bool = False) -> None:
        """Write the current log; the caller holds ``_save_lock``.

        Split out so a deletion can hold the save lock across removal,
        strict persistence and failure rollback: no other save can
        persist the transiently deleted deque in between.
        """
        path = self._messages_path
        if path is None:
            return
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
            if strict:
                raise
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

    def _local_peer_payload(self, peer: Peer) -> Dict[str, Any]:
        # local_dict: the console replaces its record with this payload, so
        # it must carry the same trust fields — a public_dict payload would
        # silently drop the verified badge and any key-change warning.
        payload = peer.local_dict()
        # Annotate like the snapshot does, so a live update or alias
        # response does not erase the frontend's blocked badge.
        blocklist = getattr(self.node, "blocklist", None)
        payload["blocked"] = bool(
            blocklist is not None and blocklist.is_blocked(payload.get("destination"))
        )
        return payload

    def record_peer(self, peer: Peer) -> None:
        self.bus.publish({"type": "peer", "peer": self._local_peer_payload(peer)})

    def _remember_removed_wire_id(self, wire_id: str) -> None:
        """Remember a wire id whose console entry is gone (caller holds the lock).

        A delivery callback already in flight when the entry was
        removed must not append the message back afterwards; see
        :meth:`record_inbound`.  Bounded: the oldest ids evict first.
        """
        self._removed_wire_ids[wire_id] = None
        while len(self._removed_wire_ids) > MAX_REMOVED_WIRE_IDS:
            self._removed_wire_ids.popitem(last=False)

    def record_inbound(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
            file=record.get("file"),
            reject_removed_wire_id=True,
        )
        if message is None:
            # A retraction (or console delete) already removed this
            # message; the delivery callback was in flight when it
            # landed, and appending now would resurrect it after a
            # successful retraction.
            return None
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
            annotate_saved,
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

    def tan_thread(self, ref: str) -> List[Dict[str, Any]]:
        """Return the thread rooted at the post *ref* names, oldest first.

        Mirrors :func:`tau.cli.cmd_tan_thread`: the same resolution and
        visibility rules, so the console shows exactly what the CLI
        shows for the same reference.  A reference that names no post,
        or a deleted or unlinked one, raises :class:`TanError` (via
        :func:`tau.tangents.tan.thread`) and the HTTP layer reports it
        as a 400.
        """
        from .identity import destination_hash, load_identity
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import apply_display_rules, thread

        if not isinstance(ref, str) or not ref.strip():
            raise ValueError("field 'ref' is required")
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
            posts = thread(conn, ref.strip(), viewer=viewer)
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

    def tan_mentions(self, *, limit: int = 30) -> List[Dict[str, Any]]:
        """Return replies to this node's own posts, newest first.

        Mirrors :func:`tau.cli.cmd_tan_mentions`: the same selection
        and visibility rules, so the console shows exactly what the
        CLI shows for the same store, plus the console's display rules
        (the thread view applies the same).  The node's block-list is
        applied as in :meth:`tan_timeline`.  Without an identity there
        is nothing of yours to be replied to, so this raises
        :class:`RequestError`, which the HTTP layer reports as a 400.
        """
        from .identity import destination_hash, load_identity
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import apply_display_rules, mentions

        # One snapshot for every console surface, as in tan_timeline.
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
            if viewer is None:
                raise RequestError(
                    "this node has no identity, so there are no replies to show; "
                    "run `tau init` to create one"
                )
            posts = mentions(
                conn,
                viewer,
                limit=limit,
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

    def tan_search(
        self,
        *,
        query: str,
        author: Optional[str] = None,
        limit: int = 30,
    ) -> List[Dict[str, Any]]:
        """Return stored posts matching a keyword, newest first.

        Mirrors :func:`tau.cli.cmd_tan_search`: the same case-insensitive
        substring match and visibility rules, so the console shows exactly
        what the CLI shows for the same store.  The node's block-list is
        applied as in :meth:`tan_timeline`, and display rules are applied
        on top.  An empty query or an author that cannot be resolved
        raises :class:`RequestError`, which the HTTP layer reports as a
        400; anything unexpected stays an exception and becomes a 500.
        """
        from .identity import destination_hash, load_identity
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import (
            TanError,
            apply_display_rules,
            resolve_author,
            search_posts,
        )

        if not isinstance(query, str) or not query.strip():
            raise RequestError("search text must not be empty")
        # One snapshot for every console surface, as in tan_timeline.
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
            target = None
            if author:
                try:
                    target, _ = resolve_author(
                        conn, author, registry=self.node.registry
                    )
                except TanError as exc:
                    raise RequestError(str(exc)) from exc
            try:
                posts = search_posts(
                    conn,
                    query,
                    author=target,
                    limit=limit,
                    viewer=viewer,
                    exclude_authors=blocked,
                    exclude_all=exclude_all,
                )
            except TanError as exc:
                raise RequestError(str(exc)) from exc
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
        # The block-list is applied inside top_posts, before the limit,
        # so blocked posts never take up result slots.
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
                exclude_authors=blocked,
            )
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

    def tan_authors(
        self, *, sort: str = "recent", limit: int = 50, offset: int = 0
    ) -> Dict[str, Any]:
        """One page of authors with a visible post in the local Tan store.

        Mirrors :func:`tau.cli.cmd_tan_authors`: the same records and
        sort orders, so the console shows exactly what the CLI shows
        for the same store.  A read-only listing must not create the
        store on a fresh data directory, so a missing database file
        yields an empty page.  An unknown *sort* raises TanError (a
        ValueError), which the HTTP layer reports as a 400.  The
        returned ``total`` is how many authors the directory holds
        behind the page, so the console can offer the next one.
        """
        from .tangents.tan import count_authors, list_authors, open_readonly_db

        # Validate before the missing-file short-circuit so an unknown
        # sort is a 400 on a fresh data directory too.
        if sort not in ("recent", "posts", "name"):
            raise ValueError(f"unknown author sort: {sort!r}")
        db_path = self._tan_db_path()
        if not db_path.exists():
            return {"authors": [], "total": 0}
        # One snapshot of the node's block-list, as in tan_timeline: a
        # blocked author is hidden here too, and a fail-closed list hides
        # every author.
        blocklist = getattr(self.node, "blocklist", None)
        blocked = blocklist.blocked if blocklist is not None else frozenset()
        exclude_all = bool(blocklist is not None and blocklist.block_all)
        # Existing stores are read-only here, as in the CLI authors command.
        conn = open_readonly_db(db_path)
        try:
            return {
                "authors": list_authors(
                    conn,
                    sort=sort,
                    limit=limit,
                    offset=offset,
                    exclude_authors=sorted(blocked),
                    exclude_all=exclude_all,
                ),
                "total": count_authors(
                    conn,
                    exclude_authors=sorted(blocked),
                    exclude_all=exclude_all,
                ),
            }
        finally:
            conn.close()


    def tan_suggest(self, limit: int = 5) -> List[Dict[str, Any]]:
        from .identity import destination_hash, load_identity
        from .tangents.tan import suggest_authors

        # Same block-list snapshot as the timeline: a blocked author is
        # never suggested, and a failed-closed list suggests nobody.
        blocklist = getattr(self.node, "blocklist", None)
        blocked = blocklist.blocked if blocklist is not None else frozenset()
        if blocklist is not None and blocklist.block_all:
            return []
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
            return suggest_authors(
                conn, viewer=viewer, limit=limit, exclude=blocked
            )
        finally:
            conn.close()

    def tan_likers(self, target: Any) -> Dict[str, Any]:
        """List who liked one Tan post, newest like first.

        Visibility matches the timeline: only a linked timeline-kind
        post whose author is neither muted nor blocked discloses
        anything, and when the owner's ruleset hides both reaction kinds
        no likers are disclosed at all. A post its author deleted is
        reported as deleted, not unknown, matching the like endpoint.
        Names resolve from the peer registry first, then from the name on
        the liker's latest stored post; an author with no known name keeps
        an empty name and the console falls back to the short hash. Only
        author identities are exposed, never reaction contents. At most
        ``MAX_LIKERS`` entries are returned; ``total`` is the full count
        before that cap.
        """
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import (
            _NOT_DELETED,
            KIND_LIKE,
            KIND_UNLIKE,
            TIMELINE_KINDS_SQL,
            TanError,
            deleted_targets,
            muted_author_set,
            post_likers,
        )

        if not isinstance(target, str) or not target.strip():
            raise ValueError("target is required")
        # Hashes are stored lowercase; normalize before any lookup.
        clean = target.strip().lower()
        # Same author exclusions as tan_timeline, including fail-closed
        # block-all, so a liker list never shows hidden identities.
        blocklist = getattr(self.node, "blocklist", None)
        blocked = set(blocklist.blocked) if blocklist is not None else set()
        exclude_all = bool(blocklist is not None and blocklist.block_all)
        conn = self._tan_conn()
        try:
            if clean in deleted_targets(conn):
                raise TanError(f"post has been deleted: {clean!r}")
            # A target is visible only when the timeline would show it:
            # linked, a timeline kind, not deleted by its author, and by
            # an author who is neither muted nor blocked.
            row = conn.execute(
                "SELECT author FROM posts WHERE hash = ? AND linked = 1 "
                f"AND kind IN ({TIMELINE_KINDS_SQL}) AND {_NOT_DELETED}",
                (clean,),
            ).fetchone()
            hidden = muted_author_set(conn) | blocked
            if row is None or exclude_all or str(row["author"]) in hidden:
                raise TanError(f"unknown post: {clean!r}")
            app = self._tan_app()
            if app is not None:
                ruleset = app.ruleset
            else:
                try:
                    ruleset = load_ruleset(
                        getattr(self.node.config, "tan_rules_path", None)
                    )
                except RulesError:
                    ruleset = Ruleset()
            if not (ruleset.displays(KIND_LIKE) or ruleset.displays(KIND_UNLIKE)):
                # Reactions are hidden for this viewer, so the timeline
                # shows no like annotations; disclose no likers either.
                return {"target": clean, "likers": [], "count": 0, "total": 0}
            entries = [
                entry
                for entry in post_likers(conn, clean)
                if entry["author"] not in hidden
            ]
            total = len(entries)
            shown = entries[:MAX_LIKERS]
            names = self._tan_author_names(
                conn, [entry["author"] for entry in shown]
            )
            likers = []
            for entry in shown:
                author = entry["author"]
                name = ""
                peer = self.node.registry.get(author)
                if peer is not None:
                    name = peer.display_name
                if not name:
                    name = names.get(author, "")
                likers.append(
                    {
                        "author": author,
                        "name": name,
                        "liked_at": entry["liked_at"],
                    }
                )
            return {
                "target": clean,
                "likers": likers,
                "count": len(likers),
                "total": total,
            }
        finally:
            conn.close()

    def _tan_author_names(
        self, conn: Any, authors: List[str]
    ) -> Dict[str, str]:
        """Latest stored display name per author, in one query.

        Rows arrive newest first, so the first name seen for an author
        is their latest. Registry names take precedence and are resolved
        by the caller.
        """
        if not authors:
            return {}
        placeholders = ", ".join("?" for _ in authors)
        rows = conn.execute(
            "SELECT author, name FROM posts WHERE author IN "
            f"({placeholders}) AND name != '' ORDER BY created_at DESC",
            list(authors),
        ).fetchall()
        names: Dict[str, str] = {}
        for row in rows:
            names.setdefault(str(row["author"]), str(row["name"]))
        return names


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

    def tan_post_count(self) -> int:
        """Number of readable posts in the local Tan store.

        Counts linked timeline-kind entries their author has not
        deleted — the same set the timeline shows — instead of the
        session-scoped post-event buffer, which misses posts already
        on disk when the bridge starts, posts pulled in by sync, and
        posts this node created itself.  Falls back to the buffer
        when the store cannot be read.
        """
        if self._tan_app() is None:
            return 0
        try:
            from .tangents.tan import TIMELINE_KINDS_SQL, _NOT_DELETED

            conn = self._tan_conn()
            try:
                row = conn.execute(
                    f"SELECT COUNT(*) AS n FROM posts "
                    f"WHERE kind IN ({TIMELINE_KINDS_SQL}) AND linked = 1 "
                    f"AND {_NOT_DELETED}"
                ).fetchone()
                return int(row["n"])
            finally:
                conn.close()
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not count Tan posts", exc_info=True)
            return len(self.tan_posts(limit=0))

    def _resolve_mute_target(self, conn, target: str):
        """Resolve a mute/unmute target the way the CLI does.

        The peer registry does not know authors whose posts are only
        cached, and a muted author may have no cached posts at all — so
        after the registry/hash/prefix lookup fails, try the name stored
        with the mute, then the name on the author's cached posts.
        """
        from .tangents.tan import (
            TanError,
            resolve_author,
            resolve_cached_author,
            resolve_muted_name,
        )

        try:
            return resolve_author(conn, target, registry=self.node.registry)
        except TanError as exc:
            match = resolve_muted_name(conn, target)
            if match is not None:
                return match
            try:
                return resolve_cached_author(conn, target)
            except TanError:
                raise exc from None

    def _tan_viewer(self) -> Optional[str]:
        """This node's author hash, or None when no identity is loaded."""
        from .identity import destination_hash, load_identity

        app = self._tan_app()
        if app is not None:
            return app.author
        identity = load_identity(self.node.config.identity_path)
        return destination_hash(identity).hex() if identity is not None else None

    def tan_mute(self, target: str) -> Dict[str, Any]:
        from .tangents.tan import TanError, mute

        conn = self._tan_conn()
        try:
            author, name = self._resolve_mute_target(conn, target)
            viewer = self._tan_viewer()
            if viewer is not None and author == viewer:
                raise TanError("cannot mute yourself")
            return mute(conn, author, name=name)
        finally:
            conn.close()

    def tan_unmute(self, target: str) -> Dict[str, Any]:
        from .tangents.tan import unmute

        conn = self._tan_conn()
        try:
            author, name = self._resolve_mute_target(conn, target)
            return {"author": author, "name": name, "unmuted": unmute(conn, author)}
        finally:
            conn.close()

    def tan_muted(self) -> List[Dict[str, Any]]:
        from .tangents.tan import author_stats_many, muted

        conn = self._tan_conn()
        try:
            records = muted(conn)
            stats = author_stats_many(conn, [record["author"] for record in records])
            for record in records:
                record.update(stats[record["author"]])
            return records
        finally:
            conn.close()

    def tan_mute_keyword(self, keyword: str) -> Dict[str, Any]:
        from .tangents.tan import mute_keyword

        conn = self._tan_conn()
        try:
            return mute_keyword(conn, keyword)
        finally:
            conn.close()

    def tan_unmute_keyword(self, keyword: str) -> Dict[str, Any]:
        from .tangents.tan import unmute_keyword

        conn = self._tan_conn()
        try:
            removed = unmute_keyword(conn, keyword)
            return {
                "keyword": " ".join(str(keyword).split()).casefold(),
                "unmuted": removed,
            }
        finally:
            conn.close()

    def tan_muted_keywords(self) -> List[Dict[str, Any]]:
        from .tangents.tan import muted_keywords

        conn = self._tan_conn()
        try:
            return muted_keywords(conn)
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

    def tan_repost(self, target: str) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        conn = self._tan_conn()
        try:
            from .tangents.tan import resolve_post

            post_hash = resolve_post(conn, target)["hash"]
        finally:
            conn.close()
        return app.repost(post_hash, publish=True)

    def tan_quote(self, target: str, text: str) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        conn = self._tan_conn()
        try:
            from .tangents.tan import resolve_post

            post_hash = resolve_post(conn, target)["hash"]
        finally:
            conn.close()
        return app.quote(post_hash, text, publish=True)

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

    def tan_edit(self, target: str, text: str) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        conn = self._tan_conn()
        try:
            from .tangents.tan import resolve_post

            post_hash = resolve_post(conn, target)["hash"]
        finally:
            conn.close()
        return app.edit(post_hash, text, publish=True)

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

    def tan_saved(
        self, *, limit: int = 30, offset: int = 0
    ) -> Dict[str, Any]:
        from .identity import destination_hash, load_identity
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import annotated_saved_posts

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
            posts, next_offset = annotated_saved_posts(
                conn, limit=limit, offset=offset, viewer=viewer, ruleset=ruleset
            )
            return {"posts": posts, "next_offset": next_offset}
        finally:
            conn.close()

    def tan_sync(self, *, explore: bool = True) -> Dict[str, Any]:
        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        return app.sync(self.node, explore=explore)

    def tan_rules(self) -> Dict[str, Any]:
        """This node's Tan admission ruleset, as ``tau tan rules`` reports it.

        The live Tan app's ruleset when the feed is enabled, so the
        console shows exactly what the running node enforces; otherwise
        the rules file in the data directory. A rules file that cannot
        be parsed fails closed here exactly as it does for admission.
        """
        from .tangents.rules import RulesError, Ruleset, load_ruleset

        app = self._tan_app()
        if app is not None:
            return app.ruleset.to_dict()
        config = getattr(self.node, "config", None)
        path = getattr(config, "tan_rules_path", None)
        if path is None:
            raise WebBridgeError("this node has no data directory")
        try:
            return load_ruleset(path).to_dict()
        except RulesError:
            return Ruleset(default_allow=False).to_dict()

    def update_tan_rules(self, payload: Any) -> Dict[str, Any]:
        """Replace the Tan admission ruleset and apply it to the running node.

        *payload* is the full ruleset as ``Ruleset.to_dict`` reports it.
        Author entries accept a peer name or author hash, resolved like
        ``tau tan rules`` resolves them; anything unresolvable, or a
        malformed ruleset, is a ``ValueError``. The saved ruleset is
        reloaded into the live Tan app, so the change applies at once
        instead of on the next restart — a CLI edit still needs a
        restart, since the CLI cannot reach the running process.
        """
        from .tangents.rules import Ruleset, save_ruleset
        from .tangents.tan import resolve_author

        if not isinstance(payload, dict):
            raise ValueError("ruleset must be a JSON object")
        config = getattr(self.node, "config", None)
        path = getattr(config, "tan_rules_path", None)
        if path is None:
            raise WebBridgeError("this node has no data directory")
        data = dict(payload)
        conn = self._tan_conn()
        try:
            for field in ("allow_authors", "block_authors"):
                values = data.get(field)
                if values is None:
                    continue
                if not isinstance(values, list):
                    raise ValueError(f"{field} must be a list of authors")
                resolved = []
                for value in values:
                    author, _ = resolve_author(
                        conn, value, registry=self.node.registry
                    )
                    resolved.append(author)
                data[field] = resolved
        finally:
            conn.close()
        ruleset = Ruleset.from_dict(data)
        save_ruleset(path, ruleset)
        app = self._tan_app()
        if app is not None:
            app.reload_ruleset()
        return ruleset.to_dict()

    def tan_verify(self) -> Dict[str, Any]:
        """Audit the local Tan store, re-verifying every entry and its chain.

        Opens the store read-only, like ``tau tan verify``: verifying
        must never create a missing store or write to an existing one.
        Only one audit runs at a time; a second caller gets a
        ``WebBridgeError`` (answered 429) instead of queuing behind a
        full-store signature check.
        """
        import sqlite3

        from .tangents.tan import verify_store

        path = self._tan_db_path()
        if not path.is_file():
            raise FileNotFoundError(f"no Tan store at {path}")
        if not self._verify_lock.acquire(blocking=False):
            raise WebBridgeError("a store audit is already running")
        try:
            conn = sqlite3.connect(
                f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=10.0
            )
            conn.row_factory = sqlite3.Row
            try:
                return verify_store(conn)
            finally:
                conn.close()
        finally:
            self._verify_lock.release()


    def messages_export(self) -> Dict[str, Any]:
        """Return the portable backup document the CLI writes.

        The same document ``tau messages export`` produces, so a
        console backup restores with the CLI and vice versa.
        """
        inbox = getattr(self.node, "inbox", None)
        if inbox is None:
            raise WebBridgeError("this node has no message inbox")
        document = inbox.export_document()
        # The console must never hand out a backup it cannot import
        # back: the import route rejects bodies over MAX_IMPORT_BYTES,
        # so an export past that size is refused here, with the CLI as
        # the route for larger histories.
        size = len(json.dumps(document, ensure_ascii=False).encode("utf-8"))
        if size > MAX_IMPORT_BYTES:
            raise ValueError(
                "this export is larger than the console can import back "
                f"({MAX_IMPORT_BYTES // (1024 * 1024)} MiB); back up with "
                "`tau messages export` from the command line instead"
            )
        return document

    def messages_import(self, document: Any) -> Dict[str, Any]:
        """Merge an export document into the node inbox.

        Returns the same ``{"imported", "present", "skipped",
        "errors"}`` counts as the CLI import; a bad entry is skipped
        with its error recorded and never aborts the rest. Newly
        imported entries are also mirrored into the console feed, so
        restored messages show up right away instead of landing in
        the inbox only. A message its sender retracted is skipped
        like on the command line, so an old export cannot resurrect
        it — or mirror it into the feed.
        """
        inbox = getattr(self.node, "inbox", None)
        if inbox is None:
            raise WebBridgeError("this node has no message inbox")
        # The inbox reports the records this call stored, so a concurrent
        # import or live arrival can never make the same entry mirror twice.
        inserted: List[Dict[str, Any]] = []
        mailbox = getattr(self.node, "mailbox", None)
        if mailbox is not None:
            # Like the CLI import: hold the mailbox transaction across
            # the import so a retract persisted by a running node is
            # seen, and it cannot land between the check and the insert.
            with mailbox.transaction():
                result = inbox.import_messages(
                    document,
                    inserted=inserted,
                    is_retracted=mailbox.is_retracted,
                )
        else:
            result = inbox.import_messages(document, inserted=inserted)
        # An invalid entry's id comes from untrusted JSON and may hold
        # a lone surrogate, which the UTF-8 response encoder cannot
        # write. Replace unencodable characters so the counts still
        # reach the console.
        for error in result["errors"]:
            for key in ("id", "error"):
                value = error.get(key)
                if isinstance(value, str):
                    error[key] = value.encode("utf-8", "replace").decode("utf-8")
        if inserted:
            self._mirror_imported_messages(inserted)
            # Other open consoles learn the new unread counts from SSE;
            # the importing tab also gets them in the response.
            unread = self.unread()
            self.bus.publish({"type": "unread", "unread": unread})
            result["unread"] = unread
        return result

    def _mirror_imported_messages(self, records: List[Dict[str, Any]]) -> None:
        """Append newly imported inbox records to the console feed.

        The feed is the console's own log, not a view of the inbox,
        so without this a restore would land in ``inbox.json`` only
        and the user would see nothing change. Records are mirrored
        in stored (oldest-first) order with the same field mapping
        live traffic uses in :meth:`record_inbound`; a disappearing
        message whose deadline already passed is skipped — the feed
        purges it on read anyway — and the log is saved once, not
        once per entry.
        """
        names = {
            item["destination"]: item["name"]
            for item in self.peers()
            if isinstance(item.get("destination"), str)
            and isinstance(item.get("name"), str)
            and item.get("name")
        }
        now = time.time()
        appended: List[Dict[str, Any]] = []
        for record in records:
            expires_at = record.get("expires_at")
            if (
                isinstance(expires_at, (int, float))
                and not isinstance(expires_at, bool)
                and float(expires_at) <= now
            ):
                continue
            direction = (
                record.get("direction")
                if record.get("direction") in ("in", "out")
                else "in"
            )
            peer_hash = str(record.get("destination") or "")
            if direction == "in":
                peer_name = str(record.get("from") or "")
                if not peer_name or peer_name == "unknown":
                    peer_name = short_hash(peer_hash) if peer_hash else "unknown"
            else:
                peer_name = names.get(peer_hash) or (
                    short_hash(peer_hash) if peer_hash else "unknown"
                )
            group = record.get("group") if record.get("kind") == "group" else None
            wire_id = record.get("id")
            # A file record has no text: describe it the way the live
            # file arrival does, so the feed shows the same line.
            if record.get("kind") == "file":
                text = file_arrival_text(record)
            else:
                text = str(record.get("text") or "")
            message = self._append_message(
                direction=direction,
                peer=peer_hash or None,
                peer_name=sanitize_name(peer_name, fallback="unknown"),
                text=text,
                sent_at=_as_float(record.get("sent_at")),
                received_at=_as_float(record.get("received_at"), default=time.time()),
                status="received" if direction == "in" else "delivered",
                group=group if isinstance(group, str) else None,
                group_name=(
                    str(record.get("group_name") or "")
                    if isinstance(record.get("group_name"), str)
                    else None
                ),
                expires_at=(
                    float(expires_at)
                    if isinstance(expires_at, (int, float))
                    and not isinstance(expires_at, bool)
                    else None
                ),
                wire_id=wire_id if isinstance(wire_id, str) else None,
                in_reply_to=record.get("in_reply_to"),
                save=False,
            )
            appended.append(message)
        self._save_messages()
        # Live consoles show arrivals from their message events; publish
        # the restored entries the same way, in feed order. Blocked
        # senders stay hidden like the messages API hides them: the
        # record is kept for unblocking, but no event shows it.
        blocklist = getattr(self.node, "blocklist", None)
        for message in appended:
            if (
                message.get("direction") == "in"
                and blocklist is not None
                and blocklist.is_blocked(
                    message.get("destination") or message.get("peer")
                )
            ):
                continue
            self.bus.publish({"type": "message", "message": message})

    def tan_export(self, *, all_authors: bool = False) -> Dict[str, Any]:
        """Return the portable backup document the CLI writes.

        The node's own chain by default, every cached author with
        *all_authors* — the same document :func:`tau tan export`
        produces, so a console backup restores with the CLI and
        vice versa.
        """
        from .tangents.tan import export_entries

        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        conn = self._tan_conn()
        try:
            if all_authors:
                document = export_entries(conn, all_authors=True)
            else:
                document = export_entries(conn, author=app.author)
        finally:
            conn.close()
        # The console must never hand out a backup it cannot import
        # back: the import route rejects bodies over MAX_IMPORT_BYTES,
        # so an export past that size is refused here, with the CLI as
        # the route for larger caches.
        size = len(json.dumps(document, ensure_ascii=False).encode("utf-8"))
        if size > MAX_IMPORT_BYTES:
            raise ValueError(
                "this export is larger than the console can import back "
                f"({MAX_IMPORT_BYTES // (1024 * 1024)} MiB); back up with "
                "`tau tan export` from the command line instead"
            )
        return document

    def tan_import(self, document: Any) -> Dict[str, Any]:
        """Store every entry in an export document, verifying each one.

        Returns the same ``{\"imported\", \"present\", \"skipped\",
        \"errors\"}`` counts as the CLI import; a bad entry is skipped
        with its error recorded and never aborts the rest.
        """
        from .tangents.tan import import_entries

        app = self._tan_app()
        if app is None:
            raise WebBridgeError("the Tan social feed is not enabled; run serve --social")
        conn = self._tan_conn()
        try:
            result = import_entries(conn, document)
        finally:
            conn.close()
        # An invalid entry's hash comes from untrusted JSON and may hold a
        # lone surrogate, which the UTF-8 response encoder cannot write.
        # Replace unencodable characters so the counts still reach the console.
        for error in result["errors"]:
            error["hash"] = error["hash"].encode("utf-8", "replace").decode("utf-8")
            error["error"] = error["error"].encode("utf-8", "replace").decode("utf-8")
        return result

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

    def send_group_message(
        self,
        group: str,
        text: str,
        *,
        timeout: float = 20.0,
        expires_at: Optional[float] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Send a message to every member of a group from the console.

        ``expires_at`` is an optional sender-set expiry (epoch seconds),
        exactly like a direct message: it travels in every leg's
        payload, a queued leg expires at the same deadline, and the
        receiver purges the message once it passes.  The outbound feed
        entry carries it too, so the console can show the deadline.

        The group is resolved by id, id prefix, or exact name through
        ``GroupStore.find`` (unknown or ambiguous queries raise
        ``ValueError``).  One outbound feed entry is recorded per leg,
        each carrying the group id/name badge, its own delivery status,
        and its outbox id while queued — the same shape as an inbound
        group message, so the feed renders both directions alike and
        queued legs flip to delivered through the same delivery-report
        machinery as a peer DM.  A leg the member explicitly refused,
        or that could not be queued, is recorded as ``failed`` rather
        than silently dropped, so the sender sees which members did
        not get the message.
        """
        if not isinstance(group, str) or not group.strip():
            raise ValueError("target group is required")
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
        # Normalise up front so the recorded text is exactly what gets
        # sent: the node normalises again before sending, so this cannot
        # change delivery.
        text = normalize_message(text)

        # Reload the persisted store first: a group created or deleted by
        # another process (``tau group``) must be seen by this send.
        self.node.groups.list_groups()
        group_record = self.node.groups.find(group.strip())
        response = self.node.send_group_message(
            group_record, text, timeout=timeout, expires_at=expires_at
        )
        messages: List[Dict[str, Any]] = []
        for result in response.get("results") or []:
            member = str(result.get("to") or "")
            peer_name = result.get("to_name") or (
                short_hash(member) if member else "unknown"
            )
            queued = bool(result.get("queued"))
            delivered = bool(result.get("delivered"))
            status = "queued" if queued else "delivered" if delivered else "failed"
            # The outbox id links a queued leg to a later delivery; only
            # present while the leg is queued. A failed leg carries no
            # wire id: the recipient never saw that message, so no reply
            # may be quoted against it.
            mail_id = result.get("id") if queued else None
            wire_id = (
                result.get("id")
                if (delivered or queued) and isinstance(result.get("id"), str)
                else None
            )
            flipped = False
            early_read: Optional[float] = None
            with self._send_lock:
                message = self._append_message(
                    direction="out",
                    peer=member or None,
                    peer_name=peer_name,
                    text=text,
                    sent_at=time.time(),
                    received_at=time.time(),
                    status=status,
                    rtt=_as_float(result.get("rtt")),
                    mail_id=mail_id,
                    group=group_record["id"],
                    group_name=group_record.get("name") or "",
                    expires_at=expires_at,
                    wire_id=wire_id,
                )
                with self._lock:
                    if mail_id is not None and mail_id in self._early_deliveries:
                        del self._early_deliveries[mail_id]
                        message["status"] = "delivered"
                        flipped = True
                    read_key = wire_id or mail_id
                    if read_key is not None:
                        early_read = self._early_reads.pop(
                            (member, read_key), None
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
            messages.append(message)
        return messages, response


    def send_file(
        self,
        target: str,
        name: str,
        data: bytes,
        *,
        timeout: float = 300.0,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Send a file to a peer from the console.

        The bytes are staged in a temporary directory under the
        sanitised file name — ``TauNode.send_file`` takes a path — and
        the staging directory is removed once the transfer finishes or
        fails.  The sent file is recorded in the console feed as a file
        entry (name, size, SHA-256) with status ``delivered``; a failed
        transfer raises instead of recording anything, like a failed
        text send.  Files are direct links only in v1, so a refused or
        unreachable peer fails fast instead of queueing.
        """
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target peer is required")
        clean_name = sanitize_file_name(name)
        if not clean_name:
            raise ValueError(f"file name {name!r} is not usable")
        if len(clean_name.encode("utf-8")) > 255:
            raise ValueError("file name is too long (more than 255 bytes)")
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("file data must be bytes")
        data = bytes(data)
        if len(data) < 1 or len(data) > MAX_FILE_BYTES:
            raise ValueError(
                f"file of {len(data)} bytes is outside the "
                f"1..{MAX_FILE_BYTES} byte limit"
            )
        try:
            peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        except NodeError as exc:
            # An unknown peer is a bad `to`, not a failed request.
            raise ValueError(str(exc)) from exc
        digest = hashlib.sha256(data).hexdigest()
        sent_at = time.time()
        with tempfile.TemporaryDirectory(prefix="tau-send-file-") as staging:
            staged = Path(staging) / clean_name
            staged.write_bytes(data)
            response = self.node.send_file(peer, staged, timeout=timeout)
        wire_id = response.get("id") if isinstance(response.get("id"), str) else None
        with self._send_lock:
            message = self._append_message(
                direction="out",
                peer=peer.hexhash,
                peer_name=peer.display_name,
                text=f"Sent file: {clean_name} ({len(data)} bytes)",
                sent_at=sent_at,
                received_at=time.time(),
                status="delivered",
                wire_id=wire_id,
                file={"name": clean_name, "size": len(data), "sha256": digest},
            )
        self.bus.publish({"type": "message", "message": message})
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

    def delete_message(self, target: int) -> Dict[str, Any]:
        """Delete one stored DM from the console history, by console id.

        Removes the entry from this console's ``messages.json`` log, and
        when the entry carries a wire id also deletes that id from the
        node inbox, so ``tau messages`` agrees with the console
        afterwards.  Only this node's copies are removed — the sender
        keeps theirs, and sealed copies already held by relays are
        unaffected.  A wire id the inbox no longer knows is not an
        error: the console entry is still removed.  Other tabs learn
        through a ``message_deleted`` event, and the unread badge is
        re-published because deleting an unread message clears it.

        Ordering keeps every failure retryable from the console: the
        inbox goes first, so an inbox failure leaves the bubble in
        place; the console log is then persisted strictly, and a
        failed write restores the entry in memory and reports an
        error instead of success.  A retry after either failure
        completes the delete (the inbox delete of an already-removed
        wire id is ignored), so no failure can leave a message the
        console reports as deleted but a restart brings back, nor a
        message deleted from the console but stuck in the inbox.
        """
        if not isinstance(target, int) or isinstance(target, bool) or target < 1:
            raise ValueError("message id is required")
        with self._lock:
            removed = next(
                (
                    message
                    for message in self._messages
                    if message.get("id") == target
                ),
                None,
            )
            if removed is None:
                raise ValueError(f"unknown message: {target!r}")
            if removed.get("status") == "queued":
                # Deleting would hide the bubble while the outbox keeps
                # delivering it, so cancellation has to go through there.
                raise ValueError(
                    f"message {target!r} is still queued; cancel it from the outbox first"
                )
            wire_id = removed.get("wire_id")
        inbox = None
        if isinstance(wire_id, str) and wire_id:
            inbox = getattr(self.node, "inbox", None)
            if inbox is not None:
                try:
                    inbox.delete(wire_id)
                except InboxError as exc:
                    # Already gone from the inbox (a stale wire id) is
                    # fine.  Any other inbox failure leaves the console
                    # entry in place, so the delete can be retried.
                    if not str(exc).startswith("unknown message"):
                        raise
        self._remove_history_entry(target, removed, publish_unread=inbox is not None)
        return {
            "ok": True,
            "deleted": {
                "id": target,
                "wire_id": wire_id if isinstance(wire_id, str) else None,
            },
        }

    def _remove_history_entry(
        self, target: int, removed: Dict[str, Any], *, publish_unread: bool
    ) -> None:
        """Remove one console history entry and tell connected tabs.

        The entry leaves the persisted log, its wire id is remembered
        so a delivery callback already in flight cannot append the
        message back afterwards (see :meth:`record_inbound`), and
        other tabs learn through a ``message_deleted`` event.  The
        caller owns the inbox side: console-initiated deletes remove
        the inbox copy first (see :meth:`delete_message`), while the
        retraction path never touches the inbox here — its record is
        already gone, and re-deleting by wire id could prefix-match a
        different message.  ``publish_unread`` re-publishes the unread
        badge when the caller consulted the node inbox.  The wire id
        stays remembered even when the save fails and the entry is
        restored: the message is back in history, so a later append
        of the same id would only duplicate it.
        """
        wire_id = removed.get("wire_id")
        # Hold the save lock across removal, strict persistence and
        # failure rollback: no other history save can persist the
        # transiently deleted deque between the failed write and the
        # in-memory restore. (Appends may still interleave, but their
        # saves wait on this lock and persist the restored log.)
        with self._save_lock:
            with self._lock:
                kept = [
                    message
                    for message in self._messages
                    if message.get("id") != target
                ]
                self._messages.clear()
                self._messages.extend(kept)
                if isinstance(wire_id, str) and wire_id:
                    # Recorded under the same lock as the append
                    # check, so the two cannot interleave.
                    self._remember_removed_wire_id(wire_id)
            # Persist the console history strictly: a failed write
            # restores the entry and reports an error, so success is
            # never reported for a delete that a restart would undo.
            try:
                self._save_messages_locked(strict=True)
            except Exception:
                with self._lock:
                    # A message can arrive while the strict write is in
                    # flight (the save lock serialises saves, not
                    # appends): with a full log the append refills the
                    # deque, and insert() on a full bounded deque raises
                    # instead of restoring the entry. Drop the oldest
                    # first — the same eviction an append would have
                    # caused — so the restore always lands.
                    if (
                        self._messages.maxlen is not None
                        and len(self._messages) >= self._messages.maxlen
                    ):
                        self._messages.popleft()
                    index = next(
                        (
                            i
                            for i, message in enumerate(self._messages)
                            if message.get("id", 0) > target
                        ),
                        len(self._messages),
                    )
                    self._messages.insert(index, removed)
                raise
        # The bus stamps its own sequence into ``id``, so the deleted
        # console id rides under ``message_id`` instead: using ``id``
        # would make other tabs drop whichever bubble shares the
        # event's sequence number.
        self.bus.publish({"type": "message_deleted", "message_id": target})
        if publish_unread:
            self.bus.publish({"type": "unread", "unread": self.unread()})

    def delete_by_wire_id(self, wire_id: str) -> Optional[Dict[str, Any]]:
        """Delete the console history entry for one retracted DM, by wire id.

        Called when the node accepts a retraction for a message this
        console already shows: the entry leaves the persisted history
        and connected tabs learn through the same ``message_deleted``
        event as a console delete, so a retracted message never
        survives in the console past a restart.  Only an inbound entry
        can match — a retract deletes a received record, never a sent
        one.  Console-only: the inbox record is already gone (the
        node's retract handler deleted it first), and re-deleting by
        wire id could prefix-match a different message.  Returns the
        delete result, or ``None`` when no inbound entry carries that
        wire id (the console window may not cover it) — the id is
        still remembered, so a delivery callback already in flight
        cannot append the message afterwards.
        """
        if not isinstance(wire_id, str) or not wire_id:
            return None
        with self._lock:
            removed = next(
                (
                    message
                    for message in self._messages
                    if message.get("direction") == "in"
                    and message.get("wire_id") == wire_id
                ),
                None,
            )
            if removed is None:
                # No entry to remove, but the retraction succeeded:
                # remember the id so a pending delivery callback
                # cannot append the message afterwards.
                self._remember_removed_wire_id(wire_id)
                return None
            target = removed.get("id")
        self._remove_history_entry(target, removed, publish_unread=True)
        return {
            "ok": True,
            "deleted": {"id": target, "wire_id": wire_id},
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

    def doctor(self) -> Dict[str, Any]:
        """Health verdicts for this node's data directory.

        The same checks as the ``tau doctor`` CLI, run against the
        running node's config.  Read-only: the checks never write
        anything or take the instance lock, so they are safe to run
        against the serving node.  The import is deferred because
        ``tau.doctor`` imports this module for its message-cleaning
        helpers.

        One check runs at a time: a second request while a scan is in
        progress gets a ``WebBridgeError`` (the route maps it to 503)
        instead of starting another full scan.  The lock is taken here,
        not in the route, so a HEAD request cannot bypass it.
        """
        from .doctor import all_ok, run_checks

        if not self._doctor_lock.acquire(blocking=False):
            raise WebBridgeError("a health check is already running")
        try:
            checks = run_checks(self.node.config)
        finally:
            self._doctor_lock.release()
        return {
            "ok": all_ok(checks),
            "checks": [check.to_dict() for check in checks],
        }

    def _was_delivered(self, mail_id: str) -> bool:
        """True if this bridge recorded a delivery for *mail_id*.

        The node's delivery callback flows through
        :meth:`record_delivery`, so a delivery by a concurrent mail
        round — one this retry did not run itself — still shows up
        here, either as a flipped console record or, for mail queued
        outside the console, in the early-delivery buffer.
        """
        with self._lock:
            if mail_id in self._early_deliveries:
                return True
            return any(
                message.get("direction") == "out"
                and message.get("mail_id") == mail_id
                and message.get("status") in ("delivered", "read")
                for message in self._messages
            )

    def retry_outbox(self, target: Optional[str] = None) -> Dict[str, Any]:
        """Try delivering queued mail now, ignoring backoff.

        The console counterpart of ``tau outbox retry``: with
        ``target`` (a message id or unique prefix) only that entry is
        attempted — nothing is deposited with the peer — otherwise the
        whole queue is retried.  Backoff is ignored either way: the
        operator asked for a retry *now*, not at the next scheduled
        attempt.  A targeted entry counts as delivered when this
        flush reports a delivery for it, or when the bridge already
        recorded one — a concurrent mail round can deliver the entry
        after resolution but before this flush visits it, and that
        delivery must not be reported as an expiry or a cancel.
        ``still_queued`` is read back from the queue afterwards.
        """
        to = None
        message_id = None
        if target is not None:
            if not isinstance(target, str) or not target.strip():
                raise ValueError("message id must not be empty")
            with self.node.mailbox.transaction():
                entry = self.node.mailbox.resolve_outbox(target.strip())
                to = entry.to
                message_id = entry.id
        summary = self.node.flush_outbox(
            respect_backoff=False,
            to=to,
            message_id=message_id,
            wait_for_lock=10.0,
        )
        with self.node.mailbox.transaction():
            still_queued = len(self.node.mailbox.outbox_list())
        payload: Dict[str, Any] = {
            "ok": True,
            "delivered": summary["delivered"],
            "deposited": summary["deposited"],
            "received": summary["received"],
            "still_queued": still_queued,
            "lock_busy": bool(summary["lock_busy"]),
        }
        if message_id is not None:
            payload["target"] = message_id
            payload["target_delivered"] = (
                summary["delivered"] > 0
                or self._was_delivered(message_id)
            )
            with self.node.mailbox.transaction():
                payload["target_still_queued"] = (
                    self.node.mailbox.get_outbox(message_id) is not None
                )
        return payload

    def held_mail(self) -> Dict[str, Any]:
        """Sealed mail held for other peers, with recipient names.

        Read under the mailbox transaction so envelopes deposited by a
        separate process are visible instead of the serving process's
        cached snapshot.  Only metadata is exposed: the sealed blobs
        stay unreadable through this API.
        """
        with self.node.mailbox.transaction():
            held = [
                self._describe_held_envelope(envelope)
                for envelope in self.node.mailbox.held_list()
            ]
        return {"held": held, "count": len(held)}

    def drop_held_mail(self, target: str) -> Dict[str, Any]:
        """Drop one held envelope so it is never delivered again.

        Resolve and remove happen inside one mailbox transaction, so a
        drop cannot miss an envelope another process just stored, and a
        fetch that lands in between is reported instead of silently
        claimed as dropped.  The tombstone stops a sender retry from
        resurrecting the envelope; a copy the recipient already fetched
        may still be delivered, and the result says so.
        """
        if not isinstance(target, str) or not target.strip():
            raise ValueError("message id is required")
        with self.node.mailbox.transaction():
            envelope = self.node.mailbox.resolve_held(target.strip())
            if not self.node.mailbox.drop_held(envelope.id, tombstone=True):
                # Fetched or expired between resolve and drop.
                raise MailboxError(
                    f"held message {target.strip()!r} is no longer present"
                )
            dropped = self._describe_held_envelope(envelope)
        dropped["note"] = (
            "a copy already fetched by the recipient may still be"
            " delivered; dropping stops this node handing it out again"
        )
        return dropped

    def _describe_held_envelope(self, envelope: Any) -> Dict[str, Any]:
        peer = self.node.registry.get(envelope.to)
        sender = (
            self.node.registry.get(bytes.fromhex(envelope.depositor))
            if envelope.depositor
            else None
        )
        return {
            "id": envelope.id,
            "to": envelope.to_hex,
            "to_name": peer.display_name if peer is not None else "",
            "from": envelope.depositor,
            "from_name": sender.display_name if sender is not None else "",
            "stored_at": envelope.stored_at,
            "expires_at": envelope.expires_at,
            "blob_bytes": len(envelope.blob),
        }

    def _describe_outbox_entry(self, entry: Any) -> Dict[str, Any]:
        payload = entry.to_dict()
        peer = self.node.registry.get(entry.to)
        payload["to_name"] = peer.display_name if peer is not None else entry.to_name
        payload["held_by"] = sorted(entry.held_by)
        payload["read_at"] = self.node.mailbox.read_at(entry.id)
        return payload

    def bundle_export(self, *, include_tan: bool = False) -> Dict[str, Any]:
        """Build a signed sneakernet bundle from this node's mail.

        Reuses :func:`tau.bundle.build_bundle`, so the console export
        is the same signed document ``tau bundle export`` writes: held
        envelopes verbatim, outbox entries sealed fresh to the
        recipient's key, and — when asked — recent Tan posts.  No
        identity or private material is included.
        """
        from .bundle import build_bundle

        identity = getattr(self.node, "identity", None)
        if identity is None:
            from .identity import load_identity

            identity = load_identity(self.node.config.identity_path)
        if identity is None:
            raise WebBridgeError("this node has no identity yet")
        tan_conn = None
        try:
            if include_tan:
                tan_conn = self._tan_conn()
            # Read under the mailbox transaction, as the outbox API
            # does: a separate process (e.g. a one-shot `tau send`)
            # may have queued mail since this process loaded its
            # snapshot, and the export must not silently omit it.
            with self.node.mailbox.transaction():
                document, summary = build_bundle(
                    identity=identity,
                    sender_name=self.node.name,
                    mailbox=self.node.mailbox,
                    registry=self.node.registry,
                    tan_conn=tan_conn,
                    include_tan=include_tan,
                )
        finally:
            if tan_conn is not None:
                tan_conn.close()
        return {"document": document, "summary": summary}

    def bundle_import(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Merge a verified sneakernet bundle document into this node.

        *document* must already have passed
        :func:`tau.bundle.parse_bundle` — the HTTP handler verifies the
        container signature and size cap on the request's original bytes
        before calling here — so this only runs
        :func:`tau.bundle.apply_bundle`: every envelope and Tan post
        meets the same checks as the network path.  Returns the merge
        report (imported / already present / skipped).
        """
        from .bundle import apply_bundle

        tan_conn = None
        try:
            if document.get("tan") is not None:
                tan_conn = self._tan_conn()
            admit = None
            if tan_conn is not None:
                admit = self._bundle_tan_admit(tan_conn)
            return apply_bundle(
                mailbox=self.node.mailbox,
                tan_conn=tan_conn,
                document=document,
                admit=admit,
            )
        finally:
            if tan_conn is not None:
                tan_conn.close()

    def _bundle_tan_admit(self, tan_conn: Any) -> Any:
        """Admission check for Tan posts carried in a bundle.

        The same ruleset the network path applies: the running Tan
        app's ruleset when the feed is enabled, else the ruleset file
        from the data directory.  A ruleset that cannot be parsed
        fails closed, denying remote posts, as ``TanApp`` does.
        """
        from .identity import destination_hash
        from .tangents.rules import RulesError, Ruleset, load_ruleset
        from .tangents.tan import following

        app = self._tan_app()
        if app is not None:
            ruleset = app.ruleset
            own_author = app.author
        else:
            try:
                ruleset = load_ruleset(
                    getattr(self.node.config, "tan_rules_path", None)
                )
            except RulesError:
                ruleset = Ruleset(default_allow=False)
            identity = getattr(self.node, "identity", None)
            own_author = (
                destination_hash(identity).hex() if identity is not None else None
            )
        followed: List[str] = []
        if ruleset.allow_followed:
            followed = [record["author"] for record in following(tan_conn)]
        return lambda author: ruleset.decide(
            author, own_author=own_author, followed=followed
        )

    def ping(self, target: str, *, timeout: float = 15.0) -> Dict[str, Any]:
        try:
            peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        except NodeError as exc:
            # The resolver reports unknown names as NodeError; treat them
            # as an unknown peer (404), not a failed request (502).
            raise PeerError(str(exc)) from exc
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

    def peer_info(self, target: str, *, timeout: float = 15.0) -> Dict[str, Any]:
        """Ask a peer what it reports about itself (``tau info``)."""
        try:
            peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        except NodeError as exc:
            # The resolver reports unknown names as NodeError; treat them
            # as an unknown peer (404), not a failed request (502).
            raise PeerError(str(exc)) from exc
        response = self.node.request_info(peer, timeout=timeout)
        return {
            "ok": True,
            "peer": peer.public_dict(),
            "info": _public_peer_info(response),
        }

    def forget_peer(self, target: str, *, force: bool = False) -> Dict[str, Any]:
        """Remove one peer from the serving node's registry.

        The CLI's ``tau peers forget`` refuses while a node is serving,
        so the console — which *is* the serving node — needs this bridge
        path to offer the same recovery for a corrupt peer record (a
        stored key ``upsert`` refuses to overwrite, so only forgetting
        lets the next genuine announce re-add a clean record). The
        mailbox transaction is held across the safety checks and the
        removal, mirroring ``cmd_peers_forget``: a one-shot send cannot
        queue mail for the peer between the check and the removal.
        Aliases are picked up first, so the save does not erase ones a
        one-shot command set while this node was serving — the same
        reason ``TauNode._save_peers`` refreshes before saving. The
        node's peer-save lock is held across the whole removal, before
        the mailbox transaction (the same order ``_save_peers`` uses):
        a concurrent alias, verification or node save could otherwise
        snapshot the peer first and write it back over our removal.
        A node-like without the lock (e.g. in tests) simply forgets.
        """
        if not isinstance(target, str) or not target.strip():
            raise ValueError("peer is required")
        lock = getattr(self.node, "_save_lock", None) or nullcontext()
        with lock, self.node.mailbox.transaction():
            self.node.registry.refresh_aliases()
            try:
                peer = self.node.registry.resolve(target.strip())
            except PeerError as exc:
                raise ValueError(str(exc)) from exc
            if peer is None:
                raise ValueError(f"no peer matches {target.strip()!r}")
            queued = self.node.mailbox.outbox_list(to=peer.destination_hash)
            if queued and not force:
                raise PeerForgetBlocked(
                    f"{peer.display_name} still has {len(queued)} queued "
                    "message(s); forgetting this peer would strand them "
                    "until it is rediscovered. Deliver or cancel the mail "
                    "first, or forget anyway.",
                    code="queued_mail",
                    queued=len(queued),
                )
            if peer.hexhash in self.node.mailbox.pending_ack_holders():
                raise ValueError(
                    f"{peer.display_name} still has mail acknowledgements "
                    "owed; wait for the next mail sweep to send them, "
                    "then try again"
                )
            removed = self.node.registry.forget(peer.destination_hash)
            try:
                self.node.registry.save()
            except Exception:
                # The old record is still on disk: put the live copy back
                # so the serving node and the file agree, then report it.
                self.node.registry.restore(removed)
                raise
        return {"ok": True, "peer": peer.public_dict()}

    def allowlist(self) -> Dict[str, Any]:
        """The DM allow-list: whether it is enforced, and who is on it."""
        return self._describe_allowlist(self.node.allowlist)

    def _describe_allowlist(self, current: Any) -> Dict[str, Any]:
        entries = []
        for destination in sorted(current.allow):
            peer = self.node.registry.get(destination)
            entries.append(
                {
                    "destination": destination,
                    "name": peer.display_name if peer is not None else "",
                }
            )
        return {"enabled": current.enabled, "entries": entries}

    def _resolve_allowlist_target(self, target: Any) -> str:
        """Resolve a console allow-list target to a destination hash.

        Mirrors the CLI's resolution: a registry name, hash or prefix
        first, else a raw 32-hex destination hash.
        """
        if not isinstance(target, str) or not target.strip():
            raise AllowListError("peer is required")
        value = target.strip()
        try:
            peer = self.node.registry.resolve(value, include_alias=False)
        except PeerError as exc:
            raise AllowListError(str(exc)) from exc
        if peer is not None:
            return peer.hexhash
        try:
            return clean_destination(value)
        except AllowListError:
            raise AllowListError(
                f"unknown peer or destination: {value!r}"
            ) from None

    def allowlist_add(self, target: Any) -> Dict[str, Any]:
        """Allow DMs from *target* and return the updated list."""
        destination = self._resolve_allowlist_target(target)
        updated = self.node.update_allowlist(
            lambda current: current.with_added(destination)
        )
        return self._describe_allowlist(updated)

    def allowlist_remove(self, target: Any) -> Dict[str, Any]:
        """Stop allowing DMs from *target* and return the updated list."""
        destination = self._resolve_allowlist_target(target)
        updated = self.node.update_allowlist(
            lambda current: current.with_removed(destination)
        )
        return self._describe_allowlist(updated)

    def allowlist_set_enabled(self, enabled: Any) -> Dict[str, Any]:
        """Turn allow-list enforcement on or off; return the updated list."""
        if not isinstance(enabled, bool):
            raise AllowListError("field 'enabled' must be a boolean")
        updated = self.node.update_allowlist(
            lambda current: current.with_enabled(enabled)
        )
        return self._describe_allowlist(updated)


    def announce(self) -> None:
        self.node.announce()
        self.bus.publish({"type": "status", "action": "announce"})

    def id_rename(self, name: str) -> Dict[str, Any]:
        """Rename this node, applying the new name live.

        The CLI refuses to rename while the node is serving because the
        running node announces with its in-memory name. The console
        runs inside that serving node, so it can update the live name
        instead: the node and its Tan app take the new name, the stored
        metadata is rewritten, and the node re-announces so peers learn
        the new name at once. The identity itself never changes.
        """
        if not isinstance(name, str):
            raise ValueError("field 'name' is required")
        new_name = sanitize_name(name, fallback="")
        if not new_name:
            raise ValueError("new name must not be empty")
        with self._rename_lock:
            old_name = getattr(self.node, "name", None) or "anonymous"
            if new_name == old_name:
                return {
                    "ok": True,
                    "name": new_name,
                    "previous_name": old_name,
                    "changed": False,
                }
            # Persist first: a failed write leaves the live name untouched.
            write_node_metadata(self.node.config.data_dir, new_name)
            self.node.name = new_name
            app = getattr(self.node, "app", None)
            if app is not None and hasattr(app, "name"):
                app.name = new_name
            try:
                self.node.announce()
            except Exception:
                # The rename already landed; the periodic announce loop
                # re-announces with the new name anyway. Any announce
                # failure is best-effort here, not a failed rename.
                log.debug("re-announce after rename failed", exc_info=True)
        self.bus.publish({"type": "status", "action": "rename"})
        return {
            "ok": True,
            "name": new_name,
            "previous_name": old_name,
            "changed": True,
        }

    def backup(self) -> Dict[str, Any]:
        """Write a backup archive of the live data directory.

        The bridge runs inside the serving node, so the archive is
        built with the live backup path: the instance lock is skipped
        (this process already holds it), each state file is read
        through a stable descriptor, because the node's own writes are
        atomic, and the mailbox is read under its transaction, so a
        delivery cannot land between the seen-marker and inbox writes
        (deliveries hold the same transaction across both steps).
        Returns the archive's temp path, download filename, manifest
        file list and size; the caller streams the file and removes
        it.  Raises :class:`WebBridgeError` when this node has no data
        directory to back up.
        """
        config = getattr(self.node, "config", None)
        if config is None or getattr(config, "data_dir", None) is None:
            raise WebBridgeError("this node has no data directory to back up")
        with self._backup_lock:
            if self._closing.is_set():
                raise WebBridgeError("the web console is shutting down")
            if self._backups_building + len(self._backup_artifacts) >= (
                MAX_CONCURRENT_BACKUPS
            ):
                raise WebBridgeError(
                    "too many backups in progress; try again shortly"
                )
            self._backups_building += 1
        try:
            result = self._build_backup(config)
        finally:
            with self._backup_lock:
                self._backups_building -= 1
                self._backup_idle.notify_all()
        target = Path(result["path"])
        with self._backup_lock:
            if self._closing.is_set():
                _unlink_quietly(target)
                raise WebBridgeError("the web console is shutting down")
            self._backup_artifacts.add(target)
        return result

    def _build_backup(self, config: Any) -> Dict[str, Any]:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        filename = f"tau-backup-{_backup_name(config.name)}-{stamp}.tar.gz"
        fd, tmp_name = tempfile.mkstemp(prefix="tau-backup-", suffix=".tar.gz")
        os.close(fd)
        target = Path(tmp_name)
        try:
            # create_backup refuses an existing target without --force;
            # the mkstemp file only reserved the name.
            target.unlink()
            # Hold the mailbox transaction while the archive is read.
            # Delivery marks a message seen (mailbox) and then records
            # it in the inbox, and holds this same transaction across
            # both steps — so a snapshot between those two writes would
            # restore a seen marker with no inbox entry, and the
            # message would then be rejected as already seen and lost.
            # Holding it here makes the backup see either both writes
            # or neither.  The mail sweep lock alone would not cover
            # this: direct deliveries never take it.
            mailbox = getattr(self.node, "mailbox", None)
            if mailbox is not None:
                with mailbox.transaction():
                    _, manifest = create_backup(config, target, live=True)
            else:
                _, manifest = create_backup(config, target, live=True)
        except BaseException:
            _unlink_quietly(target)
            raise
        return {
            "path": str(target),
            "filename": filename,
            "files": manifest.get("files", []),
            "size": target.stat().st_size,
        }

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

    def set_peer_alias(
        self, destination: str, alias: Optional[str]
    ) -> Dict[str, Any]:
        """Set a peer's local alias, or clear it when *alias* is blank.

        The alias rules stay in the registry — uniqueness, hex-like
        rejection, never sent to peers — this only routes the console's
        request through them.  A peer event goes out so every open
        console updates its peer list at once.
        """
        # Hold the node's peer-save lock across the edit and its save:
        # an announce can otherwise trigger _save_peers mid-request,
        # whose refresh_aliases() would copy the old disk alias back
        # over the new in-memory one before either save completes.
        # Mirrors peers_verify; a node-like without the lock (e.g. in
        # tests) simply edits.
        lock = getattr(self.node, "_save_lock", None) or nullcontext()
        with lock:
            if alias is not None and alias.strip():
                peer = self.node.registry.set_alias(destination, alias)
            else:
                peer = self.node.registry.clear_alias(destination)
        self.record_peer(peer)
        # The annotated payload, not bare local_dict: the hook replaces
        # its peer record with this response, and a bare dict would drop
        # the blocked badge until the next snapshot.
        return self._local_peer_payload(peer)

    def peers_safety_number(self, target: str) -> Dict[str, Any]:
        """Show a peer's pairwise safety number without recording anything.

        Showing the number must not mark the peer verified or clear a
        key-change warning: the owner still has to compare the number
        with the peer over another channel first, and only then confirm
        with :meth:`peers_verify`.
        """
        identity = getattr(self.node, "identity", None)
        if identity is None:
            raise WebBridgeError("no local identity; run `tau init` first")
        peer = self.node.registry.resolve(target)
        if peer is None:
            raise PeerError(f"no peer matches {target!r}")
        return {
            "peer": peer.hexhash,
            "name": peer.display_name,
            "safety_number": safety_number(
                identity.get_public_key(), peer.public_key
            ),
        }

    def peers_verify(self, target: str) -> Dict[str, Any]:
        """Mark a peer verified after comparing safety numbers out of band.

        Call only once the owner has compared the number shown by
        :meth:`peers_safety_number` with the peer over another channel
        and found it matches. Mirrors ``tau peers verify``: resolves
        *target* in the local registry, computes the pairwise safety
        number from this node's identity and the peer's public key,
        records the verification (clearing any key-change warning) and
        persists the registry. Verification itself stays a local
        judgement, never sent to peers.
        """
        identity = getattr(self.node, "identity", None)
        if identity is None:
            raise WebBridgeError("no local identity; run `tau init` first")
        peer = self.node.registry.resolve(target)
        if peer is None:
            raise PeerError(f"no peer matches {target!r}")
        number = safety_number(identity.get_public_key(), peer.public_key)
        previously_verified = peer.is_verified
        previous_fingerprint = peer.verified_fingerprint
        # Hold the node's peer-save lock across the mark and the write
        # when the node has one: a concurrent node save could otherwise
        # snapshot the unverified registry first and replace our file
        # after ours, silently dropping the verification.  A node-like
        # without the lock (e.g. in tests) simply marks and saves.
        lock = getattr(self.node, "_save_lock", None) or nullcontext()
        with lock:
            self.node.registry.mark_verified(peer.destination_hash, number)
            self.node.registry.save()
        return {
            "peer": peer.hexhash,
            "name": peer.display_name,
            "safety_number": number,
            "verified_at": peer.verified_at,
            "reverified": previously_verified,
            "fingerprint_changed": previously_verified
            and previous_fingerprint != number,
        }

    def _resolve_block_target(self, target: str) -> str:
        """Resolve a block/unblock target the way the CLI does.

        Registry name, destination hash or hash prefix first, then a
        bare destination hash; anything else is an unknown peer.
        """
        try:
            peer = self.node.registry.resolve(target)
        except PeerError as exc:
            raise ValueError(str(exc)) from exc
        if peer is not None:
            return peer.hexhash
        try:
            return clean_destination(target)
        except AllowListError:
            raise ValueError(f"unknown peer or destination: {target!r}") from None

    def _describe_blocked(self, destination: str) -> Dict[str, Any]:
        peer = self.node.registry.get(destination)
        return {
            "destination": destination,
            "name": peer.display_name if peer is not None else "",
        }

    def _edit_blocklist(self, target: str, *, block: bool) -> Dict[str, Any]:
        """Block or unblock a peer and persist the change.

        The running node's cached list is refreshed so the DM gate and
        the Tan display rules enforce the change at once, and a ``peer``
        event carries the new blocked flag to every console client.
        """
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target peer is required")
        destination = self._resolve_block_target(target.strip())
        config = getattr(self.node, "config", None)
        path = getattr(config, "blocklist_path", None)
        setter = getattr(self.node, "set_blocklist", None)
        # Hold the lock across load, modify, save, cache update and publish
        # so a concurrent edit cannot derive its change from the same stale list.
        with self._blocklist_lock:
            try:
                # Always re-read the file: the cached list may predate CLI
                # edits or a corruption that appeared since it was loaded.
                current = load_blocklist(path)
            except BlockListError as exc:
                if callable(setter):
                    # Fail closed in the running node until the owner
                    # repairs or removes the file.
                    setter(BlockList(block_all=True))
                raise WebBridgeError(
                    "block-list on disk is unusable; fix or remove it "
                    f"before editing ({exc})"
                ) from exc
            updated = (
                current.with_added(destination)
                if block
                else current.with_removed(destination)
            )
            changed = updated.blocked != current.blocked
            if changed:
                if path is None:
                    raise WebBridgeError("this node has no block-list path")
                save_blocklist(path, updated)
            if callable(setter):
                # Refresh the cache from the freshly loaded list, even on a
                # no-op, so a repaired file clears a previous fail-closed state.
                setter(updated)
            if changed:
                peer = self.node.registry.get(destination)
                if peer is not None:
                    # The cached list is refreshed, so the published event
                    # carries the new blocked flag at once.
                    self.record_peer(peer)
        return {
            "peer": self._describe_blocked(destination),
            "blocked": destination in updated.blocked,
            "changed": changed,
        }

    def block_peer(self, target: str) -> Dict[str, Any]:
        """Block a peer: drop their DMs and hide their Tan posts."""
        return self._edit_blocklist(target, block=True)

    def unblock_peer(self, target: str) -> Dict[str, Any]:
        """Unblock a peer."""
        return self._edit_blocklist(target, block=False)

    def groups(self) -> List[Dict[str, Any]]:
        try:
            return [
                self._describe_group(group)
                for group in self.node.groups.list_groups()
            ]
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not read group store", exc_info=True)
            return []

    def _describe_group(self, group: Dict[str, Any]) -> Dict[str, Any]:
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
        return {
            "id": group["id"],
            "name": group["name"],
            "members": members,
            "created_at": group["created_at"],
            "topic": group.get("topic", ""),
            # The console only offers member changes on groups this
            # node created; the store refuses anyone else.  A learned
            # group only claims its creator in the message, so it never
            # counts, even when it names this node.
            "created_by_me": bool(group.get("local"))
            and group.get("creator") == self.node.destination_hexhash,
        }

    def create_group(self, name: str, members: List[str]) -> Dict[str, Any]:
        """Create a group from the console, mirroring ``tau group create``.

        The creator is always a member, so this node's own hash is
        added when the caller did not include it.  Members are peer
        hashes from the console's picker; the store validates each one
        and the roster size.
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("field 'name' is required")
        if not isinstance(members, list) or not members:
            raise ValueError("field 'members' must be a non-empty list")
        cleaned: List[str] = []
        for member in members:
            if not isinstance(member, str) or not member.strip():
                raise ValueError("field 'members' must contain peer hashes")
            cleaned.append(member.strip().lower())
        own_hash = self.node.destination_hexhash
        for member in cleaned:
            if member == own_hash:
                continue
            # Only known peers can receive a group leg; an unknown hash
            # would leave a delivery that can never be sent.
            peer = self.node.registry.get(member)
            if peer is None or peer.destination_hash.hex() != member:
                raise ValueError(f"unknown peer: {member}")
        if own_hash not in cleaned:
            cleaned.append(own_hash)
        if len(cleaned) < 2:
            raise ValueError("a group needs at least one other peer")
        group = self.node.groups.create(
            name.strip(), cleaned, created_by=own_hash
        )
        return self._describe_group(group)

    def delete_group(self, query: str) -> Dict[str, Any]:
        """Delete a known group from the console, mirroring ``tau group delete``."""
        if not isinstance(query, str) or not query.strip():
            raise ValueError("field 'group' is required")
        group = self.node.groups.find(query.strip())
        if not self.node.groups.delete(group["id"]):
            raise ValueError(f"unknown group: {query.strip()!r}")
        return {"id": group["id"], "name": group["name"]}

    def change_group_members(
        self, query: str, members: List[str], *, adding: bool
    ) -> Dict[str, Any]:
        """Add peers to, or remove peers from, a group from the console.

        Mirrors ``tau group add-member`` / ``tau group remove-member``:
        only the group's creator can change its members, adding refuses
        an unknown peer before the roster changes, and removing only
        needs the hash already in the roster, so an offline member can
        still be dropped.  The id derives from the roster, so the change
        is a successor group (same name, new id); the old id is reported
        as ``previous_id``.
        """
        if not isinstance(query, str) or not query.strip():
            raise ValueError("field 'group' is required")
        if (
            not isinstance(members, list)
            or not members
            or any(
                not isinstance(member, str) or not member.strip()
                for member in members
            )
        ):
            raise ValueError("field 'members' must be a non-empty list")
        group = self.node.groups.find(query.strip())
        own_hash = self.node.destination_hexhash
        if not group["local"] or group["creator"] != own_hash:
            raise ValueError("only the creator can change a group's members")
        roster = list(group["members"])
        for target in members:
            cleaned = target.strip().lower()
            if adding:
                # The creator is always a member, so this is refused
                # before the registry lookup: the node's own hash is
                # not in the registry.
                if cleaned == own_hash:
                    raise ValueError("the creator is already a member")
                try:
                    peer = self.node.registry.resolve(cleaned)
                except PeerError as exc:
                    raise ValueError(str(exc)) from exc
                if peer is None:
                    raise ValueError(f"unknown peer: {cleaned}")
                resolved = peer.destination_hash.hex()
                if resolved in roster:
                    raise ValueError(
                        f"{peer.display_name} is already a member of"
                        f" {group['name']!r}"
                    )
                roster.append(resolved)
            else:
                # A roster hash drops the member without a registry
                # lookup, so an offline member can still be removed.
                # Anything else resolves through the registry first,
                # so the picker can pass a name as well as a hash.
                if len(cleaned) == 32 and cleaned in roster:
                    resolved = cleaned
                else:
                    try:
                        peer = self.node.registry.resolve(cleaned)
                    except PeerError as exc:
                        raise ValueError(str(exc)) from exc
                    if peer is None:
                        raise ValueError(
                            f"{target.strip()} is not a member of"
                            f" {group['name']!r}"
                        )
                    resolved = peer.destination_hash.hex()
                    if resolved not in roster:
                        raise ValueError(
                            f"{peer.display_name} is not a member of"
                            f" {group['name']!r}"
                        )
                if resolved == own_hash:
                    raise ValueError("cannot remove the creator from the group")
                roster.remove(resolved)
        new_group = self.node.groups.update_members(
            group["id"], roster, created_by=own_hash
        )
        return {
            "group": self._describe_group(new_group),
            "previous_id": group["id"],
        }

    def messages(self, limit: int = 200) -> List[Dict[str, Any]]:
        # Drop disappearing messages whose deadline has passed while the
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

    def received_file(
        self, sender: str, name: str
    ) -> Optional[Tuple[bytes, str]]:
        """Return the bytes and name of a received file, or ``None``.

        The file must be one the node inbox actually records for
        *sender* under *name*: the stored record's size and SHA-256
        are re-checked against the bytes on disk, and the recorded
        path must resolve inside this sender's directory under the
        node's files directory — a record pointing anywhere else (a
        hand-edited inbox, a planted symlink) serves nothing.

        Raises ``ValueError`` when the sender or name is not usable
        at all (a malformed hash, a name that is not already a safe
        bare file name); anything else that does not check out is
        simply not found.
        """
        try:
            sender = clean_destination(sender)
        except AllowListError as exc:
            raise ValueError("sender must be a destination hash") from exc
        if not isinstance(name, str) or sanitize_file_name(name) != name:
            raise ValueError("file name is not usable")
        # Blocked senders' files stay hidden, matching messages(); the
        # blocklist's is_blocked is fail-closed, so an unreadable list
        # hides the file too.
        blocklist = getattr(self.node, "blocklist", None)
        if blocklist is not None and blocklist.is_blocked(sender):
            return None
        inbox = getattr(self.node, "inbox", None)
        if inbox is None:
            return None
        record = None
        for entry in inbox.list_messages(peer=sender, direction="in"):
            if entry.get("kind") == "file" and entry.get("name") == name:
                record = entry
                break
        if record is None:
            return None
        files_dir = getattr(getattr(self.node, "config", None), "files_dir", None)
        if files_dir is None:
            return None
        path = record.get("path")
        if not isinstance(path, str) or not path:
            return None
        try:
            resolved = Path(path).resolve()
            base = Path(files_dir).resolve()
        except (OSError, RuntimeError, ValueError):
            # RuntimeError: symlink loops on supported Python versions.
            return None
        if resolved.parent != base / sender or resolved.name != name:
            return None
        if not resolved.is_file():
            return None
        size = record.get("size")
        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or size < 1
            or size > MAX_FILE_BYTES
        ):
            return None
        sha256 = record.get("sha256")
        if not isinstance(sha256, str):
            return None
        try:
            # Read at most one byte past the recorded size so a file
            # replaced by something much larger is never loaded whole.
            with resolved.open("rb") as handle:
                data = handle.read(size + 1)
        except OSError:
            return None
        if len(data) != size:
            return None
        if hashlib.sha256(data).hexdigest() != sha256.lower():
            return None
        return data, name

    def messages_search(
        self,
        *,
        query: str,
        peer: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return stored messages matching a keyword, newest first.

        A case-insensitive substring match over the message text, over
        the same stored history the traffic view shows: :meth:`messages`
        already purges expired disappearing messages, hides blocked
        senders and annotates reactions, so search reuses it instead of
        re-reading the log.  An optional *peer* narrows to one peer by
        destination prefix or peer-name substring.  An empty query
        raises :class:`RequestError`, which the HTTP layer reports as a
        400; anything unexpected stays an exception and becomes a 500.
        """
        if not isinstance(query, str) or not query.strip():
            raise RequestError("search text must not be empty")
        needle = query.strip().casefold()
        items = self.messages(limit=-1)
        if peer is not None and peer.strip():
            target = peer.strip().casefold()
            items = [
                item
                for item in items
                if (item.get("peer") or "").casefold().startswith(target)
                or target in (item.get("peer_name") or "").casefold()
            ]
        matches = [
            item for item in items if needle in (item.get("text") or "").casefold()
        ]
        return matches[::-1][:limit] if limit > 0 else []

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
                "posts": self.tan_post_count(),
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
        file: Optional[Dict[str, Any]] = None,
        save: bool = True,
        reject_removed_wire_id: bool = False,
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            if (
                reject_removed_wire_id
                and wire_id is not None
                and wire_id in self._removed_wire_ids
            ):
                # The entry was removed (by retraction or a console
                # delete) while this delivery callback was in flight;
                # appending now would resurrect it.  The check runs
                # under the same lock as the removal, so the two cannot
                # interleave.
                return None
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
            cleaned_file = _clean_stored_file(file) if file is not None else None
            if cleaned_file is not None:
                message["file"] = cleaned_file
            self._next_message_id += 1
            self._messages.append(message)
        # A bulk caller (import mirroring) passes save=False and saves
        # once after the loop instead of once per entry.
        if save:
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


def _backup_name(name: Any) -> str:
    """Node name made safe for a download filename.

    Keeps letters, digits and ``-_.``; anything else becomes ``-``.
    A name with nothing keepable falls back to ``node``.
    """
    cleaned = "".join(
        char if char.isascii() and (char.isalnum() or char in "-_.") else "-"
        for char in str(name or "")
    ).strip(".-")
    return cleaned[:64] or "node"


def _as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return default


def file_arrival_text(record: Dict[str, Any]) -> str:
    """Feed line for a received file: name, size and short SHA-256.

    Shared by live file delivery and backup restore so both show the
    same entry for a file.
    """
    return (
        f"Received file: {record.get('name')} "
        f"({record.get('size')} bytes, "
        f"sha256 {short_hash(str(record.get('sha256') or ''), 12)})"
    )


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


def _clamped_timeout(value: Any, default: float) -> float:
    """Clamp a request ``timeout`` to [1.0, 120.0] seconds.

    Non-numeric, boolean, and non-finite values fall back to ``default``.
    Huge JSON integers overflow ``float()``, so the conversion is guarded
    here instead of raising past the handler.
    """
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return default
    try:
        seconds = float(value)
    except OverflowError:
        # An integer too large for a float is far above the cap — in
        # either direction, so the overflow path follows the same
        # bounds as the normal clamp.
        return 120.0 if value > 0 else 1.0
    if not math.isfinite(seconds):
        return default
    return max(1.0, min(seconds, 120.0))


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
            if path == "/api/messages/export":
                # The full inbox backup goes only to the local machine,
                # even when the bridge is bound to a remote interface.
                if not _is_loopback_client(self.client_address[0]):
                    self._send_json(
                        {
                            "error": {
                                "message": "messages export is only available from localhost"
                            }
                        },
                        head_only,
                        403,
                    )
                    return
                try:
                    document = self.bridge.messages_export()
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except ValueError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json(document, head_only)
                return
            if path.startswith("/api/files/"):
                parts = [
                    unquote(part) for part in path[len("/api/files/") :].split("/")
                ]
                if len(parts) != 2 or not parts[0] or not parts[1]:
                    self._send_json(
                        {"error": {"message": "not found"}}, head_only, 404
                    )
                    return
                self._handle_download_file(parts[0], parts[1], head_only)
                return
            if path == "/api/messages/search":
                limit = _int_param(query, "limit", default=100, minimum=1, maximum=200)
                text = (query.get("q") or [""])[0]
                peer = (query.get("peer") or [None])[0]
                try:
                    found = self.bridge.messages_search(
                        query=text, peer=peer, limit=limit
                    )
                except RequestError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"messages": found, "count": len(found)}, head_only)
                return
            if path == "/api/outbox":
                self._send_json(self.bridge.outbox(), head_only)
                return
            if path == "/api/settings":
                try:
                    self._send_json(self.bridge.settings(), head_only)
                except WebBridgeError as exc:
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 503
                    )
                return
            if path == "/api/backup":
                try:
                    result = self.bridge.backup()
                except WebBridgeError as exc:
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 503
                    )
                    return
                except BackupError as exc:
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 500
                    )
                    return
                archive = Path(result["path"])
                try:
                    self._send_file(archive, result["filename"], head_only)
                finally:
                    # The archive holds the node identity: never leave
                    # the temp file behind after the download.
                    self.bridge.finish_backup(archive)
            if path == "/api/doctor":
                try:
                    self._send_json(self.bridge.doctor(), head_only)
                except WebBridgeError as exc:
                    # A check already in progress: 503, like the other
                    # bridge errors, so the caller retries instead of
                    # queueing another full scan.
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 503
                    )
                return
            if path == "/api/outbox/held":
                self._send_json(self.bridge.held_mail(), head_only)
                return
            if path == "/api/allowlist":
                self._send_json(self.bridge.allowlist(), head_only)
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
                from .tangents.tan import TanError

                try:
                    posts = self.bridge.tan_top(limit=limit, author=author)
                except TanError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
                return
            if path == "/api/tan/search":
                limit = _int_param(query, "limit", default=30, minimum=1, maximum=200)
                text = (query.get("q") or [""])[0]
                author = (query.get("author") or [None])[0]
                try:
                    posts = self.bridge.tan_search(
                        query=text, author=author, limit=limit
                    )
                except RequestError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
                return
            if path == "/api/tan/mentions":
                limit = _int_param(query, "limit", default=30, minimum=1, maximum=200)
                try:
                    posts = self.bridge.tan_mentions(limit=limit)
                except RequestError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
                return
            if path == "/api/tan/thread":
                ref = (query.get("ref") or [None])[0]
                if not ref or not ref.strip():
                    self._send_json(
                        {"error": {"message": "field 'ref' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    posts = self.bridge.tan_thread(ref.strip())
                except ValueError as exc:
                    # Only an invalid reference is a client error; an
                    # unexpected failure (a database error, say) must
                    # reach the server-error handler as a 500, so the
                    # console does not mistake it for a deleted anchor
                    # and close the thread.
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
            if path == "/api/tan/authors":
                sort = (query.get("sort") or ["recent"])[0]
                limit = _int_param(query, "limit", default=50, minimum=1, maximum=200)
                offset = _int_param(
                    query, "offset", default=0, minimum=0, maximum=100000
                )
                try:
                    result = self.bridge.tan_authors(
                        sort=sort, limit=limit, offset=offset
                    )
                except ValueError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json(
                    {
                        "authors": result["authors"],
                        "count": len(result["authors"]),
                        "total": result["total"],
                    },
                    head_only,
                )
                return
            if path == "/api/tan/suggest":
                limit = _int_param(query, "limit", default=5, minimum=1, maximum=50)
                suggestions = self.bridge.tan_suggest(limit=limit)
                self._send_json(
                    {"suggestions": suggestions, "count": len(suggestions)}, head_only
                )
                return
            if path == "/api/tan/export":
                scope = (query.get("scope") or ["mine"])[0]
                if scope not in ("mine", "all"):
                    self._send_json(
                        {"error": {"message": "query 'scope' must be 'mine' or 'all'"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    document = self.bridge.tan_export(all_authors=(scope == "all"))
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except ValueError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json(document, head_only)
                return
            if path == "/api/tan/likes":
                from .tangents.tan import TanError

                target = (query.get("target") or [None])[0]
                try:
                    payload = self.bridge.tan_likers(target)
                except (TanError, ValueError) as exc:
                    # Only target validation is a client error; a
                    # database or storage failure falls through to
                    # _handle, which logs the traceback and answers 500.
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json(payload, head_only)
                return
            if path == "/api/tan/muted":
                records = self.bridge.tan_muted()
                self._send_json(
                    {"muted": records, "count": len(records)}, head_only
                )
                return
            if path == "/api/tan/muted-keywords":
                records = self.bridge.tan_muted_keywords()
                self._send_json(
                    {"muted_keywords": records, "count": len(records)},
                    head_only,
                )
                return

            if path == "/api/tan/saved":
                limit = _int_param(query, "limit", default=30, minimum=1, maximum=200)
                offset = _int_param(
                    query, "offset", default=0, minimum=0, maximum=100000
                )
                try:
                    result = self.bridge.tan_saved(limit=limit, offset=offset)
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
                self._send_json(
                    {
                        "posts": result["posts"],
                        "count": len(result["posts"]),
                        "next_offset": result["next_offset"],
                    },
                    head_only,
                )
                return
            if path == "/api/tan/posts":
                limit = _int_param(query, "limit", default=100, minimum=1, maximum=500)
                posts = self.bridge.tan_posts(limit=limit)
                self._send_json({"posts": posts, "count": len(posts)}, head_only)
                return
            if path == "/api/tan/rules":
                from .tangents.tan import ENTRY_KINDS

                try:
                    rules = self.bridge.tan_rules()
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                self._send_json(
                    {"rules": rules, "available_kinds": list(ENTRY_KINDS)},
                    head_only,
                )
            if path == "/api/tan/verify":
                try:
                    report = self.bridge.tan_verify()
                except FileNotFoundError as exc:
                    # Expected state (no store yet). Anything else
                    # propagates to _handle, which logs the traceback
                    # and answers 500 instead of dressing an audit
                    # failure up as a client mistake.
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json(report, head_only)
                return
            self._send_json({"error": {"message": "not found"}}, head_only, 404)
            return

        if self.command == "POST":
            # Bulk imports write many entries with no per-request quota, so
            # they are accepted only from the local machine, even when the
            # bridge is bound to a remote interface.
            if path == "/api/messages/import" and not _is_loopback_client(
                self.client_address[0]
            ):
                self._send_json(
                    {
                        "error": {
                            "message": "messages import is only accepted from localhost"
                        }
                    },
                    head_only,
                    403,
                )
                return
            if path == "/api/tan/import" and not _is_loopback_client(
                self.client_address[0]
            ):
                self._send_json(
                    {"error": {"message": "tan import is only accepted from localhost"}},
                    head_only,
                    403,
                )
                return
            if path == "/api/bundle/import":
                if not _is_loopback_client(self.client_address[0]):
                    self._send_json(
                        {"error": {"message": "bundle import is only accepted from localhost"}},
                        head_only,
                        403,
                    )
                    return
                self._handle_bundle_import(head_only)
                return
            # A file send carries base64 bytes and an import carries a whole
            # message history or a whole signed chain, so each gets a larger
            # body cap than a chat message or a single action.
            if path == "/api/send-file":
                limit = MAX_FILE_BODY_BYTES
            elif path in ("/api/messages/import", "/api/tan/import"):
                limit = MAX_IMPORT_BYTES
            else:
                limit = MAX_BODY_BYTES
            body = self._read_body(max_bytes=limit)
            if path == "/api/messages":
                self._handle_send(body, head_only)
                return
            if path == "/api/send-file":
                self._handle_send_file(body, head_only)
                return
            if path == "/api/react":
                self._handle_react(body, head_only)
                return
            if path == "/api/messages/delete":
                self._handle_delete_message(body, head_only)
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
            if path == "/api/messages/import":
                try:
                    result = self.bridge.messages_import(body)
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except ValueError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "import": result}, head_only)
                return
            if path == "/api/outbox/cancel":
                self._handle_outbox_cancel(body, head_only)
                return
            if path == "/api/settings":
                self._handle_settings(body, head_only)
                return
            if path == "/api/groups":
                try:
                    group = self.bridge.create_group(
                        body.get("name"), body.get("members")
                    )
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                except OSError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                self._send_json({"ok": True, "group": group}, head_only)
                return
            if path == "/api/groups/delete":
                target = body.get("group")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'group' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    deleted = self.bridge.delete_group(target.strip())
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                except OSError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                self._send_json({"ok": True, "deleted": deleted}, head_only)
                return
            if path in (
                "/api/groups/add-member",
                "/api/groups/remove-member",
            ):
                target = body.get("group")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'group' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    changed = self.bridge.change_group_members(
                        target.strip(),
                        body.get("members"),
                        adding=(path == "/api/groups/add-member"),
                    )
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                except OSError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                self._send_json({"ok": True, **changed}, head_only)
                return
            if path == "/api/outbox/retry":
                self._handle_outbox_retry(body, head_only)
                return
            if path == "/api/outbox/drop-held":
                self._handle_outbox_drop_held(body, head_only)
                return
            if path == "/api/ping":
                self._handle_ping(body, head_only)
                return
            if path == "/api/info":
                self._handle_info(body, head_only)
                return
            if path == "/api/peers/forget":
                target = body.get("target")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    result = self.bridge.forget_peer(
                        target.strip(), force=body.get("force") is True
                    )
                except PeerForgetBlocked as exc:
                    self._send_json(
                        {
                            "error": {
                                "message": str(exc),
                                "code": exc.code,
                                "queued": exc.queued,
                            }
                        },
                        head_only,
                        400,
                    )
                    return
                except (ValueError, PeerError) as exc:
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 400
                    )
                    return
                self._send_json(result, head_only)
                return
            if path in (
                "/api/allowlist/add",
                "/api/allowlist/remove",
                "/api/allowlist/set-enabled",
            ):
                self._handle_allowlist(body, head_only, path.rsplit("/", 1)[1])
                return
            if path == "/api/peers/safety-number":
                target = body.get("target")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    result = self.bridge.peers_safety_number(target.strip())
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except PeerError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "safety_number": result}, head_only)
                return
            if path == "/api/peers/verify":
                target = body.get("target")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    result = self.bridge.peers_verify(target.strip())
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except PeerError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "verify": result}, head_only)
                return
            if path in ("/api/peers/block", "/api/peers/unblock"):
                target = body.get("target")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    if path.endswith("unblock"):
                        result = self.bridge.unblock_peer(target.strip())
                    else:
                        result = self.bridge.block_peer(target.strip())
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "block": result}, head_only)
                return
            if path == "/api/announce":
                self.bridge.announce()
                self._send_json({"ok": True}, head_only)
                return
            if path == "/api/id/rename":
                name = body.get("name")
                if not isinstance(name, str) or not name.strip():
                    self._send_json(
                        {"error": {"message": "field 'name' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    result = self.bridge.id_rename(name)
                except ValueError as exc:
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 400
                    )
                    return
                except NodeError as exc:
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 502
                    )
                    return
                self._send_json(result, head_only)
                return
            if path == "/api/peers/alias":
                destination = body.get("destination") or body.get("target")
                if not isinstance(destination, str) or not destination.strip():
                    self._send_json(
                        {"error": {"message": "field 'destination' is required"}},
                        head_only,
                        400,
                    )
                    return
                alias = body.get("alias")
                if alias is not None and not isinstance(alias, str):
                    self._send_json(
                        {"error": {"message": "field 'alias' must be a string"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    peer = self.bridge.set_peer_alias(
                        destination.strip(), alias
                    )
                except PeerError as exc:
                    self._send_json(
                        {"error": {"message": str(exc)}}, head_only, 400
                    )
                    return
                self._send_json({"ok": True, "peer": peer}, head_only)
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
            if path == "/api/tan/quote":
                target = body.get("target") or body.get("hash")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                text = body.get("text")
                if not isinstance(text, str) or not text.strip():
                    self._send_json(
                        {"error": {"message": "field 'text' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    quote = self.bridge.tan_quote(target.strip(), text)
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "post": quote}, head_only)
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
            if path in ("/api/tan/mute", "/api/tan/unmute"):
                target = body.get("target") or body.get("author")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                from .tangents.tan import TanError

                try:
                    if path.endswith("unmute"):
                        result = self.bridge.tan_unmute(target.strip())
                    else:
                        result = self.bridge.tan_mute(target.strip())
                except TanError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "mute": result}, head_only)
                return
            if path in ("/api/tan/mute-keyword", "/api/tan/unmute-keyword"):
                keyword = body.get("keyword")
                if not isinstance(keyword, str) or not keyword.strip():
                    self._send_json(
                        {"error": {"message": "field 'keyword' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    if path.endswith("unmute-keyword"):
                        result = self.bridge.tan_unmute_keyword(keyword)
                    else:
                        result = self.bridge.tan_mute_keyword(keyword)
                except Exception as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "mute": result}, head_only)
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
            if path == "/api/tan/repost":
                target = body.get("target") or body.get("hash")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    record = self.bridge.tan_repost(target.strip())
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "repost": record}, head_only)
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
            if path == "/api/bundle/export":
                self._handle_bundle_export(body, head_only)
                return
            if path == "/api/tan/edit":
                target = body.get("target") or body.get("hash")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                text = body.get("text")
                if not isinstance(text, str) or not text.strip():
                    self._send_json(
                        {"error": {"message": "field 'text' is required"}},
                        head_only,
                        400,
                    )
                    return
                try:
                    record = self.bridge.tan_edit(target.strip(), text)
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "edit": record}, head_only)
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
            if path == "/api/tan/rules":
                try:
                    rules = self.bridge.update_tan_rules(body)
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except (ValueError, ProtocolError) as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "rules": rules}, head_only)
            if path == "/api/tan/import":
                try:
                    result = self.bridge.tan_import(body)
                except WebBridgeError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 503)
                    return
                except ValueError as exc:
                    self._send_json({"error": {"message": str(exc)}}, head_only, 400)
                    return
                self._send_json({"ok": True, "import": result}, head_only)
                return
            self._send_json({"error": {"message": "not found"}}, head_only, 404)
            return

        self._send_json({"error": {"message": "method not allowed"}}, head_only, 405)

    def _handle_download_file(
        self, sender: str, name: str, head_only: bool
    ) -> None:
        """Serve one received file's bytes as an attachment.

        The bridge resolves the file against the node inbox, so only
        a file this node actually received from *sender* under *name*
        is ever served; anything else is a 404, and a malformed
        sender or name is a 400.
        """
        try:
            found = self.bridge.received_file(sender, name)
        except ValueError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        if found is None:
            self._send_json(
                {"error": {"message": "no such received file"}}, head_only, 404
            )
            return
        data, file_name = found
        # The name is already sanitised, but the header value must
        # still be ASCII-safe: quote anything outside it, and carry
        # the exact name in the RFC 5987 form for non-ASCII names.
        ascii_name = "".join(
            char if 32 <= ord(char) < 127 and char not in '"\\' else "_"
            for char in file_name
        )
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header(
            "Content-Disposition",
            f'attachment; filename="{ascii_name}"; '
            f"filename*=UTF-8''{quote(file_name)}",
        )
        self._cors_headers()
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if not head_only:
            self.wfile.write(data)

    def _handle_send(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        group = body.get("group")
        text = body.get("text")
        timeout = body.get("timeout", 20.0)
        expires_in = body.get("expires_in")
        if group is not None:
            self._handle_group_send(body, group, text, timeout, expires_in, head_only)
            return
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
        timeout = _clamped_timeout(timeout, 20.0)
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

    def _handle_group_send(
        self,
        body: Dict[str, Any],
        group: Any,
        text: Any,
        timeout: Any,
        expires_in: Any,
        head_only: bool,
    ) -> None:
        # A group send is addressed by group, not by peer, so the two
        # forms are exclusive; the node group path supports no reply
        # quoting, so that is rejected outright instead of silently
        # dropped.
        if body.get("to") is not None:
            self._send_json(
                {"error": {"message": "give 'to' or 'group', not both"}},
                head_only,
                400,
            )
            return
        if not isinstance(group, str) or not group.strip():
            self._send_json(
                {
                    "error": {
                        "message": "field 'group' must be a group id, "
                        "id prefix or name"
                    }
                },
                head_only,
                400,
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
        if body.get("reply_to") is not None:
            self._send_json(
                {"error": {"message": "group sends do not support 'reply_to'"}},
                head_only,
                400,
            )
            return
        try:
            messages, response = self.bridge.send_group_message(
                group.strip(), text, timeout=timeout, expires_at=expires_at
            )
        except ValueError as exc:
            # GroupStore.find raises ValueError for both an unknown
            # group and an ambiguous query; only the unknown case is
            # a 404, the ambiguous one stays a 400.
            status = 404 if str(exc).startswith("unknown group:") else 400
            self._send_json({"error": {"message": str(exc)}}, head_only, status)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(
            {
                "ok": bool(response.get("ok", True)),
                "messages": messages,
                "response": response,
            },
            head_only,
        )


    def _handle_send_file(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        name = body.get("name")
        data = body.get("data")
        timeout = body.get("timeout", 300.0)
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'to' is required"}}, head_only, 400
            )
            return
        if not isinstance(name, str) or not name.strip():
            self._send_json(
                {"error": {"message": "field 'name' is required"}}, head_only, 400
            )
            return
        if not isinstance(data, str):
            self._send_json(
                {"error": {"message": "field 'data' must be base64"}},
                head_only,
                400,
            )
            return
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = 300.0
        # Clamp before float(): huge ints would overflow float() otherwise.
        timeout = float(max(1, min(timeout, 300)))
        try:
            raw = base64.b64decode(data, validate=True)
        except (ValueError, base64.binascii.Error):
            self._send_json(
                {"error": {"message": "field 'data' must be valid base64"}},
                head_only,
                400,
            )
            return
        try:
            message, response = self.bridge.send_file(
                target,
                name,
                raw,
                timeout=timeout,
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
        timeout = _clamped_timeout(timeout, 20.0)
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

    def _handle_bundle_export(self, body: Dict[str, Any], head_only: bool) -> None:
        include_tan = body.get("include_tan", False)
        if not isinstance(include_tan, bool):
            self._send_json(
                {"error": {"message": "field 'include_tan' must be a boolean"}},
                head_only,
                400,
            )
            return
        try:
            result = self.bridge.bundle_export(include_tan=include_tan)
        except BundleError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except WebBridgeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 503)
            return
        from .bundle import serialize_bundle

        # Return the server's serialised bundle text, not a re-encoded
        # object: the bundle is signed, and a client that parses and
        # re-stringifies it can rewrite a number the signature covers
        # (an integral float such as 1700000000.0 becomes 1700000000)
        # and break verification of the file it downloads.
        self._send_json(
            {
                "ok": True,
                "bundle": serialize_bundle(result["document"]).decode("utf-8"),
                "summary": result["summary"],
            },
            head_only,
        )

    def _handle_bundle_import(self, head_only: bool) -> None:
        # A bundle import carries the whole signed document, up to the
        # 8 MiB bundle cap — far past the 32 KiB limit every other
        # route uses. Verify the submitted bytes themselves: measuring
        # a pretty-printed reconstruction instead would reject a
        # compact, correctly signed document near the cap, and parsing
        # through JavaScript numbers on the way in can rewrite a float
        # the signature covers.
        raw = self._read_raw_body(max_bytes=DEFAULT_MAX_BUNDLE_BYTES)
        from .bundle import parse_bundle

        try:
            document = parse_bundle(raw)
        except BundleError as exc:
            self._send_json(
                {"error": {"message": f"invalid bundle: {exc}"}}, head_only, 400
            )
            return
        try:
            result = self.bridge.bundle_import(document)
        except WebBridgeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 503)
            return
        self._send_json({"ok": True, "import": result}, head_only)

    def _handle_delete_message(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("id")
        if target is None:
            target = body.get("target")
        if not isinstance(target, int) or isinstance(target, bool) or target < 1:
            self._send_json(
                {"error": {"message": "field 'id' must be a message id"}},
                head_only,
                400,
            )
            return
        try:
            result = self.bridge.delete_message(target)
        except (ValueError, ProtocolError) as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
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

    def _handle_settings(self, body: Dict[str, Any], head_only: bool) -> None:
        setting = body.get("setting")
        if not isinstance(setting, str) or not setting.strip():
            self._send_json(
                {"error": {"message": "field 'setting' is required"}},
                head_only,
                400,
            )
            return
        if "value" not in body:
            self._send_json(
                {"error": {"message": "field 'value' is required"}},
                head_only,
                400,
            )
            return
        try:
            settings = self.bridge.update_setting(setting.strip(), body["value"])
        except ValueError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except WebBridgeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 503)
            return
        self._send_json({"ok": True, "settings": settings}, head_only)

    def _handle_outbox_retry(self, body: Dict[str, Any], head_only: bool) -> None:
        # An explicitly supplied target (even blank) must be valid; only
        # an absent one means "retry the whole queue".
        supplied = "target" in body or "id" in body
        target = body["target"] if "target" in body else body.get("id")
        if supplied and (not isinstance(target, str) or not target.strip()):
            self._send_json(
                {"error": {"message": "field 'target' must be a message id"}},
                head_only,
                400,
            )
            return
        try:
            result = self.bridge.retry_outbox(
                target.strip() if supplied else None
            )
        except ValueError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except MailboxError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        if result["lock_busy"]:
            self._send_json(
                {
                    "error": {
                        "message": "a mail round is already running; "
                        "try again shortly"
                    }
                },
                head_only,
                409,
            )
            return
        self._send_json(result, head_only)

    def _handle_outbox_drop_held(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("target") or body.get("id")
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'target' is required"}},
                head_only,
                400,
            )
            return
        try:
            dropped = self.bridge.drop_held_mail(target.strip())
        except ValueError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except MailboxError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        self._send_json({"ok": True, "dropped": dropped}, head_only)

    def _handle_ping(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        timeout = body.get("timeout", 15.0)
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'to' is required"}}, head_only, 400
            )
            return
        timeout = _clamped_timeout(timeout, 15.0)
        try:
            result = self.bridge.ping(target, timeout=timeout)
        except PeerError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(result, head_only)

    def _handle_info(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        timeout = body.get("timeout", 15.0)
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'to' is required"}}, head_only, 400
            )
            return
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = 15.0
        try:
            timeout = float(timeout)
        except OverflowError:
            # An integer too large for a float is far above the cap.
            timeout = 120.0
        timeout = max(1.0, min(timeout, 120.0))
        try:
            result = self.bridge.peer_info(target, timeout=timeout)
        except PeerError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(result, head_only)

    def _handle_allowlist(
        self, body: Dict[str, Any], head_only: bool, action: str
    ) -> None:
        try:
            if action == "set-enabled":
                result = self.bridge.allowlist_set_enabled(body.get("enabled"))
            else:
                target = body.get("target")
                if not isinstance(target, str) or not target.strip():
                    self._send_json(
                        {"error": {"message": "field 'target' is required"}},
                        head_only,
                        400,
                    )
                    return
                if action == "add":
                    result = self.bridge.allowlist_add(target)
                else:
                    result = self.bridge.allowlist_remove(target)
        except (AllowListError, PeerError) as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
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

    def _read_raw_body(self, *, max_bytes: int) -> bytes:
        """Read the request body as bytes, enforcing a size cap.

        The JSON parsing lives in :meth:`_read_body`; routes that must
        verify the submitted bytes themselves (a signed bundle) read
        them here instead, so no re-serialisation can change what the
        signature covers or how large it measures.
        """
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
        if length > max_bytes:
            raise RequestError("request body is too large")
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _read_body(self, *, max_bytes: int = MAX_BODY_BYTES) -> Dict[str, Any]:
        raw = self._read_raw_body(max_bytes=max_bytes)
        if not raw:
            return {}
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
            # Lets cross-origin console pages read the download's filename.
            self.send_header("Access-Control-Expose-Headers", "Content-Disposition")

    def _send_file(
        self, path: Path, filename: str, head_only: bool = False
    ) -> None:
        """Stream *path* as a download named *filename*."""
        # Open before any header is committed: a missing or unreadable
        # archive then reaches the caller as an ordinary error response.
        with path.open("rb") as handle:
            size = os.fstat(handle.fileno()).st_size
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/gzip")
            self.send_header(
                "Content-Disposition", f'attachment; filename="{filename}"'
            )
            self.send_header("Cache-Control", "no-store")
            self._cors_headers()
            self.send_header("Content-Length", str(size))
            self.end_headers()
            if head_only:
                return
            try:
                shutil.copyfileobj(handle, self.wfile)
            except OSError:
                # The 200 and headers are already on the wire, so a second
                # response would corrupt the reply.  Log, and drop the
                # connection so the client sees a truncated transfer.
                log.warning("backup download aborted", exc_info=True)
                self.close_connection = True

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

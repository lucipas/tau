"""A tau node: discovery, announcements and encrypted peer messaging."""

from __future__ import annotations

import base64
import errno
import hashlib
import logging
import os
import random
import socket
import sys
import tempfile
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Union

import RNS

from . import __version__
from .allowlist import (
    AllowList,
    load_allowlist,
    load_allowlist_or_deny_all,
    save_allowlist,
)
from .blocklist import BlockList, load_blocklist_or_block_all
from .config import (
    TauConfig,
    accept_files_enabled,
    send_read_receipts_enabled,
    tcp_listen_endpoints,
    write_rns_config,
)
from .groups import GroupStore
from .identity import (
    destination_hash as tau_destination_hash,
    load_or_create_identity,
    load_public_key,
)
from .inbox import Inbox, InboxError
from .instance_lock import (
    InstanceAlreadyRunning,
    InstanceLock,
    NodeError,
    instance_lock_holder,
)
from .lxmf import (
    LXMFError,
    build_lxmf_message,
    lxmf_router_delivery_hash,
    lxmf_router_propagation_hash,
    send_lxmf_message,
    start_lxmf_router,
    stop_lxmf_router,
)
from .mailbox import (
    MAX_HOLDERS_PER_MESSAGE,
    TRUSTED_PEER_MIN_AGE_SECONDS,
    Mailbox,
    MailboxError,
    MailboxFull,
    open_sealed_envelope,
    seal_message,
    seal_receipt,
)
from .peers import DESTINATION_HASH_BYTES, Peer, PeerError, PeerRegistry
from .protocol import (
    APP_NAME,
    ASPECT,
    ASPECT_FILTER,
    DEFAULT_MESSAGE_KIND,
    MAX_FILE_BYTES,
    MAX_MAIL_REQUEST_BYTES,
    MAX_MAIL_TTL,
    MAX_READ_IDS,
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    PROTOCOL_VERSION,
    REQUEST_FILE,
    REQUEST_INFO,
    REQUEST_GROUP_MESSAGE,
    REQUEST_MAIL_FETCH,
    REQUEST_MAIL_STORE,
    REQUEST_MESSAGE,
    REQUEST_PEERS,
    REQUEST_PING,
    REQUEST_READ,
    REQUEST_REACT,
    REQUEST_RETRACT,
    REQUEST_DM_EDIT,
    ProtocolError,
    build_announce,
    build_file_offer,
    build_mail_fetch,
    build_mail_store,
    build_read_receipt,
    clean_in_reply_to,
    clean_reaction,
    decode,
    derive_group_id,
    edit_dict,
    edit_unsigned_bytes,
    encode_group_message,
    encode,
    encode_message,
    generate_message_id,
    normalize_message,
    normalize_message_kind,
    parse_announce,
    parse_edit,
    parse_group_message,
    parse_file_offer,
    parse_mail_fetch,
    parse_mail_store,
    parse_message,
    parse_read_receipt,
    parse_reaction,
    parse_retract,
    reaction_dict,
    reaction_unsigned_bytes,
    retract_dict,
    retract_unsigned_bytes,
    response_error,
    response_ok,
    sanitize_file_name,
)

log = logging.getLogger(__name__)


def _budget(timeout: float, deadline: Optional[float]) -> float:
    """Clip a network timeout to what is left before a monotonic deadline."""
    if deadline is None:
        return timeout
    return max(0.0, min(timeout, deadline - time.monotonic()))


def _past(deadline: Optional[float]) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _remaining(deadline: Optional[float]) -> Optional[float]:
    """Time left before a monotonic deadline, or None when unbounded."""
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


# Reticulum adds framing and resource overhead on top of request payloads.
_REQUEST_WIRE_LIMIT = MAX_REQUEST_BYTES + 2 * 1024
_MAIL_WIRE_LIMIT = MAX_MAIL_REQUEST_BYTES + 2 * 1024
_RESPONSE_WIRE_LIMIT = MAX_RESPONSE_BYTES + 2 * 1024
_DEFAULT_LINK_TIMEOUT = 15.0
_PEER_SAVE_INTERVAL = 5.0
_PATHFINDER_MAX_HOPS = 128
_MAIL_INTERVAL = 45.0
_MAIL_REQUEST_TIMEOUT = 6.0
_EXCHANGE_TIMEOUT = 5.0
# Background peer exchange: a few known peers per sweep, each asked at most
# once per _EXCHANGE_INTERVAL, so quiet neighbours still share their peers.
_EXCHANGE_SWEEP_PEERS = 2
_EXCHANGE_INTERVAL = 900.0
# Peers contacted per background sweep.  Each contact can wait out a
# request timeout, so an uncapped sweep over a large registry holds the
# mail lock for minutes and starves announce-triggered rounds; the loop
# runs every _MAIL_INTERVAL seconds, so a cap only spreads the work out.
_MAIL_SWEEP_PEERS = 16
_DEPOSIT_RETRY_AFTER = 3600.0
# A holder record older than this no longer counts toward the holder cap:
# the holder may have shed the copy under its sender quota, so the message
# can be deposited with another relay.  This can let a message end up held
# by more than MAX_HOLDERS_PER_MESSAGE relays when the original holders
# still have their copies — accepted deliberately: extra sealed copies are
# deduped on delivery, while a strict cap could strand mail whose holders
# all shed it.  There is no shed signal from holder to sender, so record
# age is the only evidence available.  The window matches the deposit
# retry interval.
_HOLDER_RECORD_TTL = 3600.0
_FETCH_BLOB_BUDGET = 40 * 1024
# A file offer stays valid this long: the sender advertises the
# Resource right after the offer is accepted, so an offer that is
# never followed by bytes expires instead of letting a stranger
# fill the disk later with an old id.
_FILE_OFFER_TTL = 300.0
# Bounds on offers awaiting their Resource, so accepted offers that
# never receive bytes cannot grow memory without limit.
_MAX_PENDING_FILE_OFFERS = 64
_MAX_PENDING_FILE_OFFERS_PER_SENDER = 4
# Default budget for one file transfer (offer + Resource bytes).
_FILE_TRANSFER_TIMEOUT = 300.0
# A Resource advertisement's data size is the file size plus the
# metadata carried with it (the transfer id: a 3-byte length prefix
# around a msgpack'd 32-character string, 37 bytes in total).  The
# receiver matches advertisements against pending offers with this
# much slack; the exact size and hash are re-checked once the bytes
# are assembled, so the slack only admits a bounded pre-filter.
_FILE_METADATA_OVERHEAD = 64


class PeerRefusedError(NodeError):
    """Raised when a peer explicitly refuses a request.

    Distinct from transport failures: a refusal must never be retried
    over another route (such as LXMF), since that bypasses the peer's
    policy.
    """


class NotAllowedError(PeerRefusedError):
    """Raised when a peer explicitly rejects a request (allow-list denial).

    Distinct from other :class:`NodeError` failures so callers can tell
    an explicit refusal apart from an unreachable peer: a refusal must
    not be silently queued for retry.
    """


class NotAMemberError(NodeError):
    """Raised when a peer rejects a group message as not a group member.

    A changed roster or a non-member sender is a permanent refusal, so
    the group leg is not queued for retry.
    """


def check_tcp_listen_ports(config: TauConfig) -> None:
    """Fail fast when a configured TCP listen port cannot be bound.

    Reticulum is configured with ``panic_on_interface_error = No``, so
    a TCP server interface whose port is already in use fails silently
    inside Reticulum and the node serves with a dead interface.  Probe
    each configured listen endpoint first — with ``SO_REUSEADDR``, the
    way Reticulum's own server socket binds, so a socket merely in
    TIME_WAIT does not trip the check — and raise :class:`NodeError`
    naming the port instead.

    Only ``tau serve`` calls this.  Transient commands share the data
    directory (and config) with a running serve and must keep working
    while it holds the port, so they skip the check.
    """
    for endpoint in tcp_listen_endpoints(config.rns_config_file):
        _probe_tcp_listen(
            endpoint.host,
            endpoint.port,
            prefer_ipv6=endpoint.prefer_ipv6,
            device=endpoint.device,
        )


def _resolve_device_address(device: str, *, prefer_ipv6: bool) -> Optional[str]:
    """Resolve a kernel interface name to the address Reticulum binds.

    Mirrors ``TCPServerInterface.get_address_for_if``: the first address
    of the preferred family on the device, with a scope suffix for
    link-local IPv6.  Returns ``None`` when the device has no usable
    address, so the caller can fall back to the configured host.
    """
    try:
        from RNS.Interfaces import netinfo
    except ImportError:  # pragma: no cover - Reticulum is a hard dep
        return None
    try:
        ifaddr = netinfo.ifaddresses(device)
    except Exception:
        return None
    if not ifaddr:
        return None
    if (prefer_ipv6 or netinfo.AF_INET not in ifaddr) and (
        netinfo.AF_INET6 in ifaddr
    ):
        address = ifaddr[netinfo.AF_INET6][0]["addr"]
        if address.lower().startswith("fe80::"):
            return f"{address}%{device}"
        return address
    if netinfo.AF_INET in ifaddr:
        return ifaddr[netinfo.AF_INET][0]["addr"]
    return None


def _probe_tcp_listen(
    host: str,
    port: int,
    *,
    prefer_ipv6: bool = False,
    device: Optional[str] = None,
) -> None:
    if device is not None:
        # Reticulum binds an address on the device and ignores
        # listen_ip; probe that address, not the wildcard.
        resolved = _resolve_device_address(device, prefer_ipv6=prefer_ipv6)
        if resolved is not None:
            host = resolved
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise NodeError(
            f"cannot resolve TCP listen address {host}:{port} ({exc}); "
            "the TCP server interface cannot come up"
        ) from exc
    # Mirror Reticulum's address choice (TCPServerInterface
    # .get_address_for_host): prefer IPv6 only when the section asks
    # for it, IPv4 otherwise.
    preferred = socket.AF_INET6 if prefer_ipv6 else socket.AF_INET
    info = next(
        (entry for entry in infos if entry[0] == preferred),
        infos[0],
    )
    family, socktype, proto, _, sockaddr = info
    sock = socket.socket(family, socktype, proto)
    try:
        # On Windows SO_REUSEADDR lets a socket bind a port that
        # another live listener already holds, so the probe would pass
        # when it should fail. Only enable it where it means
        # "TIME_WAIT is fine" (POSIX).
        if sys.platform != "win32":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(sockaddr)
    except OSError as exc:
        if exc.errno == errno.EADDRINUSE:
            raise NodeError(
                f"TCP listen port {port} on {host} is already in use; "
                "another tau node (or another process) is already "
                "listening there. Stop it, or choose a different "
                "TCP listen port"
            ) from exc
        raise NodeError(
            f"cannot listen on TCP {host}:{port}: {exc}; "
            "the TCP server interface cannot come up"
        ) from exc
    finally:
        sock.close()


def _rns_log(message: str) -> None:
    """Send Reticulum's own log lines to stderr, never to stdout."""
    print(message, file=sys.stderr, flush=True)


class _AnnounceHandler:
    """Bridge between Reticulum announces and a :class:`TauNode`."""

    def __init__(self, node: "TauNode") -> None:
        self.node = node
        self.aspect_filter = ASPECT_FILTER

    def received_announce(
        self,
        destination_hash: bytes,
        announced_identity: RNS.Identity,
        app_data: Optional[bytes],
    ) -> None:
        self.node._on_announce(destination_hash, announced_identity, app_data)


def _read_resource_data(resource: Any) -> Optional[bytes]:
    """Read the assembled bytes of a concluded Resource.

    A received Resource keeps its bytes in a file on disk, so
    ``resource.data`` is an open binary file, not ``bytes`` — read
    it.  (A locally built Resource holds plain bytes instead.)
    """
    raw = getattr(resource, "data", None)
    if raw is None:
        return None
    if hasattr(raw, "read"):
        try:
            return bytes(raw.read())
        except Exception:  # pragma: no cover - defensive
            return None
    try:
        return bytes(raw)
    except Exception:  # pragma: no cover - defensive
        return None


def _create_unique_file(directory: Path, name: str, data: bytes) -> Path:
    """Write *data* to a new file in *directory*, never overwriting.

    The bytes go to a temporary file first, then the finished file is
    linked into place: a name appears only once its content is
    complete, so a concurrent reader — a live backup, the web console —
    never sees a half-written received file.  The temporary file lives
    in a fresh staging directory inside the sender directory: that
    keeps the staging file on the sender directory's own mount (a
    ``files/<sender>`` mount point would make a cross-device link
    fail) while keeping it out of backups, which only enumerate files
    directly inside sender directories.  The staging directory gets a
    unique random suffix, so it can never collide with a received
    file's name, and it is removed once the transfer is published.
    Each candidate is linked with ``os.link``, so an existing name,
    including a symlink, is never replaced; the next numbered name is
    tried instead.  The check and the link are one syscall, so
    concurrent transfers cannot pick the same destination.
    """
    stem = Path(name).stem
    suffix = Path(name).suffix
    staging = Path(tempfile.mkdtemp(prefix=".tau-tmp-", dir=directory))
    fd, tmp_name = tempfile.mkstemp(dir=staging, prefix=".tau-received-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        counter = 1
        while True:
            candidate_name = name if counter == 1 else f"{stem}-{counter}{suffix}"
            candidate = directory / candidate_name
            try:
                os.link(tmp_name, candidate)
            except FileExistsError:
                counter += 1
                continue
            return candidate
    finally:
        # Best effort: a crash between link and cleanup leaves a
        # staging directory behind, which is never enumerated as a
        # received file.
        try:
            os.unlink(tmp_name)
        except OSError:  # pragma: no cover - best effort
            pass
        try:
            staging.rmdir()
        except OSError:  # pragma: no cover - best effort
            pass


class TauNode:
    """A peer in a tau network.

    A node owns one Reticulum identity, announces a ``tau.node`` destination
    and answers requests over encrypted links.  Reticulum cannot be
    re-initialised in the same process, so a node is effectively one-shot: use
    one :class:`TauNode` per process.
    """

    def __init__(
        self,
        config: TauConfig,
        *,
        name: Optional[str] = None,
        registry: Optional[PeerRegistry] = None,
        on_message: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_peer: Optional[Callable[[Peer], None]] = None,
        on_delivered: Optional[Callable[[str], None]] = None,
        on_file: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_read: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_reaction: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_retract: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.config = config
        self.name = name or config.name
        self.registry = (
            registry if registry is not None else PeerRegistry(config.peers_path)
        )
        self.mailbox = Mailbox(config.mailbox_path)
        self.inbox = Inbox(config.inbox_path)
        self.groups = GroupStore(getattr(config, "groups_path", None))
        self.on_message = on_message
        self.on_peer = on_peer
        self.on_delivered = on_delivered
        self.on_file = on_file
        self.on_read = on_read
        self.on_reaction = on_reaction
        self.on_retract = on_retract
        # Ids whose delivery this node confirmed by a successful send,
        # newest last, capped so a long-lived node does not grow the
        # record without bound.  An outbox entry that disappears without
        # an id here was cancelled, expired, or removed elsewhere — not
        # delivered.
        self._delivered_ids: Deque[str] = deque(maxlen=512)

        self.reticulum: Optional[RNS.Reticulum] = None
        self.identity: Optional[RNS.Identity] = None
        self.destination: Optional[RNS.Destination] = None
        self.lxmf_router: Optional[Any] = None
        # LXMF delivery coordination (see _handle_lxmf_message): the
        # router invokes the delivery callback on its own thread, so
        # deliveries can overlap each other and node shutdown.
        # _lxmf_delivery_lock serialises the dedupe-and-store span so
        # two copies of one message cannot both claim it; the
        # stopping flag plus in-flight count let stop() wait for a
        # delivery already in progress instead of persisting state
        # and releasing the instance lock under it.
        self._lxmf_delivery_lock = threading.Lock()
        self._lxmf_state_lock = threading.Lock()
        self._lxmf_stopping = False
        self._lxmf_inflight = 0
        self._lxmf_drained = threading.Condition(self._lxmf_state_lock)

        self._announce_handler: Optional[_AnnounceHandler] = None
        self._announce_thread: Optional[threading.Thread] = None
        self._mail_thread: Optional[threading.Thread] = None
        self._mail_lock = threading.Lock()
        self._mail_round_threads: Set[threading.Thread] = set()
        self._mail_rounds_lock = threading.Lock()
        self._mail_pending: Set[str] = set()
        self._exchanged_at: Dict[bytes, float] = {}
        self._stop_event = threading.Event()
        self._started_at: Optional[float] = None
        self._links_lock = threading.Lock()
        self._active_links: Set[RNS.Link] = set()
        self._pending_file_offers: Dict[str, Dict[str, Any]] = {}
        self._file_offers_lock = threading.Lock()
        self._save_lock = threading.Lock()
        self._last_peer_save = 0.0
        self._instance_lock: Optional[InstanceLock] = None
        self._shutdown = False
        self.app: Optional[Any] = None
        self._allowlist: Optional[AllowList] = None
        self._allowlist_lock = threading.RLock()
        self._blocklist: Optional[BlockList] = None

    @property
    def allowlist(self) -> AllowList:
        """The DM allow-list, loaded lazily from the data directory.

        A list that cannot be parsed fails closed — remote senders are
        denied — mirroring the Tan admission ruleset.
        """
        if self._allowlist is None:
            self._allowlist = load_allowlist_or_deny_all(
                getattr(self.config, "allowlist_path", None)
            )
        return self._allowlist

    def set_allowlist(self, allowlist: AllowList) -> None:
        """Replace the DM allow-list, in memory and on disk.

        The web console uses this so an owner changing the list from
        the console sees it enforced immediately: unlike the CLI,
        whose saved changes only apply when the node restarts, the
        console *is* the running node.
        """
        with self._allowlist_lock:
            # Persist first: if the write fails, the running node keeps
            # the policy that is actually on disk.
            save_allowlist(self.config.allowlist_path, allowlist)
            self._allowlist = allowlist

    def update_allowlist(
        self, transform: Callable[[AllowList], AllowList]
    ) -> AllowList:
        """Atomically apply *transform* to the DM allow-list.

        The saved list is re-read first, so a change another process
        (e.g. the CLI) saved while this node runs is preserved instead
        of overwritten, and a corrupt saved file refuses the edit —
        the CLI refuses the same edit — instead of replacing the file
        with the node's fail-closed fallback. The whole
        read-modify-write is serialised, so overlapping console
        mutations cannot lose each other's changes.
        """
        with self._allowlist_lock:
            current = load_allowlist(
                getattr(self.config, "allowlist_path", None)
            )
            updated = transform(current)
            self.set_allowlist(updated)
            return updated

    @property
    def read_receipts_enabled(self) -> bool:
        """Whether this node sends read receipts when it reads a DM.

        Read live from ``node.json`` each time, so `tau config
        read-receipts off` takes effect without a restart.  A node
        that never sends receipts simply never does; senders treat
        the absence of a receipt as unknown, not unread-forever.
        """
        return send_read_receipts_enabled(self.config.data_dir)

    @property
    def blocklist(self) -> BlockList:
        """The DM/Tan block-list, loaded lazily from the data directory.

        Checked before the allow-list: blocking an allow-listed peer
        wins.  A list that cannot be parsed fails closed — every
        sender is blocked — so a corrupt list can never silently
        admit a sender the owner meant to silence.
        """
        if self._blocklist is None:
            self._blocklist = load_blocklist_or_block_all(
                getattr(self.config, "blocklist_path", None)
            )
        return self._blocklist

    def set_blocklist(self, blocklist: BlockList) -> None:
        """Replace the cached block-list (e.g. after the console edits it).

        The next enforcement check reads the new list at once, so a
        block or unblock from the console takes effect without a restart.
        """
        self._blocklist = blocklist

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self) -> "TauNode":
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()

    @property
    def destination_hexhash(self) -> str:
        if self.destination is None:
            raise NodeError("node has not been started")
        return self.destination.hexhash

    @property
    def started(self) -> bool:
        return self.reticulum is not None

    def start(
        self,
        *,
        announce: bool = True,
        exclusive: bool = False,
        mail_loop: bool = True,
        lxmf: bool = False,
        lxmf_propagation: bool = False,
    ) -> None:
        """Start Reticulum, load the identity and begin answering requests.

        With ``exclusive`` — used by ``tau serve``, the long-lived
        node — starting takes an exclusive lock on the data directory,
        so a second serving node on the same directory fails fast
        instead of corrupting the shared peer registry, mailbox and
        Tan database.  The lock is held until :meth:`stop` (or process
        exit). Transient commands can leave it off to share the directory
        briefly (the mailbox serialises its file across processes).
        Publishing requests the lock and, if it is held, routes the post
        through the running social node instead.
        With ``mail_loop`` off, no background mail sweep runs: a
        one-shot command that flushes the outbox itself (``tau outbox
        retry``) opts out so its own flush cannot lose the mail lock
        to the loop's first round and silently report nothing moved.
        With ``lxmf`` on, an LXMF router also runs alongside the node
        (see :meth:`_start_lxmf_router`); it needs the ``lxmf`` extra
        installed and must start on the main thread.  With
        ``lxmf_propagation`` on (which requires ``lxmf``), the router
        also serves as an LXMF propagation node, holding mail for
        offline recipients.
        """
        if lxmf_propagation and not lxmf:
            raise NodeError("lxmf_propagation requires lxmf")
        if self.reticulum is not None:
            raise NodeError("node is already running")
        if self._shutdown:
            raise NodeError(
                "Reticulum cannot be restarted in the same process; "
                "create a new TauNode in a new process"
            )

        self.config.prepare()
        if exclusive:
            lock = InstanceLock(
                self.config.instance_lock_path, self.config.data_dir
            )
            lock.acquire()
            self._instance_lock = lock
            # The registry was loaded when this node was constructed,
            # before the lock was held: a `tau peers forget` in between
            # could have removed a peer this node would then restore on
            # its next save. Reload under the lock so the in-memory
            # snapshot is no older than the lock itself.
            self.registry.load()
        try:
            self._start(
                announce=announce,
                mail_loop=mail_loop,
                lxmf=lxmf,
                lxmf_propagation=lxmf_propagation,
            )
        except BaseException:
            # Clean up partial startup, including workers and links, before
            # returning the directory to another process.
            try:
                self.stop()
            except Exception:  # pragma: no cover - defensive
                log.debug("Cleanup after a failed start failed", exc_info=True)
                self._stop_workers()
                self._release_instance_lock()
            raise

    def _start(
        self,
        *,
        announce: bool,
        mail_loop: bool = True,
        lxmf: bool = False,
        lxmf_propagation: bool = False,
    ) -> None:
        if not self.config.rns_config_file.exists():
            log.info("No Reticulum config found; writing a default one")
            write_rns_config(
                self.config.rns_config_file,
                auto_interface=True,
                loglevel=self.config.rns_loglevel,
            )

        log.info("Starting Reticulum with config %s", self.config.rns_config_dir)
        self.reticulum = RNS.Reticulum(
            configdir=str(self.config.rns_config_dir),
            loglevel=self.config.rns_loglevel,
            logdest=_rns_log,
        )

        self.identity = load_or_create_identity(self.config.identity_path)
        self.destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            APP_NAME,
            ASPECT,
        )
        self.destination.set_max_request_size(
            max(_REQUEST_WIRE_LIMIT, _MAIL_WIRE_LIMIT)
        )
        self.destination.set_default_app_data(lambda: build_announce(self.name))
        self.destination.set_link_established_callback(self._on_incoming_link)

        handlers = {
            REQUEST_PING: self._handle_ping,
            REQUEST_INFO: self._handle_info,
            REQUEST_MESSAGE: self._handle_message,
            REQUEST_GROUP_MESSAGE: self._handle_group_message,
            REQUEST_FILE: self._handle_file,
            REQUEST_READ: self._handle_read,
            REQUEST_REACT: self._handle_reaction,
            REQUEST_RETRACT: self._handle_retract,
            REQUEST_DM_EDIT: self._handle_dm_edit,
            REQUEST_PEERS: self._handle_peers,
            REQUEST_MAIL_STORE: self._handle_mail_store,
            REQUEST_MAIL_FETCH: self._handle_mail_fetch,
        }
        for path, handler in handlers.items():
            self.destination.register_request_handler(
                path, handler, allow=RNS.Destination.ALLOW_ALL
            )

        self._announce_handler = _AnnounceHandler(self)
        RNS.Transport.register_announce_handler(self._announce_handler)

        self._stop_event.clear()
        self._started_at = time.time()

        if self.config.announce_interval > 0:
            self._announce_thread = threading.Thread(
                target=self._announce_loop,
                name="tau-announce",
                daemon=True,
            )
            self._announce_thread.start()

        if mail_loop:
            self._mail_thread = threading.Thread(
                target=self._mail_loop,
                name="tau-mail",
                daemon=True,
            )
            self._mail_thread.start()

        if lxmf:
            self._start_lxmf_router(propagation=lxmf_propagation)

        if announce:
            self.announce()

        log.info("tau node %r ready at %s", self.name, self.destination.hexhash)

    def _start_lxmf_router(self, *, propagation: bool = False) -> None:
        """Run an LXMF router on this node's identity.

        The router listens on the LXMF delivery destination derived from
        the node's identity (the address ``tau id`` and ``tau peers``
        already show) and announces it, so LXMF clients can find a path
        to this node.  With ``propagation`` on, the router also serves
        as an LXMF propagation node, holding mail for offline
        recipients.  Incoming LXMF messages are delivered into the DM
        inbox by :meth:`_handle_lxmf_message`, registered as the
        router's delivery callback.  Storage lives under the data
        directory.  Sending over LXMF still follows under the migration
        tracked in issue #67.  A failure here is a node-start failure:
        the caller cleans up via :meth:`stop`.
        """
        assert self.identity is not None  # set above, before this runs
        try:
            self.lxmf_router = start_lxmf_router(
                self.identity,
                self.config.data_dir,
                display_name=self.name,
                propagation=propagation,
                delivery_callback=self._handle_lxmf_message,
            )
        except LXMFError as exc:
            raise NodeError(str(exc)) from exc
        delivery = lxmf_router_delivery_hash(self.lxmf_router)
        log.info(
            "LXMF router running, delivery %s",
            delivery.hex() if delivery is not None else "unknown",
        )
        propagation_hash = lxmf_router_propagation_hash(self.lxmf_router)
        if propagation_hash is not None:
            log.info(
                "LXMF propagation node enabled, propagation %s",
                propagation_hash.hex(),
            )

    def _handle_lxmf_message(self, message: Any) -> None:
        """Deliver one incoming LXMF message into the DM inbox.

        The router invokes this on its own thread with a decrypted
        ``LXMessage`` addressed to this node's delivery destination.
        It mirrors :meth:`_handle_message`: the signature must verify,
        the sender must be a known peer, and the block-list and DM
        allow-list gates apply in the same order.  A message that fails
        any gate is dropped and logged, never stored.  Any failure
        inside the handler is caught and logged — the callback must
        never raise back into the router.

        Deliveries that arrive while the node is stopping are dropped:
        :meth:`stop` sets the flag before tearing the router down and
        waits for deliveries already in flight, so no delivery writes
        to the inbox or notifies listeners after shutdown has
        persisted state and released the instance lock.
        """
        with self._lxmf_state_lock:
            if self._lxmf_stopping:
                log.info("Dropping LXMF message: node is stopping")
                return
            self._lxmf_inflight += 1
        try:
            self._deliver_lxmf_message(message)
        except Exception:  # pragma: no cover - defensive
            log.warning("Could not deliver incoming LXMF message", exc_info=True)
        finally:
            with self._lxmf_state_lock:
                self._lxmf_inflight -= 1
                if self._lxmf_inflight == 0:
                    self._lxmf_drained.notify_all()

    def _deliver_lxmf_message(self, message: Any) -> None:
        """Store one verified LXMF message, or drop it with a log line."""
        if not getattr(message, "signature_validated", False):
            # The router validates the signature against the recalled
            # source identity while unpacking; an unverified message
            # proves nothing about its sender, so it never reaches
            # the inbox — same fail-closed stance as the DM path.
            log.info("Dropping LXMF message with an unverified signature")
            return
        source_hash = getattr(message, "source_hash", None)
        if not isinstance(source_hash, (bytes, bytearray)):
            log.info("Dropping LXMF message with no source hash")
            return
        source_hex = bytes(source_hash).hex()
        sender = None
        for peer in self.registry.all():
            if peer.lxmf_hash == source_hex:
                sender = peer
                break
        if sender is None:
            # The inbox keys on tau destination hashes, and only a
            # known peer maps an LXMF delivery hash to one.  First
            # contact still needs a tau announce; an LXMF-only sender
            # cannot be attributed, so its message is dropped.
            log.info(
                "Dropping LXMF message from unknown sender %s",
                source_hex[:8],
            )
            return
        caller_hash = sender.hexhash
        if self.blocklist.is_blocked(caller_hash):
            log.info(
                "LXMF message from %s denied by the block-list",
                caller_hash,
            )
            return
        if not self.allowlist.is_allowed(caller_hash):
            log.info(
                "LXMF message from %s denied by the DM allow-list",
                caller_hash,
            )
            return
        content_as_string = getattr(message, "content_as_string", None)
        text = content_as_string() if callable(content_as_string) else None
        if not text:
            log.info(
                "Dropping LXMF message from %s with no readable text",
                caller_hash,
            )
            return
        message_hash = getattr(message, "hash", None)
        if not isinstance(message_hash, (bytes, bytearray)):
            log.info(
                "Dropping LXMF message from %s with no message hash",
                caller_hash,
            )
            return
        # A propagation copy can redeliver the same message after a
        # router restart, so dedupe on the LXMF hash exactly like the
        # DM path dedupes relayed copies on the tau message id.
        # Tau message ids are capped at 64 characters on the wire, so the
        # 5-char "lxmf:" prefix leaves 59 hex digits of the hash (236 bits,
        # still collision-free in practice).
        message_id = f"lxmf:{bytes(message_hash).hex()[:59]}"
        # The router can invoke this callback concurrently for two
        # copies of one message (direct + propagation), and each copy
        # passes the router's own duplicate check first — so the
        # is_seen check, the inbox scan, the append and the mark_seen
        # below run under one lock.  Without it both copies could
        # pass the checks before either stored anything and the
        # message would land in the inbox twice.
        with self._lxmf_delivery_lock:
            if self.mailbox.is_seen(message_id):
                log.info("Ignoring duplicate LXMF message %s", message_id)
                return
            if any(
                entry.get("id") == message_id
                for entry in self.inbox.list_messages(direction=None)
            ):
                # Stored earlier but interrupted before it was marked seen.
                self.mailbox.mark_seen(message_id)
                log.info("Ignoring duplicate LXMF message %s", message_id)
                return
            try:
                sent_at = float(getattr(message, "timestamp", None) or 0.0)
            except (TypeError, ValueError):
                sent_at = 0.0
            received_at = time.time()
            record = {
                "from": sender.display_name,
                "destination": caller_hash,
                "text": text,
                "sent_at": sent_at,
                "received_at": received_at,
                "id": message_id,
                "expires_at": None,
                "direction": "in",
            }
            log.info(
                "LXMF message from %s (%s): %s",
                sender.display_name,
                caller_hash,
                text,
            )
            try:
                self.inbox.append(record)
            except Exception:
                # Not stored, so leave it unseen: a redelivery can retry it.
                log.warning(
                    "Could not record LXMF message %s", message_id, exc_info=True
                )
                return
            self.mailbox.mark_seen(message_id)
        if self.on_message is not None:
            try:
                self.on_message(record)
            except Exception:  # pragma: no cover - user callback
                log.warning("on_message callback failed", exc_info=True)

    def _wait_for_lxmf_deliveries(self) -> None:
        """Wait for in-flight LXMF deliveries, bounded so stop never hangs.

        A delivery is short — a few store lookups and one inbox append —
        so the wait is normally immediate.  If one is wedged (for
        example a listener that never returns), shutdown proceeds after
        the timeout rather than hanging the process.
        """
        with self._lxmf_drained:
            if not self._lxmf_drained.wait_for(
                lambda: self._lxmf_inflight == 0, timeout=10
            ):
                log.warning(
                    "Stopping with %d LXMF deliver%s still in flight",
                    self._lxmf_inflight,
                    "y" if self._lxmf_inflight == 1 else "ies",
                )

    def stop(self) -> None:
        """Stop announcing, close links and persist the peer registry."""
        # No new LXMF delivery starts past this point; deliveries
        # already in flight finish before state is persisted and the
        # instance lock is released (see _handle_lxmf_message).
        with self._lxmf_state_lock:
            self._lxmf_stopping = True
        # Workers must finish before pruning/saving state or releasing the lock.
        self._stop_workers()

        if self.lxmf_router is not None:
            # The router's exit handler persists its state and tears its
            # destinations down; it runs before Reticulum exits because
            # it still needs a live Transport.  It is idempotent, so the
            # atexit handler it registered is a harmless no-op afterwards.
            try:
                stop_lxmf_router(self.lxmf_router)
            except Exception:  # pragma: no cover - defensive
                log.debug("LXMF router shutdown failed", exc_info=True)
            self.lxmf_router = None
        self._wait_for_lxmf_deliveries()

        if self._announce_handler is not None:
            try:
                RNS.Transport.deregister_announce_handler(self._announce_handler)
            except Exception:  # pragma: no cover - defensive
                log.debug("Could not deregister announce handler", exc_info=True)
            self._announce_handler = None

        with self._links_lock:
            links = list(self._active_links)
            self._active_links.clear()
        for link in links:
            try:
                link.teardown()
            except Exception:  # pragma: no cover - defensive
                pass

        try:
            self.mailbox.prune()
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not prune mailbox on shutdown", exc_info=True)

        self._save_peers(force=True)

        if self.reticulum is not None:
            # Reticulum's exit handler redirects sys.stdout/sys.stderr to
            # /dev/null as part of its shutdown.  Preserve the streams so the
            # CLI can still print a result after stopping the node.
            stdout, stderr = sys.stdout, sys.stderr
            try:
                RNS.Reticulum.exit_handler()
            except Exception:  # pragma: no cover - defensive
                log.debug("Reticulum exit handler failed", exc_info=True)
            finally:
                sys.stdout, sys.stderr = stdout, stderr
            self.reticulum = None

        self._shutdown = True
        self._release_instance_lock()

    def _stop_workers(self) -> None:
        self._stop_event.set()
        for worker in (self._announce_thread, self._mail_thread):
            if worker is not None and worker is not threading.current_thread():
                # join() rejects threads whose start() failed or was never called.
                if worker.ident is not None:
                    worker.join()

    def _release_instance_lock(self) -> None:
        lock, self._instance_lock = self._instance_lock, None
        if lock is not None:
            lock.release()

    # -- announcing and discovery -----------------------------------------

    def announce(self) -> None:
        """Send a signed announce for this node's destination."""
        if self.destination is None:
            raise NodeError("node has not been started")
        self.destination.announce(app_data=build_announce(self.name))

    def _announce_loop(self) -> None:
        interval = max(1.0, float(self.config.announce_interval))
        while not self._stop_event.is_set():
            # Jitter keeps a room full of nodes from announcing in lockstep.
            delay = interval * (0.9 + 0.2 * random.random())
            if self._stop_event.wait(delay):
                break
            try:
                self.announce()
            except Exception:  # pragma: no cover - network dependent
                log.warning("Periodic announce failed", exc_info=True)

    def _on_announce(
        self,
        announced_hash: bytes,
        identity: Optional[RNS.Identity],
        app_data: Optional[bytes],
    ) -> None:
        if self.destination is None or identity is None:
            return
        if announced_hash == self.destination.hash:
            return
        try:
            if tau_destination_hash(identity) != announced_hash:
                log.debug("Ignoring announce with a mismatched destination")
                return
            info = parse_announce(app_data)
            hops = RNS.Transport.hops_to(announced_hash)
            if hops >= _PATHFINDER_MAX_HOPS:
                hops = 0
            peer = self.registry.upsert(
                announced_hash,
                identity.get_public_key(),
                name=info["name"],
                hops=hops,
                protocol=info["v"],
                caps=info["caps"],
            )
        except (ProtocolError, ValueError) as exc:
            log.debug("Ignoring invalid announce: %s", exc)
            return

        log.info(
            "Discovered %s (%s) at %d hop(s)",
            peer.display_name,
            peer.hexhash,
            peer.hops,
        )
        self._save_peers()
        if self.on_peer is not None:
            try:
                self.on_peer(peer)
            except Exception:  # pragma: no cover - user callback
                log.warning("on_peer callback failed", exc_info=True)

        # A peer (re)appearing is the trigger for store-and-forward: try
        # to hand it anything queued for it, leave sealed copies of other
        # queued mail with it, and collect anything it holds for us.
        self._spawn_mail_round(peer)

    def _spawn_mail_round(self, peer: Peer) -> None:
        """Run a mail round for an announcing peer on its own thread.

        The thread is tracked so a one-shot caller (``tau outbox
        retry``) can wait for in-flight rounds before reading final
        mailbox state — otherwise a round that delivers after the
        caller's own flush is missed from the report.
        """

        def _run() -> None:
            try:
                self._mail_round_for_peer(peer)
            finally:
                with self._mail_rounds_lock:
                    self._mail_round_threads.discard(threading.current_thread())

        thread = threading.Thread(
            target=_run,
            name="tau-mail-flush",
            daemon=True,
        )
        with self._mail_rounds_lock:
            self._mail_round_threads.add(thread)
        thread.start()

    def wait_for_mail_rounds(self, timeout: float = 30.0) -> bool:
        """Wait for announce-triggered mail rounds to finish.

        Returns True when no round is in flight.  Rounds spawned while
        waiting are waited for too, until the budget runs out; False
        means some round is still running and the caller should treat
        mailbox state read afterwards as possibly stale.
        """
        deadline = time.time() + max(0.0, timeout)
        while True:
            with self._mail_rounds_lock:
                rounds = [
                    thread
                    for thread in self._mail_round_threads
                    if thread.is_alive()
                ]
            if not rounds:
                return True
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            for thread in rounds:
                thread.join(timeout=remaining)
                if deadline - time.time() <= 0:
                    break

    def wait_for_delivery(
        self,
        message_id: str,
        *,
        timeout: float,
        poll_interval: float = 1.0,
    ) -> bool:
        """Block until a queued message leaves the outbox, or time out.

        Returns True only when this node confirmed the delivery of the
        entry.  An entry that vanishes without that confirmation was
        cancelled, expired, or removed elsewhere, so the result is False
        at once.  False is also returned when the entry is still queued
        when the deadline passes.  Each pass runs a targeted flush that
        ignores the entry's delivery backoff: the caller explicitly asked
        to wait for this message, so a backoff from an earlier failed
        attempt must not swallow the whole wait.  The flush shares the
        same deadline, so no peer or request starts after it.  The
        queue check is bounded by the time left as well, so a stalled
        mailbox-lock holder cannot keep the wait from reporting expiry.
        Retries stay at `poll_interval`: growing the pause would let the last
        attempt land long before the deadline, so a peer that becomes
        reachable late in the wait would be missed.  The wait never
        removes the entry itself; a message still queued at the deadline
        is left for the background mail loop.
        """
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            remaining = deadline - time.monotonic()
            # The queue check itself is bounded by the time left: a
            # stalled mailbox-lock holder must not keep the wait from
            # reporting expiry, so the check treats an unacquirable lock
            # as "still queued" and the deadline check below ends it.
            if not self._still_queued(message_id, timeout=max(0.0, remaining)):
                return message_id in self._delivered_ids
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self.flush_outbox(
                timeout=min(remaining, _MAIL_REQUEST_TIMEOUT),
                message_id=message_id,
                respect_backoff=False,
                wait_for_lock=min(remaining, 5.0),
                deadline=deadline,
            )
            remaining = deadline - time.monotonic()
            if not self._still_queued(message_id, timeout=max(0.0, remaining)):
                return message_id in self._delivered_ids
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(poll_interval, remaining))

    def _owed_hashes(self) -> Set[bytes]:
        """Destination hashes the node still owes mail or acks to.

        Peers holding mail we have fetched stay too: their pending
        acknowledgements go out with the next fetch.  Peers owed a
        read receipt are kept as well, so pruning cannot strand a
        receipt for a long-unseen sender.
        """
        owed = {entry.to for entry in self.mailbox.outbox_list()}
        owed |= {entry.to for entry in self.mailbox.receipt_outbox_list()}
        for holder in self.mailbox.pending_ack_holders():
            try:
                raw = bytes.fromhex(holder)
            except ValueError:
                continue
            if len(raw) == DESTINATION_HASH_BYTES:
                owed.add(raw)
        return owed

    def _save_peers(self, *, force: bool = False) -> None:
        if self.registry.path is None:
            return
        with self._save_lock:
            now = time.time()
            if not force and now - self._last_peer_save < _PEER_SAVE_INTERVAL:
                return
            try:
                # Pick up aliases set by one-shot commands while this
                # node was serving, before the snapshot below rewrites
                # the registry file — otherwise the save would erase them.
                self.registry.refresh_aliases()
                # Drop peers we have not heard from in a while, so the
                # registry (and every sweep over it, e.g. mail flush) does
                # not accumulate dead entries forever.  Peers with queued
                # mail are spared: fresh mail for a long-unseen peer must
                # not be stranded by the prune.
                # Hold the mailbox lock across the read and the prune so a
                # one-shot process cannot queue mail for a peer between our
                # snapshot of the shared mailbox and the registry save.
                with self.mailbox.transaction():
                    owed = self._owed_hashes()
                    dropped = self.registry.prune(keep=owed)
                    if dropped:
                        log.debug("Pruned %d stale peer(s) from the registry", dropped)
                    self.registry.save()
                self._last_peer_save = now
            except OSError:
                log.warning("Could not save peer registry", exc_info=True)

    # -- request handlers --------------------------------------------------

    def _caller_label(self, remote_identity: Optional[RNS.Identity]) -> Optional[str]:
        if remote_identity is None:
            return None
        for peer in self.registry.all():
            if peer.public_key == remote_identity.get_public_key():
                return peer.hexhash
        # The caller is not in the local registry: derive the tau
        # destination hash from the verified identity.  The identity
        # hash alone is a different value, and the allow-list (and the
        # peer registry) are keyed by destination hash.
        return tau_destination_hash(remote_identity).hex()

    def _handle_ping(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        return response_ok(
            pong=True,
            server=self.name,
            node=self.destination_hexhash,
            time=time.time(),
            caller=self._caller_label(remote_identity),
        )

    def _handle_info(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        payload = self.info()
        payload["caller"] = self._caller_label(remote_identity)
        return response_ok(**payload)

    def _handle_peers(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        peers = self.registry.public_peers(include_keys=True)
        return response_ok(peers=peers[:100], count=len(peers))

    def _record_received(self, record: Dict[str, Any]) -> None:
        """Persist a delivered DM to the inbox; never break delivery."""
        try:
            self.inbox.append(record)
        except Exception:  # pragma: no cover - defensive
            log.warning("Could not record received message", exc_info=True)

    def _record_reaction(self, record: Dict[str, Any], *, strict: bool = False) -> None:
        """Persist a reaction and fan it out.

        A reaction whose target this node has never seen is still
        stored — the target may arrive later (a relayed copy can land
        after the reaction) — but it is only rendered under a message
        the inbox actually holds, so spraying unknown targets buys a
        peer nothing visible.

        With ``strict=True`` (the receive handler) a storage failure
        raises, so the handler can answer with an error instead of
        acknowledging a reaction it did not store.  Otherwise a failed
        local record is logged and delivery still succeeds — the
        sender's own copy is best-effort; the peer already accepted
        the reaction.
        """
        try:
            self.inbox.record_reaction(record)
        except Exception:  # pragma: no cover - storage failure
            log.warning("Could not record reaction", exc_info=True)
            if strict:
                raise
        if self.on_reaction is not None:
            try:
                self.on_reaction(record)
            except Exception:  # pragma: no cover - user callback
                log.warning("on_reaction callback failed", exc_info=True)

    def _record_sent(self, record: Dict[str, Any]) -> None:
        """Persist an outgoing DM to the inbox; never break sending.

        The inbox keeps both directions so a reply can quote the
        sender's own earlier message: the receiver renders the quote
        from its copy, and this node renders the peer's reply to it
        from this record.  ``Inbox.list_messages`` still shows
        received messages only.
        """
        try:
            self.inbox.append(record)
        except Exception:  # pragma: no cover - defensive
            log.warning("Could not record sent message", exc_info=True)

    def _handle_message(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            message = parse_message(data)
        except ProtocolError as exc:
            return response_error("invalid_message", str(exc))

        received_at = time.time()
        caller_hash = self._caller_label(remote_identity)
        if remote_identity is not None:
            # _caller_label falls back to the identity hash for senders
            # outside the registry; the inbox keys on Tau destination
            # hashes, so derive that here instead.
            caller_hash = tau_destination_hash(remote_identity).hex()
        if self.blocklist.is_blocked(caller_hash):
            # The block check runs before the allow-list: blocking an
            # allow-listed peer wins.  The drop is silent — the sender
            # gets a generic delivery failure, never a signal that they
            # are blocked, so blocking leaves no error oracle.  A fake
            # success reply was considered and rejected: the ticket
            # says blocked messages "simply never confirm", and a
            # success reply would falsely confirm delivery and stop
            # the sender's retries.  The message is never stored,
            # never marked seen, and never passed to on_message.
            log.info(
                "Message from %s denied by the block-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "unavailable", "message could not be delivered"
            )
        if not self.allowlist.is_allowed(caller_hash):
            log.info(
                "Message from %s denied by the DM allow-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "not_allowed", "sender is not on this node's allow-list"
            )
        caller_name = "unknown"
        if caller_hash:
            peer = self.registry.get(caller_hash)
            caller_name = peer.display_name if peer is not None else caller_hash[:8]

        # Store-and-forward retries and relay copies can deliver the same
        # message twice; the id makes delivery idempotent.
        message_id = message.get("id")
        duplicate = bool(message_id and self.mailbox.is_seen(message_id))
        # A delayed direct request past the sender's deadline is dropped
        # unrecorded (and not marked seen), so it is never shown.
        expires_at = message.get("expires_at")
        expired = expires_at is not None and expires_at <= received_at
        if expired:
            log.info("Dropping expired message %s from %s", message_id, caller_hash)
        if not duplicate and not expired:
            record = {
                "from": caller_name,
                "destination": caller_hash,
                "text": message["text"],
                "kind": message["kind"],
                "sent_at": message["sent_at"],
                "received_at": received_at,
                "id": message_id,
                "expires_at": message.get("expires_at"),
                "direction": "in",
            }
            if message.get("in_reply_to") is not None:
                record["in_reply_to"] = message["in_reply_to"]
            log.info(
                "Message from %s (%s): %s",
                caller_name,
                caller_hash or "anonymous",
                message["text"],
            )
            # Marking seen and recording the message are one delivery:
            # hold the mailbox transaction across both, so a live
            # backup never restores a seen marker with no inbox entry
            # (the message would then be rejected as already seen and
            # lost).  The record step takes the inbox transaction
            # inside this one; the order is mailbox first everywhere.
            # Re-check inside the transaction: a duplicate delivery
            # that landed while this one was in flight already marked
            # the id seen, and the transaction reloaded that mark —
            # without the re-check both copies would be stored, and a
            # later retract would leave one behind.
            recorded = False
            with self.mailbox.transaction():
                if not message_id or not self.mailbox.is_seen(message_id):
                    if message_id:
                        self.mailbox.mark_seen(message_id)
                    self._record_received(record)
                    recorded = True
            if recorded and self.on_message is not None:
                try:
                    self.on_message(record)
                except Exception:  # pragma: no cover - user callback
                    log.warning("on_message callback failed", exc_info=True)

        return response_ok(
            echo=message["text"],
            kind=message["kind"],
            received_at=received_at,
            server=self.name,
            node=self.destination_hexhash,
            duplicate=duplicate,
            expired=expired,
        )

    def _accept_group_message(
        self,
        sender_hash: str,
        group: Dict[str, Any],
        *,
        sent_at: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """Validate a group message's roster; pin the group on first sight.

        Returns the pinned group record, or ``None`` when the message
        must be rejected: the sender is not on the roster, this node is
        not on the roster, the id does not derive from the creator,
        name and roster, or the roster disagrees with the one already
        pinned for the group id (a changed roster means a new group).
        The sender is authenticated by the link identity (direct) or by
        the sealed envelope's signature (relayed), never by the payload;
        the id derivation is what binds the roster to the id, so a
        member cannot fork an id onto a recipient that has not seen it.
        The sender is also passed through, so a successor roster only
        replaces a learned group when the creator themself sent it —
        the creator field is only a claim, and any member could
        otherwise forge a successor and hijack sends by group name.
        ``sent_at`` is the message's send time on the sender's clock;
        it is what tells a queued creator message carrying an outdated
        roster (sent before the successor was learned) from an
        intentional creator restore (sent after), so the stale copy
        pins alongside instead of replacing the successor.
        """
        members = group.get("members") or []
        own_hash = self.destination_hexhash
        if sender_hash not in members or own_hash not in members:
            return None
        creator = group.get("creator") or ""
        try:
            if derive_group_id(creator, group.get("name") or "group", members) != group["id"]:
                return None
        except (ProtocolError, ValueError):
            return None
        return self.groups.record(
            group["id"],
            group.get("name") or "group",
            members,
            creator=creator,
            sender=sender_hash or "",
            sent_at=sent_at,
        )

    def _handle_group_message(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            message = parse_group_message(data)
        except ProtocolError as exc:
            return response_error("invalid_message", str(exc))

        received_at = time.time()
        caller_hash = self._caller_label(remote_identity)
        if remote_identity is not None:
            caller_hash = tau_destination_hash(remote_identity).hex()
        if self.blocklist.is_blocked(caller_hash):
            # The block check runs before the allow-list: blocking an
            # allow-listed peer wins.  The drop is silent — the sender
            # gets a generic delivery failure, never a signal that they
            # are blocked, so blocking leaves no error oracle.  The
            # message is never stored, never marked seen, and never
            # passed to on_message; the group is not pinned either.
            log.info(
                "Group message from %s denied by the block-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "unavailable", "message could not be delivered"
            )
        if not self.allowlist.is_allowed(caller_hash):
            log.info(
                "Group message from %s denied by the DM allow-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "not_allowed", "sender is not on this node's allow-list"
            )
        caller_name = "unknown"
        if caller_hash:
            peer = self.registry.get(caller_hash)
            caller_name = peer.display_name if peer is not None else caller_hash[:8]

        group = self._accept_group_message(
            caller_hash,
            {
                "id": message["group"],
                "name": message["group_name"],
                "creator": message["creator"],
                "members": message["members"],
            },
            sent_at=message.get("sent_at"),
        )
        if group is None:
            log.info(
                "Group message from %s rejected: sender or node not on "
                "the roster, or the roster changed for group %s",
                caller_hash or "anonymous",
                message["group"],
            )
            return response_error(
                "not_a_member", "sender or recipient is not a group member"
            )

        # Store-and-forward retries and relay copies can deliver the same
        # message twice; the id makes delivery idempotent.
        message_id = message.get("id")
        duplicate = bool(message_id and self.mailbox.is_seen(message_id))
        if not duplicate:
            record = {
                "from": caller_name,
                "destination": caller_hash,
                "text": message["text"],
                "sent_at": message["sent_at"],
                "received_at": received_at,
                "id": message_id,
                "kind": "group",
                "group": group["id"],
                "group_name": group["name"],
            }
            log.info(
                "Group message from %s (%s) in %s: %s",
                caller_name,
                caller_hash or "anonymous",
                group["name"],
                message["text"],
            )
            # One delivery: mark seen and record under the mailbox
            # transaction, so a live backup never restores a seen
            # marker with no inbox entry (see _handle_message).
            with self.mailbox.transaction():
                if message_id:
                    self.mailbox.mark_seen(message_id)
                self._record_received(record)
            if self.on_message is not None:
                try:
                    self.on_message(record)
                except Exception:  # pragma: no cover - user callback
                    log.warning("on_message callback failed", exc_info=True)

        return response_ok(
            echo=message["text"],
            received_at=received_at,
            server=self.name,
            node=self.destination_hexhash,
            group=group["id"],
            duplicate=duplicate,
        )

    def _handle_file(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        """Accept or refuse a signed file offer (``/tau/file``).

        The offer itself carries no bytes — it is the sender asking
        whether the transfer may start.  Acceptance registers a
        pending offer (transfer id, name, size, SHA-256) that the
        link's resource callback matches against the advertised
        Resource; the bytes are verified against the hash again once
        assembled, and never written to disk otherwise.

        Anti-replay: the offer is only honored on a link whose peer
        identified itself — an unidentified link skips the sender
        check, so it is refused outright — and only when the signed
        offer names this node as its recipient, so a captured offer
        cannot be replayed to a third node.  The Resource stage
        additionally binds the transfer to the sender authenticated
        on its link (see ``_on_resource_advertised``).

        Policy: files are off unless the owner opted in with
        ``tau config accept-files on`` — except that a sender on the
        DM allow-list is always allowed, and a sender the allow-list
        excludes is refused even when acceptance is on.  A refusal
        answers ``not_allowed`` so the sender fails fast instead of
        queueing a file nobody wants.
        """
        if remote_identity is None:
            log.info("File offer refused: sender did not identify on the link")
            return response_error(
                "invalid_offer", "sender did not identify on the link"
            )
        expected_recipient = (
            self.destination.hexhash if self.destination is not None else None
        )
        try:
            offer = parse_file_offer(
                data,
                remote_identity=remote_identity,
                expected_recipient=expected_recipient,
            )
        except ProtocolError as exc:
            return response_error("invalid_offer", str(exc))

        sender_hash = offer["from"]
        if not self.allowlist.is_allowed(sender_hash):
            log.info(
                "File offer from %s denied by the DM allow-list",
                sender_hash[:8],
            )
            return response_error(
                "not_allowed", "sender is not on this node's allow-list"
            )
        explicitly_listed = bool(
            self.allowlist.enabled and sender_hash in self.allowlist.allow
        )
        if not explicitly_listed and not accept_files_enabled(
            self.config.data_dir
        ):
            log.info(
                "File offer from %s refused: this node does not accept files",
                sender_hash[:8],
            )
            return response_error(
                "not_allowed",
                "this node does not accept files "
                "(see `tau config accept-files on`)",
            )
        self._prune_file_offers()
        with self._file_offers_lock:
            pending = list(self._pending_file_offers.values())
            if len(pending) >= _MAX_PENDING_FILE_OFFERS or (
                sum(1 for item in pending if item["from"] == sender_hash)
                >= _MAX_PENDING_FILE_OFFERS_PER_SENDER
            ):
                return response_error(
                    "busy", "too many file offers are pending; try again later"
                )
            self._pending_file_offers[offer["id"]] = {
                **offer,
                "expires_at": time.time() + _FILE_OFFER_TTL,
            }
        log.info(
            "Accepted file offer %s (%s, %d bytes) from %s",
            offer["id"][:8],
            offer["name"],
            offer["size"],
            sender_hash[:8],
        )
        return response_ok(accepted=True, id=offer["id"], name=offer["name"])

    def _handle_reaction(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            reaction = parse_reaction(data)
        except ProtocolError as exc:
            return response_error("invalid_reaction", str(exc))

        received_at = time.time()
        if remote_identity is None:
            # A reaction is state keyed by its reactor: without an
            # authenticated link identity there is no reactor to key on,
            # and an anonymous reaction could overwrite anyone's.
            return response_error(
                "unidentified", "identify on the link to react"
            )
        caller_hash = tau_destination_hash(remote_identity).hex()
        # Reactions are signed entries, like Tan likes: the signature
        # covers the whole payload and is verified against the link
        # identity, so attribution does not depend on trusting the
        # transport — a relayed or replayed reaction still has to prove
        # its reactor.  (parse_reaction already checked the base64.)
        signature_text = reaction.get("sig")
        if not signature_text:
            return response_error(
                "invalid_reaction", "reaction is missing its signature"
            )
        try:
            signature = base64.b64decode(signature_text, validate=True)
            unsigned = reaction_unsigned_bytes(data)
        except (ValueError, ProtocolError) as exc:
            return response_error("invalid_reaction", str(exc))
        try:
            verified = remote_identity.validate(signature, unsigned)
        except Exception as exc:
            return response_error(
                "invalid_reaction", f"could not verify reaction: {exc}"
            )
        if not verified:
            return response_error(
                "invalid_reaction", "reaction signature does not verify"
            )
        if not self.allowlist.is_allowed(caller_hash):
            log.info(
                "Reaction from %s denied by the DM allow-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "not_allowed", "sender is not on this node's allow-list"
            )
        caller_name = "unknown"
        if caller_hash:
            peer = self.registry.get(caller_hash)
            caller_name = peer.display_name if peer is not None else caller_hash[:8]

        # A retried or relayed reaction replays with the same id; the
        # latest-per-reactor state makes a replay harmless anyway, but
        # dedupe here keeps the inbox from churning on every retry.
        reaction_id = reaction.get("id")
        # Reactions get their own namespace, keyed by the reactor too, so a
        # reaction id can never mark a direct message id as seen.
        seen_key = f"reaction:{caller_hash}:{reaction_id}" if reaction_id else None
        duplicate = bool(seen_key and self.mailbox.is_seen(seen_key))
        if not duplicate:
            record = {
                "target": reaction["target"],
                "reaction": reaction["reaction"],
                "from": caller_name,
                "destination": caller_hash,
                "sent_at": reaction["sent_at"],
                "received_at": received_at,
                "id": reaction_id,
            }
            try:
                self._record_reaction(record, strict=True)
            except OSError as exc:
                # Do not acknowledge a reaction that was not stored, and
                # do not mark its id seen — the sender can retry with the
                # same id and it will be recorded then.
                return response_error(
                    "storage_failed", f"could not store reaction: {exc}"
                )
            if seen_key:
                self.mailbox.mark_seen(seen_key)
            log.info(
                "Reaction from %s (%s) on %s: %s",
                caller_name,
                caller_hash or "anonymous",
                reaction["target"][:8],
                reaction["reaction"] or "(retracted)",
            )

        return response_ok(
            reaction=reaction["reaction"],
            target=reaction["target"],
            received_at=received_at,
            duplicate=duplicate,
        )

    def _handle_retract(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            retract = parse_retract(data)
        except ProtocolError as exc:
            return response_error("invalid_retraction", str(exc))

        received_at = time.time()
        if remote_identity is None:
            # A retract deletes someone else's stored copy: without an
            # authenticated link identity there is no sender to check
            # against the message, and an anonymous retract could
            # delete anyone's message.
            return response_error(
                "unidentified", "identify on the link to retract"
            )
        caller_hash = tau_destination_hash(remote_identity).hex()
        # Retracts are signed entries, like reactions: the signature
        # covers the whole payload and is verified against the link
        # identity, so attribution does not depend on trusting the
        # transport — a relayed or replayed retract still has to prove
        # its sender.  (parse_retract already checked the base64.)
        signature_text = retract.get("sig")
        if not signature_text:
            return response_error(
                "invalid_retraction", "retraction is missing its signature"
            )
        try:
            signature = base64.b64decode(signature_text, validate=True)
            unsigned = retract_unsigned_bytes(data)
        except (ValueError, ProtocolError) as exc:
            return response_error("invalid_retraction", str(exc))
        try:
            verified = remote_identity.validate(signature, unsigned)
        except Exception as exc:
            return response_error(
                "invalid_retraction", f"could not verify retraction: {exc}"
            )
        if not verified:
            return response_error(
                "invalid_retraction", "retraction signature does not verify"
            )
        if not self.allowlist.is_allowed(caller_hash):
            log.info(
                "Retraction from %s denied by the DM allow-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "not_allowed", "sender is not on this node's allow-list"
            )

        # Only the message's original sender may retract it: the stored
        # copy's sender hash must match the caller.  An unknown id is
        # not an error — the message may already be deleted or never
        # have arrived — so a retried retract answers ok with
        # deleted False instead of failing.  The match must be exact:
        # resolve also matches unique prefixes, and a short target
        # must never delete a different message from the same sender.
        # Reload first: another process (a one-shot CLI import) may have
        # persisted the target after this node's in-memory copy was read.
        try:
            self.inbox.reload()
        except InboxError as exc:
            return response_error(
                "storage_failed", f"could not read inbox: {exc}"
            )
        try:
            stored = self.inbox.resolve(retract["target"])
        except ValueError:
            stored = None
        if stored is None or stored.get("id") != retract["target"]:
            return response_ok(
                target=retract["target"],
                received_at=received_at,
                deleted=False,
            )
        # Only a received record counts: an outgoing record also has the
        # caller's hash in ``destination`` (the recipient), so without the
        # direction check a recipient could delete the sender's own copy.
        if stored.get("direction") != "in" or stored.get("destination") != caller_hash:
            return response_error(
                "not_sender", "only the message's sender can retract it"
            )
        if stored.get("kind") == "file":
            # A saved file lives on disk; Inbox.delete drops only the
            # record, so retracting it would leave the file behind.
            return response_error(
                "not_supported",
                "file transfers cannot be retracted",
            )
        if stored.get("kind") == "group":
            # A group message lives in every member's inbox; deleting
            # it from one member alone would silently half-retract it.
            # Group fan-out retract is a follow-up, not v1.
            return response_error(
                "not_supported",
                "group messages cannot be retracted in v1",
            )
        # Authorize and delete as one exact-ID operation: the checks
        # above pick the error code, but the delete itself re-verifies
        # the exact id, direction, and sender inside the same inbox
        # transaction, so two concurrent retracts of one short id can
        # never reselect a different record by prefix.  Kind is
        # immutable after append, so the kind checks above cannot go
        # stale between here and the delete.
        try:
            removed = self.inbox.delete_authorized(
                retract["target"], direction="in", destination=caller_hash
            )
        except InboxError as exc:
            return response_error(
                "storage_failed", f"could not retract message: {exc}"
            )
        if removed is None:
            # A concurrent retract deleted it first: the message is
            # already gone, so answer the same as "not found".
            return response_ok(
                target=retract["target"],
                received_at=received_at,
                deleted=False,
            )
        # The inbox record is gone; a serving console keeps its own
        # copy of the message, so tell it to drop that too — otherwise
        # /api/messages keeps showing the retracted text, including
        # after a restart.  A failing console callback must not fail
        # the retraction itself: the inbox delete already landed.
        if self.on_retract is not None:
            try:
                self.on_retract(removed)
            except Exception:  # pragma: no cover - console callback
                log.warning("on_retract callback failed", exc_info=True)
        log.info(
            "Retraction from %s deleted %s",
            caller_hash[:8],
            retract["target"][:8],
        )
        return response_ok(
            target=retract["target"],
            received_at=received_at,
            deleted=True,
        )


    def _handle_dm_edit(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        """Apply a signed edit for a DM this node received."""
        try:
            edit = parse_edit(data)
        except ProtocolError as exc:
            return response_error("invalid_edit", str(exc))

        received_at = time.time()
        if remote_identity is None:
            # An edit is state keyed by its editor: without an
            # authenticated link identity there is no editor to key on,
            # and an anonymous edit could rewrite anyone's message.
            return response_error(
                "unidentified", "identify on the link to edit"
            )
        caller_hash = tau_destination_hash(remote_identity).hex()
        # Edits are signed entries, like reactions: the signature covers
        # the whole payload and is verified against the link identity, so
        # attribution does not depend on trusting the transport — a
        # relayed or replayed edit still has to prove its editor.
        # (parse_edit already checked the base64.)
        signature_text = edit.get("sig")
        if not signature_text:
            return response_error("invalid_edit", "edit is missing its signature")
        try:
            signature = base64.b64decode(signature_text, validate=True)
            unsigned = edit_unsigned_bytes(data)
        except (ValueError, ProtocolError) as exc:
            return response_error("invalid_edit", str(exc))
        try:
            verified = remote_identity.validate(signature, unsigned)
        except Exception as exc:
            return response_error(
                "invalid_edit", f"could not verify edit: {exc}"
            )
        if not verified:
            return response_error(
                "invalid_edit", "edit signature does not verify"
            )
        if self.blocklist.is_blocked(caller_hash):
            # The block check runs before the allow-list: blocking an
            # allow-listed peer wins, so a blocked sender cannot rewrite
            # a message already sitting in the inbox either.
            log.info(
                "Edit from %s denied by the block-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "unavailable", "message could not be delivered"
            )
        if not self.allowlist.is_allowed(caller_hash):
            log.info(
                "Edit from %s denied by the DM allow-list",
                caller_hash or "anonymous",
            )
            return response_error(
                "not_allowed", "sender is not on this node's allow-list"
            )
        caller_name = "unknown"
        if caller_hash:
            peer = self.registry.get(caller_hash)
            caller_name = peer.display_name if peer is not None else caller_hash[:8]

        target_id = edit["target"]
        try:
            record = self.inbox.resolve(target_id)
        except ValueError:
            # An unknown id answers ok with edited: false, so a retried
            # edit is safe (idempotent) — the sender can retry with the
            # same id and it will apply when the message arrives.
            return response_ok(
                target=target_id,
                edited=False,
                received_at=received_at,
            )
        if record.get("id") != target_id:
            # resolve() also matches unique prefixes; an edit must name
            # the full id, so a prefix match is not an edit target.
            return response_ok(
                target=target_id,
                edited=False,
                received_at=received_at,
            )
        if record.get("kind") == "group":
            # A group message has a different id on every member's
            # copy, so one edit cannot name them all; refuse instead of
            # rewriting a single member's copy.
            return response_error(
                "group_message",
                "group messages cannot be edited; send a new message instead",
            )
        if record.get("kind") == "file":
            return response_error(
                "file_message", "a file record has no text to edit"
            )
        if record.get("direction") != "in":
            # Only an inbound record names its sender in
            # ``destination``: an outbound record is this node's own
            # sent message, and no remote peer may rewrite it.
            return response_error(
                "not_sender", "only the message's sender may edit it"
            )
        if (record.get("destination") or "") != caller_hash:
            return response_error(
                "not_sender", "only the message's sender may edit it"
            )
        edit_sent_at = edit.get("sent_at")
        if edit_sent_at is None:
            # The signed sent_at is the edit's ordering key.  Falling
            # back to receipt time would let a replayed older edit look
            # new and rewind a newer one.
            return response_error(
                "invalid_edit", "edit is missing its signed sent_at"
            )
        effective_at = float(edit_sent_at)
        previous_edited_at = record.get("edited_at")
        if (
            isinstance(previous_edited_at, (int, float))
            and effective_at <= float(previous_edited_at)
        ):
            # A replayed or retried older edit must not rewind a newer
            # one; the sender's retry with the same id is harmless.
            return response_ok(
                target=target_id,
                edited=False,
                received_at=received_at,
            )
        try:
            # The ownership and ordering checks above read a snapshot;
            # the inbox repeats them atomically so racing edits cannot
            # apply out of order.
            applied = self.inbox.apply_edit(
                target_id,
                edit["text"],
                edited_at=effective_at,
                direction="in",
                destination=caller_hash,
                newer_than_existing=True,
            )
        except InboxError as exc:
            # Do not acknowledge an edit that was not stored, so the
            # sender can retry with the same id and it will apply then.
            return response_error(
                "storage_failed", f"could not store edit: {exc}"
            )
        if applied is None:
            # The record went away between the lookup and the edit (a
            # concurrent delete or retract): report it as not applied
            # rather than acknowledging an edit that landed nowhere.
            return response_ok(
                target=target_id,
                edited=False,
                received_at=received_at,
            )
        log.info(
            "Edit from %s (%s) on %s",
            caller_name,
            caller_hash or "anonymous",
            target_id[:8],
        )
        return response_ok(
            target=target_id,
            edited=True,
            received_at=received_at,
        )


    def _record_receipt(
        self, *, reader_hash: str, message_ids: List[str], read_at: float
    ) -> List[str]:
        """Apply a verified receipt: mark the ids read, notify listeners.

        Returns the ids actually recorded.  An id this node never sent,
        or sent to someone other than the reader, is rejected by
        ``Mailbox.record_read`` — a forged receipt changes nothing.
        Receipts are never receipted themselves.
        """
        recorded = []
        for message_id in message_ids:
            if self.mailbox.record_read(
                message_id, read_at, reader_hash=reader_hash
            ):
                recorded.append(message_id)
        if recorded and self.on_read is not None:
            try:
                self.on_read(
                    {
                        "reader": reader_hash,
                        "ids": recorded,
                        "read_at": read_at,
                    }
                )
            except Exception:  # pragma: no cover - user callback
                log.warning("on_read callback failed", exc_info=True)
        return recorded

    def _handle_read(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        """Accept a signed read receipt for messages this node sent."""
        try:
            receipt = parse_read_receipt(data, remote_identity=remote_identity)
        except ProtocolError as exc:
            return response_error("invalid_receipt", str(exc))
        reader_hash = receipt["from"]
        recorded = self._record_receipt(
            reader_hash=reader_hash,
            message_ids=receipt["ids"],
            read_at=receipt["read_at"],
        )
        log.info(
            "Read receipt from %s: %d of %d message(s) recorded",
            reader_hash[:8],
            len(recorded),
            len(receipt["ids"]),
        )
        return response_ok(recorded=recorded, count=len(recorded))

    def _handle_mail_store(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            store = parse_mail_store(data)
        except ProtocolError as exc:
            return response_error("invalid_mail", str(exc))
        if store["expires_at"] <= time.time():
            return response_error("mail_expired", "envelope has already expired")
        # Quotas are keyed by the peer that asked us to hold the
        # envelope: the sealed blob hides the inner sender, so the
        # depositing peer is the only attribution a holder has.  A
        # long-standing known peer gets the higher quota tier: in the
        # registry 24h+, heard directly rather than via exchange, and
        # with at least one message this node has delivered to it.
        depositor = (
            tau_destination_hash(remote_identity)
            if remote_identity is not None
            else None
        )
        trusted = False
        if depositor is not None:
            peer = self.registry.get(depositor)
            if (
                peer is not None
                and not peer.via_exchange
                and time.time() - peer.first_seen >= TRUSTED_PEER_MIN_AGE_SECONDS
                and peer.delivered_count > 0
            ):
                trusted = True
        try:
            stored = self.mailbox.hold(
                to=bytes.fromhex(store["to"]),
                blob=store["blob"],
                message_id=store["id"],
                expires_at=store["expires_at"],
                depositor=depositor,
                trusted=trusted,
            )
        except MailboxFull as exc:
            return response_error("mailbox_full", str(exc))
        except MailboxError as exc:
            return response_error("invalid_mail", str(exc))
        log.info(
            "Holding sealed mail %s for %s",
            store["id"],
            store["to"],
        )
        return response_ok(stored=stored, duplicate=not stored, id=store["id"])

    def _handle_mail_fetch(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        if remote_identity is None:
            return response_error(
                "unidentified", "identify on the link to fetch held mail"
            )
        recipient = tau_destination_hash(remote_identity)
        try:
            fetch = parse_mail_fetch(data)
        except ProtocolError as exc:
            return response_error("invalid_mail", str(exc))
        for message_id in fetch["ack"]:
            self.mailbox.drop_held(message_id, to=recipient)

        messages: List[Dict[str, Any]] = []
        budget = _FETCH_BLOB_BUDGET
        # Reload under the cross-process lock: a `tau outbox drop-held`
        # in another process removes the envelope from disk, and this
        # node's cached mailbox would otherwise keep handing it out.
        with self.mailbox.transaction():
            envelopes = self.mailbox.held_for(recipient)
        for envelope in envelopes:
            blob_text = base64.b64encode(envelope.blob).decode("ascii")
            if messages and len(blob_text) > budget:
                break
            messages.append(
                {
                    "id": envelope.id,
                    "blob": blob_text,
                    "expires_at": envelope.expires_at,
                }
            )
            budget -= len(blob_text)
            if len(messages) >= 20:
                break
        return response_ok(messages=messages, count=len(messages))

    # -- store-and-forward ---------------------------------------------------

    def _mail_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                # Rounds that found the mail lock busy are retried first,
                # then a capped sweep (outbox targets prioritised inside
                # flush_outbox) keeps each pass short.
                pending = list(self._mail_pending)
                self._mail_pending.clear()
                for hexhash in pending:
                    peer = self.registry.get(bytes.fromhex(hexhash))
                    if peer is not None:
                        self._mail_round_for_peer(peer)
                self.flush_outbox(max_peers=_MAIL_SWEEP_PEERS)
            except Exception:  # pragma: no cover - network dependent
                log.debug("Mail round failed", exc_info=True)
            try:
                self._exchange_sweep(max_peers=_EXCHANGE_SWEEP_PEERS)
            except Exception:  # pragma: no cover - network dependent
                log.debug("Peer exchange sweep failed", exc_info=True)
            if self._stop_event.wait(_MAIL_INTERVAL):
                break

    def _exchange_sweep(self, *, max_peers: int) -> None:
        """Ask a few known peers for their peers, outside the mail lock.

        Mail rounds only exchange with peers that just announced, so a quiet
        neighbour already in the registry would never share its peers.  This
        sweep covers them, least-recently-asked first, each at most once per
        _EXCHANGE_INTERVAL.
        """
        now = time.time()
        known = {p.destination_hash for p in self.registry.all()}
        self._exchanged_at = {
            key: value
            for key, value in self._exchanged_at.items()
            if key in known
        }
        due = [
            p
            for p in self.registry.all()
            if now - self._exchanged_at.get(p.destination_hash, 0.0)
            >= _EXCHANGE_INTERVAL
        ]
        due.sort(key=lambda p: self._exchanged_at.get(p.destination_hash, 0.0))
        for peer in due[:max_peers]:
            if self._stop_event.is_set():
                break
            self._exchanged_at[peer.destination_hash] = now
            try:
                self.exchange_peers(peer, timeout=_EXCHANGE_TIMEOUT)
            except Exception:  # pragma: no cover - network dependent
                log.debug("Peer exchange with %s failed", peer.hexhash, exc_info=True)

    def _mail_round_for_peer(self, peer: Peer) -> None:
        if not self._mail_lock.acquire(blocking=False):
            # Busy: queue the round for the next mail loop instead of
            # dropping it, so a long sweep can't starve a peer that just
            # announced itself.
            self._mail_pending.add(peer.hexhash)
            return
        try:
            self._flush_peer(peer, _MAIL_REQUEST_TIMEOUT, respect_backoff=False)
            self._collect_mail(peer, _MAIL_REQUEST_TIMEOUT)
        except Exception:  # pragma: no cover - network dependent
            log.debug("Mail round for %s failed", peer.hexhash, exc_info=True)
        finally:
            self._mail_lock.release()
        # Peer exchange runs after the mail lock is released: it is
        # best-effort gossip, and a slow peer must not hold up mail
        # delivery.  Stamping first also keeps the background sweep from
        # re-asking a peer this round just covered.
        self._exchanged_at[peer.destination_hash] = time.time()
        try:
            self.exchange_peers(peer, timeout=_EXCHANGE_TIMEOUT)
        except Exception:  # pragma: no cover - network dependent
            log.debug("Peer exchange with %s failed", peer.hexhash, exc_info=True)

    def flush_outbox(
        self,
        *,
        timeout: float = _MAIL_REQUEST_TIMEOUT,
        max_peers: Optional[int] = None,
        respect_backoff: bool = True,
        to: Optional[bytes] = None,
        message_id: Optional[str] = None,
        wait_for_lock: float = 0.0,
        deadline: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Try to move queued mail: direct delivery, deposits, collection.

        Returns a summary with ``delivered``, ``deposited`` and
        ``received`` counts, plus ``lock_busy`` — True when the sweep
        never ran because the mail lock stayed busy.  Safe to call any
        time the node is running; concurrent rounds are skipped rather
        than queued up.  With ``respect_backoff`` (the default) entries
        whose delivery backoff has not elapsed are left for a later
        round; pass ``False`` when the caller has fresh evidence the
        peer is reachable (an announce).  With ``to`` set, only that
        recipient is contacted — the other queued entries are left for
        a later round.  With ``message_id`` set, only that entry is
        attempted and nothing is deposited with the peer.  With
        ``wait_for_lock`` > 0, wait up to that many seconds for a
        concurrent mail round to finish instead of skipping the sweep;
        0 (the default) keeps the skip-when-busy behaviour, which is
        what the background loop wants.  A one-shot caller that must
        know the outcome waits; a background round never does.  With
        ``deadline`` (a ``time.monotonic()`` value) set, no new peer or
        request starts once it has passed, and each request is bounded
        by the time left before it.
        """
        summary = {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
            "receipts": 0,
            "lock_busy": False,
        }
        if not self.started:
            return summary
        if wait_for_lock > 0:
            acquired = self._mail_lock.acquire(timeout=wait_for_lock)
        else:
            acquired = self._mail_lock.acquire(blocking=False)
        if not acquired:
            summary["lock_busy"] = True
            return summary
        try:
            # A targeted flush shares the caller's deadline: a stalled
            # mailbox-lock holder must not hold the sweep past it.  A
            # background round (no deadline) keeps the unbounded waits.
            self.mailbox.prune(timeout=_remaining(deadline))
            summary["received"] += self._process_local_held()
            peers = self.registry.all()
            if to is not None:
                peers = [p for p in peers if p.destination_hash == to]
            # Peers we owe due mail to go first (stable sort keeps the
            # registry's most-recently-seen order inside each group), so
            # a capped sweep still spends its budget on actual delivery.
            # Backed-off recipients fall in with the rest: they still get
            # visited for deposits and collection when slots remain.
            # A backoff-ignoring round ranks every owed recipient first.
            if respect_backoff:
                targets = {
                    entry.to
                    for entry in self.mailbox.due_outbox(timeout=_remaining(deadline))
                }
            else:
                targets = {
                    entry.to
                    for entry in self.mailbox.outbox_list(timeout=_remaining(deadline))
                }
            # Peers owed queued read receipts are delivered to in the same
            # visit, so they are prioritised alongside message recipients.
            targets |= {entry.to for entry in self.mailbox.receipt_outbox_list()}
            # Peers heard only through exchange come after directly heard
            # ones inside each group, so second-hand entries cannot crowd
            # out reachable peers.
            peers.sort(
                key=lambda p: (p.destination_hash not in targets, p.via_exchange)
            )
            if max_peers is not None:
                peers = peers[:max_peers]
            for peer in peers:
                if _past(deadline):
                    break
                moved = self._flush_peer(
                    peer,
                    timeout,
                    respect_backoff=respect_backoff,
                    message_id=message_id,
                    deadline=deadline,
                )
                summary["delivered"] += moved["delivered"]
                summary["deposited"] += moved["deposited"]
                summary["receipts"] += moved.get("receipts", 0)
                if _past(deadline):
                    break
                summary["received"] += self._collect_mail(
                    peer, timeout, deadline=deadline
                )
        except TimeoutError:
            # A mailbox lock wait ran past the deadline: end the round
            # with what it moved so far; the entries stay queued for a
            # later round.
            log.debug("mailbox lock busy past deadline; ending flush round")
            return summary
        finally:
            self._mail_lock.release()
        # Receipts for messages a reader displayed but could not
        # receipt itself (a one-shot `tau messages` shares this
        # data directory with the serving node and cannot start a
        # second Reticulum instance on its listen port).  A targeted
        # retry touches only its own entry, so it skips this sweep.
        # Flushed after the mail lock is released: resolving an
        # unknown sender can wait seconds for discovery, and that
        # wait must not stall other mail rounds.
        if to is None and message_id is None:
            summary["receipts"] += self._flush_read_receipts()
        return summary

    def _flush_peer(
        self,
        peer: Peer,
        timeout: float,
        *,
        respect_backoff: bool = True,
        message_id: Optional[str] = None,
        deadline: Optional[float] = None,
    ) -> Dict[str, int]:
        moved = {"delivered": 0, "deposited": 0, "receipts": 0}
        if respect_backoff:
            due = self.mailbox.due_outbox(
                to=peer.destination_hash, timeout=_remaining(deadline)
            )
        else:
            due = self.mailbox.outbox_list(
                to=peer.destination_hash, timeout=_remaining(deadline)
            )
        if message_id is not None:
            due = [entry for entry in due if entry.id == message_id]
        if peer.caps and "mail" not in peer.caps:
            # A pre-mail peer can still receive direct messages, just not
            # hold or hand over sealed envelopes.
            for entry in due:
                if _past(deadline):
                    return moved
                if self._deliver_entry(peer, entry, timeout, deadline=deadline):
                    moved["delivered"] += 1
            if message_id is None:
                moved["receipts"] = self._flush_receipts(peer, timeout)
            return moved

        for entry in due:
            if _past(deadline):
                return moved
            if self._deliver_entry(peer, entry, timeout, deadline=deadline):
                moved["delivered"] += 1

        now = time.time()
        for entry in self.mailbox.outbox_list(timeout=_remaining(deadline)):
            if _past(deadline):
                return moved
            if entry.to == peer.destination_hash:
                continue
            if message_id is not None and entry.id != message_id:
                continue
            holders = {
                holder: deposited_at
                for holder, deposited_at in entry.held_by.items()
                if now - deposited_at < _HOLDER_RECORD_TTL
            }
            if (
                len(holders) >= MAX_HOLDERS_PER_MESSAGE
                and peer.hexhash not in holders
            ):
                continue
            last_deposit = holders.get(peer.hexhash)
            if last_deposit is not None and now - last_deposit < _DEPOSIT_RETRY_AFTER:
                continue
            if self._deposit_entry(peer, entry, timeout, deadline=deadline):
                moved["deposited"] += 1
        if message_id is None:
            moved["receipts"] = self._flush_receipts(peer, timeout)
        return moved

    def _still_queued(self, message_id: str, timeout: Optional[float] = None) -> bool:
        """True if the outbox still holds this entry, reloaded from disk.

        The sweep works from a snapshot, so a cancel can land after the
        snapshot was taken; each delivery attempt re-checks instead of
        trusting the snapshot.  A send already in flight cannot be
        recalled — cancelling only stops future attempts.  With
        ``timeout`` set, the wait for the mailbox lock is bounded by
        it: when the lock cannot be acquired in time the entry is
        treated as still queued, so a caller with its own deadline
        reports expiry instead of blocking past it.
        """
        try:
            with self.mailbox.transaction(timeout=timeout):
                return self.mailbox.get_outbox(message_id) is not None
        except TimeoutError:
            return True

    def _deliver_entry(
        self,
        peer: Peer,
        entry: Any,
        timeout: float,
        *,
        deadline: Optional[float] = None,
    ) -> bool:
        # The sweep lists the outbox before acting, so a message cancelled
        # by another process (``tau outbox cancel``) can still be sitting
        # in this pass's snapshot. Re-check the queue under the mailbox
        # lock — which reloads the on-disk state — before sending.  The
        # wait for it is bounded by the deadline, like every other lock
        # wait in a targeted flush: a stalled holder must not hold the
        # send past it.
        try:
            with self.mailbox.transaction(timeout=_remaining(deadline)):
                fresh = self.mailbox.get_outbox(entry.id)
                if fresh is None:
                    return False
                if fresh.expires_at <= time.time():
                    # Past its deadline (sender-set expiry or queue TTL) since
                    # the sweep listed it: the receiver would drop the message
                    # anyway, so do not spend a send on it.  The next prune
                    # removes the entry.
                    log.info(
                        "Skipping expired queued message %s to %s",
                        entry.id,
                        peer.hexhash,
                    )
                    return False
        except TimeoutError:
            return False
        payload = (
            encode_group_message(
                entry.text,
                message_id=entry.id,
                sent_at=entry.created_at,
                group_id=entry.group_id,
                group_name=entry.group_name,
                creator=entry.group_creator,
                members=entry.group_members,
            )
            if entry.group_id is not None
            else encode_message(
                entry.text,
                message_id=entry.id,
                sent_at=entry.created_at,
                expires_at=entry.message_expires_at,
                in_reply_to=entry.in_reply_to,
                kind=getattr(entry, "kind", None),
            )
        )
        if not self._still_queued(entry.id, timeout=_remaining(deadline)):
            return False
        via_lxmf = False
        try:
            self._request(
                peer,
                REQUEST_GROUP_MESSAGE
                if entry.group_id is not None
                else REQUEST_MESSAGE,
                payload,
                timeout,
                deadline=deadline,
            )
        except NotAMemberError as exc:
            # Permanent refusal: the recipient will not accept this roster,
            # so stop retrying rather than holding the leg until its TTL.
            log.warning("Dropping queued group message %s: %s", entry.id, exc)
            self.mailbox.remove_outbox(entry.id)
            return False
        except NodeError as exc:
            # An explicit refusal (allow-list, block-list) is final: falling
            # back to LXMF would deliver to the peer's separate destination
            # and bypass the policy that refused it.  Only transport
            # failures may try LXMF.
            if isinstance(exc, PeerRefusedError) or not self._still_queued(
                entry.id, timeout=_remaining(deadline)
            ):
                # A cancel may have landed while the direct request was in
                # flight; do not start a fresh send for a cancelled entry.
                try:
                    self.mailbox.record_attempt(
                        entry.id, error=str(exc), timeout=_remaining(deadline)
                    )
                except TimeoutError:
                    log.debug(
                        "Could not record failed delivery attempt for %s: "
                        "mailbox lock busy",
                        entry.id,
                    )
                return False
            if not self._deliver_via_lxmf(entry, timeout):
                try:
                    self.mailbox.record_attempt(
                        entry.id, error=str(exc), timeout=_remaining(deadline)
                    )
                except TimeoutError:
                    log.debug(
                        "Could not record failed delivery attempt for %s: "
                        "mailbox lock busy",
                        entry.id,
                    )
                return False
            via_lxmf = True
        # Record the confirmed delivery before the entry leaves the outbox,
        # so a waiter that sees the entry gone can tell it was delivered.
        self._delivered_ids.append(entry.id)
        try:
            self.mailbox.record_delivered(entry.id, timeout=_remaining(deadline))
            self.mailbox.remove_outbox(entry.id, timeout=_remaining(deadline))
        except TimeoutError:
            # The delivery is confirmed, so report success; the entry
            # stays queued and a later round re-delivers it (the peer
            # dedupes by message id) and retries the bookkeeping.
            log.warning(
                "Delivered queued message %s but the mailbox lock stayed "
                "busy; leaving it queued for a later round",
                entry.id,
            )
        log.info(
            "Delivered queued message %s to %s%s",
            entry.id,
            peer.hexhash,
            " over LXMF" if via_lxmf else "",
        )
        self.registry.record_delivered(peer.destination_hash)
        if self.on_delivered is not None:
            try:
                self.on_delivered(entry.id)
            except Exception:  # pragma: no cover - user callback
                log.warning("on_delivered callback failed", exc_info=True)
        return True

    def _deliver_via_lxmf(self, entry: Any, timeout: float) -> bool:
        """Try one queued entry over LXMF; True only on confirmation.

        The LXMF router must be running and the recipient must have a
        registered public key — without it no LXMF address can be
        derived.  Delivery is confirmed by the router's callback (the
        peer received it, or a propagation node accepted it); anything
        else leaves the entry queued for the next sweep.
        """
        if self.lxmf_router is None or self.identity is None:
            return False
        if getattr(entry, "group_id", None) is not None:
            # LXMF carries only plain text: a group message sent this
            # way would land in the recipient's DM inbox, not the
            # group.  Leave it queued for a direct retry instead.
            return False
        if entry.expires_at <= time.time() or (
            entry.message_expires_at is not None
            and entry.message_expires_at <= time.time()
        ):
            # The deadline passed while the direct request was in
            # flight. The LXMF receiver carries no deadline, so an
            # expired message must not go out over LXMF.
            log.debug(
                "Skipping LXMF send for expired queued message %s",
                entry.id,
            )
            return False
        recipient = self.registry.get(entry.to)
        if recipient is None or not recipient.public_key:
            log.debug(
                "No public key for %s; cannot address an LXMF message",
                entry.to_hex,
            )
            return False
        try:
            message = build_lxmf_message(
                self.identity, recipient.public_key, entry.text
            )
            return send_lxmf_message(self.lxmf_router, message, timeout=timeout)
        except (LXMFError, ValueError) as exc:
            log.debug(
                "Could not send queued message %s over LXMF: %s",
                entry.id,
                exc,
            )
            return False

    def _deposit_entry(
        self,
        peer: Peer,
        entry: Any,
        timeout: float,
        *,
        deadline: Optional[float] = None,
    ) -> bool:
        # Same re-check as _deliver_entry: do not leave a sealed copy
        # with a holder for a message cancelled since the sweep listed it.
        # The wait for the lock is bounded by the deadline, like every
        # other lock wait in a targeted flush.
        try:
            with self.mailbox.transaction(timeout=_remaining(deadline)):
                fresh = self.mailbox.get_outbox(entry.id)
                if fresh is None:
                    return False
                if fresh.expires_at <= time.time():
                    # Past its deadline since the sweep listed it: a holder
                    # would only ever hand the recipient a copy the receiver
                    # drops, so do not deposit it.  The next prune removes
                    # the entry.
                    log.info(
                        "Skipping expired queued message %s for deposit with %s",
                        entry.id,
                        peer.hexhash,
                    )
                    return False
        except TimeoutError:
            return False
        if self.identity is None:
            return False
        recipient = self.registry.get(entry.to)
        if recipient is None:
            # Without the recipient's public key the message cannot be
            # sealed for anyone else to hold; direct delivery only.
            return False
        try:
            blob = seal_message(
                self.identity,
                sender_name=self.name,
                recipient_public_key=recipient.public_key,
                message_id=entry.id,
                text=entry.text,
                sent_at=entry.created_at,
                group=(
                    {
                        "id": entry.group_id,
                        "name": entry.group_name,
                        "creator": entry.group_creator,
                        "members": entry.group_members,
                    }
                    if entry.group_id is not None
                    else None
                ),
                expires_at=entry.message_expires_at,
                in_reply_to=entry.in_reply_to,
                kind=getattr(entry, "kind", None),
            )
            payload = build_mail_store(
                to=entry.to_hex,
                message_id=entry.id,
                blob=blob,
                expires_at=entry.expires_at,
            )
        except (MailboxError, ProtocolError) as exc:
            log.debug("Could not seal queued message %s: %s", entry.id, exc)
            return False
        if not self._still_queued(entry.id, timeout=_remaining(deadline)):
            return False
        try:
            self._request(
                peer, REQUEST_MAIL_STORE, payload, timeout, deadline=deadline
            )
        except NodeError:
            return False
        try:
            self.mailbox.record_deposit(
                entry.id, peer.hexhash, timeout=_remaining(deadline)
            )
        except TimeoutError:
            # The sealed copy is with the holder; the deposit record
            # stays unrecorded, so a later round may deposit it again
            # (the holder dedupes by message id).
            log.warning(
                "Left a sealed copy of %s with %s but the mailbox lock "
                "stayed busy; the deposit is unrecorded",
                entry.id,
                peer.hexhash,
            )
        log.info(
            "Left a sealed copy of %s with %s for %s",
            entry.id,
            peer.hexhash,
            entry.to_hex,
        )
        return True

    def _flush_read_receipts(self) -> int:
        """Send receipts for inbox messages stamped read but unreceipted.

        A one-shot reader stamps ``read_at`` when it displays a DM;
        this sweep — run by the serving node's mail loop and by any
        one-shot flush — is what actually delivers those receipts.
        A sender that cannot be discovered gets its receipt queued by
        hash (see ``send_read_receipts``), so the ids are stamped and
        later sweeps do not repeat the discovery wait.  Returns how
        many receipts were sent or queued.
        """
        if not self.read_receipts_enabled:
            return 0
        if self.identity is None:
            return 0
        # The stamp may have been written by a one-shot `tau messages`
        # in another process; reload before deciding what is owed.
        try:
            self.inbox.reload()
        except Exception:
            log.debug("Could not reload inbox", exc_info=True)
            return 0
        try:
            pending = self.inbox.read_unreceipted()
        except Exception:
            log.debug("Could not list read messages", exc_info=True)
            return 0
        if not pending:
            return 0
        by_peer: Dict[str, List[str]] = {}
        read_times: Dict[str, float] = {}
        for entry in pending:
            peer_hash = str(entry.get("destination") or "")
            message_id = str(entry.get("id") or "")
            if not peer_hash or not message_id:
                continue
            ids = by_peer.setdefault(peer_hash, [])
            if message_id not in ids:
                ids.append(message_id)
            read_times[message_id] = float(entry.get("read_at") or time.time())
        dispatched = 0
        for peer_hash, ids in by_peer.items():
            read_at = max(read_times[mid] for mid in ids)
            try:
                result = self.send_read_receipts(
                    peer_hash, ids, read_at=read_at, timeout=_MAIL_REQUEST_TIMEOUT
                )
            except Exception:
                log.debug(
                    "Could not send read receipt to %s", peer_hash, exc_info=True
                )
                continue
            handled = result.get("ids") or []
            if handled:
                dispatched += 1
                try:
                    self.inbox.mark_receipt_sent(handled)
                except Exception:  # pragma: no cover - defensive
                    log.debug("Could not stamp read receipts", exc_info=True)
        return dispatched

    def _flush_receipts(self, peer: Peer, timeout: float) -> int:
        """Move queued read receipts for *peer*: direct, then deposits.

        Direct delivery uses the signed ``/tau/read`` request, which
        needs no mail capability — a pre-mail peer can still be told
        its messages were read.  Deposits (sealed copies left with a
        holder) need the mail capability, like message deposits, and
        are skipped for a recipient that advertises caps without
        ``read``: an old node would only drop the sealed receipt.
        """
        moved = 0
        # Queued receipts pause while the reader has opted out; they are
        # kept (not discarded) and resume if the setting is turned back on.
        if not self.read_receipts_enabled:
            return 0
        if peer.caps and "read" not in peer.caps:
            return 0
        for entry in self.mailbox.due_receipts(to=peer.destination_hash):
            if self._deliver_receipt_entry(peer, entry, timeout):
                moved += 1
        if peer.caps and "mail" not in peer.caps:
            return moved
        now = time.time()
        for entry in self.mailbox.receipt_outbox_list():
            if entry.to == peer.destination_hash:
                continue
            recipient = self.registry.get(entry.to)
            if recipient is not None and recipient.caps and "read" not in recipient.caps:
                continue
            holders = entry.held_by
            if (
                len(holders) >= MAX_HOLDERS_PER_MESSAGE
                and peer.hexhash not in holders
            ):
                continue
            last_deposit = holders.get(peer.hexhash)
            if last_deposit is not None and now - last_deposit < _DEPOSIT_RETRY_AFTER:
                continue
            if self._deposit_receipt_entry(peer, entry, timeout):
                moved += 1
        return moved

    def _deliver_receipt_entry(self, peer: Peer, entry: Any, timeout: float) -> bool:
        """Send one queued receipt direct over the link to its sender."""
        with self.mailbox.transaction():
            if self.mailbox.get_receipt_outbox(entry.id) is None:
                return False
        if self.identity is None:
            return False
        try:
            payload = build_read_receipt(
                self.identity,
                message_ids=list(entry.message_ids),
                read_at=entry.read_at,
            )
        except ProtocolError as exc:
            log.debug("Could not sign queued receipt %s: %s", entry.id, exc)
            return False
        try:
            self._request(peer, REQUEST_READ, payload, timeout)
        except NodeError as exc:
            self.mailbox.record_receipt_attempt(entry.id, error=str(exc))
            return False
        self.mailbox.remove_receipt_outbox(entry.id)
        log.info("Delivered read receipt %s to %s", entry.id, peer.hexhash)
        return True

    def _deposit_receipt_entry(self, peer: Peer, entry: Any, timeout: float) -> bool:
        """Leave a sealed copy of a receipt with *peer* for its sender."""
        with self.mailbox.transaction():
            if self.mailbox.get_receipt_outbox(entry.id) is None:
                return False
        if self.identity is None:
            return False
        recipient = self.registry.get(entry.to)
        if recipient is None:
            # Without the sender's public key the receipt cannot be
            # sealed for anyone else to hold; direct delivery only.
            return False
        try:
            blob = seal_receipt(
                self.identity,
                sender_name=self.name,
                recipient_public_key=recipient.public_key,
                receipt_id=entry.id,
                message_ids=list(entry.message_ids),
                read_at=entry.read_at,
            )
            payload = build_mail_store(
                to=entry.to_hex,
                message_id=entry.id,
                blob=blob,
                expires_at=entry.expires_at,
            )
        except (MailboxError, ProtocolError) as exc:
            log.debug("Could not seal queued receipt %s: %s", entry.id, exc)
            return False
        with self.mailbox.transaction():
            if self.mailbox.get_receipt_outbox(entry.id) is None:
                return False
        try:
            self._request(peer, REQUEST_MAIL_STORE, payload, timeout)
        except NodeError:
            return False
        self.mailbox.record_receipt_deposit(entry.id, peer.hexhash)
        log.info(
            "Left a sealed copy of receipt %s with %s for %s",
            entry.id,
            peer.hexhash,
            entry.to_hex,
        )
        return True

    def _collect_mail(
        self, peer: Peer, timeout: float, *, deadline: Optional[float] = None
    ) -> int:
        # Without a consumer, fetched mail would be marked seen and acked
        # while never shown to anyone, so leave it held for a live node.
        if self.on_message is None:
            return 0
        if self.identity is None or self.destination is None:
            return 0
        if peer.caps and "mail" not in peer.caps:
            return 0
        acks = self.mailbox.take_pending_acks(peer.hexhash)
        try:
            response = self._request(
                peer,
                REQUEST_MAIL_FETCH,
                build_mail_fetch(acks),
                timeout,
                deadline=deadline,
            )
        except NodeError:
            if acks:
                self.mailbox.add_pending_acks(peer.hexhash, acks)
            return 0

        received = 0
        new_acks: List[str] = []
        for item in response.get("messages") or []:
            if not isinstance(item, dict):
                continue
            message_id = item.get("id")
            blob_text = item.get("blob")
            if not isinstance(message_id, str) or not isinstance(blob_text, str):
                continue
            # Whatever happens next, this envelope has been handed over;
            # ack it so the holder can drop its copy.
            new_acks.append(message_id)
            try:
                blob = base64.b64decode(blob_text, validate=True)
                kind, payload = open_sealed_envelope(self.identity, blob)
            except (ValueError, MailboxError) as exc:
                log.warning("Dropping unreadable held mail %s: %s", message_id, exc)
                continue
            if kind == "sealed_receipt":
                # A receipt is not a message: record the reads and move
                # on.  The envelope is already acked above, and a
                # receipt is never receipted itself.
                self._record_receipt(
                    reader_hash=payload["from"],
                    message_ids=payload["ids"],
                    read_at=payload["read_at"],
                )
                continue
            message = payload
            if self._deliver_collected(message):
                received += 1
        if new_acks:
            self.mailbox.add_pending_acks(peer.hexhash, new_acks)
        return received

    def _process_local_held(self) -> int:
        """Deliver envelopes addressed to this node that it holds itself."""
        if self.on_message is None:
            return 0
        if self.identity is None or self.destination is None:
            return 0
        received = 0
        for envelope in self.mailbox.held_for(self.destination.hash):
            try:
                kind, payload = open_sealed_envelope(self.identity, envelope.blob)
            except MailboxError as exc:
                log.warning(
                    "Dropping unreadable held mail %s: %s", envelope.id, exc
                )
                payload = None
                kind = None
            still_held = self.mailbox.drop_held(
                envelope.id, to=self.destination.hash
            )
            if not still_held:
                # Dropped by the owner (tau outbox drop-held) after this
                # sweep listed it: do not surface a copy we no longer hold.
                continue
            if kind == "sealed_receipt" and payload is not None:
                self._record_receipt(
                    reader_hash=payload["from"],
                    message_ids=payload["ids"],
                    read_at=payload["read_at"],
                )
                continue
            if kind != "sealed_message" or payload is None:
                continue
            if self._deliver_collected(payload):
                received += 1
        return received

    def _deliver_collected(self, message: Dict[str, Any]) -> bool:
        """Hand a decrypted relayed message to the user, at most once."""
        sender = message.get("from")
        if self.blocklist.is_blocked(
            sender if isinstance(sender, str) else None
        ):
            # Same silent drop as a direct message, checked before the
            # allow-list.  Do not mark it seen: another holder's copy
            # may still be delivered if the sender is unblocked later.
            log.info(
                "Relayed message from %s denied by the block-list",
                sender or "unknown",
            )
            return False
        if not self.allowlist.is_allowed(
            sender if isinstance(sender, str) else None
        ):
            # The sender hash in a sealed envelope is authenticated
            # (open_message verifies it against the signing key), so
            # this is the same admission decision as a direct message.
            # Do not mark it seen: another holder's copy may still be
            # delivered if the sender is admitted later.
            log.info(
                "Relayed message from %s denied by the DM allow-list",
                sender or "unknown",
            )
            return False
        if self.mailbox.is_seen(message["id"]):
            return False
        expires_at = message.get("expires_at")
        if expires_at is not None and expires_at <= time.time():
            # Past the sender's deadline: do not record or show it.
            log.info("Dropping expired relayed message %s", message["id"])
            return False
        group = message.get("group")
        pinned: Optional[Dict[str, Any]] = None
        if group is not None:
            sender = message.get("from")
            pinned = self._accept_group_message(
                sender if isinstance(sender, str) else "",
                group,
                sent_at=message.get("sent_at"),
            )
            if pinned is None:
                log.info(
                    "Relayed group message from %s rejected: sender or "
                    "node not on the roster, or the roster changed",
                    sender or "unknown",
                )
                return False
        record = {
            "from": message["from_name"],
            "destination": message["from"],
            "text": message["text"],
            "kind": message.get("kind") or DEFAULT_MESSAGE_KIND,
            "sent_at": message["sent_at"],
            "received_at": time.time(),
            "id": message["id"],
            "relayed": True,
            "expires_at": message.get("expires_at"),
            "direction": "in",
        }
        if pinned is not None:
            record["kind"] = "group"
            record["group"] = pinned["id"]
            record["group_name"] = pinned["name"]
        if message.get("in_reply_to") is not None:
            record["in_reply_to"] = message["in_reply_to"]
        log.info(
            "Message from %s (%s) via store-and-forward: %s",
            message["from_name"],
            message["from"],
            message["text"],
        )
        # One delivery: mark seen and record under the mailbox
        # transaction, so a live backup never restores a seen marker
        # with no inbox entry (see _handle_message).  Re-check inside
        # the transaction, as there: a duplicate that landed while
        # this one was in flight already marked the id seen.
        recorded = False
        with self.mailbox.transaction():
            if not self.mailbox.is_seen(message["id"]):
                self.mailbox.mark_seen(message["id"])
                self._record_received(record)
                recorded = True
        if recorded and self.on_message is not None:
            try:
                self.on_message(record)
            except Exception:  # pragma: no cover - user callback
                log.warning("on_message callback failed", exc_info=True)
        return recorded

    # -- application layer -------------------------------------------------

    def add_request_handler(self, path: str, handler: Any) -> None:
        """Register an extra request handler on this node's destination."""
        if self.destination is None:
            raise NodeError("node has not been started")
        if not isinstance(path, str) or not path.startswith("/"):
            raise NodeError("request path must start with '/'")
        self.destination.register_request_handler(
            path, handler, allow=RNS.Destination.ALLOW_ALL
        )

    def attach_app(self, app: Any) -> Any:
        """Attach an application layer such as ``TanApp`` to this node."""
        if self.destination is None:
            raise NodeError("node has not been started")
        if self.app is not None:
            raise NodeError("an application layer is already attached")
        app.attach(self)
        self.app = app
        return app

    # -- outgoing requests -------------------------------------------------

    def info(self) -> Dict[str, Any]:
        """Return a description of this node."""
        if self.destination is None:
            raise NodeError("node has not been started")
        interfaces: List[Dict[str, Any]] = []
        try:
            interfaces = [
                {
                    "name": getattr(interface, "name", "unknown"),
                    "online": bool(getattr(interface, "online", False)),
                }
                for interface in RNS.Transport.interfaces[:16]
            ]
        except Exception:  # pragma: no cover - introspection only
            pass
        lxmf_delivery = lxmf_router_delivery_hash(self.lxmf_router)
        lxmf_propagation = lxmf_router_propagation_hash(self.lxmf_router)
        return {
            "name": self.name,
            "destination": self.destination.hexhash,
            "protocol": PROTOCOL_VERSION,
            "version": __version__,
            "rns_version": getattr(RNS, "__version__", "unknown"),
            "uptime": round(time.time() - (self._started_at or time.time()), 1),
            "peers": len(self.registry),
            "mailbox": self.mailbox.counts(),
            "interfaces": interfaces,
            "lxmf": {
                "enabled": self.lxmf_router is not None,
                "delivery": lxmf_delivery.hex() if lxmf_delivery else None,
                "propagation": (
                    lxmf_propagation.hex() if lxmf_propagation else None
                ),
            },
        }

    def resolve_peer(
        self, target: Union[str, bytes, Peer], *, timeout: float = 10.0
    ) -> Peer:
        """Find a peer by record, destination hash, prefix or name.

        Technologies like Reticulum are path based: if only a destination hash
        is known, a path request is sent and the call waits for the announce.
        """
        if isinstance(target, Peer):
            return target

        try:
            peer = self.registry.resolve(target)
        except PeerError:
            raise
        if peer is not None:
            return peer

        if self.destination is None:
            raise NodeError(f"unknown peer {target!r} (node has not been started)")

        if isinstance(target, bytes):
            dest_hash = target
        elif isinstance(target, str) and len(target.strip()) == 32:
            try:
                dest_hash = bytes.fromhex(target.strip())
            except ValueError as exc:
                raise NodeError(f"invalid destination hash: {target!r}") from exc
        else:
            raise NodeError(f"unknown peer: {target!r}")

        return self.wait_for_peer(dest_hash, timeout=timeout)

    def wait_for_peer(self, dest_hash: bytes, *, timeout: float = 10.0) -> Peer:
        """Wait for an announce that reveals the identity behind a hash."""
        if len(dest_hash) != 16:
            raise NodeError("destination hash must be 16 bytes")
        deadline = time.time() + max(0.0, timeout)
        try:
            RNS.Transport.request_path(dest_hash)
        except Exception:  # pragma: no cover - network dependent
            log.debug("Path request failed", exc_info=True)

        while time.time() < deadline:
            peer = self.registry.get(dest_hash)
            if peer is not None:
                return peer
            identity = RNS.Identity.recall(dest_hash)
            if identity is not None:
                if tau_destination_hash(identity) != dest_hash:
                    raise NodeError("announced identity does not match destination")
                info = parse_announce(getattr(identity, "app_data", None))
                return self.registry.upsert(
                    dest_hash,
                    identity.get_public_key(),
                    name=info["name"],
                    protocol=info["v"],
                    caps=info["caps"],
                )
            time.sleep(0.1)

        raise NodeError(
            "peer did not announce within "
            f"{timeout:.1f}s; check the interface and announce interval"
        )

    def _queue_read_receipts(
        self, dest_hash: bytes, message_ids: List[str], read_at: float
    ) -> List[Any]:
        """Queue receipts for *dest_hash*, at most MAX_READ_IDS per entry.

        ``Mailbox.queue_receipt`` (and the wire payload) reject batches
        over 100 ids, so a longer list is split here rather than left
        unreceipted.
        """
        entries = []
        for start in range(0, len(message_ids), MAX_READ_IDS):
            entries.append(
                self.mailbox.queue_receipt(
                    to=dest_hash,
                    message_ids=message_ids[start : start + MAX_READ_IDS],
                    read_at=read_at,
                )
            )
        return entries

    def send_read_receipts(
        self,
        target: Union[str, bytes, Peer],
        message_ids: List[str],
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
        read_at: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Tell *target* that its messages were read.

        One signed receipt covers up to ``MAX_READ_IDS`` message ids;
        a longer list is split into one receipt per batch, sent direct
        or queued per batch.  When the sender is reachable the receipt
        goes direct over the link; otherwise it is queued in the
        receipts outbox and rides store-and-forward like a message —
        including when the sender cannot even be discovered: a known
        destination hash is enough to queue by, so a failed discovery
        neither drops the receipt nor repeats on every sweep.  A node
        that opted out never sends receipts (returns
        ``{"ok": False, "disabled": True}``).  Receipts are never
        receipted themselves.
        """
        if not self.started:
            raise NodeError("node has not been started")
        if self.identity is None:
            raise NodeError("node has not been started")
        if not self.read_receipts_enabled:
            return {"ok": False, "disabled": True, "ids": []}
        ids = []
        for message_id in message_ids:
            if isinstance(message_id, str) and message_id and message_id not in ids:
                ids.append(message_id)
        if not ids:
            return {"ok": True, "ids": [], "recorded": []}
        try:
            peer = self.resolve_peer(
                target,
                timeout=discover_timeout
                if discover_timeout is not None
                else min(timeout, 10.0),
            )
        except (NodeError, PeerError):
            # Discovery failed: queue by destination hash so
            # store-and-forward can deliver the receipt when the
            # sender is reachable, instead of dropping it and waiting
            # out discovery again on the next sweep.
            dest_hash = self._target_hash(target)
            if dest_hash is None:
                raise
            stamp = read_at if isinstance(read_at, (int, float)) else time.time()
            entries = self._queue_read_receipts(dest_hash, ids, stamp)
            return {
                "ok": True,
                "queued": True,
                "receipt_id": entries[0].id,
                "receipt_ids": [entry.id for entry in entries],
                "ids": ids,
            }
        stamp = read_at if isinstance(read_at, (int, float)) else time.time()
        if len(ids) > MAX_READ_IDS:
            # One receipt names at most MAX_READ_IDS messages; send the
            # rest as further receipts.  A failed chunk is left out of
            # "ids" so the caller retries only that chunk later.
            handled: List[str] = []
            recorded: List[Any] = []
            ok = True
            for start in range(0, len(ids), MAX_READ_IDS):
                chunk = ids[start : start + MAX_READ_IDS]
                try:
                    result = self.send_read_receipts(
                        peer,
                        chunk,
                        timeout=timeout,
                        read_at=stamp,
                    )
                except Exception:
                    log.debug("Could not send a read receipt chunk", exc_info=True)
                    ok = False
                    continue
                handled.extend(result.get("ids") or [])
                recorded.extend(result.get("recorded") or [])
                ok = ok and bool(result.get("ok"))
            return {"ok": ok, "ids": handled, "recorded": recorded}
        if peer.caps and "read" not in peer.caps:
            # An old peer cannot consume a receipt, and a receipt it can
            # never read would only occupy a queue slot for its whole TTL.
            # Nothing is queued; the messages are treated as handled.
            return {"ok": True, "skipped": True, "ids": ids, "recorded": []}
        try:
            payload = build_read_receipt(
                self.identity, message_ids=ids, read_at=stamp
            )
            response = self._request(peer, REQUEST_READ, payload, timeout)
        except (NodeError, ProtocolError):
            entry = self.mailbox.queue_receipt(
                to=peer.destination_hash, message_ids=ids, read_at=stamp
            )
            return {"ok": True, "queued": True, "receipt_id": entry.id, "ids": ids}
        return {
            "ok": True,
            "ids": ids,
            "recorded": response.get("recorded", []),
        }

    def send_message(
        self,
        target: Union[str, bytes, Peer],
        text: str,
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
        queue: bool = True,
        expires_at: Optional[float] = None,
        in_reply_to: Optional[Dict[str, Any]] = None,
        kind: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a text message to a peer and return the decoded response.

        With ``queue`` enabled (the default), a message whose peer cannot
        be reached is stored in the local outbox instead of being lost:
        the returned dict has ``queued: True`` and the message id, and
        delivery is retried automatically whenever the peer — or any peer
        willing to hold a sealed copy — is heard from.  ``kind`` tags the
        message (``chat`` by default); it travels on the wire, survives
        queueing, and is stored in the recipient's inbox.

        ``expires_at`` is an optional sender-set expiry (epoch seconds):
        it travels in the message payload and inside sealed relay
        envelopes, the receiver purges the message once it passes, and
        a queued copy expires at the same deadline.  Expiry is
        cooperative — a modified client can keep copies — so this is
        hygiene, not a guarantee.

        ``in_reply_to`` quotes an earlier message in the same
        conversation: ``{"id": <wire id>, "excerpt": <short hash>}``.
        The reference travels the direct and the store-and-forward
        paths alike, and the sent message is recorded in the inbox so
        the peer's reply to it can be rendered from local history.
        """
        normalized = normalize_message(text)
        reply_to = clean_in_reply_to(in_reply_to)
        clean_kind = normalize_message_kind(kind)
        message_id = generate_message_id()
        payload = encode_message(
            normalized,
            message_id=message_id,
            kind=clean_kind,
            expires_at=expires_at,
            in_reply_to=reply_to,
        )
        peer: Optional[Peer] = None
        try:
            peer = self.resolve_peer(
                target,
                timeout=discover_timeout
                if discover_timeout is not None
                else min(timeout, 10.0),
            )
            # Remember the send (and its recipient) before the request: the
            # recipient's message callback can send a read receipt back
            # before this request returns, and that receipt must already
            # verify.  A refused or failed send is forgotten below.
            self.mailbox.record_sent(
                message_id,
                to=peer.destination_hash,
                text=normalized,
            )
            response = self._request(peer, REQUEST_MESSAGE, payload, timeout)
            self.mailbox.record_delivered(message_id)
            if isinstance(response, dict):
                # Only this node's own outbox may report a message as
                # queued, with a real local entry id. A peer that claims
                # queued (with an arbitrary id, or none at all) must not
                # be trusted: both consumers read response["queued"]/
                # response["id"] verbatim, so a spoofed claim misreports
                # delivery in the console and can make the CLI raise
                # KeyError on the missing id. Always name the outbound
                # wire message with the local id, never one the peer
                # supplied, so callers (console history, JSON output)
                # match what was actually sent and recorded — delivery
                # reports and reactions key off this id.
                response["id"] = message_id
                response.pop("expires_at", None)
                response.pop("to", None)
                response["queued"] = False
            self._record_sent(
                {
                    "from": self.name,
                    "destination": peer.hexhash,
                    "text": normalized,
                    "kind": clean_kind,
                    "sent_at": time.time(),
                    "received_at": time.time(),
                    "id": message_id,
                    "direction": "out",
                    **({"in_reply_to": reply_to} if reply_to is not None else {}),
                }
            )
            if self.registry.get(peer.destination_hash) is None:
                # A caller can hand send_message a Peer that never entered
                # the registry (resolve_peer returns Peer objects as-is);
                # a peer we just reached is a known peer, so record it
                # before stamping the delivery — otherwise the history
                # is silently lost and the peer never earns its quota tier.
                self.registry.upsert(
                    peer.destination_hash, peer.public_key, name=peer.name
                )
            self.registry.record_delivered(peer.destination_hash)
            return response
        except NotAllowedError:
            # The peer is online and explicitly refused the message;
            # queueing it would retry a refusal until its TTL expires.
            if peer is not None:
                self.mailbox.discard_sent(message_id)
            raise
        except NodeError:
            if not queue or not self.started:
                if peer is not None:
                    self.mailbox.discard_sent(message_id)
                raise
            dest_hash = (
                peer.destination_hash
                if peer is not None
                else self._target_hash(target)
            )
            if dest_hash is None:
                if peer is not None:
                    self.mailbox.discard_sent(message_id)
                raise
            entry = self.mailbox.enqueue(
                to=dest_hash,
                text=normalized,
                to_name=peer.display_name if peer is not None else "",
                message_id=message_id,
                expires_at=expires_at,
                in_reply_to=reply_to,
                kind=clean_kind,
            )
            self._record_sent(
                {
                    "from": self.name,
                    "destination": dest_hash.hex(),
                    "text": normalized,
                    "kind": clean_kind,
                    "sent_at": time.time(),
                    "received_at": time.time(),
                    "id": message_id,
                    "direction": "out",
                    **({"in_reply_to": reply_to} if reply_to is not None else {}),
                }
            )
            self.mailbox.record_sent(
                entry.id,
                to=dest_hash,
                text=normalized,
                sent_at=entry.created_at,
            )
            log.info(
                "Peer %s is unreachable; queued message %s in the outbox",
                dest_hash.hex(),
                entry.id,
            )
            threading.Thread(
                target=self.flush_outbox,
                kwargs={"timeout": _MAIL_REQUEST_TIMEOUT, "max_peers": 5},
                name="tau-mail-flush",
                daemon=True,
            ).start()
            return {
                "ok": True,
                "v": PROTOCOL_VERSION,
                "queued": True,
                "id": entry.id,
                "to": entry.to_hex,
                "expires_at": entry.expires_at,
            }

    def send_group_message(
        self,
        group: Union[str, Dict[str, Any]],
        text: str,
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
        queue: bool = True,
    ) -> Dict[str, Any]:
        """Fan *text* out to every member of a group except this node.

        *group* is a group id, id prefix, name, or group record.  Each
        leg is a ``/tau/group`` request carrying the roster; a member
        that cannot be reached is queued in the outbox (with the group
        context, so a retry re-sends the group payload) unless it
        explicitly refused the message.  Each leg is bounded by
        ``timeout``: the stages of one delivery share that leg's
        budget, and a leg the deadline leaves no time to attempt is
        queued the same way instead of dropped.  Returns a
        per-member result list: ``delivered``, ``queued``, or an
        ``error`` for that leg.
        """
        if isinstance(group, str):
            group = self.groups.find(group)
        creator = group.get("creator") or ""
        if not creator:
            raise NodeError(
                "group record has no creator; delete it and re-create it"
            )
        normalized = normalize_message(text)
        own_hash = self.destination_hexhash
        members = [m for m in group["members"] if m != own_hash]
        context = {
            "id": group["id"],
            "name": group["name"],
            "creator": creator,
            "members": list(group["members"]),
        }
        results: List[Dict[str, Any]] = []
        any_queued = False
        # Check the full group request size once, before any leg is sent
        # or queued, so an oversized message fails cleanly up front rather
        # than aborting the fan-out partway through.
        try:
            encode_group_message(
                normalized,
                message_id=generate_message_id(),
                group_id=group["id"],
                group_name=group["name"],
                creator=creator,
                members=list(group["members"]),
            )
        except ProtocolError as exc:
            raise NodeError(
                f"group message is too large to send with its roster ({exc}); "
                "shorten the message"
            ) from exc
        for member in members:
            message_id = generate_message_id()
            payload = encode_group_message(
                normalized,
                message_id=message_id,
                group_id=group["id"],
                group_name=group["name"],
                creator=creator,
                members=list(group["members"]),
            )
            peer: Optional[Peer] = None
            # One deadline bounds each leg, and the budget is per leg:
            # the stages of a single delivery (resolve, path, link,
            # response) share the leg's budget instead of each taking
            # the full timeout, but one member's leg must not consume
            # another's.  The roster is in sorted-hash order, so who
            # runs first is arbitrary — with a single fan-out-wide
            # deadline, an offline member sorted first burned the whole
            # budget waiting for a path that could never appear, and
            # every later (reachable) member was queued without an
            # attempt.  A per-leg deadline reserves the full attempt
            # window for every member while keeping each leg capped at
            # ``timeout``.
            deadline = time.monotonic() + timeout
            budget = _remaining(deadline) or 0.0
            if budget <= 0.0:
                # Out of time for another attempt — but the leg must
                # not be dropped to the deadline: queueing is local
                # disk I/O, so the member still gets the message on a
                # later retry, exactly as if the attempt had failed to
                # reach them.
                if not queue or not self.started:
                    results.append(
                        {
                            "to": member,
                            "to_name": "",
                            "ok": False,
                            "error": "not attempted: group send time limit reached",
                            "id": message_id,
                        }
                    )
                    continue
                try:
                    entry = self.mailbox.enqueue(
                        to=bytes.fromhex(member),
                        text=normalized,
                        to_name="",
                        message_id=message_id,
                        group=context,
                    )
                except MailboxError as queue_exc:
                    results.append(
                        {
                            "to": member,
                            "to_name": "",
                            "ok": False,
                            "error": f"could not queue: {queue_exc}",
                            "id": message_id,
                        }
                    )
                    continue
                self.mailbox.record_sent(
                    entry.id,
                    to=bytes.fromhex(member),
                    text=normalized,
                    sent_at=entry.created_at,
                )
                any_queued = True
                results.append(
                    {
                        "to": member,
                        "to_name": "",
                        "queued": True,
                        "id": entry.id,
                        "expires_at": entry.expires_at,
                    }
                )
                continue
            try:
                peer = self.resolve_peer(
                    member,
                    timeout=min(
                        discover_timeout
                        if discover_timeout is not None
                        else min(timeout, 10.0),
                        budget,
                    ),
                )
                # Remember the leg before the request so a read receipt
                # from this member verifies; a refused or unqueued leg
                # is forgotten below.
                self.mailbox.record_sent(
                    message_id,
                    to=bytes.fromhex(member),
                    text=normalized,
                )
                # Pass the leg's deadline through: without it, path
                # discovery, link establishment and the response could
                # each consume the leg's budget on a slow link, and
                # one leg could take close to three timeouts.
                response = self._request(
                    peer,
                    REQUEST_GROUP_MESSAGE,
                    payload,
                    _remaining(deadline) or 0.0,
                    deadline=deadline,
                )
                self.mailbox.record_delivered(message_id)
                results.append(
                    {
                        "to": member,
                        "to_name": peer.display_name,
                        "delivered": True,
                        "id": message_id,
                        "rtt": response.get("_rtt"),
                    }
                )
            except (NotAllowedError, NotAMemberError) as exc:
                # The member is online and explicitly refused; queueing
                # would retry a refusal until its TTL expires.
                self.mailbox.discard_sent(message_id)
                results.append(
                    {
                        "to": member,
                        "to_name": peer.display_name if peer is not None else "",
                        "ok": False,
                        "error": "not_allowed"
                        if isinstance(exc, NotAllowedError)
                        else "not_a_member",
                        "id": message_id,
                    }
                )
            except NodeError as exc:
                if not queue or not self.started:
                    self.mailbox.discard_sent(message_id)
                    results.append(
                        {
                            "to": member,
                            "to_name": peer.display_name if peer is not None else "",
                            "ok": False,
                            "error": str(exc),
                            "id": message_id,
                        }
                    )
                    continue
                try:
                    entry = self.mailbox.enqueue(
                        to=bytes.fromhex(member),
                        text=normalized,
                        to_name=peer.display_name if peer is not None else "",
                        message_id=message_id,
                        group=context,
                    )
                except MailboxError as queue_exc:
                    # The outbox is full (or the write failed): record the
                    # leg as failed and keep going, so the legs already
                    # delivered or queued are not lost with the exception.
                    self.mailbox.discard_sent(message_id)
                    results.append(
                        {
                            "to": member,
                            "to_name": peer.display_name if peer is not None else "",
                            "ok": False,
                            "error": f"could not queue: {queue_exc}",
                            "id": message_id,
                        }
                    )
                    continue
                # A queued leg is registered under its outbox id, like
                # a queued peer DM, so a receipt for it verifies. When
                # resolve already succeeded the wire id was registered
                # above (the outbox reuses it as its id, so this is a
                # no-op); when resolve failed, this is the leg's only
                # record, and without it the receipt would be rejected
                # as unknown.
                self.mailbox.record_sent(
                    entry.id,
                    to=bytes.fromhex(member),
                    text=normalized,
                    sent_at=entry.created_at,
                )
                any_queued = True
                log.info(
                    "Member %s is unreachable; queued group message %s "
                    "in the outbox",
                    member,
                    entry.id,
                )
                results.append(
                    {
                        "to": member,
                        "to_name": entry.to_name,
                        "queued": True,
                        "id": entry.id,
                        "expires_at": entry.expires_at,
                    }
                )
        if any_queued:
            threading.Thread(
                target=self.flush_outbox,
                kwargs={"timeout": _MAIL_REQUEST_TIMEOUT, "max_peers": 5},
                name="tau-mail-flush",
                daemon=True,
            ).start()
        # Aggregate success only when every leg was delivered or queued;
        # an explicit refusal or unqueued failure is a failed fan-out.
        all_ok = all(r.get("delivered") or r.get("queued") for r in results)
        return {
            "ok": all_ok,
            "v": PROTOCOL_VERSION,
            "group": group["id"],
            "name": group["name"],
            "results": results,
        }

    def send_file(
        self,
        target: Union[str, bytes, Peer],
        path: Union[str, Path],
        *,
        timeout: float = _FILE_TRANSFER_TIMEOUT,
        discover_timeout: Optional[float] = None,
        progress_callback: Optional[Callable[[float], None]] = None,
    ) -> Dict[str, Any]:
        """Send a small file to a peer over an RNS Resource.

        The receiver first gets a signed ``/tau/file`` offer (name,
        size, SHA-256) and may refuse it — a refused or unreachable
        peer raises instead of queueing, because files are direct
        links only in v1 (no store-and-forward).  Once accepted, the
        bytes travel as a Resource on the same link; the receiver
        verifies the hash before writing anything to disk.
        """
        source = Path(path).expanduser()
        if not source.is_file():
            raise NodeError(f"no such file: {source}")
        try:
            # Read at most one byte past the limit so an oversized (or
            # growing) file is rejected without loading it all.
            with source.open("rb") as handle:
                data = handle.read(MAX_FILE_BYTES + 1)
        except OSError as exc:
            raise NodeError(f"could not read {source}: {exc}") from exc
        if len(data) < 1 or len(data) > MAX_FILE_BYTES:
            raise NodeError(
                f"file of {len(data)} bytes is outside the 1..{MAX_FILE_BYTES} "
                "byte limit"
            )
        name = sanitize_file_name(source.name)
        if not name:
            raise NodeError(f"file name {source.name!r} is not usable")
        if self.identity is None:
            raise NodeError("node has not been started")
        transfer_id = generate_message_id()
        digest = hashlib.sha256(data).hexdigest()
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        # The offer names its recipient, so a captured offer cannot be
        # replayed to a third node: address it to this peer.
        offer = build_file_offer(
            self.identity,
            transfer_id=transfer_id,
            to=peer.destination_hash.hex(),
            name=name,
            size=len(data),
            sha256=digest,
        )
        link = self._open_link(peer, min(timeout, 30.0))
        try:
            # The offer request runs on this link, and the Resource
            # follows on the same link — the receiver's resource
            # callback matches the advertisement against this offer.
            self._request_on_link(link, REQUEST_FILE, offer, 20.0)
            concluded = threading.Event()
            state: Dict[str, Any] = {}

            def on_concluded(resource: Any) -> None:
                state["status"] = getattr(resource, "status", None)
                concluded.set()

            def on_progress(resource: Any) -> None:
                if progress_callback is None:
                    return
                try:
                    progress_callback(float(resource.get_progress()))
                except Exception:  # pragma: no cover - user callback
                    log.debug("file progress callback failed", exc_info=True)

            resource = RNS.Resource(
                data,
                link,
                metadata=transfer_id,
                advertise=False,
                callback=on_concluded,
                progress_callback=on_progress,
            )
            # Advertise exactly once: the constructor would also
            # advertise by default, and a second advertise job
            # corrupts the transfer's timing and cancels it.
            resource.advertise()
            if not concluded.wait(timeout):
                try:
                    resource.cancel()
                except Exception:  # pragma: no cover - defensive
                    pass
                raise NodeError(
                    f"file transfer {transfer_id[:8]} did not finish "
                    f"within {timeout:.0f}s"
                )
            if state.get("status") != RNS.Resource.COMPLETE:
                raise NodeError(
                    f"file transfer {transfer_id[:8]} failed "
                    f"(status={state.get('status')})"
                )
            return {
                "ok": True,
                "v": PROTOCOL_VERSION,
                "id": transfer_id,
                "name": name,
                "size": len(data),
                "sha256": digest,
                "to": peer.hexhash,
            }
        finally:
            try:
                link.teardown()
            except Exception:  # pragma: no cover - defensive
                pass

    def send_reaction(
        self,
        target: Union[str, bytes, Peer],
        message_id: str,
        reaction: str,
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Send a reaction to a peer's DM and return the decoded response.

        ``message_id`` names the reacted-to message, ``reaction`` is one
        emoji or short text token (``""`` retracts the sender's reaction
        to that message).  Reactions are their own signed message kind:
        the payload is signed with this node's Reticulum identity and
        verified by the receiver against the link identity, exactly like
        a sealed mail envelope — and the receiver resolves state per
        (message, reactor) with the latest reaction winning.

        Reactions are delivered live in v1: unlike ``send_message``
        there is no outbox queue, because the sealed-mail path is
        text-shaped throughout.  An unreachable peer raises
        :class:`NodeError`; queueing reactions for offline peers is a
        follow-up once the mailbox can carry non-text kinds.
        """
        token = clean_reaction(reaction)
        target_id = message_id.strip() if isinstance(message_id, str) else ""
        if not target_id or len(target_id) > 64:
            raise NodeError("reaction target must be a message id of 1-64 characters")
        if self.identity is None:
            raise NodeError("node has not been started")
        reaction_id = generate_message_id()
        payload = reaction_dict(target_id, token, message_id=reaction_id)
        canonical = encode(payload, max_bytes=MAX_REQUEST_BYTES)
        signature = self.identity.sign(canonical)
        if signature is None:  # pragma: no cover - identity without private keys
            raise NodeError("could not sign reaction")
        payload["sig"] = base64.b64encode(signature).decode("ascii")
        payload_bytes = encode(payload, max_bytes=MAX_REQUEST_BYTES)
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        response = self._request(peer, REQUEST_REACT, payload_bytes, timeout)
        # Record the outgoing reaction locally, so the reactor sees it
        # under the message in their own inbox and console.
        record = {
            "target": target_id,
            "reaction": token,
            "from": self.name,
            "destination": self.destination_hexhash
            if self.destination is not None
            else "",
            "sent_at": time.time(),
            "received_at": time.time(),
            "id": reaction_id,
        }
        self._record_reaction(record)
        return dict(response, id=reaction_id)

    def send_retraction(
        self,
        target: Union[str, bytes, Peer],
        message_id: str,
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Ask a peer to delete one of this node's sent DMs, and return
        the decoded response.

        ``message_id`` names the sent message.  The retract is its own
        signed message kind: the payload is signed with this node's
        Reticulum identity and verified by the receiver against the link
        identity, exactly like a reaction — and the receiver only deletes
        a message this node actually sent.  A retract for an unknown id
        answers ok with ``deleted`` False, so retries are safe.

        Retracts are delivered live in v1: unlike ``send_message``
        there is no outbox queue, because the sealed-mail path is
        text-shaped throughout.  An unreachable peer raises
        :class:`NodeError`; queueing retracts for offline peers is a
        follow-up once the mailbox can carry non-text kinds.
        """
        target_id = message_id.strip() if isinstance(message_id, str) else ""
        if not target_id or len(target_id) > 64:
            raise NodeError("retract target must be a message id of 1-64 characters")
        if self.identity is None:
            raise NodeError("node has not been started")
        retract_id = generate_message_id()
        payload = retract_dict(target_id, message_id=retract_id)
        canonical = encode(payload, max_bytes=MAX_REQUEST_BYTES)
        signature = self.identity.sign(canonical)
        if signature is None:  # pragma: no cover - identity without private keys
            raise NodeError("could not sign retraction")
        payload["sig"] = base64.b64encode(signature).decode("ascii")
        payload_bytes = encode(payload, max_bytes=MAX_REQUEST_BYTES)
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        response = self._request(peer, REQUEST_RETRACT, payload_bytes, timeout)
        return dict(response, id=retract_id)


    def send_edit(
        self,
        target: Union[str, bytes, Peer],
        message_id: str,
        text: str,
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Send an edit for a DM this node sent and return the decoded response.

        ``message_id`` names the edited message, ``text`` is the
        replacement body.  Edits are their own signed message kind:
        the payload is signed with this node's Reticulum identity and
        verified by the receiver against the link identity, exactly like
        a reaction — and the receiver applies it only when the editor is
        the message's original sender.

        Edits are delivered live in v1: unlike ``send_message`` there is
        no outbox queue, because the sealed-mail path is text-shaped
        throughout.  An unreachable peer raises :class:`NodeError`;
        queueing edits for offline peers is a follow-up once the
        mailbox can carry non-text kinds.
        """
        cleaned = normalize_message(text)
        target_id = message_id.strip() if isinstance(message_id, str) else ""
        if not target_id or len(target_id) > 64:
            raise NodeError("edit target must be a message id of 1-64 characters")
        if self.identity is None:
            raise NodeError("node has not been started")
        edit_id = generate_message_id()
        payload = edit_dict(target_id, cleaned, message_id=edit_id)
        canonical = encode(payload, max_bytes=MAX_REQUEST_BYTES)
        signature = self.identity.sign(canonical)
        if signature is None:  # pragma: no cover - identity without private keys
            raise NodeError("could not sign edit")
        payload["sig"] = base64.b64encode(signature).decode("ascii")
        payload_bytes = encode(payload, max_bytes=MAX_REQUEST_BYTES)
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        response = self._request(peer, REQUEST_DM_EDIT, payload_bytes, timeout)
        if response.get("edited") is False:
            # The receiver answered ok without applying the edit — an
            # unknown id, or a stale edit that lost to a newer one — so
            # the local copy must not change either.
            return dict(response, id=edit_id)
        # Update the local sent copy, so the editor sees the new text
        # under the message in their own inbox and console.  A missing
        # local copy (pruned since the send) is not a delivery failure.
        try:
            self.inbox.apply_edit(
                target_id, cleaned, edited_at=float(payload["sent_at"])
            )
        except InboxError as exc:
            log.warning("Could not record sent edit: %s", exc)
        # ``tau messages --sent`` reads the mailbox, not the inbox: the
        # sent record needs the new text too, or the editor's own history
        # keeps showing the original.
        try:
            self.mailbox.update_sent_text(
                target_id, cleaned, edited_at=float(payload["sent_at"])
            )
        except MailboxError as exc:
            log.warning("Could not update sent edit: %s", exc)
        return dict(response, id=edit_id)

    def _target_hash(self, target: Union[str, bytes, Peer]) -> Optional[bytes]:
        """Best-effort destination hash for a target that failed to resolve."""
        if isinstance(target, Peer):
            return target.destination_hash
        if isinstance(target, bytes):
            return target if len(target) == 16 else None
        if isinstance(target, str):
            try:
                peer = self.registry.resolve(target)
            except PeerError:
                peer = None
            if peer is not None:
                return peer.destination_hash
            value = target.strip()
            if len(value) == 32:
                try:
                    return bytes.fromhex(value)
                except ValueError:
                    return None
        return None

    def ping(
        self,
        target: Union[str, bytes, Peer],
        *,
        timeout: float = 15.0,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        return self._request(peer, REQUEST_PING, b"{}", timeout)

    def request_info(
        self,
        target: Union[str, bytes, Peer],
        *,
        timeout: float = 15.0,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        return self._request(peer, REQUEST_INFO, b"{}", timeout)

    def request_peers(
        self,
        target: Union[str, bytes, Peer],
        *,
        timeout: float = 15.0,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        return self._request(peer, REQUEST_PEERS, b"{}", timeout)

    def exchange_peers(
        self,
        target: Union[str, bytes, Peer],
        *,
        timeout: float = _EXCHANGE_TIMEOUT,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Ask a peer for the peers it knows and merge the new ones.

        Discovery is announce-only without this: a peer whose announce we
        never heard is invisible to us even though our neighbours know it.
        The answer is validated by :meth:`PeerRegistry.merge_exchanged`
        (keys must derive their destination hashes) and only new peers are
        added — a third party's claim never refreshes a known peer's
        liveness.  Returns a summary with ``source``, ``added``,
        ``updated`` and ``skipped``.  Raises :class:`NodeError` when the
        peer cannot be reached or answers with a malformed list.
        """
        response = self.request_peers(
            target, timeout=timeout, discover_timeout=discover_timeout
        )
        entries = response.get("peers") if isinstance(response, dict) else None
        if not isinstance(entries, list):
            raise NodeError("peer gave an invalid peers response")
        own_hash = self.destination.hash if self.destination is not None else None
        # Peers we still owe mail or acks are protected from eviction, so
        # gossip can never push out the record a delivery depends on.  The
        # mailbox lock is held across the read and the merge so a one-shot
        # process cannot queue mail for a peer between the two.
        with self.mailbox.transaction():
            owed = self._owed_hashes()
            summary = self.registry.merge_exchanged(
                entries[:100], now=None, self_hash=own_hash, keep=owed
            )
        if summary["added"] or summary["updated"]:
            self._save_peers(force=True)
        peer = self.resolve_peer(target)
        return {
            "source": peer.hexhash,
            "source_name": peer.display_name,
            **summary,
        }

    def request(
        self,
        target: Union[str, bytes, Peer],
        path: str,
        payload: bytes = b"{}",
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Send an arbitrary request to a peer and decode the response."""
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        return self._request(peer, path, payload, timeout)

    # -- links and requests ------------------------------------------------

    def _destination_for(self, peer: Peer) -> RNS.Destination:
        identity = RNS.Identity(create_keys=False)
        if not load_public_key(identity, peer.public_key):
            raise NodeError(f"invalid public key for peer {peer.hexhash}")
        destination = RNS.Destination(
            identity,
            RNS.Destination.OUT,
            RNS.Destination.SINGLE,
            APP_NAME,
            ASPECT,
        )
        if destination.hash != peer.destination_hash:
            raise NodeError(
                f"peer {peer.hexhash} has an identity for a different destination"
            )
        return destination

    def _wait_for_path(self, dest_hash: bytes, timeout: float) -> None:
        if RNS.Transport.has_path(dest_hash):
            return
        RNS.Transport.request_path(dest_hash)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if RNS.Transport.has_path(dest_hash):
                return
            time.sleep(0.1)
        raise NodeError(f"no path to {dest_hash.hex()} after {timeout:.1f}s")

    def _configure_link(self, link: RNS.Link) -> None:
        """Accept plain Resources on *link* only as file transfers.

        Request and response payloads travel as Reticulum request
        resources and never reach this callback — RNS dispatches
        them on the advertisement's request/response flags before
        consulting the app callback (verified against RNS 1.5.7:
        ``Link.request`` sets a request id, so ``is_request`` is
        true for over-MDU protocol requests).  It only ever sees an
        advertisement for a standalone Resource, which in tau means
        a file transfer.  An advertisement is accepted only when a
        signed ``/tau/file`` offer for (nearly) that many bytes is
        pending — the advertised size includes the transfer-id
        metadata, see ``_FILE_METADATA_OVERHEAD`` — and anything
        else is refused before a single part is requested, so a
        stranger cannot make this node buffer arbitrary data.
        """
        try:
            link.set_resource_strategy(RNS.Link.ACCEPT_APP)
            # Bind each callback to its link: an offer only matches a
            # Resource arriving from the same authenticated sender.
            link.set_resource_callback(
                lambda advertisement: self._on_resource_advertised(
                    advertisement, link
                )
            )
            link.set_resource_concluded_callback(
                lambda resource: self._on_resource_concluded(resource, link)
            )
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not configure link resources", exc_info=True)

    def _prune_file_offers(self) -> None:
        now = time.time()
        with self._file_offers_lock:
            expired = [
                transfer_id
                for transfer_id, offer in self._pending_file_offers.items()
                if offer["expires_at"] <= now
            ]
            for transfer_id in expired:
                del self._pending_file_offers[transfer_id]

    def _link_sender_hash(self, link: Any) -> Optional[str]:
        """Destination hash (hex) of the identity authenticated on *link*."""
        try:
            identity = link.get_remote_identity()
        except Exception:  # pragma: no cover - defensive
            return None
        if identity is None:
            return None
        return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()

    def _on_resource_advertised(self, advertisement: Any, link: Any = None) -> bool:
        """Accept a Resource only for a pending, matching file offer.

        The advertised data size is the offered file size plus the
        transfer-id metadata prepended to the Resource (see
        ``_FILE_METADATA_OVERHEAD``), so the match allows that much
        slack; the exact size and the SHA-256 are re-checked once
        the bytes are assembled.  Only offers made by the sender
        authenticated on *link* are considered.
        """
        sender = self._link_sender_hash(link)
        if sender is None:
            return False
        try:
            size = advertisement.get_data_size()
        except Exception:  # pragma: no cover - defensive
            log.debug("Resource advertisement without a size", exc_info=True)
            return False
        if (
            not isinstance(size, int)
            or size < 1
            or size > MAX_FILE_BYTES + _FILE_METADATA_OVERHEAD
        ):
            return False
        self._prune_file_offers()
        with self._file_offers_lock:
            return any(
                offer["from"] == sender
                and offer["size"] <= size <= offer["size"] + _FILE_METADATA_OVERHEAD
                for offer in self._pending_file_offers.values()
            )

    def _on_resource_concluded(self, resource: Any, link: Any = None) -> None:
        """Handle a fully transferred Resource: verify, save, record.

        The Resource's metadata is the transfer id from the signed
        offer; the bytes are checked against the offer's SHA-256
        before anything is written to disk, and a transfer nobody
        offered (or whose hash mismatches) is dropped silently.  The
        offer must have been made by the sender on *link*.
        """
        try:
            transfer_id = resource.metadata
            if isinstance(transfer_id, bytes):
                transfer_id = transfer_id.decode("utf-8", "ignore")
            if not isinstance(transfer_id, str):
                return
            sender = self._link_sender_hash(link)
            with self._file_offers_lock:
                offer = self._pending_file_offers.get(transfer_id)
                if offer is None or offer["from"] != sender:
                    offer = None
                else:
                    del self._pending_file_offers[transfer_id]
            if offer is None:
                log.info("Ignoring Resource for unknown transfer %r", transfer_id)
                return
            if getattr(resource, "status", None) != RNS.Resource.COMPLETE:
                log.info("File transfer %s did not complete", transfer_id[:8])
                return
            data = _read_resource_data(resource)
            if data is None:
                log.warning(
                    "File transfer %s has unreadable data", transfer_id[:8]
                )
                return
            if len(data) != offer["size"]:
                log.warning(
                    "File transfer %s size mismatch: got %d, offered %d",
                    transfer_id[:8],
                    len(data),
                    offer["size"],
                )
                return
            digest = hashlib.sha256(data).hexdigest()
            if digest != offer["sha256"]:
                log.warning(
                    "File transfer %s hash mismatch: discarding",
                    transfer_id[:8],
                )
                return
            peer = self.registry.get(offer["from"])
            if peer is not None:
                offer["from_name"] = peer.display_name
            record = self._save_received_file(offer, data)
            if record is None:
                return
            self._record_received(record)
            if self.on_file is not None:
                try:
                    self.on_file(record)
                except Exception:  # pragma: no cover - user callback
                    log.warning("on_file callback failed", exc_info=True)
        except Exception:  # pragma: no cover - defensive
            log.warning("Could not handle concluded Resource", exc_info=True)

    def _save_received_file(
        self, offer: Dict[str, Any], data: bytes
    ) -> Optional[Dict[str, Any]]:
        """Write *data* under ``files/<sender>/<name>`` and describe it.

        Never overwrites: a name that already exists gets a numeric
        suffix before the extension.  Returns the inbox record, or
        ``None`` when the write fails.
        """
        sender_dir = self.config.files_dir / offer["from"]
        try:
            sender_dir.mkdir(parents=True, exist_ok=True)
            if self.config.files_dir.is_symlink() or sender_dir.is_symlink():
                raise OSError("files directory must not be a symlink")
            target = _create_unique_file(sender_dir, offer["name"], data)
        except OSError as exc:
            log.warning("Could not save received file %s: %s", offer["name"], exc)
            return None
        log.info(
            "Received file %s (%d bytes) from %s",
            offer["name"],
            len(data),
            offer["from"][:8],
        )
        return {
            "kind": "file",
            "from": offer.get("from_name") or offer["from"][:8],
            "destination": offer["from"],
            "name": target.name,
            "size": len(data),
            "sha256": offer["sha256"],
            "path": str(target),
            "sent_at": offer.get("sent_at") or 0.0,
            "received_at": time.time(),
            "id": offer["id"],
        }

    def _on_incoming_link(self, link: RNS.Link) -> None:
        self._configure_link(link)
        with self._links_lock:
            self._active_links.add(link)
        link.set_link_closed_callback(self._on_link_closed)
        log.debug("Incoming link from %s", link)

    def _on_link_closed(self, link: RNS.Link) -> None:
        with self._links_lock:
            self._active_links.discard(link)

    def _open_link(
        self, peer: Peer, timeout: float, *, deadline: Optional[float] = None
    ) -> RNS.Link:
        # Each stage gets only the time left before the deadline, not
        # the full timeout again: path discovery first, then link
        # establishment, and the request itself after that.
        self._wait_for_path(peer.destination_hash, _budget(timeout, deadline))
        if _past(deadline):
            raise NodeError(
                f"deadline passed before a link to {peer.hexhash} was ready"
            )
        destination = self._destination_for(peer)

        established = threading.Event()
        state: Dict[str, Any] = {}

        def on_established(link: RNS.Link) -> None:
            self._configure_link(link)
            try:
                link.identify(self.identity)
            except Exception:  # pragma: no cover - defensive
                log.debug("Could not identify on link", exc_info=True)
            state["link"] = link
            established.set()

        def on_closed(link: RNS.Link) -> None:
            self._on_link_closed(link)
            state.setdefault("closed", True)
            established.set()

        link = RNS.Link(
            destination,
            established_callback=on_established,
            closed_callback=on_closed,
        )
        with self._links_lock:
            self._active_links.add(link)

        if not established.wait(_budget(timeout, deadline)):
            try:
                link.teardown()
            except Exception:  # pragma: no cover - defensive
                pass
            raise NodeError(f"link to {peer.hexhash} timed out after {timeout:.1f}s")
        if "link" not in state:
            raise NodeError(f"link to {peer.hexhash} closed before it was ready")
        return state["link"]

    def _request(
        self,
        peer: Peer,
        path: str,
        payload: bytes,
        timeout: float,
        *,
        deadline: Optional[float] = None,
    ) -> Dict[str, Any]:
        timeout = _budget(timeout, deadline)
        link = self._open_link(peer, timeout, deadline=deadline)
        try:
            return self._request_on_link(
                link, path, payload, timeout, deadline=deadline
            )
        finally:
            try:
                link.teardown()
            except Exception:  # pragma: no cover - defensive
                pass

    def _request_on_link(
        self,
        link: RNS.Link,
        path: str,
        payload: bytes,
        timeout: float,
        *,
        deadline: Optional[float] = None,
    ) -> Dict[str, Any]:
        concluded = threading.Event()
        state: Dict[str, Any] = {}

        def on_response(receipt: RNS.RequestReceipt) -> None:
            state["response"] = receipt.get_response()
            state["rtt"] = receipt.get_response_time()
            concluded.set()

        def on_failed(receipt: RNS.RequestReceipt) -> None:
            state["error"] = "request failed or timed out"
            state["status"] = receipt.get_status()
            concluded.set()

        # Do not send a request the deadline no longer leaves time to
        # answer: re-check here, after the link stages consumed time.
        if _past(deadline):
            raise NodeError(f"deadline passed before sending {path}")
        timeout = _budget(timeout, deadline)
        receipt = link.request(
            path,
            payload,
            response_callback=on_response,
            failed_callback=on_failed,
            timeout=timeout,
            max_response_size=_RESPONSE_WIRE_LIMIT,
        )
        if receipt is False:
            raise NodeError("could not send request; link is not ready")

        # The response grace is capped by the caller's deadline, if any.
        wait_for = _budget(timeout + 5.0, deadline)
        if not concluded.wait(wait_for):
            raise NodeError(f"no response to {path} within {wait_for:.1f}s")
        if "error" in state:
            raise NodeError(f"{state['error']} (status={state.get('status')})")

        response = state.get("response")
        if response is None:
            raise NodeError("peer returned an empty response")
        try:
            decoded = decode(response)
        except ProtocolError as exc:
            raise NodeError(f"peer returned an invalid response: {exc}") from exc
        if decoded.get("ok") is not True:
            error = decoded.get("error") or {}
            message = error.get("message", "unknown error")
            if error.get("code") == "not_allowed":
                raise NotAllowedError(f"peer rejected the request: {message}")
            if error.get("code") == "unavailable":
                # The block-list drops silently with this generic code;
                # it is still the peer's explicit refusal, not transport.
                raise PeerRefusedError(f"peer rejected the request: {message}")
            if error.get("code") == "not_a_member":
                raise NotAMemberError(f"peer rejected the request: {message}")
            raise NodeError(f"peer rejected the request: {message}")
        if isinstance(state.get("rtt"), (int, float)):
            decoded.setdefault("_rtt", round(float(state["rtt"]), 6))
        return decoded

"""A tau node: discovery, announcements and encrypted peer messaging."""

from __future__ import annotations

import base64
import errno
import logging
import os
import random
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

try:  # POSIX only; the instance lock is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

import RNS

from . import __version__
from .allowlist import AllowList, load_allowlist_or_deny_all
from .config import TauConfig, tcp_listen_endpoints, write_rns_config
from .identity import (
    destination_hash as tau_destination_hash,
    load_or_create_identity,
    load_public_key,
)
from .inbox import Inbox
from .mailbox import (
    MAX_HOLDERS_PER_MESSAGE,
    Mailbox,
    MailboxError,
    MailboxFull,
    open_message,
    seal_message,
)
from .peers import DESTINATION_HASH_BYTES, Peer, PeerError, PeerRegistry
from .protocol import (
    APP_NAME,
    ASPECT,
    ASPECT_FILTER,
    MAX_MAIL_REQUEST_BYTES,
    MAX_MAIL_TTL,
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    PROTOCOL_VERSION,
    REQUEST_INFO,
    REQUEST_MAIL_FETCH,
    REQUEST_MAIL_STORE,
    REQUEST_MESSAGE,
    REQUEST_PEERS,
    REQUEST_PING,
    ProtocolError,
    build_announce,
    build_mail_fetch,
    build_mail_store,
    decode,
    encode_message,
    generate_message_id,
    normalize_message,
    parse_announce,
    parse_mail_fetch,
    parse_mail_store,
    parse_message,
    response_error,
    response_ok,
)

log = logging.getLogger(__name__)

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
_FETCH_BLOB_BUDGET = 40 * 1024


class NodeError(RuntimeError):
    """Raised when a node operation fails."""


class NotAllowedError(NodeError):
    """Raised when a peer explicitly rejects a request (allow-list denial).

    Distinct from other :class:`NodeError` failures so callers can tell
    an explicit refusal apart from an unreachable peer: a refusal must
    not be silently queued for retry.
    """


class InstanceAlreadyRunning(NodeError):
    """The data directory belongs to another running node."""


class InstanceLock:
    """Exclusive advisory lock marking a data directory as in use.

    A serving node holds this lock for its whole lifetime, so a second
    serving node on the same data directory fails fast instead of
    corrupting the shared peer registry, mailbox and Tan database.
    The lock file records the holder's PID for diagnostics; the
    ``flock`` itself is the guard, so a file left behind by a crashed
    node never blocks a restart.  POSIX only — without ``fcntl`` the
    lock is a no-op, mirroring the mailbox's cross-process locking.
    """

    def __init__(self, path: Path, data_dir: Path) -> None:
        self.path = Path(path)
        self.data_dir = Path(data_dir)
        self._handle: Optional[Any] = None

    def acquire(self) -> None:
        """Take the lock, raising :class:`NodeError` when it is held."""
        if self._handle is not None or fcntl is None:
            return
        handle = open(self.path, "a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            holder = self._read_holder_pid(handle)
            handle.close()
            detail = f" (pid {holder})" if holder is not None else ""
            raise InstanceAlreadyRunning(
                "another tau node is already serving on data directory "
                f"{self.data_dir}{detail}; stop it before starting a "
                "second node on the same directory"
            ) from None
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        self._handle = handle

    @staticmethod
    def _read_holder_pid(handle: Any) -> Optional[int]:
        try:
            handle.seek(0)
            text = handle.read().strip()
        except OSError:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    def release(self) -> None:
        """Release the lock; safe to call when it is not held."""
        handle, self._handle = self._handle, None
        if handle is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:  # pragma: no cover - defensive
            log.debug("Could not unlock %s", self.path, exc_info=True)
        handle.close()


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
    ) -> None:
        self.config = config
        self.name = name or config.name
        self.registry = (
            registry if registry is not None else PeerRegistry(config.peers_path)
        )
        self.mailbox = Mailbox(config.mailbox_path)
        self.inbox = Inbox(config.inbox_path)
        self.on_message = on_message
        self.on_peer = on_peer
        self.on_delivered = on_delivered

        self.reticulum: Optional[RNS.Reticulum] = None
        self.identity: Optional[RNS.Identity] = None
        self.destination: Optional[RNS.Destination] = None

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
        self._save_lock = threading.Lock()
        self._last_peer_save = 0.0
        self._instance_lock: Optional[InstanceLock] = None
        self._shutdown = False
        self.app: Optional[Any] = None
        self._allowlist: Optional[AllowList] = None

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
        """
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
        try:
            self._start(announce=announce, mail_loop=mail_loop)
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

    def _start(self, *, announce: bool, mail_loop: bool = True) -> None:
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

        if announce:
            self.announce()

        log.info("tau node %r ready at %s", self.name, self.destination.hexhash)

    def stop(self) -> None:
        """Stop announcing, close links and persist the peer registry."""
        # Workers must finish before pruning/saving state or releasing the lock.
        self._stop_workers()

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

    def _owed_hashes(self) -> Set[bytes]:
        """Destination hashes the node still owes mail or acks to.

        Peers holding mail we have fetched stay too: their pending
        acknowledgements go out with the next fetch.
        """
        owed = {entry.to for entry in self.mailbox.outbox_list()}
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
        if not duplicate:
            if message_id:
                self.mailbox.mark_seen(message_id)
            record = {
                "from": caller_name,
                "destination": caller_hash,
                "text": message["text"],
                "sent_at": message["sent_at"],
                "received_at": received_at,
                "id": message_id,
            }
            log.info(
                "Message from %s (%s): %s",
                caller_name,
                caller_hash or "anonymous",
                message["text"],
            )
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
            duplicate=duplicate,
        )

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
        try:
            stored = self.mailbox.hold(
                to=bytes.fromhex(store["to"]),
                blob=store["blob"],
                message_id=store["id"],
                expires_at=store["expires_at"],
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
        know the outcome waits; a background round never does.
        """
        summary = {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
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
            self.mailbox.prune()
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
                targets = {entry.to for entry in self.mailbox.due_outbox()}
            else:
                targets = {entry.to for entry in self.mailbox.outbox_list()}
            # Peers heard only through exchange come after directly heard
            # ones inside each group, so second-hand entries cannot crowd
            # out reachable peers.
            peers.sort(
                key=lambda p: (p.destination_hash not in targets, p.via_exchange)
            )
            if max_peers is not None:
                peers = peers[:max_peers]
            for peer in peers:
                moved = self._flush_peer(
                    peer,
                    timeout,
                    respect_backoff=respect_backoff,
                    message_id=message_id,
                )
                summary["delivered"] += moved["delivered"]
                summary["deposited"] += moved["deposited"]
                summary["received"] += self._collect_mail(peer, timeout)
        finally:
            self._mail_lock.release()
        return summary

    def _flush_peer(
        self,
        peer: Peer,
        timeout: float,
        *,
        respect_backoff: bool = True,
        message_id: Optional[str] = None,
    ) -> Dict[str, int]:
        moved = {"delivered": 0, "deposited": 0}
        if respect_backoff:
            due = self.mailbox.due_outbox(to=peer.destination_hash)
        else:
            due = self.mailbox.outbox_list(to=peer.destination_hash)
        if message_id is not None:
            due = [entry for entry in due if entry.id == message_id]
        if peer.caps and "mail" not in peer.caps:
            # A pre-mail peer can still receive direct messages, just not
            # hold or hand over sealed envelopes.
            for entry in due:
                if self._deliver_entry(peer, entry, timeout):
                    moved["delivered"] += 1
            return moved

        for entry in due:
            if self._deliver_entry(peer, entry, timeout):
                moved["delivered"] += 1

        now = time.time()
        for entry in self.mailbox.outbox_list():
            if entry.to == peer.destination_hash:
                continue
            if message_id is not None and entry.id != message_id:
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
            if self._deposit_entry(peer, entry, timeout):
                moved["deposited"] += 1
        return moved

    def _still_queued(self, message_id: str) -> bool:
        """True if the outbox still holds this entry, reloaded from disk.

        The sweep works from a snapshot, so a cancel can land after the
        snapshot was taken; each delivery attempt re-checks instead of
        trusting the snapshot.  A send already in flight cannot be
        recalled — cancelling only stops future attempts.
        """
        with self.mailbox.transaction():
            return self.mailbox.get_outbox(message_id) is not None

    def _deliver_entry(self, peer: Peer, entry: Any, timeout: float) -> bool:
        # The sweep lists the outbox before acting, so a message cancelled
        # by another process (``tau outbox cancel``) can still be sitting
        # in this pass's snapshot. Re-check the queue under the mailbox
        # lock — which reloads the on-disk state — before sending.
        with self.mailbox.transaction():
            if self.mailbox.get_outbox(entry.id) is None:
                return False
        payload = encode_message(
            entry.text, message_id=entry.id, sent_at=entry.created_at
        )
        if not self._still_queued(entry.id):
            return False
        try:
            self._request(peer, REQUEST_MESSAGE, payload, timeout)
        except NodeError as exc:
            self.mailbox.record_attempt(entry.id, error=str(exc))
            return False
        self.mailbox.remove_outbox(entry.id)
        log.info("Delivered queued message %s to %s", entry.id, peer.hexhash)
        if self.on_delivered is not None:
            try:
                self.on_delivered(entry.id)
            except Exception:  # pragma: no cover - user callback
                log.warning("on_delivered callback failed", exc_info=True)
        return True

    def _deposit_entry(self, peer: Peer, entry: Any, timeout: float) -> bool:
        # Same re-check as _deliver_entry: do not leave a sealed copy
        # with a holder for a message cancelled since the sweep listed it.
        with self.mailbox.transaction():
            if self.mailbox.get_outbox(entry.id) is None:
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
        if not self._still_queued(entry.id):
            return False
        try:
            self._request(peer, REQUEST_MAIL_STORE, payload, timeout)
        except NodeError:
            return False
        self.mailbox.record_deposit(entry.id, peer.hexhash)
        log.info(
            "Left a sealed copy of %s with %s for %s",
            entry.id,
            peer.hexhash,
            entry.to_hex,
        )
        return True

    def _collect_mail(self, peer: Peer, timeout: float) -> int:
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
                peer, REQUEST_MAIL_FETCH, build_mail_fetch(acks), timeout
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
                message = open_message(self.identity, blob)
            except (ValueError, MailboxError) as exc:
                log.warning("Dropping unreadable held mail %s: %s", message_id, exc)
                continue
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
                message = open_message(self.identity, envelope.blob)
            except MailboxError as exc:
                log.warning(
                    "Dropping unreadable held mail %s: %s", envelope.id, exc
                )
                message = None
            still_held = self.mailbox.drop_held(
                envelope.id, to=self.destination.hash
            )
            if not still_held:
                # Dropped by the owner (tau outbox drop-held) after this
                # sweep listed it: do not surface a copy we no longer hold.
                continue
            if message is not None and self._deliver_collected(message):
                received += 1
        return received

    def _deliver_collected(self, message: Dict[str, Any]) -> bool:
        """Hand a decrypted relayed message to the user, at most once."""
        sender = message.get("from")
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
        self.mailbox.mark_seen(message["id"])
        record = {
            "from": message["from_name"],
            "destination": message["from"],
            "text": message["text"],
            "sent_at": message["sent_at"],
            "received_at": time.time(),
            "id": message["id"],
            "relayed": True,
        }
        log.info(
            "Message from %s (%s) via store-and-forward: %s",
            message["from_name"],
            message["from"],
            message["text"],
        )
        self._record_received(record)
        if self.on_message is not None:
            try:
                self.on_message(record)
            except Exception:  # pragma: no cover - user callback
                log.warning("on_message callback failed", exc_info=True)
        return True

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

    def send_message(
        self,
        target: Union[str, bytes, Peer],
        text: str,
        *,
        timeout: float = 20.0,
        discover_timeout: Optional[float] = None,
        queue: bool = True,
    ) -> Dict[str, Any]:
        """Send a text message to a peer and return the decoded response.

        With ``queue`` enabled (the default), a message whose peer cannot
        be reached is stored in the local outbox instead of being lost:
        the returned dict has ``queued: True`` and the message id, and
        delivery is retried automatically whenever the peer — or any peer
        willing to hold a sealed copy — is heard from.
        """
        normalized = normalize_message(text)
        message_id = generate_message_id()
        payload = encode_message(normalized, message_id=message_id)
        peer: Optional[Peer] = None
        try:
            peer = self.resolve_peer(
                target,
                timeout=discover_timeout
                if discover_timeout is not None
                else min(timeout, 10.0),
            )
            response = self._request(peer, REQUEST_MESSAGE, payload, timeout)
            # Only this node's own outbox may report a message as queued,
            # with a real local entry id. A peer that claims queued (with
            # an arbitrary id, or none at all) must not be trusted: both
            # consumers read response["queued"]/response["id"] verbatim,
            # so a spoofed claim misreports delivery in the console and
            # can make the CLI raise KeyError on the missing id.
            for key in ("id", "expires_at", "to"):
                response.pop(key, None)
            response["queued"] = False
            return response
        except NotAllowedError:
            # The peer is online and explicitly refused the message;
            # queueing it would retry a refusal until its TTL expires.
            raise
        except NodeError:
            if not queue or not self.started:
                raise
            dest_hash = (
                peer.destination_hash
                if peer is not None
                else self._target_hash(target)
            )
            if dest_hash is None:
                raise
            entry = self.mailbox.enqueue(
                to=dest_hash,
                text=normalized,
                to_name=peer.display_name if peer is not None else "",
                message_id=message_id,
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

    def _configure_link(self, link: RNS.Link, resource_limit: int) -> None:
        try:
            link.set_resource_strategy(RNS.Link.ACCEPT_APP)
            link.set_resource_callback(
                lambda resource: resource.get_data_size() <= resource_limit
            )
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not configure link resources", exc_info=True)

    def _on_incoming_link(self, link: RNS.Link) -> None:
        # Incoming requests can be mail-sized (a sealed envelope store is
        # up to _MAIL_WIRE_LIMIT on the wire), so accept resources up to
        # the larger of the two budgets, not just the plain request one.
        self._configure_link(link, max(_REQUEST_WIRE_LIMIT, _MAIL_WIRE_LIMIT))
        with self._links_lock:
            self._active_links.add(link)
        link.set_link_closed_callback(self._on_link_closed)
        log.debug("Incoming link from %s", link)

    def _on_link_closed(self, link: RNS.Link) -> None:
        with self._links_lock:
            self._active_links.discard(link)

    def _open_link(self, peer: Peer, timeout: float) -> RNS.Link:
        self._wait_for_path(peer.destination_hash, timeout)
        destination = self._destination_for(peer)

        established = threading.Event()
        state: Dict[str, Any] = {}

        def on_established(link: RNS.Link) -> None:
            self._configure_link(link, _RESPONSE_WIRE_LIMIT)
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

        if not established.wait(timeout):
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
    ) -> Dict[str, Any]:
        link = self._open_link(peer, timeout)
        try:
            return self._request_on_link(link, path, payload, timeout)
        finally:
            try:
                link.teardown()
            except Exception:  # pragma: no cover - defensive
                pass

    def _request_on_link(
        self, link: RNS.Link, path: str, payload: bytes, timeout: float
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

        if not concluded.wait(timeout + 5.0):
            raise NodeError(f"no response to {path} within {timeout + 5.0:.1f}s")
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
            raise NodeError(f"peer rejected the request: {message}")
        if isinstance(state.get("rtt"), (int, float)):
            decoded.setdefault("_rtt", round(float(state["rtt"]), 6))
        return decoded

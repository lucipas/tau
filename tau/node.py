"""A tau node: discovery, announcements and encrypted peer messaging."""

from __future__ import annotations

import logging
import random
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Union

import RNS

from . import __version__
from .config import TauConfig, write_rns_config
from .identity import (
    destination_hash as tau_destination_hash,
    load_or_create_identity,
)
from .peers import Peer, PeerError, PeerRegistry
from .protocol import (
    APP_NAME,
    ASPECT,
    ASPECT_FILTER,
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    PROTOCOL_VERSION,
    REQUEST_INFO,
    REQUEST_MESSAGE,
    REQUEST_PEERS,
    REQUEST_PING,
    ProtocolError,
    build_announce,
    decode,
    encode_message,
    parse_announce,
    parse_message,
    response_error,
    response_ok,
)

log = logging.getLogger(__name__)

# Reticulum adds framing and resource overhead on top of request payloads.
_REQUEST_WIRE_LIMIT = MAX_REQUEST_BYTES + 2 * 1024
_RESPONSE_WIRE_LIMIT = MAX_RESPONSE_BYTES + 2 * 1024
_DEFAULT_LINK_TIMEOUT = 15.0
_PEER_SAVE_INTERVAL = 5.0
_PATHFINDER_MAX_HOPS = 128


class NodeError(RuntimeError):
    """Raised when a node operation fails."""


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
    ) -> None:
        self.config = config
        self.name = name or config.name
        self.registry = (
            registry if registry is not None else PeerRegistry(config.peers_path)
        )
        self.on_message = on_message
        self.on_peer = on_peer

        self.reticulum: Optional[RNS.Reticulum] = None
        self.identity: Optional[RNS.Identity] = None
        self.destination: Optional[RNS.Destination] = None

        self._announce_handler: Optional[_AnnounceHandler] = None
        self._announce_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started_at: Optional[float] = None
        self._links_lock = threading.Lock()
        self._active_links: Set[RNS.Link] = set()
        self._save_lock = threading.Lock()
        self._last_peer_save = 0.0
        self._shutdown = False
        self.app: Optional[Any] = None

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

    def start(self, *, announce: bool = True) -> None:
        """Start Reticulum, load the identity and begin answering requests."""
        if self.reticulum is not None:
            raise NodeError("node is already running")
        if self._shutdown:
            raise NodeError(
                "Reticulum cannot be restarted in the same process; "
                "create a new TauNode in a new process"
            )

        self.config.prepare()
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
        self.destination.set_max_request_size(_REQUEST_WIRE_LIMIT)
        self.destination.set_default_app_data(lambda: build_announce(self.name))
        self.destination.set_link_established_callback(self._on_incoming_link)

        handlers = {
            REQUEST_PING: self._handle_ping,
            REQUEST_INFO: self._handle_info,
            REQUEST_MESSAGE: self._handle_message,
            REQUEST_PEERS: self._handle_peers,
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

        if announce:
            self.announce()

        log.info("tau node %r ready at %s", self.name, self.destination.hexhash)

    def stop(self) -> None:
        """Stop announcing, close links and persist the peer registry."""
        self._stop_event.set()

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

        if self.app is not None:
            try:
                self.app.detach()
            except Exception:  # pragma: no cover - defensive
                log.debug("Could not detach app layer", exc_info=True)
            self.app = None

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

    def _save_peers(self, *, force: bool = False) -> None:
        if self.registry.path is None:
            return
        with self._save_lock:
            now = time.time()
            if not force and now - self._last_peer_save < _PEER_SAVE_INTERVAL:
                return
            try:
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
        return remote_identity.hexhash

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
        peers = self.registry.public_peers()
        return response_ok(peers=peers[:100], count=len(peers))

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
        caller_name = "unknown"
        if caller_hash:
            peer = self.registry.get(caller_hash)
            caller_name = peer.display_name if peer is not None else caller_hash[:8]

        record = {
            "from": caller_name,
            "destination": caller_hash,
            "text": message["text"],
            "sent_at": message["sent_at"],
            "received_at": received_at,
        }
        log.info(
            "Message from %s (%s): %s",
            caller_name,
            caller_hash or "anonymous",
            message["text"],
        )
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
        )

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
    ) -> Dict[str, Any]:
        """Send a text message to a peer and return the decoded response."""
        peer = self.resolve_peer(
            target,
            timeout=discover_timeout
            if discover_timeout is not None
            else min(timeout, 10.0),
        )
        payload = encode_message(text)
        return self._request(peer, REQUEST_MESSAGE, payload, timeout)

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
        if not identity.load_public_key(peer.public_key):
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
        self._configure_link(link, _REQUEST_WIRE_LIMIT)
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
            raise NodeError(
                f"peer rejected the request: {error.get('message', 'unknown error')}"
            )
        if isinstance(state.get("rtt"), (int, float)):
            decoded.setdefault("_rtt", round(float(state["rtt"]), 6))
        return decoded

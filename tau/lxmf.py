"""LXMF addressing for tau, plus the LXMF router lifecycle.

LXMF — Reticulum's messaging layer — addresses a node at a delivery
destination derived from its Reticulum identity: app ``lxmf``, aspect
``delivery``.  That is the same destination LXMF's own ``LXMRouter``
listens on, so the hash computed here is the address any LXMF client
(including a future tau sender) uses to reach the node, and the address
an LXMF propagation node holds mail for.

Addressing never needs the ``lxmf`` package or a live Reticulum
instance: it derives destination hashes with ``RNS.Destination.hash``,
so it stays usable from the CLI, the peer registry, and tests.  The
router itself is optional — :func:`start_lxmf_router` imports LXMF
lazily, so a tau install without the ``lxmf`` extra keeps working and
only fails, with a clear error, when the router is actually requested.
"""

from __future__ import annotations

import importlib
import logging
import signal
import threading
from pathlib import Path
from typing import Any, Optional

import RNS

from .identity import load_public_key

log = logging.getLogger(__name__)

LXMF_APP_NAME = "lxmf"
LXMF_DELIVERY_ASPECT = "delivery"
LXMF_PROPAGATION_ASPECT = "propagation"

#: Install hint shown when the router is requested without the extra.
LXMF_INSTALL_HINT = 'pip install "tau-p2p[lxmf]"'


class LXMFError(Exception):
    """Raised when the LXMF router cannot start or stop cleanly."""


def lxmf_delivery_hash(identity: RNS.Identity) -> bytes:
    """Return the LXMF delivery destination hash for *identity*."""
    return RNS.Destination.hash(identity, LXMF_APP_NAME, LXMF_DELIVERY_ASPECT)


def lxmf_propagation_hash(identity: RNS.Identity) -> bytes:
    """Return the LXMF propagation destination hash for *identity*."""
    return RNS.Destination.hash(identity, LXMF_APP_NAME, LXMF_PROPAGATION_ASPECT)


def _identity_for_key(public_key: bytes) -> RNS.Identity:
    """Rebuild a keyless identity from a raw public key."""
    if not isinstance(public_key, (bytes, bytearray, memoryview)):
        raise ValueError("public key must be bytes")
    identity = RNS.Identity(create_keys=False)
    if not load_public_key(identity, bytes(public_key)):
        raise ValueError("invalid public key")
    return identity


def lxmf_delivery_hash_for_key(public_key: bytes) -> bytes:
    """Return the LXMF delivery destination hash for a raw public key.

    Peers are usually known only by their advertised public key, so this
    derives their LXMF address without needing the full identity.
    Raises :class:`ValueError` when the key cannot be loaded.
    """
    return lxmf_delivery_hash(_identity_for_key(public_key))


def lxmf_propagation_hash_for_key(public_key: bytes) -> bytes:
    """Return the LXMF propagation destination hash for a raw public key."""
    return lxmf_propagation_hash(_identity_for_key(public_key))


# -- router lifecycle ------------------------------------------------------

def start_lxmf_router(
    identity: RNS.Identity,
    data_dir: Path,
    *,
    display_name: Optional[str] = None,
) -> Any:
    """Start an ``LXMRouter`` on *identity* and register its delivery identity.

    Storage lives under ``<data_dir>/lxmf`` (LXMRouter appends ``/lxmf``
    to the storage path it is given).  The router listens on the same
    delivery destination :func:`lxmf_delivery_hash` derives, and that
    destination is announced so other LXMF clients can find a path to it.

    Raises :class:`LXMFError` when the ``lxmf`` package is not installed,
    when called off the main thread (LXMRouter installs signal handlers),
    or when the delivery identity cannot be registered.  The caller must
    have a Reticulum instance running already.
    """
    if threading.current_thread() is not threading.main_thread():
        raise LXMFError(
            "the LXMF router must start on the main thread "
            "(LXMRouter installs signal handlers)"
        )
    lxmf_module = _import_lxmf()
    previous = _signal_handlers()
    router = lxmf_module.LXMRouter(identity=identity, storagepath=str(data_dir))
    installed = _signal_handlers()
    # Remember what LXMRouter replaced so shutdown can put it back.
    setattr(router, _SIGNAL_STATE_ATTR, (previous, installed))
    try:
        delivery = router.register_delivery_identity(
            identity, display_name=display_name
        )
        if delivery is None:
            raise LXMFError("could not register the LXMF delivery identity")
        router.announce(delivery.hash)
    except BaseException:
        _shutdown_router(router)
        raise
    return router


def stop_lxmf_router(router: Optional[Any]) -> None:
    """Tear a router down via its exit handler.

    LXMRouter's exit handler is idempotent, so calling this twice — or
    letting the handler it registers with ``atexit`` run at process exit
    — is safe.  Signal handlers that LXMRouter replaced are restored,
    unless the embedding application has installed its own since.
    Passing ``None`` is a no-op.
    """
    if router is None:
        return
    _shutdown_router(router)


_SIGNAL_STATE_ATTR = "_tau_signal_state"
_MANAGED_SIGNALS = (signal.SIGINT, signal.SIGTERM)


def _signal_handlers() -> dict:
    """Return the current Python-level handler for each managed signal."""
    return {signum: signal.getsignal(signum) for signum in _MANAGED_SIGNALS}


def _shutdown_router(router: Any) -> None:
    """Run the router's exit handler and restore the signals it replaced."""
    try:
        router.exit_handler()
    finally:
        state = getattr(router, _SIGNAL_STATE_ATTR, None)
        if state is not None:
            previous, installed = state
            _restore_signal_handlers(previous, installed)
            setattr(router, _SIGNAL_STATE_ATTR, None)


def _restore_signal_handlers(previous: dict, installed: dict) -> None:
    """Restore *previous* handlers for signals still owned by the router."""
    for signum, prev in previous.items():
        # signal.signal() rejects None (handler not set from Python).
        if prev is None or installed.get(signum) is None:
            continue
        if signal.getsignal(signum) != installed[signum]:
            # The embedding application installed its own handler since.
            continue
        try:
            signal.signal(signum, prev)
        except (ValueError, OSError, TypeError):  # pragma: no cover - defensive
            log.debug("Could not restore handler for signal %s", signum, exc_info=True)


def lxmf_router_delivery_hash(router: Optional[Any]) -> Optional[bytes]:
    """Return the delivery destination hash a router listens on.

    Returns ``None`` when the router is ``None`` or has no delivery
    identity registered.
    """
    if router is None:
        return None
    destinations = getattr(router, "delivery_destinations", None) or {}
    if not destinations:
        return None
    return next(iter(destinations))


# -- sending ---------------------------------------------------------------

def _import_lxmf() -> Any:
    """Import the ``lxmf`` package or raise :class:`LXMFError`."""
    try:
        return importlib.import_module("LXMF")
    except ImportError as exc:
        raise LXMFError(
            "LXMF is not installed; install it with "
            f"{LXMF_INSTALL_HINT}"
        ) from exc


def _lxmf_destination(identity: RNS.Identity) -> Any:
    """Return the LXMF delivery ``RNS.Destination`` for *identity*."""
    return RNS.Destination(
        identity,
        RNS.Destination.OUT,
        RNS.Destination.SINGLE,
        LXMF_APP_NAME,
        LXMF_DELIVERY_ASPECT,
    )


def build_lxmf_message(
    source_identity: RNS.Identity,
    destination_public_key: bytes,
    text: str,
) -> Any:
    """Build an ``LXMessage`` carrying *text* to a peer's LXMF address.

    The destination is the peer's LXMF delivery destination, derived
    from their public key, and the source is this node's own delivery
    destination, so the receiver can attribute the message to a known
    peer exactly like an incoming delivery.  Raises
    :class:`LXMFError` when the ``lxmf`` package is not installed
    and :class:`ValueError` for a key that cannot be loaded.
    """
    lxmf_module = _import_lxmf()
    destination = _lxmf_destination(_identity_for_key(destination_public_key))
    source = _lxmf_destination(source_identity)
    return lxmf_module.LXMessage(destination, source, content=text)


def send_lxmf_message(router: Any, message: Any, *, timeout: float) -> bool:
    """Hand *message* to *router* and wait for delivery confirmation.

    The router owns retries once the message is handed over, so the
    caller waits at most *timeout* seconds for the delivery callback
    (fired when the peer receives the message, or when a propagation
    node accepts it) or the failed callback.  On timeout the router's
    copy is cancelled, so it cannot deliver late after the caller has
    kept the message queued and sends a fresh copy on the next sweep.
    Returns True only on confirmation.  Raises :class:`LXMFError`
    when the router refuses the message.
    """
    if router is None:
        raise LXMFError("no LXMF router running")
    outcome: dict = {}
    done = threading.Event()
    lock = threading.Lock()

    # The first settlement wins: a callback and the timeout path race
    # under the lock, so a confirmation that lands at the timeout
    # boundary is either counted or ignored, never half-applied.
    def _settle(ok: bool) -> None:
        with lock:
            if "ok" not in outcome:
                outcome["ok"] = ok
                done.set()

    def _delivered(_message: Any) -> None:
        _settle(True)

    def _failed(_message: Any) -> None:
        _settle(False)

    message.register_delivery_callback(_delivered)
    message.register_failed_callback(_failed)
    try:
        router.handle_outbound(message)
    except Exception as exc:
        raise LXMFError(
            f"could not hand message to the LXMF router: {exc}"
        ) from exc
    if not done.wait(timeout):
        _settle(False)
    if outcome["ok"]:
        return True
    try:
        router.cancel_outbound(getattr(message, "message_id", None))
    except Exception:  # pragma: no cover - defensive
        log.debug("Could not cancel unconfirmed LXMF message", exc_info=True)
    return False

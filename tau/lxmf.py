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
import threading
from pathlib import Path
from typing import Any, Optional

import RNS

from .identity import load_public_key

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
    try:
        lxmf_module = importlib.import_module("LXMF")
    except ImportError as exc:
        raise LXMFError(
            "LXMF is not installed; install it with "
            f"{LXMF_INSTALL_HINT}"
        ) from exc
    router = lxmf_module.LXMRouter(identity=identity, storagepath=str(data_dir))
    delivery = router.register_delivery_identity(
        identity, display_name=display_name
    )
    if delivery is None:
        router.exit_handler()
        raise LXMFError("could not register the LXMF delivery identity")
    router.announce(delivery.hash)
    return router


def stop_lxmf_router(router: Optional[Any]) -> None:
    """Tear a router down via its exit handler.

    LXMRouter's exit handler is idempotent, so calling this twice — or
    letting the handler it registers with ``atexit`` run at process exit
    — is safe.  Passing ``None`` is a no-op.
    """
    if router is None:
        return
    router.exit_handler()


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

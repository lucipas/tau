"""LXMF addressing for tau.

LXMF — Reticulum's messaging layer — addresses a node at a delivery
destination derived from its Reticulum identity: app ``lxmf``, aspect
``delivery``.  That is the same destination LXMF's own ``LXMRouter``
listens on, so the hash computed here is the address any LXMF client
(including a future tau sender) uses to reach the node, and the address
an LXMF propagation node holds mail for.

This module is addressing only: it derives destination hashes with
``RNS.Destination.hash`` and never starts a router, sends a message, or
touches the network.  That keeps it usable from the CLI, the peer
registry, and tests without a live Reticulum instance.
"""

from __future__ import annotations

import RNS

LXMF_APP_NAME = "lxmf"
LXMF_DELIVERY_ASPECT = "delivery"
LXMF_PROPAGATION_ASPECT = "propagation"


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
    loaded = identity.load_public_key(bytes(public_key))
    # RNS 1.4.1's load_public_key returns None even on success — the
    # True/False return only arrived in a later release — and it also
    # returns None on failure, so the return value alone cannot tell
    # the two apart there. A successful load runs update_hashes(),
    # which sets identity.hash; a failed load never reaches it, so
    # the hash is the version-tolerant success signal (and an explicit
    # False still means failure on newer RNS).
    if loaded is False or identity.hash is None:
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

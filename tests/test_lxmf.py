"""Tests for LXMF addressing (tau/lxmf.py) and its peer/CLI surface."""

from __future__ import annotations

import time

import pytest
import RNS

from tau import lxmf
from tau.identity import destination_hash
from tau.peers import Peer


def test_delivery_hash_matches_rns_destination_hash() -> None:
    identity = RNS.Identity()
    assert lxmf.lxmf_delivery_hash(identity) == RNS.Destination.hash(
        identity, "lxmf", "delivery"
    )


def test_delivery_hash_matches_independent_formula() -> None:
    # Recompute the hash from RNS primitives the way Destination.hash
    # does, so the test does not just call the same helper twice: the
    # LXMF delivery destination is ("lxmf", "delivery") hashed with the
    # identity hash, full-hashed and truncated.
    identity = RNS.Identity()
    name_hash = RNS.Identity.full_hash(
        RNS.Destination.expand_name(None, "lxmf", "delivery").encode("utf-8")
    )[: RNS.Identity.NAME_HASH_LENGTH // 8]
    expected = RNS.Identity.full_hash(name_hash + identity.hash)[
        : RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
    ]
    assert lxmf.lxmf_delivery_hash(identity) == expected


def test_delivery_propagation_and_tau_hashes_all_differ() -> None:
    identity = RNS.Identity()
    delivery = lxmf.lxmf_delivery_hash(identity)
    propagation = lxmf.lxmf_propagation_hash(identity)
    assert len(delivery) == 16
    assert delivery != propagation
    assert delivery != destination_hash(identity)
    assert propagation != destination_hash(identity)


def test_delivery_hash_for_key_matches_identity() -> None:
    identity = RNS.Identity()
    public_key = identity.get_public_key()
    assert lxmf.lxmf_delivery_hash_for_key(public_key) == lxmf.lxmf_delivery_hash(
        identity
    )
    assert lxmf.lxmf_propagation_hash_for_key(
        public_key
    ) == lxmf.lxmf_propagation_hash(identity)


def test_delivery_hash_for_key_rejects_bad_keys() -> None:
    with pytest.raises(ValueError):
        lxmf.lxmf_delivery_hash_for_key(bytes(32))
    with pytest.raises(ValueError):
        lxmf.lxmf_delivery_hash_for_key("not bytes")  # type: ignore[arg-type]


def test_delivery_hash_for_key_tolerates_none_return(monkeypatch) -> None:
    # RNS 1.4.1's load_public_key returns None even on success (the
    # True/False return came later). The helper must accept that.
    real_load = RNS.Identity.load_public_key

    def load_without_return(self, pub_bytes):
        real_load(self, pub_bytes)
        return None

    monkeypatch.setattr(RNS.Identity, "load_public_key", load_without_return)
    identity = RNS.Identity()
    assert lxmf.lxmf_delivery_hash_for_key(
        identity.get_public_key()
    ) == lxmf.lxmf_delivery_hash(identity)


def test_delivery_hash_for_key_rejects_failed_load_returning_none(monkeypatch) -> None:
    # 1.4.1 also returns None on failure, so a load that never sets the
    # identity hash must still be rejected.
    def load_noop(self, pub_bytes):
        return None

    monkeypatch.setattr(RNS.Identity, "load_public_key", load_noop)
    with pytest.raises(ValueError):
        lxmf.lxmf_delivery_hash_for_key(RNS.Identity().get_public_key())


def test_peer_lxmf_hash_derived_from_public_key() -> None:
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=100.0,
        last_seen=200.0,
    )
    expected = lxmf.lxmf_delivery_hash(identity).hex()
    assert peer.lxmf_hash == expected
    assert peer.to_dict()["lxmf"] == expected
    assert peer.public_dict(now=200.0)["lxmf"] == expected


def test_peer_lxmf_hash_round_trips_through_dict() -> None:
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=100.0,
        last_seen=200.0,
    )
    restored = Peer.from_dict(peer.to_dict())
    assert restored.lxmf_hash == peer.lxmf_hash


def test_peer_lxmf_hash_none_for_unloadable_key() -> None:
    peer = Peer(
        destination_hash=bytes(16),
        name="broken",
        public_key=bytes(32),
        first_seen=100.0,
        last_seen=200.0,
    )
    assert peer.lxmf_hash is None
    assert peer.to_dict()["lxmf"] is None


def test_peer_table_shows_lxmf_column(capsys) -> None:
    from tau import cli

    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=100.0,
        last_seen=200.0,
    )
    cli._print_peer_table([peer], now=200.0)
    out = capsys.readouterr().out
    assert "LXMF" in out
    assert peer.lxmf_hash in out

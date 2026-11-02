from __future__ import annotations

import json
import os
import stat

import pytest

from tau.peers import (
    AmbiguousPeer,
    Peer,
    PeerError,
    PeerRegistry,
    PUBLIC_KEY_BYTES,
)


def make_peer(registry: PeerRegistry, index: int, name: str, *, now: float) -> Peer:
    return registry.upsert(
        bytes([index]) * 16,
        bytes([index]) * PUBLIC_KEY_BYTES,
        name=name,
        hops=index,
        now=now,
    )


def test_upsert_and_lookup(tmp_path):
    registry = PeerRegistry(tmp_path / "peers.json")
    peer = make_peer(registry, 1, "alice", now=100.0)

    assert len(registry) == 1
    assert registry.get(peer.destination_hash) is peer
    assert registry.get(peer.hexhash) is peer
    assert registry.get("ALICE") is peer
    assert "alice" in registry
    assert registry.resolve(peer.hexhash[:6]) is peer


def test_update_preserves_first_seen_and_ignores_unknown_name(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 2, "bob", now=100.0)
    updated = registry.upsert(
        peer.destination_hash,
        peer.public_key,
        name="unknown",
        hops=4,
        now=200.0,
    )
    assert updated is peer
    assert peer.name == "bob"
    assert peer.first_seen == 100.0
    assert peer.last_seen == 200.0
    assert peer.hops == 4

    registry.upsert(
        peer.destination_hash, peer.public_key, name="robert", hops=2, now=300.0
    )
    assert peer.name == "robert"
    assert peer.first_seen == 100.0


def test_ambiguous_names_and_prefixes(tmp_path):
    registry = PeerRegistry()
    make_peer(registry, 3, "duplicate", now=1.0)
    make_peer(registry, 4, "duplicate", now=2.0)
    registry.upsert(b"\x09" * 16, b"\x09" * PUBLIC_KEY_BYTES, name="nine", now=3.0)
    registry.upsert(
        b"\x09" * 15 + b"\x0a", b"\x0a" * PUBLIC_KEY_BYTES, name="ten", now=4.0
    )

    with pytest.raises(AmbiguousPeer):
        registry.resolve("duplicate")
    with pytest.raises(AmbiguousPeer):
        registry.resolve("0909")


def test_resolve_rejects_bad_input(tmp_path):
    registry = PeerRegistry()
    with pytest.raises(PeerError):
        registry.resolve(b"short")
    assert registry.resolve("") is None
    assert registry.resolve("missing") is None
    with pytest.raises(PeerError):
        registry.resolve(123)


def test_upsert_validates_key_sizes(tmp_path):
    registry = PeerRegistry()
    with pytest.raises(ValueError):
        registry.upsert(b"short", b"x" * PUBLIC_KEY_BYTES)
    with pytest.raises(ValueError):
        registry.upsert(b"x" * 16, b"short")


def test_persistence_roundtrip_and_permissions(tmp_path):
    path = tmp_path / "peers.json"
    registry = PeerRegistry(path)
    make_peer(registry, 5, "carol", now=123.0)
    assert registry.save() is True
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600

    raw = json.loads(path.read_text())
    assert raw["version"] == 1
    assert len(raw["peers"]) == 1

    loaded = PeerRegistry(path)
    assert len(loaded) == 1
    peer = loaded.all()[0]
    assert peer.name == "carol"
    assert peer.destination_hash == b"\x05" * 16
    assert peer.public_key == b"\x05" * PUBLIC_KEY_BYTES


def test_load_ignores_corrupt_files_and_entries(tmp_path):
    path = tmp_path / "peers.json"
    path.write_text("not json at all")
    assert len(PeerRegistry(path)) == 0

    path.write_text(
        json.dumps(
            {
                "version": 1,
                "peers": [
                    {"destination": "nope"},
                    {"destination": "00" * 16, "public_key": "00" * 64, "last_seen": 1},
                ],
            }
        )
    )
    registry = PeerRegistry(path)
    assert len(registry) == 1


def test_prune_and_age_filter(tmp_path):
    registry = PeerRegistry()
    make_peer(registry, 6, "old", now=100.0)
    make_peer(registry, 7, "new", now=1000.0)

    assert len(registry.all(max_age=500.0, now=1100.0)) == 1
    assert registry.prune(max_age=500.0, now=1100.0) == 1
    assert len(registry) == 1


def test_max_peers_drops_oldest(tmp_path):
    registry = PeerRegistry(max_peers=2)
    make_peer(registry, 8, "one", now=1.0)
    make_peer(registry, 9, "two", now=2.0)
    make_peer(registry, 10, "three", now=3.0)

    names = {peer.name for peer in registry.all()}
    assert names == {"two", "three"}


def test_public_dict_hides_keys(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 11, "dave", now=100.0)
    public = peer.public_dict(now=160.0)
    assert "public_key" not in public
    assert public["age"] == 60.0
    assert "public_key" in peer.to_dict()

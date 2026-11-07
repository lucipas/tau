from __future__ import annotations

import json
import os
import stat

import RNS
import pytest

from tau.peers import (
    AmbiguousPeer,
    Peer,
    PeerError,
    PeerRegistry,
    PUBLIC_KEY_BYTES,
)
from tau.protocol import APP_NAME, ASPECT


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


def test_record_delivered_tracks_history_and_persists(tmp_path):
    registry = PeerRegistry(tmp_path / "peers.json")
    peer = make_peer(registry, 3, "carol", now=100.0)
    assert peer.delivered_count == 0
    assert peer.last_delivered_at is None
    assert registry.record_delivered(b"\x09" * 16) is False

    assert registry.record_delivered(peer.destination_hash, now=150.0) is True
    assert registry.record_delivered(peer.destination_hash, now=160.0) is True
    assert peer.delivered_count == 2
    assert peer.last_delivered_at == 160.0
    registry.save()

    reloaded = PeerRegistry(tmp_path / "peers.json")
    stored = reloaded.get(peer.destination_hash)
    assert stored.delivered_count == 2
    assert stored.last_delivered_at == 160.0
    # An upsert for the same peer keeps its delivered history.
    reloaded.upsert(
        peer.destination_hash, peer.public_key, name="carol", now=200.0
    )
    assert stored.delivered_count == 2
    assert stored.last_delivered_at == 160.0


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


def test_prune_keeps_guarded_peers(tmp_path):
    registry = PeerRegistry()
    guarded = make_peer(registry, 6, "old", now=100.0)
    make_peer(registry, 7, "new", now=1000.0)

    assert registry.prune(max_age=500.0, now=1100.0, keep=[guarded.destination_hash]) == 0
    assert len(registry) == 2
    assert registry.get(guarded.destination_hash) is guarded


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


# -- peer exchange -------------------------------------------------------

def make_exchange_entry(name="carol", *, last_seen=1000.0, hops=2, protocol=1):
    """A valid exchange entry backed by a real Reticulum identity."""
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    entry = {
        "destination": destination.hex(),
        "name": name,
        "public_key": identity.get_public_key().hex(),
        "last_seen": last_seen,
        "hops": hops,
        "protocol": protocol,
    }
    return entry, identity, destination


def test_exchange_dict_includes_key(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 11, "dave", now=100.0)
    exchanged = peer.exchange_dict(now=160.0)
    assert exchanged["public_key"] == peer.public_key.hex()
    assert exchanged["destination"] == peer.hexhash
    assert "public_key" not in peer.public_dict(now=160.0)


def test_public_peers_include_keys_only_when_asked(tmp_path):
    registry = PeerRegistry()
    make_peer(registry, 12, "erin", now=100.0)
    assert "public_key" not in registry.public_peers(now=160.0)[0]
    assert "public_key" in registry.public_peers(now=160.0, include_keys=True)[0]


def test_merge_exchanged_adds_new_peer(tmp_path):
    registry = PeerRegistry()
    entry, _identity, destination = make_exchange_entry(
        "carol", last_seen=900.0, hops=2
    )
    summary = registry.merge_exchanged([entry], now=1000.0)

    assert summary == {"added": 1, "updated": 0, "skipped": 0}
    peer = registry.get(destination)
    assert peer is not None
    assert peer.name == "carol"
    assert peer.public_key.hex() == entry["public_key"]
    assert peer.first_seen == 1000.0
    assert peer.last_seen == 900.0
    assert peer.hops == 3  # one more hop than the reporting peer's distance


def test_merge_exchanged_tolerates_none_returning_load(
    tmp_path, load_public_key_without_return
):
    # RNS 1.4.1's load_public_key returns None even on success;
    # exchanged entries with valid keys must still be accepted.
    registry = PeerRegistry()
    entry, _identity, destination = make_exchange_entry(
        "carol", last_seen=900.0, hops=2
    )
    summary = registry.merge_exchanged([entry], now=1000.0)

    assert summary == {"added": 1, "updated": 0, "skipped": 0}
    assert registry.get(destination) is not None


def test_merge_exchanged_rejects_key_hash_mismatch(tmp_path):
    registry = PeerRegistry()
    entry, _identity, _destination = make_exchange_entry("mallory")
    other = RNS.Identity()
    entry["public_key"] = other.get_public_key().hex()

    summary = registry.merge_exchanged([entry], now=1000.0)
    assert summary == {"added": 0, "updated": 0, "skipped": 1}
    assert len(registry) == 0


def test_merge_exchanged_skips_self_malformed_and_stale(tmp_path):
    registry = PeerRegistry()
    own_entry, _own_identity, own_destination = make_exchange_entry(
        "self", last_seen=900.0
    )
    stale_entry, _stale_identity, _stale_destination = make_exchange_entry(
        "stale", last_seen=1000.0 - 7 * 86400 - 1
    )
    good_entry, _good_identity, good_destination = make_exchange_entry(
        "good", last_seen=900.0
    )
    future_entry, _future_identity, future_destination = make_exchange_entry(
        "future", last_seen=5000.0
    )
    bad_entries = [
        "not a dict",
        {"destination": "zz", "public_key": "00" * 64, "last_seen": 900.0},
        {"destination": good_entry["destination"], "last_seen": 900.0},
        {
            "destination": good_entry["destination"],
            "public_key": "00" * 64,
            "last_seen": 900.0,
        },
    ]
    summary = registry.merge_exchanged(
        [own_entry, stale_entry, *bad_entries, good_entry, future_entry],
        now=1000.0,
        self_hash=own_destination,
    )

    assert summary == {"added": 2, "updated": 0, "skipped": 6}
    assert registry.get(good_destination) is not None
    # A future last_seen is clamped to now, not trusted.
    assert registry.get(future_destination).last_seen == 1000.0


def test_merge_exchanged_never_refreshes_known_peer(tmp_path):
    registry = PeerRegistry()
    entry, _identity, destination = make_exchange_entry("carol", last_seen=900.0)
    registry.merge_exchanged([entry], now=1000.0)
    known = registry.get(destination)
    assert known.last_seen == 900.0

    # A later, fresher-sounding claim from a third party must not move our
    # own liveness record, and a different name must not overwrite ours.
    fresher = dict(entry, last_seen=1500.0, name="caroline")
    summary = registry.merge_exchanged([fresher], now=2000.0)
    assert summary == {"added": 0, "updated": 0, "skipped": 0}
    assert known.last_seen == 900.0
    assert known.first_seen == 1000.0
    assert known.name == "carol"


def test_merge_exchanged_fills_in_unknown_name(tmp_path):
    registry = PeerRegistry()
    entry, _identity, destination = make_exchange_entry(
        "carol", last_seen=900.0
    )
    registry.merge_exchanged([dict(entry, name="unknown")], now=1000.0)
    assert registry.get(destination).name == "unknown"

    summary = registry.merge_exchanged([entry], now=1100.0)
    assert summary == {"added": 0, "updated": 1, "skipped": 0}
    assert registry.get(destination).name == "carol"


def test_merge_exchanged_ignores_non_list(tmp_path):
    registry = PeerRegistry()
    assert registry.merge_exchanged({"not": "a list"}, now=1000.0) == {
        "added": 0,
        "updated": 0,
        "skipped": 0,
    }
    assert registry.merge_exchanged(None, now=1000.0) == {
        "added": 0,
        "updated": 0,
        "skipped": 0,
    }


def test_merge_exchanged_rejects_nonfinite_and_bool_timestamps(tmp_path):
    registry = PeerRegistry()
    entries = []
    for bad_last_seen in (
        float("nan"),
        float("inf"),
        float("-inf"),
        True,
        10**400,  # overflows float() conversion
        "900",
    ):
        entry, _identity, _destination = make_exchange_entry(
            "bad", last_seen=1000.0
        )
        entry["last_seen"] = bad_last_seen
        entries.append(entry)

    summary = registry.merge_exchanged(entries, now=1000.0)
    assert summary == {"added": 0, "updated": 0, "skipped": len(entries)}
    assert len(registry) == 0


def test_merge_exchanged_marks_via_exchange_and_upsert_clears_it(tmp_path):
    registry = PeerRegistry()
    entry, _identity, destination = make_exchange_entry(
        "carol", last_seen=900.0
    )
    registry.merge_exchanged([entry], now=1000.0)
    assert registry.get(destination).via_exchange is True

    # Hearing the peer directly (its own announce) promotes it.
    peer = registry.get(destination)
    registry.upsert(
        destination, peer.public_key, name="carol", now=1100.0
    )
    assert registry.get(destination).via_exchange is False
    assert registry.get(destination).last_seen == 1100.0


def test_merge_exchanged_drops_colliding_names(tmp_path):
    registry = PeerRegistry()
    make_peer(registry, 1, "alice", now=100.0)

    # A reporter must not shadow alice's name with another identity.
    entry, _identity, destination = make_exchange_entry(
        "alice", last_seen=900.0
    )
    registry.merge_exchanged([entry], now=1000.0)
    assert registry.get(destination).name == "unknown"

    # Nor fill in a missing name with one another peer already uses.
    other_entry, _other_identity, other_destination = make_exchange_entry(
        "bob", last_seen=900.0
    )
    registry.merge_exchanged([dict(other_entry, name="unknown")], now=1000.0)
    assert registry.get(other_destination).name == "unknown"
    summary = registry.merge_exchanged(
        [dict(other_entry, name="alice")], now=1100.0
    )
    assert summary["updated"] == 0
    assert registry.get(other_destination).name == "unknown"


def test_merge_exchanged_never_evicts_kept_peers(tmp_path):
    registry = PeerRegistry(max_peers=2)
    kept = make_peer(registry, 1, "one", now=100.0)
    make_peer(registry, 2, "two", now=200.0)

    entry, _identity, destination = make_exchange_entry(
        "carol", last_seen=900.0
    )
    summary = registry.merge_exchanged(
        [entry], now=1000.0, keep=[kept.destination_hash]
    )
    assert summary["added"] == 1
    assert len(registry) == 2
    assert registry.get(kept.destination_hash) is not None
    assert registry.get(destination) is not None
    # The oldest unprotected peer was evicted instead.
    assert {p.name for p in registry.all()} == {"one", "carol"}


def test_merge_exchanged_exceeds_cap_rather_than_evicting_all_kept(tmp_path):
    registry = PeerRegistry(max_peers=2)
    first = make_peer(registry, 1, "one", now=100.0)
    second = make_peer(registry, 2, "two", now=200.0)

    entry, _identity, destination = make_exchange_entry(
        "carol", last_seen=900.0
    )
    summary = registry.merge_exchanged(
        [entry],
        now=1000.0,
        keep=[first.destination_hash, second.destination_hash],
    )
    assert summary["added"] == 1
    assert len(registry) == 3
    assert registry.get(destination) is not None

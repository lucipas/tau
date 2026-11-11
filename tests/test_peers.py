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
    safety_number,
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


def test_remove_drops_peer_and_persists(tmp_path):
    registry = PeerRegistry(tmp_path / "peers.json")
    peer = make_peer(registry, 1, "alice", now=100.0)
    make_peer(registry, 2, "bob", now=100.0)

    removed = registry.remove(peer.destination_hash)
    assert removed is peer
    assert len(registry) == 1
    assert registry.get(peer.destination_hash) is None
    assert registry.remove(peer.destination_hash) is None

    registry.save()
    reloaded = PeerRegistry(tmp_path / "peers.json")
    assert len(reloaded) == 1
    assert reloaded.get(peer.destination_hash) is None
    assert reloaded.get("bob") is not None


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


def test_from_dict_rejects_nonfinite_timestamps():
    # JSON accepts NaN/Infinity literals, so a hand-edited registry file
    # can carry them; a non-finite timestamp used to crash every reader
    # that formats an age (e.g. `tau peers show` via _ago).
    registry = PeerRegistry()
    peer = make_peer(registry, 3, "carol", now=100.0)
    raw = peer.to_dict()
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError):
            Peer.from_dict(dict(raw, last_seen=bad))
        # A non-finite first_seen falls back to last_seen, like a missing one.
        recovered = Peer.from_dict(dict(raw, first_seen=bad))
        assert recovered.first_seen == peer.last_seen
        assert recovered.last_seen == peer.last_seen

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


# -- local aliases ----------------------------------------------------------


def test_alias_roundtrip_persists(tmp_path):
    path = tmp_path / "peers.json"
    registry = PeerRegistry(path)
    peer = make_peer(registry, 1, "alice", now=100.0)
    registry.set_alias(peer.hexhash, "my laptop")

    reloaded = PeerRegistry(path)
    stored = reloaded.get(peer.destination_hash)
    assert stored is not None
    assert stored.alias == "my laptop"
    assert stored.display_name == "my laptop"


def test_display_name_prefers_alias_over_name(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 2, "bob", now=100.0)
    assert peer.display_name == "bob"
    registry.set_alias(peer.hexhash, "relay-berlin")
    assert peer.display_name == "relay-berlin"


def test_resolve_and_get_accept_alias(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 3, "carol", now=100.0)
    registry.set_alias(peer.hexhash, "carol-laptop")
    assert registry.resolve("carol-laptop") is peer
    assert registry.resolve("CAROL-LAPTOP") is peer
    assert registry.get("carol-laptop") is peer
    # The asserted name still resolves too.
    assert registry.resolve("carol") is peer


def test_alias_wins_over_another_peers_name(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 4, "dave", now=100.0)
    make_peer(registry, 5, "laptop", now=200.0)
    registry.set_alias(peer.hexhash, "laptop")
    assert registry.resolve("laptop") is peer


def test_set_alias_rejects_duplicates_case_insensitively(tmp_path):
    registry = PeerRegistry()
    first = make_peer(registry, 6, "erin", now=100.0)
    second = make_peer(registry, 7, "frank", now=200.0)
    registry.set_alias(first.hexhash, "spare")
    with pytest.raises(PeerError, match="already used"):
        registry.set_alias(second.hexhash, "SPARE")
    # Re-setting the same alias on the same peer is fine.
    assert registry.set_alias(first.hexhash, "spare") is first


def test_set_alias_rejects_empty_and_unknown_peer(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 8, "grace", now=100.0)
    with pytest.raises(PeerError, match="must not be empty"):
        registry.set_alias(peer.hexhash, "   ")
    with pytest.raises(PeerError, match="unknown peer"):
        registry.set_alias("missing", "spare")


def test_set_alias_rejects_hash_like_values(tmp_path):
    registry = PeerRegistry()
    other = make_peer(registry, 10, "ivan", now=100.0)
    peer = make_peer(registry, 11, "judy", now=200.0)
    # A full hash and a hash prefix of another peer must not become aliases.
    with pytest.raises(PeerError, match="looks like a destination hash"):
        registry.set_alias(peer.hexhash, other.hexhash)
    with pytest.raises(PeerError, match="looks like a destination hash"):
        registry.set_alias(peer.hexhash, other.hexhash[:8])
    assert peer.alias is None


def test_clear_alias_and_clearing_without_one(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 9, "heidi", now=100.0)
    with pytest.raises(PeerError, match="has no alias"):
        registry.clear_alias(peer.hexhash)
    registry.set_alias(peer.hexhash, "home")
    assert registry.clear_alias("home") is peer
    assert peer.alias is None
    assert peer.display_name == "heidi"
    assert registry.resolve("home") is None


def test_upsert_preserves_alias(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 10, "ivan", now=100.0)
    registry.set_alias(peer.hexhash, "workstation")
    registry.upsert(
        peer.destination_hash,
        peer.public_key,
        name="ivan-new",
        hops=1,
        now=200.0,
    )
    assert peer.alias == "workstation"
    assert peer.name == "ivan-new"


def test_alias_never_leaves_the_local_registry(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 11, "judy", now=100.0)
    registry.set_alias(peer.hexhash, "secret-label")
    assert "alias" not in peer.public_dict()
    assert "alias" not in peer.exchange_dict()
    assert peer.to_dict()["alias"] == "secret-label"


def test_from_dict_tolerates_bad_alias_values(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 12, "ken", now=100.0)
    raw = peer.to_dict()
    raw["alias"] = 42
    assert Peer.from_dict(raw).alias is None
    raw["alias"] = "   "
    assert Peer.from_dict(raw).alias is None


def test_set_alias_rejects_hash_like_aliases(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 13, "mia", now=100.0)
    other = make_peer(registry, 14, "nick", now=200.0)
    # Another peer's full destination hash, its hash prefix, and any
    # hex-only string long enough to be read as a prefix are all refused.
    with pytest.raises(PeerError, match="looks like a destination hash"):
        registry.set_alias(peer.hexhash, other.hexhash)
    with pytest.raises(PeerError, match="looks like a destination hash"):
        registry.set_alias(peer.hexhash, other.hexhash[:8])
    with pytest.raises(PeerError, match="looks like a destination hash"):
        registry.set_alias(peer.hexhash, "deadbeef")
    # A short hex string cannot be a prefix, so it is a fine alias.
    assert registry.set_alias(peer.hexhash, "abc").alias == "abc"


def test_from_dict_drops_hash_like_alias(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 15, "olive", now=100.0)
    raw = peer.to_dict()
    raw["alias"] = peer.hexhash[:8]
    assert Peer.from_dict(raw).alias is None


def test_refresh_aliases_picks_up_another_process(tmp_path):
    path = tmp_path / "peers.json"
    cli = PeerRegistry(path)
    peer = make_peer(cli, 16, "pam", now=100.0)
    cli.save()
    # A second registry loaded earlier, as a running node's snapshot.
    node_view = PeerRegistry(path)
    assert node_view.get(peer.destination_hash).alias is None

    cli.set_alias(peer.hexhash, "field-kit")
    assert node_view.refresh_aliases() == 1
    assert node_view.get(peer.destination_hash).alias == "field-kit"
    # The node's next save keeps the alias instead of erasing it.
    node_view.save()
    assert PeerRegistry(path).get(peer.destination_hash).alias == "field-kit"

    cli.clear_alias(peer.hexhash)
    assert node_view.refresh_aliases() == 1
    assert node_view.get(peer.destination_hash).alias is None
    assert node_view.refresh_aliases() == 0


def test_refresh_aliases_ignores_unknown_peers_and_missing_keys(tmp_path):
    path = tmp_path / "peers.json"
    registry = PeerRegistry(path)
    peer = make_peer(registry, 17, "quinn", now=100.0)
    registry.save()

    raw = json.loads(path.read_text())
    raw["peers"].append(
        {"destination": "00" * 16, "alias": "ghost", "name": "ghost"}
    )
    raw["peers"][0].pop("alias")
    path.write_text(json.dumps(raw))

    assert registry.refresh_aliases() == 0
    assert registry.get(peer.destination_hash).alias is None


def test_resolve_can_exclude_aliases(tmp_path):
    registry = PeerRegistry()
    named = make_peer(registry, 18, "laptop", now=100.0)
    aliased = make_peer(registry, 19, "dave", now=200.0)
    registry.set_alias(aliased.hexhash, "laptop")
    # Aliases win by default...
    assert registry.resolve("laptop") is aliased
    # ...but a security boundary can ask for asserted names only.
    assert registry.resolve("laptop", include_alias=False) is named
    assert registry.resolve("LAPTOP", include_alias=False) is named
    assert registry.resolve("dave", include_alias=False) is aliased
    assert registry.resolve("laptop", include_alias=True) is aliased


def test_load_drops_duplicate_aliases(tmp_path):
    path = tmp_path / "peers.json"
    registry = PeerRegistry(path)
    first = make_peer(registry, 20, "rachel", now=100.0)
    second = make_peer(registry, 21, "sam", now=200.0)
    registry.set_alias(first.hexhash, "shared")
    registry.set_alias(second.hexhash, "other")
    registry.save()

    # Two writers racing can leave the same alias on two peers; the
    # file keeps the first occurrence and drops the second on load.
    raw = json.loads(path.read_text())
    raw["peers"][1]["alias"] = "SHARED"
    path.write_text(json.dumps(raw))

    reloaded = PeerRegistry(path)
    assert reloaded.get(first.destination_hash).alias == "shared"
    assert reloaded.get(second.destination_hash).alias is None
    assert reloaded.resolve("shared") is reloaded.get(first.destination_hash)


def test_refresh_aliases_never_creates_a_collision(tmp_path):
    path = tmp_path / "peers.json"
    node_view = PeerRegistry(path)
    first = make_peer(node_view, 22, "tina", now=100.0)
    second = make_peer(node_view, 23, "uma", now=200.0)
    node_view.set_alias(first.hexhash, "field-kit")

    # A stale one-shot snapshot that omits the first peer and gives its
    # alias to the second one.
    raw = json.loads(path.read_text())
    raw["peers"] = [entry for entry in raw["peers"] if entry["destination"] != first.hexhash]
    raw["peers"][0]["alias"] = "field-kit"
    path.write_text(json.dumps(raw))

    assert node_view.refresh_aliases() == 0
    # The first peer kept its alias (it was absent from the file) and
    # the colliding alias was not applied to the second peer.
    assert node_view.get(first.destination_hash).alias == "field-kit"
    assert node_view.get(second.destination_hash).alias is None
    assert node_view.resolve("field-kit") is node_view.get(first.destination_hash)

    # The node's next save heals the file instead of persisting the clash.
    node_view.save()
    healed = PeerRegistry(path)
    assert healed.get(first.destination_hash).alias == "field-kit"
    assert healed.get(second.destination_hash).alias is None


def test_safety_number_is_grouped_symmetric_and_deterministic():
    key_a = b"\x01" * PUBLIC_KEY_BYTES
    key_b = b"\x02" * PUBLIC_KEY_BYTES

    number = safety_number(key_a, key_b)
    groups = number.split(" ")
    assert len(groups) == 12
    assert all(len(group) == 5 and group.isdigit() for group in groups)
    # Same from either side of the pair, and stable across calls.
    assert safety_number(key_b, key_a) == number
    assert safety_number(key_a, key_b) == number
    # A different key gives a different number.
    assert safety_number(key_a, b"\x03" * PUBLIC_KEY_BYTES) != number
    # A single key is that identity's own fingerprint.
    fingerprint = safety_number(key_a)
    assert len(fingerprint.split(" ")) == 12
    assert fingerprint != number


def test_safety_number_rejects_bad_keys():
    with pytest.raises(ValueError):
        safety_number()
    with pytest.raises(ValueError):
        safety_number(b"short")


def test_mark_verified_records_fingerprint_and_survives_reload(tmp_path):
    path = tmp_path / "peers.json"
    registry = PeerRegistry(path)
    peer = make_peer(registry, 1, "alice", now=100.0)
    assert not peer.is_verified

    registry.mark_verified(peer.destination_hash, "00001 00002", now=500.0)
    assert peer.is_verified
    assert peer.verified_at == 500.0
    assert peer.verified_fingerprint == "00001 00002"
    assert registry.save() is True

    loaded = PeerRegistry(path)
    reloaded = loaded.get(peer.destination_hash)
    assert reloaded is not None
    assert reloaded.is_verified
    assert reloaded.verified_at == 500.0
    assert reloaded.verified_fingerprint == "00001 00002"


def test_mark_verified_rejects_unknown_and_ambiguous_targets(tmp_path):
    registry = PeerRegistry()
    make_peer(registry, 1, "alice", now=100.0)
    with pytest.raises(PeerError):
        registry.mark_verified("missing", "00001", now=100.0)

    make_peer(registry, 2, "duplicate", now=100.0)
    make_peer(registry, 3, "duplicate", now=100.0)
    with pytest.raises(PeerError):
        registry.mark_verified("duplicate", "00001", now=100.0)


def test_upsert_refuses_key_change_and_stamps_it(tmp_path, caplog):
    registry = PeerRegistry()
    peer = make_peer(registry, 1, "alice", now=100.0)

    with caplog.at_level("WARNING", logger="tau.peers"):
        updated = registry.upsert(
            peer.destination_hash,
            b"\x02" * PUBLIC_KEY_BYTES,
            name="mallory",
            hops=1,
            now=200.0,
        )

    # The recorded key — and the verification hanging off it — is kept.
    assert updated is peer
    assert peer.public_key == b"\x01" * PUBLIC_KEY_BYTES
    assert peer.name == "alice"
    assert peer.last_seen == 100.0
    assert peer.key_change_at == 200.0
    assert any("different public key" in record.message for record in caplog.records)


def test_upsert_key_change_keeps_verification(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 1, "alice", now=100.0)
    registry.mark_verified(peer.destination_hash, "00001", now=150.0)

    registry.upsert(
        peer.destination_hash, b"\x02" * PUBLIC_KEY_BYTES, now=200.0
    )
    assert peer.is_verified
    assert peer.verified_fingerprint == "00001"
    assert peer.public_key == b"\x01" * PUBLIC_KEY_BYTES


def test_prune_spares_verified_peers(tmp_path):
    registry = PeerRegistry()
    verified = make_peer(registry, 1, "alice", now=100.0)
    registry.mark_verified(verified.destination_hash, "00001", now=150.0)
    make_peer(registry, 2, "bob", now=100.0)

    assert registry.prune(max_age=500.0, now=1100.0) == 1
    assert registry.get(verified.destination_hash) is verified
    assert len(registry) == 1


def test_max_peers_spares_verified_peers(tmp_path):
    registry = PeerRegistry(max_peers=2)
    verified = make_peer(registry, 1, "one", now=1.0)
    registry.mark_verified(verified.destination_hash, "00001", now=1.0)
    make_peer(registry, 2, "two", now=2.0)
    make_peer(registry, 3, "three", now=3.0)

    names = {peer.name for peer in registry.all()}
    assert names == {"one", "three"}


def test_local_dict_includes_trust_state_public_dict_does_not(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 1, "alice", now=100.0)
    registry.mark_verified(peer.destination_hash, "00001", now=150.0)

    assert "verified" not in peer.public_dict(now=200.0)
    assert "verified_at" not in peer.public_dict(now=200.0)
    local = peer.local_dict(now=200.0)
    assert local["verified"] is True
    assert local["verified_at"] == 150.0
    assert local["key_change_at"] is None


def test_local_dict_includes_alias_public_dict_does_not(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 1, "alice", now=100.0)

    assert peer.local_dict(now=200.0)["alias"] is None
    registry.set_alias(peer.destination_hash, "laptop")

    local = peer.local_dict(now=200.0)
    assert local["alias"] == "laptop"
    # The alias is owner-only, like verification: it never leaves in a
    # public description or a peer-exchange payload.
    assert "alias" not in peer.public_dict(now=200.0)
    assert "alias" not in peer.exchange_dict(now=200.0)


def test_from_dict_tolerates_bad_verification_fields(tmp_path):
    path = tmp_path / "peers.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "peers": [
                    {
                        "destination": "00" * 16,
                        "public_key": "00" * 64,
                        "last_seen": 1,
                        "verified_at": "not-a-time",
                        "verified_fingerprint": 42,
                        "key_change_at": float("nan"),
                    }
                ],
            }
        )
    )
    registry = PeerRegistry(path)
    assert len(registry) == 1
    peer = registry.all()[0]
    assert not peer.is_verified
    assert peer.verified_fingerprint is None
    assert peer.key_change_at is None

def test_mark_verified_clears_key_change_warning(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 1, "alice", now=100.0)
    registry.upsert(
        peer.destination_hash, b"\x02" * PUBLIC_KEY_BYTES, now=200.0
    )
    assert peer.key_change_at == 200.0

    registry.mark_verified(peer.destination_hash, "00001", now=300.0)
    assert peer.is_verified
    # Re-verifying answers the warning: it must not warn forever after.
    assert peer.key_change_at is None


def test_clear_verifications_drops_all_markers(tmp_path):
    registry = PeerRegistry()
    first = make_peer(registry, 1, "alice", now=100.0)
    second = make_peer(registry, 2, "bob", now=100.0)
    registry.mark_verified(first.destination_hash, "00001", now=150.0)
    registry.mark_verified(second.destination_hash, "00002", now=150.0)

    assert registry.clear_verifications() == 2
    assert not first.is_verified
    assert first.verified_fingerprint is None
    assert not second.is_verified
    assert registry.clear_verifications() == 0


def test_forget_removes_peer_and_rejects_unknown(tmp_path):
    registry = PeerRegistry()
    peer = make_peer(registry, 1, "alice", now=100.0)
    make_peer(registry, 2, "bob", now=100.0)

    removed = registry.forget("alice")
    assert removed is peer
    assert len(registry) == 1
    assert registry.get("alice") is None

    with pytest.raises(PeerError):
        registry.forget("alice")

    make_peer(registry, 3, "duplicate", now=100.0)
    make_peer(registry, 4, "duplicate", now=100.0)
    with pytest.raises(PeerError):
        registry.forget("duplicate")

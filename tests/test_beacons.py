"""Live location beacons (issue #468): store, wire marker, node tick."""

from __future__ import annotations

import json
import time

import pytest
import RNS

from tau.beacons import (
    BEACON_DEFAULT_DURATION,
    BEACON_UPDATE_INTERVAL,
    Beacon,
    Beacons,
    BeaconsError,
    beacons_lock,
    coarsen_location,
    load_beacons,
    read_location_file,
    save_beacons,
)
from tau.config import TauConfig
from tau.dm_requests import DmRequest
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.mailbox import Mailbox, open_message, seal_message
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    APP_NAME,
    ASPECT,
    ProtocolError,
    clean_beacon,
    derive_group_id,
    encode_group_message,
    encode_message,
    parse_group_message,
    parse_message,
)

PIN = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
PEER_HASH = "ab" * 16
GROUP_HASH = "cd" * 16
NOW = 1_800_000_000.0


def test_clean_beacon_accepts_valid_marker():
    assert clean_beacon(
        {"id": "aabbccdd", "expires_at": NOW + 3600, "seq": 3}
    ) == {"id": "aabbccdd", "expires_at": NOW + 3600.0, "seq": 3}
    assert clean_beacon(None) is None


def test_clean_beacon_rejects_malformed_markers():
    assert clean_beacon({"expires_at": NOW + 3600, "seq": 1}) is None
    assert clean_beacon({"id": "", "expires_at": NOW + 3600, "seq": 1}) is None
    assert clean_beacon({"id": "x" * 65, "expires_at": NOW + 3600, "seq": 1}) is None
    assert clean_beacon({"id": "a", "expires_at": "soon", "seq": 1}) is None
    assert clean_beacon({"id": "a", "expires_at": float("inf"), "seq": 1}) is None
    assert clean_beacon({"id": "a", "expires_at": NOW + 3600, "seq": 0}) is None
    assert clean_beacon({"id": "a", "expires_at": NOW + 3600, "seq": 1.5}) is None
    assert clean_beacon({"id": "a", "expires_at": NOW + 3600, "seq": True}) is None
    assert clean_beacon("beacon") is None


def test_coarsen_location_rounds_down_and_drops_name():
    assert coarsen_location(PIN) == {"lat": 34.05, "lon": -83.88}
    assert coarsen_location({"lat": 34.059, "lon": -83.881}) == {
        "lat": 34.05,
        "lon": -83.89,
    }
    with pytest.raises(BeaconsError):
        coarsen_location({"lat": 91, "lon": 0})


def test_read_location_file(tmp_path):
    path = tmp_path / "position.json"
    path.write_text(json.dumps(PIN), encoding="utf-8")
    assert read_location_file(path) == PIN
    assert read_location_file(tmp_path / "missing.json") is None
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert read_location_file(broken) is None
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"lat": 91, "lon": 0}), encoding="utf-8")
    assert read_location_file(bad) is None


def _beacons_with_one():
    beacons = Beacons()
    beacon = beacons.start(
        target=PEER_HASH,
        target_kind="dm",
        target_name="alice",
        location=PIN,
        now=NOW,
    )
    return beacons, beacon


def test_beacon_active_due_and_expired():
    _, beacon = _beacons_with_one()
    assert beacon.is_active(NOW)
    assert not beacon.is_due(NOW)
    assert beacon.is_due(NOW + BEACON_UPDATE_INTERVAL)
    assert beacon.is_active(NOW + BEACON_DEFAULT_DURATION - 1)
    assert not beacon.is_active(NOW + BEACON_DEFAULT_DURATION)
    assert not beacon.is_due(NOW + BEACON_DEFAULT_DURATION)


def test_beacons_start_refuses_second_active_beacon():
    beacons, _ = _beacons_with_one()
    with pytest.raises(BeaconsError):
        beacons.start(
            target=PEER_HASH,
            target_kind="dm",
            location=PIN,
            now=NOW + 10,
        )


def test_beacons_stop_and_update_location():
    beacons, beacon = _beacons_with_one()
    updated = beacons.update_location(
        PEER_HASH, "dm", location={"lat": 35.0, "lon": -84.0}, now=NOW + 60
    )
    assert updated.location == {"lat": 35.0, "lon": -84.0}
    assert beacons.by_target(PEER_HASH, "dm", NOW + 60) is beacon
    stopped = beacons.stop(PEER_HASH, "dm", NOW + 120)
    assert stopped is beacon
    assert not beacon.is_active(NOW + 120)
    assert beacons.stop(PEER_HASH, "dm", NOW + 120) is None
    with pytest.raises(BeaconsError):
        beacons.update_location(PEER_HASH, "dm", location=PIN, now=NOW + 130)


def test_beacons_stop_by_id():
    beacons, beacon = _beacons_with_one()
    assert beacons.stop_by_id("00" * 8, NOW + 60) is None
    stopped = beacons.stop_by_id(beacon.id, NOW + 60)
    assert stopped is beacon
    assert not beacon.is_active(NOW + 60)
    # Stopping again is a no-op, not an error.
    assert beacons.stop_by_id(beacon.id, NOW + 60) is None
    # An expired beacon is already over, so it cannot be stopped.
    expired = beacons.start(
        target=GROUP_HASH,
        target_kind="group",
        target_name="crew",
        location=PIN,
        duration=60.0,
        now=NOW,
    )
    assert beacons.stop_by_id(expired.id, NOW + 3600) is None
    assert expired.stopped is False


def test_beacons_start_validates_duration_and_interval():
    beacons = Beacons()
    with pytest.raises(BeaconsError):
        beacons.start(
            target=PEER_HASH, target_kind="dm", location=PIN,
            duration=0, now=NOW,
        )
    with pytest.raises(BeaconsError):
        beacons.start(
            target=PEER_HASH, target_kind="dm", location=PIN,
            duration=9 * 3600, now=NOW,
        )
    with pytest.raises(BeaconsError):
        beacons.start(
            target=PEER_HASH, target_kind="dm", location=PIN,
            interval=30, now=NOW,
        )


def test_beacons_persist_roundtrip(tmp_path):
    path = tmp_path / "beacons.json"
    beacons, beacon = _beacons_with_one()
    beacons.start(
        target=GROUP_HASH,
        target_kind="group",
        target_name="crew",
        location=PIN,
        coarse=True,
        location_file="/tmp/position.json",
        now=NOW,
    )
    with beacons_lock(path):
        save_beacons(path, beacons)
    loaded = load_beacons(path)
    assert set(loaded.beacons) == set(beacons.beacons)
    reloaded = loaded.get(beacon.id)
    assert reloaded is not None
    assert reloaded.target == PEER_HASH
    assert reloaded.target_kind == "dm"
    assert reloaded.location == PIN
    assert reloaded.next_update_at == NOW + BEACON_UPDATE_INTERVAL
    group = next(
        b for b in loaded.beacons.values() if b.target_kind == "group"
    )
    assert group.coarse is True
    assert group.location_file == "/tmp/position.json"


def test_beacon_current_location_rereads_file_and_coarsens(tmp_path):
    path = tmp_path / "position.json"
    path.write_text(
        json.dumps({"lat": 34.059, "lon": -83.881, "name": "my house"}),
        encoding="utf-8",
    )
    beacon = Beacon(
        id="aabbccdd",
        target=PEER_HASH,
        target_kind="dm",
        started_at=NOW,
        expires_at=NOW + 3600,
        location=PIN,
        location_file=str(path),
    )
    # The file moves the beacon; coarse drops the precise name.
    assert beacon.current_location() == {
        "lat": 34.059,
        "lon": -83.881,
        "name": "my house",
    }
    beacon.coarse = True
    assert beacon.current_location() == {"lat": 34.05, "lon": -83.89}
    # A broken file keeps the last position instead of failing.
    path.write_text("{broken", encoding="utf-8")
    beacon.coarse = False
    assert beacon.current_location() == PIN


def test_message_roundtrip_with_beacon():
    marker = {"id": "aabbccdd", "expires_at": NOW + 3600, "seq": 2}
    data = encode_message(
        "on my way", message_id="b" * 32, location=PIN, beacon=marker,
        kind="location",
    )
    parsed = parse_message(data)
    assert parsed["location"] == PIN
    assert parsed["beacon"] == marker
    # A beacon without a pin is rejected: an update that renders
    # nowhere is noise.
    with pytest.raises(ProtocolError):
        encode_message("on my way", beacon=marker)
    # A malformed marker is rejected at encode time, dropped at parse.
    with pytest.raises(ProtocolError):
        encode_message(
            "on my way", location=PIN, beacon={"id": "a", "seq": 0},
        )


def test_group_message_roundtrip_with_beacon():
    marker = {"id": "aabbccdd", "expires_at": NOW + 3600, "seq": 1}
    members = [PEER_HASH, GROUP_HASH]
    group_id = derive_group_id(PEER_HASH, "crew", members)
    data = encode_group_message(
        "on my way",
        group_id=group_id,
        group_name="crew",
        creator=PEER_HASH,
        members=members,
        location=PIN,
        beacon=marker,
    )
    parsed = parse_group_message(data)
    assert parsed["location"] == PIN
    assert parsed["beacon"] == marker


def test_seal_and_open_carries_beacon():
    sender = RNS.Identity()
    recipient = RNS.Identity()
    marker = {"id": "aabbccdd", "expires_at": NOW + 3600, "seq": 1}
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-beacon",
        text="on my way",
        sent_at=4242.0,
        location=PIN,
        beacon=marker,
        kind="location",
    )
    message = open_message(recipient, blob)
    assert message["location"] == PIN
    assert message["beacon"] == marker


def test_outbox_entry_roundtrip_carries_beacon(tmp_path):
    mailbox = Mailbox(tmp_path / "mailbox.json")
    marker = {"id": "aabbccdd", "expires_at": NOW + 3600, "seq": 1}
    mailbox.enqueue(
        to=bytes.fromhex(PEER_HASH),
        text="on my way",
        message_id="c" * 32,
        location=PIN,
        beacon=marker,
        kind="location",
    )
    mailbox.save()
    reloaded = Mailbox(tmp_path / "mailbox.json")
    entries = reloaded.outbox_list()
    assert len(entries) == 1
    assert entries[0].location == PIN
    assert entries[0].beacon == marker


def test_inbox_keeps_beacon_marker(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    marker = {"id": "aabbccdd", "expires_at": NOW + 3600, "seq": 2}
    inbox.append(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "on my way",
            "kind": "location",
            "sent_at": NOW,
            "received_at": NOW,
            "id": "d" * 32,
            "direction": "in",
            "location": PIN,
            "beacon": marker,
        }
    )
    reloaded = Inbox(tmp_path / "inbox.json")
    entries = reloaded.list_messages(limit=None)
    assert len(entries) == 1
    assert entries[0]["beacon"] == marker
    assert entries[0]["location"] == PIN


def test_dm_request_keeps_beacon_marker():
    marker = {"id": "aabbccdd", "expires_at": NOW + 3600, "seq": 1}
    request = DmRequest(
        id="e" * 32,
        sender=PEER_HASH,
        text="on my way",
        kind="location",
        sent_at=NOW,
        received_at=NOW,
        request_expires_at=NOW + 7 * 86400,
        location=PIN,
        beacon=marker,
    )
    assert DmRequest.from_dict(request.to_dict()).beacon == marker


def make_peer(name: str = "alice") -> Peer:
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    return Peer(
        destination_hash=destination,
        name=name,
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )


def make_node(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    return TauNode(config, registry=PeerRegistry(config.peers_path))


def test_node_start_beacon_resolves_registered_peer(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    beacon = node.start_beacon("alice", PIN, now=NOW)
    assert beacon.target == peer.hexhash
    assert beacon.target_kind == "dm"
    assert beacon.target_name == "alice"
    assert beacon.next_update_at == NOW + BEACON_UPDATE_INTERVAL
    assert beacon.expires_at == NOW + BEACON_DEFAULT_DURATION
    with pytest.raises(NodeError):
        node.start_beacon("alice", PIN, now=NOW + 10)
    with pytest.raises(NodeError):
        node.start_beacon("nobody", PIN, now=NOW + 10)


def test_node_send_beacon_update_sends_marker_and_advances(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    beacon = node.start_beacon("alice", PIN, now=NOW)
    calls = []

    def fake_send(target, text, **kwargs):
        calls.append((target, text, kwargs))
        return {"ok": True, "queued": True, "id": "f" * 32}

    monkeypatch.setattr(node, "send_message", fake_send)
    node.send_beacon_update(beacon, now=NOW)
    assert len(calls) == 1
    target, text, kwargs = calls[0]
    assert target == peer.destination_hash
    assert text == "Rock Springs Park"
    assert kwargs["kind"] == "location"
    assert kwargs["location"] == PIN
    assert kwargs["beacon"] == {
        "id": beacon.id,
        "expires_at": beacon.expires_at,
        "seq": 1,
    }
    assert beacon.updates_sent == 1
    assert beacon.next_update_at == NOW + BEACON_UPDATE_INTERVAL
    # The counters persisted: a fresh load sees the sent update.
    stored = load_beacons(node.config.beacons_path)
    assert stored.get(beacon.id).updates_sent == 1
    # The next update carries sequence 2.
    node.send_beacon_update(beacon, now=NOW + BEACON_UPDATE_INTERVAL)
    assert calls[1][2]["beacon"]["seq"] == 2


def test_node_send_beacon_update_coarse_and_file(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    position = tmp_path / "position.json"
    position.write_text(
        json.dumps({"lat": 34.059, "lon": -83.881, "name": "my house"}),
        encoding="utf-8",
    )
    beacon = node.start_beacon(
        "alice",
        PIN,
        coarse=True,
        location_file=str(position),
        now=NOW,
    )
    calls = []

    def fake_send(target, text, **kwargs):
        calls.append(kwargs)
        return {"ok": True, "queued": True, "id": "f" * 32}

    monkeypatch.setattr(node, "send_message", fake_send)
    node.send_beacon_update(beacon, now=NOW)
    # Coarse: the file's fresh position, rounded, nameless.
    assert calls[0]["location"] == {"lat": 34.05, "lon": -83.89}
    assert calls[0]["beacon"]["seq"] == 1


def test_send_due_beacon_updates_sends_only_due(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    beacons = Beacons()
    due = beacons.start(
        target=peer.hexhash, target_kind="dm", location=PIN, now=NOW
    )
    beacons.start(
        target="ee" * 16,
        target_kind="dm",
        location=PIN,
        interval=3600,
        now=NOW,
    )
    save_beacons(node.config.beacons_path, beacons)
    sent = []

    def fake_update(beacon, *, now=None):
        sent.append(beacon.id)
        beacon.updates_sent += 1
        beacon.next_update_at = (now or NOW) + beacon.interval

    monkeypatch.setattr(node, "send_beacon_update", fake_update)
    monkeypatch.setattr(time, "time", lambda: NOW + BEACON_UPDATE_INTERVAL)
    try:
        node._send_due_beacon_updates()
    finally:
        monkeypatch.undo()
    assert sent == [due.id]


def test_node_group_beacon_update(tmp_path, monkeypatch):
    import types

    node = make_node(tmp_path)
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    # A bare destination stand-in: only the hexhash is consulted, and
    # building a live RNS destination needs a running Reticulum.
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    alice = make_peer("alice")
    bob = make_peer("bob")
    group = node.groups.create(
        "crew",
        [alice.hexhash, bob.hexhash, own_hash],
        created_by=alice.hexhash,
    )
    beacon = node.start_beacon("crew", PIN, group=True, now=NOW)
    assert beacon.target == group["id"]
    assert beacon.target_kind == "group"
    calls = []

    def fake_group_send(record, text, **kwargs):
        calls.append((record, text, kwargs))
        return {"ok": True, "results": []}

    monkeypatch.setattr(node, "send_group_message", fake_group_send)
    node.send_beacon_update(beacon, now=NOW)
    assert len(calls) == 1
    record, text, kwargs = calls[0]
    assert record["id"] == group["id"]
    assert kwargs["kind"] == "location"
    assert kwargs["location"] == PIN
    assert kwargs["beacon"]["id"] == beacon.id
    assert kwargs["beacon"]["seq"] == 1

# -- broadcast-list beacons (issue #820) -------------------------------------

def test_beacons_start_broadcast_validates_name():
    beacons = Beacons()
    beacon = beacons.start(
        target="  Family  ", target_kind="broadcast", location=PIN, now=NOW
    )
    assert beacon.target == "Family"
    assert beacon.target_kind == "broadcast"
    assert beacon.target_name == ""
    # The name is cleaned, not lowercased: "Family" stays "Family".
    with pytest.raises(BeaconsError):
        beacons.start(target="   ", target_kind="broadcast", location=PIN, now=NOW)
    with pytest.raises(BeaconsError):
        beacons.start(target=123, target_kind="broadcast", location=PIN, now=NOW)
    # A second active beacon for the same list is refused, like a
    # second beacon for the same peer or group.
    with pytest.raises(BeaconsError):
        beacons.start(
            target="family", target_kind="broadcast", location=PIN, now=NOW
        )
    # Round-trip: the kind and the name survive the store.
    restored = Beacons.from_dict(beacons.to_dict())
    assert restored.get(beacon.id).target == "Family"
    assert restored.get(beacon.id).target_kind == "broadcast"


def _make_broadcast_node(tmp_path, members=("alice", "bob")):
    import types

    node = make_node(tmp_path)
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    # A bare destination stand-in: only the hexhash is consulted, and
    # building a live RNS destination needs a running Reticulum.
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    peers = {}
    for name in members:
        peer = make_peer(name)
        node.registry.upsert(
            peer.destination_hash, peer.public_key, name=peer.name, now=1.0
        )
        peers[name] = peer
    node.broadcasts.create(
        "family", [peers[name].hexhash for name in members] + [own_hash]
    )
    return node, own_hash, peers


def test_node_start_beacon_broadcast_resolves_list(tmp_path):
    node, own_hash, peers = _make_broadcast_node(tmp_path)
    beacon = node.start_beacon("family", PIN, broadcast=True, now=NOW)
    assert beacon.target == "family"
    assert beacon.target_kind == "broadcast"
    assert beacon.target_name == "family"
    assert beacon.next_update_at == NOW + BEACON_UPDATE_INTERVAL
    # Name lookup is case-insensitive, like the list store itself.
    with pytest.raises(NodeError):
        node.start_beacon("Family", PIN, broadcast=True, now=NOW + 10)
    with pytest.raises(NodeError):
        node.start_beacon("nobody", PIN, broadcast=True, now=NOW + 10)
    with pytest.raises(NodeError):
        node.start_beacon("family", PIN, group=True, broadcast=True, now=NOW + 10)


def test_node_broadcast_beacon_update_fans_out_per_member(tmp_path, monkeypatch):
    node, own_hash, peers = _make_broadcast_node(tmp_path)
    beacon = node.start_beacon("family", PIN, broadcast=True, now=NOW)
    calls = []

    def fake_send(target, text, **kwargs):
        calls.append((target, text, kwargs))
        return {"ok": True, "id": "f" * 32}

    monkeypatch.setattr(node, "send_message", fake_send)
    result = node.send_beacon_update(beacon, now=NOW)
    # One DM per member — and never to this node itself.
    assert len(calls) == 2
    assert {bytes(c[0]).hex() for c in calls} == {
        peers["alice"].hexhash,
        peers["bob"].hexhash,
    }
    for _target, text, kwargs in calls:
        assert text == "Rock Springs Park"
        assert kwargs["kind"] == "location"
        assert kwargs["location"] == PIN
        assert kwargs["beacon"] == {
            "id": beacon.id,
            "expires_at": beacon.expires_at,
            "seq": 1,
        }
    assert result["ok"] is True
    assert result["broadcast"] == "family"
    assert result["sent_at"] == NOW
    # Legs follow the list's stored order, and the store keeps members
    # sorted by hash — not the order they were added in.
    assert [leg["to"] for leg in result["results"]] == sorted(
        [peers["alice"].hexhash, peers["bob"].hexhash]
    )
    assert all(leg["delivered"] for leg in result["results"])
    assert beacon.updates_sent == 1
    assert beacon.next_update_at == NOW + BEACON_UPDATE_INTERVAL
    stored = load_beacons(node.config.beacons_path)
    assert stored.get(beacon.id).updates_sent == 1


def test_node_broadcast_beacon_update_resolves_members_each_tick(
    tmp_path, monkeypatch
):
    node, own_hash, peers = _make_broadcast_node(tmp_path)
    beacon = node.start_beacon("family", PIN, broadcast=True, now=NOW)
    calls = []

    def fake_send(target, text, **kwargs):
        calls.append(target)
        return {"ok": True, "id": "f" * 32}

    monkeypatch.setattr(node, "send_message", fake_send)
    node.send_beacon_update(beacon, now=NOW)
    assert len(calls) == 2
    # Removing a member stops their updates at the next tick.
    node.broadcasts.remove_members("family", [peers["bob"].hexhash])
    node.send_beacon_update(beacon, now=NOW + BEACON_UPDATE_INTERVAL)
    assert len(calls) == 3
    assert bytes(calls[2]).hex() == peers["alice"].hexhash
    # Deleting the list fails the tick, like an unknown group.
    node.broadcasts.delete("family")
    with pytest.raises(NodeError):
        node.send_beacon_update(beacon, now=NOW + 2 * BEACON_UPDATE_INTERVAL)


def test_node_broadcast_beacon_update_self_only_list_fails_tick(tmp_path):
    # A list containing only this node has no one to send to: the
    # tick fails like a deleted list instead of counting as sent.
    node, own_hash, peers = _make_broadcast_node(tmp_path, members=())
    beacon = node.start_beacon("family", PIN, broadcast=True, now=NOW)
    with pytest.raises(NodeError, match="has no members to send to"):
        node.send_beacon_update(beacon, now=NOW)
    assert beacon.updates_sent == 0
    assert beacon.next_update_at == NOW + BEACON_UPDATE_INTERVAL


def test_node_broadcast_beacon_update_failed_leg_keeps_going(
    tmp_path, monkeypatch
):
    from tau.node import NotAllowedError

    node, own_hash, peers = _make_broadcast_node(tmp_path)
    beacon = node.start_beacon("family", PIN, broadcast=True, now=NOW)
    calls = []

    def fake_send(target, text, **kwargs):
        calls.append(target)
        if bytes(target).hex() == peers["bob"].hexhash:
            raise NotAllowedError("refused")
        return {"ok": True, "id": "f" * 32}

    monkeypatch.setattr(node, "send_message", fake_send)
    result = node.send_beacon_update(beacon, now=NOW)
    assert len(calls) == 2
    assert result["ok"] is False
    by_member = {leg["to"]: leg for leg in result["results"]}
    assert by_member[peers["alice"].hexhash]["delivered"] is True
    assert by_member[peers["bob"].hexhash]["ok"] is False
    # The tick still counts as sent: the schedule advances even
    # when one leg failed, like a group fan-out with a refused leg.
    assert beacon.updates_sent == 1

from __future__ import annotations

import json
import logging
import threading
import time
from types import SimpleNamespace

import RNS
import pytest

from tau.config import TauConfig
from tau.inbox import Inbox
from tau.mailbox import Mailbox, MailboxError, MailboxFull
from tau.node import (
    AnnouncementsOnlyError,
    NodeError,
    NotAllowedError,
    NotAMemberError,
    PeerRefusedError,
    PeerRejectedError,
    TauNode,
    _AnnounceHandler,
)
from tau.peers import Peer, PeerRegistry
from tau.protocol import APP_NAME, ASPECT, build_announce, parse_message


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


def test_destination_for_rebuilds_the_same_destination(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer()
    destination = node._destination_for(peer)
    assert destination.hash == peer.destination_hash
    assert destination.hexhash == peer.hexhash


def test_destination_for_tolerates_none_returning_load(
    tmp_path, load_public_key_without_return
):
    # RNS 1.4.1's load_public_key returns None even on success; the
    # node must still resolve a peer's destination from its key.
    node = make_node(tmp_path)
    peer = make_peer()
    destination = node._destination_for(peer)
    assert destination.hash == peer.destination_hash


def test_destination_for_rejects_mismatched_identity(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer()
    peer.destination_hash = b"\x00" * 16
    with pytest.raises(NodeError):
        node._destination_for(peer)


def test_resolve_peer_uses_registry_when_not_started(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    assert node.resolve_peer(peer.hexhash) is not None
    assert node.resolve_peer("alice") is not None
    assert node.resolve_peer(peer) is peer


def test_resolve_unknown_peer_without_identity_data(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.resolve_peer("nobody")
    with pytest.raises(NodeError):
        node.resolve_peer(b"\x00" * 16)


def _make_self_destination():
    identity = RNS.Identity()
    destination_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    return identity, destination_hash


def test_resolve_peer_refuses_own_hash(tmp_path):
    node = make_node(tmp_path)
    identity, own_hash = _make_self_destination()
    node.destination = SimpleNamespace(hash=own_hash, hexhash=own_hash.hex())
    with pytest.raises(NodeError, match="cannot resolve this node as a peer"):
        node.resolve_peer(own_hash)
    with pytest.raises(NodeError, match="cannot resolve this node as a peer"):
        node.resolve_peer(own_hash.hex())
    self_peer = Peer(
        destination_hash=own_hash,
        name="tester",
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    with pytest.raises(NodeError, match="cannot resolve this node as a peer"):
        node.resolve_peer(self_peer)
    assert node.registry.get(own_hash) is None


def test_wait_for_peer_refuses_own_hash_without_recall(tmp_path, monkeypatch):
    # The circular loop: Reticulum remembers the node's own announced
    # identity, so recalling the node's own hash succeeds and the
    # registry gains the node as its own peer. The guard must fire
    # before any path request or recall happens.
    node = make_node(tmp_path)
    identity, own_hash = _make_self_destination()
    node.destination = SimpleNamespace(hash=own_hash, hexhash=own_hash.hex())
    recalled = []
    monkeypatch.setattr(
        RNS.Identity,
        "recall",
        staticmethod(lambda dest_hash: recalled.append(dest_hash) or identity),
    )
    requested = []
    monkeypatch.setattr(
        RNS.Transport,
        "request_path",
        staticmethod(lambda dest_hash: requested.append(dest_hash)),
    )
    with pytest.raises(NodeError, match="cannot resolve this node as a peer"):
        node.wait_for_peer(own_hash, timeout=0.1)
    assert recalled == []
    assert requested == []
    assert node.registry.get(own_hash) is None


def test_drop_self_peer_purges_registry_and_persists(tmp_path):
    node = make_node(tmp_path)
    identity, own_hash = _make_self_destination()
    node.destination = SimpleNamespace(hash=own_hash, hexhash=own_hash.hex())
    node.registry.upsert(
        own_hash, identity.get_public_key(), name="tester", now=time.time()
    )
    other = make_peer()
    node.registry.upsert(
        other.destination_hash, other.public_key, name=other.name, now=time.time()
    )
    assert len(node.registry) == 2

    node._drop_self_peer()

    assert node.registry.self_hash == own_hash
    assert node.registry.get(own_hash) is None
    assert node.registry.get(other.destination_hash) is not None
    reloaded = PeerRegistry(node.config.peers_path)
    assert reloaded.get(own_hash) is None
    assert reloaded.get(other.destination_hash) is not None


def test_send_message_to_self_never_enters_outbox(tmp_path):
    # The resolve guard raises before delivery, but send_message's
    # NodeError fallback re-derives the target hash and queues the
    # send — a self-targeted send must be rejected there too, never
    # queued for store-and-forward to our own hash (Qodo Fixer, #623).
    node = make_node(tmp_path)
    identity, own_hash = _make_self_destination()
    node.destination = SimpleNamespace(hash=own_hash, hexhash=own_hash.hex())
    node.reticulum = SimpleNamespace()  # started, so the queue fallback is live
    with pytest.raises(NodeError, match="cannot resolve this node as a peer"):
        node.send_message(own_hash.hex(), "hello self")
    assert node.mailbox.outbox_list() == []
    assert node.registry.get(own_hash) is None


def test_info_requires_start(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.info()
    with pytest.raises(NodeError):
        node.send_message("alice", "hi")


def test_send_message_sends_kind_on_the_wire(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    sent = {}

    def fake_request(peer_arg, path, payload, timeout):
        sent["payload"] = payload
        return {"ok": True, "delivered": True}

    monkeypatch.setattr(node, "_request", fake_request)
    node.send_message("alice", "are you there?", kind="ping")
    assert parse_message(sent["payload"])["kind"] == "ping"


def test_send_message_sends_location_on_the_wire(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    sent = {}

    def fake_request(peer_arg, path, payload, timeout):
        sent["payload"] = payload
        return {"ok": True, "delivered": True}

    monkeypatch.setattr(node, "_request", fake_request)
    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    node.send_message("alice", "meet here", location=pin)
    parsed = parse_message(sent["payload"])
    assert parsed["location"] == pin
    # A pin without an explicit kind is a location message, so the
    # receiver can filter on it; an explicit kind wins.
    assert parsed["kind"] == "location"
    node.send_message("alice", "meet here", kind="chat", location=pin)
    assert parse_message(sent["payload"])["kind"] == "chat"


def test_send_message_sends_poll_options_on_the_wire(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    sent = {}

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        sent["payload"] = payload
        return {"ok": True, "delivered": True}

    monkeypatch.setattr(node, "_request", fake_request)
    node.send_message("alice", "which day?", poll_options=["Saturday", "Sunday"])
    parsed = parse_message(sent["payload"])
    assert parsed["poll_options"] == ["Saturday", "Sunday"]
    assert parsed["poll_vote"] is None
    assert parsed["poll_close"] is None
    # The sender's own record carries the options and the shared wire
    # time, so the tally can resolve the poll it sent.
    stored = node.inbox.list_messages(direction="out")
    assert len(stored) == 1
    assert stored[0]["poll_options"] == ["Saturday", "Sunday"]
    assert stored[0]["leg_sent_at"] == parsed["sent_at"]


def test_send_message_rejects_bad_poll_options(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.send_message("alice", "which day?", poll_options=["only one"])
    with pytest.raises(NodeError):
        node.send_message(
            "alice", "which day?", poll_options=["Saturday", "saturday"]
        )
    with pytest.raises(NodeError):
        node.send_message(
            "alice",
            "which day?",
            poll_options=["Saturday", "Sunday"],
            poll_vote={
                "target": {"author": "ab" * 16, "sent_at": 1.0},
                "option": 0,
            },
        )


def test_send_message_vote_carries_target_and_option(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    sent = {}

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        sent["payload"] = payload
        return {"ok": True, "delivered": True}

    monkeypatch.setattr(node, "_request", fake_request)
    target = {"author": "ab" * 16, "sent_at": 1000.0}
    node.send_message(
        "alice", "voted: Saturday", poll_vote={"target": target, "option": 0}
    )
    parsed = parse_message(sent["payload"])
    assert parsed["poll_vote"] == {"target": target, "option": 0}
    stored = node.inbox.list_messages(direction="out")
    assert stored[0]["poll_vote"] == {"target": target, "option": 0}
    assert stored[0]["leg_sent_at"] == parsed["sent_at"]


def test_send_message_queues_poll_for_offline_peer(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    monkeypatch.setattr(node, "flush_outbox", lambda **kwargs: None)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )

    def unreachable(*args, **kwargs):
        raise NodeError("no path to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_message(
        "alice", "which day?", poll_options=["Saturday", "Sunday"]
    )

    assert response["queued"] is True
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.poll_options == ["Saturday", "Sunday"]
    # The retry re-sends the same poll payload, and the sent record
    # keeps the options and the shared wire time.
    sent = {}

    def reachable(peer_arg, path, payload, timeout, **kwargs):
        sent["payload"] = payload
        return {"ok": True, "delivered": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(peer, entry, 1.0) is True
    assert parse_message(sent["payload"])["poll_options"] == [
        "Saturday",
        "Sunday",
    ]
    stored = node.inbox.list_messages(direction="out")
    assert stored and stored[0]["poll_options"] == ["Saturday", "Sunday"]


def test_incoming_dm_poll_stores_poll_fields(tmp_path):
    from tau.protocol import decode, encode_message

    node, _, alice_identity, alice = _typing_node(tmp_path)
    response = node._handle_message(
        "/tau/message",
        encode_message(
            "which day?",
            message_id="m-poll",
            poll_options=["Saturday", "Sunday"],
        ),
        b"req-poll",
        alice_identity,
        1000.0,
    )
    assert decode(response)["ok"] is True
    stored = node.inbox.list_messages(peer=alice.hexhash)
    assert len(stored) == 1
    assert stored[0]["poll_options"] == ["Saturday", "Sunday"]
    assert stored[0].get("poll_vote") is None
    assert stored[0].get("poll_close") is None


def test_send_message_rejects_invalid_location(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.send_message("alice", "hi", location={"lat": 91, "lon": 0})


def test_send_message_queues_location_for_offline_peer(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    # The queue path spawns a background flush; stub it out so the
    # entry is still queued when the test inspects it.
    monkeypatch.setattr(node, "flush_outbox", lambda **kwargs: None)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )

    def unreachable(*args, **kwargs):
        raise NodeError("no path to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    response = node.send_message("alice", "meet here", location=pin)

    assert response["queued"] is True
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.location == pin
    # The retry re-sends the same pin, and the sent record keeps it.
    sent = {}

    def reachable(peer_arg, path, payload, timeout, **kwargs):
        sent["payload"] = payload
        return {"ok": True, "delivered": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(peer, entry, 1.0) is True
    assert parse_message(sent["payload"])["location"] == pin
    stored = node.inbox.list_messages(direction="out")
    assert stored and stored[0]["location"] == pin


def test_send_message_rejects_invalid_kind(tmp_path):
    from tau.protocol import ProtocolError

    node = make_node(tmp_path)
    with pytest.raises(ProtocolError):
        node.send_message("alice", "hi", kind="has space")


def test_send_message_queues_kind_for_offline_peer(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    # The queue path spawns a background flush; stub it out so the
    # entry is still queued when the test inspects it.
    monkeypatch.setattr(node, "flush_outbox", lambda **kwargs: None)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )

    def unreachable(*args, **kwargs):
        raise NodeError("no path to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_message("alice", "doorbell rang", kind="alert")

    assert response["queued"] is True
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.kind == "alert"


def test_queued_kind_survives_retry_on_the_wire(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    # The queue path spawns a background flush; stub it out so the
    # retry below is the only delivery attempt.
    monkeypatch.setattr(node, "flush_outbox", lambda **kwargs: None)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )

    def unreachable(*args, **kwargs):
        raise NodeError("no path to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_message("alice", "doorbell rang", kind="alert")
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.kind == "alert"

    sent = {}

    def reachable(peer_arg, path, payload, timeout, **kwargs):
        sent["payload"] = payload
        return {"ok": True, "delivered": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(peer, entry, 1.0) is True
    assert parse_message(sent["payload"])["kind"] == "alert"
    assert node.mailbox.get_outbox(response["id"]) is None


def test_send_message_records_sent_message_in_inbox(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "_request", lambda *args, **kwargs: {"ok": True})

    response = node.send_message("alice", "hello there")

    assert response["ok"] is True
    entries = node.inbox.list_messages(direction=None)
    assert len(entries) == 1
    entry = entries[0]
    assert entry["direction"] == "out"
    assert entry["destination"] == peer.hexhash
    assert entry["text"] == "hello there"
    assert entry["from"] == "tester"
    assert entry["id"]
    # The record survives a reload from disk.
    assert len(Inbox(node.config.inbox_path)) == 1


def test_send_message_records_queued_message_in_inbox(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )

    def _fail(*args, **kwargs):
        raise NodeError("no path to peer")

    monkeypatch.setattr(node, "_request", _fail)
    monkeypatch.setattr(node, "flush_outbox", lambda **kwargs: None)
    node.reticulum = object()  # started, without a live Reticulum

    response = node.send_message("alice", "queued hello")

    assert response["queued"] is True
    assert len(node.mailbox.outbox_list()) == 1
    entries = node.inbox.list_messages(direction=None)
    assert len(entries) == 1
    assert entries[0]["direction"] == "out"
    assert entries[0]["text"] == "queued hello"
    assert entries[0]["id"] == response["id"]


def test_send_message_records_nothing_when_peer_refuses(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )

    def _denied(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", _denied)
    with pytest.raises(NotAllowedError):
        node.send_message("alice", "hello")
    assert len(node.inbox) == 0


def _group_reaction_node(tmp_path):
    """A node with a signing identity and a crew group to react in."""
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
    for peer in (alice, bob):
        node.registry.upsert(
            peer.destination_hash, peer.public_key, name=peer.name, now=1.0
        )
    group = node.groups.create(
        "crew",
        [alice.hexhash, bob.hexhash, own_hash],
        created_by=alice.hexhash,
    )
    return node, group, alice, bob


def test_send_group_reaction_fans_out_one_signed_payload(tmp_path, monkeypatch):
    import base64

    from tau.protocol import REQUEST_REACT, parse_reaction, reaction_unsigned_bytes

    node, group, alice, bob = _group_reaction_node(tmp_path)
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append((peer_arg, path, payload))
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_reaction("crew", "m-1", "👍")

    assert response["ok"] is True
    assert response["group"] == group["id"]
    assert response["target"] == "m-1"
    assert response["reaction"] == "👍"
    assert response["cleared"] is False
    # Both members got a leg; this node never reacts to itself.
    # The roster is in sorted-hash order, so compare as a set.
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        bob.hexhash,
    }
    assert all(r.get("delivered") for r in response["results"])
    assert len(calls) == 2
    assert all(path == REQUEST_REACT for _, path, _ in calls)
    # One signed payload, one reaction id, for every leg.
    assert calls[0][2] == calls[1][2]
    payload = parse_reaction(calls[0][2])
    assert payload["target"] == "m-1"
    assert payload["reaction"] == "👍"
    assert payload["id"] == response["id"]
    assert node.identity.validate(
        base64.b64decode(payload["sig"]),
        reaction_unsigned_bytes(calls[0][2]),
    )
    # The local copy is recorded exactly once, under the same id.
    stored = node.inbox.reactions_for("m-1")
    assert len(stored) == 1
    assert stored[0]["id"] == response["id"]
    assert stored[0]["reaction"] == "👍"


def test_send_group_reaction_records_signed_send_time(tmp_path, monkeypatch):
    """The local reaction record carries the signed send time
    (CodeRabbit round-13 review).

    The payload is signed before the fan-out legs run; the local
    record used to take time.time() after the legs finished, so a
    reaction signed before a clear but recorded after it carried
    the later time and replaced the clear locally, while receivers
    — who order by the signed time — kept it.  Slowing each leg
    separates the two moments: the stored record must still match
    the signed payload exactly.
    """
    import time as _time

    from tau.protocol import REQUEST_REACT, parse_reaction

    node, group, alice, bob = _group_reaction_node(tmp_path)
    calls = []

    def slow_request(peer_arg, path, payload, timeout, deadline=None):
        _time.sleep(0.05)
        calls.append((peer_arg, path, payload))
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", slow_request)

    response = node.send_group_reaction("crew", "m-1", "👍")

    assert response["ok"] is True
    assert len(calls) == 2
    payload = parse_reaction(calls[0][2])
    stored = node.inbox.reactions_for("m-1")
    assert len(stored) == 1
    assert stored[0]["id"] == response["id"]
    assert stored[0]["sent_at"] == payload["sent_at"]


def test_send_group_reaction_reports_failed_leg(tmp_path, monkeypatch):
    node, _, alice, bob = _group_reaction_node(tmp_path)

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NotAllowedError("peer rejected the request: not allowed")
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_reaction("crew", "m-1", "👍")

    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("error") == "not_allowed"
    # One delivered leg is enough for the local copy, recorded once.
    stored = node.inbox.reactions_for("m-1")
    assert len(stored) == 1
    assert stored[0]["id"] == response["id"]


def test_send_group_reaction_records_nothing_when_every_leg_fails(
    tmp_path, monkeypatch
):
    node, _, _, _ = _group_reaction_node(tmp_path)

    def _fail(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", _fail)

    response = node.send_group_reaction("crew", "m-1", "👍")

    assert response["ok"] is False
    assert all(r.get("error") == "no route to peer" for r in response["results"])
    assert node.inbox.reactions_for("m-1") == []


def test_send_group_reaction_clear_fans_out_retraction(tmp_path, monkeypatch):
    node, _, alice, bob = _group_reaction_node(tmp_path)
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_reaction("crew", "m-1", "", timeout=5.0)

    assert response["ok"] is True
    assert response["cleared"] is True
    assert response["reaction"] == ""
    assert len(calls) == 2
    assert calls[0] == calls[1]
    from tau.protocol import parse_reaction

    assert parse_reaction(calls[0])["reaction"] == ""


def test_send_group_reaction_rejects_unknown_and_solo_groups(tmp_path):
    node, group, _, _ = _group_reaction_node(tmp_path)
    with pytest.raises(ValueError, match="unknown group"):
        node.send_group_reaction("nope", "m-1", "👍")
    # A group whose only member is this node cannot be created
    # (groups need at least 2 members), so pass the record directly.
    # A usage error, not a delivery failure: the console maps it to a
    # 400 and the CLI prints it plain.
    solo = {
        "id": "00" * 16,
        "name": "solo",
        "members": [node.destination.hexhash],
        "creator": node.destination.hexhash,
    }
    with pytest.raises(ValueError, match="no other members"):
        node.send_group_reaction(solo, "m-1", "👍")


def _reaction_node(tmp_path):
    """A node with a signing identity, started enough to queue."""
    import types

    node = make_node(tmp_path)
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    node.reticulum = object()  # started enough to queue
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    return node, peer


def test_send_reaction_queues_for_offline_peer(tmp_path, monkeypatch):
    from tau.protocol import REQUEST_REACT, parse_reaction

    node, peer = _reaction_node(tmp_path)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_reaction("alice", "m-1", "👍")

    assert response["queued"] is True
    assert response["id"]
    assert response["target"] == "m-1"
    # The local copy is recorded on delivery, not at queue time.
    assert node.inbox.reactions_for("m-1") == []
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.kind == "reaction"
    assert entry.to == peer.destination_hash
    assert entry.reaction["target"] == "m-1"
    assert entry.reaction["token"] == "👍"
    assert entry.reaction["reaction_id"] == response["id"]
    # The retry re-sends the identical signed bytes.
    sent = {}

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        sent["payload"] = payload
        assert path == REQUEST_REACT
        return {"ok": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(peer, entry, 1.0) is True
    payload = parse_reaction(sent["payload"])
    assert payload["target"] == "m-1"
    assert payload["reaction"] == "👍"
    assert payload["id"] == response["id"]
    assert node.mailbox.outbox_list() == []
    stored = node.inbox.reactions_for("m-1")
    assert len(stored) == 1
    assert stored[0]["id"] == response["id"]
    assert stored[0]["reaction"] == "👍"


def test_send_reaction_no_queue_raises_for_offline_peer(tmp_path, monkeypatch):
    node, _ = _reaction_node(tmp_path)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    with pytest.raises(NodeError, match="no route to peer"):
        node.send_reaction("alice", "m-1", "👍", queue=False)
    assert node.mailbox.outbox_list() == []


def test_send_reaction_refusal_is_not_queued(tmp_path, monkeypatch):
    node, _ = _reaction_node(tmp_path)

    def denied(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", denied)
    with pytest.raises(NotAllowedError):
        node.send_reaction("alice", "m-1", "👍")
    assert node.mailbox.outbox_list() == []
    assert node.inbox.reactions_for("m-1") == []


def _retract_node(tmp_path):
    """A node with a signing identity, started enough to queue, and one sent DM."""
    import types

    node = make_node(tmp_path)
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    node.reticulum = object()  # started enough to queue
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    node.inbox.append(
        {
            "from": "tester",
            "destination": peer.hexhash,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "m-1",
            "direction": "out",
        }
    )
    return node, peer


def test_send_retraction_queues_for_offline_peer(tmp_path, monkeypatch):
    from tau.protocol import REQUEST_RETRACT, parse_retract

    node, peer = _retract_node(tmp_path)
    retracted = []
    node.on_retract = retracted.append

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_retraction("alice", "m-1")

    assert response["queued"] is True
    assert response["id"]
    assert response["target"] == "m-1"
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.kind == "retract"
    assert entry.to == peer.destination_hash
    assert entry.retract["target"] == "m-1"
    assert entry.retract["retract_id"] == response["id"]
    # The sender's own copy stays until the retract delivers.
    assert node.inbox.resolve("m-1")["text"] == "hello"
    assert retracted == []
    # The retry re-sends the identical signed bytes.
    sent = {}

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        sent["payload"] = payload
        assert path == REQUEST_RETRACT
        return {"ok": True, "deleted": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(peer, entry, 1.0) is True
    payload = parse_retract(sent["payload"])
    assert payload["target"] == "m-1"
    assert payload["id"] == response["id"]
    assert node.mailbox.outbox_list() == []
    # Delivery drops the sender's own copies too, and records the
    # sent-retraction marker so the console drops its copy as well.
    with pytest.raises(ValueError):
        node.inbox.resolve("m-1")
    assert node.mailbox.is_sent_retracted("m-1")
    assert len(retracted) == 1
    assert retracted[0]["id"] == "m-1"


def test_send_retraction_no_queue_raises_for_offline_peer(tmp_path, monkeypatch):
    node, _ = _retract_node(tmp_path)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    with pytest.raises(NodeError, match="no route to peer"):
        node.send_retraction("alice", "m-1", queue=False)
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "hello"


def test_send_retraction_refusal_is_not_queued(tmp_path, monkeypatch):
    node, _ = _retract_node(tmp_path)

    def denied(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", denied)
    with pytest.raises(NotAllowedError):
        node.send_retraction("alice", "m-1")
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "hello"


def test_deliver_queued_retract_drops_refused_leg(tmp_path, monkeypatch):
    node, peer = _retract_node(tmp_path)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_retraction("alice", "m-1")
    entry = node.mailbox.outbox_list()[0]

    def refused(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", refused)
    assert node._deliver_entry(peer, entry, 1.0) is False
    # A refusal is final: the leg is dropped, not held until its TTL —
    # and the sender's own copy stays, since the retract never landed.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "hello"


def test_send_reaction_not_a_member_is_not_queued(tmp_path, monkeypatch):
    node, _ = _reaction_node(tmp_path)

    def removed(*args, **kwargs):
        raise NotAMemberError("peer rejected the request: not a member")

    monkeypatch.setattr(node, "_request", removed)
    with pytest.raises(NotAMemberError):
        node.send_reaction("alice", "m-1", "👍")
    # A not-a-member refusal is final, like any other: the reaction
    # must not be queued for a sweep that would only drop it.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.reactions_for("m-1") == []


def test_send_group_reaction_queues_unreachable_legs(tmp_path, monkeypatch):
    from tau.protocol import REQUEST_REACT, parse_reaction

    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NodeError("no route to peer")
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_group_reaction("crew", "m-1", "👍")

    assert response["ok"] is False
    assert response["queued"] == 1
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("queued") is True
    assert by_to[bob.hexhash].get("queued_id")
    # One delivered leg is enough for the local copy, recorded once.
    assert len(node.inbox.reactions_for("m-1")) == 1
    # The queued leg is one outbox entry with the group context.
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.kind == "reaction"
    assert entry.group_id == group["id"]
    assert entry.to == bob.destination_hash
    assert entry.reaction["reaction_id"] == response["id"]
    # The sweep delivers it with the same signed bytes, and the local
    # copy stays exactly one: the second leg does not fan out twice.
    sent = {}

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        sent["payload"] = payload
        assert path == REQUEST_REACT
        return {"ok": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(bob, entry, 1.0) is True
    assert parse_reaction(sent["payload"])["id"] == response["id"]
    assert node.mailbox.outbox_list() == []
    assert len(node.inbox.reactions_for("m-1")) == 1


def test_deliver_queued_reaction_drops_refused_leg(tmp_path, monkeypatch):
    node, peer = _reaction_node(tmp_path)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_reaction("alice", "m-1", "👍")
    entry = node.mailbox.outbox_list()[0]

    def refused(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", refused)
    assert node._deliver_entry(peer, entry, 1.0) is False
    # A refusal is final: the leg is dropped, not held until its TTL.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.reactions_for("m-1") == []


def test_deliver_queued_reaction_drops_not_a_member_leg(tmp_path, monkeypatch):
    node, peer = _reaction_node(tmp_path)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_reaction("alice", "m-1", "👍")
    entry = node.mailbox.outbox_list()[0]

    def removed(*args, **kwargs):
        raise NotAMemberError("peer rejected the request: not a member")

    monkeypatch.setattr(node, "_request", removed)
    assert node._deliver_entry(peer, entry, 1.0) is False
    # Not-a-member is a permanent refusal, like any other: the leg is
    # dropped instead of retrying until its TTL.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.reactions_for("m-1") == []


def test_send_group_reaction_block_list_refusal_is_not_queued(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            # The block-list drops silently with the generic
            # "unavailable" code, which _request raises as a plain
            # PeerRefusedError — not NotAllowedError.
            raise PeerRefusedError("peer rejected the request: unavailable")
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_group_reaction("crew", "m-1", "👍")

    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("ok") is False
    # The block-list refusal is reported, not queued: main reports
    # the refusal text itself for a plain PeerRefusedError.
    assert "unavailable" in by_to[bob.hexhash].get("error", "")
    # An explicit refusal is final: no leg is queued for bob, so the
    # sweep never retries a peer that already said no.
    assert node.mailbox.outbox_list() == []
    assert len(node.inbox.reactions_for("m-1")) == 1


def test_queued_group_reaction_delivers_in_announcements_only_group(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    # This node's copy says the group is announcements-only, and
    # this node is not the creator — a member post would be skipped.
    # A reaction is not a post: the live path accepts it, so the
    # queued retry must deliver it too.
    node.groups.record(
        group["id"],
        "crew",
        list(group["members"]),
        creator=alice.hexhash,
        sender=alice.hexhash,
        sent_at=200.0,
        announcements_only=True,
    )

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NodeError("no route to peer")
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_group_reaction("crew", "m-1", "👍")
    assert response["queued"] == 1
    entry = node.mailbox.outbox_list()[0]
    assert entry.kind == "reaction"

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        return {"ok": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(bob, entry, 1.0) is True
    assert node.mailbox.outbox_list() == []
    assert len(node.inbox.reactions_for("m-1")) == 1


def test_send_group_reaction_full_outbox_keeps_fan_out_going(
    tmp_path, monkeypatch
):
    from tau.protocol import REQUEST_REACT

    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NodeError("no route to peer")
        assert path == REQUEST_REACT
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    def full(*args, **kwargs):
        raise MailboxFull("outbox is full")

    monkeypatch.setattr(node.mailbox, "enqueue", full)
    response = node.send_group_reaction("crew", "m-1", "👍")

    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    # The full outbox fails only bob's leg; it must not abort the
    # fan-out or lose the local record for the delivered leg.
    assert by_to[bob.hexhash].get("ok") is False
    assert "could not queue: outbox is full" in by_to[bob.hexhash].get(
        "error", ""
    )
    assert node.mailbox.outbox_list() == []
    assert len(node.inbox.reactions_for("m-1")) == 1


def test_send_reaction_full_outbox_raises_node_error(tmp_path, monkeypatch):
    node, peer = _reaction_node(tmp_path)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)

    def full(*args, **kwargs):
        raise MailboxFull("outbox is full")

    monkeypatch.setattr(node.mailbox, "enqueue", full)
    # A full outbox is a delivery failure: cmd_react and the web API
    # catch NodeError, so it must arrive as one — not as the raw
    # MailboxError.
    with pytest.raises(NodeError, match="could not queue reaction"):
        node.send_reaction("alice", "m-1", "👍")
    assert node.mailbox.outbox_list() == []


def test_queued_group_reaction_is_never_deposited(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_group_reaction("crew", "m-1", "👍")
    entries = node.mailbox.outbox_list()
    assert len(entries) == 2
    assert all(entry.kind == "reaction" for entry in entries)

    calls = []

    def recorded(*args, **kwargs):
        calls.append(args)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", recorded)
    # No sealed copy is ever left with a holder for a group reaction:
    # legs carry a different wire id per member, so no single sealed
    # copy names every member's copy.
    assert node._deposit_entry(alice, entries[0], 1.0) is False
    assert node._deposit_entry(bob, entries[1], 1.0) is False
    assert calls == []
    assert len(node.mailbox.outbox_list()) == 2


def _typing_node(tmp_path):
    """A node with a destination stand-in and a known alice identity."""
    import types

    node = make_node(tmp_path)
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    # A bare destination stand-in: only the hexhash is consulted, and
    # building a live RNS destination needs a running Reticulum.
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    alice_identity = RNS.Identity()
    alice_destination = RNS.Destination.hash(alice_identity, APP_NAME, ASPECT)
    alice = Peer(
        destination_hash=alice_destination,
        name="alice",
        public_key=alice_identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    node.registry.upsert(
        alice.destination_hash, alice.public_key, name="alice", now=1.0
    )
    return node, identity, alice_identity, alice


def test_handle_typing_records_start_stop_and_expiry(tmp_path):
    from tau.protocol import decode, encode_typing

    node, _, alice_identity, alice = _typing_node(tmp_path)
    events = []
    node.on_typing = events.append

    response = node._handle_typing(
        "/tau/typing", encode_typing("start"), b"req-1", alice_identity, 1000.0
    )
    assert decode(response)["ok"] is True
    assert node.typing_peers(alice.hexhash) == [alice.hexhash]
    assert len(events) == 1
    assert events[0]["conversation"] == alice.hexhash
    assert events[0]["peer"] == alice.hexhash
    assert events[0]["name"] == "alice"
    assert events[0]["state"] == "start"
    assert events[0]["group"] is None

    response = node._handle_typing(
        "/tau/typing", encode_typing("stop"), b"req-2", alice_identity, 1001.0
    )
    assert decode(response)["ok"] is True
    assert node.typing_peers(alice.hexhash) == []
    assert events[-1]["state"] == "stop"

    # Expiry is lazy: a start without a stop clears itself after the
    # TTL, even when no further event arrives.
    node._note_typing(alice.hexhash, alice.hexhash, "start", now=100.0)
    assert node.typing_peers(alice.hexhash, now=107.0) == [alice.hexhash]
    assert node.typing_peers(alice.hexhash, now=109.0) == []


def test_beacon_update_callback_fires_and_survives_failure(tmp_path):
    from tau.beacons import Beacons

    node, _, _, _ = _typing_node(tmp_path)
    beacons = Beacons()
    beacon = beacons.start(
        target="ab" * 16,
        target_kind="dm",
        target_name="alice",
        location={"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"},
        now=1000.0,
    )
    events = []
    node.on_beacon_update = lambda b, r: events.append((b, r))
    node._publish_beacon_update(beacon, {"id": "00" * 16, "queued": False})
    assert len(events) == 1
    assert events[0][0] is beacon
    assert events[0][1] == {"id": "00" * 16, "queued": False}
    # A failing callback must not take the update down with it.
    def _boom(beacon, result):
        raise RuntimeError("boom")

    node.on_beacon_update = _boom
    node._publish_beacon_update(beacon, {"id": "00" * 16, "queued": False})
    node.on_beacon_update = None
    node._publish_beacon_update(beacon, {"id": "00" * 16, "queued": False})


def test_handle_typing_rejects_anonymous_and_non_member(tmp_path):
    from tau.protocol import decode, encode_typing

    node, _, alice_identity, alice = _typing_node(tmp_path)
    group = node.groups.create(
        "crew",
        [alice.hexhash, node.destination.hexhash],
        created_by=alice.hexhash,
    )

    response = node._handle_typing(
        "/tau/typing", encode_typing("start"), b"req-1", None, 1000.0
    )
    assert decode(response)["error"]["code"] == "unidentified"

    stranger_identity = RNS.Identity()
    response = node._handle_typing(
        "/tau/typing",
        encode_typing("start", group=group["id"]),
        b"req-2",
        stranger_identity,
        1000.0,
    )
    assert decode(response)["error"]["code"] == "not_a_member"

    # A member's group typing lands under the group conversation.
    response = node._handle_typing(
        "/tau/typing",
        encode_typing("start", group=group["id"]),
        b"req-3",
        alice_identity,
        1000.0,
    )
    assert decode(response)["ok"] is True
    assert node.typing_peers(f"group:{group['id']}") == [alice.hexhash]


def test_incoming_message_clears_typing(tmp_path):
    from tau.protocol import decode, encode_message

    node, _, alice_identity, alice = _typing_node(tmp_path)
    events = []
    node.on_typing = events.append
    node._note_typing(alice.hexhash, alice.hexhash, "start", now=time.time())
    assert node.typing_peers(alice.hexhash) == [alice.hexhash]

    response = node._handle_message(
        "/tau/message",
        encode_message("hi", message_id="m-1"),
        b"req-1",
        alice_identity,
        1000.0,
    )
    assert decode(response)["ok"] is True
    assert node.typing_peers(alice.hexhash) == []
    assert events[-1]["state"] == "stop"


def test_send_typing_delivers_live_only(tmp_path, monkeypatch):
    from tau.protocol import parse_typing, REQUEST_TYPING

    node, _, _, alice = _typing_node(tmp_path)
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append((peer_arg, path, payload))
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_typing("alice", "start")

    assert response["ok"] is True
    assert response["state"] == "start"
    assert response["to"] == alice.hexhash
    assert len(calls) == 1
    assert calls[0][0].hexhash == alice.hexhash
    assert calls[0][1] == REQUEST_TYPING
    assert parse_typing(calls[0][2])["state"] == "start"

    with pytest.raises(NodeError, match="typing state must be"):
        node.send_typing("alice", "maybe")


def test_send_group_typing_fans_out(tmp_path, monkeypatch):
    from tau.protocol import parse_typing, REQUEST_TYPING

    node, group, alice, bob = _group_reaction_node(tmp_path)
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append((peer_arg, path, payload))
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_typing("crew", "start")

    assert response["ok"] is True
    assert response["group"] == group["id"]
    assert response["state"] == "start"
    # Both members got a leg; this node never types to itself.
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        bob.hexhash,
    }
    assert all(r.get("delivered") for r in response["results"])
    assert len(calls) == 2
    assert all(path == REQUEST_TYPING for _, path, _ in calls)
    # One payload for every leg.
    assert calls[0][2] == calls[1][2]
    payload = parse_typing(calls[0][2])
    assert payload["state"] == "start"
    assert payload["group"] == group["id"]


def test_send_group_typing_rejects_unknown_and_solo_groups(tmp_path):
    node, group, _, _ = _group_reaction_node(tmp_path)
    with pytest.raises(ValueError, match="unknown group"):
        node.send_group_typing("nope", "start")
    solo = {
        "id": "00" * 16,
        "name": "solo",
        "members": [node.destination.hexhash],
        "creator": node.destination.hexhash,
    }
    with pytest.raises(ValueError, match="no other members"):
        node.send_group_typing(solo, "start")


def test_send_group_message_uses_one_send_time_for_all_legs(
    tmp_path, monkeypatch
):
    from tau.protocol import parse_group_message

    node, _, _, _ = _group_reaction_node(tmp_path)
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_message("crew", "hi all")

    assert response["ok"] is True
    assert len(payloads) == 2
    # The wire ids still differ per leg, but the send time is one
    # value for the whole send — that shared time is what lets a
    # reaction name the logical message for every member.
    sent_ats = {parse_group_message(payload)["sent_at"] for payload in payloads}
    assert len(sent_ats) == 1


def test_send_group_poll_carries_options_on_every_leg(tmp_path, monkeypatch):
    from tau.protocol import parse_group_message

    node, group, _, _ = _group_reaction_node(tmp_path)
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_message(
        "crew", "which day?", poll_options=["Saturday", "Sunday"]
    )

    assert response["ok"] is True
    assert len(payloads) == 2
    for payload in payloads:
        assert parse_group_message(payload)["poll_options"] == [
            "Saturday",
            "Sunday",
        ]
    # The sender's own record carries the options and the shared time,
    # so the tally can resolve the poll it sent.
    stored = node.inbox.list_messages(
        limit=None, group=group["id"], direction=None
    )
    assert len(stored) == 1
    assert stored[0]["poll_options"] == ["Saturday", "Sunday"]
    assert stored[0]["leg_sent_at"] == response["sent_at"]


def test_send_group_poll_rejects_bad_options(tmp_path):
    node, _, _, _ = _group_reaction_node(tmp_path)
    with pytest.raises(NodeError):
        node.send_group_message("crew", "which day?", poll_options=["only one"])
    with pytest.raises(NodeError):
        node.send_group_message(
            "crew", "which day?", poll_options=["Saturday", "saturday"]
        )
    with pytest.raises(NodeError):
        node.send_group_message(
            "crew",
            "which day?",
            poll_options=["Saturday", "Sunday"],
            poll_vote={
                "target": {"author": "ab" * 16, "sent_at": 1.0},
                "option": 0,
            },
        )


def test_send_group_vote_carries_target_and_option(tmp_path, monkeypatch):
    from tau.protocol import parse_group_message

    node, group, _, _ = _group_reaction_node(tmp_path)
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    first = node.send_group_message(
        "crew", "which day?", poll_options=["Saturday", "Sunday"]
    )
    assert first["ok"] is True
    payloads.clear()
    target = {"author": node.destination.hexhash, "sent_at": first["sent_at"]}
    response = node.send_group_message(
        "crew",
        "voted: Sunday",
        poll_vote={"target": target, "option": 1},
    )

    assert response["ok"] is True
    assert len(payloads) == 2
    for payload in payloads:
        assert parse_group_message(payload)["poll_vote"] == {
            "target": target,
            "option": 1,
        }
    stored = node.inbox.list_messages(
        limit=None, group=group["id"], direction=None
    )
    assert stored[0]["poll_vote"] == {"target": target, "option": 1}


def test_send_group_message_refused_when_announcements_only_for_member(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    # The creator's message pins the setting on this member's node.
    node.groups.record(
        group["id"],
        "crew",
        list(group["members"]),
        creator=alice.hexhash,
        sender=alice.hexhash,
        sent_at=100.0,
        announcements_only=True,
    )
    assert node.groups.get(group["id"])["announcements_only"] is True
    with pytest.raises(NodeError, match="announcements-only"):
        node.send_group_message("crew", "hello?")
    with pytest.raises(NodeError, match="announcements-only"):
        node.send_group_message(
            "crew", "which day?", poll_options=["Saturday", "Sunday"]
        )
    # A vote is not a post: it still goes out.
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_group_message(
        "crew",
        "voted",
        poll_vote={
            "target": {"author": alice.hexhash, "sent_at": 100.0},
            "option": 0,
        },
    )
    assert response["ok"] is True
    assert len(payloads) == 2


def test_send_group_file_refused_when_announcements_only_for_member(tmp_path):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.groups.record(
        group["id"],
        "crew",
        list(group["members"]),
        creator=alice.hexhash,
        sender=alice.hexhash,
        sent_at=100.0,
        announcements_only=True,
    )
    path = tmp_path / "note.txt"
    path.write_text("hello", encoding="utf-8")
    with pytest.raises(NodeError, match="announcements-only"):
        node.send_group_file("crew", path)


def test_send_group_message_allowed_for_creator_when_announcements_only(
    tmp_path, monkeypatch
):
    from tau.protocol import parse_group_message

    node, group, alice, bob = _group_reaction_node(tmp_path)
    own = node.destination.hexhash
    mine = node.groups.create(
        "mine", [own, alice.hexhash, bob.hexhash], created_by=own
    )
    node.groups.set_announcements_only(mine["id"], True, created_by=own)
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_group_message("mine", "announcement")
    assert response["ok"] is True
    assert len(payloads) == 2
    for payload in payloads:
        assert parse_group_message(payload)["group_announcements_only"] is True


def test_deliver_entry_skips_queued_post_when_group_goes_announcements_only(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    _group_leg_entry(node, group, bob, text="queued hello", leg_id="bob-leg-1")
    snapshot = node.mailbox.get_outbox("bob-leg-1")
    # The creator restricts the group after the leg was queued.
    node.groups.record(
        group["id"],
        "crew",
        list(group["members"]),
        creator=alice.hexhash,
        sender=alice.hexhash,
        sent_at=200.0,
        announcements_only=True,
    )
    calls = []
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: calls.append(args) or {"ok": True},
    )
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is False
    assert calls == []
    # The leg stays queued until its TTL (the user can cancel it).
    assert node.mailbox.get_outbox("bob-leg-1") is not None
    # The skip is recorded as a failed attempt, so the backoff moves
    # the leg out of the sweep's way instead of re-checking it every
    # round while it stays queued.
    skipped = node.mailbox.get_outbox("bob-leg-1")
    assert skipped.attempts == 1
    assert skipped.next_attempt_at is not None
    assert skipped.next_attempt_at > time.time()


def test_skip_paces_restricted_leg_once_per_sweep(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    _group_leg_entry(node, group, bob, text="queued hello", leg_id="bob-leg-1")
    snapshot = node.mailbox.get_outbox("bob-leg-1")
    # The creator restricts the group after the leg was queued.
    node.groups.record(
        group["id"],
        "crew",
        list(group["members"]),
        creator=alice.hexhash,
        sender=alice.hexhash,
        sent_at=200.0,
        announcements_only=True,
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True},
    )
    # The direct skip records the attempt.
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is False
    assert node.mailbox.get_outbox("bob-leg-1").attempts == 1
    # The deposit loop visits the same leg once per holder peer in the
    # sweep — those visits must not record again, or one sweep would
    # compound the backoff without a network attempt and delay the leg
    # for up to an hour after the creator lifts the restriction.
    assert node._deposit_entry(alice, snapshot, timeout=1.0) is False
    assert node._deposit_entry(alice, snapshot, timeout=1.0) is False
    assert node.mailbox.get_outbox("bob-leg-1").attempts == 1


def test_deliver_entry_drops_queued_group_leg_when_peer_refuses_not_allowed(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    # The drop fires only when this node's own copy agrees the group
    # is announcements-only: pin it first.  A vote leg is used because
    # a member post would be caught by this node's own skip before any
    # send; votes are exempt from the skip, so the refusal is what
    # reaches the drop branch.
    node.groups.record(
        group["id"],
        "crew",
        list(group["members"]),
        creator=alice.hexhash,
        sender=alice.hexhash,
        sent_at=200.0,
        announcements_only=True,
    )
    node.mailbox.enqueue(
        to=bob.destination_hash,
        text="voted: Sunday",
        message_id="bob-leg-1",
        group={
            "id": group["id"],
            "name": group["name"],
            "creator": group["creator"],
            "members": list(group["members"]),
        },
        sent_at=100.0,
        poll_vote={
            "target": {"author": alice.hexhash, "sent_at": 100.0},
            "option": 1,
        },
    )
    snapshot = node.mailbox.get_outbox("bob-leg-1")

    def refused(*args, **kwargs):
        raise AnnouncementsOnlyError(
            "peer rejected the request: group is announcements-only: "
            "only the creator can post"
        )

    monkeypatch.setattr(node, "_request", refused)
    # A group leg refused as announcements-only is a policy refusal,
    # like not-a-member: the group will not take this post, so the leg
    # is dropped instead of held until its TTL.
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is False
    assert node.mailbox.get_outbox("bob-leg-1") is None


def test_deliver_entry_keeps_queued_group_leg_when_refusal_is_stale(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    _group_leg_entry(node, group, bob, text="queued hello", leg_id="bob-leg-1")
    snapshot = node.mailbox.get_outbox("bob-leg-1")

    def refused(*args, **kwargs):
        raise AnnouncementsOnlyError(
            "peer rejected the request: group is announcements-only: "
            "only the creator can post"
        )

    monkeypatch.setattr(node, "_request", refused)
    # This node's copy says the group is open, so the recipient's
    # refusal must be a stale "on" — the creator lifted it, but the
    # recipient has not pinned the "off" yet.  The leg stays queued
    # for retry instead of being dropped, or the post would be lost
    # for good once the recipient catches up.
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is False
    kept = node.mailbox.get_outbox("bob-leg-1")
    assert kept is not None
    assert kept.attempts == 1


def test_deliver_entry_keeps_queued_group_leg_when_peer_refuses_allow_list(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    _group_leg_entry(node, group, bob, text="queued hello", leg_id="bob-leg-1")
    snapshot = node.mailbox.get_outbox("bob-leg-1")

    def refused(*args, **kwargs):
        raise NotAllowedError(
            "peer rejected the request: sender is not on this node's "
            "allow-list"
        )

    monkeypatch.setattr(node, "_request", refused)
    # An allow-list refusal is changeable — the recipient may allow-list
    # the sender later — so the leg must stay queued for retry instead
    # of being dropped with the announcements-only refusals.
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is False
    kept = node.mailbox.get_outbox("bob-leg-1")
    assert kept is not None
    assert kept.attempts == 1


def test_request_maps_announcements_only_to_its_own_error(tmp_path):
    from tau.protocol import response_error

    node, group, alice, bob = _group_reaction_node(tmp_path)

    class _Receipt:
        def get_response(self):
            return response_error(
                "announcements_only",
                "group is announcements-only: only the creator can post",
            )

        def get_response_time(self):
            return 0.004

    class _Link:
        def request(
            self, path, payload, *, response_callback, failed_callback, **kwargs
        ):
            response_callback(_Receipt())
            return object()

    # The refusal maps to the distinct error, and that error is still
    # a NotAllowedError for every existing refusal handler.
    with pytest.raises(AnnouncementsOnlyError, match="announcements-only"):
        node._request_on_link(_Link(), "/tau/group", b"{}", 1.0)
    with pytest.raises(NotAllowedError):
        node._request_on_link(_Link(), "/tau/group", b"{}", 1.0)


def test_deliver_entry_drop_on_not_allowed_is_bounded_by_deadline(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    # Pin this node's copy announcements-only first (see the drop
    # test): a vote leg is what reaches the refusal while the copy
    # agrees.
    node.groups.record(
        group["id"],
        "crew",
        list(group["members"]),
        creator=alice.hexhash,
        sender=alice.hexhash,
        sent_at=200.0,
        announcements_only=True,
    )
    node.mailbox.enqueue(
        to=bob.destination_hash,
        text="voted: Sunday",
        message_id="bob-leg-1",
        group={
            "id": group["id"],
            "name": group["name"],
            "creator": group["creator"],
            "members": list(group["members"]),
        },
        sent_at=100.0,
        poll_vote={
            "target": {"author": alice.hexhash, "sent_at": 100.0},
            "option": 1,
        },
    )
    snapshot = node.mailbox.get_outbox("bob-leg-1")

    def refused(*args, **kwargs):
        raise AnnouncementsOnlyError(
            "peer rejected the request: group is announcements-only: "
            "only the creator can post"
        )

    monkeypatch.setattr(node, "_request", refused)
    seen = {}
    real_remove = node._remove_queued_version

    def recording_remove(message_id, *, text, edited_at, timeout=None):
        seen["timeout"] = timeout
        return real_remove(
            message_id, text=text, edited_at=edited_at, timeout=timeout
        )

    monkeypatch.setattr(node, "_remove_queued_version", recording_remove)
    assert (
        node._deliver_entry(
            bob, snapshot, timeout=1.0, deadline=time.monotonic() + 1.0
        )
        is False
    )
    assert node.mailbox.get_outbox("bob-leg-1") is None
    # The removal is bounded by the remaining delivery deadline, so a
    # mailbox lock held by another process cannot stall the flush
    # after the budget has expired.
    assert seen["timeout"] is not None
    assert seen["timeout"] <= 1.0


def test_deliver_entry_carries_announcements_flag_on_retry(
    tmp_path, monkeypatch
):
    from tau.protocol import parse_group_message

    node, group, alice, bob = _group_reaction_node(tmp_path)
    own = node.destination.hexhash
    mine = node.groups.create(
        "mine", [own, alice.hexhash, bob.hexhash], created_by=own
    )
    node.groups.set_announcements_only(mine["id"], True, created_by=own)
    _group_leg_entry(node, mine, bob, text="announcement", leg_id="bob-leg-1")
    snapshot = node.mailbox.get_outbox("bob-leg-1")
    calls = []
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: calls.append(args) or {"ok": True},
    )
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is True
    assert len(calls) == 1
    parsed = parse_group_message(calls[0][2])
    assert parsed["group_announcements_only"] is True


def test_send_group_poll_scheduled_queues_poll_payload(tmp_path):
    node, group, _, _ = _group_reaction_node(tmp_path)
    send_after = time.time() + 3600.0
    response = node.send_group_message(
        "crew",
        "which day?",
        poll_options=["Saturday", "Sunday"],
        send_after=send_after,
    )

    assert response["ok"] is True
    assert all(result.get("queued") for result in response["results"])
    for result in response["results"]:
        entry = node.mailbox.get_outbox(result["id"])
        assert entry is not None
        # A queued leg re-sends the poll payload on retry, not plain chat.
        assert entry.poll_options == ["Saturday", "Sunday"]
        assert entry.poll_vote is None
        assert entry.poll_close is None


def test_send_group_message_reply_attaches_logical_reference(
    tmp_path, monkeypatch
):
    # A reply to a received group message carries the logical
    # reference (group, author, shared sent_at) on every leg, so a
    # member who holds a different leg id for the quoted send can
    # still resolve the quote against its own copy.
    from tau.protocol import excerpt_hash, parse_group_message

    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.inbox.append_group_message(
        {
            "from": "alice",
            "destination": alice.hexhash,
            "text": "original thought",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "a1" * 16,
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
        },
        group=group["id"],
        author=alice.hexhash,
        sent_at=1000.0,
        members={alice.hexhash, bob.hexhash, node.destination.hexhash},
    )
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_message(
        "crew",
        "seconded",
        in_reply_to={
            "id": "a1" * 16,
            "excerpt": excerpt_hash("original thought"),
        },
    )

    assert response["ok"] is True
    assert len(payloads) == 2
    for payload in payloads:
        ref = parse_group_message(payload)["in_reply_to"]
        assert ref["id"] == "a1" * 16
        assert ref["excerpt"] == excerpt_hash("original thought")
        assert ref["group"] == group["id"]
        assert ref["author"] == alice.hexhash
        assert ref["sent_at"] == 1000.0


def test_send_group_message_reply_to_own_send_names_self_as_author(
    tmp_path, monkeypatch
):
    # Quoting one of this node's own sent group messages names this
    # node as the author and the legs' shared send time, so the
    # recipients resolve it against the copy they hold.
    from tau.protocol import excerpt_hash, parse_group_message

    node, group, _, _ = _group_reaction_node(tmp_path)
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    first = node.send_group_message("crew", "my earlier point")
    assert first["ok"] is True
    leg_id = first["results"][0]["id"]
    payloads.clear()

    response = node.send_group_message(
        "crew",
        "as I said",
        in_reply_to={
            "id": leg_id,
            "excerpt": excerpt_hash("my earlier point"),
        },
    )

    assert response["ok"] is True
    assert len(payloads) == 2
    for payload in payloads:
        ref = parse_group_message(payload)["in_reply_to"]
        assert ref["id"] == leg_id
        assert ref["group"] == group["id"]
        assert ref["author"] == node.destination.hexhash
        assert ref["sent_at"] == first["sent_at"]


def test_send_group_message_reply_to_unknown_id_keeps_wire_reference(
    tmp_path, monkeypatch
):
    # A quoted id the local inbox cannot resolve — the sender is
    # quoting from another device's history — keeps its wire-id
    # reference: the send never fails for want of a reference its
    # own history cannot supply.
    from tau.protocol import excerpt_hash, parse_group_message

    node, _, _, _ = _group_reaction_node(tmp_path)
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_message(
        "crew",
        "seconded",
        in_reply_to={
            "id": "d" * 32,
            "excerpt": excerpt_hash("someone else's thought"),
        },
    )

    assert response["ok"] is True
    assert len(payloads) == 2
    for payload in payloads:
        assert parse_group_message(payload)["in_reply_to"] == {
            "id": "d" * 32,
            "excerpt": excerpt_hash("someone else's thought"),
        }


def test_send_message_reply_keeps_wire_reference_only(
    tmp_path, monkeypatch
):
    # Direct messages keep the wire-id behavior: no logical
    # reference is attached, because a DM has one copy per side and
    # the wire id already names it.
    from tau.protocol import parse_message

    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "resolve_peer", lambda *args, **kwargs: peer)
    calls = []

    def fake_request(*args, **kwargs):
        calls.append(args)
        return {"ok": True, "echo": "seconded"}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_message(
        peer,
        "seconded",
        in_reply_to={"id": "a" * 32, "excerpt": "0123456789abcdef"},
    )

    assert response["ok"] is True
    assert len(calls) == 1
    assert parse_message(calls[0][2])["in_reply_to"] == {
        "id": "a" * 32,
        "excerpt": "0123456789abcdef",
    }


def test_send_group_message_with_send_after_queues_without_delivery(
    tmp_path, monkeypatch
):
    node, group, _, _ = _group_reaction_node(tmp_path)
    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    send_after = time.time() + 3600.0
    response = node.send_group_message("crew", "later", send_after=send_after)

    # No delivery attempt at all — both members are reachable, but
    # the message is scheduled, not sent.
    assert calls == []
    assert response["ok"] is True
    assert len(response["results"]) == 2
    assert all(result.get("queued") for result in response["results"])
    assert all(
        result.get("send_after") == send_after for result in response["results"]
    )
    for result in response["results"]:
        entry = node.mailbox.get_outbox(result["id"])
        assert entry is not None
        assert entry.send_after == send_after
        assert entry.is_due() is False
        assert entry.group_id == group["id"]
    # The shared send time is one value for the whole send, so a
    # reaction or retract can still name the logical message.
    sent_ats = {
        node.mailbox.get_outbox(result["id"]).wire_sent_at
        for result in response["results"]
    }
    assert len(sent_ats) == 1
    # The sent message is still recorded in local history.
    sent = node.inbox.list_messages(direction="out")
    assert len(sent) == 1
    assert sent[0]["kind"] == "group"
    assert sent[0]["leg_sent_at"] == response["sent_at"]


def test_send_group_poll_with_send_after_queues_poll_intact(
    tmp_path, monkeypatch
):
    node, group, _, _ = _group_reaction_node(tmp_path)
    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    send_after = time.time() + 3600.0
    response = node.send_group_message(
        "crew",
        "Lunch?",
        send_after=send_after,
        poll_options=["Monday", "Tuesday"],
    )

    # No delivery attempt at all — the poll is scheduled, not sent.
    assert calls == []
    assert response["ok"] is True
    assert len(response["results"]) == 2
    assert all(result.get("queued") for result in response["results"])
    assert all(
        result.get("send_after") == send_after for result in response["results"]
    )
    for result in response["results"]:
        entry = node.mailbox.get_outbox(result["id"])
        assert entry is not None
        assert entry.send_after == send_after
        assert entry.poll_options == ["Monday", "Tuesday"]
        assert entry.group_id == group["id"]
    # The sent record carries the poll, so the sender's history
    # renders it as a poll with its options.
    sent = node.inbox.list_messages(direction="out")
    assert len(sent) == 1
    assert sent[0]["poll_options"] == ["Monday", "Tuesday"]
    assert sent[0]["leg_sent_at"] == response["sent_at"]


def test_scheduled_group_poll_publishes_with_options_when_due(
    tmp_path, monkeypatch
):
    node, group, alice, _ = _group_reaction_node(tmp_path)
    send_after = time.time() + 3600.0
    response = node.send_group_message(
        "crew",
        "Lunch?",
        send_after=send_after,
        poll_options=["Monday", "Tuesday"],
    )
    assert all(result.get("queued") for result in response["results"])
    delivered = []
    monkeypatch.setattr(
        node,
        "_deliver_entry",
        lambda p, e, timeout, **kwargs: (
            delivered.append(e),
            True,
        )[1],
    )
    monkeypatch.setattr(
        node,
        "_deposit_entry",
        lambda p, e, timeout, **kwargs: False,
    )
    peer = alice
    # Not due yet: the flush leaves both legs queued.
    node._flush_peer(peer, 1.0, respect_backoff=False)
    assert delivered == []
    assert len(node.mailbox.outbox_list()) == 2
    # Once the time passes, the same flush delivers the poll with
    # its question and options intact.
    for result in response["results"]:
        node.mailbox.get_outbox(result["id"]).send_after = time.time() - 1.0
    node.mailbox.save()
    node._flush_peer(peer, 1.0, respect_backoff=False)
    assert len(delivered) == 1
    assert delivered[0].text == "Lunch?"
    assert delivered[0].poll_options == ["Monday", "Tuesday"]


def test_send_group_message_scheduled_resolves_unregistered_member(
    tmp_path, monkeypatch
):
    node, group, alice, _ = _group_reaction_node(tmp_path)
    carol = make_peer("carol")
    node.groups.update_members(
        group["id"],
        [alice.hexhash, carol.hexhash, node.destination.hexhash],
        created_by=alice.hexhash,
    )
    # Carol is in the roster but has never announced, so the sweep
    # would never visit her: resolve her now, like a direct send does.
    resolved = []

    def fake_resolve(target, *, timeout=10.0):
        resolved.append(target)
        if target == carol.hexhash:
            # Discovered now: register her, like wait_for_peer does.
            node.registry.upsert(
                carol.destination_hash, carol.public_key, name="carol"
            )
            return carol
        found = node.registry.get(bytes.fromhex(target))
        if found is not None:
            return found
        raise NodeError("no announce")

    monkeypatch.setattr(node, "resolve_peer", fake_resolve)
    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    send_after = time.time() + 3600.0
    response = node.send_group_message("crew", "later", send_after=send_after)

    # Still no delivery attempt — only the resolve, for registration.
    assert calls == []
    assert response["ok"] is True
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        carol.hexhash,
    }
    assert all(result.get("queued") for result in response["results"])
    assert set(resolved) == {alice.hexhash, carol.hexhash}
    assert node.registry.get(carol.destination_hash) is not None
    carol_result = [
        result for result in response["results"] if result["to"] == carol.hexhash
    ][0]
    assert carol_result["to_name"] == "carol"
    assert carol_result["send_after"] == send_after


def test_send_group_message_scheduled_to_undiscovered_member_still_queues(
    tmp_path, monkeypatch
):
    node, _, _, _ = _group_reaction_node(tmp_path)

    def _no_peer(*args, **kwargs):
        raise NodeError("no announce")

    monkeypatch.setattr(node, "resolve_peer", _no_peer)

    def _boom(*args, **kwargs):
        raise AssertionError("no delivery attempt for a scheduled leg")

    monkeypatch.setattr(node, "_request", _boom)
    send_after = time.time() + 3600.0
    response = node.send_group_message("crew", "later", send_after=send_after)

    # A member who cannot be resolved yet still queues: the announce
    # path registers them and triggers a round when they appear,
    # exactly as for an unreachable member.
    assert response["ok"] is True
    assert all(result.get("queued") for result in response["results"])
    assert all(result.get("to_name") == "" for result in response["results"])
    for result in response["results"]:
        entry = node.mailbox.get_outbox(result["id"])
        assert entry is not None
        assert entry.send_after == send_after


def test_send_group_message_rejects_nonfinite_send_after(tmp_path):
    node, _, _, _ = _group_reaction_node(tmp_path)
    with pytest.raises(NodeError):
        node.send_group_message("crew", "hi", send_after=float("nan"))
    with pytest.raises(NodeError):
        node.send_group_message("crew", "hi", send_after="tomorrow")


def test_send_group_message_rejects_scheduled_send_without_queue(tmp_path):
    node, _, _, _ = _group_reaction_node(tmp_path)
    # queue=False means "do not queue": a scheduled message is queued
    # by definition, so the combination is refused before anything is
    # recorded or enqueued.
    with pytest.raises(NodeError, match="without queueing"):
        node.send_group_message(
            "crew", "later", queue=False, send_after=time.time() + 3600.0
        )
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.sent_list() == []
    assert node.inbox.list_messages(direction="out") == []


def test_send_group_message_failed_schedule_leaves_no_sent_record(tmp_path):
    node, _, _, _ = _group_reaction_node(tmp_path)
    # A schedule past the message's own expiry is rejected at enqueue.
    send_after = time.time() + 3600.0
    expires_at = time.time() + 60.0
    response = node.send_group_message(
        "crew", "never", send_after=send_after, expires_at=expires_at
    )
    # Every leg failed to queue, so nothing was recorded or enqueued.
    assert response["ok"] is False
    assert all(not result.get("queued") for result in response["results"])
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.sent_list() == []
    assert node.inbox.list_messages(direction="out") == []


def test_flush_never_delivers_scheduled_group_leg_early(
    tmp_path, monkeypatch
):
    node, _, alice, _ = _group_reaction_node(tmp_path)
    send_after = time.time() + 3600.0
    response = node.send_group_message("crew", "later", send_after=send_after)
    assert all(result.get("queued") for result in response["results"])
    delivered = []
    monkeypatch.setattr(
        node,
        "_deliver_entry",
        lambda p, e, timeout, **kwargs: (
            delivered.append(e.id),
            True,
        )[1],
    )
    monkeypatch.setattr(
        node,
        "_deposit_entry",
        lambda p, e, timeout, **kwargs: False,
    )
    peer = make_peer("alice")
    node._flush_peer(peer, 1.0, respect_backoff=False)
    assert delivered == []
    assert len(node.mailbox.outbox_list()) == 2


def test_send_group_reaction_signs_group_context(tmp_path, monkeypatch):
    from tau.protocol import REQUEST_REACT, parse_reaction

    node, group, alice, _ = _group_reaction_node(tmp_path)
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append(payload)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    node.send_group_reaction(
        "crew",
        "other-leg-1",
        "👍",
        target_author=alice.hexhash,
        target_sent_at=100.0,
    )

    assert len(calls) == 2
    payload = parse_reaction(calls[0])
    assert payload["group"] == group["id"]
    assert payload["target_author"] == alice.hexhash
    assert payload["target_sent_at"] == 100.0
    assert payload["target"] == "other-leg-1"


def test_send_group_retraction_fans_out_one_signed_payload(tmp_path, monkeypatch):
    import base64

    from tau.protocol import REQUEST_RETRACT, parse_retract, retract_unsigned_bytes

    node, group, alice, bob = _group_reaction_node(tmp_path)
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append((peer_arg, path, payload))
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)

    assert response["ok"] is True
    assert response["group"] == group["id"]
    assert response["target"] == "leg-1"
    # Both members got a leg; this node never retracts from itself.
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        bob.hexhash,
    }
    assert all(r.get("delivered") for r in response["results"])
    assert all(r.get("deleted") for r in response["results"])
    assert len(calls) == 2
    assert all(path == REQUEST_RETRACT for _, path, _ in calls)
    # One signed payload, one retract id, for every leg.
    assert calls[0][2] == calls[1][2]
    payload = parse_retract(calls[0][2])
    assert payload["target"] == "leg-1"
    assert payload["group"] == group["id"]
    assert payload["target_author"] == node.destination.hexhash
    assert payload["target_sent_at"] == 100.0
    assert payload["id"] == response["id"]
    assert node.identity.validate(
        base64.b64decode(payload["sig"]),
        retract_unsigned_bytes(calls[0][2]),
    )


def test_send_group_retraction_stamps_target_author(tmp_path, monkeypatch):
    # A moderator retract names the message's author in the signed
    # group context, so receivers resolve the author's copy (and key
    # the durable marker by the author) instead of the creator's.
    import base64

    from tau.protocol import REQUEST_RETRACT, parse_retract, retract_unsigned_bytes

    node, group, alice, bob = _group_reaction_node(tmp_path)
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append((peer_arg, path, payload))
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction(
        "crew", "leg-1", target_sent_at=100.0, target_author=alice.hexhash
    )

    assert response["ok"] is True
    assert len(calls) == 2
    assert calls[0][2] == calls[1][2]
    payload = parse_retract(calls[0][2])
    assert payload["target_author"] == alice.hexhash
    assert payload["target_author"] != node.destination.hexhash
    assert payload["group"] == group["id"]
    assert payload["target_sent_at"] == 100.0
    assert node.identity.validate(
        base64.b64decode(payload["sig"]),
        retract_unsigned_bytes(calls[0][2]),
    )


def test_send_group_retraction_reports_failed_leg(tmp_path, monkeypatch):
    node, _, alice, bob = _group_reaction_node(tmp_path)

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NotAllowedError("peer rejected the request: not allowed")
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)

    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("error") == "not_allowed"


def test_send_group_retraction_reports_member_without_group_retract(
    tmp_path, monkeypatch
):
    # An older member answers the DM retract ok but never saw the
    # group context: it kept its copy, so the leg is not delivered
    # and the sender keeps its own copy for a retry.
    node, _, alice, bob = _group_reaction_node(tmp_path)

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            return {"ok": True, "deleted": False, "_rtt": 0.004}
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)

    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("delivered") is None
    assert (
        by_to[bob.hexhash].get("error")
        == "member runs a version without group retract"
    )


def test_send_group_retraction_cancels_queued_legs(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    # Bob's leg never left: it sits in the outbox with the shared
    # send time of the original fan-out.
    node.mailbox.enqueue(
        to=bob.destination_hash,
        text="hi all",
        message_id="bob-leg-1",
        group={
            "id": group["id"],
            "name": group["name"],
            "creator": group["creator"],
            "members": list(group["members"]),
        },
        sent_at=100.0,
    )
    assert node.mailbox.get_outbox("bob-leg-1") is not None
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append(peer_arg.hexhash)
        return {"ok": True, "deleted": False, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)

    # The queued leg is gone, so Bob never receives the message — and
    # the live retract still went out to him, so a copy that reached
    # him another way is deleted and later copies lose to the marker.
    assert response["cancelled_queued"] == ["bob-leg-1"]
    assert node.mailbox.get_outbox("bob-leg-1") is None
    assert response["ok"] is True
    assert set(calls) == {alice.hexhash, bob.hexhash}


def test_send_group_retraction_queues_unreachable_leg(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NodeError("no route to peer")
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)

    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    bob_result = by_to[bob.hexhash]
    assert bob_result.get("queued") is True
    assert bob_result.get("delivered") is None
    assert bob_result.get("queued_id")
    assert bob_result["id"] == response["id"]
    # The queued leg is one outbox entry with the group context and
    # the same pre-signed bytes the live legs carried.
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.kind == "retract"
    assert entry.to == bob.destination_hash
    assert entry.group_id == group["id"]
    assert entry.group_name == "crew"
    assert entry.group_creator == group["creator"]
    assert entry.group_members == group["members"]
    assert entry.retract["target"] == "leg-1"
    assert entry.retract["retract_id"] == response["id"]
    assert entry.retract["target_author"] == node.destination.hexhash
    assert entry.retract["target_sent_at"] == 100.0
    assert entry.retract["payload_b64"]
    assert entry.id == bob_result["queued_id"]


def test_send_group_retraction_dedupes_queued_legs(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def unreachable(peer_arg, path, payload, timeout, deadline=None):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)

    first = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    second = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)

    # Both members are unreachable, so both legs queue on the first
    # call; the second call reports the already-queued legs instead
    # of storing duplicates under new retract ids.
    assert len(node.mailbox.outbox_list()) == 2
    by_to = {r["to"]: r for r in second["results"]}
    assert by_to[bob.hexhash].get("queued") is True
    first_by_to = {r["to"]: r for r in first["results"]}
    assert (
        by_to[bob.hexhash]["queued_id"]
        == first_by_to[bob.hexhash]["queued_id"]
    )
    assert by_to[bob.hexhash]["id"] == first_by_to[bob.hexhash]["id"]


def test_send_group_retraction_no_queue_reports_failure(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def unreachable(peer_arg, path, payload, timeout, deadline=None):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)

    response = node.send_group_retraction(
        "crew", "leg-1", target_sent_at=100.0, queue=False
    )

    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[bob.hexhash].get("queued") is None
    assert by_to[bob.hexhash].get("error") == "no route to peer"
    assert node.mailbox.outbox_list() == []


def test_send_group_retraction_refusal_is_never_queued(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NotAllowedError("peer rejected the request: not allowed")
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)

    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[bob.hexhash].get("error") == "not_allowed"
    assert by_to[bob.hexhash].get("queued") is None
    assert node.mailbox.outbox_list() == []


def test_deliver_queued_group_retract_drops_local_copies_on_last_leg(
    tmp_path, monkeypatch
):
    from tau.protocol import REQUEST_RETRACT

    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "crew hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent(
        "leg-1", to=alice.destination_hash, text="crew hello"
    )

    def unreachable(peer_arg, path, payload, timeout, deadline=None):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    assert response["ok"] is False
    entries = {e.to_hex: e for e in node.mailbox.outbox_list()}
    assert set(entries) == {alice.hexhash, bob.hexhash}

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        assert path == REQUEST_RETRACT
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", reachable)
    # The first leg delivers, but the fan-out is not complete: the
    # local copies stay until the last queued leg lands.
    assert node._deliver_entry(alice, entries[alice.hexhash], 1.0) is True
    assert node.inbox.resolve("leg-1")["text"] == "crew hello"
    assert len(node.mailbox.outbox_list()) == 1
    # The last leg completes the fan-out: the local copies go now.
    assert node._deliver_entry(bob, entries[bob.hexhash], 1.0) is True
    with pytest.raises(ValueError):
        node.inbox.resolve("leg-1")
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.sent_list() == []
    assert node.mailbox.is_sent_group_retracted(group["id"], 100.0) is True


def test_deliver_queued_group_retract_refused_sibling_keeps_local_copies(
    tmp_path, monkeypatch
):
    # Qodo Warning on PR #706: the sweep dropped the sender's local
    # copies once no sibling legs remained queued — but a refused leg
    # is gone from the outbox without delivering, so "no siblings"
    # did not mean the fan-out completed. A refused sibling blocks
    # the cleanup durably: the last delivered leg still leaves the
    # outbox, but the local copies stay for a retry.
    from tau.protocol import REQUEST_RETRACT

    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "crew hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent(
        "leg-1", to=alice.destination_hash, text="crew hello"
    )

    def unreachable(peer_arg, path, payload, timeout, deadline=None):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    assert response["ok"] is False
    entries = {e.to_hex: e for e in node.mailbox.outbox_list()}
    assert set(entries) == {alice.hexhash, bob.hexhash}

    def refuse_bob(peer_arg, path, payload, timeout, deadline=None):
        assert path == REQUEST_RETRACT
        if peer_arg.hexhash == bob.hexhash:
            raise NotAllowedError("peer rejected the request: not allowed")
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", refuse_bob)
    # Bob's leg is refused: it drops, the cleanup is blocked, and the
    # local copy is still there.
    assert node._deliver_entry(bob, entries[bob.hexhash], 1.0) is False
    assert node.mailbox.is_retract_cleanup_blocked(response["id"]) is True
    assert node.inbox.resolve("leg-1")["text"] == "crew hello"
    # Alice's leg delivers — the last queued leg — but the fan-out
    # never completed, so the local copies stay.
    assert node._deliver_entry(alice, entries[alice.hexhash], 1.0) is True
    assert node.inbox.resolve("leg-1")["text"] == "crew hello"
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.is_sent_group_retracted(group["id"], 100.0) is False


def test_send_group_retraction_failed_leg_blocks_sweep_cleanup(
    tmp_path, monkeypatch
):
    # The same block, recorded on the live path: Bob refuses while
    # Alice is unreachable, so Alice's leg is queued — and when it
    # later delivers, the sweep must not drop the local copies, since
    # Bob still holds the message.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "crew hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent(
        "leg-1", to=alice.destination_hash, text="crew hello"
    )

    def mixed(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NotAllowedError("peer rejected the request: not allowed")
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", mixed)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[bob.hexhash].get("error") == "not_allowed"
    assert by_to[alice.hexhash].get("queued") is True
    assert node.mailbox.is_retract_cleanup_blocked(response["id"]) is True

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", reachable)
    [entry] = node.mailbox.outbox_list()
    assert node._deliver_entry(alice, entry, 1.0) is True
    assert node.inbox.resolve("leg-1")["text"] == "crew hello"
    assert node.mailbox.is_sent_group_retracted(group["id"], 100.0) is False


def test_send_group_retraction_peer_rejection_is_final(tmp_path, monkeypatch):
    # A peer rejection (not the sender, an invalid retract) is final
    # on the live fan-out, exactly like a refusal — never queued —
    # while a storage failure stays transient and queues the leg.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def rejected(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise PeerRejectedError(
                "peer rejected the request: not the sender",
                code="not_sender",
            )
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", rejected)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[bob.hexhash].get("error") == (
        "peer rejected the request: not the sender"
    )
    assert by_to[bob.hexhash].get("queued") is None
    assert node.mailbox.outbox_list() == []

    def storage_failed(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise PeerRejectedError(
                "peer rejected the request: could not record retraction",
                code="storage_failed",
            )
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", storage_failed)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[bob.hexhash].get("queued") is True


def test_deliver_queued_group_retract_corrupt_sibling_keeps_local_copies(
    tmp_path, monkeypatch
):
    # Qodo Warning on PR #706: a group-retract leg whose stored
    # payload did not survive carried no retract id, so dropping it
    # recorded no block and a valid sibling's delivery deleted the
    # sender's copies. The id now survives as a hint (see
    # OutboxEntry.group_retract_id), so the corrupt leg still blocks
    # the cleanup — and still counts as a sibling until it drops.
    import json

    from tau.protocol import REQUEST_RETRACT

    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "crew hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent(
        "leg-1", to=alice.destination_hash, text="crew hello"
    )

    def unreachable(peer_arg, path, payload, timeout, deadline=None):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    assert response["ok"] is False
    entries = {e.to_hex: e for e in node.mailbox.outbox_list()}
    assert set(entries) == {alice.hexhash, bob.hexhash}

    # Corrupt Bob's stored leg on disk: the signed bytes are gone,
    # but the retract id survives in the stored record.
    data = json.loads(node.mailbox.path.read_text())
    [stored] = [
        e for e in data["outbox"] if e["id"] == entries[bob.hexhash].id
    ]
    stored["retract"] = {"retract_id": response["id"]}
    node.mailbox.path.write_text(json.dumps(data))

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        assert path == REQUEST_RETRACT
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", reachable)
    # Bob's corrupt leg drops — and the block is recorded from the
    # hint, so Alice's later delivery keeps the local copies.
    assert node._deliver_entry(bob, entries[bob.hexhash], 1.0) is False
    assert node.mailbox.is_retract_cleanup_blocked(response["id"]) is True
    assert node._deliver_entry(alice, entries[alice.hexhash], 1.0) is True
    assert node.inbox.resolve("leg-1")["text"] == "crew hello"
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.is_sent_group_retracted(group["id"], 100.0) is False


def test_deliver_queued_group_retract_keeps_leg_when_marker_write_fails(
    tmp_path, monkeypatch
):
    # Qodo Warning on PR #706: the sweep removed a finally-failed leg
    # before writing its cleanup block, so a failed marker write (or
    # a stop between the two steps) left neither the leg nor the
    # failure record. The marker is now written in the same
    # transaction as the removal: a write failure keeps the leg queued
    # for a later round.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "crew hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent(
        "leg-1", to=alice.destination_hash, text="crew hello"
    )

    def unreachable(peer_arg, path, payload, timeout, deadline=None):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    assert response["ok"] is False
    entries = {e.to_hex: e for e in node.mailbox.outbox_list()}
    assert set(entries) == {alice.hexhash, bob.hexhash}

    def refuse_bob(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NotAllowedError("peer rejected the request: not allowed")
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", refuse_bob)

    def fail_marker(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(
        node.mailbox, "_record_retract_cleanup_blocked_locked", fail_marker
    )
    # The refusal is final, but the marker could not be recorded: the
    # leg stays queued for a later round instead of being lost.
    assert node._deliver_entry(bob, entries[bob.hexhash], 1.0) is False
    assert {e.id for e in node.mailbox.outbox_list()} == {
        entries[alice.hexhash].id,
        entries[bob.hexhash].id,
    }
    assert node.mailbox.is_retract_cleanup_blocked(response["id"]) is False


def test_send_group_retraction_records_block_before_later_legs(
    tmp_path, monkeypatch
):
    # Qodo Informational on PR #706: the live fan-out queued legs
    # before every member's result was known, so a concurrent sweep
    # delivering an early leg could drop the sender's local copies
    # before a later member's refusal was recorded. The block is now
    # recorded as soon as a leg fails finally, inside the loop.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "crew hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent(
        "leg-1", to=alice.destination_hash, text="crew hello"
    )

    seen = {"calls": 0, "blocked_during_second": None}

    def refuse_then_unreachable(peer_arg, path, payload, timeout, deadline=None):
        seen["calls"] += 1
        if seen["calls"] == 1:
            raise NotAllowedError("peer rejected the request: not allowed")
        # Second member: the first member's refusal must already be
        # recorded — a sweep delivering this leg now must keep the
        # local copies.
        seen["blocked_during_second"] = bool(
            node.mailbox._retract_cleanup_blocked
        )
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", refuse_then_unreachable)
    response = node.send_group_retraction("crew", "leg-1", target_sent_at=100.0)
    assert response["ok"] is False
    assert seen["calls"] == 2
    assert seen["blocked_during_second"] is True
    assert node.mailbox.is_retract_cleanup_blocked(response["id"]) is True
    # The second member's leg is queued; when it later delivers, the
    # local copies stay.
    [entry] = node.mailbox.outbox_list()
    peer = alice if entry.to == alice.destination_hash else bob

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(peer, entry, 1.0) is True
    assert node.inbox.resolve("leg-1")["text"] == "crew hello"
    assert node.mailbox.is_sent_group_retracted(group["id"], 100.0) is False


def test_send_group_retraction_rejects_unknown_and_solo_groups(tmp_path):
    node, group, _, _ = _group_reaction_node(tmp_path)
    with pytest.raises(ValueError, match="unknown group"):
        node.send_group_retraction("nope", "leg-1", target_sent_at=100.0)
    # A group whose only member is this node cannot be created
    # (groups need at least 2 members), so pass the record directly.
    solo = {
        "id": "00" * 16,
        "name": "solo",
        "members": [node.destination.hexhash],
        "creator": node.destination.hexhash,
    }
    with pytest.raises(ValueError, match="no other members"):
        node.send_group_retraction(solo, "leg-1", target_sent_at=100.0)


def test_send_group_retraction_uses_retained_roster_after_roster_change(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    carol = make_peer("carol")
    node.groups.update_members(
        group["id"],
        [alice.hexhash, carol.hexhash, node.destination.hexhash],
        created_by=alice.hexhash,
    )
    # The old id is retired, but a message sent under it still
    # retracts — to the old roster, never the successor's.
    assert node.groups.get(group["id"]) is None
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append(peer_arg.hexhash)
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction(
        group["id"], "leg-1", target_sent_at=100.0
    )

    assert response["ok"] is True
    assert response["group"] == group["id"]
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        bob.hexhash,
    }
    assert set(calls) == {alice.hexhash, bob.hexhash}


def test_send_group_retraction_retired_id_never_resolves_to_namesake(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    carol = make_peer("carol")
    node.groups.update_members(
        group["id"],
        [alice.hexhash, carol.hexhash, node.destination.hexhash],
        created_by=alice.hexhash,
    )
    # A live group whose NAME is the retired id: find() would resolve
    # the id to this group through the name fallback.
    node.groups.create(
        group["id"],
        [alice.hexhash, carol.hexhash],
        created_by=alice.hexhash,
    )
    assert node.groups.find(group["id"])["name"] == group["id"]
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append(peer_arg.hexhash)
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction(
        group["id"], "leg-1", target_sent_at=100.0
    )

    # The retract went to the retired roster (alice and bob), not to
    # the namesake's members (alice and carol).
    assert response["ok"] is True
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        bob.hexhash,
    }
    assert set(calls) == {alice.hexhash, bob.hexhash}


def test_send_group_retraction_unresolved_id_never_resolves_to_namesake(
    tmp_path, monkeypatch
):
    node, _, alice, _ = _group_reaction_node(tmp_path)
    carol = make_peer("carol")
    # A live group named after a 32-hex id that is neither live nor
    # retained (e.g. retired before retention landed): the retract
    # must fail as an unknown group, never fan out to the namesake.
    orphan_id = "ff" * 16
    node.groups.create(
        orphan_id,
        [alice.hexhash, carol.hexhash],
        created_by=alice.hexhash,
    )
    assert node.groups.find(orphan_id)["name"] == orphan_id

    def _boom(*args, **kwargs):
        raise AssertionError("no leg may be attempted for an unknown group")

    monkeypatch.setattr(node, "_request", _boom)

    with pytest.raises(ValueError, match="unknown group"):
        node.send_group_retraction(orphan_id, "leg-1", target_sent_at=100.0)


def test_send_group_retraction_still_resolves_32_char_name(
    tmp_path, monkeypatch
):
    # A 32-character name that is not hex can never be an id: the
    # unresolved-id guard must not fire, and find() still resolves
    # the name.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    named = node.groups.create(
        "n" * 32,
        [alice.hexhash, bob.hexhash, node.destination.hexhash],
        created_by=alice.hexhash,
    )
    assert named["name"] == "n" * 32
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append(peer_arg.hexhash)
        return {"ok": True, "deleted": True, "group_retract": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_retraction("n" * 32, "leg-1", target_sent_at=100.0)

    assert response["ok"] is True
    assert response["group"] == named["id"]
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        bob.hexhash,
    }
    assert set(calls) == {alice.hexhash, bob.hexhash}


def test_send_group_retraction_needs_the_shared_send_time(tmp_path):
    node, _, _, _ = _group_reaction_node(tmp_path)
    with pytest.raises(NodeError, match="shared send time"):
        node.send_group_retraction("crew", "leg-1")


def test_send_group_edit_fans_out_one_signed_payload(tmp_path, monkeypatch):
    import base64

    from tau.protocol import REQUEST_DM_EDIT, edit_unsigned_bytes, parse_edit

    node, group, alice, bob = _group_reaction_node(tmp_path)
    # The sender's own record: id is the first leg id, with every leg
    # listed, so the fan-out can update the sent records afterwards.
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1", "leg-2"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent("leg-1", to=alice.destination_hash, text="typo here")
    node.mailbox.record_sent("leg-2", to=bob.destination_hash, text="typo here")
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append((peer_arg, path, payload))
        return {"ok": True, "edited": True, "group_edit": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )

    assert response["ok"] is True
    assert response["group"] == group["id"]
    assert response["target"] == "leg-1"
    # Both members got a leg; this node never edits for itself.
    assert {r["to"] for r in response["results"]} == {
        alice.hexhash,
        bob.hexhash,
    }
    assert all(r.get("delivered") for r in response["results"])
    assert all(r.get("edited") for r in response["results"])
    assert len(calls) == 2
    assert all(path == REQUEST_DM_EDIT for _, path, _ in calls)
    # One signed payload, one edit id, for every leg.
    assert calls[0][2] == calls[1][2]
    payload = parse_edit(calls[0][2])
    assert payload["target"] == "leg-1"
    assert payload["text"] == "fixed here"
    assert payload["group"] == group["id"]
    assert payload["target_author"] == node.destination.hexhash
    assert payload["target_sent_at"] == 100.0
    assert payload["id"] == response["id"]
    assert node.identity.validate(
        base64.b64decode(payload["sig"]),
        edit_unsigned_bytes(calls[0][2]),
    )
    # The sender's own copies took the new text too.
    entry = node.inbox.list_messages(direction="out")[0]
    assert entry["text"] == "fixed here"
    assert entry["edited_at"]
    sent = {record["id"]: record for record in node.mailbox.sent_list()}
    assert sent["leg-1"]["text"] == "fixed here"
    assert sent["leg-2"]["text"] == "fixed here"


def test_send_group_edit_defers_scheduled_legs(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    send_after = time.time() + 3600.0
    response = node.send_group_message("crew", "typo here", send_after=send_after)
    assert all(result.get("queued") for result in response["results"])
    sent_at = response["sent_at"]
    record = node.inbox.list_messages(direction="out")[0]

    def _boom(*args, **kwargs):
        raise AssertionError("no live edit may go out for a scheduled leg")

    monkeypatch.setattr(node, "_request", _boom)
    edit = node.send_group_edit(
        "crew", record["id"], "fixed here", target_sent_at=sent_at
    )

    # No live edit went out: both members are covered by their
    # re-queued legs, which already carry the edited text — so the
    # replacement text stays unsent until the scheduled time.
    assert edit["ok"] is True
    assert {r["to"] for r in edit["results"]} == {alice.hexhash, bob.hexhash}
    assert all(r.get("deferred") for r in edit["results"])
    assert all(r.get("delivered") for r in edit["results"])
    legs = node.mailbox.outbox_list()
    assert len(legs) == 2
    assert all(leg.text == "fixed here" for leg in legs)
    assert all(leg.send_after == send_after for leg in legs)
    # The sender's own copies took the new text too.
    assert node.inbox.list_messages(direction="out")[0]["text"] == "fixed here"


def test_send_group_edit_keeps_poll_options_on_scheduled_requeue(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    send_after = time.time() + 3600.0
    response = node.send_group_message(
        "crew",
        "which day?",
        poll_options=["Saturday", "Sunday"],
        send_after=send_after,
    )
    assert all(result.get("queued") for result in response["results"])
    sent_at = response["sent_at"]
    record = node.inbox.list_messages(direction="out")[0]

    monkeypatch.setattr(
        node,
        "_request",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no live edit may go out for a scheduled leg")
        ),
    )
    edit = node.send_group_edit(
        "crew", record["id"], "which weekend?", target_sent_at=sent_at
    )

    assert edit["ok"] is True
    legs = node.mailbox.outbox_list()
    assert len(legs) == 2
    assert all(leg.text == "which weekend?" for leg in legs)
    assert all(leg.poll_options == ["Saturday", "Sunday"] for leg in legs)


def test_send_group_edit_skips_live_edit_when_scheduled_requeue_fails(
    tmp_path, monkeypatch
):
    # A scheduled leg that was cancelled but could not be re-queued
    # must not receive the live edit: the member would get the
    # replacement text before the scheduled time, for a message that
    # is now gone entirely.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    send_after = time.time() + 3600.0
    response = node.send_group_message("crew", "typo here", send_after=send_after)
    assert all(result.get("queued") for result in response["results"])
    sent_at = response["sent_at"]
    record = node.inbox.list_messages(direction="out")[0]
    legs = {
        leg.to_hex: leg for leg in node.mailbox.outbox_list()
    }
    bob_leg = legs[bob.hexhash]
    real_enqueue = node.mailbox.enqueue

    def _failing_enqueue(*args, **kwargs):
        if kwargs.get("message_id") == bob_leg.id:
            raise MailboxError("disk full")
        return real_enqueue(*args, **kwargs)

    monkeypatch.setattr(node.mailbox, "enqueue", _failing_enqueue)

    def _boom(*args, **kwargs):
        raise AssertionError("no live edit may go out for a failed scheduled leg")

    monkeypatch.setattr(node, "_request", _boom)
    edit = node.send_group_edit(
        "crew", record["id"], "fixed here", target_sent_at=sent_at
    )

    # No live edit went out to anyone: Alice's leg re-queued fine,
    # so she is deferred, while Bob's leg is gone and the failure is
    # reported — with the edited text never handed to him early.
    assert edit["ok"] is False
    by_to = {r["to"]: r for r in edit["results"]}
    assert by_to[alice.hexhash].get("deferred") is True
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("error", "").startswith("could not re-queue")
    assert by_to[bob.hexhash].get("delivered") is None
    assert set(edit["cancelled_queued"]) == {
        legs[alice.hexhash].id,
        bob_leg.id,
    }
    assert edit["requeued"] == [legs[alice.hexhash].id]
    assert node.mailbox.get_outbox(bob_leg.id) is None
    requeued = node.mailbox.get_outbox(legs[alice.hexhash].id)
    assert requeued is not None
    assert requeued.text == "fixed here"
    # The local copy is untouched, so the edit stays retryable.
    assert node.inbox.list_messages(direction="out")[0]["text"] == "typo here"


def test_send_group_edit_restores_failed_scheduled_leg_for_retry(
    tmp_path, monkeypatch
):
    # A scheduled leg whose re-queue fails but whose restore succeeds
    # must still get no live edit in this call: the restored leg
    # carries the ORIGINAL text, so a live edit would hand the member
    # the replacement before the scheduled time.  The retry then finds
    # the restored leg and re-queues it with the edited text.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    send_after = time.time() + 3600.0
    response = node.send_group_message("crew", "typo here", send_after=send_after)
    assert all(result.get("queued") for result in response["results"])
    sent_at = response["sent_at"]
    record = node.inbox.list_messages(direction="out")[0]
    legs = {
        leg.to_hex: leg for leg in node.mailbox.outbox_list()
    }
    bob_leg = legs[bob.hexhash]
    real_enqueue = node.mailbox.enqueue

    def _flaky_enqueue(*args, **kwargs):
        if (
            kwargs.get("message_id") == bob_leg.id
            and kwargs.get("text") == "fixed here"
        ):
            raise MailboxError("disk full")
        return real_enqueue(*args, **kwargs)

    monkeypatch.setattr(node.mailbox, "enqueue", _flaky_enqueue)

    def _boom(*args, **kwargs):
        raise AssertionError("no live edit may go out for a failed scheduled leg")

    monkeypatch.setattr(node, "_request", _boom)
    edit = node.send_group_edit(
        "crew", record["id"], "fixed here", target_sent_at=sent_at
    )

    # No live edit went out to anyone: Alice's leg re-queued fine, so
    # she is deferred, while Bob's leg is back in the outbox with its
    # original text and the failure is reported — with the edited text
    # never handed to him early.
    assert edit["ok"] is False
    by_to = {r["to"]: r for r in edit["results"]}
    assert by_to[alice.hexhash].get("deferred") is True
    assert by_to[bob.hexhash].get("error", "").startswith("could not re-queue")
    restored = node.mailbox.get_outbox(bob_leg.id)
    assert restored is not None
    assert restored.text == "typo here"
    assert restored.send_after is not None and restored.send_after > time.time()
    # The local copy is untouched, so the edit stays retryable.
    assert node.inbox.list_messages(direction="out")[0]["text"] == "typo here"

    # Retry with a healthy mailbox: the restored leg is found again
    # and re-queued carrying the edited text, still with no live edit.
    monkeypatch.setattr(node.mailbox, "enqueue", real_enqueue)
    retry = node.send_group_edit(
        "crew", record["id"], "fixed here", target_sent_at=sent_at
    )
    assert retry["ok"] is True
    retry_by_to = {r["to"]: r for r in retry["results"]}
    assert retry_by_to[alice.hexhash].get("deferred") is True
    assert retry_by_to[bob.hexhash].get("deferred") is True
    assert node.mailbox.get_outbox(bob_leg.id).text == "fixed here"
    assert node.inbox.list_messages(direction="out")[0]["text"] == "fixed here"


def test_send_group_edit_reports_failed_leg_and_keeps_the_local_copy(
    tmp_path, monkeypatch
):
    node, _, alice, bob = _group_reaction_node(tmp_path)
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "crew",
            "leg_ids": ["leg-1", "leg-2"],
            "leg_sent_at": 100.0,
        }
    )

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NotAllowedError("peer rejected the request: not allowed")
        return {"ok": True, "edited": True, "group_edit": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )

    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("error") == "not_allowed"
    # The local copy is untouched, so the edit stays retryable.
    entry = node.inbox.list_messages(direction="out")[0]
    assert entry["text"] == "typo here"
    assert "edited_at" not in entry


def test_send_group_edit_reports_member_without_group_edit(
    tmp_path, monkeypatch
):
    # An older member answers the DM edit ok but never saw the
    # group context: it kept its copy, so the leg is not delivered
    # and the sender keeps its own copy for a retry.
    node, _, alice, bob = _group_reaction_node(tmp_path)

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            return {"ok": True, "edited": False, "_rtt": 0.004}
        return {"ok": True, "edited": True, "group_edit": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )

    assert response["ok"] is False
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("delivered") is None
    assert (
        by_to[bob.hexhash].get("error") == "member runs a version without group edit"
    )


def test_send_group_edit_requeues_cancelled_legs_with_edited_text(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    # Bob's leg never left: it sits in the outbox with the shared
    # send time of the original fan-out.
    node.mailbox.enqueue(
        to=bob.destination_hash,
        text="typo here",
        message_id="bob-leg-1",
        group={
            "id": group["id"],
            "name": group["name"],
            "creator": group["creator"],
            "members": list(group["members"]),
        },
        sent_at=100.0,
    )
    assert node.mailbox.get_outbox("bob-leg-1") is not None
    calls = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        calls.append(peer_arg.hexhash)
        return {"ok": True, "edited": False, "group_edit": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )

    # The queued leg is re-queued under its own id carrying the edited
    # text, so Bob still receives the message — edited — instead of
    # losing it entirely; the live edit still went out to him, so a
    # copy that reached him another way is rewritten.
    assert response["cancelled_queued"] == ["bob-leg-1"]
    assert response["requeued"] == ["bob-leg-1"]
    requeued = node.mailbox.get_outbox("bob-leg-1")
    assert requeued is not None
    assert requeued.text == "fixed here"
    assert requeued.wire_sent_at == 100.0
    assert response["ok"] is True
    assert set(calls) == {alice.hexhash, bob.hexhash}


def test_send_group_edit_requeue_carries_edit_timestamp(tmp_path, monkeypatch):
    # The re-queued leg carries the edit's signed send time, so a
    # recipient that first meets the message through the queue stores
    # it as the record's edited_at — and a replayed older edit cannot
    # rewind it.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.mailbox.enqueue(
        to=bob.destination_hash,
        text="typo here",
        message_id="bob-leg-1",
        group={
            "id": group["id"],
            "name": group["name"],
            "creator": group["creator"],
            "members": list(group["members"]),
        },
        sent_at=100.0,
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {
            "ok": True,
            "edited": False,
            "group_edit": True,
            "_rtt": 0.004,
        },
    )
    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )
    requeued = node.mailbox.get_outbox("bob-leg-1")
    assert requeued is not None
    assert requeued.edited_at == response["sent_at"]


def _group_leg_entry(node, group, peer, *, text, leg_id, edited_at=None):
    return node.mailbox.enqueue(
        to=peer.destination_hash,
        text=text,
        message_id=leg_id,
        group={
            "id": group["id"],
            "name": group["name"],
            "creator": group["creator"],
            "members": list(group["members"]),
        },
        sent_at=100.0,
        edited_at=edited_at,
    )


def test_deliver_entry_sends_fresh_text_after_edit_requeue(
    tmp_path, monkeypatch
):
    # A background sweep captured the original leg before the edit
    # replaced it: the delivery must encode the freshly read entry —
    # the edited text with its edit timestamp — not the snapshot.
    from tau.protocol import parse_group_message

    node, group, alice, bob = _group_reaction_node(tmp_path)
    _group_leg_entry(node, group, bob, text="typo here", leg_id="bob-leg-1")
    snapshot = node.mailbox.get_outbox("bob-leg-1")
    assert snapshot.text == "typo here"
    # The edit cancels the leg and re-queues it under the same id.
    assert node.mailbox.remove_outbox("bob-leg-1") is True
    _group_leg_entry(
        node, group, bob, text="fixed here", leg_id="bob-leg-1",
        edited_at=150.0,
    )
    calls = []
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: calls.append(args) or {"ok": True},
    )
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is True
    assert len(calls) == 1
    parsed = parse_group_message(calls[0][2])
    assert parsed["text"] == "fixed here"
    assert parsed["edited_at"] == 150.0
    assert node.mailbox.get_outbox("bob-leg-1") is None


def test_deliver_entry_aborts_when_entry_replaced_after_fresh_read(
    tmp_path, monkeypatch
):
    # The edit lands after the fresh read but before the send: the
    # delivery aborts instead of sending the superseded version, and
    # the replacement stays queued for a later round.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    _group_leg_entry(node, group, bob, text="typo here", leg_id="bob-leg-1")
    snapshot = node.mailbox.get_outbox("bob-leg-1")
    real_get = node.mailbox.get_outbox
    calls = {"n": 0}

    def flaky_get(message_id):
        calls["n"] += 1
        if calls["n"] == 1:
            # An edit cancels and re-queues the leg mid-delivery.
            node.mailbox.remove_outbox("bob-leg-1")
            _group_leg_entry(
                node, group, bob, text="fixed here", leg_id="bob-leg-1",
                edited_at=150.0,
            )
            return snapshot
        return real_get(message_id)

    monkeypatch.setattr(node.mailbox, "get_outbox", flaky_get)
    sent = []
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: sent.append(args) or {"ok": True},
    )
    assert node._deliver_entry(bob, snapshot, timeout=1.0) is False
    assert sent == []
    requeued = node.mailbox.get_outbox("bob-leg-1")
    assert requeued is not None
    assert requeued.text == "fixed here"
    assert requeued.edited_at == 150.0


def test_deliver_entry_keeps_replacement_when_it_lands_during_send(
    tmp_path, monkeypatch
):
    # The edit lands after the pre-send check but while the request is
    # in flight: the confirmed delivery stands, but the replacement
    # must not be deleted with the version that was sent.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    entry = _group_leg_entry(
        node, group, bob, text="typo here", leg_id="bob-leg-1"
    )

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        # An edit cancels and re-queues the leg while sending.
        node.mailbox.remove_outbox("bob-leg-1")
        _group_leg_entry(
            node, group, bob, text="fixed here", leg_id="bob-leg-1",
            edited_at=150.0,
        )
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)
    assert node._deliver_entry(bob, entry, timeout=1.0) is True
    requeued = node.mailbox.get_outbox("bob-leg-1")
    assert requeued is not None
    assert requeued.text == "fixed here"
    assert requeued.edited_at == 150.0


def test_send_group_edit_needs_the_shared_send_time(tmp_path):
    node, _, _, _ = _group_reaction_node(tmp_path)
    with pytest.raises(NodeError, match="shared send time"):
        node.send_group_edit("crew", "leg-1", "fixed here")


def _edit_node(tmp_path):
    """A node with a signing identity, started enough to queue."""
    import types

    node = make_node(tmp_path)
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    node.reticulum = object()  # started enough to queue
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    return node, peer


def _seed_sent_dm(node, peer, message_id="m-1", text="typo here"):
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": text,
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": message_id,
            "direction": "out",
        }
    )
    node.mailbox.record_sent(message_id, to=peer.destination_hash, text=text)


def test_send_edit_queues_for_offline_peer(tmp_path, monkeypatch):
    from tau.protocol import REQUEST_DM_EDIT, parse_edit

    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_edit("alice", "m-1", "fixed here")

    assert response["queued"] is True
    assert response["id"]
    assert response["target"] == "m-1"
    # The local copy updates on delivery, not at queue time.
    assert node.inbox.resolve("m-1")["text"] == "typo here"
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.kind == "edit"
    assert entry.to == peer.destination_hash
    assert entry.edit["target"] == "m-1"
    assert entry.edit["text"] == "fixed here"
    assert entry.edit["edit_id"] == response["id"]
    # The retry re-sends the identical signed bytes.
    sent = {}

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        sent["payload"] = payload
        assert path == REQUEST_DM_EDIT
        return {"ok": True, "edited": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(peer, entry, 1.0) is True
    import base64

    assert sent["payload"] == base64.b64decode(
        entry.edit["payload_b64"], validate=True
    )
    payload = parse_edit(sent["payload"])
    assert payload["target"] == "m-1"
    assert payload["text"] == "fixed here"
    assert payload["id"] == response["id"]
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "fixed here"
    assert node.inbox.resolve("m-1")["edited_at"] == payload["sent_at"]
    assert node.mailbox.sent_record("m-1")["text"] == "fixed here"


def test_send_edit_no_queue_raises_for_offline_peer(tmp_path, monkeypatch):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    with pytest.raises(NodeError, match="no route to peer"):
        node.send_edit("alice", "m-1", "fixed here", queue=False)
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_send_edit_refusal_is_not_queued(tmp_path, monkeypatch):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def denied(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", denied)
    with pytest.raises(NotAllowedError):
        node.send_edit("alice", "m-1", "fixed here")
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_send_edit_supersedes_older_queued_edit(tmp_path, monkeypatch):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    first = node.send_edit("alice", "m-1", "first fix")
    second = node.send_edit("alice", "m-1", "second fix")

    # Only the newer edit waits in the outbox.
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    assert entries[0].edit["edit_id"] == second["id"]
    assert entries[0].edit["edit_id"] != first["id"]
    assert entries[0].edit["text"] == "second fix"


def test_send_edit_queued_original_is_refused(tmp_path, monkeypatch):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)
    # The original send is still queued: the peer has never seen it.
    node.mailbox.enqueue(
        to=peer.destination_hash, text="typo here", message_id="m-1"
    )

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    with pytest.raises(NodeError, match="has not been sent yet"):
        node.send_edit("alice", "m-1", "fixed here")
    # Nothing new was queued: only the original still waits.
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    assert entries[0].id == "m-1"
    assert entries[0].edit is None
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_send_group_edit_queues_unreachable_legs(tmp_path, monkeypatch):
    from tau.protocol import REQUEST_DM_EDIT, parse_edit

    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1", "leg-2"],
            "leg_sent_at": 100.0,
        }
    )
    node.mailbox.record_sent("leg-1", to=alice.destination_hash, text="typo here")
    node.mailbox.record_sent("leg-2", to=bob.destination_hash, text="typo here")

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NodeError("no route to peer")
        return {"ok": True, "edited": True, "group_edit": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )

    assert response["ok"] is False
    assert response["queued"] == 1
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[alice.hexhash].get("delivered") is True
    assert by_to[bob.hexhash].get("queued") is True
    assert by_to[bob.hexhash].get("queued_id")
    # The local copy waits for a full fan-out: one leg still queued.
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    entries = node.mailbox.outbox_list()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.kind == "edit"
    assert entry.group_id == group["id"]
    assert entry.to == bob.destination_hash
    assert entry.edit["edit_id"] == response["id"]
    assert entry.edit["target_author"] is not None
    assert entry.edit["target_sent_at"] == 100.0
    # The sweep delivers it with the same signed bytes, and the local
    # copies update now that the fan-out is complete.
    sent = {}

    def reachable(peer_arg, path, payload, timeout, deadline=None):
        sent["payload"] = payload
        assert path == REQUEST_DM_EDIT
        return {"ok": True, "edited": True, "group_edit": True}

    monkeypatch.setattr(node, "_request", reachable)
    assert node._deliver_entry(bob, entry, 1.0) is True
    assert parse_edit(sent["payload"])["id"] == response["id"]
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "fixed here"
    assert node.mailbox.sent_record("leg-1")["text"] == "fixed here"
    assert node.mailbox.sent_record("leg-2")["text"] == "fixed here"


def test_send_group_edit_no_queue_reports_failed_leg(tmp_path, monkeypatch):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        if peer_arg.hexhash == bob.hexhash:
            raise NodeError("no route to peer")
        return {"ok": True, "edited": True, "group_edit": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0, queue=False
    )

    assert response["ok"] is False
    assert response["queued"] == 0
    by_to = {r["to"]: r for r in response["results"]}
    assert by_to[bob.hexhash].get("error")
    assert node.mailbox.outbox_list() == []


def test_deliver_queued_edit_drops_refused_leg(tmp_path, monkeypatch):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def refused(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", refused)
    assert node._deliver_entry(peer, entry, 1.0) is False
    # A refusal is final: the leg is dropped, not held until its TTL.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_deliver_queued_edit_keeps_local_copy_when_peer_has_no_copy(
    tmp_path, monkeypatch
):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def no_copy(peer_arg, path, payload, timeout, deadline=None):
        return {"ok": True, "edited": False, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", no_copy)
    assert node._deliver_entry(peer, entry, 1.0) is False
    # The peer had no copy: the leg is dropped and the local copy
    # keeps the old text, exactly as on the live path.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_deliver_queued_edit_drops_unusable_payload(tmp_path, monkeypatch):
    import json

    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")

    def boom(*args, **kwargs):
        raise AssertionError("an unusable edit must never be sent")

    monkeypatch.setattr(node, "_request", boom)
    path = node.mailbox.path
    raw = json.loads(path.read_text())
    raw["outbox"][0]["edit"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))
    # A fresh read drops the corrupt payload but keeps kind "edit".
    entry = Mailbox(path).outbox_list()[0]
    assert entry.kind == "edit"
    assert entry.edit is None
    # The delivery path re-reads the entry from disk too, so the stale
    # in-memory copy above never reaches the wire either.
    stale = node.mailbox.outbox_list()[0]

    assert node._deliver_entry(peer, stale, 1.0) is False
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_queued_group_edit_is_never_deposited(tmp_path, monkeypatch):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    calls = []

    def recorded(*args, **kwargs):
        calls.append(args)
        return {"ok": True}

    monkeypatch.setattr(node, "_request", recorded)
    # No sealed copy is ever left with a holder for a group edit:
    # legs carry a different wire id per member, so no single sealed
    # copy names every member's copy.
    assert node._deposit_entry(alice, entries[0], 1.0) is False
    assert node._deposit_entry(bob, entries[1], 1.0) is False
    assert calls == []
    assert len(node.mailbox.outbox_list()) == 2


def test_deliver_queued_group_edit_cancels_siblings_on_refusal(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1", "leg-2"],
            "leg_sent_at": 100.0,
        }
    )

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )
    assert response["queued"] == 2
    entries = node.mailbox.outbox_list()
    assert len(entries) == 2
    assert entries[0].edit["edit_id"] == entries[1].edit["edit_id"]

    def refused(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", refused)
    assert node._deliver_entry(alice, entries[0], 1.0) is False
    # The refusal kills the whole edit: the sibling leg goes with it,
    # since the fan-out can no longer complete — and the local copy
    # keeps the old text.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def _queue_group_edit(tmp_path, monkeypatch):
    """Queue a two-leg group edit; returns its outbox entries."""
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1", "leg-2"],
            "leg_sent_at": 100.0,
        }
    )

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )
    assert response["queued"] == 2
    entries = node.mailbox.outbox_list()
    assert len(entries) == 2
    assert entries[0].edit["edit_id"] == entries[1].edit["edit_id"]
    return node, group, alice, bob, entries


def test_deliver_queued_group_edit_drops_siblings_when_a_leg_expires(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # One leg's TTL ran out while the other was still queued: the
    # prune aborts the whole edit instead of leaving the sibling to
    # deliver later and rewrite the local copy on its own.
    path = node.mailbox.path
    raw = json.loads(path.read_text())
    for stored in raw["outbox"]:
        if stored["id"] == entries[0].id:
            stored["expires_at"] = 100.0
    path.write_text(json.dumps(raw))
    node.mailbox.prune()
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def test_deliver_queued_group_edit_keeps_local_copy_while_a_leg_is_outstanding(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # The first leg expired but has not been pruned yet: the second
    # leg may still deliver to its member, but the local copy must
    # not change — the fan-out is no longer complete.
    path = node.mailbox.path
    raw = json.loads(path.read_text())
    for stored in raw["outbox"]:
        if stored["id"] == entries[0].id:
            stored["expires_at"] = 100.0
    path.write_text(json.dumps(raw))

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    assert node._deliver_entry(bob, entries[1], 1.0) is True
    assert [e.id for e in node.mailbox.outbox_list()] == [entries[0].id]
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    # The next prune takes the expired leg too: nothing of the edit
    # is left queued.
    node.mailbox.prune()
    assert node.mailbox.outbox_list() == []


def test_cancel_queued_group_edit_leg_aborts_the_edit(tmp_path, monkeypatch):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # Cancelling one leg (``tau outbox cancel`` / the console) aborts
    # the whole edit: the sibling goes with it, so a later delivery
    # can never apply the local edit for a partial fan-out.
    assert (
        node.mailbox.remove_outbox(entries[0].id, abort_group_edit=True)
        is True
    )
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def test_deliver_queued_group_edit_drops_siblings_on_corrupt_payload(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # A leg whose stored payload did not survive can never deliver:
    # its siblings go with it, since the fan-out can no longer
    # complete.
    path = node.mailbox.path
    raw = json.loads(path.read_text())
    for stored in raw["outbox"]:
        if stored["id"] == entries[0].id:
            stored["edit"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))

    def boom(*args, **kwargs):
        raise AssertionError("an unusable edit must never be sent")

    monkeypatch.setattr(node, "_request", boom)
    stale = node.mailbox.outbox_list()[0]
    assert node._deliver_entry(alice, stale, 1.0) is False
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def test_deliver_queued_group_edit_keeps_local_copy_while_a_corrupt_sibling_is_queued(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # The second leg's stored payload did not survive, but the entry
    # keeps the edit id as a hint: the first leg may still deliver
    # to its member, but the local copy must not change — the
    # corrupt sibling is still queued, so the fan-out is not
    # complete.
    path = node.mailbox.path
    raw = json.loads(path.read_text())
    for stored in raw["outbox"]:
        if stored["id"] == entries[1].id:
            stored["edit"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    assert node._deliver_entry(alice, entries[0], 1.0) is True
    assert [e.id for e in node.mailbox.outbox_list()] == [entries[1].id]
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def test_drop_queued_edit_siblings_defers_when_mailbox_lock_is_busy(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # Another thread holds the mailbox lock: the deadline-bounded
    # sibling cleanup must give up promptly instead of waiting
    # indefinitely, and the siblings stay queued for a later round.
    held = threading.Event()
    release = threading.Event()

    def hold_lock():
        node.mailbox._lock.acquire()
        held.set()
        release.wait(timeout=30)
        node.mailbox._lock.release()

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert held.wait(timeout=5)
    try:
        start = time.monotonic()
        node._drop_queued_edit_siblings(
            entries[0].edit["edit_id"],
            exclude=entries[0].id,
            deadline=time.monotonic() + 0.2,
        )
        assert time.monotonic() - start < 5.0
    finally:
        release.set()
        holder.join(timeout=5)
    assert [e.id for e in node.mailbox.outbox_list()] == [
        entries[0].id,
        entries[1].id,
    ]


def test_deliver_queued_corrupt_edit_without_hint_defers_when_mailbox_lock_is_busy(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # The leg's stored payload did not survive and even the edit id
    # is gone, so the corrupt branch falls back to matching siblings
    # by group, send time and text — a mailbox lookup that must stay
    # deadline-bounded when another thread holds the lock.
    path = node.mailbox.path
    raw = json.loads(path.read_text())
    for stored in raw["outbox"]:
        if stored["id"] == entries[0].id:
            stored["edit"] = {"payload_b64": "!!!not-base64"}
    path.write_text(json.dumps(raw))
    with node.mailbox.transaction():
        fresh = node.mailbox.get_outbox(entries[0].id)
    assert fresh.edit is None
    assert fresh.group_edit_id is None

    held = threading.Event()
    release = threading.Event()

    def hold_lock():
        node.mailbox._lock.acquire()
        held.set()
        release.wait(timeout=10)
        node.mailbox._lock.release()

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert held.wait(timeout=5)
    try:
        start = time.monotonic()
        assert (
            node._deliver_queued_edit(
                alice, fresh, 1.0, deadline=time.monotonic() + 0.2
            )
            is False
        )
        assert time.monotonic() - start < 5.0
    finally:
        release.set()
        holder.join(timeout=5)
    assert [e.id for e in node.mailbox.outbox_list()] == [
        entries[0].id,
        entries[1].id,
    ]


def test_deliver_queued_corrupt_edit_drops_leg_and_siblings_together(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    # The first leg's stored payload did not survive: dropping it
    # must drop its sibling under the same mailbox transaction. A
    # lock that goes busy after the leg's own removal would leave
    # the sibling queued to deliver later and apply the local edit
    # for a partial fan-out — so simulate exactly that: the second
    # top-level lock acquisition fails.
    path = node.mailbox.path
    raw = json.loads(path.read_text())
    for stored in raw["outbox"]:
        if stored["id"] == entries[0].id:
            stored["edit"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))
    with node.mailbox.transaction():
        fresh = node.mailbox.get_outbox(entries[0].id)
    assert fresh.edit is None

    real_lock = node.mailbox._lock

    class _FailSecondAcquireLock:
        """An RLock that refuses the second fresh acquisition.

        Nested acquires by the thread that already holds the lock
        still succeed, exactly like the real RLock — only a new
        top-level acquisition can fail, which is what a lock going
        busy between two separate operations looks like.
        """

        def __init__(self, real):
            self._real = real
            self.top_level = 0

        def acquire(self, blocking=True, timeout=-1):
            if self._real._is_owned():
                return self._real.acquire(blocking, timeout)
            if timeout is not None and timeout >= 0:
                # A deadline-bounded wait can fail when the lock is
                # busy; a blocking wait cannot — the real RLock just
                # waits — so only bounded acquires refuse here.
                self.top_level += 1
                if self.top_level >= 2:
                    return False
            return self._real.acquire(blocking, timeout)

        def release(self):
            return self._real.release()

        def __enter__(self):
            if not self.acquire():
                raise TimeoutError("mailbox lock is held by another thread")
            return self

        def __exit__(self, *exc):
            self.release()
            return False

    node.mailbox._lock = _FailSecondAcquireLock(real_lock)
    try:
        assert (
            node._deliver_queued_edit(
                alice, fresh, 1.0, deadline=time.monotonic() + 5.0
            )
            is False
        )
    finally:
        node.mailbox._lock = real_lock
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def test_deliver_queued_edit_retries_sibling_check_after_timeout(
    tmp_path, monkeypatch
):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "edited": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings
    calls = []

    def flaky(edit_id, *, exclude=None, deadline=None):
        calls.append(edit_id)
        if len(calls) == 1:
            raise TimeoutError("simulated busy mailbox")
        return real_siblings(edit_id, exclude=exclude, deadline=deadline)

    monkeypatch.setattr(node, "_queued_edit_siblings", flaky)
    assert (
        node._deliver_entry(peer, entry, 1.0, deadline=time.monotonic() + 0.2)
        is True
    )
    assert len(calls) == 2
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "fixed here"


def test_deliver_queued_edit_skips_local_copy_when_sibling_check_stays_busy(
    tmp_path, monkeypatch
):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "edited": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(peer, entry, 1.0, deadline=time.monotonic() + 0.2)
        is True
    )
    # The delivery itself is confirmed, but the fan-out cannot be
    # proven complete: the local copy stays old rather than showing
    # an edit a queued sibling may never have received.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_deliver_queued_edit_defers_local_apply_until_a_later_round(
    tmp_path, monkeypatch
):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "edited": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(peer, entry, 1.0, deadline=time.monotonic() + 0.2)
        is True
    )
    # The delivery is confirmed, but the local copy must not change
    # until the fan-out is proven: the apply is deferred, durably.
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"
    pending = node.mailbox.pending_local_edits()
    assert [record["edit_id"] for record in pending] == [
        entry.edit["edit_id"]
    ]
    assert pending[0]["target"] == "m-1"
    assert pending[0]["text"] == "fixed here"

    # A later round retries the check — no resend — and applies.
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)
    node._retry_pending_local_edits()
    assert node.inbox.resolve("m-1")["text"] == "fixed here"
    assert node.mailbox.pending_local_edits() == []


def test_deferred_local_apply_waits_for_a_queued_sibling(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(
            alice, entries[0], 1.0, deadline=time.monotonic() + 0.2
        )
        is True
    )
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    assert len(node.mailbox.pending_local_edits()) == 1

    # The sibling is still queued: a retry must not apply yet.
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)
    node._retry_pending_local_edits()
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    assert len(node.mailbox.pending_local_edits()) == 1

    # The sibling delivers and applies the edit itself; the retry
    # then clears the now-redundant record.
    assert node._deliver_entry(bob, entries[1], 1.0) is True
    assert node.inbox.resolve("leg-1")["text"] == "fixed here"
    node._retry_pending_local_edits()
    assert node.mailbox.pending_local_edits() == []


def test_aborted_group_edit_drops_its_deferred_local_apply(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(
            alice, entries[0], 1.0, deadline=time.monotonic() + 0.2
        )
        is True
    )
    assert len(node.mailbox.pending_local_edits()) == 1

    # The remaining sibling is refused: the edit is aborted, so the
    # deferred apply must go with it — the local copy must never
    # show an edit a member never received.
    def refused(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", refused)
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)
    assert node._deliver_entry(bob, entries[1], 1.0) is False
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.pending_local_edits() == []
    node._retry_pending_local_edits()
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def test_deferred_local_apply_kept_when_local_write_fails(
    tmp_path, monkeypatch
):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "edited": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(peer, entry, 1.0, deadline=time.monotonic() + 0.2)
        is True
    )
    assert len(node.mailbox.pending_local_edits()) == 1
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)

    # A transient local-write failure must not lose the retry: the
    # record stays, and a later round applies it once the write
    # succeeds.
    real_apply = node._record_sent_edit_locally
    calls = []

    def flaky_apply(target, cleaned, edited_at, *, peer_hex):
        calls.append(1)
        if len(calls) == 1:
            return ["inbox copy not updated: simulated failure"]
        return real_apply(target, cleaned, edited_at, peer_hex=peer_hex)

    monkeypatch.setattr(node, "_record_sent_edit_locally", flaky_apply)
    node._retry_pending_local_edits()
    assert node.inbox.resolve("m-1")["text"] == "typo here"
    assert len(node.mailbox.pending_local_edits()) == 1
    node._retry_pending_local_edits()
    assert node.inbox.resolve("m-1")["text"] == "fixed here"
    assert node.mailbox.pending_local_edits() == []


def test_deferred_local_apply_restored_when_oserror_escapes(
    tmp_path, monkeypatch
):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "edited": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(peer, entry, 1.0, deadline=time.monotonic() + 0.2)
        is True
    )
    assert len(node.mailbox.pending_local_edits()) == 1
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)

    # An OSError escaping the local apply must not lose the retry:
    # the claimed record is restored, and a later round applies it
    # once the disk failure clears.
    real_apply = node._record_sent_edit_locally
    calls = []

    def flaky_apply(target, cleaned, edited_at, *, peer_hex):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("simulated disk failure")
        return real_apply(target, cleaned, edited_at, peer_hex=peer_hex)

    monkeypatch.setattr(node, "_record_sent_edit_locally", flaky_apply)
    node._retry_pending_local_edits()
    assert node.inbox.resolve("m-1")["text"] == "typo here"
    assert len(node.mailbox.pending_local_edits()) == 1
    node._retry_pending_local_edits()
    assert node.inbox.resolve("m-1")["text"] == "fixed here"
    assert node.mailbox.pending_local_edits() == []


def test_deferred_local_apply_restored_when_inbox_apply_raises_oserror(
    tmp_path, monkeypatch
):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)

    def unreachable(*args, **kwargs):
        raise NodeError("no route to peer")

    monkeypatch.setattr(node, "_request", unreachable)
    node.send_edit("alice", "m-1", "fixed here")
    entry = node.mailbox.outbox_list()[0]

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "edited": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(peer, entry, 1.0, deadline=time.monotonic() + 0.2)
        is True
    )
    assert len(node.mailbox.pending_local_edits()) == 1
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)

    # An OSError from the inbox itself (e.g. its lock file cannot
    # be opened) is a local failure, not a reason to lose the retry.
    real_apply_edit = node.inbox.apply_edit
    calls = []

    def flaky_apply_edit(message_id, text, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("simulated disk failure")
        return real_apply_edit(message_id, text, **kwargs)

    monkeypatch.setattr(node.inbox, "apply_edit", flaky_apply_edit)
    node._retry_pending_local_edits()
    assert node.inbox.resolve("m-1")["text"] == "typo here"
    assert len(node.mailbox.pending_local_edits()) == 1
    node._retry_pending_local_edits()
    assert node.inbox.resolve("m-1")["text"] == "fixed here"
    assert node.mailbox.pending_local_edits() == []


def test_last_leg_delivery_keeps_pending_record_when_local_apply_fails(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    # The first leg delivers but its sibling check times out: the
    # local apply is deferred, durably.
    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(
            alice, entries[0], 1.0, deadline=time.monotonic() + 0.2
        )
        is True
    )
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    assert len(node.mailbox.pending_local_edits()) == 1
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)

    # The last leg delivers, but the local apply fails once: the
    # pending record must survive for a later flush instead of
    # being dropped with the fan-out.
    real_apply = node._record_sent_edit_locally
    calls = []

    def flaky_apply(target, cleaned, edited_at, *, peer_hex):
        calls.append(1)
        if len(calls) == 1:
            return ["inbox copy not updated: simulated failure"]
        return real_apply(target, cleaned, edited_at, peer_hex=peer_hex)

    monkeypatch.setattr(node, "_record_sent_edit_locally", flaky_apply)
    assert node._deliver_entry(bob, entries[1], 1.0) is True
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    assert len(node.mailbox.pending_local_edits()) == 1

    # A later flush retries the apply — no resend — and clears it.
    node._retry_pending_local_edits()
    assert node.inbox.resolve("leg-1")["text"] == "fixed here"
    assert node.mailbox.pending_local_edits() == []


def test_deferred_local_apply_of_file_message_is_dropped(tmp_path):
    node, peer = _edit_node(tmp_path)
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "kind": "file",
            "name": "notes.txt",
            "size": 12,
            "sha256": "ab" * 32,
            "path": "files/holder/notes.txt",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "m-1",
            "direction": "out",
        }
    )
    node.mailbox.record_sent("m-1", to=peer.destination_hash, text="notes.txt")
    assert node.mailbox.add_pending_local_edit(
        {
            "edit_id": "edit-file",
            "target": "m-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": peer.destination_hash.hex(),
        }
    )
    # A file record has no text to edit, so the apply can never
    # succeed: the record is dropped instead of restored, and a
    # later round does not resurrect it.
    node._retry_pending_local_edits()
    assert node.mailbox.pending_local_edits() == []
    assert node.inbox.resolve("m-1").get("text") is None
    node._retry_pending_local_edits()
    assert node.mailbox.pending_local_edits() == []


def test_deferred_local_apply_of_missing_message_is_dropped(tmp_path):
    node, peer = _edit_node(tmp_path)
    assert node.mailbox.add_pending_local_edit(
        {
            "edit_id": "edit-gone",
            "target": "gone-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": peer.destination_hash.hex(),
        }
    )
    # The message left local history, so there is nothing to update:
    # the record is dropped instead of retried forever.
    node._retry_pending_local_edits()
    assert node.mailbox.pending_local_edits() == []


def test_deferred_local_apply_with_empty_text_is_dropped(tmp_path):
    node, peer = _edit_node(tmp_path)
    _seed_sent_dm(node, peer)
    assert node.mailbox.add_pending_local_edit(
        {
            "edit_id": "edit-empty",
            "target": "m-1",
            "text": "",
            "sent_at": 100.0,
            "peer_hex": peer.destination_hash.hex(),
        }
    )
    # An empty replacement can never apply, so the malformed record
    # is dropped instead of retried forever; the original stands.
    node._retry_pending_local_edits()
    assert node.mailbox.pending_local_edits() == []
    assert node.inbox.resolve("m-1")["text"] == "typo here"


def test_cancel_between_sibling_timeout_and_insert_does_not_resurrect_edit(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    alice_leg = next(e for e in entries if e.to == alice.destination_hash)
    bob_leg = next(e for e in entries if e.to == bob.destination_hash)

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    real_add = node.mailbox.add_pending_local_edit

    def cancelling_add(record, *, timeout=None):
        # Cancellation lands between the timed-out sibling check and
        # the insertion: the remaining leg goes, and the edit is
        # marked aborted before the record is inserted.
        node.mailbox.remove_outbox(bob_leg.id, abort_group_edit=True)
        return real_add(record, timeout=timeout)

    monkeypatch.setattr(node.mailbox, "add_pending_local_edit", cancelling_add)
    assert (
        node._deliver_entry(
            alice, alice_leg, 1.0, deadline=time.monotonic() + 5.0
        )
        is True
    )
    # The insertion was refused for the aborted edit: no record, no
    # later apply for the partial fan-out.
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.pending_local_edits() == []
    node._retry_pending_local_edits()
    assert node.inbox.resolve("leg-1")["text"] == "typo here"


def test_retry_skips_deferred_apply_cancelled_before_claim(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    alice_leg = next(e for e in entries if e.to == alice.destination_hash)
    bob_leg = next(e for e in entries if e.to == bob.destination_hash)

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(
            alice, alice_leg, 1.0, deadline=time.monotonic() + 5.0
        )
        is True
    )
    assert len(node.mailbox.pending_local_edits()) == 1
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)

    # Cancellation lands after the retry snapshots the record but
    # before its claim: the claim must find nothing and skip the
    # apply, not work from the stale snapshot.
    real_pending = node.mailbox.pending_local_edits
    calls = []

    def cancelling_pending(*args, **kwargs):
        result = real_pending(*args, **kwargs)
        if not calls:
            calls.append(1)
            node.mailbox.remove_outbox(bob_leg.id, abort_group_edit=True)
        return result

    monkeypatch.setattr(node.mailbox, "pending_local_edits", cancelling_pending)
    node._retry_pending_local_edits()
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    assert node.mailbox.pending_local_edits() == []


def test_second_group_leg_clears_deferred_local_apply(
    tmp_path, monkeypatch
):
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    alice_leg = next(e for e in entries if e.to == alice.destination_hash)
    bob_leg = next(e for e in entries if e.to == bob.destination_hash)

    def delivered(peer_arg, path_arg, payload, timeout, deadline=None):
        return {"ok": True, "group_edit": True, "received_at": 120.0}

    monkeypatch.setattr(node, "_request", delivered)
    real_siblings = node._queued_edit_siblings

    def always_busy(edit_id, *, exclude=None, deadline=None):
        raise TimeoutError("simulated busy mailbox")

    monkeypatch.setattr(node, "_queued_edit_siblings", always_busy)
    assert (
        node._deliver_entry(
            alice, alice_leg, 1.0, deadline=time.monotonic() + 5.0
        )
        is True
    )
    assert len(node.mailbox.pending_local_edits()) == 1
    # The second leg delivers with a working sibling check: no legs
    # remain, so the local edit applies here — and the pending record
    # from the first leg is already cleared, not applied again later.
    monkeypatch.setattr(node, "_queued_edit_siblings", real_siblings)
    assert (
        node._deliver_entry(bob, bob_leg, 1.0, deadline=time.monotonic() + 5.0)
        is True
    )
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "fixed here"
    assert node.mailbox.pending_local_edits() == []


def test_supersede_cancellation_and_claim_cannot_interleave(
    tmp_path, monkeypatch
):
    # A deferred apply exists for a queued group edit.  Superseding
    # it cancels the legs and marks the edit aborted; a flush
    # claiming the deferred apply at the same time must not observe
    # the legs gone while the record is still eligible.
    node, group, alice, bob, entries = _queue_group_edit(tmp_path, monkeypatch)
    edit_id = entries[0].edit["edit_id"]
    node.mailbox.add_pending_local_edit(
        {
            "edit_id": edit_id,
            "target": "leg-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": "ab" * 16,
        }
    )
    real_abort = node.mailbox.abort_pending_local_edit
    arrived = threading.Event()
    go = threading.Event()

    def pausing_abort(edit_id_arg, *, now=None, timeout=None):
        arrived.set()
        assert go.wait(timeout=5)
        return real_abort(edit_id_arg, now=now, timeout=timeout)

    monkeypatch.setattr(node.mailbox, "abort_pending_local_edit", pausing_abort)
    results = {}

    def cancel():
        results["cancelled"] = node._cancel_queued_edits(
            "leg-1", group_id=group["id"]
        )

    cancel_thread = threading.Thread(target=cancel)
    cancel_thread.start()
    assert arrived.wait(timeout=5)

    def claim():
        results["claimed"] = node._claim_pending_local_edit(edit_id)

    claim_thread = threading.Thread(target=claim)
    claim_thread.start()
    time.sleep(0.2)
    go.set()
    cancel_thread.join(timeout=5)
    claim_thread.join(timeout=5)
    assert not cancel_thread.is_alive()
    assert not claim_thread.is_alive()
    assert len(results["cancelled"]) == 2
    # The cancellation removed the legs and marked the edit aborted
    # in one transaction: the concurrent claim could not observe
    # legs-gone with the record still eligible.
    assert results["claimed"] is None
    assert node.mailbox.pending_local_edits() == []


def test_live_send_abort_and_claim_cannot_interleave(
    tmp_path, monkeypatch
):
    # A live group edit aborts when a member refuses it: the queued
    # legs for the other members are dropped and the edit marked
    # aborted.  A flush claiming a deferred apply at the same time
    # must not observe the legs gone while the record is still
    # eligible.
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1", "leg-2"],
            "leg_sent_at": 100.0,
        }
    )
    monkeypatch.setattr(
        "tau.node.generate_message_id", lambda: "edit-t13"
    )
    node.mailbox.add_pending_local_edit(
        {
            "edit_id": "edit-t13",
            "target": "leg-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": "ab" * 16,
        }
    )

    def route(peer, path, payload, timeout, deadline=None):
        if peer.destination_hash == alice.destination_hash:
            raise NodeError("no route to peer")
        raise NotAMemberError("peer rejected the request: not a member")

    monkeypatch.setattr(node, "_request", route)
    real_abort = node.mailbox.abort_pending_local_edit
    arrived = threading.Event()
    go = threading.Event()

    def pausing_abort(edit_id_arg, *, now=None, timeout=None):
        if edit_id_arg == "edit-t13":
            arrived.set()
            assert go.wait(timeout=5)
        return real_abort(edit_id_arg, now=now, timeout=timeout)

    monkeypatch.setattr(node.mailbox, "abort_pending_local_edit", pausing_abort)
    results = {}

    def send():
        results["response"] = node.send_group_edit(
            "crew", "leg-1", "fixed here", target_sent_at=100.0
        )

    send_thread = threading.Thread(target=send)
    send_thread.start()
    assert arrived.wait(timeout=5)

    def claim():
        results["claimed"] = node._claim_pending_local_edit("edit-t13")

    claim_thread = threading.Thread(target=claim)
    claim_thread.start()
    time.sleep(0.2)
    go.set()
    send_thread.join(timeout=5)
    claim_thread.join(timeout=5)
    assert not send_thread.is_alive()
    assert not claim_thread.is_alive()
    assert results["claimed"] is None
    assert node.mailbox.pending_local_edits() == []
    assert node.mailbox.outbox_list() == []


def test_send_group_edit_aborts_queued_legs_when_a_member_refuses(
    tmp_path, monkeypatch
):
    node, group, alice, bob = _group_reaction_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    node.inbox.append(
        {
            "from": "holder",
            "destination": "",
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "leg-1",
            "direction": "out",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
            "leg_ids": ["leg-1", "leg-2"],
            "leg_sent_at": 100.0,
        }
    )

    def mixed(peer_arg, path_arg, payload, timeout, deadline=None):
        if peer_arg.hexhash == alice.hexhash:
            raise NodeError("no route to peer")
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", mixed)
    response = node.send_group_edit(
        "crew", "leg-1", "fixed here", target_sent_at=100.0
    )
    # Bob's refusal dooms the edit, so Alice's queued leg is dropped
    # instead of delivering later and rewriting the local copy alone —
    # and the report says so instead of claiming it is queued.
    assert response["ok"] is False
    assert response["queued"] == 0
    assert node.mailbox.outbox_list() == []
    assert node.inbox.resolve("leg-1")["text"] == "typo here"
    alice_result = [
        r for r in response["results"] if r["to"] == alice.hexhash
    ][0]
    assert alice_result.get("queued") is False
    assert "aborted" in alice_result.get("error", "")
    bob_result = [r for r in response["results"] if r["to"] == bob.hexhash][0]
    assert bob_result.get("error") == "not_allowed"


def test_send_group_message_records_leg_sent_at(tmp_path, monkeypatch):
    node, _, _, _ = _group_reaction_node(tmp_path)

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        return {"ok": True}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_message("crew", "hi all")

    assert response["ok"] is True
    sent = node.inbox.list_messages(direction="out")
    assert len(sent) == 1
    # The sent record carries the legs' shared send time, so a later
    # retract can name the logical message for every member.
    assert sent[0]["leg_sent_at"] == response["sent_at"]
    assert sent[0]["leg_ids"]


def test_send_group_message_returns_shared_sent_at(tmp_path, monkeypatch):
    # Every leg of one send shares a send time, and the response
    # reports it: the console stores it on each outbound entry so a
    # reaction to a leg names the logical message every member holds.
    from tau.protocol import parse_group_message

    node, _, _, _ = _group_reaction_node(tmp_path)
    payloads = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        payloads.append(payload)
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)

    response = node.send_group_message("crew", "hi all")

    assert response["ok"] is True
    sent_at = response["sent_at"]
    assert isinstance(sent_at, float)
    assert len(payloads) == 2
    assert [parse_group_message(p)["sent_at"] for p in payloads] == [
        sent_at,
        sent_at,
    ]


def test_node_cannot_be_restarted_after_shutdown(tmp_path):
    node = make_node(tmp_path)
    node._shutdown = True
    with pytest.raises(NodeError, match="cannot be restarted"):
        node.start()


def test_save_peers_prunes_stale_peers_but_keeps_owed_ones(tmp_path, caplog):
    node = make_node(tmp_path)
    stale = make_peer("stale")
    fresh = make_peer("fresh")
    owed = make_peer("owed")
    node.registry.upsert(
        stale.destination_hash, stale.public_key, name="stale", now=1.0
    )
    node.registry.upsert(fresh.destination_hash, fresh.public_key, name="fresh")
    node.registry.upsert(owed.destination_hash, owed.public_key, name="owed", now=1.0)
    node.mailbox.enqueue(to=owed.destination_hash, text="hello")

    with caplog.at_level(logging.DEBUG, logger="tau.node"):
        node._save_peers(force=True)

    assert "Pruned 1 stale peer(s) from the registry" in caplog.text
    assert node.registry.get(stale.destination_hash) is None
    assert node.registry.get(fresh.destination_hash) is not None
    assert node.registry.get(owed.destination_hash) is not None

    reloaded = PeerRegistry(node.registry.path)
    assert reloaded.get(stale.destination_hash) is None
    assert len(reloaded) == 2


def test_save_peers_keeps_holder_with_pending_acks(tmp_path):
    node = make_node(tmp_path)
    holder = make_peer("holder")
    node.registry.upsert(
        holder.destination_hash, holder.public_key, name="holder", now=1.0
    )
    node.mailbox.add_pending_acks(holder.hexhash, ["message-1"])

    node._save_peers(force=True)

    assert node.registry.get(holder.destination_hash) is not None
    assert node.mailbox.pending_ack_holders() == [holder.hexhash]


# -- peer exchange -------------------------------------------------------

from tau.protocol import decode  # noqa: E402


def make_exchange_entry(name="carol", *, last_seen=1000.0, hops=2):
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    entry = {
        "destination": destination.hex(),
        "name": name,
        "public_key": identity.get_public_key().hex(),
        "last_seen": last_seen,
        "hops": hops,
        "protocol": 1,
    }
    return entry, destination


def test_handle_peers_serves_exchange_entries(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=time.time()
    )
    raw = node._handle_peers("/tau/peers", b"{}", b"request-id", None, time.time())
    response = decode(raw)

    assert response["ok"] is True
    assert response["count"] == 1
    served = response["peers"][0]
    assert served["destination"] == peer.hexhash
    assert served["public_key"] == peer.public_key.hex()


def test_exchange_peers_merges_new_peers(tmp_path):
    node = make_node(tmp_path)
    source = make_peer("bob")
    node.registry.upsert(
        source.destination_hash, source.public_key, name="bob", now=100.0
    )
    entry, destination = make_exchange_entry("carol", last_seen=time.time())
    node._request = lambda peer, path, payload, timeout: {
        "ok": True,
        "peers": [entry],
        "count": 1,
    }

    summary = node.exchange_peers(source)

    assert summary["source"] == source.hexhash
    assert summary["added"] == 1
    assert summary["skipped"] == 0
    learned = node.registry.get(destination)
    assert learned is not None
    assert learned.name == "carol"
    assert learned.public_key.hex() == entry["public_key"]


def test_exchange_peers_rejects_malformed_response(tmp_path):
    node = make_node(tmp_path)
    source = make_peer("bob")
    node._request = lambda peer, path, payload, timeout: {"ok": True}
    with pytest.raises(Exception, match="invalid peers response"):
        node.exchange_peers(source)


def _profile_peer():
    """A peer identity plus its signed profile document."""
    from tau.profile import build_profile

    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    peer = Peer(
        destination_hash=destination,
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    doc = build_profile(
        identity,
        name="alice",
        bio="mesh tinkerer",
        avatar="ab" * 16,
        links=["https://example.com"],
        version=2,
        created_at=1000.0,
    )
    return peer, doc


def test_handle_profile_serves_local_profile_when_newer(tmp_path):
    from tau.profile import save_local_profile
    from tau.protocol import encode_profile_request

    node = make_node(tmp_path)
    _, doc = _profile_peer()
    save_local_profile(node.config.profile_path, doc)
    raw = node._handle_profile(
        "/tau/profile",
        encode_profile_request(1),
        b"request-id",
        None,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is True
    assert response["profile"]["version"] == 2
    assert response["profile"]["bio"] == "mesh tinkerer"


def test_handle_profile_withholds_profile_when_not_newer(tmp_path):
    from tau.profile import save_local_profile
    from tau.protocol import encode_profile_request

    node = make_node(tmp_path)
    _, doc = _profile_peer()
    save_local_profile(node.config.profile_path, doc)
    raw = node._handle_profile(
        "/tau/profile",
        encode_profile_request(2),
        b"request-id",
        None,
        time.time(),
    )
    assert decode(raw)["profile"] is None


def test_handle_profile_without_local_profile_answers_none(tmp_path):
    from tau.protocol import encode_profile_request

    node = make_node(tmp_path)
    assert node.local_profile is None
    raw = node._handle_profile(
        "/tau/profile",
        encode_profile_request(0),
        b"request-id",
        None,
        time.time(),
    )
    assert decode(raw)["profile"] is None


def test_handle_profile_rejects_bad_request(tmp_path):
    node = make_node(tmp_path)
    raw = node._handle_profile(
        "/tau/profile", b'{"type": "ping"}', b"request-id", None, time.time()
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "bad_request"


def test_request_profile_stores_verified_profile(tmp_path):
    from tau.protocol import REQUEST_PROFILE, encode_profile_request

    node = make_node(tmp_path)
    peer, doc = _profile_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    seen = {}

    def fake_request(peer_arg, path, payload, timeout):
        seen["path"] = path
        seen["payload"] = payload
        return {"ok": True, "v": 1, "profile": doc}

    node._request = fake_request
    result = node.request_profile(peer)
    assert seen["path"] == REQUEST_PROFILE
    assert result is not None and result["version"] == 2
    stored = node.profile_store.get(peer.hexhash)
    assert stored is not None and stored["bio"] == "mesh tinkerer"
    # The request names the version already held (0 here), so a peer
    # with nothing newer can answer with nothing.
    assert decode(seen["payload"])["known_version"] == 0


def test_request_profile_returns_none_when_peer_has_nothing_new(tmp_path):
    node = make_node(tmp_path)
    peer, doc = _profile_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node.profile_store.store(doc)
    seen = {}

    def fake_request(peer_arg, path, payload, timeout):
        seen["payload"] = payload
        return {"ok": True, "v": 1, "profile": None}

    node._request = fake_request
    assert node.request_profile(peer) is None
    # The request named the stored version, so the peer knew not to send.
    assert decode(seen["payload"])["known_version"] == 2


def test_request_profile_rejects_profile_for_another_peer(tmp_path):
    node = make_node(tmp_path)
    peer, _doc = _profile_peer()
    other_peer, other_doc = _profile_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "profile": other_doc,
    }
    with pytest.raises(NodeError, match="does not match the peer"):
        node.request_profile(peer)
    assert node.profile_store.get(peer.hexhash) is None


def test_request_profile_rejects_tampered_profile(tmp_path):
    node = make_node(tmp_path)
    peer, doc = _profile_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    tampered = dict(doc)
    tampered["bio"] = "forged"
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "profile": tampered,
    }
    with pytest.raises(NodeError, match="invalid profile"):
        node.request_profile(peer)
    assert node.profile_store.get(peer.hexhash) is None


def test_request_profile_rejects_bad_response(tmp_path):
    node = make_node(tmp_path)
    peer, _doc = _profile_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node._request = lambda p, path, payload, timeout: {"ok": False}
    with pytest.raises(NodeError, match="invalid profile response"):
        node.request_profile(peer)


def _follow_list_peer():
    """A peer identity plus its signed follow-list document."""
    from tau.follows import build_follow_list

    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    peer = Peer(
        destination_hash=destination,
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    followed = [
        RNS.Destination.hash(RNS.Identity(), APP_NAME, ASPECT).hex()
        for _ in range(2)
    ]
    doc = build_follow_list(
        identity, authors=followed, version=2, updated_at=1000.0
    )
    return peer, doc


def test_handle_follows_serves_local_follow_list_when_newer(tmp_path):
    from tau.follows import save_local_follow_list
    from tau.protocol import encode_follows_request

    node = make_node(tmp_path)
    _, doc = _follow_list_peer()
    save_local_follow_list(node.config.follow_list_path, doc)
    raw = node._handle_follows(
        "/tau/follows",
        encode_follows_request(1),
        b"request-id",
        None,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is True
    assert response["follow_list"]["version"] == 2
    assert response["follow_list"]["authors"] == doc["authors"]


def test_handle_follows_withholds_follow_list_when_not_newer(tmp_path):
    from tau.follows import save_local_follow_list
    from tau.protocol import encode_follows_request

    node = make_node(tmp_path)
    _, doc = _follow_list_peer()
    save_local_follow_list(node.config.follow_list_path, doc)
    raw = node._handle_follows(
        "/tau/follows",
        encode_follows_request(2),
        b"request-id",
        None,
        time.time(),
    )
    assert decode(raw)["follow_list"] is None


def test_handle_follows_without_local_list_answers_none(tmp_path):
    from tau.protocol import encode_follows_request

    node = make_node(tmp_path)
    assert node.local_follow_list is None
    raw = node._handle_follows(
        "/tau/follows",
        encode_follows_request(0),
        b"request-id",
        None,
        time.time(),
    )
    assert decode(raw)["follow_list"] is None


def test_handle_follows_rejects_bad_request(tmp_path):
    node = make_node(tmp_path)
    raw = node._handle_follows(
        "/tau/follows", b'{"type": "ping"}', b"request-id", None, time.time()
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "bad_request"


def test_request_follows_stores_verified_list(tmp_path):
    from tau.protocol import REQUEST_FOLLOWS, encode_follows_request

    node = make_node(tmp_path)
    peer, doc = _follow_list_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    seen = {}

    def fake_request(peer_arg, path, payload, timeout):
        seen["path"] = path
        seen["payload"] = payload
        return {"ok": True, "v": 1, "follow_list": doc}

    node._request = fake_request
    result = node.request_follows(peer)
    assert seen["path"] == REQUEST_FOLLOWS
    assert result is not None and result["version"] == 2
    stored = node.follow_list_store.get(peer.hexhash)
    assert stored is not None and stored["authors"] == doc["authors"]
    # The request names the version already held (0 here), so a peer
    # with nothing newer can answer with nothing.
    assert decode(seen["payload"])["known_version"] == 0


def test_request_follows_returns_none_when_peer_has_nothing_new(tmp_path):
    node = make_node(tmp_path)
    peer, doc = _follow_list_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node.follow_list_store.store(doc)
    seen = {}

    def fake_request(peer_arg, path, payload, timeout):
        seen["payload"] = payload
        return {"ok": True, "v": 1, "follow_list": None}

    node._request = fake_request
    assert node.request_follows(peer) is None
    # The request named the stored version, so the peer knew not to send.
    assert decode(seen["payload"])["known_version"] == 2


def test_request_follows_rejects_list_for_another_peer(tmp_path):
    node = make_node(tmp_path)
    peer, _doc = _follow_list_peer()
    other_peer, other_doc = _follow_list_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "follow_list": other_doc,
    }
    with pytest.raises(NodeError, match="does not match the peer"):
        node.request_follows(peer)
    assert node.follow_list_store.get(peer.hexhash) is None


def test_request_follows_rejects_tampered_list(tmp_path):
    node = make_node(tmp_path)
    peer, doc = _follow_list_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    tampered = dict(doc)
    tampered["authors"] = []
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "follow_list": tampered,
    }
    with pytest.raises(NodeError, match="invalid follow list"):
        node.request_follows(peer)
    assert node.follow_list_store.get(peer.hexhash) is None


def test_request_follows_rejects_bad_response(tmp_path):
    node = make_node(tmp_path)
    peer, _doc = _follow_list_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node._request = lambda p, path, payload, timeout: {"ok": False}
    with pytest.raises(NodeError, match="invalid follow list response"):
        node.request_follows(peer)


def _page_peer():
    """A peer identity plus its signed personal page document."""
    from tau.pages import build_page

    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    peer = Peer(
        destination_hash=destination,
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    doc = build_page(
        identity,
        title="Mesh meetup",
        markdown="# Mesh meetup\n\nSaturday, 2pm, Rock Springs Park.\n",
        version=2,
        updated_at=1000.0,
    )
    return peer, doc


def test_handle_page_serves_local_page_when_newer(tmp_path):
    from tau.pages import save_local_page
    from tau.protocol import decode, encode_page_request

    node = make_node(tmp_path)
    _, doc = _page_peer()
    save_local_page(node.config.page_path, doc)
    raw = node._handle_page(
        "/tau/page",
        encode_page_request(1),
        b"request-id",
        None,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is True
    assert response["page"]["version"] == 2
    assert response["page"]["title"] == "Mesh meetup"


def test_handle_page_withholds_page_when_not_newer(tmp_path):
    from tau.pages import save_local_page
    from tau.protocol import decode, encode_page_request

    node = make_node(tmp_path)
    _, doc = _page_peer()
    save_local_page(node.config.page_path, doc)
    raw = node._handle_page(
        "/tau/page",
        encode_page_request(2),
        b"request-id",
        None,
        time.time(),
    )
    assert decode(raw)["page"] is None


def test_handle_page_without_local_page_answers_none(tmp_path):
    from tau.protocol import decode, encode_page_request

    node = make_node(tmp_path)
    assert node.local_page is None
    raw = node._handle_page(
        "/tau/page",
        encode_page_request(0),
        b"request-id",
        None,
        time.time(),
    )
    assert decode(raw)["page"] is None


def test_handle_page_rejects_bad_request(tmp_path):
    from tau.protocol import decode

    node = make_node(tmp_path)
    raw = node._handle_page(
        "/tau/page", b'{"type": "ping"}', b"request-id", None, time.time()
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "bad_request"


def test_handle_page_denies_unlisted_caller_when_enforced(tmp_path):
    import json

    from tau.pages import save_local_page
    from tau.protocol import decode, encode_page_request

    node = make_node(tmp_path)
    _, doc = _page_peer()
    save_local_page(node.config.page_path, doc)
    # Allow only a different peer; the caller below is a stranger.
    other = RNS.Identity()
    other_hash = RNS.Destination.hash(other, APP_NAME, ASPECT).hex()
    node.config.page_allowlist_path.write_text(
        json.dumps({"enabled": True, "allow": [other_hash]}),
        encoding="utf-8",
    )
    stranger = RNS.Identity()
    raw = node._handle_page(
        "/tau/page",
        encode_page_request(0),
        b"request-id",
        stranger,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"


def test_handle_page_serves_listed_caller_when_enforced(tmp_path):
    import json

    from tau.pages import save_local_page
    from tau.protocol import decode, encode_page_request

    node = make_node(tmp_path)
    _, doc = _page_peer()
    save_local_page(node.config.page_path, doc)
    caller = RNS.Identity()
    caller_hash = RNS.Destination.hash(caller, APP_NAME, ASPECT).hex()
    node.config.page_allowlist_path.write_text(
        json.dumps({"enabled": True, "allow": [caller_hash]}),
        encoding="utf-8",
    )
    raw = node._handle_page(
        "/tau/page",
        encode_page_request(0),
        b"request-id",
        caller,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is True
    assert response["page"]["version"] == 2


def test_handle_page_fails_closed_on_corrupt_allowlist(tmp_path):
    from tau.pages import save_local_page
    from tau.protocol import decode, encode_page_request

    node = make_node(tmp_path)
    _, doc = _page_peer()
    save_local_page(node.config.page_path, doc)
    node.config.page_allowlist_path.write_text(
        "{not json", encoding="utf-8"
    )
    raw = node._handle_page(
        "/tau/page",
        encode_page_request(0),
        b"request-id",
        RNS.Identity(),
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"


def test_request_page_stores_verified_page(tmp_path):
    from tau.protocol import REQUEST_PAGE, decode, encode_page_request

    node = make_node(tmp_path)
    peer, doc = _page_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    seen = {}

    def fake_request(peer_arg, path, payload, timeout):
        seen["path"] = path
        seen["payload"] = payload
        return {"ok": True, "v": 1, "page": doc}

    node._request = fake_request
    result = node.request_page(peer)
    assert seen["path"] == REQUEST_PAGE
    assert result is not None and result["version"] == 2
    stored = node.page_store.get(peer.hexhash)
    assert stored is not None and stored["title"] == "Mesh meetup"
    # The request names the version already held (0 here), so a peer
    # with nothing newer can answer with nothing.
    assert decode(seen["payload"])["known_version"] == 0


def test_request_page_returns_none_when_peer_has_nothing_new(tmp_path):
    from tau.protocol import decode

    node = make_node(tmp_path)
    peer, doc = _page_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node.page_store.store(doc)
    seen = {}

    def fake_request(peer_arg, path, payload, timeout):
        seen["payload"] = payload
        return {"ok": True, "v": 1, "page": None}

    node._request = fake_request
    assert node.request_page(peer) is None
    # The request named the stored version, so the peer knew not to send.
    assert decode(seen["payload"])["known_version"] == 2


def test_request_page_rejects_page_for_another_peer(tmp_path):
    node = make_node(tmp_path)
    peer, _doc = _page_peer()
    other_peer, other_doc = _page_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "page": other_doc,
    }
    with pytest.raises(NodeError, match="does not match the peer"):
        node.request_page(peer)
    assert node.page_store.get(peer.hexhash) is None


def test_request_page_rejects_tampered_page(tmp_path):
    node = make_node(tmp_path)
    peer, doc = _page_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    tampered = dict(doc)
    tampered["markdown"] = "forged"
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "page": tampered,
    }
    with pytest.raises(NodeError, match="invalid page"):
        node.request_page(peer)
    assert node.page_store.get(peer.hexhash) is None


def test_request_page_rejects_bad_response(tmp_path):
    node = make_node(tmp_path)
    peer, _doc = _page_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node._request = lambda p, path, payload, timeout: {"ok": False}
    with pytest.raises(NodeError, match="invalid page response"):
        node.request_page(peer)


def _page_peer_with_files():
    """A peer identity, its signed page with one linked file, and the bytes."""
    from tau.pages import build_page, make_page_file_entry

    data = b"meetup notes and map"
    entry = make_page_file_entry("notes.txt", data)
    peer_identity = RNS.Identity()
    destination = RNS.Destination.hash(peer_identity, APP_NAME, ASPECT)
    peer = Peer(
        destination_hash=destination,
        name="alice",
        public_key=peer_identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    doc = build_page(
        peer_identity,
        title="Mesh meetup",
        markdown="# Mesh meetup\n\nSaturday, 2pm.\n",
        files=[entry],
        version=2,
        updated_at=1000.0,
    )
    return peer, doc, entry, data


def test_handle_page_file_serves_linked_file(tmp_path):
    import base64

    from tau.pages import save_local_page, store_page_file
    from tau.protocol import decode, encode_page_file_request

    node = make_node(tmp_path)
    _peer, doc, entry, data = _page_peer_with_files()
    save_local_page(node.config.page_path, doc)
    store_page_file(node.config.page_files_dir, entry["sha256"], data)
    raw = node._handle_page_file(
        "/tau/page/file",
        encode_page_file_request(entry["sha256"]),
        b"request-id",
        None,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is True
    assert response["file"]["name"] == "notes.txt"
    assert response["file"]["sha256"] == entry["sha256"]
    assert response["file"]["size"] == len(data)
    assert base64.b64decode(response["file"]["data"]) == data


def test_handle_page_file_rejects_unknown_sha(tmp_path):
    from tau.pages import save_local_page
    from tau.protocol import decode, encode_page_file_request

    node = make_node(tmp_path)
    _peer, doc, entry, data = _page_peer_with_files()
    save_local_page(node.config.page_path, doc)
    raw = node._handle_page_file(
        "/tau/page/file",
        encode_page_file_request("0" * 64),
        b"request-id",
        None,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "unknown_file"


def test_handle_page_file_without_local_page(tmp_path):
    from tau.protocol import decode, encode_page_file_request

    node = make_node(tmp_path)
    assert node.local_page is None
    raw = node._handle_page_file(
        "/tau/page/file",
        encode_page_file_request("0" * 64),
        b"request-id",
        None,
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "no_page"


def test_handle_page_file_denies_unlisted_caller(tmp_path):
    import json

    from tau.pages import save_local_page, store_page_file
    from tau.protocol import decode, encode_page_file_request

    node = make_node(tmp_path)
    _peer, doc, entry, data = _page_peer_with_files()
    save_local_page(node.config.page_path, doc)
    store_page_file(node.config.page_files_dir, entry["sha256"], data)
    other = RNS.Identity()
    other_hash = RNS.Destination.hash(other, APP_NAME, ASPECT).hex()
    node.config.page_allowlist_path.write_text(
        json.dumps({"enabled": True, "allow": [other_hash]}),
        encoding="utf-8",
    )
    raw = node._handle_page_file(
        "/tau/page/file",
        encode_page_file_request(entry["sha256"]),
        b"request-id",
        RNS.Identity(),
        time.time(),
    )
    response = decode(raw)
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"


def test_request_page_fetches_linked_files(tmp_path):
    import base64

    from tau.pages import load_page_file
    from tau.protocol import REQUEST_PAGE, REQUEST_PAGE_FILE

    node = make_node(tmp_path)
    peer, doc, entry, data = _page_peer_with_files()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    seen = []

    def fake_request(peer_arg, path, payload, timeout):
        seen.append(path)
        if path == REQUEST_PAGE:
            return {"ok": True, "v": 1, "page": doc}
        assert path == REQUEST_PAGE_FILE
        return {
            "ok": True,
            "v": 1,
            "file": {
                "name": entry["name"],
                "sha256": entry["sha256"],
                "size": len(data),
                "data": base64.b64encode(data).decode("ascii"),
            },
        }

    node._request = fake_request
    result = node.request_page(peer)
    assert result is not None and result["version"] == 2
    assert seen == [REQUEST_PAGE, REQUEST_PAGE_FILE]
    assert load_page_file(node.config.page_files_dir, entry["sha256"]) == data


def test_request_page_tolerates_failed_file_fetch(tmp_path):
    from tau.pages import load_page_file
    from tau.protocol import REQUEST_PAGE, REQUEST_PAGE_FILE

    node = make_node(tmp_path)
    peer, doc, entry, _data = _page_peer_with_files()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )

    def fake_request(peer_arg, path, payload, timeout):
        if path == REQUEST_PAGE:
            return {"ok": True, "v": 1, "page": doc}
        assert path == REQUEST_PAGE_FILE
        return {"ok": False, "v": 1, "error": {"code": "gone", "message": "x"}}

    node._request = fake_request
    result = node.request_page(peer)
    assert result is not None and result["version"] == 2
    # The page is stored even though its file could not be fetched.
    assert node.page_store.get(peer.hexhash) is not None
    assert load_page_file(node.config.page_files_dir, entry["sha256"]) is None


def test_request_page_file_rejects_hash_mismatch(tmp_path):
    import base64

    node = make_node(tmp_path)
    peer, _doc, entry, _data = _page_peer_with_files()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    wrong = base64.b64encode(b"different bytes here").decode("ascii")
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "file": {
            "name": entry["name"],
            "sha256": entry["sha256"],
            "size": len(b"different bytes here"),
            "data": wrong,
        },
    }
    with pytest.raises(NodeError, match="do not match the page"):
        node.request_page_file(peer, entry)


def test_request_page_file_rejects_wrong_size(tmp_path):
    import base64

    node = make_node(tmp_path)
    peer, _doc, entry, data = _page_peer_with_files()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    node._request = lambda p, path, payload, timeout: {
        "ok": True,
        "v": 1,
        "file": {
            "name": entry["name"],
            "sha256": entry["sha256"],
            "size": 999,
            "data": base64.b64encode(data).decode("ascii"),
        },
    }
    with pytest.raises(NodeError, match="size does not match"):
        node.request_page_file(peer, entry)


def test_on_announce_fetches_newer_profile(tmp_path, monkeypatch):
    node = _heard_node(tmp_path, monkeypatch, hops=1)
    fetches = []
    rounds = []
    monkeypatch.setattr(node, "_spawn_mail_round", lambda peer: rounds.append(peer))
    monkeypatch.setattr(
        node, "_spawn_profile_fetch", lambda peer: fetches.append(peer)
    )
    monkeypatch.setattr(node, "_save_peers", lambda: None)
    # The fetch only runs on a started node (reticulum up).
    node.reticulum = object()
    identity = RNS.Identity()
    announced_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT)

    node._on_announce(announced_hash, identity, build_announce("alice", profile_version=3))

    assert len(fetches) == 1
    assert fetches[0].hexhash == announced_hash.hex()
    assert len(rounds) == 1


def test_on_announce_skips_profile_fetch_when_version_known(
    tmp_path, monkeypatch
):
    node = _heard_node(tmp_path, monkeypatch, hops=1)
    fetches = []
    monkeypatch.setattr(node, "_spawn_mail_round", lambda peer: None)
    monkeypatch.setattr(
        node, "_spawn_profile_fetch", lambda peer: fetches.append(peer)
    )
    monkeypatch.setattr(node, "_save_peers", lambda: None)
    node.reticulum = object()
    peer, doc = _profile_peer()
    node.profile_store.store(doc)
    identity = RNS.Identity()
    # Rebuild the announce from the profile owner's key so the
    # destination matches the stored profile's author.
    announced_hash = peer.destination_hash
    # Announce the same version the store already holds.
    node._on_announce(
        announced_hash,
        _identity_for_peer(peer),
        build_announce("alice", profile_version=2),
    )
    assert fetches == []


def _identity_for_peer(peer: Peer) -> RNS.Identity:
    identity = RNS.Identity(create_keys=False)
    identity.load_public_key(peer.public_key)
    return identity


def test_mail_round_exchanges_peers(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("bob")
    calls = []
    node._flush_peer = lambda p, timeout, **_kwargs: calls.append(("flush", p.hexhash))
    node._collect_mail = lambda p, timeout, **_kwargs: calls.append(("collect", p.hexhash))

    def exchange(p, **kwargs):
        # The exchange runs after the mail lock is released, so a slow
        # peer cannot hold up mail delivery.
        assert not node._mail_lock.locked()
        calls.append(("exchange", p.hexhash))

    node.exchange_peers = exchange

    node._mail_round_for_peer(peer)

    assert calls == [
        ("flush", peer.hexhash),
        ("collect", peer.hexhash),
        ("exchange", peer.hexhash),
    ]
    assert node._exchanged_at[peer.destination_hash] > 0


def test_mail_round_survives_failed_exchange(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("bob")
    node._flush_peer = lambda p, timeout, **_kwargs: {"delivered": 0, "deposited": 0}
    node._collect_mail = lambda p, timeout, **_kwargs: 0

    def failing_exchange(p, **kwargs):
        raise RuntimeError("link broke")

    node.exchange_peers = failing_exchange
    node._mail_round_for_peer(peer)  # must not raise


def test_exchange_sweep_asks_least_recent_first(tmp_path):
    node = make_node(tmp_path)
    first = make_peer("first")
    second = make_peer("second")
    third = make_peer("third")
    for peer, when in ((first, 100.0), (second, 200.0), (third, 300.0)):
        node.registry.upsert(
            peer.destination_hash, peer.public_key, name=peer.name, now=when
        )
    # Second was asked recently; the sweep should skip it and ask the
    # other two.  Never-asked peers keep registry (most-recently-seen) order.
    node._exchanged_at[second.destination_hash] = time.time()
    asked = []
    node.exchange_peers = lambda p, **kwargs: asked.append(p.hexhash)

    node._exchange_sweep(max_peers=2)

    assert asked == [third.hexhash, first.hexhash]


def test_exchange_sweep_skips_recently_asked(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("only")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="only", now=100.0
    )
    node._exchanged_at[peer.destination_hash] = time.time()
    asked = []
    node.exchange_peers = lambda p, **kwargs: asked.append(p.hexhash)

    node._exchange_sweep(max_peers=2)

    assert asked == []


def test_flush_outbox_prefers_direct_peers(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    direct = make_peer("direct")
    node.registry.upsert(
        direct.destination_hash, direct.public_key, name="direct", now=100.0
    )
    # A peer learned only via exchange, with a much fresher reported
    # last_seen, must still come after directly heard peers in the sweep.
    entry, destination = make_exchange_entry("gossip", last_seen=time.time())
    node.registry.merge_exchanged([entry], now=time.time())
    assert node.registry.get(destination).via_exchange is True

    order = []
    node._flush_peer = lambda p, timeout, **_kwargs: (
        order.append(p.hexhash),
        {"delivered": 0, "deposited": 0},
    )[1]
    node._collect_mail = lambda p, timeout, **_kwargs: 0

    node.flush_outbox()

    assert order == [direct.hexhash, destination.hex()]


def test_flush_outbox_to_filter_contacts_only_that_peer(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    alice = make_peer("alice")
    bob = make_peer("bob")
    node.registry.upsert(
        alice.destination_hash, alice.public_key, name="alice", now=100.0
    )
    node.registry.upsert(bob.destination_hash, bob.public_key, name="bob", now=100.0)
    node.mailbox.enqueue(
        to=alice.destination_hash, text="for alice", message_id="aa" + "11" * 15
    )

    calls = []

    def _flush_peer(peer, timeout, **kwargs):
        calls.append((peer.hexhash, kwargs.get("respect_backoff")))
        return {"delivered": 0, "deposited": 0}

    node._flush_peer = _flush_peer
    node._collect_mail = lambda p, timeout, **_kwargs: 0

    # A retry aimed at bob must not spend its budget on alice.
    node.flush_outbox(to=bob.destination_hash, respect_backoff=False)
    assert calls == [(bob.hexhash, False)]

    node.flush_outbox(to=alice.destination_hash, respect_backoff=False)
    assert calls == [(bob.hexhash, False), (alice.hexhash, False)]

    # An unknown recipient contacts nobody instead of sweeping the registry.
    node.flush_outbox(to=b"\x00" * 16, respect_backoff=False)
    assert calls == [(bob.hexhash, False), (alice.hexhash, False)]


def test_flush_outbox_skips_when_lock_busy_unless_waiting(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=100.0
    )
    node.mailbox.enqueue(
        to=peer.destination_hash, text="for alice", message_id="aa" + "11" * 15
    )

    calls = []
    node._flush_peer = lambda p, timeout, **_kwargs: (
        calls.append(p.hexhash),
        {"delivered": 0, "deposited": 0},
    )[1]
    node._collect_mail = lambda p, timeout, **_kwargs: 0

    # A background round holding the lock makes a plain flush skip,
    # and the summary says so instead of looking like a quiet sweep.
    node._mail_lock.acquire()
    try:
        assert node.flush_outbox() == {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
            "receipts": 0,
            "lock_busy": True,
        }
        assert calls == []
        # A bounded wait that expires reports the same, rather than
        # silently pretending the retry was attempted.
        assert node.flush_outbox(wait_for_lock=0.2) == {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
            "receipts": 0,
            "lock_busy": True,
        }
        assert calls == []
    finally:
        node._mail_lock.release()

    # With wait_for_lock, the same flush waits for the round to finish
    # and then runs instead of reporting nothing moved.
    acquired = threading.Event()

    def _hold_then_release():
        node._mail_lock.acquire()
        acquired.set()
        time.sleep(0.2)
        node._mail_lock.release()

    holder = threading.Thread(target=_hold_then_release)
    holder.start()
    assert acquired.wait(timeout=5.0)
    assert node.flush_outbox(wait_for_lock=5.0) == {
        "delivered": 0,
        "deposited": 0,
        "received": 0,
        "receipts": 0,
        "lock_busy": False,
    }
    holder.join(timeout=5.0)
    assert calls == [peer.hexhash]


def test_wait_for_mail_rounds_joins_in_flight_rounds(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    started = threading.Event()
    finished: list = []

    def _slow_round(other: Peer) -> None:
        started.set()
        time.sleep(0.2)
        finished.append(other.hexhash)

    node._mail_round_for_peer = _slow_round
    node._spawn_mail_round(peer)
    assert started.wait(timeout=5.0)
    assert node.wait_for_mail_rounds(timeout=5.0) is True
    assert finished == [peer.hexhash]

    # A round that outlasts the budget reports False instead of hanging.
    node._mail_round_for_peer = lambda other: time.sleep(2.0)
    node._spawn_mail_round(peer)
    assert node.wait_for_mail_rounds(timeout=0.1) is False
    assert node.wait_for_mail_rounds(timeout=5.0) is True
def test_deliver_entry_skips_message_cancelled_by_another_process(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16, text="do not send", message_id="cc" + "33" * 15
    )
    # A second process (``tau outbox cancel``) removes the entry from the
    # shared mailbox file after this node's sweep listed it.
    other = Mailbox(node.config.mailbox_path)
    assert other.remove_outbox(entry.id) is True

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deliver_entry(make_peer(), entry, timeout=1.0) is False
    assert calls == []


def test_deliver_entry_sends_message_still_queued(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16, text="send me", message_id="cc" + "44" * 15
    )

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deliver_entry(make_peer(), entry, timeout=1.0) is True
    assert len(calls) == 1
    assert node.mailbox.get_outbox(entry.id) is None


def test_send_message_distrusts_peer_queued_claim(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {
            "ok": True,
            "queued": True,
            "id": "spoofed-id",
            "to": "00" * 16,
            "expires_at": 9999999999.0,
            "echo": "hi",
            "received_at": 1234.0,
            "server": "alice",
            "node": peer.hexhash,
            "duplicate": False,
            "_rtt": 0.012,
        },
    )

    response = node.send_message("alice", "hi")

    # The peer claimed the message was queued with an id it invented;
    # the node reports a plain delivery, names the local wire
    # message, and drops the peer's queue keys.
    assert response["queued"] is False
    # The spoofed id is discarded: the response names the local wire
    # message instead, matching the sent message in the inbox.
    assert response["id"] != "spoofed-id"
    sent = node.inbox.list_messages(direction="out")
    assert len(sent) == 1
    assert sent[0]["id"] == response["id"]
    assert "to" not in response
    assert "expires_at" not in response
    assert response["echo"] == "hi"
    assert response["received_at"] == 1234.0
    assert response["server"] == "alice"
    assert response["duplicate"] is False
    assert response["_rtt"] == 0.012


def test_send_message_with_send_after_queues_without_delivery(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    send_after = time.time() + 3600.0
    response = node.send_message("alice", "later", send_after=send_after)

    # No delivery attempt at all — the peer is reachable, but the
    # message is scheduled, not sent.
    assert calls == []
    assert response["queued"] is True
    assert response["send_after"] == send_after
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.send_after == send_after
    assert entry.is_due() is False
    # The sent message is still recorded in local history.
    sent = node.inbox.list_messages(direction="out")
    assert [message["id"] for message in sent] == [response["id"]]


def test_send_message_rejects_nonfinite_send_after(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.send_message("alice", "hi", send_after=float("nan"))
    with pytest.raises(NodeError):
        node.send_message("alice", "hi", send_after="tomorrow")


def test_send_message_scheduled_to_undiscovered_hash_keeps_schedule(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    node.reticulum = object()  # started: the fallback may queue
    peer = make_peer("bob")
    # The peer has never announced: resolution fails, so the send
    # queues through the unresolved-peer fallback.
    def _no_peer(*args, **kwargs):
        raise NodeError("no announce")

    monkeypatch.setattr(node, "resolve_peer", _no_peer)
    send_after = time.time() + 3600.0
    response = node.send_message(peer.hexhash, "later", send_after=send_after)

    assert response["queued"] is True
    assert response["send_after"] == send_after
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.send_after == send_after
    assert entry.is_due() is False

    # The peer announces afterwards: even a backoff-ignoring flush
    # must not deliver the message before its scheduled time.
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="bob", now=100.0
    )
    delivered = []
    monkeypatch.setattr(
        node,
        "_deliver_entry",
        lambda p, e, timeout, **kwargs: (
            delivered.append(e.id),
            True,
        )[1],
    )
    monkeypatch.setattr(
        node,
        "_deposit_entry",
        lambda p, e, timeout, **kwargs: False,
    )
    node._flush_peer(peer, 1.0, respect_backoff=False)
    assert delivered == []
    assert node.mailbox.get_outbox(entry.id) is not None


def test_send_message_rejects_scheduled_send_without_queue(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    # queue=False means "do not queue": a scheduled message is queued
    # by definition, so the combination is refused before anything is
    # recorded or enqueued.
    with pytest.raises(NodeError, match="without queueing"):
        node.send_message(
            "alice", "later", queue=False, send_after=time.time() + 3600.0
        )
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.sent_list() == []
    assert node.inbox.list_messages(direction="out") == []


def test_send_message_scheduled_to_supplied_peer_registers_it(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    # The peer was never registered: resolve_peer returns a supplied
    # Peer as-is, so without registration flush_outbox (which only
    # visits registry peers) would never deliver the scheduled message.
    assert node.registry.get(peer.destination_hash) is None
    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    send_after = time.time() + 3600.0
    response = node.send_message(peer, "later", send_after=send_after)

    assert calls == []
    assert response["queued"] is True
    assert node.registry.get(peer.destination_hash) is not None
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.send_after == send_after


def test_send_message_failed_schedule_leaves_no_sent_record(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    # A schedule past the message's own expiry is rejected at enqueue.
    send_after = time.time() + 3600.0
    expires_at = time.time() + 60.0
    with pytest.raises(MailboxError):
        node.send_message(
            "alice", "never", send_after=send_after, expires_at=expires_at
        )
    # Nothing was queued, and no sent record lingers behind.
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.sent_list() == []
    assert node.inbox.list_messages(direction="out") == []


def test_flush_outbox_never_delivers_scheduled_entry_early(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    alice = make_peer("alice")
    bob = make_peer("bob")
    node.registry.upsert(
        alice.destination_hash, alice.public_key, name="alice", now=100.0
    )
    node.registry.upsert(
        bob.destination_hash, bob.public_key, name="bob", now=100.0
    )
    now = time.time()
    scheduled = node.mailbox.enqueue(
        to=alice.destination_hash,
        text="not yet",
        message_id="aa" + "55" * 15,
        send_after=now + 3600.0,
    )
    plain = node.mailbox.enqueue(
        to=alice.destination_hash,
        text="now",
        message_id="aa" + "66" * 15,
    )
    # A scheduled message for bob: another peer's visit must not
    # deposit a sealed copy of it early either.
    for_other = node.mailbox.enqueue(
        to=bob.destination_hash,
        text="not yet either",
        message_id="aa" + "77" * 15,
        send_after=now + 3600.0,
    )

    delivered = []
    deposited = []
    monkeypatch.setattr(
        node,
        "_deliver_entry",
        lambda peer, entry, timeout, **kwargs: (
            delivered.append(entry.id),
            True,
        )[1],
    )
    monkeypatch.setattr(
        node,
        "_deposit_entry",
        lambda peer, entry, timeout, **kwargs: (
            deposited.append(entry.id),
            True,
        )[1],
    )
    node._collect_mail = lambda p, timeout, **_kwargs: 0

    # A backoff-ignoring round (fresh announce, `tau outbox retry`)
    # still leaves scheduled entries queued.
    node.flush_outbox(respect_backoff=False)
    assert delivered == [plain.id]
    assert for_other.id not in deposited
    assert scheduled.id not in deposited
    assert node.mailbox.get_outbox(scheduled.id) is not None
    assert node.mailbox.get_outbox(for_other.id) is not None

    # Once the scheduled time passes, the same round delivers it.
    node.mailbox.get_outbox(scheduled.id).send_after = time.time() - 1.0
    node.mailbox.save()
    delivered.clear()
    node.flush_outbox(respect_backoff=False)
    assert scheduled.id in delivered


def test_send_message_distrusts_peer_queued_claim_without_id(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "queued": True, "echo": "hi"},
    )

    # A queued claim with no id used to make the CLI raise KeyError
    # on response["id"] after the flush; the normalized response
    # must not raise anywhere downstream.
    response = node.send_message("alice", "hi")

    assert response["queued"] is False
    # The response still names the local wire message, so downstream
    # readers of response["id"] never see a peer-invented id.
    assert response["id"]
    assert response["echo"] == "hi"


def test_send_message_keeps_well_behaved_delivery_response(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {
            "ok": True,
            "echo": "hi",
            "received_at": 1234.0,
            "server": "alice",
            "node": peer.hexhash,
            "duplicate": False,
            "_rtt": 0.012,
        },
    )

    response = node.send_message("alice", "hi")

    assert response["queued"] is False
    # A well-behaved delivery still names the local wire message.
    assert response["id"]
    assert response["echo"] == "hi"
    assert response["received_at"] == 1234.0
    assert response["server"] == "alice"
    assert response["node"] == peer.hexhash
    assert response["duplicate"] is False
    assert response["_rtt"] == 0.012


def test_deliver_entry_carries_reply_reference(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    reply_to = {"id": "a" * 32, "excerpt": "0123456789abcdef"}
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16,
        text="seconded",
        message_id="cc" + "66" * 15,
        in_reply_to=reply_to,
    )

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deliver_entry(make_peer(), entry, timeout=1.0) is True
    assert len(calls) == 1
    from tau.protocol import parse_message

    sent = parse_message(calls[0][2])
    assert sent["in_reply_to"] == reply_to
    assert sent["text"] == "seconded"


def test_deliver_collected_holds_mailbox_transaction(tmp_path):
    # A collected (relayed) delivery marks the message seen and then
    # records it.  Both steps run under the mailbox transaction, so a
    # live backup holding the same transaction sees either both writes
    # or neither — never a seen marker with no inbox entry.
    node = make_node(tmp_path)
    original_append = node.inbox.append
    started = threading.Event()

    def slow_append(record):
        started.set()
        time.sleep(0.3)
        return original_append(record)

    node.inbox.append = slow_append  # type: ignore[method-assign]
    message = {
        "id": "relayed-1",
        "from": "ab" * 16,
        "from_name": "alice",
        "text": "hello via relay",
        "sent_at": 1000.0,
    }
    result = []
    worker = threading.Thread(
        target=lambda: result.append(node._deliver_collected(message)),
        daemon=True,
    )
    worker.start()
    assert started.wait(timeout=5)
    # The delivery is inside the slowed record step: the mailbox
    # transaction must still be held, so acquiring it times out.
    with pytest.raises(TimeoutError):
        with node.mailbox.transaction(timeout=0.1):
            pass
    worker.join(timeout=5)
    assert result == [True]
    # The transaction is free again once the delivery finishes.
    with node.mailbox.transaction(timeout=1.0):
        pass
    stored = json.loads((tmp_path / "inbox.json").read_text(encoding="utf-8"))
    assert any(entry["id"] == "relayed-1" for entry in stored["messages"])


def test_send_message_records_sent_message_for_quote_lookup(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "resolve_peer", lambda *args, **kwargs: peer)
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "echo": "hello"},
    )
    reply_to = {"id": "a" * 32, "excerpt": "0123456789abcdef"}

    response = node.send_message(peer, "hello", in_reply_to=reply_to)

    assert response["ok"] is True
    assert response["id"]
    # The sent message is recorded so the peer's reply to it renders
    # from local history; it stays out of the received listing.
    assert node.inbox.list_messages() == []
    sent = node.inbox.list_messages(direction="out")
    assert len(sent) == 1
    assert sent[0]["text"] == "hello"
    assert sent[0]["destination"] == peer.hexhash
    assert sent[0]["in_reply_to"] == reply_to
    assert node.inbox.find(response["id"], peer=peer.hexhash) is not None


def test_send_message_ignores_peer_supplied_id(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "resolve_peer", lambda *args, **kwargs: peer)
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "id": "ff" * 16},
    )

    response = node.send_message(peer, "hello")

    assert response["id"] != "ff" * 16
    sent = node.inbox.list_messages(direction="out")
    assert sent[0]["id"] == response["id"]


def test_send_message_queues_reply_when_peer_is_offline(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "resolve_peer", lambda *args, **kwargs: peer)
    monkeypatch.setattr(node, "reticulum", object())

    def _boom(*args, **kwargs):
        from tau.node import NodeError

        raise NodeError("no route")

    monkeypatch.setattr(node, "_request", _boom)
    monkeypatch.setattr(node, "flush_outbox", lambda *args, **kwargs: None)
    reply_to = {"id": "a" * 32, "excerpt": "0123456789abcdef"}

    response = node.send_message(peer, "seconded", in_reply_to=reply_to)

    assert response["queued"] is True
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.in_reply_to == reply_to
    assert node.inbox.list_messages(direction="out")[0]["id"] == response["id"]


def test_deposit_entry_skips_message_cancelled_by_another_process(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    recipient = make_peer("bob")
    node.registry.upsert(
        recipient.destination_hash, recipient.public_key, name="bob", now=1.0
    )
    entry = node.mailbox.enqueue(
        to=recipient.destination_hash,
        text="do not hold",
        message_id="cc" + "55" * 15,
    )
    other = Mailbox(node.config.mailbox_path)
    assert other.remove_outbox(entry.id) is True

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deposit_entry(make_peer("carol"), entry, timeout=1.0) is False
    assert calls == []


def test_save_peers_preserves_cli_set_alias(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("aliased")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="aliased", now=time.time()
    )
    node.registry.save()

    # A one-shot `tau peers alias` while the node serves: a separate
    # registry instance edits the same file.
    cli_registry = PeerRegistry(node.registry.path)
    cli_registry.set_alias(peer.hexhash, "field-kit")

    node._save_peers(force=True)

    assert node.registry.get(peer.destination_hash).alias == "field-kit"
    assert (
        PeerRegistry(node.registry.path).get(peer.destination_hash).alias
        == "field-kit"
    )


def _seed_wait_entry(node, message_id="dd" + "66" * 15):
    return node.mailbox.enqueue(
        to=b"\xbb" * 16, text="wait for me", message_id=message_id
    )


def test_wait_for_delivery_returns_at_once_when_already_delivered(
    tmp_path, monkeypatch
):
    # A delivery that landed before the wait started is still
    # recognised through the confirmation record — no flush needed.
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)
    node.mailbox.remove_outbox(entry.id)
    node._delivered_ids.append(entry.id)
    calls = []
    monkeypatch.setattr(node, "flush_outbox", lambda **kwargs: calls.append(kwargs))
    assert node.wait_for_delivery(entry.id, timeout=5.0) is True
    assert calls == []


def test_wait_for_delivery_returns_when_flush_delivers(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)

    calls = []

    def fake_flush(**kwargs):
        calls.append(kwargs)
        node.mailbox.remove_outbox(entry.id)
        # A real delivery records the id, as _deliver_entry does.
        node._delivered_ids.append(entry.id)
        return {"delivered": 1, "deposited": 0, "received": 0, "lock_busy": False}

    monkeypatch.setattr(node, "flush_outbox", fake_flush)
    assert node.wait_for_delivery(entry.id, timeout=5.0, poll_interval=0.01) is True
    assert len(calls) == 1
    # The wait drives a targeted flush that ignores the entry's backoff:
    # the caller explicitly asked to wait for this message.
    assert calls[0]["message_id"] == entry.id
    assert calls[0]["respect_backoff"] is False
    # The flush shares the wait's deadline, so no peer or request
    # starts once it has passed.
    assert calls[0]["deadline"] is not None


def test_open_link_checks_deadline_after_path_discovery(tmp_path, monkeypatch):
    # Path discovery must not hand the full timeout to link
    # establishment as well: once the deadline has passed, opening the
    # link fails at once instead of waiting again.
    node = make_node(tmp_path)
    peer = make_peer("bob")

    def slow_path(dest_hash, timeout):
        time.sleep(0.25)

    monkeypatch.setattr(node, "_wait_for_path", slow_path)
    deadline = time.monotonic() + 0.1
    with pytest.raises(NodeError):
        node._open_link(peer, 5.0, deadline=deadline)


def test_request_on_link_refuses_after_deadline(tmp_path):
    # A request must not be sent when the link stages already consumed
    # the deadline.
    node = make_node(tmp_path)

    class _DeadLink:
        def request(self, *args, **kwargs):
            raise AssertionError("must not send after the deadline")

    deadline = time.monotonic() - 1.0
    with pytest.raises(NodeError):
        node._request_on_link(_DeadLink(), "/tau/ping", b"{}", 5.0, deadline=deadline)


def test_wait_for_delivery_keeps_retries_at_poll_interval(tmp_path, monkeypatch):
    # Retries stay at poll_interval: growing the pause would let the
    # last attempt land long before the deadline, so a peer that
    # becomes reachable late in the wait would be missed.
    #
    # The timing is driven by a fake clock, not the wall clock: the
    # poll loop used to be measured live, so on a loaded or
    # CPU-throttled machine a late wake-up could push a gap over the
    # bound (or drop the retry count below it) and fail the test
    # intermittently.  ``wait_for_delivery`` reads ``time.monotonic``
    # and ``time.sleep`` through the ``time`` module, so patching both
    # here makes the wait deterministic: every sleep advances the
    # clock by exactly ``poll_interval``.
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)
    stamps = []
    clock = [0.0]

    def fake_monotonic():
        return clock[0]

    def fake_sleep(seconds):
        clock[0] += max(0.0, seconds)

    monkeypatch.setattr(time, "monotonic", fake_monotonic)
    monkeypatch.setattr(time, "sleep", fake_sleep)

    def fake_flush(**kwargs):
        stamps.append(time.monotonic())
        return {"delivered": 0, "deposited": 0, "received": 0, "lock_busy": False}

    monkeypatch.setattr(node, "flush_outbox", fake_flush)
    assert (
        node.wait_for_delivery(entry.id, timeout=0.5, poll_interval=0.05) is False
    )
    assert len(stamps) >= 8
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    assert gaps, "the wait must retry the flush more than once"
    assert max(gaps) < 0.15


def test_wait_for_delivery_rejects_unconfirmed_removal(tmp_path, monkeypatch):
    # The entry leaves the outbox without a delivery confirmation —
    # cancelled by another process, or delivered by one — and the wait
    # must not report it as delivered.
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)

    def fake_flush(**kwargs):
        # Removal without a delivery record, as a cancel would do.
        node.mailbox.remove_outbox(entry.id)
        return {"delivered": 0, "deposited": 0, "received": 0, "lock_busy": False}

    monkeypatch.setattr(node, "flush_outbox", fake_flush)
    assert node.wait_for_delivery(entry.id, timeout=5.0, poll_interval=0.01) is False
    assert node.mailbox.get_outbox(entry.id) is None


def test_wait_for_delivery_times_out_when_entry_stays_queued(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)
    calls = []
    monkeypatch.setattr(
        node, "flush_outbox", lambda **kwargs: calls.append(kwargs) or {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
            "lock_busy": False,
        }
    )
    started = time.monotonic()
    assert (
        node.wait_for_delivery(entry.id, timeout=0.2, poll_interval=0.01) is False
    )
    assert time.monotonic() - started >= 0.2
    assert calls, "the wait must keep retrying the flush until the deadline"
    assert node.mailbox.get_outbox(entry.id) is not None


def test_wait_for_delivery_returns_when_mailbox_lock_stalls(tmp_path, monkeypatch):
    # A stalled mailbox-lock holder must not keep the wait from
    # reporting expiry: the queue check is bounded by the time left,
    # so the wait returns False at the deadline instead of blocking.
    fcntl = pytest.importorskip("fcntl")
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)
    monkeypatch.setattr(
        node, "flush_outbox", lambda **kwargs: {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
            "lock_busy": False,
        }
    )
    lock_path = tmp_path / "mailbox.json.lock"
    handle = open(lock_path, "a")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        started = time.monotonic()
        assert (
            node.wait_for_delivery(entry.id, timeout=0.3, poll_interval=0.05)
            is False
        )
        elapsed = time.monotonic() - started
        assert elapsed >= 0.3
        assert elapsed < 2.0, "the wait must not block past its deadline"
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
    assert node.mailbox.get_outbox(entry.id) is not None


def test_flush_outbox_with_deadline_returns_when_mailbox_lock_stalls(
    tmp_path,
):
    # A stalled mailbox-lock holder must not hold a targeted flush
    # past its deadline: prune and the outbox reads share the
    # remaining budget, so the round ends promptly with the entry
    # still queued.
    fcntl = pytest.importorskip("fcntl")
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)
    node.reticulum = object()  # started enough for flush_outbox
    lock_path = tmp_path / "mailbox.json.lock"
    handle = open(lock_path, "a")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        started = time.monotonic()
        summary = node.flush_outbox(timeout=1.0, deadline=time.monotonic() + 0.3)
        elapsed = time.monotonic() - started
        assert elapsed < 2.0, "the flush must not block past its deadline"
        assert summary["delivered"] == 0
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
    assert node.mailbox.get_outbox(entry.id) is not None


def _heard_node(tmp_path, monkeypatch, *, hops=2):
    """A node far enough along to take announces, without a transport."""
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(hash=b"\x00" * 16)
    monkeypatch.setattr(RNS.Transport, "hops_to", lambda digest: hops)
    return node


def _foreign_identity():
    identity = RNS.Identity()
    announced_hash = RNS.Destination.hash(identity, "lxmf", "delivery")
    return identity, announced_hash


def test_announce_handler_listens_to_all_aspects(tmp_path):
    # Reticulum treats a None aspect filter as "no filter": the
    # handler receives every announce and the node filters locally.
    node = make_node(tmp_path)
    assert _AnnounceHandler(node).aspect_filter is None


def test_on_announce_records_foreign_announce(tmp_path, monkeypatch):
    seen = []
    node = _heard_node(tmp_path, monkeypatch, hops=2)
    node.on_foreign = seen.append
    saved = []
    monkeypatch.setattr(node, "_save_peers", lambda: saved.append(True))
    identity, announced_hash = _foreign_identity()

    node._on_announce(announced_hash, identity, None)

    assert node.registry.all() == []
    assert saved == []
    assert node._mail_round_threads == set()
    heard = node.foreign_announces()
    assert len(heard) == 1
    assert heard[0]["destination"] == announced_hash.hex()
    assert heard[0]["hops"] == 2
    assert heard[0]["count"] == 1
    assert heard[0]["first_seen"] <= heard[0]["last_seen"]
    assert len(seen) == 1
    assert seen[0]["destination"] == announced_hash.hex()

    # Hearing it again bumps the count, not the entry — and does not
    # call on_foreign a second time.
    node._on_announce(announced_hash, identity, None)
    heard = node.foreign_announces()
    assert len(heard) == 1
    assert heard[0]["count"] == 2
    assert len(seen) == 1


def test_on_announce_ignores_own_destination(tmp_path, monkeypatch):
    node = _heard_node(tmp_path, monkeypatch)
    identity, _ = _foreign_identity()
    node._on_announce(node.destination.hash, identity, None)
    assert node.foreign_announces() == []
    assert node.registry.all() == []


def test_on_announce_still_registers_tau_peer(tmp_path, monkeypatch):
    node = _heard_node(tmp_path, monkeypatch, hops=1)
    rounds = []
    saved = []
    monkeypatch.setattr(node, "_spawn_mail_round", lambda peer: rounds.append(peer))
    monkeypatch.setattr(node, "_save_peers", lambda: saved.append(True))
    identity = RNS.Identity()
    announced_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT)

    node._on_announce(announced_hash, identity, build_announce("alice"))

    assert node.foreign_announces() == []
    peers = node.registry.all()
    assert len(peers) == 1
    assert peers[0].name == "alice"
    assert peers[0].hexhash == announced_hash.hex()
    assert len(rounds) == 1
    assert len(saved) == 1


def test_foreign_announces_capped(tmp_path, monkeypatch):
    from tau.node import MAX_FOREIGN_ANNOUNCES

    node = _heard_node(tmp_path, monkeypatch)
    first = bytes([0, 0]) + b"\x00" * 14
    for index in range(MAX_FOREIGN_ANNOUNCES + 5):
        digest = bytes([index // 256, index % 256]) + b"\x00" * 14
        node._record_foreign_announce(digest)
    heard = node.foreign_announces()
    assert len(heard) == MAX_FOREIGN_ANNOUNCES
    destinations = {entry["destination"] for entry in heard}
    assert first.hex() not in destinations
    assert (bytes([0, MAX_FOREIGN_ANNOUNCES + 4]) + b"\x00" * 14).hex() in destinations


def test_foreign_announces_newest_first(tmp_path, monkeypatch):
    node = _heard_node(tmp_path, monkeypatch)
    older = bytes([0, 1]) + b"\x00" * 14
    newer = bytes([0, 2]) + b"\x00" * 14
    node._record_foreign_announce(older)
    node._record_foreign_announce(newer)
    node._foreign_announces[older.hex()]["last_seen"] = 1000.0
    node._foreign_announces[newer.hex()]["last_seen"] = 2000.0
    heard = node.foreign_announces()
    assert [entry["destination"] for entry in heard] == [
        newer.hex(),
        older.hex(),
    ]

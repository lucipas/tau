from __future__ import annotations

import json
import logging
import threading
import time

import RNS
import pytest

from tau.config import TauConfig
from tau.inbox import Inbox
from tau.mailbox import Mailbox, MailboxError
from tau.node import NodeError, NotAllowedError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import APP_NAME, ASPECT, parse_message


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
    node = make_node(tmp_path)
    entry = _seed_wait_entry(node)
    stamps = []

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

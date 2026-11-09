"""Group DMs: wire format, roster store, handler and fan-out.

Covers issue #80: private group DMs with a fixed member list, carried
over the existing encrypted links with offline legs queued in the
store-and-forward outbox.
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace

import RNS
import pytest

from tau import cli
from tau.config import TauConfig
from tau.groups import GroupStore
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.mailbox import Mailbox, open_message, seal_message
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    ProtocolError,
    decode,
    derive_group_id,
    encode_group_message,
    generate_group_id,
    parse_group_message,
)

OWN_HASH = "ab" * 16
OTHER_HASH = "cd" * 16
THIRD_HASH = "ef" * 16
STRANGER_HASH = "12" * 16


def make_node(tmp_path, name="alice", on_message=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
    )
    node.destination = SimpleNamespace(hexhash=OWN_HASH, hash=bytes.fromhex(OWN_HASH))
    return node


def make_sender():
    identity = RNS.Identity()
    return identity, destination_hash(identity).hex()


def make_group_payload(
    text="hello group",
    *,
    sender_members=None,
    group_id=None,
    group_name="weekend",
    creator=None,
    message_id="group-msg-1",
):
    members = sender_members or [OWN_HASH, OTHER_HASH, THIRD_HASH]
    creator = creator or members[0]
    return encode_group_message(
        text,
        message_id=message_id,
        group_id=group_id or derive_group_id(creator, group_name, members),
        group_name=group_name,
        creator=creator,
        members=members,
    )


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


# -- wire format ------------------------------------------------------------


def test_group_message_roundtrip():
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    group_id = derive_group_id(OWN_HASH, "weekend", members)
    payload = encode_group_message(
        "hi all",
        message_id="gm-1",
        group_id=group_id,
        group_name="weekend",
        creator=OWN_HASH,
        members=members,
        sent_at=123.0,
    )
    parsed = parse_group_message(payload)
    assert parsed["text"] == "hi all"
    assert parsed["id"] == "gm-1"
    assert parsed["group"] == group_id
    assert parsed["group_name"] == "weekend"
    assert parsed["creator"] == OWN_HASH
    assert parsed["members"] == members
    assert parsed["sent_at"] == 123.0


def test_group_id_derives_from_creator_name_and_roster():
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    first = derive_group_id(OWN_HASH, "weekend", members)
    # Order-independent and deterministic.
    assert derive_group_id(OWN_HASH, "weekend", list(reversed(members))) == first
    assert derive_group_id(OWN_HASH, "weekend", members) == first
    # Any change to the roster, name or creator is a new group.
    assert derive_group_id(OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH]) != first
    assert derive_group_id(OWN_HASH, "other", members) != first
    assert derive_group_id(OTHER_HASH, "weekend", members) != first


def test_group_message_rejects_bad_rosters():
    group_id = derive_group_id(OWN_HASH, "g", [OWN_HASH, OTHER_HASH])
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x", group_id=group_id, group_name="g", creator=OWN_HASH, members=[OWN_HASH]
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            creator=OWN_HASH,
            members=[OWN_HASH, OWN_HASH],
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            creator=OWN_HASH,
            members=["not-a-hash", OTHER_HASH],
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            creator=OWN_HASH,
            members=[f"{i:032x}" for i in range(33)],
        )


def test_parse_group_message_rejects_forked_roster():
    # A member reuses a known group id with a different roster: the id
    # no longer derives from the roster, so the message is rejected at
    # parse time instead of pinning a fork.
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    real_id = derive_group_id(OWN_HASH, "weekend", members)
    forked = [OWN_HASH, OTHER_HASH, STRANGER_HASH]
    raw = json.dumps(
        {
            "type": "group_message",
            "v": 1,
            "text": "forked roster",
            "group": real_id,
            "group_name": "weekend",
            "creator": OWN_HASH,
            "members": forked,
            "sent_at": 1.0,
            "id": "gm-fork",
        }
    ).encode("utf-8")
    with pytest.raises(ProtocolError, match="does not match"):
        parse_group_message(raw)


def test_parse_group_message_rejects_wrong_type():
    raw = json.dumps(
        {
            "type": "message",
            "v": 1,
            "text": "not a group message",
            "group": derive_group_id(OWN_HASH, "g", [OWN_HASH, OTHER_HASH]),
            "group_name": "g",
            "creator": OWN_HASH,
            "members": [OWN_HASH, OTHER_HASH],
        }
    ).encode("utf-8")
    with pytest.raises(ProtocolError):
        parse_group_message(raw)


# -- roster store -----------------------------------------------------------


def test_group_store_create_find_and_list(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create(
        "weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
    )
    assert group["id"] == derive_group_id(OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH])
    assert group["name"] == "weekend"
    assert group["members"] == [OWN_HASH, OTHER_HASH]
    assert group["creator"] == OWN_HASH
    assert group["local"] is True
    assert store.get(group["id"]) == group
    assert store.find(group["id"]) == group
    assert store.find(group["id"][:8]) == group
    assert store.find("weekend") == group
    assert [item["id"] for item in store.list_groups()] == [group["id"]]
    assert len(store) == 1
    # Creating the same group again is idempotent: same derived id.
    again = store.create("weekend", [OTHER_HASH, OWN_HASH], created_by=OWN_HASH)
    assert again["id"] == group["id"]
    assert len(store) == 1


def test_group_store_find_errors(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    first = store.create("same", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    second = store.create("same", [OWN_HASH, THIRD_HASH], created_by=OWN_HASH)
    with pytest.raises(ValueError, match="unknown group"):
        store.find("nope")
    with pytest.raises(ValueError, match="2 groups are named"):
        store.find("same")
    assert store.find(first["id"][:4]) == first or store.find(second["id"][:4]) == second


def test_group_store_record_first_roster_wins(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group_id = derive_group_id(OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH])
    first = store.record(
        group_id, "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH
    )
    assert first is not None
    assert first["id"] == group_id
    assert first["local"] is False
    # Same roster again: the pinned record comes back unchanged.
    assert (
        store.record(group_id, "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH)
        == first
    )
    # A different roster cannot reuse the id: derivation fails, so the
    # record is refused before the roster comparison is even reached.
    assert (
        store.record(group_id, "weekend", [OWN_HASH, THIRD_HASH], creator=OWN_HASH)
        is None
    )
    assert store.get(group_id) == first


def test_group_store_learned_cap_refuses_without_evicting(tmp_path):
    from tau.groups import MAX_LEARNED_GROUPS

    store = GroupStore(tmp_path / "groups.json")
    created = store.create("mine", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    for i in range(MAX_LEARNED_GROUPS):
        members = [OTHER_HASH, f"{i + 1:032x}"]
        group_id = derive_group_id(OTHER_HASH, f"learned-{i}", members)
        assert (
            store.record(group_id, f"learned-{i}", members, creator=OTHER_HASH)
            is not None
        )
    # The learned cap is full: one more learned group is refused, and
    # the locally created group is still there.
    extra_members = [OTHER_HASH, "ff" * 16]
    extra_id = derive_group_id(OTHER_HASH, "extra", extra_members)
    assert (
        store.record(extra_id, "extra", extra_members, creator=OTHER_HASH) is None
    )
    assert store.get(created["id"]) is not None
    assert len(store) == MAX_LEARNED_GROUPS + 1
    # Deleting a learned group frees a slot for the next one.
    assert store.delete(
        derive_group_id(OTHER_HASH, "learned-0", [OTHER_HASH, f"{1:032x}"])
    ) is True
    assert (
        store.record(extra_id, "extra", extra_members, creator=OTHER_HASH)
        is not None
    )


def test_group_store_delete(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    assert store.delete(group["id"]) is True
    assert store.get(group["id"]) is None
    assert store.list_groups() == []
    assert store.delete(group["id"]) is False
    assert store.delete("00" * 16) is False


def test_group_store_save_failure_rolls_back(tmp_path, monkeypatch):
    import tau.groups as groups_module

    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(groups_module, "atomic_write_json", _boom)
    # A failed pin is not acknowledged: the record is refused and the
    # store keeps only what is on disk.
    members = [OWN_HASH, THIRD_HASH]
    group_id = derive_group_id(OWN_HASH, "other", members)
    assert store.record(group_id, "other", members, creator=OWN_HASH) is None
    assert store.get(group_id) is None
    # A failed create raises instead of reporting a group that is gone
    # after a restart, and the earlier group survives.
    with pytest.raises(OSError):
        store.create("third", [OWN_HASH, "ff" * 16], created_by=OWN_HASH)
    assert store.get(group["id"]) is not None
    reloaded = GroupStore(tmp_path / "groups.json")
    assert [item["id"] for item in reloaded.list_groups()] == [group["id"]]


def test_group_store_survives_reload_and_drops_corrupt(tmp_path):
    path = tmp_path / "groups.json"
    store = GroupStore(path)
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    raw = json.loads(path.read_text())
    raw["groups"].append({"id": "not-a-group"})
    raw["groups"].append({"id": generate_group_id(), "members": [OWN_HASH]})
    # An id that does not derive from its roster is dropped as corrupt,
    # even though the shape is otherwise valid.
    raw["groups"].append(
        {
            "id": generate_group_id(),
            "name": "forged",
            "members": [OWN_HASH, OTHER_HASH],
            "creator": OWN_HASH,
            "created_by": OWN_HASH,
            "created_at": 1.0,
        }
    )
    path.write_text(json.dumps(raw))
    reloaded = GroupStore(path)
    assert [item["id"] for item in reloaded.list_groups()] == [group["id"]]


# -- handler ----------------------------------------------------------------


def test_group_message_delivered_and_group_pinned(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", members)
    response = call_handler(
        node._handle_group_message,
        encode_group_message(
            "hi all",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=members,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["group"] == group_id
    assert len(received) == 1
    record = received[0]
    assert record["kind"] == "group"
    assert record["group"] == group_id
    assert record["group_name"] == "weekend"
    assert record["text"] == "hi all"
    # The inbox keeps the group fields; the store pinned the roster.
    entry = node.inbox.list_messages()[0]
    assert entry["kind"] == "group" and entry["group"] == group_id
    assert node.inbox.list_messages(group=group_id)[0]["text"] == "hi all"
    assert node.inbox.list_messages(group=generate_group_id()) == []
    pinned = node.groups.get(group_id)
    assert pinned is not None and pinned["members"] == sorted(members)
    assert pinned["name"] == "weekend"
    assert pinned["creator"] == sender_hash


def test_group_message_from_non_member_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    # The sender's link identity is not on the roster it claims.
    response = call_handler(
        node._handle_group_message,
        make_group_payload(
            sender_members=[OWN_HASH, OTHER_HASH, THIRD_HASH]
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert received == []
    assert len(node.inbox) == 0
    assert len(node.groups) == 0


def test_group_message_from_blocked_sender_dropped_silently(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    # Blocking wins over the allow-list: the sender gets a generic
    # delivery failure, never a signal that they are blocked.
    node._blocklist = node.blocklist.with_added(sender_hash)
    members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", members)
    response = call_handler(
        node._handle_group_message,
        encode_group_message(
            "hi all",
            message_id="gm-blocked",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=members,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "unavailable"
    assert received == []
    assert len(node.inbox) == 0
    # The group is not pinned from a blocked sender's message.
    assert node.groups.get(group_id) is None


def test_group_message_for_group_without_this_node_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    response = call_handler(
        node._handle_group_message,
        make_group_payload(
            sender_members=[sender_hash, OTHER_HASH, THIRD_HASH]
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert received == []
    assert len(node.inbox) == 0


def test_group_message_with_changed_roster_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    first_members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", first_members)
    first = call_handler(
        node._handle_group_message,
        encode_group_message(
            "first",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=first_members,
        ),
        remote_identity=sender,
    )
    assert first["ok"] is True
    # Same id, different roster: the id no longer derives from the
    # roster, so the message is rejected at parse time — a changed
    # roster means a new group, and a member cannot fork the id.
    second = call_handler(
        node._handle_group_message,
        json.dumps(
            {
                "type": "group_message",
                "v": 1,
                "text": "second",
                "group": group_id,
                "group_name": "weekend",
                "creator": sender_hash,
                "members": [OWN_HASH, sender_hash, OTHER_HASH],
                "sent_at": 1.0,
                "id": "gm-2",
            }
        ).encode("utf-8"),
        remote_identity=sender,
    )
    assert second["ok"] is False
    assert second["error"]["code"] == "invalid_message"
    assert len(received) == 1


def test_group_message_dedupes_by_id(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    payload = encode_group_message(
        "once",
        message_id="gm-dup",
        group_id=derive_group_id(sender_hash, "weekend", [OWN_HASH, sender_hash, THIRD_HASH]),
        group_name="weekend",
        creator=sender_hash,
        members=[OWN_HASH, sender_hash, THIRD_HASH],
    )
    first = call_handler(node._handle_group_message, payload, remote_identity=sender)
    second = call_handler(node._handle_group_message, payload, remote_identity=sender)
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert len(received) == 1
    assert len(node.inbox) == 1


# -- relayed group messages --------------------------------------------------


def test_relayed_group_message_delivered_as_group(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    recipient = RNS.Identity()
    # The sealed envelope is addressed to this node: its identity is the
    # node destination, so the test recipient is the node's own identity.
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    members = [own, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", members)
    blob = seal_message(
        sender,
        sender_name="bob",
        recipient_public_key=recipient.get_public_key(),
        message_id="gm-relay-1",
        text="via a holder",
        sent_at=99.0,
        group={
            "id": group_id,
            "name": "weekend",
            "creator": sender_hash,
            "members": members,
        },
    )
    message = open_message(recipient, blob)
    assert message["group"]["id"] == group_id
    assert node._deliver_collected(message) is True
    assert len(received) == 1
    record = received[0]
    assert record["kind"] == "group"
    assert record["group"] == group_id
    assert record["relayed"] is True
    assert node.groups.get(group_id) is not None


def test_relayed_group_message_with_forked_roster_rejected(tmp_path):
    """A sealed envelope cannot fork an id either: the relayed group
    is validated against the derived id before delivery."""
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    members = [own, sender_hash, THIRD_HASH]
    real_id = derive_group_id(sender_hash, "weekend", members)
    forked_members = [own, sender_hash, OTHER_HASH]
    blob = seal_message(
        sender,
        sender_name="bob",
        recipient_public_key=recipient.get_public_key(),
        message_id="gm-relay-fork",
        text="forked roster",
        sent_at=99.0,
        group={
            "id": real_id,
            "name": "weekend",
            "creator": sender_hash,
            "members": forked_members,
        },
    )
    message = open_message(recipient, blob)
    assert node._deliver_collected(message) is False
    assert received == []
    assert len(node.inbox) == 0
    assert node.groups.get(real_id) is None


def test_relayed_group_message_from_non_member_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    members = [own, OTHER_HASH, THIRD_HASH]
    blob = seal_message(
        sender,
        sender_name="mallory",
        recipient_public_key=recipient.get_public_key(),
        message_id="gm-relay-2",
        text="forged roster",
        sent_at=99.0,
        group={
            "id": derive_group_id(sender_hash, "weekend", members),
            "name": "weekend",
            "creator": sender_hash,
            # The sender is not on the roster it claims.
            "members": members,
        },
    )
    message = open_message(recipient, blob)
    assert node._deliver_collected(message) is False
    assert received == []
    assert len(node.inbox) == 0


def test_seal_open_without_group_has_no_group_field():
    sender = RNS.Identity()
    recipient = RNS.Identity()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="plain-1",
        text="plain",
        sent_at=1.0,
    )
    assert "group" not in open_message(recipient, blob)


# -- outbox group context ----------------------------------------------------


def test_outbox_entry_group_roundtrip(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    entry = box.enqueue(
        to=bytes.fromhex(OTHER_HASH),
        text="queued group leg",
        group={
            "id": derive_group_id(OWN_HASH, "weekend", members),
            "name": "weekend",
            "creator": OWN_HASH,
            "members": members,
        },
    )
    assert entry.group_id is not None
    assert entry.group_creator == OWN_HASH
    reloaded = Mailbox(tmp_path / "mailbox.json")
    again = reloaded.get_outbox(entry.id)
    assert again is not None
    assert again.group_id == entry.group_id
    assert again.group_name == "weekend"
    assert again.group_creator == OWN_HASH
    assert again.group_members == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    assert again.to_dict()["group"]["id"] == entry.group_id
    assert again.to_dict()["group"]["creator"] == OWN_HASH


def test_outbox_entry_rejects_bad_group_context(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(Exception):
        box.enqueue(
            to=bytes.fromhex(OTHER_HASH),
            text="bad group",
            group={"id": "short", "name": "g", "members": [OWN_HASH]},
        )


# -- fan-out -----------------------------------------------------------------


def _stub_peer(hexhash, name="peer"):
    now = time.time()
    return Peer(
        destination_hash=bytes.fromhex(hexhash),
        name=name,
        hops=1,
        first_seen=now,
        last_seen=now,
        public_key=b"\x00" * 64,
    )


def test_send_group_message_fans_out_and_queues_offline(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for queueing
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    calls = []

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        calls.append((peer.hexhash, path, payload, deadline))
        return {"ok": True}

    def fake_resolve(target, timeout=10.0):
        if target == OTHER_HASH:
            return _stub_peer(OTHER_HASH, "bob")
        raise NodeError("unreachable")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = fake_resolve  # type: ignore[method-assign]
    result = node.send_group_message(group, "hello all")
    assert result["ok"] is True
    assert result["group"] == group["id"]
    by_to = {entry["to"]: entry for entry in result["results"]}
    assert by_to[OTHER_HASH]["delivered"] is True
    assert by_to[THIRD_HASH]["queued"] is True
    assert len(calls) == 1
    assert calls[0][1] == "/tau/group"
    # The leg's deadline rides along, so one slow leg cannot
    # consume its whole budget stage by stage.
    assert calls[0][3] is not None
    parsed = parse_group_message(calls[0][2])
    assert parsed["text"] == "hello all"
    assert parsed["group"] == group["id"]
    assert parsed["members"] == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    # The offline leg is queued with its group context intact.
    queued = node.mailbox.outbox_list()
    assert len(queued) == 1
    assert queued[0].to_hex == THIRD_HASH
    assert queued[0].group_id == group["id"]
    assert queued[0].group_creator == OWN_HASH
    assert queued[0].group_members == [OWN_HASH, OTHER_HASH, THIRD_HASH]

    # Each leg is registered for read receipts, like a peer DM: the
    # delivered leg under its wire id, so a recipient's receipt
    # verifies instead of being rejected as unknown. (The queued leg
    # is registered under the same id twice over: the outbox entry
    # reuses the wire id as its id, so the second registration is a
    # no-op, exactly as for a queued peer DM.)
    delivered_id = by_to[OTHER_HASH]["id"]
    assert node.mailbox.sent_record(delivered_id) is not None
    assert (
        node.mailbox.record_read(delivered_id, time.time(), reader_hash=OTHER_HASH)
        is True
    )
    assert node.mailbox.read_at(delivered_id) is not None
    third_records = [
        record for record in node.mailbox.sent_list() if record["to"] == THIRD_HASH
    ]
    assert len(third_records) == 1
    assert third_records[0]["id"] == by_to[THIRD_HASH]["id"]
    assert third_records[0]["id"] == queued[0].id


def test_send_group_message_skips_self_and_records_refusal(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        from tau.node import NotAllowedError

        raise NotAllowedError("not allowed")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    result = node.send_group_message(group, "hello")
    # Neither leg is delivered or queued: both members refused, and the
    # sender never sends to itself. The fan-out reports failure.
    assert result["ok"] is False
    assert all(entry.get("error") == "not_allowed" for entry in result["results"])
    assert len(result["results"]) == 2
    assert node.mailbox.outbox_list() == []
    # A refused leg leaves no sent record behind: a later receipt
    # for its id must not verify.
    assert node.mailbox.sent_list() == []


def test_send_group_message_treats_not_a_member_as_final(tmp_path):
    from tau.node import NotAMemberError

    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        raise NotAMemberError("peer rejected the request: not a member")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    result = node.send_group_message(group, "hello")
    # A membership refusal is permanent: the legs are reported failed,
    # never queued for retry until their TTL.
    assert result["ok"] is False
    assert all(entry.get("error") == "not_a_member" for entry in result["results"])
    assert node.mailbox.outbox_list() == []


def test_send_group_message_keeps_results_when_outbox_full(tmp_path):
    from tau.mailbox import MailboxFull

    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def unreachable(peer, path, payload, timeout, *, deadline=None):
        raise NodeError("unreachable")

    node._request = unreachable  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    real_enqueue = node.mailbox.enqueue
    calls = {"n": 0}

    def flaky_enqueue(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] > 1:
            raise MailboxFull("outbox is full")
        return real_enqueue(*args, **kwargs)

    node.mailbox.enqueue = flaky_enqueue  # type: ignore[method-assign]
    result = node.send_group_message(group, "hello")
    # The first leg queued; the second leg's full outbox is a per-leg
    # failure, not a lost fan-out.
    assert result["ok"] is False
    assert len(result["results"]) == 2
    assert result["results"][0].get("queued") is True
    assert result["results"][1].get("ok") is False
    assert result["results"][1]["error"].startswith("could not queue")
    assert len(node.mailbox.outbox_list()) == 1
    # The queued leg is registered under its id (wire id and outbox
    # id are the same here); the leg that could not be queued leaves
    # no sent record behind.
    sent = node.mailbox.sent_list()
    assert len(sent) == 1
    assert sent[0]["to"] == OTHER_HASH
    assert sent[0]["id"] == result["results"][0]["id"]


def test_send_group_message_queues_legs_past_the_deadline(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def no_request(peer, path, payload, timeout, *, deadline=None):
        raise AssertionError("leg attempted past the deadline")

    node._request = no_request  # type: ignore[method-assign]
    # A zero timeout leaves no budget for any attempt: every leg must
    # still reach the outbox instead of being dropped to the deadline,
    # so the members get the message on a later retry.
    result = node.send_group_message(group, "hello", timeout=0.0)
    assert result["ok"] is True
    assert len(result["results"]) == 2
    assert all(entry.get("queued") is True for entry in result["results"])
    queued = node.mailbox.outbox_list()
    assert len(queued) == 2
    assert all(entry.group_id == group["id"] for entry in queued)
    assert all(
        node.mailbox.sent_record(entry["id"]) is not None
        for entry in result["results"]
    )


def test_send_group_message_slow_first_leg_does_not_starve_later_legs(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    attempts = []

    def fake_resolve(target, timeout=10.0):
        return _stub_peer(target, "peer")

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        attempts.append(peer.hexhash)
        if peer.hexhash == OTHER_HASH:
            # An offline member's path wait consumes its whole leg
            # budget before the attempt fails.
            time.sleep(1.2)
            raise NodeError("no path")
        return {"ok": True}

    node.resolve_peer = fake_resolve  # type: ignore[method-assign]
    node._request = fake_request  # type: ignore[method-assign]
    # The roster is attempted in sorted order (OTHER before THIRD).
    # With a single fan-out-wide deadline, the first leg burning its
    # budget left every later member queued without an attempt — the
    # regression behind the flaky
    # test_three_nodes_exchange_group_messages integration failure,
    # where an offline member sorting first starved the online one.
    result = node.send_group_message(group, "hello", timeout=1.0)
    by_to = {entry["to"]: entry for entry in result["results"]}
    assert by_to[OTHER_HASH]["queued"] is True
    assert by_to[THIRD_HASH]["delivered"] is True
    assert attempts == [OTHER_HASH, THIRD_HASH]


def test_send_group_message_rejects_oversized_text_before_any_leg(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    members = [OWN_HASH] + [f"{i:032x}" for i in range(1, 32)]
    group = node.groups.create("big", members, created_by=OWN_HASH)
    calls = []
    node._request = lambda peer, path, payload, timeout, **kwargs: calls.append(peer)  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    # 2000 four-byte characters fit the DM text limit but exceed the
    # 8 KiB group request once the 32-member roster is included.
    with pytest.raises(NodeError, match="too large"):
        node.send_group_message(group, "\U0001d11e" * 2000)
    assert calls == []
    assert node.mailbox.outbox_list() == []


def test_deliver_entry_re_encodes_group_payload(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
    )
    entry = node.mailbox.enqueue(
        to=bytes.fromhex(OTHER_HASH),
        text="retry me",
        group={
            "id": group["id"],
            "name": "weekend",
            "creator": OWN_HASH,
            "members": [OWN_HASH, OTHER_HASH],
        },
    )
    seen = []

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        seen.append((path, parse_group_message(payload)))
        return {"ok": True}

    node._request = fake_request  # type: ignore[method-assign]
    assert node._deliver_entry(_stub_peer(OTHER_HASH), entry, 5.0) is True
    assert len(seen) == 1
    assert seen[0][0] == "/tau/group"
    assert seen[0][1]["text"] == "retry me"
    assert seen[0][1]["group"] == group["id"]
    assert node.mailbox.get_outbox(entry.id) is None


# -- CLI ---------------------------------------------------------------------


def test_parser_group_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["group", "create", "weekend", "--peer", "bob", "--peer", "carol"]
    )
    assert args.group_command == "create"
    assert args.name == "weekend"
    assert args.peer == ["bob", "carol"]

    send_args = parser.parse_args(["group", "send", "weekend", "hello all"])
    assert send_args.group_command == "send"
    assert send_args.group == "weekend"
    assert send_args.message == "hello all"

    list_args = parser.parse_args(["group", "list"])
    assert list_args.group_command == "list"

    delete_args = parser.parse_args(["group", "delete", "weekend"])
    assert delete_args.group_command == "delete"
    assert delete_args.group == "weekend"

    messages_args = parser.parse_args(["messages", "--group", "weekend"])
    assert messages_args.group == "weekend"


def test_group_list_on_empty_store(tmp_path, capsys):
    code = cli.main(["group", "list", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    assert "No groups yet" in capsys.readouterr().out


def test_group_list_json(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    code = cli.main(
        ["group", "list", "--data-dir", str(tmp_path / "node"), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["groups"][0]["name"] == "weekend"
    assert len(payload["groups"][0]["members"]) == 2


def test_group_delete_via_cli(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    code = cli.main(
        ["group", "delete", "weekend", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "deleted group weekend" in capsys.readouterr().out
    # list_groups reloads from disk, so it sees the other process's delete.
    assert store.list_groups() == []
    assert (
        cli.main(["group", "delete", "weekend", "--data-dir", str(tmp_path / "node")])
        != 0
    )


def test_messages_group_filter(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    inbox = Inbox(config.inbox_path)
    group = GroupStore(config.groups_path).create(
        "weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
    )
    inbox.append(
        {
            "from": "bob",
            "destination": OTHER_HASH,
            "text": "group hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "g-1",
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
        }
    )
    inbox.append(
        {
            "from": "bob",
            "destination": OTHER_HASH,
            "text": "plain hello",
            "sent_at": 11.0,
            "received_at": 21.0,
            "id": "m-1",
        }
    )
    code = cli.main(
        [
            "messages",
            "--group",
            "weekend",
            "--data-dir",
            str(tmp_path / "node"),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["text"] == "group hello"

    code = cli.main(
        [
            "messages",
            "--group",
            "nope",
            "--data-dir",
            str(tmp_path / "node"),
            "--json",
        ]
    )
    assert code != 0

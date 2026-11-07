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
    message_id="group-msg-1",
):
    members = sender_members or [OWN_HASH, OTHER_HASH, THIRD_HASH]
    return encode_group_message(
        text,
        message_id=message_id,
        group_id=group_id or generate_group_id(),
        group_name=group_name,
        members=members,
    )


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


# -- wire format ------------------------------------------------------------


def test_group_message_roundtrip():
    group_id = generate_group_id()
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    payload = encode_group_message(
        "hi all",
        message_id="gm-1",
        group_id=group_id,
        group_name="weekend",
        members=members,
        sent_at=123.0,
    )
    parsed = parse_group_message(payload)
    assert parsed["text"] == "hi all"
    assert parsed["id"] == "gm-1"
    assert parsed["group"] == group_id
    assert parsed["group_name"] == "weekend"
    assert parsed["members"] == members
    assert parsed["sent_at"] == 123.0


def test_group_message_rejects_bad_rosters():
    group_id = generate_group_id()
    with pytest.raises(ProtocolError):
        encode_group_message("x", group_id=group_id, group_name="g", members=[OWN_HASH])
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            members=[OWN_HASH, OWN_HASH],
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x", group_id=group_id, group_name="g", members=["not-a-hash", OTHER_HASH]
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            members=[f"{i:032x}" for i in range(33)],
        )


def test_parse_group_message_rejects_wrong_type():
    raw = json.dumps(
        {
            "type": "message",
            "v": 1,
            "text": "not a group message",
            "group": generate_group_id(),
            "group_name": "g",
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
    assert len(group["id"]) == 32
    assert group["name"] == "weekend"
    assert group["members"] == [OWN_HASH, OTHER_HASH]
    assert store.get(group["id"]) == group
    assert store.find(group["id"]) == group
    assert store.find(group["id"][:8]) == group
    assert store.find("weekend") == group
    assert [item["id"] for item in store.list_groups()] == [group["id"]]
    assert len(store) == 1


def test_group_store_find_errors(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    first = store.create("same", [OWN_HASH, OTHER_HASH])
    second = store.create("same", [OWN_HASH, THIRD_HASH])
    with pytest.raises(ValueError, match="unknown group"):
        store.find("nope")
    with pytest.raises(ValueError, match="2 groups are named"):
        store.find("same")
    assert store.find(first["id"][:4]) == first or store.find(second["id"][:4]) == second


def test_group_store_record_first_roster_wins(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group_id = generate_group_id()
    first = store.record(group_id, "weekend", [OWN_HASH, OTHER_HASH])
    assert first is not None
    assert first["id"] == group_id
    # Same roster again: the pinned record comes back unchanged.
    assert store.record(group_id, "renamed", [OWN_HASH, OTHER_HASH]) == first
    # A different roster for the same id is refused.
    assert store.record(group_id, "weekend", [OWN_HASH, THIRD_HASH]) is None
    assert store.get(group_id) == first


def test_group_store_survives_reload_and_drops_corrupt(tmp_path):
    path = tmp_path / "groups.json"
    store = GroupStore(path)
    group = store.create("weekend", [OWN_HASH, OTHER_HASH])
    raw = json.loads(path.read_text())
    raw["groups"].append({"id": "not-a-group"})
    raw["groups"].append({"id": generate_group_id(), "members": [OWN_HASH]})
    path.write_text(json.dumps(raw))
    reloaded = GroupStore(path)
    assert [item["id"] for item in reloaded.list_groups()] == [group["id"]]


# -- handler ----------------------------------------------------------------


def test_group_message_delivered_and_group_pinned(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = generate_group_id()
    response = call_handler(
        node._handle_group_message,
        encode_group_message(
            "hi all",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
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
    assert pinned is not None and pinned["members"] == members
    assert pinned["name"] == "weekend"


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
    group_id = generate_group_id()
    first_members = [OWN_HASH, sender_hash, THIRD_HASH]
    first = call_handler(
        node._handle_group_message,
        encode_group_message(
            "first",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            members=first_members,
        ),
        remote_identity=sender,
    )
    assert first["ok"] is True
    # Same id, different roster: a changed roster means a new group.
    second = call_handler(
        node._handle_group_message,
        encode_group_message(
            "second",
            message_id="gm-2",
            group_id=group_id,
            group_name="weekend",
            members=[OWN_HASH, sender_hash, OTHER_HASH],
        ),
        remote_identity=sender,
    )
    assert second["ok"] is False
    assert second["error"]["code"] == "not_a_member"
    assert len(received) == 1


def test_group_message_dedupes_by_id(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    payload = encode_group_message(
        "once",
        message_id="gm-dup",
        group_id=generate_group_id(),
        group_name="weekend",
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
    group_id = generate_group_id()
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
            "members": [own, sender_hash, THIRD_HASH],
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
    blob = seal_message(
        sender,
        sender_name="mallory",
        recipient_public_key=recipient.get_public_key(),
        message_id="gm-relay-2",
        text="forged roster",
        sent_at=99.0,
        group={
            "id": generate_group_id(),
            "name": "weekend",
            # The sender is not on the roster it claims.
            "members": [own, OTHER_HASH, THIRD_HASH],
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
    entry = box.enqueue(
        to=bytes.fromhex(OTHER_HASH),
        text="queued group leg",
        group={
            "id": generate_group_id(),
            "name": "weekend",
            "members": [OWN_HASH, OTHER_HASH, THIRD_HASH],
        },
    )
    assert entry.group_id is not None
    reloaded = Mailbox(tmp_path / "mailbox.json")
    again = reloaded.get_outbox(entry.id)
    assert again is not None
    assert again.group_id == entry.group_id
    assert again.group_name == "weekend"
    assert again.group_members == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    assert again.to_dict()["group"]["id"] == entry.group_id


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

    def fake_request(peer, path, payload, timeout):
        calls.append((peer.hexhash, path, payload))
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
    parsed = parse_group_message(calls[0][2])
    assert parsed["text"] == "hello all"
    assert parsed["group"] == group["id"]
    assert parsed["members"] == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    # The offline leg is queued with its group context intact.
    queued = node.mailbox.outbox_list()
    assert len(queued) == 1
    assert queued[0].to_hex == THIRD_HASH
    assert queued[0].group_id == group["id"]
    assert queued[0].group_members == [OWN_HASH, OTHER_HASH, THIRD_HASH]


def test_send_group_message_skips_self_and_records_refusal(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def fake_request(peer, path, payload, timeout):
        from tau.node import NotAllowedError

        raise NotAllowedError("not allowed")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    result = node.send_group_message(group, "hello")
    # Neither leg is delivered or queued: both members refused, and the
    # sender never sends to itself.
    assert all(entry.get("error") == "not_allowed" for entry in result["results"])
    assert len(result["results"]) == 2
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
            "members": [OWN_HASH, OTHER_HASH],
        },
    )
    seen = []

    def fake_request(peer, path, payload, timeout):
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

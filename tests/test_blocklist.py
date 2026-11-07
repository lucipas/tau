"""Block-list (deny-list): module, node enforcement and CLI behaviour."""

from __future__ import annotations

import json
import os
import stat
import time
from types import SimpleNamespace

import pytest
import RNS

from tau import cli
from tau.blocklist import (
    BlockList,
    BlockListError,
    load_blocked_set,
    load_blocklist,
    load_blocklist_or_block_all,
    save_blocklist,
)
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.mailbox import open_message, seal_message
from tau.node import TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    APP_NAME,
    ASPECT,
    decode,
    encode_message,
)


# -- module ---------------------------------------------------------------

ALICE = "aa" * 16
BOB = "bb" * 16


def test_missing_file_blocks_nobody(tmp_path):
    blocklist = load_blocklist(tmp_path / "blocklist.json")
    assert blocklist.blocked == frozenset()
    assert blocklist.block_all is False
    assert blocklist.is_blocked(ALICE) is False
    assert blocklist.is_blocked(None) is False


def test_unreadable_location_fails_closed(tmp_path):
    # A path that cannot even be statted (here: a file where a
    # directory should be) is not "missing" — it fails closed.
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(BlockListError):
        load_blocklist(blocker / "blocklist.json")
    blocked = load_blocklist_or_block_all(blocker / "blocklist.json")
    assert blocked.block_all is True
    assert blocked.is_blocked(ALICE) is True
    assert blocked.is_blocked(BOB) is True


def test_blocked_set_admits_only_unblocked():
    blocklist = BlockList(blocked=frozenset({ALICE}))
    assert blocklist.is_blocked(ALICE) is True
    assert blocklist.is_blocked(ALICE.upper()) is True
    assert blocklist.is_blocked(BOB) is False
    assert blocklist.is_blocked(None) is False
    assert blocklist.is_blocked("not-a-hash") is False


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "blocklist.json"
    saved = save_blocklist(path, BlockList(blocked=frozenset({BOB, ALICE})))
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_blocklist(path)
    assert loaded.blocked == frozenset({ALICE, BOB})
    assert loaded.block_all is False
    assert json.loads(path.read_text())["blocked"] == sorted((ALICE, BOB))


def test_with_added_removed_are_immutable():
    base = BlockList()
    added = base.with_added(ALICE)
    assert base.blocked == frozenset()
    assert added.blocked == frozenset({ALICE})
    assert added.with_removed(ALICE).blocked == frozenset()


def test_from_dict_rejects_bad_values():
    with pytest.raises(BlockListError):
        BlockList.from_dict(["not", "a", "dict"])
    with pytest.raises(BlockListError):
        BlockList.from_dict({"blocked": ["zz" * 16]})
    with pytest.raises(BlockListError):
        BlockList.from_dict({"blocked": "aa" * 16})


def test_from_dict_requires_blocked_field():
    # A persisted list that lost its blocked key must fail closed
    # (block all) rather than silently admit everyone.
    with pytest.raises(BlockListError):
        BlockList.from_dict({})


def test_from_dict_rejects_null_blocked():
    # A present null is malformed, not an empty list: it must fail
    # closed rather than switch enforcement off.
    with pytest.raises(BlockListError):
        BlockList.from_dict({"blocked": None})


def test_corrupt_file_raises_and_fails_closed(tmp_path):
    path = tmp_path / "blocklist.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(BlockListError):
        load_blocklist(path)
    blocked = load_blocklist_or_block_all(path)
    assert blocked.block_all is True
    assert blocked.is_blocked(ALICE) is True


def test_fail_closed_logs_the_bad_file(tmp_path, caplog):
    import logging

    path = tmp_path / "blocklist.json"
    path.write_text('{"other": []}', encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="tau.blocklist"):
        blocked = load_blocklist_or_block_all(path)
    assert blocked.block_all is True
    assert "unusable" in caplog.text
    assert str(path) in caplog.text


def test_load_blocked_set_never_raises(tmp_path):
    path = tmp_path / "blocklist.json"
    path.write_text("{not json", encoding="utf-8")
    assert load_blocked_set(path) == frozenset()
    save_blocklist(path, BlockList(blocked=frozenset({ALICE})))
    assert load_blocked_set(path) == frozenset({ALICE})


# -- node enforcement ------------------------------------------------------

def make_node(tmp_path, name="holder", on_message=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
    )
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    return node


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


def test_message_from_blocked_sender_is_dropped_silently(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_blocklist(
        node.config.blocklist_path,
        BlockList(blocked=frozenset({destination_hash(sender).hex()})),
    )
    result = call_handler(
        node._handle_message, encode_message("hello", message_id="block-1"), sender
    )
    # No oracle: the reply is the same success shape a delivered
    # message gets, never a blocked signal.
    assert result["ok"] is True
    assert result["duplicate"] is False
    assert received == []
    # Dropped messages are not marked seen: unblocking the sender and
    # retrying the same id delivers it as a fresh message.
    save_blocklist(node.config.blocklist_path, BlockList())
    node._blocklist = None
    retry = call_handler(
        node._handle_message, encode_message("hello", message_id="block-1"), sender
    )
    assert retry["ok"] is True
    assert retry["duplicate"] is False
    assert len(received) == 1


def test_block_wins_over_allow_list(tmp_path):
    from tau.allowlist import AllowList, save_allowlist

    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({sender_hash})),
    )
    save_blocklist(
        node.config.blocklist_path, BlockList(blocked=frozenset({sender_hash}))
    )
    result = call_handler(node._handle_message, encode_message("hi"), sender)
    assert result["ok"] is False
    assert result["error"]["code"] == "unavailable"
    assert received == []


def test_message_from_unblocked_sender_passes(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_blocklist(node.config.blocklist_path, BlockList(blocked=frozenset({ALICE})))
    result = call_handler(node._handle_message, encode_message("hi"), sender)
    assert result["ok"] is True
    assert len(received) == 1


# -- relayed (store-and-forward) enforcement ------------------------------

def make_relayed_message(sender, recipient, message_id="relay-1"):
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id=message_id,
        text="via holder",
        sent_at=1.0,
    )
    return open_message(recipient, blob)


def make_recipient_node(tmp_path, on_message=None):
    node = make_node(tmp_path, on_message=on_message)
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    return node, recipient


def test_relayed_message_from_blocked_sender_is_dropped(tmp_path):
    received = []
    node, recipient = make_recipient_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_blocklist(
        node.config.blocklist_path,
        BlockList(blocked=frozenset({destination_hash(sender).hex()})),
    )
    message = make_relayed_message(sender, recipient)
    assert node._deliver_collected(message) is False
    assert received == []
    assert node.mailbox.is_seen("relay-1") is False


def test_relayed_message_from_unblocked_sender_passes(tmp_path):
    received = []
    node, recipient = make_recipient_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_blocklist(node.config.blocklist_path, BlockList(blocked=frozenset({ALICE})))
    message = make_relayed_message(sender, recipient)
    assert node._deliver_collected(message) is True
    assert len(received) == 1
    assert received[0]["relayed"] is True


# -- CLI -------------------------------------------------------------------

def _seed_peer(tmp_path, name="alice"):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    identity = RNS.Identity()
    sender_hash = destination_hash(identity)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(sender_hash, identity.get_public_key(), name=name, now=100.0)
    registry.save()
    return data_dir, sender_hash


def test_block_cli_add_list_remove(tmp_path, capsys):
    data_dir, sender_hash = _seed_peer(tmp_path)
    code = cli.main(["block", "add", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert sender_hash.hex() in out
    assert "alice" in out

    code = cli.main(["block", "list", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["blocked"] == [sender_hash.hex()]
    assert payload["entries"][0]["name"] == "alice"

    code = cli.main(["block", "add", "alice", "--data-dir", str(data_dir)])
    assert code == 0  # idempotent

    code = cli.main(["block", "remove", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    capsys.readouterr()  # drain the text listing before the JSON one
    code = cli.main(["block", "list", "--data-dir", str(data_dir), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["blocked"] == []


def test_block_cli_unknown_peer(tmp_path, capsys):
    data_dir, _ = _seed_peer(tmp_path)
    code = cli.main(["block", "add", "nobody", "--data-dir", str(data_dir)])
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err


def test_messages_hides_blocked_senders(tmp_path, capsys):
    data_dir, sender_hash = _seed_peer(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    other = RNS.Identity()
    other_hash = destination_hash(other)
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "from alice",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-1",
        }
    )
    inbox.append(
        {
            "from": "mallory",
            "destination": other_hash.hex(),
            "text": "from mallory",
            "sent_at": 11.0,
            "received_at": 21.0,
            "id": "m-2",
        }
    )
    save_blocklist(
        config.blocklist_path, BlockList(blocked=frozenset({other_hash.hex()}))
    )

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["id"] == "m-1"

    # Unblocking restores the view: nothing was deleted.
    save_blocklist(config.blocklist_path, BlockList())
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2

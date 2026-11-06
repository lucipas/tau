"""DM allow-list: module, node enforcement and CLI behaviour."""

from __future__ import annotations

import json
import os
import stat
import time
from types import SimpleNamespace

import pytest
import RNS

from tau import cli
from tau.allowlist import (
    AllowList,
    AllowListError,
    clean_destination,
    load_allowlist,
    load_allowlist_or_deny_all,
    save_allowlist,
)
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.mailbox import open_message, seal_message
from tau.node import NodeError, NotAllowedError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    APP_NAME,
    ASPECT,
    decode,
    encode_message,
    response_error,
)


# -- module ---------------------------------------------------------------

ALICE = "aa" * 16
BOB = "bb" * 16


def test_missing_file_is_disabled(tmp_path):
    allowlist = load_allowlist(tmp_path / "allowlist.json")
    assert allowlist.enabled is False
    assert allowlist.allow == frozenset()
    assert allowlist.is_allowed(ALICE) is True
    assert allowlist.is_allowed(None) is True


def test_unreadable_location_fails_closed(tmp_path):
    # A path that cannot even be statted (here: a file where a
    # directory should be) is not "missing" — it fails closed.
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(AllowListError):
        load_allowlist(blocker / "allowlist.json")
    denied = load_allowlist_or_deny_all(blocker / "allowlist.json")
    assert denied.enabled is True
    assert denied.is_allowed(ALICE) is False


def test_enabled_list_admits_only_listed():
    allowlist = AllowList(enabled=True, allow=frozenset({ALICE}))
    assert allowlist.is_allowed(ALICE) is True
    assert allowlist.is_allowed(ALICE.upper()) is True
    assert allowlist.is_allowed(BOB) is False
    assert allowlist.is_allowed(None) is False
    assert allowlist.is_allowed("not-a-hash") is False


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "allowlist.json"
    saved = save_allowlist(path, AllowList(enabled=True, allow=frozenset({BOB, ALICE})))
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_allowlist(path)
    assert loaded.enabled is True
    assert loaded.allow == frozenset({ALICE, BOB})
    assert json.loads(path.read_text())["allow"] == sorted((ALICE, BOB))


def test_with_added_removed_enabled_are_immutable():
    base = AllowList()
    added = base.with_added(ALICE)
    assert base.allow == frozenset()
    assert added.allow == frozenset({ALICE})
    assert added.with_removed(ALICE).allow == frozenset()
    assert added.with_enabled(True).enabled is True
    assert added.enabled is False


def test_from_dict_rejects_bad_values():
    with pytest.raises(AllowListError):
        AllowList.from_dict(["not", "a", "dict"])
    with pytest.raises(AllowListError):
        AllowList.from_dict({"enabled": "yes"})
    with pytest.raises(AllowListError):
        AllowList.from_dict({"enabled": True, "allow": ["zz" * 16]})
    with pytest.raises(AllowListError):
        AllowList.from_dict({"enabled": True, "allow": "aa" * 16})


def test_from_dict_requires_enabled_field():
    # A persisted list that lost its enabled flag must fail closed
    # (deny all) rather than silently turn enforcement off.
    with pytest.raises(AllowListError):
        AllowList.from_dict({"allow": [ALICE]})


def test_clean_destination_normalises_and_validates():
    assert clean_destination("  " + "AA" * 16 + "  ") == "aa" * 16
    with pytest.raises(AllowListError):
        clean_destination("aa" * 15)
    with pytest.raises(AllowListError):
        clean_destination("zz" * 16)
    with pytest.raises(AllowListError):
        clean_destination(1234)


def test_corrupt_file_raises_and_fails_closed(tmp_path):
    path = tmp_path / "allowlist.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(AllowListError):
        load_allowlist(path)
    denied = load_allowlist_or_deny_all(path)
    assert denied.enabled is True
    assert denied.allow == frozenset()
    assert denied.is_allowed(ALICE) is False


def test_fail_closed_logs_the_bad_file(tmp_path, caplog):
    import logging

    path = tmp_path / "allowlist.json"
    path.write_text('{"allow": []}', encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="tau.allowlist"):
        denied = load_allowlist_or_deny_all(path)
    assert denied.enabled is True
    assert "unusable" in caplog.text
    assert str(path) in caplog.text


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


def test_message_allowed_when_list_disabled(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    result = call_handler(
        node._handle_message, encode_message("hello"), sender
    )
    assert result["ok"] is True
    assert len(received) == 1


def test_message_denied_when_sender_not_listed(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    result = call_handler(
        node._handle_message, encode_message("hello", message_id="deny-1"), sender
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"
    assert received == []
    # Denied messages are not marked seen: admitting the sender and
    # retrying the same id delivers it as a fresh message.
    save_allowlist(
        node.config.allowlist_path,
        AllowList(
            enabled=True,
            allow=frozenset({destination_hash(sender).hex()}),
        ),
    )
    node._allowlist = None
    retry = call_handler(
        node._handle_message, encode_message("hello", message_id="deny-1"), sender
    )
    assert retry["ok"] is True
    assert retry["duplicate"] is False
    assert len(received) == 1


def test_message_allowed_when_sender_listed(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    # The sender is not in the peer registry: admission uses the tau
    # destination hash derived from the verified identity, so listing
    # that hash admits them.
    save_allowlist(
        node.config.allowlist_path,
        AllowList(
            enabled=True,
            allow=frozenset({destination_hash(sender).hex()}),
        ),
    )
    result = call_handler(
        node._handle_message, encode_message("hi"), sender
    )
    assert result["ok"] is True
    assert len(received) == 1
    assert received[0]["destination"] == destination_hash(sender).hex()


def test_identity_hash_alone_does_not_admit(tmp_path):
    # The allow-list is keyed by tau destination hash, not by the
    # Reticulum identity hash: listing the latter must not admit.
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({sender.hexhash})),
    )
    result = call_handler(
        node._handle_message, encode_message("hi"), sender
    )
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"
    assert received == []


def test_anonymous_message_denied_when_list_enabled(tmp_path):
    node = make_node(tmp_path)
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    result = call_handler(node._handle_message, encode_message("hi"))
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"


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


def test_relayed_message_denied_when_sender_not_listed(tmp_path):
    received = []
    node, recipient = make_recipient_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    message = make_relayed_message(sender, recipient)
    assert node._deliver_collected(message) is False
    assert received == []
    assert node.mailbox.is_seen("relay-1") is False


def test_relayed_message_allowed_when_sender_listed(tmp_path):
    received = []
    node, recipient = make_recipient_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    save_allowlist(
        node.config.allowlist_path,
        AllowList(
            enabled=True,
            allow=frozenset({destination_hash(sender).hex()}),
        ),
    )
    message = make_relayed_message(sender, recipient)
    assert node._deliver_collected(message) is True
    assert len(received) == 1
    assert received[0]["relayed"] is True
    assert received[0]["destination"] == destination_hash(sender).hex()


# -- explicit denial is not queued ------------------------------------------

def make_peer(name="alice"):
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    return Peer(
        destination_hash=destination,
        name=name,
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )


def test_send_message_does_not_queue_explicit_denial(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )

    def denied(*args, **kwargs):
        raise NotAllowedError("peer rejected the request: not allowed")

    monkeypatch.setattr(node, "_request", denied)
    with pytest.raises(NotAllowedError):
        node.send_message("alice", "hello")
    assert node.mailbox.outbox_list() == []


class _FakeReceipt:
    def __init__(self, response):
        self._response = response

    def get_response(self):
        return self._response

    def get_response_time(self):
        return 0.01

    def get_status(self):
        return 0


class _FakeLink:
    def __init__(self, response):
        self._response = response

    def request(self, path, payload, response_callback, failed_callback,
                timeout, max_response_size):
        receipt = _FakeReceipt(self._response)
        response_callback(receipt)
        return receipt


def test_request_on_link_maps_not_allowed(tmp_path):
    node = make_node(tmp_path)
    link = _FakeLink(response_error("not_allowed", "sender is not allowed"))
    with pytest.raises(NotAllowedError):
        node._request_on_link(link, "/tau/message", b"{}", 5.0)


def test_request_on_link_other_errors_stay_node_errors(tmp_path):
    node = make_node(tmp_path)
    link = _FakeLink(response_error("invalid_message", "bad payload"))
    with pytest.raises(NodeError) as exc_info:
        node._request_on_link(link, "/tau/message", b"{}", 5.0)
    assert not isinstance(exc_info.value, NotAllowedError)


# -- CLI -------------------------------------------------------------------

def run_cli(*argv):
    return cli.main(list(argv))


def make_peer_in_registry(data_dir, name="alice"):
    config = TauConfig.resolve(data_dir=data_dir, name="tester")
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination, identity.get_public_key(), name=name, now=1.0
    )
    registry.save()
    return destination.hex()


def test_cli_list_empty_and_disabled(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert run_cli("allowlist", "list", "--data-dir", str(data_dir)) == 0
    out = capsys.readouterr().out
    assert "off" in out
    assert "no senders listed" in out


def test_cli_add_on_off_remove_roundtrip(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert run_cli("allowlist", "add", ALICE, "--data-dir", str(data_dir)) == 0
    assert run_cli("allowlist", "on", "--data-dir", str(data_dir)) == 0
    assert run_cli("allowlist", "list", "--data-dir", str(data_dir)) == 0
    out = capsys.readouterr().out
    assert "enforced" in out
    assert ALICE in out

    assert run_cli("allowlist", "remove", ALICE, "--data-dir", str(data_dir)) == 0
    assert run_cli("allowlist", "off", "--data-dir", str(data_dir)) == 0
    assert run_cli("allowlist", "list", "--data-dir", str(data_dir)) == 0
    out = capsys.readouterr().out
    assert "off" in out
    assert ALICE not in out


def test_cli_add_resolves_peer_name(tmp_path, capsys):
    data_dir = tmp_path / "node"
    peer_hash = make_peer_in_registry(data_dir, name="alice")
    assert run_cli("allowlist", "add", "alice", "--data-dir", str(data_dir)) == 0
    capsys.readouterr()  # drain the human-readable add output
    assert run_cli("allowlist", "list", "--json", "--data-dir", str(data_dir)) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["enabled"] is False
    assert payload["entries"] == [
        {"destination": peer_hash, "name": "alice"}
    ]


def test_cli_add_unknown_peer_errors(tmp_path, capsys):
    data_dir = tmp_path / "node"
    code = run_cli("allowlist", "add", "nobody", "--data-dir", str(data_dir))
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err


def test_cli_add_short_prefix_errors(tmp_path, capsys):
    data_dir = tmp_path / "node"
    make_peer_in_registry(data_dir, name="alice")
    # Too short to be a hash prefix and not a peer name: unknown.
    code = run_cli("allowlist", "add", "al", "--data-dir", str(data_dir))
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err

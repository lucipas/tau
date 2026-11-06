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
from tau.node import TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import APP_NAME, ASPECT, decode, encode_message


# -- module ---------------------------------------------------------------

ALICE = "aa" * 16
BOB = "bb" * 16


def test_missing_file_is_disabled(tmp_path):
    allowlist = load_allowlist(tmp_path / "allowlist.json")
    assert allowlist.enabled is False
    assert allowlist.allow == frozenset()
    assert allowlist.is_allowed(ALICE) is True
    assert allowlist.is_allowed(None) is True


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
        AllowList(enabled=True, allow=frozenset({sender.hexhash})),
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
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({sender.hexhash})),
    )
    result = call_handler(
        node._handle_message, encode_message("hi"), sender
    )
    assert result["ok"] is True
    assert len(received) == 1


def test_anonymous_message_denied_when_list_enabled(tmp_path):
    node = make_node(tmp_path)
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({ALICE})),
    )
    result = call_handler(node._handle_message, encode_message("hi"))
    assert result["ok"] is False
    assert result["error"]["code"] == "not_allowed"


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

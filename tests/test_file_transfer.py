"""File transfer: signed offers, receiver policy, Resource handling."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import RNS
import pytest

from tau import cli
from tau.allowlist import AllowList
from tau.config import (
    TauConfig,
    accept_files_enabled,
    set_node_setting,
)
from tau.inbox import Inbox
from tau.node import NodeError, TauNode
from tau.peers import PeerRegistry
from tau.protocol import (
    APP_NAME,
    ASPECT,
    MAX_FILE_BYTES,
    ProtocolError,
    build_file_offer,
    decode,
    parse_file_offer,
    sanitize_file_name,
)


def make_node(tmp_path, name="tester"):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    return TauNode(config, registry=PeerRegistry(config.peers_path))


def sender_identity():
    return RNS.Identity()


def sender_hash(identity):
    return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()


def make_offer(identity, *, name="notes.txt", data=b"hello file", transfer_id="a" * 32):
    return build_file_offer(
        identity,
        transfer_id=transfer_id,
        name=name,
        size=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


def handle_offer(node, identity, offer_bytes):
    raw = node._handle_file("/tau/file", offer_bytes, b"req", identity, 0.0)
    return decode(raw)


# -- file names ------------------------------------------------------------


def test_sanitize_file_name_keeps_only_the_basename():
    assert sanitize_file_name("../evil.txt") == "evil.txt"
    assert sanitize_file_name("/abs/path/photo.png") == "photo.png"
    assert sanitize_file_name("a/b/c.txt") == "c.txt"
    assert sanitize_file_name("plain.txt") == "plain.txt"


def test_sanitize_file_name_rejects_unusable_names():
    assert sanitize_file_name("") == ""
    assert sanitize_file_name(".") == ""
    assert sanitize_file_name("..") == ""
    assert sanitize_file_name("../..") == ""
    assert sanitize_file_name(None) == ""
    assert sanitize_file_name("a" * 300) == "a" * 255


def test_sanitize_file_name_drops_control_characters():
    assert sanitize_file_name("a\x00b\x1bc.txt") == "abc.txt"


# -- signed offers ---------------------------------------------------------


def test_file_offer_roundtrip():
    identity = sender_identity()
    raw = make_offer(identity)
    parsed = parse_file_offer(raw, remote_identity=identity)
    assert parsed["id"] == "a" * 32
    assert parsed["name"] == "notes.txt"
    assert parsed["size"] == len(b"hello file")
    assert parsed["sha256"] == hashlib.sha256(b"hello file").hexdigest()
    assert parsed["from"] == sender_hash(identity)


def test_file_offer_rejects_tampered_payload():
    identity = sender_identity()
    raw = bytearray(make_offer(identity))
    payload = json.loads(bytes(raw))
    payload["size"] = 999
    tampered = json.dumps(payload).encode()
    with pytest.raises(ProtocolError, match="signature does not verify"):
        parse_file_offer(tampered, remote_identity=identity)


def test_file_offer_rejects_wrong_remote_identity():
    identity = sender_identity()
    other = sender_identity()
    with pytest.raises(ProtocolError, match="does not match the link"):
        parse_file_offer(make_offer(identity), remote_identity=other)


def test_file_offer_rejects_oversized_and_empty():
    identity = sender_identity()
    with pytest.raises(ProtocolError, match="exceeds"):
        build_file_offer(
            identity,
            transfer_id="b" * 32,
            name="big.bin",
            size=MAX_FILE_BYTES + 1,
            sha256="0" * 64,
        )
    with pytest.raises(ProtocolError, match="exceeds"):
        build_file_offer(
            identity,
            transfer_id="b" * 32,
            name="empty.bin",
            size=0,
            sha256="0" * 64,
        )


def test_file_offer_build_sanitizes_the_name():
    identity = sender_identity()
    raw = build_file_offer(
        identity,
        transfer_id="b" * 32,
        name="../evil.txt",
        size=4,
        sha256="0" * 64,
    )
    assert parse_file_offer(raw, remote_identity=identity)["name"] == "evil.txt"


def sign_offer(identity, **overrides):
    """Sign a file-offer payload by hand, for parse-side rejection tests."""
    import base64

    from tau.protocol import encode

    payload = {
        "type": "file_offer",
        "v": 1,
        "id": "d" * 32,
        "name": "notes.txt",
        "size": 4,
        "sha256": "0" * 64,
        "sent_at": 1.0,
        "from": sender_hash(identity),
        "from_key": identity.get_public_key().hex(),
    }
    payload.update(overrides)
    payload["sig"] = base64.b64encode(identity.sign(encode(payload))).decode()
    return encode(payload)


def test_file_offer_parse_rejects_an_unsafe_name():
    identity = sender_identity()
    with pytest.raises(ProtocolError, match="not a safe bare file name"):
        parse_file_offer(sign_offer(identity, name="../evil"), remote_identity=identity)


def test_file_offer_rejects_bad_hash():
    identity = sender_identity()
    with pytest.raises(ProtocolError, match="sha256"):
        build_file_offer(
            identity,
            transfer_id="b" * 32,
            name="ok.txt",
            size=4,
            sha256="not-a-digest",
        )


def test_announce_advertises_the_file_capability():
    from tau.protocol import build_announce, parse_announce

    assert "file" in parse_announce(build_announce("alice"))["caps"]


# -- receiver policy -------------------------------------------------------


def test_handle_file_refuses_when_accept_files_is_off(tmp_path):
    node = make_node(tmp_path)
    identity = sender_identity()
    response = handle_offer(node, identity, make_offer(identity))
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"
    assert node._pending_file_offers == {}


def test_handle_file_accepts_when_accept_files_is_on(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    response = handle_offer(node, identity, make_offer(identity))
    assert response == {
        "ok": True,
        "v": 1,
        "accepted": True,
        "id": "a" * 32,
        "name": "notes.txt",
    }
    assert "a" * 32 in node._pending_file_offers


def test_handle_file_allows_allowlisted_sender_without_opt_in(tmp_path):
    node = make_node(tmp_path)
    identity = sender_identity()
    node._allowlist = AllowList(enabled=True, allow={sender_hash(identity)})
    response = handle_offer(node, identity, make_offer(identity))
    assert response["ok"] is True
    assert "a" * 32 in node._pending_file_offers


def test_handle_file_refuses_non_allowlisted_sender_even_when_on(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    other = sender_identity()
    node._allowlist = AllowList(enabled=True, allow={sender_hash(other)})
    response = handle_offer(node, identity, make_offer(identity))
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"


def test_handle_file_rejects_an_invalid_offer(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    response = handle_offer(node, identity, b"not json")
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_offer"


# -- settings --------------------------------------------------------------


def test_accept_files_defaults_off_and_needs_an_explicit_true(tmp_path):
    assert accept_files_enabled(tmp_path) is False
    set_node_setting(tmp_path, "accept_files", "yes")
    assert accept_files_enabled(tmp_path) is False
    set_node_setting(tmp_path, "accept_files", True)
    assert accept_files_enabled(tmp_path) is True
    set_node_setting(tmp_path, "accept_files", False)
    assert accept_files_enabled(tmp_path) is False


# -- resource advertisement / conclusion -----------------------------------


def test_resource_advertised_only_for_a_matching_pending_offer(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))

    assert node._on_resource_advertised(
        SimpleNamespace(get_data_size=lambda: len(b"hello file"))
    )
    assert not node._on_resource_advertised(
        SimpleNamespace(get_data_size=lambda: 12345)
    )
    assert not node._on_resource_advertised(
        SimpleNamespace(get_data_size=lambda: MAX_FILE_BYTES + 1)
    )


def test_resource_concluded_saves_verified_file_and_records_it(tmp_path):
    received = []
    node = make_node(tmp_path)
    node.on_file = received.append
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))

    resource = SimpleNamespace(
        status=RNS.Resource.COMPLETE,
        metadata="a" * 32,
        data=b"hello file",
    )
    node._on_resource_concluded(resource)

    saved = tmp_path / "files" / sender_hash(identity) / "notes.txt"
    assert saved.read_bytes() == b"hello file"
    assert saved.stat().st_mode & 0o777 == 0o600
    assert len(received) == 1
    assert received[0]["kind"] == "file"
    assert received[0]["name"] == "notes.txt"
    assert received[0]["size"] == len(b"hello file")
    assert received[0]["path"] == str(saved)
    entries = node.inbox.list_messages()
    assert len(entries) == 1
    assert entries[0]["kind"] == "file"
    assert entries[0]["sha256"] == hashlib.sha256(b"hello file").hexdigest()
    assert "a" * 32 not in node._pending_file_offers


def test_resource_concluded_reads_file_backed_data(tmp_path):
    """A received Resource keeps its bytes in an open file, not bytes."""
    import io

    received = []
    node = make_node(tmp_path)
    node.on_file = received.append
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))

    resource = SimpleNamespace(
        status=RNS.Resource.COMPLETE,
        metadata="a" * 32,
        data=io.BytesIO(b"hello file"),
    )
    node._on_resource_concluded(resource)

    saved = tmp_path / "files" / sender_hash(identity) / "notes.txt"
    assert saved.read_bytes() == b"hello file"
    assert len(received) == 1


def test_resource_concluded_discards_a_hash_mismatch(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))

    resource = SimpleNamespace(
        status=RNS.Resource.COMPLETE,
        metadata="a" * 32,
        data=b"tampered bytes",
    )
    node._on_resource_concluded(resource)

    saved = tmp_path / "files" / sender_hash(identity) / "notes.txt"
    assert not saved.exists()
    assert node.inbox.list_messages() == []


def test_resource_concluded_ignores_an_unoffered_transfer(tmp_path):
    node = make_node(tmp_path)
    resource = SimpleNamespace(
        status=RNS.Resource.COMPLETE,
        metadata="c" * 32,
        data=b"hello file",
    )
    node._on_resource_concluded(resource)
    assert not (tmp_path / "files").exists()
    assert node.inbox.list_messages() == []


def test_resource_concluded_never_overwrites_an_existing_file(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))
    first = SimpleNamespace(
        status=RNS.Resource.COMPLETE, metadata="a" * 32, data=b"hello file"
    )
    node._on_resource_concluded(first)

    handle_offer(
        node, identity, make_offer(identity, data=b"hello file", transfer_id="b" * 32)
    )
    second = SimpleNamespace(
        status=RNS.Resource.COMPLETE, metadata="b" * 32, data=b"hello file"
    )
    node._on_resource_concluded(second)

    sender_dir = tmp_path / "files" / sender_hash(identity)
    assert (sender_dir / "notes.txt").read_bytes() == b"hello file"
    assert (sender_dir / "notes-2.txt").read_bytes() == b"hello file"


# -- send_file validation --------------------------------------------------


def test_send_file_rejects_missing_and_empty_files(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    with pytest.raises(NodeError, match="no such file"):
        node.send_file("alice", tmp_path / "missing.bin")
    empty = tmp_path / "empty.bin"
    empty.write_bytes(b"")
    with pytest.raises(NodeError, match="outside the"):
        node.send_file("alice", empty)


def test_send_file_rejects_oversized_files(tmp_path, monkeypatch):
    monkeypatch.setattr("tau.node.MAX_FILE_BYTES", 10)
    node = make_node(tmp_path)
    big = tmp_path / "big.bin"
    big.write_bytes(b"x" * 11)
    with pytest.raises(NodeError, match="outside the"):
        node.send_file("alice", big)


# -- inbox file records ----------------------------------------------------


def test_inbox_keeps_file_records_and_drops_corrupt_ones(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": "b" * 32,
            "name": "notes.txt",
            "size": 10,
            "sha256": "0" * 64,
            "path": str(tmp_path / "files" / "notes.txt"),
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "a" * 32,
        }
    )
    assert inbox.list_messages()[0]["kind"] == "file"

    raw = json.loads((tmp_path / "inbox.json").read_text())
    raw["messages"].append(
        {"kind": "file", "name": "../evil", "size": 1, "sha256": "0" * 64}
    )
    (tmp_path / "inbox.json").write_text(json.dumps(raw))
    reloaded = Inbox(tmp_path / "inbox.json")
    assert len(reloaded) == 1
    assert reloaded.list_messages()[0]["kind"] == "file"


# -- CLI -------------------------------------------------------------------


def test_parser_send_file_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["send-file", "--to", "alice", "photo.png", "--timeout", "60"]
    )
    assert args.command == "send-file"
    assert args.to == "alice"
    assert str(args.path) == "photo.png"
    assert args.timeout == 60.0


def test_parser_config_accept_files():
    parser = cli.build_parser()
    args = parser.parse_args(["config", "accept-files", "on"])
    assert args.command == "config"
    assert args.config_command == "accept-files"
    assert args.state == "on"


def test_cmd_config_toggles_accept_files(tmp_path):
    parser = cli.build_parser()
    args = parser.parse_args(
        ["config", "accept-files", "on", "--data-dir", str(tmp_path)]
    )
    assert cli.cmd_config(args) == 0
    assert accept_files_enabled(tmp_path) is True
    args = parser.parse_args(
        ["config", "accept-files", "off", "--data-dir", str(tmp_path)]
    )
    assert cli.cmd_config(args) == 0
    assert accept_files_enabled(tmp_path) is False


def test_cmd_messages_shows_file_entries(tmp_path, capsys):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": "b" * 32,
            "name": "notes.txt",
            "size": 10,
            "sha256": "a" * 64,
            "path": str(tmp_path / "files" / "notes.txt"),
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "a" * 32,
        }
    )
    parser = cli.build_parser()
    args = parser.parse_args(["messages", "--data-dir", str(tmp_path)])
    assert cli.cmd_messages(args) == 0
    out = capsys.readouterr().out
    assert "file: notes.txt (10 bytes" in out
    assert "saved to" in out

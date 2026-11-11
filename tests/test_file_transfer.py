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
from tau.node import NodeError, TauNode, _create_unique_file
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


# The test receiver is never started, so it has no destination hash of
# its own; offers default to this stand-in recipient.
RECIPIENT_HASH = "e" * 32


def make_offer(
    identity,
    *,
    to=RECIPIENT_HASH,
    name="notes.txt",
    data=b"hello file",
    transfer_id="a" * 32,
):
    return build_file_offer(
        identity,
        transfer_id=transfer_id,
        to=to,
        name=name,
        size=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


def set_destination(node, destination_hash):
    """Give an unstarted test node a destination, as if it were serving."""
    node.destination = SimpleNamespace(hexhash=destination_hash)


def sender_link(identity):
    """A fake link authenticated as *identity* (or unidentified)."""
    remote = identity if identity is not None else None
    return SimpleNamespace(get_remote_identity=lambda: remote)


def handle_offer(node, identity, offer_bytes, *, remote="sender"):
    """Deliver *offer_bytes* to *node* as if from *identity*.

    ``remote`` selects the identity authenticated on the link:
    ``"sender"`` (the signer), another identity (a replay), or
    ``None`` (an unidentified link).
    """
    if remote == "sender":
        remote_identity = identity
    else:
        remote_identity = remote
    raw = node._handle_file(
        "/tau/file", offer_bytes, b"req", remote_identity, 0.0
    )
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


def test_create_unique_file_publishes_complete_file(tmp_path):
    # The file appears under its final name only once its content is
    # complete, and no temporary file or staging directory is left
    # behind.
    target = _create_unique_file(tmp_path, "notes.txt", b"hello file")
    assert target == tmp_path / "notes.txt"
    assert target.read_bytes() == b"hello file"
    assert target.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [target]


def test_create_unique_file_numbers_suffixes_and_never_overwrites(tmp_path):
    first = _create_unique_file(tmp_path, "notes.txt", b"one")
    second = _create_unique_file(tmp_path, "notes.txt", b"two")
    third = _create_unique_file(tmp_path, "notes.txt", b"three")
    assert first.name == "notes.txt"
    assert second.name == "notes-2.txt"
    assert third.name == "notes-3.txt"
    assert first.read_bytes() == b"one"
    assert second.read_bytes() == b"two"
    assert third.read_bytes() == b"three"


def test_create_unique_file_skips_symlink_names(tmp_path):
    outside = tmp_path / "outside.txt"
    outside.write_text("secret\n", encoding="utf-8")
    (tmp_path / "notes.txt").symlink_to(outside)
    target = _create_unique_file(tmp_path, "notes.txt", b"hello file")
    assert target.name == "notes-2.txt"
    assert target.read_bytes() == b"hello file"
    assert outside.read_text(encoding="utf-8") == "secret\n"


def test_create_unique_file_works_when_tau_tmp_is_a_received_file(tmp_path):
    # `.tau-tmp` is itself a valid received filename: a file with that
    # name must not break later transfers from the same sender.
    (tmp_path / ".tau-tmp").write_bytes(b"a real received file")
    target = _create_unique_file(tmp_path, "notes.txt", b"hello file")
    assert target == tmp_path / "notes.txt"
    assert target.read_bytes() == b"hello file"
    assert (tmp_path / ".tau-tmp").read_bytes() == b"a real received file"


# -- signed offers ---------------------------------------------------------


def test_file_offer_roundtrip():
    identity = sender_identity()
    raw = make_offer(identity)
    parsed = parse_file_offer(
        raw, remote_identity=identity, expected_recipient=RECIPIENT_HASH
    )
    assert parsed["id"] == "a" * 32
    assert parsed["to"] == RECIPIENT_HASH
    assert parsed["name"] == "notes.txt"
    assert parsed["size"] == len(b"hello file")
    assert parsed["sha256"] == hashlib.sha256(b"hello file").hexdigest()
    assert parsed["from"] == sender_hash(identity)


def test_file_offer_rejects_an_offer_addressed_elsewhere():
    identity = sender_identity()
    with pytest.raises(ProtocolError, match="addressed to another node"):
        parse_file_offer(
            make_offer(identity),
            remote_identity=identity,
            expected_recipient="f" * 32,
        )


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
            to=RECIPIENT_HASH,
            name="big.bin",
            size=MAX_FILE_BYTES + 1,
            sha256="0" * 64,
        )
    with pytest.raises(ProtocolError, match="exceeds"):
        build_file_offer(
            identity,
            transfer_id="b" * 32,
            to=RECIPIENT_HASH,
            name="empty.bin",
            size=0,
            sha256="0" * 64,
        )


def test_file_offer_build_sanitizes_the_name():
    identity = sender_identity()
    raw = build_file_offer(
        identity,
        transfer_id="b" * 32,
        to=RECIPIENT_HASH,
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
        "to": RECIPIENT_HASH,
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
            to=RECIPIENT_HASH,
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


def test_handle_file_rejects_an_unidentified_link(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    response = handle_offer(node, identity, make_offer(identity), remote=None)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_offer"
    assert node._pending_file_offers == {}


def test_handle_file_rejects_a_replayed_offer(tmp_path):
    # Mallory captured Bob's signed offer and replays it over her own
    # link: the signature is genuinely Bob's, but the link says
    # Mallory, so the offer must not be honored as Bob's.
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    bob = sender_identity()
    mallory = sender_identity()
    response = handle_offer(
        node, bob, make_offer(bob), remote=mallory
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_offer"
    assert node._pending_file_offers == {}


def test_handle_file_rejects_an_offer_addressed_elsewhere(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    set_destination(node, "f" * 32)
    identity = sender_identity()
    response = handle_offer(node, identity, make_offer(identity))
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_offer"
    assert node._pending_file_offers == {}


def test_handle_file_accepts_an_offer_addressed_to_this_node(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    set_destination(node, RECIPIENT_HASH)
    identity = sender_identity()
    response = handle_offer(node, identity, make_offer(identity))
    assert response["ok"] is True
    assert "a" * 32 in node._pending_file_offers


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
    link = sender_link(identity)

    assert node._on_resource_advertised(
        SimpleNamespace(get_data_size=lambda: len(b"hello file")), link
    )
    assert not node._on_resource_advertised(
        SimpleNamespace(get_data_size=lambda: 12345), link
    )
    assert not node._on_resource_advertised(
        SimpleNamespace(get_data_size=lambda: MAX_FILE_BYTES + 1), link
    )


def test_resource_advertised_rejects_other_senders_and_unidentified_links(
    tmp_path,
):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))
    advertisement = SimpleNamespace(get_data_size=lambda: len(b"hello file"))

    assert not node._on_resource_advertised(
        advertisement, sender_link(sender_identity())
    )
    assert not node._on_resource_advertised(advertisement, sender_link(None))
    assert not node._on_resource_advertised(advertisement)


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
    node._on_resource_concluded(resource, sender_link(identity))

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
    node._on_resource_concluded(resource, sender_link(identity))

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
    node._on_resource_concluded(resource, sender_link(identity))

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
    node._on_resource_concluded(resource, sender_link(sender_identity()))
    assert not (tmp_path / "files").exists()
    assert node.inbox.list_messages() == []


def test_resource_concluded_ignores_a_wrong_sender_link(tmp_path):
    # A Resource arriving on a link authenticated as someone other
    # than the offeror must not consume the offer or save the file.
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))

    resource = SimpleNamespace(
        status=RNS.Resource.COMPLETE,
        metadata="a" * 32,
        data=b"hello file",
    )
    node._on_resource_concluded(resource, sender_link(sender_identity()))
    assert not (tmp_path / "files").exists()
    assert node.inbox.list_messages() == []
    # The offer survives: the real sender can still deliver it.
    assert "a" * 32 in node._pending_file_offers


def test_resource_concluded_never_overwrites_an_existing_file(tmp_path):
    node = make_node(tmp_path)
    set_node_setting(tmp_path, "accept_files", True)
    identity = sender_identity()
    handle_offer(node, identity, make_offer(identity, data=b"hello file"))
    first = SimpleNamespace(
        status=RNS.Resource.COMPLETE, metadata="a" * 32, data=b"hello file"
    )
    node._on_resource_concluded(first, sender_link(identity))

    handle_offer(
        node, identity, make_offer(identity, data=b"hello file", transfer_id="b" * 32)
    )
    second = SimpleNamespace(
        status=RNS.Resource.COMPLETE, metadata="b" * 32, data=b"hello file"
    )
    node._on_resource_concluded(second, sender_link(identity))

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


def test_send_file_never_loads_a_file_with_unbounded_reads(
    tmp_path, monkeypatch
):
    """An oversized file is refused without an unbounded read.

    The sender reads at most one byte past the limit, so a huge
    file cannot exhaust memory; this guards the shape of that fix
    by asserting the unbounded ``read_bytes`` path is never taken.
    """
    from pathlib import Path as _Path

    monkeypatch.setattr("tau.node.MAX_FILE_BYTES", 10)
    reads = []
    real_read_bytes = _Path.read_bytes

    def tracking_read_bytes(self):
        reads.append(str(self))
        return real_read_bytes(self)

    monkeypatch.setattr(_Path, "read_bytes", tracking_read_bytes)
    node = make_node(tmp_path)
    big = tmp_path / "big.bin"
    big.write_bytes(b"x" * 11)
    with pytest.raises(NodeError, match="outside the"):
        node.send_file("alice", big)
    assert reads == []


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

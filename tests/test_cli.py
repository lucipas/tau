from __future__ import annotations

import json
import time
import os
import stat
from types import SimpleNamespace

import RNS
import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.identity import destination_hash, load_identity
from tau.inbox import Inbox
from tau.mailbox import Mailbox
from tau.node import InstanceLock
from tau.peers import PeerRegistry


def test_parser_accepts_global_options_before_and_after_command():
    parser = cli.build_parser()

    before = parser.parse_args(["--json", "--data-dir", "/tmp/a", "init"])
    assert before.json_output is True
    assert str(before.data_dir) == "/tmp/a"

    after = parser.parse_args(["init", "--json", "--data-dir", "/tmp/b"])
    assert after.json_output is True
    assert str(after.data_dir) == "/tmp/b"


def test_parser_send_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["send", "--to", "alice", "hello world"])
    assert args.command == "send"
    assert args.to == "alice"
    assert args.message == "hello world"
    assert args.timeout == 20.0


def test_parser_serve_http_options():
    parser = cli.build_parser()

    default = parser.parse_args(["serve", "--http"])
    assert default.http == "127.0.0.1:8787"
    assert default.web_dir is None
    assert default.http_allow_remote is False

    explicit = parser.parse_args(
        ["serve", "--http", "0.0.0.0:9000", "--http-allow-remote"]
    )
    assert explicit.http == "0.0.0.0:9000"
    assert explicit.http_allow_remote is True


def test_parser_requires_a_command():
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_init_and_id_end_to_end(tmp_path, capsys):
    data_dir = tmp_path / "node"
    code = cli.main(
        [
            "init",
            "--data-dir",
            str(data_dir),
            "--name",
            "alice",
            "--tcp-listen",
            "127.0.0.1:4343",
            "--no-auto-interface",
            "--json",
        ]
    )
    assert code == 0
    init_payload = json.loads(capsys.readouterr().out)
    assert init_payload["name"] == "alice"
    assert init_payload["config_written"] is True
    assert len(init_payload["destination"]) == 32

    config_text = (data_dir / "reticulum" / "config").read_text()
    assert "type = TCPServerInterface" in config_text
    assert "listen_port = 4343" in config_text
    assert "AutoInterface" not in config_text

    identity = load_identity(data_dir / "identity")
    assert identity is not None
    assert destination_hash(identity).hex() == init_payload["destination"]

    code = cli.main(["id", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    id_payload = json.loads(capsys.readouterr().out)
    assert id_payload["destination"] == init_payload["destination"]
    assert id_payload["name"] == "alice"


def test_init_keeps_existing_config_without_force(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert cli.main(["init", "--data-dir", str(data_dir), "--json"]) == 0
    capsys.readouterr()
    config_path = data_dir / "reticulum" / "config"
    original = config_path.read_text()

    assert (
        cli.main(
            [
                "init",
                "--data-dir",
                str(data_dir),
                "--tcp-listen",
                "4344",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["config_written"] is False
    assert config_path.read_text() == original


def test_id_without_identity_returns_error(tmp_path, capsys):
    code = cli.main(["id", "--data-dir", str(tmp_path / "missing")])
    assert code == 1
    captured = capsys.readouterr()
    assert "run `tau init` first" in captured.err


def _init_node(tmp_path, dirname, name):
    data_dir = tmp_path / dirname
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", name, "--json"]) == 0
    )
    return data_dir


def _id_destination(data_dir, capsys):
    assert cli.main(["id", "--data-dir", str(data_dir), "--json"]) == 0
    return json.loads(capsys.readouterr().out)["destination"]


def test_parser_id_export_import_subcommands():
    parser = cli.build_parser()

    export = parser.parse_args(["id", "export", "--to", "/tmp/backup"])
    assert export.func is cli.cmd_id_export
    assert str(export.to) == "/tmp/backup"
    assert export.force is False

    restore = parser.parse_args(["id", "import", "--from", "/tmp/backup", "--force"])
    assert restore.func is cli.cmd_id_import
    assert str(restore.from_path) == "/tmp/backup"
    assert restore.force is True

    show = parser.parse_args(["id"])
    assert show.func is cli.cmd_id
    assert show.id_command is None


def test_id_export_import_round_trip(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    destination_a = _id_destination(node_a, capsys)

    backup = tmp_path / "alice.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_a), "--json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["destination"] == destination_a
    assert payload["path"] == str(backup)
    assert stat.S_IMODE(os.stat(backup).st_mode) == 0o600

    # A fresh node imports the backup and takes over the same address.
    node_b = tmp_path / "b"
    assert (
        cli.main(
            ["id", "import", "--from", str(backup), "--data-dir", str(node_b), "--json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["destination"] == destination_a
    assert _id_destination(node_b, capsys) == destination_a


def test_id_export_refuses_existing_backup_without_force(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    backup = tmp_path / "backup"
    assert (
        cli.main(["id", "export", "--to", str(backup), "--data-dir", str(node)]) == 0
    )
    capsys.readouterr()

    code = cli.main(["id", "export", "--to", str(backup), "--data-dir", str(node)])
    assert code == 1
    assert "already exists" in capsys.readouterr().err
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node), "--force"]
        )
        == 0
    )


def test_id_export_without_identity_returns_error(tmp_path, capsys):
    code = cli.main(
        [
            "id",
            "export",
            "--to",
            str(tmp_path / "backup"),
            "--data-dir",
            str(tmp_path / "missing"),
        ]
    )
    assert code == 1
    assert "run `tau init` first" in capsys.readouterr().err


def test_id_export_to_identity_file_refused(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    code = cli.main(
        ["id", "export", "--to", str(node / "identity"), "--data-dir", str(node)]
    )
    assert code == 1
    assert "itself" in capsys.readouterr().err


def test_id_export_leaves_existing_parent_permissions_alone(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    shared = tmp_path / "shared"
    shared.mkdir()
    os.chmod(shared, 0o755)
    backup = shared / "alice.identity"
    assert (
        cli.main(["id", "export", "--to", str(backup), "--data-dir", str(node)]) == 0
    )
    assert stat.S_IMODE(os.stat(shared).st_mode) == 0o755
    assert stat.S_IMODE(os.stat(backup).st_mode) == 0o600

    # A missing parent is still created privately.
    nested = tmp_path / "new" / "deep" / "alice.identity"
    assert (
        cli.main(["id", "export", "--to", str(nested), "--data-dir", str(node)]) == 0
    )
    assert stat.S_IMODE(os.stat(nested.parent).st_mode) == 0o700


def test_id_export_refuses_dangling_symlink_without_force(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    backup = tmp_path / "backup"
    os.symlink(tmp_path / "does-not-exist", backup)
    assert not backup.exists()
    assert os.path.lexists(backup)

    code = cli.main(["id", "export", "--to", str(backup), "--data-dir", str(node)])
    assert code == 1
    assert "already exists" in capsys.readouterr().err
    assert os.path.islink(backup)

    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node), "--force"]
        )
        == 0
    )
    assert not os.path.islink(backup)
    assert backup.is_file()


def test_id_import_rejects_non_identity_file(tmp_path, capsys):
    bad = tmp_path / "bad"
    bad.write_text("not an identity", encoding="utf-8")
    code = cli.main(
        ["id", "import", "--from", str(bad), "--data-dir", str(tmp_path / "fresh")]
    )
    assert code == 1
    assert "not a valid identity backup" in capsys.readouterr().err


def test_id_import_missing_backup_returns_error(tmp_path, capsys):
    code = cli.main(
        [
            "id",
            "import",
            "--from",
            str(tmp_path / "nope"),
            "--data-dir",
            str(tmp_path / "fresh"),
        ]
    )
    assert code == 1
    assert "no identity backup" in capsys.readouterr().err


def test_id_import_refuses_existing_identity_without_force(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    backup = tmp_path / "alice.identity"
    assert (
        cli.main(["id", "export", "--to", str(backup), "--data-dir", str(node_a)])
        == 0
    )
    capsys.readouterr()
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_b = _id_destination(node_b, capsys)

    code = cli.main(["id", "import", "--from", str(backup), "--data-dir", str(node_b)])
    assert code == 1
    assert "already exists" in capsys.readouterr().err
    assert _id_destination(node_b, capsys) == destination_b

    assert (
        cli.main(
            ["id", "import", "--from", str(backup), "--data-dir", str(node_b), "--force"]
        )
        == 0
    )
    capsys.readouterr()
    assert _id_destination(node_b, capsys) != destination_b


def test_id_import_refuses_when_node_serving(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    backup = tmp_path / "alice.identity"
    assert (
        cli.main(["id", "export", "--to", str(backup), "--data-dir", str(node_a)])
        == 0
    )
    capsys.readouterr()
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()

    config_b = TauConfig.resolve(data_dir=node_b)
    holder = InstanceLock(config_b.instance_lock_path, config_b.data_dir)
    holder.acquire()
    try:
        code = cli.main(
            [
                "id",
                "import",
                "--from",
                str(backup),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
    finally:
        holder.release()
    assert code == 1
    assert "already serving" in capsys.readouterr().err


def test_peers_on_empty_registry(tmp_path, capsys):
    code = cli.main(["peers", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    assert "No peers known" in capsys.readouterr().out


def test_parser_messages_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["messages", "--peer", "alice", "--limit", "5"])
    assert args.command == "messages"
    assert args.peer == "alice"
    assert args.limit == 5

    defaults = parser.parse_args(["messages"])
    assert defaults.peer is None
    assert defaults.limit == 30


def _seed_messages(tmp_path):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(sender_hash, sender.get_public_key(), name="alice", now=100.0)
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "first hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-1",
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "second hello",
            "sent_at": 11.0,
            "received_at": 21.0,
            "id": "m-2",
        }
    )
    return data_dir, sender_hash


def test_messages_lists_newest_first(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "second hello" in out
    assert "first hello" in out
    assert out.index("second hello") < out.index("first hello")
    assert "alice" in out


def test_messages_json_and_peer_filter(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert [item["id"] for item in payload["messages"]] == ["m-2", "m-1"]

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--peer", "alice", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--peer", sender_hash.hex()[:8], "--limit", "1", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["id"] == "m-2"


def test_messages_empty_and_unknown_peer(tmp_path, capsys):
    code = cli.main(["messages", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    assert "No messages received yet" in capsys.readouterr().out

    _, sender_hash = _seed_messages(tmp_path)
    code = cli.main(
        ["messages", "--data-dir", str(tmp_path / "node"), "--peer", "nobody"]
    )
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err

    code = cli.main(["messages", "--data-dir", str(tmp_path / "node"), "--limit", "-1"])
    assert code == 1
    assert "must not be negative" in capsys.readouterr().err


def test_messages_peer_filter_for_unregistered_sender(tmp_path, capsys):
    """--peer works by hash/prefix even when the sender left the registry."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "unknown",
            "destination": "abcd1111111111111111111111111111",
            "text": "from a stranger",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "u-1",
        }
    )
    inbox.append(
        {
            "from": "unknown",
            "destination": "abcd2222222222222222222222222222",
            "text": "from another stranger",
            "sent_at": 10.0,
            "received_at": 21.0,
            "id": "u-2",
        }
    )

    code = cli.main(
        [
            "messages",
            "--data-dir",
            str(data_dir),
            "--peer",
            "abcd1111111111111111111111111111",
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["u-1"]

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--peer", "abcd1", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["u-1"]

    code = cli.main(["messages", "--data-dir", str(data_dir), "--peer", "abcd"])
    assert code == 1
    assert "match the hash prefix" in capsys.readouterr().err

    # A well-formed hash with no stored messages filters to an empty list.
    code = cli.main(
        [
            "messages",
            "--data-dir",
            str(data_dir),
            "--peer",
            "ff" * 16,
            "--json",
        ]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0


def test_messages_shared_prefix_is_ambiguous(tmp_path, capsys):
    """A prefix shared by a registered peer and an inbox sender is rejected."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        bytes.fromhex("abcd1111111111111111111111111111"),
        b"\x02" * 64,
        name="carol",
        now=100.0,
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "carol",
            "destination": "abcd1111111111111111111111111111",
            "text": "from the registered peer",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "r-1",
        }
    )
    inbox.append(
        {
            "from": "unknown",
            "destination": "abcd2222222222222222222222222222",
            "text": "from the unregistered sender",
            "sent_at": 10.0,
            "received_at": 21.0,
            "id": "r-2",
        }
    )

    code = cli.main(["messages", "--data-dir", str(data_dir), "--peer", "abcd"])
    assert code == 1
    assert "2 senders match the hash prefix" in capsys.readouterr().err

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--peer", "abcd1", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["r-1"]

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--peer", "abcd2", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["r-2"]


def test_messages_out_of_range_timestamp(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "ab" * 16,
            "text": "from the far future",
            "sent_at": 10.0,
            "received_at": 10**15,
            "id": "far-1",
        }
    )

    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "from the far future" in out
    assert "unknown time" in out
# -- outbox held / drop-held ---------------------------------------------------


def _hold_for_cli(tmp_path, entries):
    """Hold *entries* of (message_id, destination, name) in a fresh data dir."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    box = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    for message_id, destination, name in entries:
        box.hold(
            to=destination,
            blob=b"sealed-bytes",
            message_id=message_id,
            expires_at=time.time() + 3600,
        )
        if name is not None:
            registry.upsert(
                destination,
                RNS.Identity().get_public_key(),
                name=name,
            )
    registry.save()
    return data_dir


# -- peers --exchange ------------------------------------------------------

def test_parser_peers_exchange_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["peers", "--exchange", "--to", "bob"])
    assert args.command == "peers"
    assert args.exchange is True
    assert args.to == "bob"

    plain = parser.parse_args(["peers"])
    assert plain.exchange is False
    assert plain.to is None


class _FakeExchangeNode:
    instances = []

    def __init__(self, config, name=None):
        self.config = config
        self.peer = SimpleNamespace(hexhash="aa" * 16, display_name="bob")
        self.registry = SimpleNamespace(all=lambda: [self.peer])
        self.started = False
        type(self).instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.started = False

    def resolve_peer(self, target):
        return self.peer

    def exchange_peers(self, peer):
        return {
            "source": peer.hexhash,
            "source_name": peer.display_name,
            "added": 2,
            "updated": 1,
            "skipped": 0,
        }


def test_peers_exchange_reports_what_was_learned(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(cli, "TauNode", _FakeExchangeNode)
    _FakeExchangeNode.instances.clear()
    code = cli.main(
        ["peers", "--exchange", "--data-dir", str(tmp_path / "node"), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["added"] == 2
    assert payload["updated"] == 1
    assert payload["peers"] == 1
    assert payload["failed"] == 0
    assert payload["exchanged"][0]["source"] == "aa" * 16
    assert _FakeExchangeNode.instances[0].started is False


def test_peers_exchange_failure_is_reported_not_fatal(
    tmp_path, capsys, monkeypatch
):
    class _FailingNode(_FakeExchangeNode):
        def exchange_peers(self, peer):
            raise cli.NodeError("no path to peer")

    monkeypatch.setattr(cli, "TauNode", _FailingNode)
    code = cli.main(["peers", "--exchange", "--data-dir", str(tmp_path / "node")])
    assert code == cli._EXIT_ERROR
    out = capsys.readouterr().out
    assert "failed" in out
    assert "0 new peer(s)" in out
def test_parser_outbox_subcommands():
    parser = cli.build_parser()

    bare = parser.parse_args(["outbox"])
    assert bare.command == "outbox"
    assert bare.outbox_command is None
    assert bare.func is cli.cmd_outbox

    listed = parser.parse_args(["outbox", "list"])
    assert listed.outbox_command == "list"
    assert listed.func is cli.cmd_outbox

    cancelled = parser.parse_args(["outbox", "cancel", "abcdef12"])
    assert cancelled.outbox_command == "cancel"
    assert cancelled.target == "abcdef12"
    assert cancelled.func is cli.cmd_outbox_cancel

    retried = parser.parse_args(["outbox", "retry", "abcdef12"])
    assert retried.outbox_command == "retry"
    assert retried.target == "abcdef12"
    assert retried.func is cli.cmd_outbox_retry

    retried_all = parser.parse_args(["outbox", "retry"])
    assert retried_all.outbox_command == "retry"
    assert retried_all.target is None
    assert retried_all.func is cli.cmd_outbox_retry


def test_outbox_held_empty(tmp_path, capsys):
    data_dir = tmp_path / "node"
    code = cli.main(["outbox", "held", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Not holding any sealed messages" in capsys.readouterr().out


def test_outbox_held_lists_envelopes(tmp_path, capsys):
    bob = b"\xbb" * 16
    data_dir = _hold_for_cli(
        tmp_path,
        [
            ("a" * 32, bob, "bob"),
            ("shared-alpha", b"\xaa" * 16, None),
        ],
    )
    code = cli.main(["outbox", "held", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "aaaaaaaa" in out
    assert "bob" in out
    assert "shared-a" in out
    assert "2 sealed message(s) held" in out

    code = cli.main(["outbox", "held", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert payload["held"][0]["to_name"] == "bob"
    assert payload["held"][0]["blob_bytes"] == len(b"sealed-bytes")
    # The sealed blob itself must never be printed.
    assert "blob" not in payload["held"][0]
    assert "sealed-bytes" not in json.dumps(payload)


def test_outbox_drop_held_by_prefix(tmp_path, capsys):
    data_dir = _hold_for_cli(tmp_path, [("a" * 32, b"\xbb" * 16, "bob")])
    code = cli.main(
        ["outbox", "drop-held", "aaaaaaaa", "--data-dir", str(data_dir)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "dropped aaaaaaaa" in out
    assert "bob" in out
    assert "already fetched" in out
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    assert Mailbox(config.mailbox_path).held_list() == []


def test_outbox_drop_held_json_includes_delivery_caveat(tmp_path, capsys):
    """The already-fetched caveat is in the JSON result too.

    Regression test for the CodeRabbit Minor on PR #59: the text
    output printed the caveat but ``--json`` omitted it, so an
    operator reading the machine output could miss that a fetched
    copy may still be delivered.
    """
    data_dir = _hold_for_cli(tmp_path, [("a" * 32, b"\xbb" * 16, "bob")])
    code = cli.main(
        ["outbox", "drop-held", "aaaaaaaa", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dropped"]["id"] == "a" * 32
    assert "already fetched" in payload["dropped"]["note"]


def test_outbox_drop_held_unknown_and_ambiguous(tmp_path, capsys):
    data_dir = _hold_for_cli(
        tmp_path,
        [
            ("shared-alpha", b"\xbb" * 16, None),
            ("shared-beta", b"\xbb" * 16, None),
        ],
    )
    code = cli.main(
        ["outbox", "drop-held", "no-such", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "unknown held message" in capsys.readouterr().err

    code = cli.main(
        ["outbox", "drop-held", "shared-", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "2 held messages match prefix" in capsys.readouterr().err


def test_outbox_held_escapes_control_characters_in_ids(tmp_path, capsys):
    data_dir = _hold_for_cli(tmp_path, [("evil\x1b[31mred\nid", b"\xbb" * 16, None)])
    code = cli.main(["outbox", "held", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "\n" not in out.splitlines()[1]
    assert "evil" in out

    # JSON keeps the raw id for machines; only human output is cleaned.
    code = cli.main(["outbox", "held", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["held"][0]["id"] == "evil\x1b[31mred\nid"


def test_outbox_drop_held_reports_vanished_envelope(tmp_path, capsys, monkeypatch):
    data_dir = _hold_for_cli(tmp_path, [("a" * 32, b"\xbb" * 16, "bob")])
    # The envelope is gone by removal time (fetch ack / prune in another
    # process): the command must say so instead of reporting a drop.
    monkeypatch.setattr(Mailbox, "drop_held", lambda self, *a, **k: False)
    code = cli.main(
        ["outbox", "drop-held", "aaaaaaaa", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "no longer present" in capsys.readouterr().err


def _seed_outbox(data_dir):
    from tau.config import TauConfig
    from tau.mailbox import Mailbox

    config = TauConfig.resolve(data_dir=data_dir)
    box = Mailbox(config.mailbox_path)
    first = box.enqueue(
        to=b"\xbb" * 16, text="wrong peer", message_id="aa" + "11" * 15
    )
    second = box.enqueue(
        to=b"\xbb" * 16, text="keep me", message_id="bb" + "22" * 15
    )
    return config, box, first, second


def test_outbox_cancel_by_prefix_removes_only_that_entry(tmp_path, capsys):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config, _, first, second = _seed_outbox(data_dir)

    code = cli.main(
        ["outbox", "cancel", "--data-dir", str(data_dir), first.id[:8]]
    )
    assert code == 0
    assert "cancelled aa111111" in capsys.readouterr().out
    assert [e.id for e in Mailbox(config.mailbox_path).outbox_list()] == [
        second.id
    ]


def test_outbox_cancel_json_and_unknown(tmp_path, capsys):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config, _, first, _ = _seed_outbox(data_dir)

    code = cli.main(
        ["outbox", "cancel", "--data-dir", str(data_dir), "--json", first.id]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cancelled"]["id"] == first.id
    assert payload["cancelled"]["text"] == "wrong peer"

    code = cli.main(
        ["outbox", "cancel", "--data-dir", str(data_dir), "ff" * 16]
    )
    assert code == 1
    assert "unknown queued message" in capsys.readouterr().err


def test_outbox_cancel_ambiguous_prefix_is_an_error(tmp_path, capsys):
    from tau.config import TauConfig
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir)
    box = Mailbox(config.mailbox_path)
    box.enqueue(to=b"\xbb" * 16, text="one", message_id="aa" + "11" * 15)
    box.enqueue(to=b"\xbb" * 16, text="two", message_id="aa" + "22" * 15)

    code = cli.main(["outbox", "cancel", "--data-dir", str(data_dir), "aa"])
    assert code == 1
    assert "2 queued messages match prefix" in capsys.readouterr().err
    assert len(Mailbox(config.mailbox_path).outbox_list()) == 2


class _FakeRetryNode:
    """Stub TauNode for `tau outbox retry`: records the flush call and,
    when ``deliver`` is set, removes every matching queued entry from
    the shared mailbox file to simulate a successful delivery (firing
    the node's delivery callback, as a real delivery would).  When
    ``vanish`` is set, matching entries disappear without any delivery
    callback, as a concurrent cancel would remove them.  When
    ``deposit`` is set, the remaining entries gain a holder."""

    instances: list = []
    summary = {"delivered": 0, "deposited": 0, "received": 0}
    deliver = False
    vanish = False
    deposit = False

    def __init__(self, config, **kwargs):
        from tau.mailbox import Mailbox

        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.on_delivered = kwargs.get("on_delivered")
        self.started = False
        self.start_kwargs: dict = {}
        self.flush_calls: list = []
        _FakeRetryNode.instances.append(self)

    def start(self, **kwargs):
        self.started = True
        self.start_kwargs = kwargs

    def stop(self):
        self.started = False

    def wait_for_mail_rounds(self, timeout=30.0):
        return True

    def flush_outbox(self, **kwargs):
        self.flush_calls.append(kwargs)
        target = kwargs.get("message_id")
        to = kwargs.get("to")
        doomed = []
        for entry in self.mailbox.outbox_list():
            if target is not None:
                if entry.id == target:
                    doomed.append(entry)
            elif to is None or entry.to == to:
                doomed.append(entry)
        if _FakeRetryNode.deliver:
            for entry in doomed:
                self.mailbox.remove_outbox(entry.id)
                if self.on_delivered is not None:
                    self.on_delivered(entry.id)
        elif _FakeRetryNode.vanish:
            for entry in doomed:
                self.mailbox.remove_outbox(entry.id)
        if _FakeRetryNode.deposit:
            # A sealed copy handed to a holder is persisted on the entry.
            for entry in self.mailbox.outbox_list():
                self.mailbox.record_deposit(entry.id, "ab" * 16)
        return dict(_FakeRetryNode.summary)


def _seed_retry_outbox(data_dir):
    """Two queued entries for two different recipients, both registered."""
    from tau.config import TauConfig
    from tau.mailbox import Mailbox
    from tau.peers import PeerRegistry

    config = TauConfig.resolve(data_dir=data_dir)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(b"\xbb" * 16, b"\x02" * 64, name="bob", now=100.0)
    registry.upsert(b"\xcc" * 16, b"\x03" * 64, name="carol", now=100.0)
    registry.save()
    box = Mailbox(config.mailbox_path)
    first = box.enqueue(to=b"\xbb" * 16, text="for bob", message_id="aa" + "11" * 15)
    second = box.enqueue(
        to=b"\xcc" * 16, text="for carol", message_id="bb" + "22" * 15
    )
    return config, box, first, second


def test_outbox_retry_all_ignores_backoff(tmp_path, capsys, monkeypatch):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config, _, _, _ = _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()
    _FakeRetryNode.summary = {"delivered": 2, "deposited": 0, "received": 0}
    _FakeRetryNode.deliver = True

    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["delivered"] == 2
    assert payload["still_queued"] == 0
    assert len(_FakeRetryNode.instances) == 1
    node = _FakeRetryNode.instances[0]
    assert node.started is False
    # No background mail loop: the retry owns the only flush, so the
    # loop's first sweep cannot take the mail lock and make this flush
    # silently report nothing moved.
    assert node.start_kwargs == {"mail_loop": False}
    assert node.flush_calls == [
        {
            "timeout": 6.0,
            "max_peers": None,
            "respect_backoff": False,
            "to": None,
            "message_id": None,
            "wait_for_lock": 60.0,
        }
    ]
    assert Mailbox(config.mailbox_path).outbox_list() == []


def test_outbox_retry_counts_delivery_by_a_concurrent_round(
    tmp_path, capsys, monkeypatch
):
    """The outcome is confirmed by delivery callbacks, not the summary.

    An announce-triggered mail round can deliver a queued message
    before (or instead of) this command's own flush; the flush then
    reports zero deliveries while the node's delivery callback confirms
    the message is gone. The retry must still report it delivered.
    """
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config, _, _, _ = _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()
    # A concurrent round delivered both entries; our own flush moved nothing.
    _FakeRetryNode.summary = {"delivered": 0, "deposited": 0, "received": 0}
    _FakeRetryNode.deliver = True

    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["delivered"] == 2
    assert payload["still_queued"] == 0
    assert Mailbox(config.mailbox_path).outbox_list() == []


def test_outbox_retry_reports_unconfirmed_departures(
    tmp_path, capsys, monkeypatch
):
    """Entries that vanish without a delivery callback are not delivered.

    If another process cancels a queued message mid-retry, the entry
    disappears from the queue but no delivery was confirmed; the retry
    must say so instead of reporting a delivery that never happened.
    """
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config, _, _, _ = _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()
    _FakeRetryNode.summary = {"delivered": 0, "deposited": 0, "received": 0}
    _FakeRetryNode.deliver = False
    monkeypatch.setattr(_FakeRetryNode, "vanish", True)

    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["delivered"] == 0
    assert payload["still_queued"] == 0
    assert payload["left_queue_unconfirmed"] == 2
    assert Mailbox(config.mailbox_path).outbox_list() == []

    _seed_retry_outbox(data_dir)
    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "2 message(s) left the queue without a delivery confirmation" in out


def test_outbox_retry_lock_busy_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()
    _FakeRetryNode.summary = {
        "delivered": 0,
        "deposited": 0,
        "received": 0,
        "lock_busy": True,
    }
    _FakeRetryNode.deliver = False

    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir)])
    assert code == 1
    assert "still running after 60s; nothing was attempted" in capsys.readouterr().err
    node = _FakeRetryNode.instances[0]
    assert node.started is False


def test_outbox_retry_all_reports_still_queued(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()
    # Both entries get a real holder persisted, and the flush summary
    # reports the same two deposits: each must be counted once.
    _FakeRetryNode.summary = {"delivered": 0, "deposited": 2, "received": 0}
    _FakeRetryNode.deliver = False
    monkeypatch.setattr(_FakeRetryNode, "deposit", True)

    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "retried 2 queued message(s): 0 delivered, 2 deposited, 0 received" in out
    assert "2 message(s) still queued" in out


def test_outbox_retry_target_only_contacts_that_recipient(
    tmp_path, capsys, monkeypatch
):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config, _, first, second = _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()
    _FakeRetryNode.summary = {"delivered": 1, "deposited": 0, "received": 0}
    _FakeRetryNode.deliver = True

    code = cli.main(
        ["outbox", "retry", "--data-dir", str(data_dir), "--json", first.id[:8]]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["target"] == first.id
    assert payload["target_delivered"] is True
    node = _FakeRetryNode.instances[0]
    assert node.flush_calls == [
        {
            "timeout": 6.0,
            "max_peers": None,
            "respect_backoff": False,
            "to": b"\xbb" * 16,
            "message_id": first.id,
            "wait_for_lock": 60.0,
        }
    ]
    # Only the targeted entry was delivered; the other stays queued.
    assert [e.id for e in Mailbox(config.mailbox_path).outbox_list()] == [
        second.id
    ]


def test_outbox_retry_target_reports_still_queued(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    _, _, first, _ = _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()
    _FakeRetryNode.summary = {"delivered": 0, "deposited": 0, "received": 0}
    _FakeRetryNode.deliver = False

    code = cli.main(
        ["outbox", "retry", "--data-dir", str(data_dir), first.id[:8]]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "still queued: aa111111 for bob (attempt 0, next try now)" in out


def test_outbox_retry_empty_does_not_start_a_node(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()

    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Outbox is empty" in capsys.readouterr().out
    assert _FakeRetryNode.instances == []

    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    assert json.loads(capsys.readouterr().out)["still_queued"] == 0
    assert _FakeRetryNode.instances == []


def test_outbox_retry_unknown_and_ambiguous(tmp_path, capsys, monkeypatch):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config, _, _, _ = _seed_retry_outbox(data_dir)
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()

    code = cli.main(
        ["outbox", "retry", "--data-dir", str(data_dir), "ff" * 16]
    )
    assert code == 1
    assert "unknown queued message" in capsys.readouterr().err
    assert _FakeRetryNode.instances == []
    assert len(Mailbox(config.mailbox_path).outbox_list()) == 2

    box = Mailbox(config.mailbox_path)
    box.enqueue(to=b"\xbb" * 16, text="third", message_id="aa" + "33" * 15)
    code = cli.main(["outbox", "retry", "--data-dir", str(data_dir), "aa"])
    assert code == 1
    assert "2 queued messages match prefix" in capsys.readouterr().err
    assert _FakeRetryNode.instances == []


def test_outbox_retry_recipient_not_in_registry(tmp_path, capsys, monkeypatch):
    from tau.config import TauConfig
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir)
    box = Mailbox(config.mailbox_path)
    entry = box.enqueue(
        to=b"\xbb" * 16, text="no such peer", message_id="aa" + "11" * 15
    )
    monkeypatch.setattr(cli, "TauNode", _FakeRetryNode)
    _FakeRetryNode.instances.clear()

    code = cli.main(
        ["outbox", "retry", "--data-dir", str(data_dir), entry.id[:8]]
    )
    assert code == 1
    assert "not in the peer registry" in capsys.readouterr().err
    assert _FakeRetryNode.instances == []


def test_serve_stops_node_when_bridge_start_fails_late(tmp_path, monkeypatch):
    """A late console bind failure stops the node (issue #36)."""
    import socket

    from tau.node import NodeError
    from tau.webapi import WebBridge, WebBridgeError

    events = []

    class RecordingNode:
        def __init__(self, config, **kwargs):
            self.config = config
            self.identity = None

        def start(self, *, announce=True, exclusive=False):
            events.append("node.start")

        def stop(self):
            events.append("node.stop")

    def failing_start(self):
        events.append("bridge.start")
        raise WebBridgeError("web console bind raced")

    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html></html>", encoding="utf-8")
    # A real TauNode creates the data dir in start(), but the instance-lock
    # probe runs before that, so serve on a fresh dir expects `tau init`
    # (or an existing dir) to have created it first.
    (tmp_path / "node").mkdir()
    monkeypatch.setattr(cli, "TauNode", RecordingNode)
    monkeypatch.setattr(WebBridge, "start", failing_start)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])

    args = cli.build_parser().parse_args(
        [
            "serve",
            "--data-dir",
            str(tmp_path / "node"),
            "--http",
            f"127.0.0.1:{port}",
            "--web-dir",
            str(web_dir),
        ]
    )
    with pytest.raises(NodeError, match="web console bind raced"):
        cli.cmd_serve(args)

    assert events == ["node.start", "bridge.start", "node.stop"]

from __future__ import annotations

import json
import os
import stat

import pytest

from tau import cli
from tau.config import TauConfig
from tau.identity import destination_hash, load_identity
from tau.node import InstanceLock


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

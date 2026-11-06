from __future__ import annotations

import json

import pytest

from tau import cli
from tau.identity import destination_hash, load_identity


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

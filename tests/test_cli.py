from __future__ import annotations

import json
import time

import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.identity import destination_hash, load_identity
from tau.mailbox import Mailbox
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


def test_peers_on_empty_registry(tmp_path, capsys):
    code = cli.main(["peers", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    assert "No peers known" in capsys.readouterr().out


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


def test_parser_outbox_subcommands():
    parser = cli.build_parser()

    bare = parser.parse_args(["outbox"])
    assert bare.func is cli.cmd_outbox

    listed = parser.parse_args(["outbox", "list"])
    assert listed.func is cli.cmd_outbox

    held = parser.parse_args(["outbox", "held"])
    assert held.func is cli.cmd_outbox_held

    drop = parser.parse_args(["outbox", "drop-held", "abc123"])
    assert drop.func is cli.cmd_outbox_drop_held
    assert drop.target == "abc123"


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

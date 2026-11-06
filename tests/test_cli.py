from __future__ import annotations

import json

import RNS
import pytest

from tau import cli
from tau.config import TauConfig
from tau.identity import destination_hash, load_identity
from tau.inbox import Inbox
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

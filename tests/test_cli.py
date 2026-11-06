from __future__ import annotations

import json
from types import SimpleNamespace

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

from __future__ import annotations

import json
import os
import socket
from types import SimpleNamespace

import pytest

from tau import cli
from tau.config import TauConfig
from tau.inbox import Inbox
from tau.local_send import SendServer, send_server_available
from tau.mailbox import Mailbox
from tau.node import NodeError


class _FakeRunningNode:
    """Stub TauNode for the serving side of `tau send`."""

    def __init__(self, config, behaviour=None, deliver_on_flush=False):
        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.behaviour = behaviour or {}
        self.deliver_on_flush = deliver_on_flush
        self.calls: list = []
        self.sent: list = []
        self.flush_calls: list = []
        self.peer = SimpleNamespace(hexhash="ab" * 16, display_name="alice")

    def resolve_peer(self, target, timeout=10.0):
        return self.peer

    def _target_hash(self, target):
        return bytes.fromhex(self.peer.hexhash)

    def send_message(self, target, text, **kwargs):
        self.calls.append((target, text))
        self.sent.append((target, text, kwargs.get("in_reply_to")))
        mode = self.behaviour.get(target, "delivered")
        if mode == "error":
            raise NodeError(f"no path to {target}")
        if mode == "queued":
            entry = self.mailbox.enqueue(
                to=b"\xbb" * 16,
                text=text,
                message_id=f"{len(self.calls):032x}",
            )
            return {"ok": True, "queued": True, "id": entry.id, "to": entry.to_hex}
        return {"ok": True, "echo": text, "_rtt": 0.05}

    def flush_outbox(self, **kwargs):
        self.flush_calls.append(kwargs)
        if self.deliver_on_flush:
            for entry in self.mailbox.outbox_list():
                self.mailbox.remove_outbox(entry.id)


class _FakeLocalNode:
    """Stub TauNode for the one-shot path, to prove it never starts."""

    instances: list = []

    def __init__(self, config, **kwargs):
        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.started = False
        self.calls: list = []
        _FakeLocalNode.instances.append(self)

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.started = False

    def resolve_peer(self, target, timeout=10.0):
        return SimpleNamespace(hexhash="ab" * 16, display_name="alice")

    def _target_hash(self, target):
        return bytes.fromhex("ab" * 16)

    def send_message(self, target, text, **kwargs):
        self.calls.append((target, text))
        return {"ok": True, "echo": text, "_rtt": 0.05}

    def flush_outbox(self, **kwargs):
        pass


def _seed_reply_conversation(tmp_path):
    """One received message from the peer, for --reply-to resolution."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "ab" * 16,
            "text": "original thought",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "a" * 32,
            "direction": "in",
        }
    )
    return data_dir


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_uses_running_node(tmp_path, capsys, monkeypatch):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config)
    server = SendServer(tmp_path, lambda **request: cli._send_local_message(node, **request))
    try:
        assert os.stat(server.path).st_mode & 0o777 == 0o600
        assert send_server_available(tmp_path)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["send", "--data-dir", str(tmp_path), "--to", "alice", "hello mesh"]) == 0
        assert _FakeLocalNode.instances == []
        assert node.calls == [("alice", "hello mesh")]
        assert "delivered to alice" in capsys.readouterr().out
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_via_running_node_flips_queued_to_delivered(tmp_path, capsys, monkeypatch):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config, behaviour={"alice": "queued"}, deliver_on_flush=True)
    server = SendServer(tmp_path, lambda **request: cli._send_local_message(node, **request))
    try:
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["send", "--data-dir", str(tmp_path), "--to", "alice", "hi"]) == 0
        assert node.flush_calls != []
        assert node.mailbox.outbox_list() == []
        assert "delivered to alice" in capsys.readouterr().out
    finally:
        server.stop()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_via_running_node_reports_server_error(tmp_path, capsys, monkeypatch):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config, behaviour={"alice": "error"})
    server = SendServer(tmp_path, lambda **request: cli._send_local_message(node, **request))
    try:
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["send", "--data-dir", str(tmp_path), "--to", "alice", "hi"]) == 1
        assert _FakeLocalNode.instances == []
        assert "no path to alice" in capsys.readouterr().err
    finally:
        server.stop()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_reply_to_through_running_node(tmp_path, capsys, monkeypatch):
    data_dir = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    node = _FakeRunningNode(config)
    server = SendServer(data_dir, lambda **request: cli._send_local_message(node, **request))
    try:
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        code = cli.main(
            [
                "send",
                "--data-dir",
                str(data_dir),
                "--to",
                "alice",
                "--reply-to",
                "aaaa",
                "--json",
                "seconded, again",
            ]
        )
        assert code == 0
        assert len(node.sent) == 1
        assert node.sent[0][2]["id"] == "a" * 32
        result = json.loads(capsys.readouterr().out)
        assert "_reply_id" not in result
        assert result["echo"] == "seconded, again"
    finally:
        server.stop()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_stale_send_socket_falls_back_to_local_node(tmp_path, capsys, monkeypatch):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    (tmp_path / "send.sock").write_bytes(b"")
    assert not send_server_available(tmp_path)
    _FakeLocalNode.instances.clear()
    monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
    assert cli.main(["send", "--data-dir", str(tmp_path), "--to", "alice", "hi"]) == 0
    node = _FakeLocalNode.instances[0]
    assert node.calls == [("alice", "hi")]
    assert node.started is False
    assert "delivered to alice" in capsys.readouterr().out

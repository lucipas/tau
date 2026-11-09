from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from tau import cli
from tau.config import TauConfig
from tau.inbox import Inbox
from tau.local_send import SendServer, send_server_available
from tau.mailbox import Mailbox
from tau.node import InstanceLock, NodeError


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
            self.mailbox.record_sent(entry.id, to=b"\xbb" * 16, text=text)
            return {"ok": True, "queued": True, "id": entry.id, "to": entry.to_hex}
        return {"ok": True, "echo": text, "_rtt": 0.05}

    def flush_outbox(self, **kwargs):
        self.flush_calls.append(kwargs)
        if self.deliver_on_flush:
            for entry in self.mailbox.outbox_list():
                # The real node records delivery before leaving the outbox.
                self.mailbox.record_delivered(entry.id)
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


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_serve_continues_when_send_server_cannot_bind(tmp_path, caplog, capsys, monkeypatch):
    """A send socket that cannot bind must not stop `tau serve` (CodeRabbit Major)."""
    import tau.local_send

    events = []

    class RecordingNode:
        def __init__(self, config, **kwargs):
            self.config = config
            self.identity = None

        def start(self, *, announce=True, exclusive=False, lxmf=False):
            events.append("node.start")

        def stop(self):
            events.append("node.stop")

        def info(self):
            return {
                "name": "test",
                "destination": "ab" * 16,
                "rns_version": "test",
                "mailbox": {},
                "lxmf": {},
            }

    class FailingSendServer:
        def __init__(self, *args, **kwargs):
            raise OSError("no socket support here")

    (tmp_path / "node").mkdir()
    monkeypatch.setattr(cli, "TauNode", RecordingNode)
    monkeypatch.setattr(tau.local_send, "SendServer", FailingSendServer)

    args = cli.build_parser().parse_args(
        ["serve", "--data-dir", str(tmp_path / "node"), "--duration", "0"]
    )
    assert cli.cmd_serve(args) == 0
    assert events == ["node.start", "node.stop"]
    assert not (tmp_path / "node" / "send.sock").exists()
    assert "Could not start the local send server" in caplog.text
    # The degraded state is visible in the ready banner, not just the log.
    assert "socket unavailable" in capsys.readouterr().out

    # ... and as a flag on the JSON ready event.
    events.clear()
    args = cli.build_parser().parse_args(
        ["serve", "--data-dir", str(tmp_path / "node"), "--duration", "0", "--json"]
    )
    assert cli.cmd_serve(args) == 0
    ready = json.loads(capsys.readouterr().out.strip().splitlines()[0])
    assert ready["event"] == "ready"
    assert ready["send_socket"] is False


def test_instance_lock_free_reads_holder_pid(tmp_path):
    """The probe is ground truth; a PID in the lock file is not trusted."""
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    # No lock file at all.
    assert cli._instance_lock_free(config) is True
    # A dead holder's PID with no lock held: free, via the probe.
    proc = subprocess.Popen(["true"])
    proc.wait()
    config.instance_lock_path.write_text(str(proc.pid))
    assert cli._instance_lock_free(config) is True
    # A live holder blocks.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        assert cli._instance_lock_free(config) is False
    finally:
        lock.release()


def test_instance_lock_free_ignores_dead_pid_when_lock_held(tmp_path):
    """A dead PID in the file must not read as free while locked.

    A serve records its PID just after taking the lock, so the file
    can still name a dead predecessor mid-startup; trusting the PID
    would start a second node on the serving node's identity
    (CodeRabbit Major on PR #190).
    """
    fcntl = pytest.importorskip("fcntl")
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    proc = subprocess.Popen(["true"])
    proc.wait()
    config.instance_lock_path.write_text(str(proc.pid))
    handle = open(config.instance_lock_path, "a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert cli._instance_lock_free(config) is False
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def test_instance_lock_free_ignores_dead_pid_when_windows_lock_held(
    tmp_path, windows_lock
):
    """Same dead-PID case on simulated Windows (msvcrt lock)."""
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    proc = subprocess.Popen(["true"])
    proc.wait()
    config.instance_lock_path.write_text(str(proc.pid))
    handle = open(config.instance_lock_path, "a+", encoding="utf-8")
    try:
        windows_lock.locking(handle.fileno(), windows_lock.LK_NBLCK, 1)
        assert cli._instance_lock_free(config) is False
    finally:
        windows_lock.locking(handle.fileno(), windows_lock.LK_UNLCK, 1)
        handle.close()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_waits_for_transient_lock_holder(tmp_path):
    """A standalone `tau tan post` holds the lock without a send server;
    a send during it proceeds locally once the lock frees (Qodo Warning)."""
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    released = threading.Event()

    def release_later():
        time.sleep(0.2)
        lock.release()
        released.set()

    thread = threading.Thread(target=release_later, daemon=True)
    thread.start()
    try:
        assert cli._send_routes_via_running_node(config) is False
    finally:
        thread.join(timeout=5)
    assert released.is_set()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_fails_closed_when_lock_stays_held(tmp_path, monkeypatch):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    monkeypatch.setattr(cli, "_SEND_ROUTE_WAIT_SECONDS", 0)
    try:
        with pytest.raises(NodeError, match="not accepting sends"):
            cli._send_routes_via_running_node(config)
    finally:
        lock.release()


def test_instance_lock_free_sees_windows_lock_held(tmp_path, windows_lock):
    """The send probe must see a serving node on simulated Windows.

    With a no-op lock the probe always reads free and `tau send`
    starts a second Reticulum instance on the serving node's identity.
    """
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        assert cli._instance_lock_free(config) is False
    finally:
        lock.release()
    assert cli._instance_lock_free(config) is True


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_fails_closed_when_windows_lock_stays_held(
    tmp_path, monkeypatch, windows_lock
):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    monkeypatch.setattr(cli, "_SEND_ROUTE_WAIT_SECONDS", 0)
    try:
        with pytest.raises(NodeError, match="not accepting sends"):
            cli._send_routes_via_running_node(config)
    finally:
        lock.release()

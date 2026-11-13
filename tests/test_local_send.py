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

    def send_retraction(self, target, message_id, **kwargs):
        self.calls.append(("retract", target, message_id))
        if self.behaviour.get("retract") == "error":
            raise NodeError(f"no path to {target}")
        return {"ok": True, "target": message_id, "deleted": True, "id": "r-1"}

    def send_group_retraction(self, group, message_id, **kwargs):
        self.calls.append(
            ("retract_group", group, message_id, kwargs.get("target_sent_at"))
        )
        if self.behaviour.get("retract_group") == "partial":
            return {
                "ok": False,
                "group": group,
                "target": message_id,
                "id": "r-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": "ab" * 16,
                        "to_name": "alice",
                        "ok": False,
                        "error": "offline",
                        "id": "r-1",
                    }
                ],
            }
        return {
            "ok": True,
            "group": group,
            "target": message_id,
            "id": "r-1",
            "cancelled_queued": [],
            "results": [
                {
                    "to": "ab" * 16,
                    "to_name": "alice",
                    "delivered": True,
                    "deleted": True,
                    "id": "r-1",
                }
            ],
        }

    def send_group_edit(self, group, message_id, text, **kwargs):
        self.calls.append(
            ("edit_group", group, message_id, text, kwargs.get("target_sent_at"))
        )
        if self.behaviour.get("edit_group") == "partial":
            return {
                "ok": False,
                "group": group,
                "target": message_id,
                "id": "e-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": "ab" * 16,
                        "to_name": "alice",
                        "ok": False,
                        "error": "offline",
                        "id": "e-1",
                    }
                ],
            }
        return {
            "ok": True,
            "group": group,
            "target": message_id,
            "id": "e-1",
            "cancelled_queued": [],
            "results": [
                {
                    "to": "ab" * 16,
                    "to_name": "alice",
                    "delivered": True,
                    "edited": True,
                    "id": "e-1",
                }
            ],
        }

    def send_edit(self, target, message_id, text, **kwargs):
        self.calls.append(("edit", target, message_id, text))
        return {"ok": True, "target": message_id, "edited": True, "id": "e-1"}

    def flush_outbox(self, **kwargs):
        self.flush_calls.append(kwargs)
        if self.deliver_on_flush:
            for entry in self.mailbox.outbox_list():
                # The real node records delivery before leaving the outbox.
                self.mailbox.record_delivered(entry.id)
                self.mailbox.remove_outbox(entry.id)


class _FakeBridge:
    """Stub WebBridge: records the sent-copy drops the dispatch asks for."""

    def __init__(self):
        self.deleted_wire_ids: list = []
        self.deleted_groups: list = []

    def delete_sent_by_wire_id(self, wire_id):
        self.deleted_wire_ids.append(wire_id)
        return {"ok": True, "deleted": {"id": 1, "wire_id": wire_id}}

    def delete_sent_group(self, group_id, sent_at):
        self.deleted_groups.append((group_id, sent_at))
        return {"ok": True, "deleted_ids": [1, 2]}


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
def test_retract_uses_running_node(tmp_path, capsys, monkeypatch):
    import RNS

    from tau.identity import destination_hash
    from tau.peers import PeerRegistry

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": recipient_hash.hex(),
            "text": "sent hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-9",
            "direction": "out",
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-9", to=recipient_hash, text="sent hello"
    )
    node = _FakeRunningNode(config)
    server = SendServer(
        data_dir, lambda **request: cli._dispatch_local_send(node, **request)
    )
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["retract", "--data-dir", str(data_dir), "m-9"]) == 0
        # No one-shot node started: the serving node carried the retract.
        assert _FakeLocalNode.instances == []
        assert node.calls == [("retract", recipient_hash.hex(), "m-9")]
        assert "retracted" in capsys.readouterr().out
        assert Inbox(config.inbox_path).list_messages(direction="out") == []
        assert Mailbox(config.mailbox_path).sent_list() == []
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_group_retract_uses_running_node(tmp_path, capsys, monkeypatch):
    import RNS

    from tau.identity import destination_hash
    from tau.peers import PeerRegistry

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "group hello",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-10",
            "direction": "out",
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "team",
            "leg_ids": ["m-10"],
            "leg_sent_at": 100.0,
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-10", to=recipient_hash, text="group hello"
    )
    node = _FakeRunningNode(config)
    server = SendServer(
        data_dir, lambda **request: cli._dispatch_local_send(node, **request)
    )
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["retract", "--data-dir", str(data_dir), "m-10"]) == 0
        # No one-shot node started: the serving node carried the retract.
        assert _FakeLocalNode.instances == []
        assert node.calls == [
            ("retract_group", "ab" * 16, "m-10", 100.0)
        ]
        assert "retracted your group message" in capsys.readouterr().out
        assert Inbox(config.inbox_path).list_messages(direction="out") == []
        assert Mailbox(config.mailbox_path).sent_list() == []
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_dispatch_retract_leaves_console_copy_until_drop_op(tmp_path):
    # The retract op no longer drops the console copy: the CLI deletes
    # its inbox copy first and only then asks the serving node to drop
    # the console entry (finding 3 of issue #330). A dispatch retract
    # on its own must leave the console copy alone.
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config)
    bridge = _FakeBridge()
    result = cli._dispatch_local_send(
        node, bridge=bridge, op="retract", target="alice", message_id="m-9"
    )
    assert result["deleted"] is True
    assert bridge.deleted_wire_ids == []
    assert bridge.deleted_groups == []
    result = cli._dispatch_local_send(
        node, bridge=bridge, op="drop_console_copy", message_id="m-9"
    )
    assert result["ok"] is True
    assert bridge.deleted_wire_ids == ["m-9"]
    assert bridge.deleted_groups == []


def test_dispatch_drop_console_copy_without_bridge_is_a_noop(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config)
    result = cli._dispatch_local_send(
        node, op="drop_console_copy", message_id="m-9"
    )
    assert result["ok"] is True
    result = cli._dispatch_local_send(
        node,
        op="drop_console_group",
        group="ab" * 16,
        target_sent_at=100.0,
    )
    assert result["ok"] is True


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_send_running_timeout_bounds_slow_reply(tmp_path):
    # A bounded send_running gives up on a serving node that stalls
    # instead of waiting forever (Qodo informational on PR #332: the
    # console-drop notification must not hang the retract).
    from tau.local_ipc import LocalIPCUnavailable
    from tau.local_send import send_running

    def _slow(**request):
        time.sleep(3)
        return {"ok": True}

    server = SendServer(tmp_path, _slow)
    try:
        assert send_server_available(tmp_path)
        start = time.monotonic()
        with pytest.raises(LocalIPCUnavailable):
            send_running(
                tmp_path, reply_timeout=0.2, op="send", target="alice", text="hi"
            )
        assert time.monotonic() - start < 2.0
    finally:
        server.stop()


def test_drop_console_copy_via_running_node_without_server_is_best_effort(
    tmp_path,
):
    # No serving node answering: the drop is swallowed, never raised —
    # the retract already succeeded on the wire.
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    cli._drop_console_copy_via_running_node(config, message_id="m-9")
    cli._drop_console_copy_via_running_node(
        config, group="ab" * 16, target_sent_at=100.0
    )


def test_dispatch_retract_without_bridge_is_a_noop(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config)
    result = cli._dispatch_local_send(
        node, op="retract", target="alice", message_id="m-9"
    )
    assert result["deleted"] is True


def test_dispatch_retract_failure_leaves_console_copy(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config, behaviour={"retract": "error"})
    bridge = _FakeBridge()
    with pytest.raises(NodeError, match="no path to alice"):
        cli._dispatch_local_send(
            node, bridge=bridge, op="retract", target="alice", message_id="m-9"
        )
    assert bridge.deleted_wire_ids == []
    assert bridge.deleted_groups == []


def test_dispatch_group_retract_leaves_console_legs_until_drop_op(tmp_path):
    # Like the DM retract: the group retract op no longer drops the
    # console legs — the CLI asks for the drop only after its inbox
    # delete succeeds, and only on a full fan-out.
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config)
    bridge = _FakeBridge()
    result = cli._dispatch_local_send(
        node,
        bridge=bridge,
        op="retract_group",
        group="ab" * 16,
        message_id="m-10",
        target_sent_at=100.0,
    )
    assert result["ok"] is True
    assert bridge.deleted_groups == []

    partial = _FakeRunningNode(config, behaviour={"retract_group": "partial"})
    bridge = _FakeBridge()
    result = cli._dispatch_local_send(
        partial,
        bridge=bridge,
        op="retract_group",
        group="ab" * 16,
        message_id="m-10",
        target_sent_at=100.0,
    )
    assert result["ok"] is False
    assert bridge.deleted_groups == []

    result = cli._dispatch_local_send(
        node,
        bridge=bridge,
        op="drop_console_group",
        group="ab" * 16,
        target_sent_at=100.0,
    )
    assert result["ok"] is True
    assert bridge.deleted_groups == [("ab" * 16, 100.0)]


def test_dispatch_group_edit_runs_on_the_serving_node(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config)
    result = cli._dispatch_local_send(
        node,
        op="edit_group",
        group="ab" * 16,
        message_id="m-10",
        text="group fixed",
        target_sent_at=100.0,
    )
    assert result["ok"] is True
    assert node.calls == [
        ("edit_group", "ab" * 16, "m-10", "group fixed", 100.0)
    ]

    partial = _FakeRunningNode(config, behaviour={"edit_group": "partial"})
    result = cli._dispatch_local_send(
        partial,
        op="edit_group",
        group="ab" * 16,
        message_id="m-10",
        text="group fixed",
        target_sent_at=100.0,
    )
    assert result["ok"] is False


def test_dispatch_group_ops_accept_leg_timeout(tmp_path):
    # A fan-out client forwards its per-leg bound as `leg_timeout` in
    # the request (the reply wait stays unbounded, so the operation
    # timeout cannot travel as `timeout` — send_running swallows that
    # keyword).  Both fan-out ops must accept it, or the dispatch
    # raises on the unexpected keyword and the CLI path breaks.
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = _FakeRunningNode(config)
    result = cli._dispatch_local_send(
        node,
        op="edit_group",
        group="ab" * 16,
        message_id="m-10",
        text="group fixed",
        target_sent_at=100.0,
        leg_timeout=5.0,
    )
    assert result["ok"] is True
    result = cli._dispatch_local_send(
        node,
        op="retract_group",
        group="ab" * 16,
        message_id="m-10",
        target_sent_at=100.0,
        leg_timeout=5.0,
    )
    assert result["ok"] is True


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_retract_via_running_node_drops_console_copy(tmp_path, capsys, monkeypatch):
    # End to end: a console-sent DM retracted through the CLI while the
    # console serves leaves the feed, and the persisted log agrees so a
    # restart cannot resurrect it.
    import RNS

    from tau.identity import destination_hash
    from tau.peers import PeerRegistry
    from tau.webapi import WebBridge

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": recipient_hash.hex(),
            "text": "sent hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-9",
            "direction": "out",
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-9", to=recipient_hash, text="sent hello"
    )
    node = _FakeRunningNode(config)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge._append_message(
        direction="out",
        peer=recipient_hash.hex(),
        peer_name="alice",
        text="sent hello",
        sent_at=10.0,
        received_at=20.0,
        status="delivered",
        wire_id="m-9",
    )
    assert len(bridge.messages()) == 1
    server = SendServer(
        data_dir,
        lambda **request: cli._dispatch_local_send(
            node, bridge=bridge, **request
        ),
    )
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["retract", "--data-dir", str(data_dir), "m-9"]) == 0
        assert _FakeLocalNode.instances == []
        assert node.calls == [("retract", recipient_hash.hex(), "m-9")]
        assert "retracted" in capsys.readouterr().out
        assert Inbox(config.inbox_path).list_messages(direction="out") == []
        assert Mailbox(config.mailbox_path).sent_list() == []
        assert bridge.messages() == []
        persisted = json.loads(bridge._messages_path.read_text(encoding="utf-8"))
        assert persisted["messages"] == []
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_retract_via_running_node_keeps_console_copy_when_inbox_delete_fails(
    tmp_path, capsys, monkeypatch
):
    # Regression test (finding 3 of issue #330): the console copy used
    # to be dropped inside the retract op, before the CLI deleted its
    # inbox copy — a failing inbox delete left the console copy gone
    # while the inbox record was restored. The drop now happens only
    # after the inbox delete succeeds, so a failure keeps the console
    # entry (a retry heals both sides).
    import RNS

    from tau.identity import destination_hash
    from tau.inbox import InboxError
    from tau.peers import PeerRegistry
    from tau.webapi import WebBridge

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": recipient_hash.hex(),
            "text": "sent hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-9",
            "direction": "out",
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-9", to=recipient_hash, text="sent hello"
    )
    node = _FakeRunningNode(config)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge._append_message(
        direction="out",
        peer=recipient_hash.hex(),
        peer_name="alice",
        text="sent hello",
        sent_at=10.0,
        received_at=20.0,
        status="delivered",
        wire_id="m-9",
    )
    assert len(bridge.messages()) == 1
    server = SendServer(
        data_dir,
        lambda **request: cli._dispatch_local_send(
            node, bridge=bridge, **request
        ),
    )
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)

        def _failing_delete(self, message_id):
            raise InboxError("disk full")

        monkeypatch.setattr(cli.Inbox, "delete", _failing_delete)
        assert cli.main(["retract", "--data-dir", str(data_dir), "m-9"]) != 0
        assert "local copy could not be deleted" in capsys.readouterr().err
        assert _FakeLocalNode.instances == []
        assert node.calls == [("retract", recipient_hash.hex(), "m-9")]
        # The inbox record is still there — and so is the console copy.
        assert Inbox(config.inbox_path).list_messages(direction="out") != []
        assert len(bridge.messages()) == 1
        assert bridge.messages()[0]["wire_id"] == "m-9"
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_retract_via_running_node_records_marker_before_slow_console_drop(
    tmp_path, capsys, monkeypatch
):
    # Regression test (Qodo informational on PR #332): a serving node
    # that stalls on the console-drop op must not leave the retract
    # unrecorded — the durable marker is written before the
    # notification is sent, and the notification itself is bounded,
    # so the CLI returns promptly and the marker heals the console
    # copy at the next serve start.
    import RNS

    from tau.identity import destination_hash
    from tau.peers import PeerRegistry
    from tau.webapi import WebBridge

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": recipient_hash.hex(),
            "text": "sent hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-9",
            "direction": "out",
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-9", to=recipient_hash, text="sent hello"
    )
    node = _FakeRunningNode(config)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge._append_message(
        direction="out",
        peer=recipient_hash.hex(),
        peer_name="alice",
        text="sent hello",
        sent_at=10.0,
        received_at=20.0,
        status="delivered",
        wire_id="m-9",
    )
    assert len(bridge.messages()) == 1

    def _stalling_dispatch(**request):
        if request.get("op") == "drop_console_copy":
            time.sleep(3)
        return cli._dispatch_local_send(node, bridge=bridge, **request)

    monkeypatch.setattr(cli, "_DROP_CONSOLE_COPY_TIMEOUT_SECONDS", 0.2)
    server = SendServer(data_dir, _stalling_dispatch)
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        start = time.monotonic()
        assert cli.main(["retract", "--data-dir", str(data_dir), "m-9"]) == 0
        assert time.monotonic() - start < 2.0
        assert _FakeLocalNode.instances == []
        assert node.calls == [("retract", recipient_hash.hex(), "m-9")]
        assert "retracted" in capsys.readouterr().out
        assert Inbox(config.inbox_path).list_messages(direction="out") == []
        assert Mailbox(config.mailbox_path).sent_list() == []
        # The marker is durable even though the drop never landed.
        assert Mailbox(config.mailbox_path).is_sent_retracted("m-9")
        # The console copy is still there at this instant — the marker
        # heals it at the next serve start.
        assert len(bridge.messages()) == 1
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_group_retract_via_running_node_drops_console_legs(
    tmp_path, capsys, monkeypatch
):
    # End to end: a console-sent group message retracted through the CLI
    # drops every leg entry from the feed, and leaves a later send in
    # the same group alone.
    import RNS

    from tau.identity import destination_hash
    from tau.peers import PeerRegistry
    from tau.webapi import WebBridge

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "group hello",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-10",
            "direction": "out",
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "team",
            "leg_ids": ["m-10"],
            "leg_sent_at": 100.0,
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-10", to=recipient_hash, text="group hello"
    )
    node = _FakeRunningNode(config)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    for wire_id, sent_at in (("m-10", 100.0), ("m-11", 100.0), ("m-12", 200.0)):
        bridge._append_message(
            direction="out",
            peer=recipient_hash.hex(),
            peer_name="alice",
            text="group hello",
            sent_at=sent_at,
            received_at=sent_at + 1.0,
            status="delivered",
            group="ab" * 16,
            wire_id=wire_id,
        )
    assert len(bridge.messages()) == 3
    server = SendServer(
        data_dir,
        lambda **request: cli._dispatch_local_send(
            node, bridge=bridge, **request
        ),
    )
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["retract", "--data-dir", str(data_dir), "m-10"]) == 0
        assert _FakeLocalNode.instances == []
        assert node.calls == [("retract_group", "ab" * 16, "m-10", 100.0)]
        assert "retracted your group message" in capsys.readouterr().out
        assert Inbox(config.inbox_path).list_messages(direction="out") == []
        assert Mailbox(config.mailbox_path).sent_list() == []
        assert [entry["wire_id"] for entry in bridge.messages()] == ["m-12"]
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_group_retract_unknown_group_via_running_node_reports_usage_error(
    tmp_path, capsys, monkeypatch
):
    # Regression test (Qodo warning on PR #327): the serving node's
    # unknown-group ValueError must cross local IPC as that error —
    # the CLI reports it plainly, never as a delivery failure.
    import RNS

    from tau.identity import destination_hash
    from tau.peers import PeerRegistry

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "group hello",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-10",
            "direction": "out",
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "team",
            "leg_ids": ["m-10"],
            "leg_sent_at": 100.0,
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-10", to=recipient_hash, text="group hello"
    )
    node = _FakeRunningNode(config)

    def _unknown_group(*args, **kwargs):
        raise ValueError("unknown group: 'abababababababababababababababab'")

    monkeypatch.setattr(node, "send_group_retraction", _unknown_group)
    server = SendServer(
        data_dir, lambda **request: cli._dispatch_local_send(node, **request)
    )
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert cli.main(["retract", "--data-dir", str(data_dir), "m-10"]) != 0
        # No one-shot node started: the serving node carried the retract.
        assert _FakeLocalNode.instances == []
        err = capsys.readouterr().err
        assert "unknown group" in err
        assert "could not deliver group retract" not in err
        assert "members online" not in err
    finally:
        server.stop()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_edit_uses_running_node(tmp_path, capsys, monkeypatch):
    import RNS

    from tau.identity import destination_hash
    from tau.peers import PeerRegistry

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    config.prepare()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        recipient_hash, recipient.get_public_key(), name="alice", now=100.0
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": recipient_hash.hex(),
            "text": "sent hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-9",
            "direction": "out",
        }
    )
    Mailbox(config.mailbox_path).record_sent(
        "m-9", to=recipient_hash, text="sent hello"
    )
    node = _FakeRunningNode(config)
    server = SendServer(
        data_dir, lambda **request: cli._dispatch_local_send(node, **request)
    )
    try:
        assert send_server_available(data_dir)
        _FakeLocalNode.instances.clear()
        monkeypatch.setattr(cli, "TauNode", _FakeLocalNode)
        assert (
            cli.main(["edit", "--data-dir", str(data_dir), "m-9", "fixed hello"])
            == 0
        )
        # No one-shot node started: the serving node carried the edit.
        assert _FakeLocalNode.instances == []
        assert node.calls == [
            ("edit", recipient_hash.hex(), "m-9", "fixed hello")
        ]
        assert "edited your message to alice" in capsys.readouterr().out
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

        def start(
            self,
            *,
            announce=True,
            exclusive=False,
            mail_loop=True,
            lxmf=False,
            lxmf_propagation=False,
        ):
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

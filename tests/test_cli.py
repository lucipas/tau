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
from tau.config import TauConfig, write_node_metadata
from tau.identity import destination_hash, load_identity
from tau.inbox import Inbox
from tau.lxmf import lxmf_delivery_hash
from tau.mailbox import Mailbox
from tau.node import InstanceLock
from tau.peers import PeerRegistry, safety_number


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
    assert args.to == ["alice"]
    assert args.message == "hello world"
    assert args.timeout == 20.0
    assert args.expire is None

    ephemeral = parser.parse_args(
        ["send", "--to", "alice", "--expire", "1h", "vanish soon"]
    )
    assert ephemeral.expire == "1h"


def test_parse_duration():
    assert cli._parse_duration("30s") == 30
    assert cli._parse_duration("5m") == 300
    assert cli._parse_duration("2h") == 7200
    assert cli._parse_duration("7d") == 604800
    assert cli._parse_duration("90") == 90
    assert cli._parse_duration("  10m ") == 600
    with pytest.raises(ValueError):
        cli._parse_duration("soon")
    with pytest.raises(ValueError):
        cli._parse_duration("0s")
    with pytest.raises(ValueError):
        cli._parse_duration("-5m")
    with pytest.raises(ValueError):
        cli._parse_duration("31d")
    with pytest.raises(ValueError):
        cli._parse_duration("")


def test_parser_send_to_is_repeatable():
    parser = cli.build_parser()
    args = parser.parse_args(["send", "--to", "alice", "--to", "bob", "hello"])
    assert args.to == ["alice", "bob"]


class _FakeSendNode:
    """Stub TauNode for `tau send`: per-target behaviour drives whether
    a recipient is delivered, queued (a real outbox entry is written,
    so the post-send flush can flip it), or fails."""

    instances: list = []
    behaviour: dict = {}
    deliver_on_flush = False

    def __init__(self, config, **kwargs):
        from types import SimpleNamespace

        from tau.mailbox import Mailbox

        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.started = False
        self.calls: list = []
        self.sent: list = []
        self.flush_calls: list = []
        self.peer = SimpleNamespace(hexhash="ab" * 16, display_name="alice")
        _FakeSendNode.instances.append(self)

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.started = False

    def resolve_peer(self, target, timeout=10.0):
        return self.peer

    def _target_hash(self, target):
        return bytes.fromhex(self.peer.hexhash)

    def send_message(self, target, text, **kwargs):
        from tau.node import NodeError

        self.calls.append((target, text))
        self.sent.append((target, text, kwargs.get("in_reply_to")))
        mode = _FakeSendNode.behaviour.get(target, "delivered")
        if mode == "error":
            raise NodeError(f"no path to {target}")
        if mode == "full":
            from tau.mailbox import MailboxFull

            raise MailboxFull("outbox is full")
        if mode == "spoof":
            # A malicious peer replying with another recipient's label
            # and a queued flag but no message id.
            return {"ok": True, "target": "mallory", "queued": True, "echo": text}
        if mode == "spoof_known_id":
            # A malicious peer claiming queued with the id of a message
            # already in this node's outbox (e.g. one it holds a sealed
            # copy of) — the reply must still be treated as delivered.
            entries = self.mailbox.outbox_list()
            known_id = entries[0].id if entries else "00" * 16
            return {"ok": True, "queued": True, "id": known_id, "to": "bb" * 16}
        if mode == "queued":
            entry = self.mailbox.enqueue(
                to=b"\xbb" * 16,
                text=text,
                message_id=f"{len(self.calls):032x}",
            )
            return {
                "ok": True,
                "queued": True,
                "id": entry.id,
                "to": entry.to_hex,
            }
        return {"ok": True, "echo": text, "_rtt": 0.05}

    def flush_outbox(self, **kwargs):
        self.flush_calls.append(kwargs)
        if _FakeSendNode.deliver_on_flush:
            for entry in self.mailbox.outbox_list():
                self.mailbox.remove_outbox(entry.id)


def _run_send(tmp_path, monkeypatch, argv, behaviour=None, deliver_on_flush=False):
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = dict(behaviour or {})
    _FakeSendNode.deliver_on_flush = deliver_on_flush
    return cli.main(["send", "--data-dir", str(tmp_path / "node"), *argv])


def test_send_to_several_peers_delivers_each(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--to", "bob", "hi"],
        behaviour={"alice": "delivered", "bob": "delivered"},
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "delivered to alice" in out
    assert "delivered to bob" in out
    node = _FakeSendNode.instances[0]
    assert node.calls == [("alice", "hi"), ("bob", "hi")]
    assert node.started is False
    assert node.flush_calls == []


def test_send_full_outbox_does_not_stop_later_recipients(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--to", "bob", "hi"],
        behaviour={"alice": "full", "bob": "delivered"},
    )
    assert code != 0
    out = capsys.readouterr().out
    # Later recipients are still attempted and reported.
    assert _FakeSendNode.instances[0].calls == [("alice", "hi"), ("bob", "hi")]
    assert "delivered to bob" in out


def test_send_mixed_delivered_and_queued(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--to", "bob", "hi"],
        behaviour={"alice": "delivered", "bob": "queued"},
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "delivered to alice" in out
    assert "queued for bob" in out
    # The queued hint is printed once, after every recipient's line.
    assert out.count("it will be delivered automatically") == 1
    node = _FakeSendNode.instances[0]
    assert len(node.flush_calls) == 1
    assert node.flush_calls[0]["timeout"] == 3.0
    assert node.flush_calls[0]["max_peers"] == 5


def test_send_queued_entry_flips_when_flush_delivers(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "bob", "hi"],
        behaviour={"bob": "queued"},
        deliver_on_flush=True,
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "delivered to bob" in out
    assert "queued for bob" not in out


def test_send_one_failure_does_not_stop_the_rest(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--to", "carol", "--to", "bob", "hi"],
        behaviour={"alice": "delivered", "carol": "error", "bob": "delivered"},
    )
    assert code == 1
    captured = capsys.readouterr()
    assert "delivered to alice" in captured.out
    assert "delivered to bob" in captured.out
    assert "failed to send to carol: no path to carol" in captured.err
    node = _FakeSendNode.instances[0]
    assert [call[0] for call in node.calls] == ["alice", "carol", "bob"]


def test_send_json_multi_uses_results_list(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--to", "bob", "--json", "hi"],
        behaviour={"alice": "delivered", "bob": "queued"},
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [r["target"] for r in payload["results"]] == ["alice", "bob"]
    assert payload["delivered"] == 1
    assert payload["queued"] == 1
    assert payload["failed"] == 0


def test_send_json_single_keeps_bare_shape(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--json", "hi"],
        behaviour={"alice": "delivered"},
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    # The complete payload is the bare peer response: no "results"
    # wrapper and no "queued" key added for a direct delivery.
    assert payload == {"ok": True, "echo": "hi", "_rtt": 0.05}


def test_send_json_single_queued_keeps_bare_shape(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "bob", "--json", "hi"],
        behaviour={"bob": "queued"},
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["queued"] is True
    assert "id" in payload


def test_send_peer_response_cannot_relabel_or_fake_queue(
    tmp_path, capsys, monkeypatch
):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--to", "bob", "--json", "hi"],
        behaviour={"alice": "delivered", "bob": "spoof"},
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    bob = payload["results"][1]
    assert bob["target"] == "bob"
    assert bob["queued"] is False
    assert payload["delivered"] == 2
    assert payload["queued"] == 0


def test_send_spoofed_queued_with_known_id_is_delivered(
    tmp_path, capsys, monkeypatch
):
    from tau.config import TauConfig
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir)
    box = Mailbox(config.mailbox_path)
    held = box.enqueue(to=b"\xbb" * 16, text="held", message_id="cc" * 16)

    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "bob", "--json", "hi"],
        behaviour={"bob": "spoof_known_id"},
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    # The peer's queued claim used a pre-existing outbox id, so the
    # delivered message is reported delivered, not queued.
    assert payload["queued"] is False
    assert payload["id"] == held.id
    # The real queued entry is untouched by the spoof.
    assert Mailbox(config.mailbox_path).get_outbox(held.id) is not None


def test_parser_send_wait_argument():
    parser = cli.build_parser()
    default = parser.parse_args(["send", "--to", "alice", "hello"])
    assert default.wait is None
    waited = parser.parse_args(["send", "--to", "alice", "--wait", "30", "hello"])
    assert waited.wait == 30.0


class _FakeWaitNode:
    """Stub TauNode for `tau send --wait`: queues into the real mailbox
    and drives the real `TauNode.wait_for_delivery` loop, so the CLI
    wiring and the wait loop are both exercised.  When ``deliver`` is
    set, the first targeted flush removes the entry and records the
    delivery, as a real delivery would.  When ``vanish`` is set, the
    entry disappears without a delivery record, as a concurrent
    cancel would remove it."""

    instances: list = []
    deliver = False
    vanish = False

    def __init__(self, config, **kwargs):
        from collections import deque

        from tau.mailbox import Mailbox

        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.started = False
        self.flush_calls: list = []
        self._delivered_ids = deque(maxlen=512)
        _FakeWaitNode.instances.append(self)

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.started = False

    def send_message(self, target, text, **kwargs):
        entry = self.mailbox.enqueue(
            to=b"\xbb" * 16, text=text, message_id="ee" + "77" * 15
        )
        return {
            "ok": True,
            "v": 1,
            "queued": True,
            "id": entry.id,
            "to": entry.to_hex,
            "expires_at": entry.expires_at,
        }

    def flush_outbox(self, **kwargs):
        self.flush_calls.append(kwargs)
        target = kwargs.get("message_id")
        if target is not None:
            if _FakeWaitNode.deliver:
                self.mailbox.remove_outbox(target)
                self._delivered_ids.append(target)
            elif _FakeWaitNode.vanish:
                self.mailbox.remove_outbox(target)
        return {"delivered": 0, "deposited": 0, "received": 0, "lock_busy": False}

    def _still_queued(self, message_id, timeout=None):
        from tau.node import TauNode as _RealNode

        return _RealNode._still_queued(self, message_id, timeout=timeout)

    def wait_for_delivery(self, message_id, *, timeout, poll_interval=1.0):
        from tau.node import TauNode as _RealNode

        return _RealNode.wait_for_delivery(
            self, message_id, timeout=timeout, poll_interval=poll_interval
        )


def test_send_wait_delivers_while_waiting(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(cli, "TauNode", _FakeWaitNode)
    _FakeWaitNode.instances.clear()
    _FakeWaitNode.deliver = True
    _FakeWaitNode.vanish = False
    code = cli.main(
        ["send", "--to", "bob", "--wait", "5", "hello",
         "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "delivered to bob after" in capsys.readouterr().out
    node = _FakeWaitNode.instances[0]
    assert node.flush_calls[0]["message_id"] == "ee" + "77" * 15
    assert node.flush_calls[0]["respect_backoff"] is False


def test_send_wait_json_reports_waited_seconds(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(cli, "TauNode", _FakeWaitNode)
    _FakeWaitNode.instances.clear()
    _FakeWaitNode.deliver = True
    _FakeWaitNode.vanish = False
    code = cli.main(
        ["send", "--to", "bob", "--wait", "5", "hello",
         "--data-dir", str(tmp_path / "node"), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["delivered"] is True
    assert "queued" not in payload
    assert payload["waited"] >= 0


def test_send_wait_times_out_with_nonzero_exit(tmp_path, capsys, monkeypatch):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    monkeypatch.setattr(cli, "TauNode", _FakeWaitNode)
    _FakeWaitNode.instances.clear()
    _FakeWaitNode.deliver = False
    _FakeWaitNode.vanish = False
    code = cli.main(
        ["send", "--to", "bob", "--wait", "0.3", "hello",
         "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "still queued for bob after" in capsys.readouterr().out
    # The entry is left for the background mail loop, not dropped.
    assert len(Mailbox(TauConfig.resolve(data_dir=data_dir).mailbox_path).outbox_list()) == 1


def test_send_wait_reports_unconfirmed_removal_not_delivery(
    tmp_path, capsys, monkeypatch
):
    # The entry vanishes mid-wait without a delivery confirmation
    # (a concurrent cancel): the command must not claim delivery.
    monkeypatch.setattr(cli, "TauNode", _FakeWaitNode)
    _FakeWaitNode.instances.clear()
    _FakeWaitNode.deliver = False
    _FakeWaitNode.vanish = True
    code = cli.main(
        ["send", "--to", "bob", "--wait", "5", "hello",
         "--data-dir", str(tmp_path / "node"), "--json"]
    )
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["delivered"] is False
    assert payload["queued"] is False
    code = cli.main(
        ["send", "--to", "bob", "--wait", "5", "hello",
         "--data-dir", str(tmp_path / "node")]
    )
    assert code == 1
    out = capsys.readouterr().out
    assert "no longer queued for bob after" in out
    assert "not confirmed" in out


def test_send_wait_json_multi_counts_unconfirmed_removal_as_not_delivered(
    tmp_path, capsys, monkeypatch
):
    # An entry that leaves the queue mid-wait without a delivery
    # confirmation must not be counted in the multi-recipient
    # `delivered` total.
    monkeypatch.setattr(cli, "TauNode", _FakeWaitNode)
    _FakeWaitNode.instances.clear()
    _FakeWaitNode.deliver = False
    _FakeWaitNode.vanish = True
    code = cli.main(
        ["send", "--to", "alice", "--to", "bob", "--wait", "5", "hello",
         "--data-dir", str(tmp_path / "node"), "--json"]
    )
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["delivered"] == 1
    assert payload["queued"] == 0
    assert payload["failed"] == 0
    alice, bob = payload["results"]
    assert alice["delivered"] is False
    assert bob["delivered"] is True


def test_send_wait_rejects_nonfinite_and_explicit_zero_with_no_queue(
    tmp_path, capsys, monkeypatch
):
    # Non-finite waits and an explicit --wait (even 0) with --no-queue
    # fail at once, before the message is read.
    for bad in ("nan", "inf"):
        code = cli.main(
            ["send", "--to", "bob", "--wait", bad, "hi",
             "--data-dir", str(tmp_path / "node")]
        )
        assert code == 2
        assert "finite" in capsys.readouterr().err
    code = cli.main(
        ["send", "--to", "bob", "--wait", "0", "--no-queue", "hi",
         "--data-dir", str(tmp_path / "node")]
    )
    assert code == 2
    assert "--no-queue" in capsys.readouterr().err
    # ...while --wait 0 on its own is accepted and behaves like no wait.
    monkeypatch.setattr(cli, "TauNode", _FakeWaitNode)
    _FakeWaitNode.instances.clear()
    _FakeWaitNode.deliver = False
    _FakeWaitNode.vanish = False
    code = cli.main(
        ["send", "--to", "bob", "--wait", "0", "hello",
         "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "queued for bob" in capsys.readouterr().out


def test_send_wait_rejects_negative_and_no_queue(tmp_path, capsys):
    code = cli.main(
        ["send", "--to", "bob", "--wait", "-1", "hi",
         "--data-dir", str(tmp_path / "node")]
    )
    assert code == 2
    assert "--wait must not be negative" in capsys.readouterr().err
    code = cli.main(
        ["send", "--to", "bob", "--wait", "5", "--no-queue", "hi",
         "--data-dir", str(tmp_path / "node")]
    )
    assert code == 2
    assert "--no-queue" in capsys.readouterr().err


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
    assert id_payload["lxmf"] == lxmf_delivery_hash(identity).hex()
    assert len(id_payload["lxmf"]) == 32


def test_id_text_output_includes_lxmf_address(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        cli.main(
            [
                "init",
                "--data-dir",
                str(data_dir),
                "--name",
                "alice",
                "--no-auto-interface",
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    identity = load_identity(data_dir / "identity")
    assert identity is not None

    code = cli.main(["id", "--data-dir", str(data_dir)])
    assert code == 0
    lines = capsys.readouterr().out.splitlines()
    expected = f"lxmf:          {lxmf_delivery_hash(identity).hex()}"
    assert expected in lines


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


def test_parser_status_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["status"])
    assert args.command == "status"


def test_status_reports_empty_node(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()  # drain the --json output of `tau init`
    code = cli.main(["status", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["name"] == "bob"
    assert payload["destination"] == _id_destination(data_dir, capsys)
    assert payload["data_dir"] == str(data_dir)
    assert payload["lxmf"]
    assert payload["serving"] is False
    assert payload["serving_pid"] is None
    assert payload["peers"] == 0
    assert payload["outbox"] == {"queued": 0, "due": 0, "held": 0}
    assert payload["inbox"] == {"messages": 0}
    assert payload["tan"] == {"posts": 0, "following": 0}
    assert payload["allowlist"] == {"enabled": False, "entries": 0}
    # Status is read-only: it must not create a Tan database to count it.
    assert not (data_dir / "tan.sqlite3").exists()


def test_status_text_output(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "bob")
    code = cli.main(["status", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "serving:     no" in out
    assert "peers:       0" in out
    assert "outbox:      0 queued (0 due), 0 held" in out
    assert "inbox:       0 messages" in out
    assert "tan:         0 posts, following 0 authors" in out
    assert "allowlist:   disabled (0 entries)" in out


def test_status_counts_seeded_state(tmp_path, capsys):
    from tau.allowlist import AllowList, save_allowlist
    from tau.tangents.tan import create_post, follow, init_db

    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()  # drain the --json output of `tau init`
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    peer_identity = RNS.Identity()
    peer_hash = destination_hash(peer_identity)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(peer_hash, peer_identity.get_public_key(), name="alice", now=100.0)
    registry.save()
    box = Mailbox(config.mailbox_path)
    first = box.enqueue(to=b"\xbb" * 16, text="due now", message_id="aa" + "11" * 15)
    box.enqueue(to=b"\xbb" * 16, text="backing off", message_id="bb" + "22" * 15)
    box.record_attempt(first.id)
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": peer_hash.hex(),
            "text": "hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-1",
        }
    )
    conn = init_db(config.social_db_path)
    try:
        create_post(conn, peer_identity, "first post", name="alice", now=1000)
        create_post(conn, peer_identity, "second post", name="alice", now=1001)
        follow(conn, peer_hash.hex(), name="alice")
    finally:
        conn.close()
    save_allowlist(
        config.allowlist_path,
        AllowList(enabled=True, allow=frozenset({peer_hash.hex()})),
    )

    code = cli.main(["status", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["peers"] == 1
    # One entry is due; the attempted one backs off for 60s.
    assert payload["outbox"] == {"queued": 2, "due": 1, "held": 0}
    assert payload["inbox"] == {"messages": 1}
    assert payload["tan"] == {"posts": 2, "following": 1}
    assert payload["allowlist"] == {"enabled": True, "entries": 1}


def test_status_expired_mail_is_queued_not_due(tmp_path, capsys):
    # Expired outbox entries linger until a prune runs, but they will
    # never be delivered, so status counts them as queued, not due.
    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()  # drain the --json output of `tau init`
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    box = Mailbox(config.mailbox_path)
    box.enqueue(
        to=b"\xbb" * 16,
        text="stale",
        message_id="cc" + "33" * 15,
        now=time.time() - 8 * 24 * 60 * 60,
    )
    code = cli.main(["status", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["outbox"] == {"queued": 1, "due": 0, "held": 0}


def test_status_reports_serving_node(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()  # drain the --json output of `tau init`
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        code = cli.main(["status", "--data-dir", str(data_dir), "--json"])
        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["serving"] is True
        assert payload["serving_pid"] == os.getpid()
    finally:
        lock.release()


def test_status_without_identity_returns_error(tmp_path, capsys):
    code = cli.main(["status", "--data-dir", str(tmp_path / "missing")])
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


def test_parser_id_rename_subcommand():
    parser = cli.build_parser()
    args = parser.parse_args(["id", "rename", "bob"])
    assert args.func is cli.cmd_id_rename
    assert args.new_name == "bob"


def test_id_rename_end_to_end(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    destination = _id_destination(node, capsys)
    capsys.readouterr()

    code = cli.main(["id", "rename", "bob", "--data-dir", str(node), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "name": "bob",
        "previous_name": "alice",
        "changed": True,
        "destination": destination,
    }

    assert cli.main(["id", "--data-dir", str(node), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "bob"
    assert _id_destination(node, capsys) == destination
    assert json.loads((node / "node.json").read_text())["name"] == "bob"


def test_id_rename_text_output_collapses_whitespace(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    code = cli.main(["id", "rename", "  bob   smith  ", "--data-dir", str(node)])
    assert code == 0
    assert 'Renamed node "alice" to "bob smith".' in capsys.readouterr().out


def test_id_rename_same_name_is_noop(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    code = cli.main(["id", "rename", "alice", "--data-dir", str(node), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["changed"] is False
    assert payload["name"] == "alice"
    assert payload["previous_name"] == "alice"
    assert json.loads((node / "node.json").read_text())["name"] == "alice"


def test_id_rename_rejects_empty_name(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    code = cli.main(["id", "rename", "   ", "--data-dir", str(node)])
    assert code == 1
    assert "must not be empty" in capsys.readouterr().err
    assert json.loads((node / "node.json").read_text())["name"] == "alice"


def test_id_rename_rejects_control_only_name(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    code = cli.main(["id", "rename", "\x01\x02", "--data-dir", str(node)])
    assert code == 1
    assert "must not be empty" in capsys.readouterr().err
    assert json.loads((node / "node.json").read_text())["name"] == "alice"


def test_id_rename_refuses_without_identity(tmp_path, capsys):
    code = cli.main(["id", "rename", "bob", "--data-dir", str(tmp_path / "fresh")])
    assert code == 1
    assert "run `tau init` first" in capsys.readouterr().err


def test_id_rename_refuses_while_serving(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=node)
    holder = InstanceLock(config.instance_lock_path, config.data_dir)
    holder.acquire()
    try:
        code = cli.main(["id", "rename", "bob", "--data-dir", str(node)])
    finally:
        holder.release()
    assert code == 1
    assert "already serving" in capsys.readouterr().err
    assert json.loads((node / "node.json").read_text())["name"] == "alice"


def test_refresh_served_name_picks_up_rename(tmp_path):
    node_dir = tmp_path / "node"
    write_node_metadata(node_dir, "bob")
    config = TauConfig.resolve(data_dir=node_dir)
    announced = []
    node = SimpleNamespace(name="alice", announce=lambda: announced.append(True))
    cli._refresh_served_name(config, node, SimpleNamespace(), no_announce=False)
    assert node.name == "bob"
    assert announced == [True]


def test_refresh_served_name_sanitises_hand_edited_name(tmp_path):
    node_dir = tmp_path / "node"
    node_dir.mkdir()
    (node_dir / "node.json").write_text(
        json.dumps({"name": "  bob\x01  ", "protocol": "tau"}), encoding="utf-8"
    )
    config = TauConfig.resolve(data_dir=node_dir)
    node = SimpleNamespace(name="alice", announce=lambda: None)
    cli._refresh_served_name(config, node, SimpleNamespace(), no_announce=True)
    assert node.name == "bob"


def test_refresh_served_name_respects_name_override(tmp_path):
    node_dir = tmp_path / "node"
    write_node_metadata(node_dir, "bob")
    config = TauConfig.resolve(data_dir=node_dir)
    announced = []
    node = SimpleNamespace(name="carol", announce=lambda: announced.append(True))
    cli._refresh_served_name(
        config, node, SimpleNamespace(name="carol"), no_announce=False
    )
    assert node.name == "carol"
    assert announced == []


def test_refresh_served_name_noop_without_change(tmp_path):
    node_dir = tmp_path / "node"
    write_node_metadata(node_dir, "alice")
    config = TauConfig.resolve(data_dir=node_dir)
    announced = []
    node = SimpleNamespace(name="alice", announce=lambda: announced.append(True))
    cli._refresh_served_name(config, node, SimpleNamespace(), no_announce=False)
    assert node.name == "alice"
    assert announced == []


def test_peers_on_empty_registry(tmp_path, capsys):
    code = cli.main(["peers", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    assert "No peers known" in capsys.readouterr().out


def _init_and_seed_peer(tmp_path, peer_name="alice", capsys=None):
    """Init a node and register one peer built from a real identity."""
    data_dir = tmp_path / "node"
    assert (
        cli.main(
            [
                "init",
                "--data-dir",
                str(data_dir),
                "--name",
                "owner",
                "--no-auto-interface",
                "--json",
            ]
        )
        == 0
    )
    if capsys is not None:
        capsys.readouterr()  # drain the init payload so later reads are clean
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    peer_identity = RNS.Identity()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name=peer_name,
        now=100.0,
    )
    registry.save()
    return data_dir, config, peer_identity


def test_id_includes_safety_number(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        cli.main(
            [
                "init",
                "--data-dir",
                str(data_dir),
                "--name",
                "alice",
                "--no-auto-interface",
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()
    identity = load_identity(data_dir / "identity")
    assert identity is not None

    code = cli.main(["id", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["safety_number"] == safety_number(identity.get_public_key())

    code = cli.main(["id", "--data-dir", str(data_dir)])
    assert code == 0
    lines = capsys.readouterr().out.splitlines()
    assert (
        f"safety number: {safety_number(identity.get_public_key())}" in lines
    )


def test_peers_verify_marks_peer_verified(tmp_path, capsys):
    data_dir, config, peer_identity = _init_and_seed_peer(tmp_path, capsys=capsys)
    owner = load_identity(data_dir / "identity")
    assert owner is not None
    expected = safety_number(
        owner.get_public_key(), peer_identity.get_public_key()
    )

    code = cli.main(["peers", "verify", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "Verified alice" in out
    assert expected in out

    registry = PeerRegistry(config.peers_path)
    peer = registry.get("alice")
    assert peer is not None
    assert peer.is_verified
    assert peer.verified_fingerprint == expected

    # The table now shows the verified marker.
    code = cli.main(["peers", "--data-dir", str(data_dir)])
    assert code == 0
    assert "✓" in capsys.readouterr().out


def test_peers_verify_json_reports_reverification(tmp_path, capsys):
    data_dir, config, _peer_identity = _init_and_seed_peer(tmp_path, capsys=capsys)

    code = cli.main(
        ["peers", "verify", "alice", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    first = json.loads(capsys.readouterr().out)
    assert first["reverified"] is False
    assert first["fingerprint_changed"] is False

    code = cli.main(
        ["peers", "verify", "alice", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    second = json.loads(capsys.readouterr().out)
    assert second["reverified"] is True
    assert second["safety_number"] == first["safety_number"]


def test_peers_verify_unknown_peer_returns_error(tmp_path, capsys):
    data_dir, _config, _peer_identity = _init_and_seed_peer(tmp_path, capsys=capsys)

    code = cli.main(["peers", "verify", "nobody", "--data-dir", str(data_dir)])
    assert code == 1
    assert "no peer matches" in capsys.readouterr().err


def test_peers_safety_number_lists_pairwise_numbers(tmp_path, capsys):
    data_dir, _config, peer_identity = _init_and_seed_peer(tmp_path, capsys=capsys)
    owner = load_identity(data_dir / "identity")
    assert owner is not None
    expected = safety_number(
        owner.get_public_key(), peer_identity.get_public_key()
    )

    code = cli.main(["peers", "--safety-number", "--data-dir", str(data_dir)])
    assert code == 0
    assert expected in capsys.readouterr().out

    code = cli.main(
        ["peers", "--safety-number", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["peers"][0]["safety_number"] == expected


def test_peers_safety_number_without_identity_returns_error(tmp_path, capsys):
    code = cli.main(
        ["peers", "--safety-number", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 1
    assert "run `tau init` first" in capsys.readouterr().err


def test_peers_forget_removes_peer(tmp_path, capsys):
    data_dir, config, _peer_identity = _init_and_seed_peer(
        tmp_path, capsys=capsys
    )

    code = cli.main(["peers", "forget", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Forgot alice" in capsys.readouterr().out
    assert PeerRegistry(config.peers_path).get("alice") is None

    code = cli.main(["peers", "forget", "alice", "--data-dir", str(data_dir)])
    assert code == 1
    assert "no peer matches" in capsys.readouterr().err


def test_peers_forget_refuses_queued_mail_without_force(tmp_path, capsys):
    data_dir, config, peer_identity = _init_and_seed_peer(
        tmp_path, capsys=capsys
    )
    mailbox = Mailbox(config.mailbox_path)
    mailbox.enqueue(
        to=destination_hash(peer_identity), text="hello", to_name="alice"
    )

    code = cli.main(["peers", "forget", "alice", "--data-dir", str(data_dir)])
    assert code == 1
    assert "queued message" in capsys.readouterr().err
    assert PeerRegistry(config.peers_path).get("alice") is not None

    code = cli.main(
        ["peers", "forget", "alice", "--force", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "will not be attempted" in capsys.readouterr().out
    assert PeerRegistry(config.peers_path).get("alice") is None


def test_id_import_different_identity_clears_verifications(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    # Seed and verify a peer on node A, then import a different identity.
    config_a = TauConfig.resolve(data_dir=node_a, name="alice")
    peer_identity = RNS.Identity()
    registry = PeerRegistry(config_a.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="bob",
        now=100.0,
    )
    registry.save()
    assert (
        cli.main(["peers", "verify", "bob", "--data-dir", str(node_a)]) == 0
    )
    capsys.readouterr()
    assert PeerRegistry(config_a.peers_path).get("bob").is_verified

    node_b = _init_node(tmp_path, "b", "carol")
    capsys.readouterr()
    backup = tmp_path / "carol.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_b), "--json"]
        )
        == 0
    )
    capsys.readouterr()

    code = cli.main(
        [
            "id",
            "import",
            "--from",
            str(backup),
            "--data-dir",
            str(node_a),
            "--force",
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["verifications_cleared"] == 1
    assert not PeerRegistry(config_a.peers_path).get("bob").is_verified


def test_id_import_same_identity_keeps_verifications(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    config_a = TauConfig.resolve(data_dir=node_a, name="alice")
    peer_identity = RNS.Identity()
    registry = PeerRegistry(config_a.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="bob",
        now=100.0,
    )
    registry.save()
    assert (
        cli.main(["peers", "verify", "bob", "--data-dir", str(node_a)]) == 0
    )
    capsys.readouterr()

    backup = tmp_path / "alice.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_a), "--json"]
        )
        == 0
    )
    capsys.readouterr()

    # Re-importing the same identity is not an identity change.
    code = cli.main(
        [
            "id",
            "import",
            "--from",
            str(backup),
            "--data-dir",
            str(node_a),
            "--force",
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["verifications_cleared"] == 0
    assert PeerRegistry(config_a.peers_path).get("bob").is_verified


def test_id_import_registry_save_failure_aborts_replacement(
    tmp_path, capsys, monkeypatch
):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    destination_a = _id_destination(node_a, capsys)
    config_a = TauConfig.resolve(data_dir=node_a, name="alice")
    peer_identity = RNS.Identity()
    registry = PeerRegistry(config_a.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="bob",
        now=100.0,
    )
    registry.save()
    assert (
        cli.main(["peers", "verify", "bob", "--data-dir", str(node_a)]) == 0
    )
    capsys.readouterr()

    node_b = _init_node(tmp_path, "b", "carol")
    capsys.readouterr()
    backup = tmp_path / "carol.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_b), "--json"]
        )
        == 0
    )
    capsys.readouterr()

    # The registry write fails: the import must abort with the old
    # identity still in place, not commit the new key alongside stale
    # verified markers.
    monkeypatch.setattr(cli.PeerRegistry, "save", lambda self: False)
    code = cli.main(
        [
            "id",
            "import",
            "--from",
            str(backup),
            "--data-dir",
            str(node_a),
            "--force",
        ]
    )
    assert code == 1
    assert "identity not replaced" in capsys.readouterr().err
    assert _id_destination(node_a, capsys) == destination_a
    assert PeerRegistry(config_a.peers_path).get("bob").is_verified


def test_parser_messages_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["messages", "--peer", "alice", "--limit", "5"])
    assert args.command == "messages"
    assert args.peer == "alice"
    assert args.limit == 5

    defaults = parser.parse_args(["messages"])
    assert defaults.peer is None
    assert defaults.limit == 30
    assert defaults.search is None
    assert defaults.sent is False

    args = parser.parse_args(["messages", "--search", "hello"])
    assert args.search == "hello"

    args = parser.parse_args(["messages", "--sent"])
    assert args.sent is True


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


def test_messages_search_filters_by_text(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(["messages", "--data-dir", str(data_dir), "--search", "SECOND", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["id"] == "m-2"

    code = cli.main(["messages", "--data-dir", str(data_dir), "--search", "hello", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2

    code = cli.main(["messages", "--data-dir", str(data_dir), "--search", "nothing-here"])
    assert code == 0
    assert "No messages matching 'nothing-here'." in capsys.readouterr().out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--search", "  "])
    assert code == 1
    assert "--search must not be empty" in capsys.readouterr().err


def test_messages_search_does_not_mark_unmatched_unread_read(tmp_path, capsys):
    # Both seeded messages are unread; only the newer one matches.
    # Searching is a read-only lookup: it marks nothing read, so the
    # older unmatched message keeps its unread state instead of being
    # cleared by the read cursor.
    data_dir, sender_hash = _seed_messages(tmp_path)

    code = cli.main(["messages", "--data-dir", str(data_dir), "--search", "SECOND", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["id"] == "m-2"

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"unread": {sender_hash.hex(): 2}, "total": 2}


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


def test_parser_messages_delete_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["messages", "delete", "abcd1234"])
    assert args.command == "messages"
    assert args.target == "abcd1234"
    assert args.func is cli.cmd_messages_delete

    # Plain `tau messages` still lists.
    args = parser.parse_args(["messages"])
    assert args.func is cli.cmd_messages


def test_messages_list_shows_short_id(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "#m-2" in out
    assert "#m-1" in out


def test_messages_delete_by_id(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(["messages", "delete", "m-2", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "deleted [m-2] from alice" in out
    assert "only this node's copy was deleted" in out

    # The deletion persists and the other message survives.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-1"]


def test_messages_delete_json_and_errors(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    # Ambiguous prefix: both seeded ids start with "m-".
    code = cli.main(["messages", "delete", "m-", "--data-dir", str(data_dir)])
    assert code == 1
    assert "2 messages match prefix" in capsys.readouterr().err

    # Unknown id.
    code = cli.main(["messages", "delete", "nope", "--data-dir", str(data_dir)])
    assert code == 1
    assert "unknown message" in capsys.readouterr().err

    code = cli.main(
        ["messages", "delete", "m-1", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["deleted"]["id"] == "m-1"
    assert payload["deleted"]["text"] == "first hello"


def test_parser_messages_unread_and_count_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["messages", "--unread", "--count"])
    assert args.unread is True
    assert args.count is True

    defaults = parser.parse_args(["messages"])
    assert defaults.unread is False
    assert defaults.count is False


def test_messages_listing_marks_entries_read(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    # Both entries were unread when first listed.
    assert out.count("● ") == 2

    # A second listing shows the same messages, now read.
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "● " not in out
    assert "second hello" in out

    # --unread finds nothing once the conversation has been read.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--unread"])
    assert code == 0
    assert "No unread messages." in capsys.readouterr().out


def test_messages_unread_filter_and_json_flags(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--unread", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert all(item["unread"] for item in payload["messages"])

    # The --unread listing itself advances the cursor.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--unread", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 0

    # A --peer listing only marks that conversation read.
    inbox = Inbox(TauConfig.resolve(data_dir=data_dir, name="bob").inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "third hello",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-3",
        }
    )
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--peer", "alice", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 3
    assert [item["unread"] for item in payload["messages"]] == [True, False, False]


def test_messages_count_does_not_advance_the_cursor(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "2 unread" in out
    assert "Total: 2 unread" in out

    # Counting is read-only: the count is unchanged afterwards.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--count", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"unread": {sender_hash.hex(): 2}, "total": 2}

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--count", "--peer", "alice"]
    )
    assert code == 0
    assert "Total: 2 unread" in capsys.readouterr().out

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--count", "--peer", "nobody"]
    )
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err


def test_messages_unread_filter_applies_before_limit(tmp_path, capsys):
    """An older unread message is not crowded out by newer read ones."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    registry = PeerRegistry(config.peers_path)
    alice = RNS.Identity()
    alice_hash = destination_hash(alice)
    carol = RNS.Identity()
    carol_hash = destination_hash(carol)
    registry.upsert(alice_hash, alice.get_public_key(), name="alice", now=100.0)
    registry.upsert(carol_hash, carol.get_public_key(), name="carol", now=100.0)
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": alice_hash.hex(),
            "text": "old unread",
            "sent_at": 10.0,
            "received_at": 10.0,
            "id": "a-1",
        }
    )
    for index, received_at in enumerate((11.0, 12.0, 13.0)):
        inbox.append(
            {
                "from": "carol",
                "destination": carol_hash.hex(),
                "text": f"read {index}",
                "sent_at": 11.0,
                "received_at": received_at,
                "id": f"c-{index}",
            }
        )
    inbox.mark_read(peer=carol_hash.hex())

    # The three newest messages are all read; --unread must still find
    # Alice's older one instead of reporting nothing.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--unread", "--limit", "3", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["a-1"]
    assert payload["more_unread"] == {}


def test_messages_limit_truncation_reports_unshown_unread(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)
    inbox = Inbox(TauConfig.resolve(data_dir=data_dir, name="bob").inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "third hello",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-3",
        }
    )

    # Two of three unread fit in the window; the third stays unread and
    # is reported, not silently marked read.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--unread", "--limit", "2", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-3", "m-2"]
    assert payload["more_unread"] == {sender_hash.hex(): 1}

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"unread": {sender_hash.hex(): 3}, "total": 3}

    # The text listing prints the same hint, and the plain listing
    # truncates the same way.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--limit", "2"])
    assert code == 0
    out = capsys.readouterr().out
    assert "1 more unread from alice" in out
    assert "raise --limit to see them" in out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count", "--json"])
    assert json.loads(capsys.readouterr().out)["total"] == 3


def test_messages_count_empty(tmp_path, capsys):
    code = cli.main(["messages", "--data-dir", str(tmp_path / "node"), "--count"])
    assert code == 0
    assert "No unread messages." in capsys.readouterr().out

    code = cli.main(
        ["messages", "--data-dir", str(tmp_path / "node"), "--count", "--json"]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"unread": {}, "total": 0}
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


def test_outbox_held_shows_per_sender_usage(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    box = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    box.hold(
        to=b"\xbb" * 16,
        blob=b"sealed-bytes",
        message_id="a" * 32,
        expires_at=time.time() + 3600,
        depositor=sender_hash,
    )
    box.hold(
        to=b"\xbb" * 16,
        blob=b"sealed-bytes",
        message_id="b" * 32,
        expires_at=time.time() + 3600,
        depositor=sender_hash,
    )
    box.hold(
        to=b"\xaa" * 16,
        blob=b"sealed-bytes",
        message_id="c" * 32,
        expires_at=time.time() + 3600,
    )
    registry.upsert(sender_hash, sender.get_public_key(), name="alice")
    registry.save()

    code = cli.main(["outbox", "held", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "Held per sender:" in out
    assert "alice" in out
    assert "unknown" in out

    code = cli.main(["outbox", "held", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["held"][0]["from"] == sender_hash.hex()
    assert payload["held"][0]["from_name"] == "alice"
    assert payload["senders"] == [
        {
            "sender": sender_hash.hex(),
            "name": "alice",
            "count": 2,
            "bytes": 2 * len(b"sealed-bytes"),
        },
        {"sender": "", "name": "", "count": 1, "bytes": len(b"sealed-bytes")},
    ]


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


def test_parser_peers_forget_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["peers", "forget", "alice"])
    assert args.command == "peers"
    assert args.peers_command == "forget"
    assert args.target == "alice"
    assert args.func is cli.cmd_peers_forget

    listing = parser.parse_args(["peers", "--max-age", "1"])
    assert listing.func is cli.cmd_peers


def _seed_forget_peers(data_dir):
    from tau.config import TauConfig
    from tau.peers import PeerRegistry

    config = TauConfig.resolve(data_dir=data_dir)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(b"\xbb" * 16, b"\x02" * 64, name="bob", now=100.0)
    registry.upsert(b"\xcc" * 16, b"\x03" * 64, name="carol", now=100.0)
    registry.save()
    return config


def test_peers_forget_removes_peer_and_keeps_others(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = _seed_forget_peers(data_dir)

    code = cli.main(["peers", "forget", "bob", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Forgot bob" in capsys.readouterr().out
    registry = PeerRegistry(config.peers_path)
    assert registry.get("bob") is None
    assert registry.get("carol") is not None


def test_peers_forget_json_and_prefix(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = _seed_forget_peers(data_dir)

    code = cli.main(
        ["peers", "forget", "bb" * 4, "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["forgotten"]["name"] == "bob"
    assert PeerRegistry(config.peers_path).get("bob") is None


def test_peers_forget_unknown_peer(tmp_path, capsys):
    data_dir = tmp_path / "node"
    _seed_forget_peers(data_dir)

    code = cli.main(["peers", "forget", "nobody", "--data-dir", str(data_dir)])
    assert code == 1
    assert "no peer matches" in capsys.readouterr().err


def test_peers_forget_refuses_queued_mail(tmp_path, capsys):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config = _seed_forget_peers(data_dir)
    box = Mailbox(config.mailbox_path)
    box.enqueue(to=b"\xbb" * 16, text="for bob", message_id="aa" + "11" * 15)

    code = cli.main(["peers", "forget", "bob", "--data-dir", str(data_dir)])
    assert code == 1
    assert "queued" in capsys.readouterr().err
    assert PeerRegistry(config.peers_path).get("bob") is not None


def test_peers_forget_refuses_pending_acks(tmp_path, capsys):
    from tau.mailbox import Mailbox

    data_dir = tmp_path / "node"
    config = _seed_forget_peers(data_dir)
    box = Mailbox(config.mailbox_path)
    box.add_pending_acks("bb" * 16, ["aa" + "11" * 15])

    code = cli.main(["peers", "forget", "bob", "--data-dir", str(data_dir)])
    assert code == 1
    assert "acknowledgements" in capsys.readouterr().err
    assert PeerRegistry(config.peers_path).get("bob") is not None


def test_peers_forget_refuses_while_serving(tmp_path, capsys):
    from tau.node import InstanceLock

    data_dir = tmp_path / "node"
    config = _seed_forget_peers(data_dir)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        code = cli.main(["peers", "forget", "bob", "--data-dir", str(data_dir)])
    finally:
        lock.release()
    assert code == 1
    assert "already serving" in capsys.readouterr().err
    assert PeerRegistry(config.peers_path).get("bob") is not None


def test_peers_forget_holds_mailbox_transaction(tmp_path, capsys, monkeypatch):
    import fcntl

    data_dir = tmp_path / "node"
    config = _seed_forget_peers(data_dir)
    lock_path = config.mailbox_path.with_name(config.mailbox_path.name + ".lock")
    blocked = {}

    real_save = PeerRegistry.save

    def save_and_probe(self):
        # A non-blocking flock on the mailbox lock file must fail while
        # forget holds the mailbox transaction across the registry save:
        # a concurrent send could not enqueue between the safety check
        # and the removal.
        with open(lock_path, "a") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                blocked["held"] = True
            else:
                blocked["held"] = False
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return real_save(self)

    monkeypatch.setattr(PeerRegistry, "save", save_and_probe)
    code = cli.main(["peers", "forget", "bob", "--data-dir", str(data_dir)])
    assert code == 0
    assert blocked.get("held") is True
    assert PeerRegistry(config.peers_path).get("bob") is None


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

        def start(self, *, announce=True, exclusive=False, lxmf=False):
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


# -- peers alias ------------------------------------------------------------


def _seed_alias_peer(tmp_path):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    identity = RNS.Identity()
    peer_hash = destination_hash(identity)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(peer_hash, identity.get_public_key(), name="alice", now=100.0)
    registry.save()
    return data_dir, peer_hash


def test_peers_alias_sets_lists_and_unaliases(tmp_path, capsys):
    data_dir, peer_hash = _seed_alias_peer(tmp_path)
    code = cli.main(
        ["peers", "alias", peer_hash.hex(), "my-laptop",
         "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "alias 'my-laptop' set for alice" in capsys.readouterr().out

    code = cli.main(["peers", "--data-dir", str(data_dir)])
    assert code == 0
    assert "my-laptop (alice)" in capsys.readouterr().out

    code = cli.main(
        ["peers", "unalias", "my-laptop", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "removed alias for alice" in capsys.readouterr().out

    code = cli.main(["peers", "--data-dir", str(data_dir)])
    assert code == 0
    assert "my-laptop" not in capsys.readouterr().out


def test_peers_alias_json_includes_alias(tmp_path, capsys):
    data_dir, peer_hash = _seed_alias_peer(tmp_path)
    code = cli.main(
        ["peers", "alias", "alice", "my-laptop",
         "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["alias"] == "my-laptop"
    assert payload["name"] == "alice"


def test_peers_alias_unknown_peer_and_duplicates(tmp_path, capsys):
    data_dir, peer_hash = _seed_alias_peer(tmp_path)
    code = cli.main(
        ["peers", "alias", "nobody", "spare", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err

    assert cli.main(
        ["peers", "alias", peer_hash.hex(), "spare",
         "--data-dir", str(data_dir)]
    ) == 0
    capsys.readouterr()

    identity = RNS.Identity()
    other_hash = destination_hash(identity)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    registry = PeerRegistry(config.peers_path)
    registry.upsert(other_hash, identity.get_public_key(), name="carol", now=200.0)
    registry.save()

    code = cli.main(
        ["peers", "alias", "carol", "SPARE", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "already used" in capsys.readouterr().err


def test_parser_peers_alias_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["peers", "alias", "bob", "laptop"])
    assert args.command == "peers"
    assert args.peers_command == "alias"
    assert args.peer == "bob"
    assert args.alias == "laptop"

    args = parser.parse_args(["peers", "unalias", "laptop"])
    assert args.peers_command == "unalias"
    assert args.peer == "laptop"

    plain = parser.parse_args(["peers"])
    assert plain.command == "peers"
    assert plain.peers_command is None


def test_allowlist_add_ignores_aliases_and_lists_asserted_names(tmp_path, capsys):
    data_dir, peer_hash = _seed_alias_peer(tmp_path)
    # A second peer aliased "alice" must not be the one allow-listed when
    # the operator names "alice".
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    identity = RNS.Identity()
    other_hash = destination_hash(identity)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(other_hash, identity.get_public_key(), name="carol", now=200.0)
    registry.set_alias(other_hash, "alice")
    registry.save()

    code = cli.main(["allowlist", "add", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    capsys.readouterr()

    code = cli.main(
        ["allowlist", "list", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [entry["destination"] for entry in payload["entries"]] == [
        peer_hash.hex()
    ]
    assert [entry["name"] for entry in payload["entries"]] == ["alice"]

    code = cli.main(["allowlist", "list", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert f"{peer_hash.hex()} (alice)" in out
    assert "carol" not in out


def test_parser_messages_export_import_arguments():
    parser = cli.build_parser()
    export = parser.parse_args(
        ["messages", "export", "--peer", "alice", "--output", "/tmp/out.json"]
    )
    assert export.func is cli.cmd_messages_export
    assert export.peer == "alice"
    assert str(export.output) == "/tmp/out.json"

    import_args = parser.parse_args(["messages", "import", "backup.json"])
    assert import_args.func is cli.cmd_messages_import
    assert str(import_args.file) == "backup.json"


def test_messages_export_import_round_trip(tmp_path, capsys):
    source_dir, sender_hash = _seed_messages(tmp_path)
    export_path = tmp_path / "messages-backup.json"

    code = cli.main(
        ["messages", "export", "--data-dir", str(source_dir), "--output", str(export_path)]
    )
    assert code == 0
    assert "Exported 2 messages" in capsys.readouterr().out
    document = json.loads(export_path.read_text(encoding="utf-8"))
    assert document["format"] == "tau-messages-export"
    assert [entry["id"] for entry in document["messages"]] == ["m-1", "m-2"]

    target_dir = tmp_path / "other-node"
    code = cli.main(
        ["messages", "import", "--data-dir", str(target_dir), str(export_path)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Imported 2 messages (0 already present, 0 skipped)" in out

    code = cli.main(["messages", "--data-dir", str(target_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-2", "m-1"]

    # Re-importing the same file reports both as present, not as new.
    code = cli.main(
        ["messages", "import", "--data-dir", str(target_dir), str(export_path)]
    )
    assert code == 0
    assert "Imported 0 messages (2 already present, 0 skipped)" in capsys.readouterr().out


def test_messages_export_to_stdout_and_peer_filter(tmp_path, capsys):
    source_dir, sender_hash = _seed_messages(tmp_path)
    code = cli.main(["messages", "export", "--data-dir", str(source_dir)])
    assert code == 0
    document = json.loads(capsys.readouterr().out)
    assert len(document["messages"]) == 2

    code = cli.main(
        [
            "messages",
            "export",
            "--data-dir",
            str(source_dir),
            "--peer",
            sender_hash.hex()[:8],
        ]
    )
    assert code == 0
    document = json.loads(capsys.readouterr().out)
    assert len(document["messages"]) == 2

    code = cli.main(
        ["messages", "export", "--data-dir", str(source_dir), "--peer", "nobody"]
    )
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err


def test_messages_import_rejects_bad_files(tmp_path, capsys):
    target_dir = tmp_path / "node"
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{not json", encoding="utf-8")
    code = cli.main(
        ["messages", "import", "--data-dir", str(target_dir), str(bad_json)]
    )
    assert code == 1
    assert "not valid JSON" in capsys.readouterr().err

    wrong_format = tmp_path / "wrong.json"
    wrong_format.write_text(
        json.dumps({"format": "other", "version": 1, "messages": []}),
        encoding="utf-8",
    )
    code = cli.main(
        ["messages", "import", "--data-dir", str(target_dir), str(wrong_format)]
    )
    assert code == 1
    assert "unsupported export format" in capsys.readouterr().err


def _seed_reply_conversation(tmp_path):
    """One received message and one sent message from the same peer."""
    from tau.protocol import excerpt_hash

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    peer_hash = "ab" * 16
    inbox.append(
        {
            "from": "alice",
            "destination": peer_hash,
            "text": "original thought",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "a" * 32,
            "direction": "in",
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": peer_hash,
            "text": "seconded",
            "sent_at": 11.0,
            "received_at": 21.0,
            "id": "b" * 32,
            "direction": "in",
            "in_reply_to": {
                "id": "a" * 32,
                "excerpt": excerpt_hash("original thought"),
            },
        }
    )
    return data_dir, peer_hash


def test_messages_renders_quote_from_local_history(tmp_path, capsys):
    data_dir, _ = _seed_reply_conversation(tmp_path)
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "↩ alice: original thought" in out
    assert "seconded" in out


def test_messages_reports_unavailable_original(tmp_path, capsys):
    from tau.protocol import excerpt_hash

    data_dir, peer_hash = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": peer_hash,
            "text": "reply to a ghost",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "c" * 32,
            "direction": "in",
            "in_reply_to": {
                "id": "d" * 32,
                "excerpt": excerpt_hash("never arrived"),
            },
        }
    )
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    assert "↩ original not available" in capsys.readouterr().out


def test_messages_quote_with_wrong_excerpt_is_unavailable(tmp_path, capsys):
    data_dir, peer_hash = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": peer_hash,
            "text": "quoting something else",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "e" * 32,
            "direction": "in",
            "in_reply_to": {"id": "a" * 32, "excerpt": "0" * 16},
        }
    )
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    assert "↩ original not available" in capsys.readouterr().out


def test_resolve_reply_target_scopes_to_the_conversation(tmp_path):
    data_dir, peer_hash = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)

    ref = cli._resolve_reply_target(inbox, mailbox, "aaaa", peer_hash)
    assert ref["id"] == "a" * 32
    # Another peer's message with the same id shape must not resolve.
    with pytest.raises(cli.NodeError, match="no message"):
        cli._resolve_reply_target(inbox, mailbox, "aaaa", "cd" * 16)
    with pytest.raises(cli.NodeError, match="no message"):
        cli._resolve_reply_target(inbox, mailbox, "zzzz", peer_hash)


def test_resolve_reply_target_rejects_prefix_shared_across_stores(tmp_path):
    data_dir, peer_hash = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    # A queued message to the same peer shares the "aaaa" prefix with the
    # received one, so the reference is ambiguous and must be refused.
    mailbox.enqueue(
        to=bytes.fromhex(peer_hash),
        text="queued",
        message_id="aaaa" + "c" * 28,
    )
    with pytest.raises(cli.NodeError, match="2 messages"):
        cli._resolve_reply_target(inbox, mailbox, "aaaa", peer_hash)


def test_send_reply_to_resolves_prefix_and_sends_reference(
    tmp_path, capsys, monkeypatch
):
    data_dir, _ = _seed_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    code = cli.main(
        [
            "send",
            "--data-dir",
            str(data_dir),
            "--to",
            "alice",
            "--reply-to",
            "aaaa",
            "seconded, again",
        ]
    )
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert len(node.sent) == 1
    _, _, in_reply_to = node.sent[0]
    assert in_reply_to["id"] == "a" * 32
    assert in_reply_to["excerpt"] == cli.excerpt_hash("original thought")
    assert "in reply to aaaaaaaa" in capsys.readouterr().out


def test_send_reply_to_offline_unregistered_hash_does_not_discover(
    tmp_path, capsys, monkeypatch
):
    data_dir, peer_hash = _seed_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()

    def _no_discovery(self, target, timeout=10.0):
        raise cli.NodeError("unknown peer")

    monkeypatch.setattr(_FakeSendNode, "resolve_peer", _no_discovery)
    code = cli.main(
        [
            "send",
            "--data-dir",
            str(data_dir),
            "--to",
            peer_hash,
            "--reply-to",
            "aaaa",
            "seconded offline",
        ]
    )
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert len(node.sent) == 1
    assert node.sent[0][2]["id"] == "a" * 32


def test_send_reply_to_unknown_reference_is_an_error(
    tmp_path, capsys, monkeypatch
):
    data_dir, _ = _seed_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    code = cli.main(
        [
            "send",
            "--data-dir",
            str(data_dir),
            "--to",
            "alice",
            "--reply-to",
            "zzzz",
            "dangling",
        ]
    )
    assert code == 1
    assert "no message" in capsys.readouterr().err
    assert _FakeSendNode.instances[0].sent == []


def test_messages_renders_reactions(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)
    inbox = Inbox(TauConfig.resolve(data_dir=data_dir, name="bob").inbox_path)
    inbox.record_reaction(
        {
            "target": "m-1",
            "destination": sender_hash.hex(),
            "from": "alice",
            "reaction": "❤️",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "react-1",
        }
    )
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "❤️ alice" in out


def test_parser_react_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["react", "abcdef12", "👍"])
    assert args.command == "react"
    assert args.message == "abcdef12"
    assert args.reaction == "👍"
    assert args.clear is False

    cleared = parser.parse_args(["react", "abcdef12", "--clear"])
    assert cleared.reaction is None
    assert cleared.clear is True


def test_react_json_and_clear(tmp_path, capsys, monkeypatch):
    data_dir, sender_hash = _seed_messages(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(
        ["react", "--data-dir", str(data_dir), "m-2", "--clear", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["target"] == "m-2"
    assert payload["to"] == sender_hash.hex()
    assert payload["cleared"] is True
    assert _FakeReactNode.instances[0].sent == [
        (sender_hash.hex(), "m-2", "")
    ]


def test_react_rejects_a_blank_token_without_clear(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_messages(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()
    code = cli.main(["react", "--data-dir", str(data_dir), "m-1", "   "])
    assert code == cli._EXIT_ERROR
    assert "--clear" in capsys.readouterr().err
    assert _FakeReactNode.instances == []


def test_react_rejects_an_oversized_token(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_messages(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()
    code = cli.main(["react", "--data-dir", str(data_dir), "m-1", "x" * 33])
    assert code == cli._EXIT_ERROR
    assert "longer than 32" in capsys.readouterr().err
    assert _FakeReactNode.instances == []


def test_react_rejects_unknown_or_ambiguous_message(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_messages(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["react", "--data-dir", str(data_dir), "zzz", "👍"])
    assert code == cli._EXIT_ERROR
    assert "unknown message" in capsys.readouterr().err
    assert _FakeReactNode.instances == []


def test_react_requires_exactly_one_of_token_or_clear(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)
    code = cli.main(["react", "--data-dir", str(data_dir), "m-1"])
    assert code == cli._EXIT_ERROR
    code = cli.main(["react", "--data-dir", str(data_dir), "m-1", "👍", "--clear"])
    assert code == cli._EXIT_ERROR


def test_react_sends_to_the_message_author(tmp_path, capsys, monkeypatch):
    data_dir, sender_hash = _seed_messages(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["react", "--data-dir", str(data_dir), "m-1", "👍"])
    assert code == 0
    out = capsys.readouterr().out
    assert "👍" in out
    node = _FakeReactNode.instances[0]
    assert node.started is False
    assert node.sent == [(sender_hash.hex(), "m-1", "👍")]


class _FakeReactNode:
    instances = []

    def __init__(self, config, name=None):
        self.config = config
        self.sent = []
        self.started = False
        type(self).instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.started = False

    def send_reaction(
        self, target, message_id, reaction, *, timeout=20.0, discover_timeout=None
    ):
        self.sent.append((target, message_id, reaction))
        return {"ok": True, "reaction": reaction, "target": message_id, "id": "r-1"}

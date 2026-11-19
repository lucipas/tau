from __future__ import annotations

import hashlib
import json
import logging
import time
import os
import stat
from types import SimpleNamespace

import RNS
import pytest
import RNS

from tau import cli
from tau.blocklist import BlockList, save_blocklist
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


def test_parser_trace_and_mesh_arguments():
    parser = cli.build_parser()

    traced = parser.parse_args(["trace", "alice"])
    assert traced.command == "trace"
    assert traced.peer == "alice"
    assert traced.timeout == 15.0
    assert traced.discover_timeout == 10.0
    assert traced.func is cli.cmd_trace

    meshed = parser.parse_args(["mesh"])
    assert meshed.command == "mesh"
    assert meshed.func is cli.cmd_mesh


def test_parser_send_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["send", "--to", "alice", "hello world"])
    assert args.command == "send"
    assert args.to == ["alice"]
    assert args.message == "hello world"
    assert args.timeout == 20.0
    assert args.expire is None
    assert args.kind is None

    kinded = parser.parse_args(["send", "--to", "alice", "--kind", "ping", "hi"])
    assert kinded.kind == "ping"

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


def test_parser_send_delay():
    parser = cli.build_parser()
    args = parser.parse_args(["send", "--to", "alice", "--delay", "5m", "hello"])
    assert args.delay == "5m"


def test_send_delay_schedules_without_delivery(tmp_path, capsys, monkeypatch):
    before = time.time()
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--delay", "2h", "later"],
        behaviour={"alice": "queued"},
    )
    assert code == 0
    node = _FakeSendNode.instances[-1]
    assert len(node.send_kwargs) == 1
    send_after = node.send_kwargs[0]["send_after"]
    assert before + 7200 <= send_after <= time.time() + 7200
    out = capsys.readouterr().out
    assert "scheduled for alice" in out
    assert "sends in 1h" in out
    assert "it will be delivered at its scheduled time" in out


def test_send_delay_json_reports_send_after(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--json", "--to", "alice", "--delay", "5m", "later"],
        behaviour={"alice": "queued"},
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["queued"] is True
    assert payload["send_after"] is not None
    assert payload["send_after"] > time.time()


def test_send_delay_rejects_bad_combinations(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--delay", "5m", "--no-queue", "hi"],
    )
    assert code == 2
    assert "--delay cannot be combined with --no-queue" in capsys.readouterr().err
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--delay", "2h", "--expire", "1h", "hi"],
    )
    assert code == 2
    assert "--delay must be shorter than --expire" in capsys.readouterr().err
    code = _run_send(
        tmp_path, monkeypatch, ["--to", "alice", "--delay", "soon", "hi"]
    )
    assert code == 1
    assert "invalid --delay duration" in capsys.readouterr().err
    code = _run_send(
        tmp_path, monkeypatch, ["--to", "alice", "--delay", "31d", "hi"]
    )
    assert code == 1
    assert "--delay duration must be at most 30d" in capsys.readouterr().err
    # 30d is past the effective maximum: the mail TTL less an hour
    # of delivery slack. 29d fits and queues.
    code = _run_send(
        tmp_path, monkeypatch, ["--to", "alice", "--delay", "30d", "hi"]
    )
    assert code == 2
    assert "--delay must be at most 29d23h" in capsys.readouterr().err
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--delay", "29d", "hi"],
        behaviour={"alice": "queued"},
    )
    assert code == 0


def test_parser_send_location():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["send", "--to", "alice", "--location", "34.05,-83.88 Rock Springs Park"]
    )
    assert args.location == "34.05,-83.88 Rock Springs Park"
    args = parser.parse_args(["group", "send", "weekend", "--location", "34.05,-83.88"])
    assert args.location == "34.05,-83.88"


def test_send_location_reaches_node_and_defaults_text(
    tmp_path, capsys, monkeypatch
):
    # A bare pin needs no message text: the place name becomes the
    # text, and the pin rides as a structured field.  stdin is not a
    # TTY under pytest, so stand in a TTY for the default-text branch.
    import io
    import sys

    class _TtyStdin(io.StringIO):
        def isatty(self):
            return True

    monkeypatch.setattr(sys, "stdin", _TtyStdin())
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--location", "34.05,-83.88 Rock Springs Park"],
        behaviour={"alice": "delivered"},
    )
    assert code == 0
    node = _FakeSendNode.instances[-1]
    assert node.calls == [("alice", "Rock Springs Park")]
    assert node.send_kwargs[0]["location"] == {
        "lat": 34.05,
        "lon": -83.88,
        "name": "Rock Springs Park",
    }
    assert node.send_kwargs[0]["kind"] is None


def test_send_location_with_text_keeps_text(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["--to", "alice", "--location", "34.05,-83.88", "meet here"],
        behaviour={"alice": "delivered"},
    )
    assert code == 0
    node = _FakeSendNode.instances[-1]
    assert node.calls == [("alice", "meet here")]
    assert node.send_kwargs[0]["location"] == {"lat": 34.05, "lon": -83.88}


def test_send_location_rejects_bad_coordinates(tmp_path, capsys, monkeypatch):
    code = _run_send(
        tmp_path, monkeypatch, ["--to", "alice", "--location", "91,0", "hi"]
    )
    assert code == 1
    assert "out of range" in capsys.readouterr().err
    code = _run_send(
        tmp_path, monkeypatch, ["--to", "alice", "--location", "not-a-place", "hi"]
    )
    assert code == 1
    assert "--location needs" in capsys.readouterr().err


def test_print_message_entry_shows_location_pin(tmp_path, capsys):
    from tau.inbox import Inbox
    from tau.peers import PeerRegistry

    config = TauConfig.resolve(data_dir=tmp_path / "node", name="bob")
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    entry = {
        "from": "alice",
        "destination": "ab" * 16,
        "text": "Rock Springs Park",
        "kind": "location",
        "sent_at": 10.0,
        "received_at": 20.0,
        "id": "msg-1",
        "direction": "in",
        "location": {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"},
    }
    cli._print_message_entry(
        inbox, registry, entry, blocked=set(), own_hash="00" * 16
    )
    out = capsys.readouterr().out
    assert "📍 Rock Springs Park — 34.05, -83.88" in out
    assert "openstreetmap.org" in out
    assert "mlat=34.05&mlon=-83.88" in out


def test_outbox_lists_scheduled_entry_next_try(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()  # drain the --json output of `tau init`
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    box = Mailbox(config.mailbox_path)
    box.enqueue(
        to=b"\xbb" * 16,
        text="later",
        message_id="dd" + "44" * 15,
        send_after=time.time() + 7200.0,
    )
    code = cli.main(["outbox", "--data-dir", str(data_dir)])
    assert code == 0
    assert "in 1h" in capsys.readouterr().out


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
        self.send_kwargs: list = []
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
        self.send_kwargs.append(kwargs)
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
                send_after=kwargs.get("send_after"),
            )
            # The real node records the send when it queues (see
            # TauNode.send_message), so the sent record exists for the
            # queued-to-delivered flip to confirm against.
            self.mailbox.record_sent(entry.id, to=b"\xbb" * 16, text=text)
            return {
                "ok": True,
                "queued": True,
                "id": entry.id,
                "to": entry.to_hex,
                **(
                    {"send_after": entry.send_after}
                    if entry.send_after is not None
                    else {}
                ),
            }
        return {"ok": True, "echo": text, "_rtt": 0.05}

    def flush_outbox(self, **kwargs):
        self.flush_calls.append(kwargs)
        if _FakeSendNode.deliver_on_flush:
            for entry in self.mailbox.outbox_list():
                # The real node records delivery before leaving the outbox.
                self.mailbox.record_delivered(entry.id)
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


def test_app_log_level_mapping():
    from types import SimpleNamespace

    from tau.cli import _app_log_level, _rns_log_level

    assert _app_log_level(SimpleNamespace()) == logging.WARNING
    assert _app_log_level(SimpleNamespace(verbose=1)) == logging.INFO
    assert _app_log_level(SimpleNamespace(verbose=2)) == logging.DEBUG
    assert _app_log_level(SimpleNamespace(verbose=3)) == logging.DEBUG
    assert (
        _app_log_level(SimpleNamespace(quiet=True, verbose=2)) == logging.ERROR
    )
    assert _rns_log_level(SimpleNamespace()) == 2
    assert _rns_log_level(SimpleNamespace(verbose=1)) == 4
    assert _rns_log_level(SimpleNamespace(verbose=2)) == 5
    assert _rns_log_level(SimpleNamespace(quiet=True)) == 1
    assert _rns_log_level(SimpleNamespace(rns_log_level=7)) == 7
    assert _rns_log_level(SimpleNamespace(rns_log_level=99)) == 8
    assert _rns_log_level(SimpleNamespace(rns_log_level=-1)) == 0


def test_send_verbose_narrates_route_and_progress(
    tmp_path, capsys, caplog, monkeypatch
):
    # The logger level is forced here so the test proves the log calls
    # exist and say the right things; the flag-to-level mapping itself
    # is covered by test_app_log_level_mapping.
    caplog.set_level(logging.DEBUG, logger="tau.cli")
    code = _run_send(
        tmp_path,
        monkeypatch,
        ["-vv", "--to", "alice", "--to", "bob", "hi"],
        behaviour={"alice": "delivered", "bob": "queued"},
    )
    assert code == 0
    out = capsys.readouterr().out
    # Verbosity goes to stderr (logs); stdout keeps its historical shape.
    assert "delivered to alice" in out
    assert "queued for bob" in out
    assert "local one-shot node" not in out
    assert "sending to alice" not in out
    records = [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == "tau.cli"
    ]
    assert any(
        level == logging.DEBUG
        and "no serving node holds the data directory" in message
        for level, message in records
    )
    assert any(
        level == logging.INFO
        and "sending via a local one-shot node" in message
        for level, message in records
    )
    assert any(
        level == logging.DEBUG
        and "sending to alice via the local node" in message
        for level, message in records
    )
    assert any(
        level == logging.DEBUG
        and "send to alice finished" in message
        and "delivered" in message
        for level, message in records
    )
    assert any(
        level == logging.DEBUG
        and "send to bob finished" in message
        and "queued" in message
        for level, message in records
    )


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


def test_parser_group_topic_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["group", "topic", "weekend", "planning the meetup"])
    assert args.group_command == "topic"
    assert args.group == "weekend"
    assert args.text == "planning the meetup"
    assert args.clear is False
    assert args.file is None

    shown = parser.parse_args(["group", "topic", "weekend"])
    assert shown.text is None

    cleared = parser.parse_args(["group", "topic", "weekend", "--clear"])
    assert cleared.clear is True


def test_group_topic_set_show_and_clear(tmp_path, capsys):
    from tau.groups import GroupStore

    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()  # drain the --json output of `tau init`
    own_hash = _id_destination(data_dir, capsys)
    store = GroupStore(data_dir / "groups.json")
    group = store.create("weekend", [own_hash, "cd" * 16], created_by=own_hash)

    base = ["group", "topic", "--data-dir", str(data_dir), "weekend"]
    assert cli.main(base) == 0
    assert "has no topic set" in capsys.readouterr().out

    assert cli.main([*base, "planning the meetup"]) == 0
    out = capsys.readouterr().out
    assert "planning the meetup" in out
    assert "next message" in out
    # The CLI wrote through its own store instance: re-read from disk.
    assert (
        GroupStore(data_dir / "groups.json").get(group["id"])["topic"]
        == "planning the meetup"
    )

    assert cli.main([*base, "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["group"]["topic"] == "planning the meetup"

    assert cli.main(["group", "list", "--data-dir", str(data_dir)]) == 0
    assert "topic: planning the meetup" in capsys.readouterr().out

    assert cli.main([*base, "--clear"]) == 0
    assert "cleared the topic" in capsys.readouterr().out
    assert GroupStore(data_dir / "groups.json").get(group["id"])["topic"] == ""

    # A group this node did not create refuses a topic change.
    store.create("other", ["cd" * 16, "ef" * 16], created_by="cd" * 16)
    assert (
        cli.main(["group", "topic", "--data-dir", str(data_dir), "other", "forged"])
        == 1
    )
    assert "only the creator" in capsys.readouterr().err


def test_group_topic_flag_conflicts_rejected(tmp_path, capsys):
    from tau.groups import GroupStore

    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()  # drain the --json output of `tau init`
    own_hash = _id_destination(data_dir, capsys)
    store = GroupStore(data_dir / "groups.json")
    group = store.create("weekend", [own_hash, "cd" * 16], created_by=own_hash)

    base = ["group", "topic", "--data-dir", str(data_dir), "weekend"]
    # --file together with a positional text is rejected, not silently won.
    assert cli.main([*base, "some text", "--file", "whatever.txt"]) == 1
    assert "not both" in capsys.readouterr().err
    # --file together with --clear is rejected, not silently dropped.
    assert cli.main([*base, "--file", "whatever.txt", "--clear"]) == 1
    assert "not both" in capsys.readouterr().err
    # A blank file is rejected instead of silently clearing the topic.
    blank = tmp_path / "blank.txt"
    blank.write_text("   \n", encoding="utf-8")
    assert cli.main([*base, "--file", str(blank)]) == 1
    assert "blank" in capsys.readouterr().err
    assert (
        GroupStore(data_dir / "groups.json").get(group["id"]).get("topic", "")
        == ""
    )


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


def test_peers_table_flags_lookalike_and_rename(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    registry = PeerRegistry(config.peers_path)
    verified = RNS.Identity()
    registry.upsert(
        destination_hash(verified),
        verified.get_public_key(),
        name="alice",
        now=1000.0,
    )
    registry.mark_verified(destination_hash(verified), "00001", now=1100.0)
    lookalike = RNS.Identity()
    registry.upsert(
        destination_hash(lookalike),
        lookalike.get_public_key(),
        name="Alice",
        now=1200.0,
    )
    renamed = RNS.Identity()
    registry.upsert(
        destination_hash(renamed),
        renamed.get_public_key(),
        name="carol",
        now=1000.0,
    )
    registry.mark_verified(destination_hash(renamed), "00002", now=1100.0)
    registry.upsert(
        destination_hash(renamed),
        renamed.get_public_key(),
        name="caroline",
        now=1200.0,
    )
    registry.save()

    code = cli.main(["peers", "--data-dir", str(data_dir)])
    assert code == 0
    captured = capsys.readouterr()
    assert "lookalike of alice" in captured.out
    assert "announces with the same name as alice" in captured.err
    assert "changed its announced name from 'carol'" in captured.err

    code = cli.main(["peers", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    by_name = {entry["name"]: entry for entry in payload["peers"]}
    assert by_name["Alice"]["lookalike_of"] == "alice"
    assert by_name["alice"]["lookalike_of"] is None
    assert by_name["caroline"]["previous_name"] == "carol"
    assert by_name["caroline"]["name_change_at"] == 1200.0


# -- heard --------------------------------------------------------------------

_HEARD_RECORD = {
    "destination": "ab" * 16,
    "hops": 2,
    "first_seen": 1000.0,
    "last_seen": 2000.0,
    "count": 3,
}


class _FakeHeardNode:
    """Stub TauNode for `tau heard`: records how it was started."""

    def __init__(self, config, name=None, on_foreign=None):
        self.started = {}
        self.on_foreign = on_foreign
        if on_foreign is not None:
            on_foreign(dict(_HEARD_RECORD))

    def start(self, **kwargs):
        self.started.update(kwargs)

    def stop(self):
        pass

    def foreign_announces(self):
        return [dict(_HEARD_RECORD)]


def test_heard_lists_foreign_announces(tmp_path, capsys, monkeypatch):
    started = {}

    def _factory(config, name=None, on_foreign=None):
        node = _FakeHeardNode(config, name=name, on_foreign=on_foreign)
        started.update(node.started)
        started["node"] = node
        return node

    monkeypatch.setattr(cli, "TauNode", _factory)
    code = cli.main(["heard", "--data-dir", str(tmp_path / "node"), "--duration", "0"])
    assert code == 0
    assert started["node"].started.get("announce") is False
    out = capsys.readouterr().out
    assert "ab" * 16 in out
    assert "1 non-tau announce(s) in 0.0s" in out


def test_heard_json(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(cli, "TauNode", _FakeHeardNode)
    code = cli.main(
        ["heard", "--data-dir", str(tmp_path / "node"), "--duration", "0", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["heard"][0]["destination"] == "ab" * 16
    assert payload["heard"][0]["count"] == 3
    assert payload["duration"] == 0.0


def test_heard_on_empty_air(tmp_path, capsys, monkeypatch):
    class _SilentHeardNode(_FakeHeardNode):
        def __init__(self, config, name=None, on_foreign=None):
            self.started = {}

        def foreign_announces(self):
            return []

    monkeypatch.setattr(cli, "TauNode", _SilentHeardNode)
    code = cli.main(["heard", "--data-dir", str(tmp_path / "node"), "--duration", "0"])
    assert code == 0
    assert "No non-tau announces heard." in capsys.readouterr().out


def test_heard_rejects_non_finite_duration(tmp_path, capsys, monkeypatch):
    # A Qodo Fixer commit keeps --duration inf from waiting forever
    # (and emitting invalid JSON): it must fail before a node starts.
    monkeypatch.setattr(cli, "TauNode", _FakeHeardNode)
    code = cli.main(["heard", "--data-dir", str(tmp_path / "node"), "--duration", "inf"])
    assert code == 1
    assert "must be a finite number of seconds" in capsys.readouterr().err


# -- peers show ---------------------------------------------------------------

def _seed_peer_show(tmp_path):
    """One peer with inbox messages, a queued message and a held envelope."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    identity = RNS.Identity()
    peer_hash = destination_hash(identity)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        peer_hash,
        identity.get_public_key(),
        name="alice",
        hops=2,
        caps=("mail", "tan"),
        now=1000.0,
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    for index, text in enumerate(("first hello", "second hello")):
        inbox.append(
            {
                "from": "alice",
                "destination": peer_hash.hex(),
                "text": text,
                "sent_at": 10.0 + index,
                "received_at": 20.0 + index,
                "id": f"m-{index + 1}",
            }
        )
    box = Mailbox(config.mailbox_path)
    box.enqueue(to=peer_hash, text="queued hello", to_name="alice", now=1000.0)
    box.hold(
        to=peer_hash,
        blob=b"sealed-bytes",
        message_id="held-1",
        expires_at=time.time() + 3600,
    )
    return data_dir, identity, peer_hash


def test_parser_peers_show_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["peers", "show", "alice"])
    assert args.command == "peers"
    assert args.peers_command == "show"
    assert args.target == "alice"
    assert args.func is cli.cmd_peers_show

    plain = parser.parse_args(["peers"])
    assert plain.peers_command is None
    assert plain.func is cli.cmd_peers


def test_peers_show_by_name(tmp_path, capsys):
    data_dir, identity, peer_hash = _seed_peer_show(tmp_path)
    code = cli.main(["peers", "show", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert peer_hash.hex() in out
    assert lxmf_delivery_hash(identity).hex() in out
    assert "first seen:" in out
    assert "last seen:" in out
    assert "hops:         2" in out
    assert "protocol:     1" in out
    assert "caps:         mail, tan" in out
    assert "via exchange: no" in out
    assert "queued mail:  1 message(s) waiting for this peer" in out
    assert "held mail:    1 sealed message(s) held for this peer" in out
    assert "received:     2 message(s) from this peer in the inbox" in out


def test_peers_show_by_hash_prefix_and_full_hash(tmp_path, capsys):
    data_dir, _, peer_hash = _seed_peer_show(tmp_path)
    code = cli.main(
        ["peers", "show", peer_hash.hex()[:8], "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "alice" in capsys.readouterr().out

    code = cli.main(["peers", "show", peer_hash.hex(), "--data-dir", str(data_dir)])
    assert code == 0
    assert "alice" in capsys.readouterr().out


def test_peers_show_json(tmp_path, capsys):
    data_dir, identity, peer_hash = _seed_peer_show(tmp_path)
    code = cli.main(["peers", "show", "alice", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["name"] == "alice"
    assert payload["destination"] == peer_hash.hex()
    assert payload["lxmf"] == lxmf_delivery_hash(identity).hex()
    assert payload["public_key"] == identity.get_public_key().hex()
    assert payload["first_seen"] == 1000.0
    assert payload["last_seen"] == 1000.0
    assert payload["hops"] == 2
    assert payload["protocol"] == 1
    assert payload["caps"] == ["mail", "tan"]
    assert payload["via_exchange"] is False
    assert payload["queued"] == 1
    assert payload["held"] == 1
    assert payload["received"] == 2


def test_peers_show_unknown_peer(tmp_path, capsys):
    data_dir, _, _ = _seed_peer_show(tmp_path)
    code = cli.main(["peers", "show", "nobody", "--data-dir", str(data_dir)])
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err


def test_peers_show_ambiguous_name(tmp_path, capsys):
    data_dir, _, first_hash = _seed_peer_show(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    registry = PeerRegistry(config.peers_path)
    other = RNS.Identity()
    registry.upsert(
        destination_hash(other), other.get_public_key(), name="alice", now=1000.0
    )
    registry.save()
    code = cli.main(["peers", "show", "alice", "--data-dir", str(data_dir)])
    assert code == 1
    assert "use a destination hash" in capsys.readouterr().err
    # A full hash still resolves unambiguously.
    code = cli.main(
        ["peers", "show", first_hash.hex(), "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert first_hash.hex() in capsys.readouterr().out


def test_peers_show_via_exchange(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    source = PeerRegistry(config.peers_path)
    identity = RNS.Identity()
    peer_hash = destination_hash(identity)
    peer = source.upsert(peer_hash, identity.get_public_key(), name="carol")
    registry = PeerRegistry(config.peers_path)
    merged = registry.merge_exchanged([peer.exchange_dict()])
    assert merged["added"] == 1
    registry.save()
    code = cli.main(["peers", "show", "carol", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "via exchange: yes — never heard directly" in out
    assert "queued mail:  0 message(s)" in out
    assert "received:     0 message(s)" in out


def _init_owner_node(tmp_path, capsys):
    """Init a node directory and drain the init payload."""
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
    capsys.readouterr()
    return data_dir


def test_parser_profile_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(
        [
            "profile",
            "set",
            "--bio",
            "hello",
            "--link",
            "https://a.example",
            "--link",
            "https://b.example",
        ]
    )
    assert args.command == "profile"
    assert args.profile_command == "set"
    assert args.bio == "hello"
    assert args.links == ["https://a.example", "https://b.example"]
    assert args.func is cli.cmd_profile_set

    show = parser.parse_args(["profile", "show"])
    assert show.profile_command == "show"
    assert show.func is cli.cmd_profile_show

    peer_profile = parser.parse_args(["peers", "profile", "alice"])
    assert peer_profile.peers_command == "profile"
    assert peer_profile.target == "alice"
    assert peer_profile.cached is False
    assert peer_profile.func is cli.cmd_peers_profile

    # A bare `tau profile` names no subcommand: argparse must fail
    # cleanly instead of dispatching with no function set.
    with pytest.raises(SystemExit):
        parser.parse_args(["profile"])


def test_parser_page_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["page", "publish", "meetup.md"])
    assert args.command == "page"
    assert args.page_command == "publish"
    assert args.file == "meetup.md"
    assert args.title is None
    assert args.func is cli.cmd_page_publish

    titled = parser.parse_args(
        ["page", "publish", "meetup.md", "--title", "Mesh meetup"]
    )
    assert titled.title == "Mesh meetup"

    show = parser.parse_args(["page", "show"])
    assert show.page_command == "show"
    assert show.func is cli.cmd_page_show

    fetch = parser.parse_args(["page", "fetch", "alice"])
    assert fetch.page_command == "fetch"
    assert fetch.target == "alice"
    assert fetch.cached is False
    assert fetch.timeout == 15.0
    assert fetch.func is cli.cmd_page_fetch

    allow = parser.parse_args(["page", "allow", "add", "alice"])
    assert allow.page_command == "allow"
    assert allow.page_allow_command == "add"
    assert allow.peer == "alice"
    assert allow.func is cli.cmd_page_allow

    allow_list = parser.parse_args(["page", "allow", "list"])
    assert allow_list.page_allow_command == "list"
    assert allow_list.func is cli.cmd_page_allow

    # A bare `tau page` names no subcommand: argparse must fail
    # cleanly instead of dispatching with no function set.
    with pytest.raises(SystemExit):
        parser.parse_args(["page"])


def test_page_publish_and_show_end_to_end(tmp_path, capsys):
    from tau.pages import load_local_page

    data_dir = _init_owner_node(tmp_path, capsys)
    page_file = tmp_path / "meetup.md"
    page_file.write_text(
        "# Mesh meetup\n\nSaturday, 2pm, Rock Springs Park.\n",
        encoding="utf-8",
    )

    code = cli.main(
        ["page", "publish", str(page_file), "--data-dir", str(data_dir)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Published page version 1 — Mesh meetup." in out

    stored = load_local_page(data_dir / "page.json")
    assert stored is not None
    assert stored["version"] == 1
    assert stored["title"] == "Mesh meetup"
    assert "Rock Springs Park" in stored["markdown"]

    # A second publish bumps the version and takes the new title.
    page_file.write_text(
        "# New heading\n\nMoved to Sunday.\n", encoding="utf-8"
    )
    code = cli.main(
        [
            "page",
            "publish",
            str(page_file),
            "--title",
            "Sunday meetup",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    assert "Published page version 2 — Sunday meetup." in capsys.readouterr().out
    stored = load_local_page(data_dir / "page.json")
    assert stored["version"] == 2
    assert stored["title"] == "Sunday meetup"

    code = cli.main(["page", "show", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["version"] == 2
    assert payload["title"] == "Sunday meetup"
    assert "Moved to Sunday" in payload["markdown"]
    assert "key" not in payload and "sig" not in payload

    code = cli.main(["page", "show", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "title:   Sunday meetup" in out
    assert "version: 2" in out
    assert "Moved to Sunday" in out


def test_page_show_without_page(tmp_path, capsys):
    data_dir = _init_owner_node(tmp_path, capsys)
    code = cli.main(["page", "show", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No page published yet" in capsys.readouterr().out


def test_page_publish_rejects_oversize_file(tmp_path, capsys):
    from tau.pages import load_local_page

    data_dir = _init_owner_node(tmp_path, capsys)
    page_file = tmp_path / "big.md"
    page_file.write_text("# Big\n\n" + "x" * 9000, encoding="utf-8")
    code = cli.main(
        ["page", "publish", str(page_file), "--data-dir", str(data_dir)]
    )
    assert code != 0
    assert load_local_page(data_dir / "page.json") is None


def test_parser_page_file_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["page", "file", "alice", "notes.txt", "--to", "out.txt", "--force"]
    )
    assert args.page_command == "file"
    assert args.target == "alice"
    assert args.selector == "notes.txt"
    assert args.to == "out.txt"
    assert args.force is True
    assert args.cached is False
    assert args.timeout == 15.0
    assert args.func is cli.cmd_page_file

    publish = parser.parse_args(
        ["page", "publish", "meetup.md", "--file", "a.txt", "--file", "b.txt"]
    )
    assert publish.attach == ["a.txt", "b.txt"]
    assert publish.file == "meetup.md"
    assert publish.func is cli.cmd_page_publish


def test_page_publish_with_files_end_to_end(tmp_path, capsys):
    import json

    from tau.pages import hash_page_file, load_local_page, load_page_file

    data_dir = _init_owner_node(tmp_path, capsys)
    page_file = tmp_path / "meetup.md"
    page_file.write_text(
        "# Mesh meetup\n\nSaturday, 2pm, Rock Springs Park.\n",
        encoding="utf-8",
    )
    notes = tmp_path / "notes.txt"
    notes.write_bytes(b"meetup notes")
    flyer = tmp_path / "flyer.txt"
    flyer.write_bytes(b"bring a chair")

    code = cli.main(
        [
            "page",
            "publish",
            str(page_file),
            "--file",
            str(notes),
            "--file",
            str(flyer),
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Published page version 1 — Mesh meetup." in out
    assert "linked notes.txt (12 bytes)" in out
    assert "linked flyer.txt (13 bytes)" in out

    stored = load_local_page(data_dir / "page.json")
    assert stored is not None
    assert len(stored["files"]) == 2
    assert stored["files"][0]["name"] == "notes.txt"
    assert stored["files"][0]["sha256"] == hash_page_file(b"meetup notes")
    assert stored["files"][0]["size"] == 12
    assert (
        load_page_file(data_dir / "page-files", stored["files"][0]["sha256"])
        == b"meetup notes"
    )

    code = cli.main(["page", "show", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "notes.txt (12 bytes, downloaded)" in out
    assert "flyer.txt (13 bytes, downloaded)" in out

    code = cli.main(["page", "show", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [entry["name"] for entry in payload["files"]] == [
        "notes.txt",
        "flyer.txt",
    ]


def test_page_publish_rejects_too_many_and_duplicate_files(tmp_path, capsys):
    data_dir = _init_owner_node(tmp_path, capsys)
    page_file = tmp_path / "meetup.md"
    page_file.write_text("# Mesh meetup\n\nSaturday.\n", encoding="utf-8")
    attached = []
    for index in range(6):
        path = tmp_path / f"file{index}.txt"
        path.write_bytes(b"x")
        attached.append(str(path))
    code = cli.main(
        ["page", "publish", str(page_file), "--data-dir", str(data_dir)]
        + [item for path in attached for item in ("--file", path)]
    )
    assert code != 0

    first = tmp_path / "a" / "notes.txt"
    first.parent.mkdir()
    first.write_bytes(b"one")
    second = tmp_path / "b" / "notes.txt"
    second.parent.mkdir()
    second.write_bytes(b"two")
    code = cli.main(
        [
            "page",
            "publish",
            str(page_file),
            "--file",
            str(first),
            "--file",
            str(second),
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code != 0


def test_page_publish_rejects_oversize_attached_file(tmp_path, capsys):
    from tau.pages import load_local_page

    data_dir = _init_owner_node(tmp_path, capsys)
    page_file = tmp_path / "meetup.md"
    page_file.write_text("# Mesh meetup\n\nSaturday.\n", encoding="utf-8")
    big = tmp_path / "big.bin"
    big.write_bytes(b"x" * (32 * 1024 + 1))
    code = cli.main(
        [
            "page",
            "publish",
            str(page_file),
            "--file",
            str(big),
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code != 0
    assert load_local_page(data_dir / "page.json") is None


def _seed_peer_page(tmp_path, data_dir, *, with_blob=True):
    """A peer 'alice' with a stored signed page linking one file."""
    import RNS

    from tau.config import TauConfig
    from tau.pages import (
        PageStore,
        build_page,
        make_page_file_entry,
        store_page_file,
    )
    from tau.peers import PeerRegistry
    from tau.protocol import APP_NAME, ASPECT

    config = TauConfig.resolve(data_dir=data_dir)
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    data = b"meetup notes and map"
    entry = make_page_file_entry("notes.txt", data)
    doc = build_page(
        identity,
        title="Mesh meetup",
        markdown="# Mesh meetup\n\nSaturday, 2pm.\n",
        files=[entry],
        version=1,
        updated_at=1000.0,
    )
    registry = PeerRegistry(config.peers_path)
    registry.upsert(destination, identity.get_public_key(), name="alice", now=1.0)
    registry.save()
    store = PageStore(config.pages_path)
    store.store(doc)
    store.save()
    if with_blob:
        store_page_file(config.page_files_dir, entry["sha256"], data)
    return entry, data


def test_page_file_cached_end_to_end(tmp_path, capsys):
    import json

    data_dir = _init_owner_node(tmp_path, capsys)
    entry, data = _seed_peer_page(tmp_path, data_dir)
    out = tmp_path / "out.txt"

    code = cli.main(
        [
            "page",
            "file",
            "alice",
            "notes.txt",
            "--cached",
            "--to",
            str(out),
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    assert out.read_bytes() == data
    assert "Saved notes.txt (20 bytes)" in capsys.readouterr().out

    # The same file by SHA-256 prefix, in JSON mode.
    out2 = tmp_path / "out2.txt"
    code = cli.main(
        [
            "page",
            "file",
            "alice",
            entry["sha256"][:12],
            "--cached",
            "--to",
            str(out2),
            "--data-dir",
            str(data_dir),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["file"]["name"] == "notes.txt"
    assert out2.read_bytes() == data

    # Refuses to overwrite without --force.
    code = cli.main(
        [
            "page",
            "file",
            "alice",
            "notes.txt",
            "--cached",
            "--to",
            str(out),
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code != 0


def test_page_file_cached_without_blob_fails(tmp_path, capsys):
    data_dir = _init_owner_node(tmp_path, capsys)
    _seed_peer_page(tmp_path, data_dir, with_blob=False)
    out = tmp_path / "out.txt"
    code = cli.main(
        [
            "page",
            "file",
            "alice",
            "notes.txt",
            "--cached",
            "--to",
            str(out),
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code != 0
    assert not out.exists()

    code = cli.main(
        [
            "page",
            "file",
            "alice",
            "missing.txt",
            "--cached",
            "--to",
            str(out),
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code != 0


def test_profile_set_and_show_end_to_end(tmp_path, capsys):
    from tau.profile import load_local_profile

    data_dir = _init_owner_node(tmp_path, capsys)
    avatar = tmp_path / "avatar.png"
    avatar.write_bytes(b"fake image bytes")

    code = cli.main(
        [
            "profile",
            "set",
            "--display-name",
            "Owner",
            "--bio",
            "mesh tinkerer",
            "--avatar",
            str(avatar),
            "--link",
            "https://example.com",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Published profile version 1 for Owner." in out
    assert "mesh tinkerer" in out
    assert "https://example.com" in out

    stored = load_local_profile(data_dir / "profile.json")
    assert stored is not None
    assert stored["version"] == 1
    assert stored["bio"] == "mesh tinkerer"
    assert stored["links"] == ["https://example.com"]
    assert stored["avatar"] == hashlib.sha256(b"fake image bytes").hexdigest()

    # A second set bumps the version and keeps what was not re-set.
    code = cli.main(
        ["profile", "set", "--bio", "still tinkering", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "Published profile version 2" in capsys.readouterr().out
    stored = load_local_profile(data_dir / "profile.json")
    assert stored["version"] == 2
    assert stored["bio"] == "still tinkering"
    assert stored["links"] == ["https://example.com"]
    assert stored["name"] == "Owner"

    code = cli.main(["profile", "show", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["version"] == 2
    assert payload["bio"] == "still tinkering"
    assert payload["links"] == ["https://example.com"]

    code = cli.main(["profile", "show", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "name:    Owner" in out
    assert "bio:     still tinkering" in out
    assert "version: 2" in out


def test_profile_show_without_profile(tmp_path, capsys):
    data_dir = _init_owner_node(tmp_path, capsys)
    code = cli.main(["profile", "show", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No profile published yet" in capsys.readouterr().out

    code = cli.main(["profile", "show", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"profile": None}


def test_profile_set_rejects_bad_values(tmp_path, capsys):
    data_dir = _init_owner_node(tmp_path, capsys)
    base = ["profile", "set", "--data-dir", str(data_dir)]

    code = cli.main(base + ["--bio", "x" * 281])
    assert code == 1
    assert "longer than 280" in capsys.readouterr().err

    code = cli.main(
        base
        + [
            "--link",
            "https://a.example",
            "--link",
            "https://b.example",
            "--link",
            "https://c.example",
            "--link",
            "https://d.example",
        ]
    )
    assert code == 1
    assert "at most 3 links" in capsys.readouterr().err

    code = cli.main(base + ["--link", "ftp://example.com/file"])
    assert code == 1
    assert "must be an http(s) URL" in capsys.readouterr().err

    code = cli.main(base + ["--avatar", "no-such-file.png"])
    assert code == 1
    assert "neither an existing file" in capsys.readouterr().err


def test_profile_set_without_identity(tmp_path, capsys):
    code = cli.main(
        ["profile", "set", "--bio", "hi", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 1
    assert "no identity" in capsys.readouterr().err


def _seed_peer_profile(tmp_path):
    """One peer in the registry with a signed profile in the store."""
    from tau.profile import ProfileStore, build_profile

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    peer_identity = RNS.Identity()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=100.0,
    )
    registry.save()
    doc = build_profile(
        peer_identity,
        name="alice",
        bio="mesh tinkerer",
        avatar="ab" * 16,
        links=["https://example.com"],
        version=2,
        created_at=1000.0,
    )
    store = ProfileStore(config.profiles_path)
    assert store.store(doc) is True
    assert store.save() is True
    return data_dir, peer_identity, doc


def test_peers_profile_cached(tmp_path, capsys):
    data_dir, _, _ = _seed_peer_profile(tmp_path)
    code = cli.main(
        ["peers", "profile", "alice", "--cached", "--data-dir", str(data_dir)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "name:    alice" in out
    assert "bio:     mesh tinkerer" in out
    assert "avatar:  " + "ab" * 16 in out
    assert "link:    https://example.com" in out
    assert "version: 2" in out

    code = cli.main(
        [
            "peers",
            "profile",
            "alice",
            "--cached",
            "--data-dir",
            str(data_dir),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"]["bio"] == "mesh tinkerer"
    assert payload["profile"]["version"] == 2
    assert payload["fetched"] is False


def test_peers_profile_cached_without_profile(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    registry = PeerRegistry(config.peers_path)
    identity = RNS.Identity()
    registry.upsert(
        destination_hash(identity),
        identity.get_public_key(),
        name="alice",
        now=100.0,
    )
    registry.save()
    code = cli.main(
        ["peers", "profile", "alice", "--cached", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "has no public profile yet" in capsys.readouterr().out


def test_peers_profile_unknown_peer(tmp_path, capsys):
    data_dir, _, _ = _seed_peer_profile(tmp_path)
    code = cli.main(
        ["peers", "profile", "nobody", "--cached", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "no peer matches" in capsys.readouterr().err


def _seed_peer_follow_list(tmp_path):
    """One peer in the registry with a signed follow list in the store."""
    from tau.follows import FollowListStore, build_follow_list

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    peer_identity = RNS.Identity()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=100.0,
    )
    registry.save()
    followed = [
        destination_hash(RNS.Identity()).hex(),
        destination_hash(RNS.Identity()).hex(),
    ]
    doc = build_follow_list(
        peer_identity, authors=followed, version=2, updated_at=1000.0
    )
    store = FollowListStore(config.follow_lists_path)
    assert store.store(doc) is True
    assert store.save() is True
    return data_dir, peer_identity, doc


def test_parser_tan_follow_list_arguments():
    parser = cli.build_parser()
    publish = parser.parse_args(["tan", "publish-follows"])
    assert publish.tan_command == "publish-follows"
    assert publish.func is cli.cmd_tan_publish_follows

    follows_of = parser.parse_args(["tan", "follows-of", "alice"])
    assert follows_of.tan_command == "follows-of"
    assert follows_of.target == "alice"
    assert follows_of.cached is False
    assert follows_of.func is cli.cmd_tan_follows_of

    cached = parser.parse_args(["tan", "follows-of", "alice", "--cached"])
    assert cached.cached is True

    followers = parser.parse_args(["tan", "followers"])
    assert followers.tan_command == "followers"
    assert followers.func is cli.cmd_tan_followers


def test_tan_publish_follows_end_to_end(tmp_path, capsys):
    from tau.follows import load_local_follow_list
    from tau.tangents.tan import follow, init_db

    data_dir = _init_owner_node(tmp_path, capsys)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    followed_hashes = [
        destination_hash(RNS.Identity()).hex(),
        destination_hash(RNS.Identity()).hex(),
    ]
    conn = init_db(config.social_db_path)
    try:
        for author in followed_hashes:
            follow(conn, author, name="peer")
    finally:
        conn.close()

    code = cli.main(["tan", "publish-follows", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "Published follow list version 1 (2 author(s))." in out

    stored = load_local_follow_list(data_dir / "follow-list.json")
    assert stored is not None
    assert stored["version"] == 1
    assert stored["authors"] == sorted(followed_hashes)

    # A second publish bumps the version and keeps the same list.
    code = cli.main(["tan", "publish-follows", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Published follow list version 2" in capsys.readouterr().out
    stored = load_local_follow_list(data_dir / "follow-list.json")
    assert stored["version"] == 2
    assert stored["authors"] == sorted(followed_hashes)

    code = cli.main(
        ["tan", "publish-follows", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["version"] == 3
    assert payload["authors"] == sorted(followed_hashes)
    assert "key" not in payload and "sig" not in payload


def test_tan_publish_follows_empty_list(tmp_path, capsys):
    from tau.follows import load_local_follow_list

    data_dir = _init_owner_node(tmp_path, capsys)
    code = cli.main(["tan", "publish-follows", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Published follow list version 1 (0 author(s))." in capsys.readouterr().out
    stored = load_local_follow_list(data_dir / "follow-list.json")
    assert stored is not None and stored["authors"] == []


def test_tan_follows_of_cached(tmp_path, capsys):
    data_dir, _, doc = _seed_peer_follow_list(tmp_path)
    code = cli.main(
        ["tan", "follows-of", "alice", "--cached", "--data-dir", str(data_dir)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "alice follows:" in out
    assert "version: 2" in out
    for author in doc["authors"]:
        assert author in out

    code = cli.main(
        [
            "tan",
            "follows-of",
            "alice",
            "--cached",
            "--data-dir",
            str(data_dir),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["follow_list"]["authors"] == doc["authors"]
    assert payload["follow_list"]["version"] == 2
    assert payload["fetched"] is False


def test_tan_follows_of_cached_without_list(tmp_path, capsys):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    registry = PeerRegistry(config.peers_path)
    identity = RNS.Identity()
    registry.upsert(
        destination_hash(identity),
        identity.get_public_key(),
        name="alice",
        now=100.0,
    )
    registry.save()
    code = cli.main(
        ["tan", "follows-of", "alice", "--cached", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "has not published a follow list yet" in capsys.readouterr().out


def test_tan_follows_of_unknown_peer(tmp_path, capsys):
    data_dir, _, _ = _seed_peer_follow_list(tmp_path)
    code = cli.main(
        ["tan", "follows-of", "nobody", "--cached", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "no peer matches" in capsys.readouterr().err


def test_tan_followers_lists_stored_followers(tmp_path, capsys):
    from tau.follows import FollowListStore, build_follow_list
    from tau.tangents.tan import create_post, init_db

    data_dir = _init_owner_node(tmp_path, capsys)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    owner = load_identity(config.identity_path)
    owner_author = destination_hash(owner).hex()
    peer_identity = RNS.Identity()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=100.0,
    )
    registry.save()
    conn = init_db(config.social_db_path)
    try:
        create_post(conn, peer_identity, "hello from alice", name="alice", now=1000)
    finally:
        conn.close()
    doc = build_follow_list(
        peer_identity, authors=[owner_author], version=1, updated_at=1000.0
    )
    store = FollowListStore(config.follow_lists_path)
    assert store.store(doc) is True
    assert store.save() is True

    code = cli.main(["tan", "followers", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "1 post(s)" in out

    code = cli.main(["tan", "followers", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["followers"][0]["author"] == destination_hash(peer_identity).hex()
    assert payload["followers"][0]["name"] == "alice"
    assert payload["followers"][0]["posts"] == 1
    assert payload["followers"][0]["followed"] is False


def test_tan_followers_empty_when_no_list_names_you(tmp_path, capsys):
    from tau.follows import FollowListStore, build_follow_list

    data_dir = _init_owner_node(tmp_path, capsys)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    peer_identity = RNS.Identity()
    doc = build_follow_list(
        peer_identity,
        authors=[destination_hash(RNS.Identity()).hex()],
        version=1,
        updated_at=1000.0,
    )
    store = FollowListStore(config.follow_lists_path)
    assert store.store(doc) is True
    assert store.save() is True

    code = cli.main(["tan", "followers", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No followers yet" in capsys.readouterr().out

    code = cli.main(["tan", "followers", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"followers": [], "count": 0}


def test_tan_followers_needs_identity(tmp_path, capsys):
    code = cli.main(["tan", "followers", "--data-dir", str(tmp_path / "empty")])
    assert code == 1
    assert "no identity" in capsys.readouterr().err


def test_peers_show_includes_profile(tmp_path, capsys):
    data_dir, _, _ = _seed_peer_profile(tmp_path)
    code = cli.main(["peers", "show", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "profile:" in out
    assert "bio:     mesh tinkerer" in out
    assert "link:    https://example.com" in out

    code = cli.main(
        ["peers", "show", "alice", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"]["bio"] == "mesh tinkerer"
    assert payload["profile"]["version"] == 2


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
    assert args.kind is None

    kinded = parser.parse_args(["messages", "--kind", "ping"])
    assert kinded.kind == "ping"

    defaults = parser.parse_args(["messages"])
    assert defaults.peer is None
    assert defaults.limit == 30
    assert defaults.kind is None
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


def test_quote_line_prefers_logical_reference_over_foreign_leg_id(tmp_path):
    """A group reply that carries the logical reference resolves
    against this node's own copy even when the reply names another
    member's leg id — and even when the same text appears under
    another author, where the excerpt fallback cannot attribute the
    quote.
    """
    from tau.protocol import excerpt_hash

    inbox = Inbox(tmp_path / "inbox.json")
    registry = PeerRegistry(tmp_path / "peers.json")
    group_id = "cd" * 16
    alice_hash = "aa" * 16
    bob_hash = "bb" * 16
    for author, author_hash, leg_id in (
        ("alice", alice_hash, "a1" * 16),
        ("bob", bob_hash, "b1" * 16),
    ):
        inbox.append(
            {
                "from": author,
                "destination": author_hash,
                "text": "same words",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "id": leg_id,
                "kind": "group",
                "group": group_id,
                "group_name": "crew",
            }
        )
    reply = {
        "destination": bob_hash,
        "from": "bob",
        "text": "seconded",
        "sent_at": 1002.0,
        "received_at": 1003.0,
        "id": "r1" * 16,
        "kind": "group",
        "group": group_id,
        "in_reply_to": {
            "id": "f" * 32,
            "excerpt": excerpt_hash("same words"),
            "group": group_id,
            "author": alice_hash,
            "sent_at": 1000.0,
        },
    }
    assert (
        cli._quote_line(inbox, registry, reply, blocked=frozenset(), own_hash=None)
        == "↩ alice: same words"
    )
    # Without the logical reference the same reply cannot resolve:
    # the wire id names a leg this node never held, and the excerpt
    # matches two authors' messages, so no author can be named.
    bare = dict(reply)
    bare["in_reply_to"] = {
        "id": "f" * 32,
        "excerpt": excerpt_hash("same words"),
    }
    assert (
        cli._quote_line(inbox, registry, bare, blocked=frozenset(), own_hash=None)
        == "↩ original not available"
    )


def test_messages_hides_non_member_group_reaction(tmp_path, capsys):
    """A stored reaction from a non-member never prints on a group
    message, even when the authorization recheck or its cleanup failed
    and left it in the inbox (CodeRabbit round-11 follow-up).

    The copy is appended directly and the reaction stored directly —
    the way a copy imported by another process lands, with no
    arrival purge running in this process — so the listing's roster
    filter is the only thing standing between the stored reaction
    and the display.  A member's reaction on the same message still
    prints.
    """
    from tau.groups import GroupStore, derive_group_id

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    member = RNS.Identity()
    member_hash = destination_hash(member)
    outsider = RNS.Identity()
    outsider_hash = destination_hash(outsider)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(sender_hash, sender.get_public_key(), name="alice", now=100.0)
    registry.upsert(member_hash, member.get_public_key(), name="carol", now=100.0)
    registry.upsert(
        outsider_hash, outsider.get_public_key(), name="mallory", now=100.0
    )
    registry.save()
    groups = GroupStore(config.groups_path)
    members = [sender_hash.hex(), member_hash.hex()]
    group_id = derive_group_id(sender_hash.hex(), "crew", members)
    assert groups.record(
        group_id,
        "crew",
        members,
        creator=sender_hash.hex(),
        sender=sender_hash.hex(),
    )
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "crew hello",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-g1",
        }
    )
    inbox.record_reaction(
        {
            "target": "m-g1",
            "destination": outsider_hash.hex(),
            "from": "mallory",
            "reaction": "👎",
            "sent_at": 21.0,
            "received_at": 22.0,
            "id": "r-1",
        }
    )
    inbox.record_reaction(
        {
            "target": "m-g1",
            "destination": member_hash.hex(),
            "from": "carol",
            "reaction": "👍",
            "sent_at": 21.0,
            "received_at": 22.0,
            "id": "r-2",
        }
    )
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "crew hello" in out
    assert "👍 carol" in out
    assert "👎" not in out
    assert "mallory" not in out


def test_messages_hides_all_reactions_when_no_roster_pinned(tmp_path, capsys):
    """A group copy imported without its roster shows no reactions at
    all — not even a member's — until the roster pins (CodeRabbit
    Major, follow-up to the round-16 fail-closed rule).  With no
    roster there is no membership to verify, so the listing hides
    every stored reaction instead of trusting them; once the group
    pins, the member's reaction prints and the outsider's stays
    hidden.
    """
    from tau.groups import GroupStore, derive_group_id

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    member = RNS.Identity()
    member_hash = destination_hash(member)
    outsider = RNS.Identity()
    outsider_hash = destination_hash(outsider)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(sender_hash, sender.get_public_key(), name="alice", now=100.0)
    registry.upsert(member_hash, member.get_public_key(), name="carol", now=100.0)
    registry.upsert(
        outsider_hash, outsider.get_public_key(), name="mallory", now=100.0
    )
    registry.save()
    members = [sender_hash.hex(), member_hash.hex()]
    group_id = derive_group_id(sender_hash.hex(), "crew", members)
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "crew hello",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-g1",
        }
    )
    inbox.record_reaction(
        {
            "target": "m-g1",
            "destination": outsider_hash.hex(),
            "from": "mallory",
            "reaction": "👎",
            "sent_at": 21.0,
            "received_at": 22.0,
            "id": "r-1",
        }
    )
    inbox.record_reaction(
        {
            "target": "m-g1",
            "destination": member_hash.hex(),
            "from": "carol",
            "reaction": "👍",
            "sent_at": 21.0,
            "received_at": 22.0,
            "id": "r-2",
        }
    )
    # No roster pinned: nothing prints, not even the member's.
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "crew hello" in out
    assert "👍" not in out
    assert "👎" not in out
    # Pin the roster: the member's reaction prints, the outsider's
    # stays hidden.
    groups = GroupStore(config.groups_path)
    assert groups.record(
        group_id,
        "crew",
        members,
        creator=sender_hash.hex(),
        sender=sender_hash.hex(),
    )
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "👍 carol" in out
    assert "👎" not in out
    assert "mallory" not in out


def test_messages_kind_filter_and_tag(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "are you there?",
            "kind": "ping",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-3",
        }
    )

    code = cli.main(["messages", "--data-dir", str(data_dir), "--kind", "ping"])
    assert code == 0
    out = capsys.readouterr().out
    assert "are you there?" in out
    assert "[ping]" in out
    assert "second hello" not in out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    kinds = {item["id"]: item["kind"] for item in payload["messages"]}
    assert kinds == {"m-3": "ping", "m-2": "chat", "m-1": "chat"}

    code = cli.main(["messages", "--data-dir", str(data_dir), "--kind", "has space"])
    assert code == 1
    assert "message kind" in capsys.readouterr().err


def test_parser_note_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["note", "buy milk"])
    assert args.command == "note"
    assert args.message == "buy milk"
    assert args.file is None

    args = parser.parse_args(["note", "--file", "notes.txt"])
    assert args.message is None
    assert str(args.file) == "notes.txt"


def test_note_saves_and_lists(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()

    code = cli.main(["note", "--data-dir", str(data_dir), "buy milk on the way home"])
    assert code == 0
    out = capsys.readouterr().out
    assert "note saved #" in out
    assert "[note]" in out
    assert "buy milk on the way home" in out

    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "buy milk on the way home" in out
    assert "[note]" in out
    assert "alice" in out
    # A note is read the moment it is written: no unread marker.
    assert "●" not in out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    assert "No unread messages." in capsys.readouterr().out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--kind", "note"])
    assert code == 0
    assert "buy milk on the way home" in capsys.readouterr().out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--kind", "chat"])
    assert code == 0
    assert "buy milk on the way home" not in capsys.readouterr().out

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--search", "MILK"]
    )
    assert code == 0
    assert "buy milk on the way home" in capsys.readouterr().out


def test_note_json_and_file(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    note_file = tmp_path / "note.txt"
    note_file.write_text("call bob about the mesh node\n", encoding="utf-8")

    code = cli.main(
        ["note", "--data-dir", str(data_dir), "--file", str(note_file), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["note"]["kind"] == "note"
    assert payload["note"]["text"] == "call bob about the mesh node"
    assert payload["note"]["from"] == "alice"
    assert payload["note"]["direction"] == "in"


def test_note_requires_identity(tmp_path, capsys):
    data_dir = tmp_path / "node"
    code = cli.main(["note", "--data-dir", str(data_dir), "hello"])
    assert code == 1
    assert "run `tau init` first" in capsys.readouterr().err


def test_note_rejects_empty_text(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()

    code = cli.main(["note", "--data-dir", str(data_dir), "   "])
    assert code == 1
    assert "must not be empty" in capsys.readouterr().err


def test_react_to_note_is_refused(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    assert cli.main(["note", "--data-dir", str(data_dir), "a note", "--json"]) == 0
    note_id = json.loads(capsys.readouterr().out)["note"]["id"]

    # A note's destination is this node, not a remote author: there is
    # no peer to react to, so refuse before starting a node.
    code = cli.main(["react", "--data-dir", str(data_dir), note_id, "👍"])
    assert code == 1
    assert "cannot be reacted to" in capsys.readouterr().err


def test_listing_notes_sends_no_receipt(tmp_path, capsys, monkeypatch):
    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    assert cli.main(["note", "--data-dir", str(data_dir), "just a note"]) == 0
    capsys.readouterr()

    started = []
    receipted = []

    class _RecordingNode:
        def __init__(self, *args, **kwargs):
            started.append(True)

        def start(self, **kwargs):
            pass

        def stop(self):
            pass

        def send_read_receipts(self, peer_hash, ids):
            receipted.append((peer_hash, list(ids)))
            return {"ids": list(ids), "ok": True}

    monkeypatch.setattr(cli, "TauNode", _RecordingNode)

    # Listing a note-only inbox must not start a node at all: there is
    # no peer to receipt, and receipting our own destination would make
    # the node discover itself.
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    assert "just a note" in capsys.readouterr().out
    assert started == []
    assert receipted == []

    # Control: the same listing with a real peer message does reach
    # for a node, so the skip above proves the filter, not a dead path.
    data_dir2 = _init_node(tmp_path, "node2", "bob")
    capsys.readouterr()
    config2 = TauConfig.resolve(data_dir=data_dir2, name="bob")
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    registry2 = PeerRegistry(config2.peers_path)
    registry2.upsert(sender_hash, sender.get_public_key(), name="alice", now=100.0)
    registry2.save()
    Inbox(config2.inbox_path).append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "hello bob",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-1",
        }
    )
    code = cli.main(["messages", "--data-dir", str(data_dir2)])
    assert code == 0
    assert "hello bob" in capsys.readouterr().out
    assert started == [True]
    assert receipted == [(sender_hash.hex(), ["m-1"])]


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
    assert payload == {"unread": {sender_hash.hex(): 2}, "unread_groups": {}, "total": 2}


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


def test_parser_messages_prune_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["messages", "prune", "--older-than", "90d"])
    assert args.command == "messages"
    assert args.older_than == "90d"
    assert args.func is cli.cmd_messages_prune
    assert args.dry_run is False

    args = parser.parse_args(
        [
            "messages",
            "prune",
            "--older-than",
            "90d",
            "--peer",
            "alice",
            "--dry-run",
        ]
    )
    assert args.peer == "alice"
    assert args.dry_run is True

    # Scope flags given before `prune` survive the subparser: the prune
    # subparser uses SUPPRESS defaults, so a None default cannot
    # silently discard the parent value and widen the prune.
    args = parser.parse_args(
        ["messages", "--peer", "alice", "prune", "--older-than", "90d"]
    )
    assert args.peer == "alice"
    assert args.func is cli.cmd_messages_prune
    args = parser.parse_args(
        ["messages", "--group", "team", "prune", "--older-than", "90d"]
    )
    assert args.group == "team"
    assert args.func is cli.cmd_messages_prune


def test_messages_prune_dry_run_reports_without_deleting(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(
        [
            "messages",
            "prune",
            "--older-than",
            "90d",
            "--dry-run",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Would prune 2 messages older than 90d" in out
    assert "dry run" in out

    # Nothing was deleted.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-2", "m-1"]


def test_messages_prune_deletes_and_persists(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(
        ["messages", "prune", "--older-than", "90d", "--data-dir", str(data_dir)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Pruned 2 messages older than 90d" in out
    assert "only this node's stored inbox copies were deleted" in out
    assert "Sent history shown by `tau messages --sent`" in out
    assert "still queued in the outbox remain stored locally" in out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["messages"] == []


def test_messages_prune_scopes_to_peer(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(
        [
            "messages",
            "prune",
            "--older-than",
            "90d",
            "--peer",
            "alice",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    assert "Pruned 2 messages older than 90d (with alice)" in capsys.readouterr().out


def test_messages_prune_keeps_scope_given_before_prune(tmp_path, capsys):
    from tau.groups import GroupStore

    data_dir, sender_hash = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "carol",
            "destination": "cd" * 16,
            "text": "carol old",
            "sent_at": 10.0,
            "received_at": 10.0,
            "id": "m-3",
        }
    )
    store = GroupStore(config.groups_path)
    group = store.create(
        "team", [sender_hash.hex(), "cd" * 16], created_by=sender_hash.hex()
    )
    inbox.append(
        {
            "from": "carol",
            "destination": "cd" * 16,
            "text": "group old",
            "sent_at": 10.0,
            "received_at": 10.0,
            "id": "m-4",
            "kind": "group",
            "group": group["id"],
            "group_name": "team",
        }
    )

    # --peer before `prune` still scopes the prune to alice: carol's
    # DM and group message survive.
    code = cli.main(
        [
            "messages",
            "--peer",
            "alice",
            "prune",
            "--older-than",
            "90d",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    assert "Pruned 2 messages older than 90d (with alice)" in capsys.readouterr().out
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-4", "m-3"]

    # --group before `prune` still scopes the prune to the group.
    code = cli.main(
        [
            "messages",
            "--group",
            "team",
            "prune",
            "--older-than",
            "90d",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    assert "Pruned 1 messages older than 90d (in group team)" in capsys.readouterr().out
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-3"]


def test_messages_prune_json_and_errors(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(
        [
            "messages",
            "prune",
            "--older-than",
            "90d",
            "--data-dir",
            str(data_dir),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["pruned"] == 2
    assert payload["dry_run"] is False

    # Unknown peer.
    code = cli.main(
        [
            "messages",
            "prune",
            "--older-than",
            "90d",
            "--peer",
            "nope",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err

    # Bad duration, and a cutoff past the 10y cap.
    code = cli.main(
        ["messages", "prune", "--older-than", "bogus", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "invalid --older-than duration" in capsys.readouterr().err
    code = cli.main(
        ["messages", "prune", "--older-than", "20y", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "must be at most 10y" in capsys.readouterr().err


def test_parser_pin_unpin_pinned_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["pin", "abcd1234"])
    assert args.message == "abcd1234"
    assert args.func is cli.cmd_pin

    args = parser.parse_args(["unpin", "abcd1234"])
    assert args.message == "abcd1234"
    assert args.func is cli.cmd_unpin

    args = parser.parse_args(["pinned", "--peer", "alice", "--limit", "5"])
    assert args.peer == "alice"
    assert args.limit == 5
    assert args.func is cli.cmd_pinned

    defaults = parser.parse_args(["pinned"])
    assert defaults.peer is None
    assert defaults.limit == 30


def test_pin_unpin_round_trip(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(["pin", "m-2", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "pinned [m-2] from alice" in out
    assert "pins are local-only" in out

    # The pin survives a fresh process and shows in both listings.
    code = cli.main(["pinned", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "second hello" in out
    assert "first hello" not in out

    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "📌" in out
    assert "#m-2" in out

    code = cli.main(["unpin", "m-2", "--data-dir", str(data_dir)])
    assert code == 0
    assert "unpinned [m-2] from alice" in capsys.readouterr().out

    code = cli.main(["pinned", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No pinned messages." in capsys.readouterr().out


def test_pin_json_and_errors(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    # Ambiguous prefix: both seeded ids start with "m-".
    code = cli.main(["pin", "m-", "--data-dir", str(data_dir)])
    assert code == 1
    assert "2 messages match prefix" in capsys.readouterr().err

    # Unknown id.
    code = cli.main(["pin", "nope", "--data-dir", str(data_dir)])
    assert code == 1
    assert "unknown message" in capsys.readouterr().err

    code = cli.main(["pin", "m-1", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["pinned"]["id"] == "m-1"
    assert payload["already_pinned"] is False

    # Pinning twice reports the pin was already there.
    code = cli.main(["pin", "m-1", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["already_pinned"] is True

    code = cli.main(["unpin", "m-1", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["unpinned"]["id"] == "m-1"
    assert payload["was_pinned"] is True

    # Unpinning an unpinned message is a no-op, not an error.
    code = cli.main(["unpin", "m-1", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["was_pinned"] is False


def test_pinned_peer_filter_and_json(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(["pinned", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No pinned messages." in capsys.readouterr().out

    cli.main(["pin", "m-1", "--data-dir", str(data_dir)])
    capsys.readouterr()

    code = cli.main(["pinned", "--peer", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    assert "first hello" in capsys.readouterr().out

    code = cli.main(["pinned", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["id"] == "m-1"
    assert payload["messages"][0]["pinned"] is True


def test_pinned_peer_prefix_finds_sent_pin(tmp_path, capsys):
    """A pinned sent message is findable by its recipient's hash prefix.

    The recipient is not in the registry and never sent anything, so
    only the sent record names them — the peer lookup must search
    sent pins too.
    """
    data_dir, _ = _seed_messages(tmp_path)
    recipient = "cd" * 16
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": recipient,
            "text": "sent pinned note",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "s-1",
            "direction": "out",
        }
    )
    code = cli.main(["pin", "s-1", "--data-dir", str(data_dir)])
    assert code == 0
    capsys.readouterr()

    code = cli.main(["pinned", "--peer", "cdcdcdcd", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "sent pinned note" in out
    # Sent pins render as from you, never as unread.
    assert "you →" in out
    assert "●" not in out

    code = cli.main(["pinned", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    sent = [item for item in payload["messages"] if item["id"] == "s-1"]
    assert sent and sent[0]["unread"] is False


def test_pinned_blocked_filter_applies_to_received_only(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)
    recipient = "cd" * 16
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": recipient,
            "text": "sent pinned note",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "s-1",
            "direction": "out",
        }
    )
    cli.main(["pin", "m-1", "--data-dir", str(data_dir)])
    cli.main(["pin", "s-1", "--data-dir", str(data_dir)])
    capsys.readouterr()

    def _pinned_text():
        code = cli.main(["pinned", "--data-dir", str(data_dir)])
        assert code == 0
        return capsys.readouterr().out

    # Blocking the sender hides the received pin, not the sent one.
    (data_dir / "blocklist.json").write_text(
        json.dumps({"blocked": [sender_hash.hex()]}), encoding="utf-8"
    )
    out = _pinned_text()
    assert "first hello" not in out
    assert "sent pinned note" in out

    # Blocking the recipient hides nothing: sent pins are your own.
    (data_dir / "blocklist.json").write_text(
        json.dumps({"blocked": [recipient]}), encoding="utf-8"
    )
    out = _pinned_text()
    assert "first hello" in out
    assert "sent pinned note" in out

    # An unreadable list hides received pins (fail closed) but keeps sent ones.
    (data_dir / "blocklist.json").write_text("{not json", encoding="utf-8")
    out = _pinned_text()
    assert "first hello" not in out
    assert "sent pinned note" in out


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


def test_parser_messages_unread_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["messages", "unread", "alice"])
    assert args.peer == "alice"
    assert args.all is False

    args = parser.parse_args(["messages", "unread", "alice", "--all"])
    assert args.all is True


def test_messages_unread_marks_newest_message_unread_again(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    # Listing marks both messages read.
    assert cli.main(["messages", "--data-dir", str(data_dir)]) == 0
    capsys.readouterr()

    code = cli.main(["messages", "unread", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    assert "marked 1 message from alice unread" in capsys.readouterr().out

    # Only the newest message is unread again; the older one stays read.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--unread", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["text"] == "second hello"


def test_messages_unread_all_marks_whole_conversation_unread(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)
    assert cli.main(["messages", "--data-dir", str(data_dir)]) == 0
    capsys.readouterr()

    code = cli.main(
        [
            "messages",
            "unread",
            "alice",
            "--all",
            "--data-dir",
            str(data_dir),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"peer": sender_hash.hex(), "unread": 2, "total": 2}


def test_messages_unread_group_scopes_to_the_group(tmp_path, capsys):
    from tau.groups import GroupStore, derive_group_id

    data_dir, sender_hash = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    carol = RNS.Identity()
    carol_hash = destination_hash(carol)
    members = [sender_hash.hex(), carol_hash.hex()]
    group_id = derive_group_id(sender_hash.hex(), "crew", members)
    GroupStore(config.groups_path).record(
        group_id, "crew", members, creator=sender_hash.hex()
    )
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "group hello",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "g-1",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
        }
    )

    # Listing marks everything read.
    assert cli.main(["messages", "--data-dir", str(data_dir)]) == 0
    capsys.readouterr()

    code = cli.main(
        [
            "messages",
            "unread",
            "--group",
            "crew",
            "--data-dir",
            str(data_dir),
        ]
    )
    assert code == 0
    assert "marked 1 message in group crew unread" in capsys.readouterr().out

    # Only the group post is unread again; the DMs stay read.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--unread", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["text"] == "group hello"


def test_messages_unread_group_covers_rename_lineage(tmp_path, capsys):
    from tau.groups import GroupStore, derive_group_id

    data_dir, sender_hash = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    carol = RNS.Identity()
    carol_hash = destination_hash(carol)
    dave = RNS.Identity()
    dave_hash = destination_hash(dave)
    members = [sender_hash.hex(), carol_hash.hex()]
    old_id = derive_group_id(sender_hash.hex(), "crew", members)
    groups = GroupStore(config.groups_path)
    groups.record(old_id, "crew", members, creator=sender_hash.hex())
    # A successor under the same name, carried by the creator so it
    # supersedes the predecessor, with the old id in its lineage.
    new_members = members + [dave_hash.hex()]
    new_id = derive_group_id(sender_hash.hex(), "crew", new_members)
    groups.record(
        new_id,
        "crew",
        new_members,
        creator=sender_hash.hex(),
        sender=sender_hash.hex(),
        renamed_from=[old_id],
    )
    inbox = Inbox(config.inbox_path)
    for message_id, group_id in (("g-old", old_id), ("g-new", new_id)):
        inbox.append(
            {
                "from": "alice",
                "destination": sender_hash.hex(),
                "text": f"hello {message_id}",
                "sent_at": 12.0,
                "received_at": 22.0,
                "id": message_id,
                "kind": "group",
                "group": group_id,
                "group_name": "crew",
            }
        )

    # Listing marks everything read.
    assert cli.main(["messages", "--data-dir", str(data_dir)]) == 0
    capsys.readouterr()

    code = cli.main(
        [
            "messages",
            "unread",
            "--group",
            "crew",
            "--all",
            "--data-dir",
            str(data_dir),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"group": new_id, "unread": 2, "total": 2}


def test_messages_unread_unknown_peer(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)

    code = cli.main(["messages", "unread", "nobody", "--data-dir", str(data_dir)])
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err


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
    assert payload == {"unread": {sender_hash.hex(): 2}, "unread_groups": {}, "total": 2}

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


def test_messages_count_ignores_sent_records(tmp_path, capsys):
    data_dir, sender_hash = _seed_messages(tmp_path)

    # A reply sent after reading the conversation must not inflate
    # the unread count: your own mail is never unread for you.
    inbox = Inbox(TauConfig.resolve(data_dir=data_dir, name="bob").inbox_path)
    inbox.mark_read()
    inbox.append(
        {
            "from": "bob",
            "destination": sender_hash.hex(),
            "text": "my reply",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-sent",
            "direction": "out",
        }
    )

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"unread": {}, "unread_groups": {}, "total": 0}


def test_messages_count_combined_filter_excludes_blocked_senders(tmp_path, capsys):
    from tau.groups import GroupStore

    data_dir, sender_hash = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    store = GroupStore(config.groups_path)
    group = store.create(
        "crew", [sender_hash.hex(), "cd" * 16], created_by=sender_hash.hex()
    )
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "crew chat",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-group",
            "kind": "group",
            "group": group["id"],
            "group_name": "crew",
        }
    )
    # The listing hides alice's posts once she is blocked; the
    # combined peer-and-group count must hide them too.
    save_blocklist(
        config.blocklist_path, BlockList(blocked=frozenset({sender_hash.hex()}))
    )

    code = cli.main(
        [
            "messages",
            "--data-dir",
            str(data_dir),
            "--count",
            "--peer",
            "alice",
            "--group",
            "crew",
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"unread": {}, "unread_groups": {}, "total": 0}


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
    assert payload == {"unread": {sender_hash.hex(): 3}, "unread_groups": {}, "total": 3}

    # The text listing prints the same hint, and the plain listing
    # truncates the same way.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--limit", "2"])
    assert code == 0
    out = capsys.readouterr().out
    assert "1 more unread from alice" in out
    assert "raise --limit to see them" in out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count", "--json"])
    assert json.loads(capsys.readouterr().out)["total"] == 3


def test_messages_dm_listing_does_not_consume_hidden_group_post(tmp_path, capsys):
    """A hidden group post must not pin the DM cursor (issue #409)."""
    from tau.ignored_groups import IgnoredGroups, save_ignored_groups

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(sender_hash, sender.get_public_key(), name="alice", now=100.0)
    registry.save()
    group_id = "11" * 16
    inbox = Inbox(config.inbox_path)
    # The group post arrives first: under the old per-sender cursor
    # it would cap the DM advance at seq 0, so the shown DM would
    # reappear as unread forever.
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "group hello",
            "sent_at": 9.0,
            "received_at": 19.0,
            "id": "g-1",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
        }
    )
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
    save_ignored_groups(
        config.ignored_groups_path, IgnoredGroups(frozenset({group_id}))
    )

    # The default listing hides the ignored group's post and shows
    # the DMs; showing the DM must mark it read without consuming
    # the group post the owner never saw.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-2", "m-1"]

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "unread": {},
        "unread_groups": {group_id: 1},
        "total": 1,
    }


def test_messages_count_empty(tmp_path, capsys):
    code = cli.main(["messages", "--data-dir", str(tmp_path / "node"), "--count"])
    assert code == 0
    assert "No unread messages." in capsys.readouterr().out

    code = cli.main(
        ["messages", "--data-dir", str(tmp_path / "node"), "--count", "--json"]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"unread": {}, "unread_groups": {}, "total": 0}
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

        def start(
            self, *, announce=True, exclusive=False, lxmf=False,
            lxmf_propagation=False
        ):
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


# -- peers note ---------------------------------------------------------------

def test_peers_note_sets_shows_and_clears(tmp_path, capsys):
    data_dir, peer_hash = _seed_alias_peer(tmp_path)
    code = cli.main(
        ["peers", "note", peer_hash.hex(), "met at ATO",
         "--data-dir", str(data_dir)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "note for alice" in out
    assert "met at ATO" in out

    code = cli.main(["peers", "note", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "note for alice" in out
    assert "met at ATO" in out

    code = cli.main(
        ["peers", "show", "alice", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "note:         met at ATO" in capsys.readouterr().out

    code = cli.main(
        ["peers", "note", "alice", "--clear", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "removed note for alice" in capsys.readouterr().out

    code = cli.main(["peers", "note", "alice", "--data-dir", str(data_dir)])
    assert code == 0
    assert "no note set for alice" in capsys.readouterr().out


def test_peers_note_json_and_errors(tmp_path, capsys):
    data_dir, peer_hash = _seed_alias_peer(tmp_path)
    code = cli.main(
        ["peers", "note", "alice", "roof node operator",
         "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["note"] == "roof node operator"
    assert payload["name"] == "alice"

    code = cli.main(
        ["peers", "note", "nobody", "hello", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err

    code = cli.main(
        ["peers", "note", "alice", "   ", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "must not be empty" in capsys.readouterr().err

    code = cli.main(
        ["peers", "note", "alice", "hello", "--clear",
         "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "cannot save a note and --clear" in capsys.readouterr().err

    code = cli.main(
        ["peers", "note", "alice", "--clear", "--data-dir", str(data_dir)]
    )
    assert code == 0
    capsys.readouterr()
    code = cli.main(
        ["peers", "note", "alice", "--clear", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "has no note" in capsys.readouterr().err


def test_parser_peers_note_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["peers", "note", "bob", "hello there"])
    assert args.command == "peers"
    assert args.peers_command == "note"
    assert args.peer == "bob"
    assert args.text == "hello there"
    assert args.clear is False
    assert args.func is cli.cmd_peers_note

    args = parser.parse_args(["peers", "note", "bob"])
    assert args.text is None

    args = parser.parse_args(["peers", "note", "bob", "--clear"])
    assert args.clear is True


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


def test_messages_import_skips_retracted_group_messages(tmp_path, capsys):
    # Regression test (CodeRabbit Major on PR #322): a group message
    # retracted on the target node must not come back through an
    # export made before the retract.
    source_dir, sender_hash = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=source_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "crew hello",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "g-1",
            "kind": "group",
            "group": "cc" * 16,
        }
    )
    export_path = tmp_path / "messages-backup.json"

    code = cli.main(
        ["messages", "export", "--data-dir", str(source_dir), "--output", str(export_path)]
    )
    assert code == 0

    target_dir = tmp_path / "other-node"
    # The sender retracted the group message on the target node before
    # the import.
    mailbox = Mailbox(target_dir / "mailbox.json")
    mailbox.record_group_retraction(sender_hash.hex(), "cc" * 16, 12.0)

    code = cli.main(
        ["messages", "import", "--data-dir", str(target_dir), str(export_path)]
    )
    assert code == 1
    out = capsys.readouterr().out
    assert "Imported 2 messages (0 already present, 1 skipped)" in out
    assert "skipped g-1: message was retracted by its sender" in out

    code = cli.main(["messages", "--data-dir", str(target_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-2", "m-1"]


def test_messages_import_skips_retracted_messages(tmp_path, capsys):
    source_dir, sender_hash = _seed_messages(tmp_path)
    export_path = tmp_path / "messages-backup.json"

    code = cli.main(
        ["messages", "export", "--data-dir", str(source_dir), "--output", str(export_path)]
    )
    assert code == 0

    target_dir = tmp_path / "other-node"
    # The sender retracted m-1 on the target node before the import.
    mailbox = Mailbox(target_dir / "mailbox.json")
    mailbox.record_retraction(sender_hash.hex(), "m-1")

    code = cli.main(
        ["messages", "import", "--data-dir", str(target_dir), str(export_path)]
    )
    assert code == 1
    out = capsys.readouterr().out
    assert "Imported 1 messages (0 already present, 1 skipped)" in out
    assert "skipped m-1: message was retracted by its sender" in out

    code = cli.main(["messages", "--data-dir", str(target_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["id"] for item in payload["messages"]] == ["m-2"]


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


def _seed_group_reply_conversation(tmp_path):
    """One received group message in \"weekend\", plus a same-prefix
    message in a different group that must never resolve for it."""
    from tau.groups import GroupStore
    from tau.protocol import excerpt_hash

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    store = GroupStore(config.groups_path)
    group = store.create("weekend", members, created_by="ab" * 16)
    other_members = ["ab" * 16, "12" * 16, "34" * 16]
    other = store.create("other", other_members, created_by="ab" * 16)
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "cd" * 16,
            "text": "original thought",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "a" * 32,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": "cd" * 16,
            "text": "seconded",
            "sent_at": 11.0,
            "received_at": 21.0,
            "id": "b" * 32,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
            "in_reply_to": {
                "id": "a" * 32,
                "excerpt": excerpt_hash("original thought"),
            },
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": "cd" * 16,
            "text": "not this group",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "a" * 31 + "f",
            "kind": "group",
            "group": other["id"],
            "group_name": "other",
            "direction": "in",
        }
    )
    return data_dir, group, other


def test_resolve_group_reply_target_scopes_to_the_group(tmp_path):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)

    ref = cli._resolve_group_reply_target(inbox, mailbox, "aaaa", group["id"])
    assert ref["id"] == "a" * 32
    # The other group's message shares the "aaaa" prefix but must not
    # leak into this group's resolution: its full id is unknown here.
    with pytest.raises(cli.NodeError, match="no message"):
        cli._resolve_group_reply_target(
            inbox, mailbox, "a" * 31 + "f", group["id"]
        )
    with pytest.raises(cli.NodeError, match="no message"):
        cli._resolve_group_reply_target(inbox, mailbox, "zzzz", group["id"])


def test_resolve_group_reply_target_rejects_prefix_shared_across_stores(
    tmp_path,
):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    # A queued leg in the same group shares the "aaaa" prefix with the
    # received one, so the reference is ambiguous and must be refused.
    mailbox.enqueue(
        to=bytes.fromhex("ef" * 16),
        text="queued",
        message_id="aaaa" + "c" * 28,
        group={
            "id": group["id"],
            "name": "weekend",
            "creator": "ab" * 16,
            "members": ["ab" * 16, "cd" * 16, "ef" * 16],
        },
    )
    with pytest.raises(cli.NodeError, match="2 messages"):
        cli._resolve_group_reply_target(inbox, mailbox, "aaaa", group["id"])


def test_parser_group_pin_unpin_pinned_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["group", "pin", "weekend", "aaaa"])
    assert args.group == "weekend"
    assert args.message == "aaaa"
    assert args.func is cli.cmd_group_pin

    args = parser.parse_args(["group", "unpin", "weekend", "aaaa"])
    assert args.group == "weekend"
    assert args.message == "aaaa"
    assert args.func is cli.cmd_group_unpin

    args = parser.parse_args(["group", "pinned", "weekend", "--limit", "5"])
    assert args.group == "weekend"
    assert args.limit == 5
    assert args.func is cli.cmd_group_pinned

    defaults = parser.parse_args(["group", "pinned", "weekend"])
    assert defaults.limit == 30


def test_resolve_group_pin_target_scopes_to_the_group(tmp_path):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    group_ids = [group["id"]]

    entry = cli._resolve_group_pin_target(inbox, group_ids, "aaaa")
    assert entry["id"] == "a" * 32
    # The other group's message shares the "aaaa" prefix but must not
    # leak into this group's resolution: its full id is unknown here.
    with pytest.raises(cli.NodeError, match="no message"):
        cli._resolve_group_pin_target(inbox, group_ids, "a" * 31 + "f")
    with pytest.raises(cli.NodeError, match="no message"):
        cli._resolve_group_pin_target(inbox, group_ids, "zzzz")
    with pytest.raises(cli.NodeError, match="must not be empty"):
        cli._resolve_group_pin_target(inbox, group_ids, "  ")

    # Two messages in the same group sharing a prefix are refused.
    inbox.append(
        {
            "from": "alice",
            "destination": "cd" * 16,
            "text": "near miss",
            "sent_at": 13.0,
            "received_at": 23.0,
            "id": "a" * 31 + "c",
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
        }
    )
    with pytest.raises(cli.NodeError, match="2 messages"):
        cli._resolve_group_pin_target(inbox, group_ids, "aaaa")


def test_group_pin_unpin_round_trip(tmp_path, capsys):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)

    code = cli.main(
        ["group", "pin", "weekend", "aaaa", "--data-dir", str(data_dir)]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "pinned [aaaaaaaa] from alice" in out
    assert 'in group "weekend"' in out
    assert "pins are local-only" in out

    # The pin survives a fresh process and shows in the group listing.
    code = cli.main(["group", "pinned", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "original thought" in out
    assert "seconded" not in out

    # ... and in the global listing, like any pin.
    code = cli.main(["pinned", "--data-dir", str(data_dir)])
    assert code == 0
    assert "original thought" in capsys.readouterr().out

    code = cli.main(
        ["group", "unpin", "weekend", "aaaa", "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "unpinned [aaaaaaaa] from alice" in capsys.readouterr().out

    code = cli.main(["group", "pinned", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    assert 'No pinned messages in group "weekend".' in capsys.readouterr().out


def test_group_pin_json_and_errors(tmp_path, capsys):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "cd" * 16,
            "text": "near miss",
            "sent_at": 13.0,
            "received_at": 23.0,
            "id": "a" * 31 + "c",
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
        }
    )

    # Ambiguous prefix within the group.
    code = cli.main(
        ["group", "pin", "weekend", "aaaa", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "2 messages in this group match" in capsys.readouterr().err

    # A message from the other group is refused, not pinned.
    code = cli.main(
        ["group", "pin", "weekend", "a" * 31 + "f", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "no message" in capsys.readouterr().err

    # Unknown group.
    code = cli.main(
        ["group", "pin", "nope", "aaaa", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "unknown group" in capsys.readouterr().err

    # Unknown message.
    code = cli.main(
        ["group", "pin", "weekend", "zzzz", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "no message" in capsys.readouterr().err

    # Negative limit is refused.
    code = cli.main(
        ["group", "pinned", "weekend", "--limit", "-1", "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "--limit must not be negative" in capsys.readouterr().err

    code = cli.main(
        ["group", "pin", "weekend", "b" * 32, "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["pinned"]["id"] == "b" * 32
    assert payload["already_pinned"] is False
    assert payload["group"] == group["id"]

    # Pinning twice reports the pin was already there.
    code = cli.main(
        ["group", "pin", "weekend", "b" * 32, "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["already_pinned"] is True

    # The group listing reports the pin in JSON.
    code = cli.main(
        ["group", "pinned", "weekend", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["id"] == "b" * 32
    assert payload["group"] == group["id"]

    code = cli.main(
        ["group", "unpin", "weekend", "b" * 32, "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["unpinned"]["id"] == "b" * 32
    assert payload["was_pinned"] is True

    # Unpinning an unpinned message is a no-op, not an error.
    code = cli.main(
        ["group", "unpin", "weekend", "b" * 32, "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["was_pinned"] is False


def test_group_pinned_blocked_filter_applies_to_received_only(tmp_path, capsys):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "cd" * 16,
            "text": "sent pinned note",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "s-1",
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "out",
        }
    )
    cli.main(["group", "pin", "weekend", "aaaa", "--data-dir", str(data_dir)])
    cli.main(["group", "pin", "weekend", "s-1", "--data-dir", str(data_dir)])
    capsys.readouterr()

    def _pinned_text():
        code = cli.main(["group", "pinned", "weekend", "--data-dir", str(data_dir)])
        assert code == 0
        return capsys.readouterr().out

    # Blocking the sender hides the received pin, not the sent one.
    (data_dir / "blocklist.json").write_text(
        json.dumps({"blocked": ["cd" * 16]}), encoding="utf-8"
    )
    out = _pinned_text()
    assert "original thought" not in out
    assert "sent pinned note" in out
    # A group send is labelled by its group, never as an unknown peer.
    assert "you → weekend" in out
    assert "unknown" not in out


def test_group_messages_renders_quote_from_local_history(tmp_path, capsys):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--group", group["id"]]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "↩ alice: original thought" in out
    assert "seconded" in out


def test_group_messages_quote_falls_back_to_excerpt(tmp_path, capsys):
    """Group legs carry per-member ids: the quoted leg id may never
    have been held here, but the same text under another leg id still
    renders the quote via the excerpt hash."""
    from tau.protocol import excerpt_hash

    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "carol",
            "destination": "ef" * 16,
            "text": "agreed, different leg",
            "sent_at": 13.0,
            "received_at": 23.0,
            "id": "c" * 32,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
            "in_reply_to": {
                "id": "d" * 32,
                "excerpt": excerpt_hash("original thought"),
            },
        }
    )
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--group", group["id"]]
    )
    assert code == 0
    out = capsys.readouterr().out
    # Two quote lines: `seconded` via its exact-id reference and
    # `agreed, different leg` via the excerpt fallback.
    assert out.count("↩ alice: original thought") == 2
    assert "agreed, different leg" in out


class _FakeGroupMailbox:
    """Stand-in mailbox: every queued leg is still queued."""

    def get_outbox(self, message_id):
        return object()


class _FakeGroupSendNode:
    """Stub TauNode for `tau group send`: records the reply reference."""

    instances: list = []
    # Tests opt into queued legs (a `--wait` forward's offline member)
    # and decide whether the wait finds them delivered.
    queue_legs = False
    deliver_on_wait = False

    def __init__(self, config, **kwargs):
        from tau.groups import GroupStore

        self.config = config
        self.groups = GroupStore(config.groups_path)
        self.mailbox = _FakeGroupMailbox()
        self.sent: list = []
        self.send_kwargs: list = []
        self.wait_calls: list = []
        _FakeGroupSendNode.instances.append(self)

    def start(self):
        pass

    def stop(self):
        pass

    def wait_for_delivery(self, message_id, timeout=0.0):
        self.wait_calls.append((message_id, timeout))
        return _FakeGroupSendNode.deliver_on_wait

    def send_group_message(self, group, text, **kwargs):
        self.sent.append((group, text, kwargs.get("in_reply_to")))
        self.send_kwargs.append(kwargs)
        if kwargs.get("send_after") is not None or _FakeGroupSendNode.queue_legs:
            return {
                "ok": True,
                "group": group["id"],
                "name": group["name"],
                "results": [
                    {
                        "to": "cd" * 16,
                        "to_name": "alice",
                        "queued": True,
                        "id": "f" * 32,
                        **(
                            {"send_after": kwargs["send_after"]}
                            if kwargs.get("send_after") is not None
                            else {}
                        ),
                    },
                    {
                        "to": "ab" * 16,
                        "to_name": "bob",
                        "queued": True,
                        "id": "e" * 32,
                        **(
                            {"send_after": kwargs["send_after"]}
                            if kwargs.get("send_after") is not None
                            else {}
                        ),
                    },
                ],
            }
        return {
            "ok": True,
            "group": group["id"],
            "name": group["name"],
            "results": [
                {
                    "to": "cd" * 16,
                    "to_name": "alice",
                    "delivered": True,
                    "id": "f" * 32,
                }
            ],
        }

    def flush_outbox(self, **kwargs):
        pass


def test_group_send_reply_to_resolves_prefix_and_sends_reference(
    tmp_path, capsys, monkeypatch
):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "send",
            "--data-dir",
            str(data_dir),
            "--reply-to",
            "aaaa",
            "weekend",
            "seconded, again",
        ]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert len(node.sent) == 1
    sent_group, _, in_reply_to = node.sent[0]
    assert sent_group["id"] == group["id"]
    assert in_reply_to["id"] == "a" * 32
    assert in_reply_to["excerpt"] == cli.excerpt_hash("original thought")
    assert "delivered to alice" in capsys.readouterr().out


def test_group_send_reply_to_unknown_reference_is_an_error(
    tmp_path, capsys, monkeypatch
):
    data_dir, _, _ = _seed_group_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "send",
            "--data-dir",
            str(data_dir),
            "--reply-to",
            "zzzz",
            "weekend",
            "dangling",
        ]
    )
    assert code == 1
    assert "no message" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def test_group_send_reply_to_empty_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir, _, _ = _seed_group_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "send",
            "--data-dir",
            str(data_dir),
            "--reply-to",
            "",
            "weekend",
            "dangling",
        ]
    )
    assert code == 1
    assert "must not be empty" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def _run_group_send(tmp_path, monkeypatch, argv):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    return (
        cli.main(["group", "send", "--data-dir", str(data_dir), *argv]),
        group,
    )


def test_parser_group_send_delay():
    parser = cli.build_parser()
    args = parser.parse_args(["group", "send", "--delay", "5m", "weekend", "hello"])
    assert args.delay == "5m"


def test_group_send_delay_schedules_without_delivery(
    tmp_path, capsys, monkeypatch
):
    before = time.time()
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--delay", "2h", "weekend", "later"]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert len(node.send_kwargs) == 1
    send_after = node.send_kwargs[0]["send_after"]
    assert before + 7200 <= send_after <= time.time() + 7200
    out = capsys.readouterr().out
    assert "scheduled for alice" in out
    assert "scheduled for bob" in out
    assert "sends in" in out
    assert "it will be delivered at its scheduled time" in out


def test_group_send_delay_json_reports_send_after(tmp_path, capsys, monkeypatch):
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--json", "--delay", "5m", "weekend", "later"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["send_after"] is not None
    assert payload["send_after"] > time.time()
    assert all(result.get("send_after") for result in payload["results"])


def test_group_send_delay_rejects_bad_combinations(tmp_path, capsys, monkeypatch):
    code, _ = _run_group_send(
        tmp_path,
        monkeypatch,
        ["--delay", "5m", "--no-queue", "weekend", "hi"],
    )
    assert code == 2
    assert "--delay cannot be combined with --no-queue" in capsys.readouterr().err
    code, _ = _run_group_send(
        tmp_path,
        monkeypatch,
        ["--delay", "2h", "--expire", "1h", "weekend", "hi"],
    )
    assert code == 2
    assert "--delay must be shorter than --expire" in capsys.readouterr().err
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--delay", "soon", "weekend", "hi"]
    )
    assert code == 1
    assert "invalid --delay duration" in capsys.readouterr().err
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--delay", "30d", "weekend", "hi"]
    )
    assert code == 2
    assert "--delay must be at most 29d23h" in capsys.readouterr().err
    # 29d fits and queues.
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--delay", "29d", "weekend", "hi"]
    )
    assert code == 0


def test_group_send_delay_no_queue_rejects_before_reading_input(
    tmp_path, capsys, monkeypatch
):
    # No message text: reading stdin would block, so the conflict must
    # be rejected before _read_message runs.
    class _BlockingStdin:
        def read(self, *args, **kwargs):
            raise AssertionError("stdin was read")

        def isatty(self):
            return False

    monkeypatch.setattr("sys.stdin", _BlockingStdin())
    data_dir, _, _ = _seed_group_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "send",
            "--data-dir",
            str(data_dir),
            "--delay",
            "5m",
            "--no-queue",
            "weekend",
        ]
    )
    assert code == 2
    assert "--delay cannot be combined with --no-queue" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def _seed_group_poll_conversation(tmp_path, *, close=False):
    """A weekend group with one poll from alice, one vote, maybe a close."""
    from tau.groups import GroupStore

    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "ab" * 16,
            "text": "which day?",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "p" * 32,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
            "poll_options": ["Saturday", "Sunday"],
        }
    )
    inbox.append(
        {
            "from": "bob",
            "destination": "cd" * 16,
            "text": "voted: Saturday",
            "sent_at": 1002.0,
            "received_at": 1003.0,
            "id": "v" * 32,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
            "poll_vote": {
                "target": {"author": "ab" * 16, "sent_at": 1000.0},
                "option": 0,
            },
        }
    )
    if close:
        inbox.append(
            {
                "from": "alice",
                "destination": "ab" * 16,
                "text": "closed the poll",
                "sent_at": 1004.0,
                "received_at": 1005.0,
                "id": "c" * 31 + "1",
                "kind": "group",
                "group": group["id"],
                "group_name": "weekend",
                "direction": "in",
                "poll_close": {"author": "ab" * 16, "sent_at": 1000.0},
            }
        )
    return data_dir, group


def _seed_dm_poll_conversation(tmp_path, *, close=False):
    """A DM with alice: one poll from her, one vote, maybe a close."""
    from tau.peers import PeerRegistry

    data_dir = _init_node(tmp_path, "node", "bob")
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    peer_identity = RNS.Identity()
    peer_hash = destination_hash(peer_identity).hex()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=100.0,
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": peer_hash,
            "text": "which day?",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "p" * 32,
            "kind": "chat",
            "direction": "in",
            "poll_options": ["Saturday", "Sunday"],
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": peer_hash,
            "text": "voted: Saturday",
            "sent_at": 1002.0,
            "received_at": 1003.0,
            "id": "v" * 32,
            "kind": "chat",
            "direction": "in",
            "poll_vote": {
                "target": {"author": peer_hash, "sent_at": 1000.0},
                "option": 0,
            },
        }
    )
    if close:
        inbox.append(
            {
                "from": "alice",
                "destination": peer_hash,
                "text": "closed the poll",
                "sent_at": 1004.0,
                "received_at": 1005.0,
                "id": "c" * 31 + "1",
                "kind": "chat",
                "direction": "in",
                "poll_close": {"author": peer_hash, "sent_at": 1000.0},
            }
        )
    return data_dir, peer_hash


def test_parser_dm_poll_vote_close_poll():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["send", "--to", "alice", "which day?", "--poll-options", "Saturday", "Sunday"]
    )
    assert args.func is cli.cmd_send
    assert args.poll_options == ["Saturday", "Sunday"]
    args = parser.parse_args(["vote", "--to", "alice", "pppp", "2"])
    assert args.func is cli.cmd_vote
    assert args.poll == "pppp"
    assert args.option == "2"
    args = parser.parse_args(["close-poll", "--to", "alice", "pppp"])
    assert args.func is cli.cmd_close_poll


def test_resolve_dm_poll(tmp_path):
    data_dir, peer_hash = _seed_dm_poll_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    poll = cli._resolve_dm_poll(inbox, "pppp", peer_hash)
    assert poll["text"] == "which day?"
    # A vote record never resolves as the poll itself.
    with pytest.raises(cli.NodeError, match="no poll"):
        cli._resolve_dm_poll(inbox, "vvvv", peer_hash)
    # A plain message in the DM is not a poll either.
    with pytest.raises(cli.NodeError, match="no poll"):
        cli._resolve_dm_poll(inbox, "zzzz", peer_hash)


def test_dm_send_posts_poll_options(tmp_path, capsys, monkeypatch):
    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    code = cli.main(
        [
            "send",
            "--data-dir",
            str(data_dir),
            "--to",
            "alice",
            "which day?",
            "--poll-options",
            "Saturday",
            "Sunday",
        ]
    )
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert len(node.send_kwargs) == 1
    assert node.send_kwargs[0]["poll_options"] == ["Saturday", "Sunday"]
    assert node.send_kwargs[0]["poll_vote"] is None
    assert "which day?" in capsys.readouterr().out


def test_dm_send_rejects_one_option(tmp_path, capsys, monkeypatch):
    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    code = cli.main(
        [
            "send",
            "--data-dir",
            str(data_dir),
            "--to",
            "alice",
            "which day?",
            "--poll-options",
            "Saturday",
        ]
    )
    assert code == 2
    assert "a poll needs 2-8 options" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_dm_vote_sends_target_and_option(tmp_path, capsys, monkeypatch):
    data_dir, peer_hash = _seed_dm_poll_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    code = cli.main(
        ["vote", "--data-dir", str(data_dir), "--to", "alice", "pppp", "2"]
    )
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert len(node.send_kwargs) == 1
    assert node.send_kwargs[0]["poll_vote"] == {
        "target": {"author": peer_hash, "sent_at": 1000.0},
        "option": 1,
    }
    assert "voted for 'Sunday'" in capsys.readouterr().out


def test_dm_vote_accepts_option_text(tmp_path, capsys, monkeypatch):
    data_dir, peer_hash = _seed_dm_poll_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    code = cli.main(
        ["vote", "--data-dir", str(data_dir), "--to", "alice", "pppp", "Saturday"]
    )
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert node.send_kwargs[0]["poll_vote"]["option"] == 0


def test_dm_vote_rejects_closed_poll_and_bad_option(
    tmp_path, capsys, monkeypatch
):
    data_dir, _ = _seed_dm_poll_conversation(tmp_path / "closed", close=True)
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    code = cli.main(
        ["vote", "--data-dir", str(data_dir), "--to", "alice", "pppp", "1"]
    )
    assert code == 2
    assert "this poll is closed" in capsys.readouterr().err
    assert _FakeSendNode.instances == []

    data_dir, _ = _seed_dm_poll_conversation(tmp_path / "open")
    code = cli.main(
        ["vote", "--data-dir", str(data_dir), "--to", "alice", "pppp", "3"]
    )
    assert code == 2
    assert "option must be a number 1-2" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_dm_close_poll_requires_creator(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_dm_poll_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    code = cli.main(
        ["close-poll", "--data-dir", str(data_dir), "--to", "alice", "pppp"]
    )
    assert code == 2
    assert "only the poll's creator can close it" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_dm_close_poll_happy_path(tmp_path, capsys, monkeypatch):
    data_dir = _init_node(tmp_path, "node", "bob")
    capsys.readouterr()
    own_hash = _id_destination(data_dir, capsys)
    from tau.peers import PeerRegistry

    peer_identity = RNS.Identity()
    peer_hash = destination_hash(peer_identity).hex()
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=100.0,
    )
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": peer_hash,
            "text": "which day?",
            "sent_at": 1001.0,
            "received_at": 1001.0,
            "id": "p" * 32,
            "kind": "chat",
            "direction": "out",
            "leg_sent_at": 1000.0,
            "poll_options": ["Saturday", "Sunday"],
        }
    )
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    code = cli.main(
        ["close-poll", "--data-dir", str(data_dir), "--to", "alice", "pppp"]
    )
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert node.send_kwargs[0]["poll_close"] == {
        "author": own_hash,
        "sent_at": 1000.0,
    }
    assert "closed the poll" in capsys.readouterr().out


def test_dm_messages_renders_poll_tally_and_hides_votes(tmp_path, capsys):
    data_dir, _ = _seed_dm_poll_conversation(tmp_path)
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "which day?" in out
    assert "1. Saturday — 1 vote(s)" in out
    assert "2. Sunday — 0 vote(s)" in out
    assert "1 voter(s) total" in out
    # The vote itself is not a chat message.
    assert "voted: Saturday" not in out


def test_parser_group_poll_vote_close_poll():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["group", "poll", "weekend", "which day?", "Saturday", "Sunday"]
    )
    assert args.func is cli.cmd_group_poll
    assert args.options == ["Saturday", "Sunday"]
    args = parser.parse_args(["group", "vote", "weekend", "pppp", "2"])
    assert args.func is cli.cmd_group_vote
    assert args.poll == "pppp"
    assert args.option == "2"
    args = parser.parse_args(["group", "close-poll", "weekend", "pppp"])
    assert args.func is cli.cmd_group_close_poll


def test_resolve_group_poll(tmp_path):
    data_dir, group = _seed_group_poll_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    poll = cli._resolve_group_poll(inbox, "pppp", group["id"])
    assert poll["text"] == "which day?"
    # A vote record never resolves as the poll itself.
    with pytest.raises(cli.NodeError, match="no poll"):
        cli._resolve_group_poll(inbox, "vvvv", group["id"])
    # A plain message in the group is not a poll either.
    with pytest.raises(cli.NodeError, match="no poll"):
        cli._resolve_group_poll(inbox, "aaaa", group["id"])
    with pytest.raises(cli.NodeError, match="no poll"):
        cli._resolve_group_poll(inbox, "zzzz", group["id"])


def test_group_poll_posts_options(tmp_path, capsys, monkeypatch):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "poll",
            "--data-dir",
            str(data_dir),
            "weekend",
            "which day?",
            "Saturday",
            "Sunday",
        ]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert len(node.send_kwargs) == 1
    assert node.send_kwargs[0]["poll_options"] == ["Saturday", "Sunday"]
    assert node.send_kwargs[0]["poll_vote"] is None
    assert "poll posted to weekend" in capsys.readouterr().out


def test_group_poll_rejects_one_option(tmp_path, capsys, monkeypatch):
    data_dir, _, _ = _seed_group_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "poll",
            "--data-dir",
            str(data_dir),
            "weekend",
            "which day?",
            "Saturday",
        ]
    )
    assert code == 2
    assert "a poll needs 2-8 options" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def test_group_vote_sends_target_and_option(tmp_path, capsys, monkeypatch):
    data_dir, group = _seed_group_poll_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "vote",
            "--data-dir",
            str(data_dir),
            "weekend",
            "pppp",
            "2",
        ]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert len(node.send_kwargs) == 1
    assert node.send_kwargs[0]["poll_vote"] == {
        "target": {"author": "ab" * 16, "sent_at": 1000.0},
        "option": 1,
    }
    assert "voted for 'Sunday'" in capsys.readouterr().out


def test_group_vote_accepts_option_text(tmp_path, capsys, monkeypatch):
    data_dir, group = _seed_group_poll_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "vote",
            "--data-dir",
            str(data_dir),
            "weekend",
            "pppp",
            "Saturday",
        ]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert node.send_kwargs[0]["poll_vote"]["option"] == 0


def test_group_vote_rejects_closed_poll_and_bad_option(
    tmp_path, capsys, monkeypatch
):
    data_dir, group = _seed_group_poll_conversation(
        tmp_path / "closed", close=True
    )
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "vote",
            "--data-dir",
            str(data_dir),
            "weekend",
            "pppp",
            "1",
        ]
    )
    assert code == 2
    assert "this poll is closed" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []

    data_dir, group = _seed_group_poll_conversation(tmp_path / "open")
    code = cli.main(
        [
            "group",
            "vote",
            "--data-dir",
            str(data_dir),
            "weekend",
            "pppp",
            "3",
        ]
    )
    assert code == 2
    assert "option must be a number 1-2" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def test_group_close_poll_requires_creator(tmp_path, capsys, monkeypatch):
    data_dir, group = _seed_group_poll_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "close-poll",
            "--data-dir",
            str(data_dir),
            "weekend",
            "pppp",
        ]
    )
    assert code == 2
    assert "only the poll's creator can close it" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def test_group_close_poll_happy_path(tmp_path, capsys, monkeypatch):
    data_dir = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    own_hash = _id_destination(data_dir, capsys)
    from tau.groups import GroupStore

    store = GroupStore(data_dir / "groups.json")
    group = store.create(
        "weekend", [own_hash, "cd" * 16], created_by=own_hash
    )
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "",
            "text": "which day?",
            "sent_at": 1001.0,
            "received_at": 1001.0,
            "id": "p" * 32,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "out",
            "leg_sent_at": 1000.0,
            "poll_options": ["Saturday", "Sunday"],
        }
    )
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "group",
            "close-poll",
            "--data-dir",
            str(data_dir),
            "weekend",
            "pppp",
        ]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert node.send_kwargs[0]["poll_close"] == {
        "author": own_hash,
        "sent_at": 1000.0,
    }
    assert "closed the poll" in capsys.readouterr().out


def test_group_messages_renders_poll_tally_and_hides_votes(
    tmp_path, capsys
):
    data_dir, group = _seed_group_poll_conversation(tmp_path)
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--group", group["id"]]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "which day?" in out
    assert "1. Saturday — 1 vote(s)" in out
    assert "2. Sunday — 0 vote(s)" in out
    assert "1 voter(s) total" in out
    # The vote itself is not a chat message.
    assert "voted: Saturday" not in out


def test_parser_channel_poll_vote_close_poll():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["channel", "poll", "general", "which day?", "Saturday", "Sunday"]
    )
    assert args.func is cli.cmd_channel_poll
    assert args.options == ["Saturday", "Sunday"]
    args = parser.parse_args(["channel", "vote", "general", "pppp", "2"])
    assert args.func is cli.cmd_channel_vote
    assert args.poll == "pppp"
    assert args.option == "2"
    args = parser.parse_args(["channel", "close-poll", "general", "pppp"])
    assert args.func is cli.cmd_channel_close_poll


def _seed_channel_poll_conversation(tmp_path):
    """One node joined to #general with a posted channel poll."""
    from tau.channels import channel_history, init_db as init_channel_db

    data_dir = _init_node(tmp_path, "node", "alice")
    assert (
        cli.main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    code = cli.main(
        [
            "channel",
            "poll",
            "--data-dir",
            str(data_dir),
            "--no-publish",
            "general",
            "which day?",
            "Saturday",
            "Sunday",
        ]
    )
    assert code == 0
    conn = init_channel_db(data_dir / "channels.sqlite3")
    try:
        poll = channel_history(conn, "general")[0]
    finally:
        conn.close()
    return data_dir, poll


def test_channel_poll_vote_close_round_trip(tmp_path, capsys):
    data_dir, poll = _seed_channel_poll_conversation(tmp_path)
    prefix = poll["hash"][:8]
    capsys.readouterr()

    code = cli.main(
        [
            "channel",
            "vote",
            "--data-dir",
            str(data_dir),
            "--no-publish",
            "general",
            prefix,
            "Sunday",
        ]
    )
    assert code == 0
    assert "voted for 'Sunday'" in capsys.readouterr().out

    code = cli.main(["channel", "read", "--data-dir", str(data_dir), "general"])
    assert code == 0
    out = capsys.readouterr().out
    assert "which day?" in out
    assert "1. Saturday — 0 votes" in out
    assert "2. Sunday — 1 vote" in out
    assert "your vote" in out
    # The vote itself is not a chat message.
    assert "voted: Sunday" not in out

    code = cli.main(
        [
            "channel",
            "close-poll",
            "--data-dir",
            str(data_dir),
            "--no-publish",
            "general",
            prefix,
        ]
    )
    assert code == 0
    assert "closed the poll" in capsys.readouterr().out

    code = cli.main(["channel", "read", "--data-dir", str(data_dir), "general"])
    assert code == 0
    assert "closed" in capsys.readouterr().out

    code = cli.main(
        [
            "channel",
            "vote",
            "--data-dir",
            str(data_dir),
            "--no-publish",
            "general",
            prefix,
            "1",
        ]
    )
    assert code == 2
    assert "this poll is closed" in capsys.readouterr().err


def test_channel_poll_rejects_one_option(tmp_path, capsys):
    data_dir = _init_node(tmp_path, "node", "alice")
    assert (
        cli.main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    code = cli.main(
        [
            "channel",
            "poll",
            "--data-dir",
            str(data_dir),
            "--no-publish",
            "general",
            "which day?",
            "Saturday",
        ]
    )
    assert code == 2
    assert "a poll needs 2-8 options" in capsys.readouterr().err


def test_parser_group_send_wait():
    parser = cli.build_parser()
    default = parser.parse_args(["group", "send", "weekend", "hello"])
    assert default.wait is None
    waited = parser.parse_args(
        ["group", "send", "--wait", "30", "weekend", "hello"]
    )
    assert waited.wait == 30.0


def test_group_send_wait_delivers_while_waiting(
    tmp_path, capsys, monkeypatch
):
    # An offline member's queued leg is waited on instead of the
    # one-shot flush, and a delivery inside the budget reports
    # delivered with its waited time.
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    monkeypatch.setattr(_FakeGroupSendNode, "queue_legs", True)
    monkeypatch.setattr(_FakeGroupSendNode, "deliver_on_wait", True)
    _FakeGroupSendNode.instances.clear()
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--wait", "5", "weekend", "hello"]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    # The legs share one 5s deadline: the second leg gets whatever
    # budget remains, never a fresh 5s.
    assert [call[0] for call in node.wait_calls] == ["f" * 32, "e" * 32]
    assert node.wait_calls[0][1] == pytest.approx(5.0, abs=0.05)
    assert 0 < node.wait_calls[1][1] <= node.wait_calls[0][1]
    out = capsys.readouterr().out
    assert "delivered to alice after" in out
    assert "delivered to bob after" in out


def test_group_send_wait_json_reports_waited_seconds(
    tmp_path, capsys, monkeypatch
):
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    monkeypatch.setattr(_FakeGroupSendNode, "queue_legs", True)
    monkeypatch.setattr(_FakeGroupSendNode, "deliver_on_wait", True)
    _FakeGroupSendNode.instances.clear()
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--json", "--wait", "5", "weekend", "hello"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert all(result.get("delivered") for result in payload["results"])
    assert all(result.get("waited") is not None for result in payload["results"])


def test_group_send_wait_times_out_with_nonzero_exit(
    tmp_path, capsys, monkeypatch
):
    # A leg still queued when the wait expires is reported as still
    # queued, and the exit code is non-zero like `tau send --wait`.
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    monkeypatch.setattr(_FakeGroupSendNode, "queue_legs", True)
    monkeypatch.setattr(_FakeGroupSendNode, "deliver_on_wait", False)
    _FakeGroupSendNode.instances.clear()
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--wait", "5", "weekend", "hello"]
    )
    assert code == 1
    node = _FakeGroupSendNode.instances[0]
    # The legs share one 5s deadline: the second leg gets whatever
    # budget remains, never a fresh 5s.
    assert [call[0] for call in node.wait_calls] == ["f" * 32, "e" * 32]
    assert node.wait_calls[0][1] == pytest.approx(5.0, abs=0.05)
    assert 0 < node.wait_calls[1][1] <= node.wait_calls[0][1]
    out = capsys.readouterr().out
    assert "still queued for alice after" in out
    assert "still queued for bob after" in out


def test_group_send_wait_rejects_no_queue(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code, _ = _run_group_send(
        tmp_path, monkeypatch, ["--wait", "5", "--no-queue", "weekend", "hi"]
    )
    assert code == 2
    assert "--wait cannot be combined with --no-queue" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def test_direct_reply_target_still_quotes_received_group_message(tmp_path):
    """Narrowing the DM/group boundary: a *received* group message
    from a peer keeps quoting in a DM to that peer, exactly as before
    this PR — only the new group-send records stay out of DMs."""
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    ref = cli._resolve_reply_target(inbox, mailbox, "a" * 32, "cd" * 16)
    assert ref["id"] == "a" * 32


def test_direct_reply_target_excludes_queued_group_leg(tmp_path):
    """A queued group leg to the peer is not a DM reply target: its
    id belongs to the group send."""
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    mailbox.enqueue(
        to=bytes.fromhex("cd" * 16),
        text="queued leg",
        message_id="d" * 32,
        group={
            "id": group["id"],
            "name": "weekend",
            "creator": "ab" * 16,
            "members": ["ab" * 16, "cd" * 16, "ef" * 16],
        },
    )
    with pytest.raises(cli.NodeError, match="no message"):
        cli._resolve_reply_target(inbox, mailbox, "dddd", "cd" * 16)


def test_direct_quote_of_received_group_message_renders(tmp_path, capsys):
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "cd" * 16,
            "text": "quoting the group in a DM",
            "sent_at": 13.0,
            "received_at": 23.0,
            "id": "e" * 32,
            "direction": "in",
            "in_reply_to": {
                "id": "a" * 32,
                "excerpt": cli.excerpt_hash("original thought"),
            },
        }
    )
    code = cli.main(
        [
            "messages",
            "--data-dir",
            str(data_dir),
            "--search",
            "quoting the group",
        ]
    )
    assert code == 0
    assert "↩ alice: original thought" in capsys.readouterr().out


def test_group_reply_target_dedupes_queued_leg_with_sent_record(tmp_path):
    """A queued leg whose id is already covered by the group send's
    inbox record (in leg_ids) is one candidate, not an ambiguity."""
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    mailbox = Mailbox(config.mailbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "hello group",
            "sent_at": 13.0,
            "received_at": 23.0,
            "id": "c" * 31 + "1",
            "leg_ids": ["c" * 31 + "1", "d" * 32],
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "out",
        }
    )
    mailbox.enqueue(
        to=bytes.fromhex("ef" * 16),
        text="hello group",
        message_id="d" * 32,
        group={
            "id": group["id"],
            "name": "weekend",
            "creator": "ab" * 16,
            "members": ["ab" * 16, "cd" * 16, "ef" * 16],
        },
    )
    ref = cli._resolve_group_reply_target(inbox, mailbox, "d" * 32, group["id"])
    assert ref["id"] == "d" * 32
    assert ref["excerpt"] == cli.excerpt_hash("hello group")


def test_group_quote_with_duplicate_text_is_unavailable(tmp_path, capsys):
    """Two members posting identical text leaves the excerpt fallback
    with no author to attribute the quote to."""
    data_dir, group, _ = _seed_group_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    for sender, destination in (("alice", "cd" * 16), ("carol", "ef" * 16)):
        inbox.append(
            {
                "from": sender,
                "destination": destination,
                "text": "same words",
                "sent_at": 13.0,
                "received_at": 23.0,
                "id": "f" * 31 + ("1" if sender == "alice" else "2"),
                "kind": "group",
                "group": group["id"],
                "group_name": "weekend",
                "direction": "in",
            }
        )
    inbox.append(
        {
            "from": "bob",
            "destination": "ab" * 16,
            "text": "which one?",
            "sent_at": 14.0,
            "received_at": 24.0,
            "id": "c" * 32,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "direction": "in",
            "in_reply_to": {
                "id": "d" * 32,
                "excerpt": cli.excerpt_hash("same words"),
            },
        }
    )
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--group", group["id"]]
    )
    assert code == 0
    assert "↩ original not available" in capsys.readouterr().out


def _run_forward(tmp_path, monkeypatch, argv):
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    _FakeSendNode.deliver_on_flush = False
    return cli.main(["forward", "--data-dir", str(tmp_path / "node"), *argv])


def test_parser_forward_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["forward", "--to", "bob", "abcdef12"])
    assert args.command == "forward"
    assert args.to == ["bob"]
    assert args.message == "abcdef12"
    assert args.timeout == 20.0
    assert args.wait is None
    assert args.no_queue is False

    multi = parser.parse_args(
        ["forward", "--to", "alice", "--to", "bob", "--wait", "5", "abcdef12"]
    )
    assert multi.to == ["alice", "bob"]
    assert multi.wait == 5.0

    group = parser.parse_args(["forward", "--group", "weekend", "abcdef12"])
    assert group.to is None
    assert group.group == "weekend"
    assert group.message == "abcdef12"

    with pytest.raises(SystemExit):
        parser.parse_args(["forward", "abcdef12"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["forward", "--to", "bob", "--group", "weekend", "abcdef12"]
        )


def test_forward_sends_attributed_text(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_reply_conversation(tmp_path)
    code = _run_forward(tmp_path, monkeypatch, ["--to", "bob", "aaaa"])
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert len(node.sent) == 1
    target, text, _ = node.sent[0]
    assert target == "bob"
    assert text == "Forwarded from alice:\noriginal thought"
    assert "delivered to bob" in capsys.readouterr().out


def test_forward_to_group_fans_out_attributed_text(
    tmp_path, capsys, monkeypatch
):
    from tau.groups import GroupStore

    data_dir, _ = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    store = GroupStore(config.groups_path)
    store.create("weekend", ["ab" * 16, "cd" * 16], created_by="ab" * 16)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        ["forward", "--data-dir", str(tmp_path / "node"), "--group", "weekend", "aaaa"]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert len(node.sent) == 1
    group, text, _ = node.sent[0]
    assert group["name"] == "weekend"
    assert text == "Forwarded from alice:\noriginal thought"
    assert "delivered to alice" in capsys.readouterr().out


def test_forward_to_group_unknown_group_is_an_error(
    tmp_path, capsys, monkeypatch
):
    data_dir, _ = _seed_reply_conversation(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        ["forward", "--data-dir", str(tmp_path / "node"), "--group", "nope", "aaaa"]
    )
    assert code == 1
    assert "unknown group" in capsys.readouterr().err


def test_forward_to_group_waits_for_queued_legs(
    tmp_path, capsys, monkeypatch
):
    # A group forward honours --wait: the queued legs are waited on
    # instead of the one-shot flush, and a delivery inside the budget
    # reports delivered.
    from tau.groups import GroupStore

    data_dir, _ = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    store = GroupStore(config.groups_path)
    store.create("weekend", ["ab" * 16, "cd" * 16], created_by="ab" * 16)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    monkeypatch.setattr(_FakeGroupSendNode, "queue_legs", True)
    monkeypatch.setattr(_FakeGroupSendNode, "deliver_on_wait", True)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "forward",
            "--data-dir",
            str(tmp_path / "node"),
            "--group",
            "weekend",
            "--wait",
            "5",
            "aaaa",
        ]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    # The legs share one 5s deadline: the second leg gets whatever
    # budget remains, never a fresh 5s.
    assert [call[0] for call in node.wait_calls] == ["f" * 32, "e" * 32]
    assert node.wait_calls[0][1] == pytest.approx(5.0, abs=0.05)
    assert 0 < node.wait_calls[1][1] <= node.wait_calls[0][1]
    assert "delivered to alice" in capsys.readouterr().out


def test_forward_to_group_wait_conflicts_with_no_queue(
    tmp_path, capsys, monkeypatch
):
    from tau.groups import GroupStore

    data_dir, _ = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    store = GroupStore(config.groups_path)
    store.create("weekend", ["ab" * 16, "cd" * 16], created_by="ab" * 16)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "forward",
            "--data-dir",
            str(tmp_path / "node"),
            "--group",
            "weekend",
            "--wait",
            "5",
            "--no-queue",
            "aaaa",
        ]
    )
    assert code == 2
    assert "--wait cannot be combined with --no-queue" in capsys.readouterr().err
    assert _FakeGroupSendNode.instances == []


def test_forward_prefers_registry_alias_for_sender_label(
    tmp_path, monkeypatch
):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(sender_hash, sender.get_public_key(), name="alice", now=100.0)
    registry.set_alias(sender_hash, "ally")
    registry.save()
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "hello there",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "c" * 32,
            "direction": "in",
        }
    )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "carol", "cccc"])
    assert code == 0
    assert (
        _FakeSendNode.instances[0].sent[0][1]
        == "Forwarded from ally:\nhello there"
    )


def test_forward_own_sent_message_labels_you(tmp_path, monkeypatch):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "ab" * 16,
            "text": "my earlier note",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "d" * 32,
            "direction": "out",
        }
    )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "carol", "dddd"])
    assert code == 0
    assert (
        _FakeSendNode.instances[0].sent[0][1]
        == "Forwarded from you:\nmy earlier note"
    )


def test_forward_blocked_sender_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_reply_conversation(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({"ab" * 16})))
    code = _run_forward(tmp_path, monkeypatch, ["--to", "carol", "aaaa"])
    assert code == 1
    assert "blocked peer" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_forward_unknown_sender_uses_short_hash(tmp_path, monkeypatch):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "unknown",
            "destination": "ab" * 16,
            "text": "nameless hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "2" * 32,
            "direction": "in",
        }
    )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "carol", "2222"])
    assert code == 0
    assert (
        _FakeSendNode.instances[0].sent[0][1]
        == "Forwarded from abababab:\nnameless hello"
    )


def test_forward_own_sent_message_ignores_blocked_recipient(
    tmp_path, monkeypatch
):
    # A sent message carries its *recipient* in `destination`: blocking
    # that recipient must not stop the owner forwarding their own words.
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({"ab" * 16})))
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "ab" * 16,
            "text": "my earlier note",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "d" * 32,
            "direction": "out",
        }
    )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "carol", "dddd"])
    assert code == 0
    assert (
        _FakeSendNode.instances[0].sent[0][1]
        == "Forwarded from you:\nmy earlier note"
    )


def test_forward_unknown_reference_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_reply_conversation(tmp_path)
    code = _run_forward(tmp_path, monkeypatch, ["--to", "bob", "zzzz"])
    assert code == 1
    assert "unknown message: 'zzzz'" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_forward_ambiguous_prefix_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    for message_id in ("e" * 31 + "1", "e" * 31 + "2"):
        inbox.append(
            {
                "from": "alice",
                "destination": "ab" * 16,
                "text": "shared prefix",
                "sent_at": 10.0,
                "received_at": 20.0,
                "id": message_id,
                "direction": "in",
            }
        )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "bob", "eeee"])
    assert code == 1
    assert "messages match prefix" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def _seed_tan_post(tmp_path, *, text="worth sharing", poll=False):
    """A node with one foreign Tan post by a registered peer named alice."""
    from tau.tangents.tan import create_poll, create_post, init_db

    data_dir = _init_node(tmp_path, "node", "bob")
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    peer_identity = RNS.Identity()
    peer_hash = destination_hash(peer_identity)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(peer_hash, peer_identity.get_public_key(), name="alice", now=100.0)
    registry.save()
    conn = init_db(config.social_db_path)
    try:
        if poll:
            post = create_poll(
                conn,
                peer_identity,
                text,
                ["red", "blue"],
                name="alice",
                now=1000,
            )
        else:
            post = create_post(conn, peer_identity, text, name="alice", now=1000)
    finally:
        conn.close()
    return data_dir, post, peer_hash.hex()


def _run_share(tmp_path, monkeypatch, argv):
    monkeypatch.setattr(cli, "TauNode", _FakeSendNode)
    _FakeSendNode.instances.clear()
    _FakeSendNode.behaviour = {}
    _FakeSendNode.deliver_on_flush = False
    return cli.main(
        ["tan", "share", "--data-dir", str(tmp_path / "node"), *argv]
    )


def test_parser_tan_share_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["tan", "share", "--to", "bob", "abcdef12"]
    )
    assert args.command == "tan"
    assert args.to == ["bob"]
    assert args.target == "abcdef12"
    assert args.timeout == 20.0
    assert args.wait is None
    assert args.no_queue is False

    multi = parser.parse_args(
        ["tan", "share", "--to", "alice", "--to", "bob", "--wait", "5", "abcdef12"]
    )
    assert multi.to == ["alice", "bob"]
    assert multi.wait == 5.0

    group = parser.parse_args(
        ["tan", "share", "--group", "weekend", "abcdef12"]
    )
    assert group.to is None
    assert group.group == "weekend"
    assert group.target == "abcdef12"

    with pytest.raises(SystemExit):
        parser.parse_args(["tan", "share", "abcdef12"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["tan", "share", "--to", "bob", "--group", "weekend", "abcdef12"]
        )


def test_tan_share_sends_attributed_text(tmp_path, capsys, monkeypatch):
    _, post, _ = _seed_tan_post(tmp_path)
    code = _run_share(tmp_path, monkeypatch, ["--to", "bob", post["hash"][:12]])
    assert code == 0
    node = _FakeSendNode.instances[0]
    assert len(node.sent) == 1
    target, text, _ = node.sent[0]
    assert target == "bob"
    assert text == (
        f"Shared Tan post by alice:\nworth sharing\n(tan {post['hash']})"
    )
    assert "delivered to bob" in capsys.readouterr().out


def test_tan_share_to_group_fans_out_attributed_text(
    tmp_path, capsys, monkeypatch
):
    from tau.groups import GroupStore

    _, post, _ = _seed_tan_post(tmp_path)
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="bob")
    store = GroupStore(config.groups_path)
    store.create("weekend", ["ab" * 16, "cd" * 16], created_by="ab" * 16)
    monkeypatch.setattr(cli, "TauNode", _FakeGroupSendNode)
    _FakeGroupSendNode.instances.clear()
    code = cli.main(
        [
            "tan",
            "share",
            "--data-dir",
            str(tmp_path / "node"),
            "--group",
            "weekend",
            post["hash"][:12],
        ]
    )
    assert code == 0
    node = _FakeGroupSendNode.instances[0]
    assert len(node.sent) == 1
    group, text, _ = node.sent[0]
    assert group["name"] == "weekend"
    assert text == (
        f"Shared Tan post by alice:\nworth sharing\n(tan {post['hash']})"
    )
    assert "delivered to alice" in capsys.readouterr().out


def test_tan_share_own_post_labels_you(tmp_path, monkeypatch):
    from tau.tangents.tan import create_post, init_db

    data_dir = _init_node(tmp_path, "node", "bob")
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    identity = load_identity(config.identity_path)
    conn = init_db(config.social_db_path)
    try:
        post = create_post(conn, identity, "my own thought", name="bob", now=1000)
    finally:
        conn.close()
    code = _run_share(tmp_path, monkeypatch, ["--to", "carol", post["hash"][:12]])
    assert code == 0
    assert _FakeSendNode.instances[0].sent[0][1] == (
        f"Shared Tan post by you:\nmy own thought\n(tan {post['hash']})"
    )


def test_tan_share_poll_includes_question_and_options(
    tmp_path, monkeypatch
):
    _, poll, _ = _seed_tan_post(tmp_path, text="best colour?", poll=True)
    code = _run_share(tmp_path, monkeypatch, ["--to", "bob", poll["hash"][:12]])
    assert code == 0
    assert _FakeSendNode.instances[0].sent[0][1] == (
        "Shared Tan poll by alice:\nbest colour?\n1. red\n2. blue\n"
        f"(tan {poll['hash']})"
    )


def test_tan_share_repost_shares_the_original(tmp_path, monkeypatch):
    from tau.tangents.tan import create_post, create_repost, init_db

    data_dir = _init_node(tmp_path, "node", "bob")
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    alice = RNS.Identity()
    carol = RNS.Identity()
    conn = init_db(config.social_db_path)
    try:
        original = create_post(conn, alice, "worth sharing", name="alice", now=1000)
        repost = create_repost(conn, carol, original["hash"], name="carol", now=1001)
    finally:
        conn.close()
    code = _run_share(tmp_path, monkeypatch, ["--to", "bob", repost["hash"][:12]])
    assert code == 0
    assert _FakeSendNode.instances[0].sent[0][1] == (
        f"Shared Tan post by alice:\nworth sharing\n(tan {original['hash']})"
    )


def test_tan_share_blocked_author_is_an_error(tmp_path, capsys, monkeypatch):
    _, post, peer_hash = _seed_tan_post(tmp_path)
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="bob")
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({peer_hash})))
    code = _run_share(tmp_path, monkeypatch, ["--to", "carol", post["hash"][:12]])
    assert code == 1
    assert "blocked peer" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_tan_share_hidden_poll_is_an_error(tmp_path, capsys, monkeypatch):
    _, poll, _ = _seed_tan_post(tmp_path, text="best colour?", poll=True)
    assert (
        cli.main(
            [
                "tan",
                "rules",
                "--data-dir",
                str(tmp_path / "node"),
                "--kinds",
                "post",
            ]
        )
        == 0
    )
    capsys.readouterr()
    code = _run_share(tmp_path, monkeypatch, ["--to", "bob", poll["hash"][:12]])
    assert code == 1
    assert "hidden by your Tan display rules" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_tan_share_malformed_rules_is_an_error(tmp_path, capsys, monkeypatch):
    # A rules file that cannot be parsed must fail the share, not
    # silently become "display everything" and send a kind the file
    # used to hide.
    _, poll, _ = _seed_tan_post(tmp_path, text="best colour?", poll=True)
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="bob")
    config.tan_rules_path.write_text("{not json", encoding="utf-8")
    code = _run_share(tmp_path, monkeypatch, ["--to", "bob", poll["hash"][:12]])
    assert code == 1
    assert "could not be read" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_tan_share_unknown_post_is_an_error(tmp_path, capsys, monkeypatch):
    _seed_tan_post(tmp_path)
    code = _run_share(tmp_path, monkeypatch, ["--to", "bob", "cd" * 32])
    assert code == 1
    assert "unknown post" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_tan_share_truncates_long_post_to_the_message_limit(
    tmp_path, monkeypatch
):
    _, post, _ = _seed_tan_post(tmp_path, text="x" * 2000)
    code = _run_share(tmp_path, monkeypatch, ["--to", "bob", post["hash"][:12]])
    assert code == 0
    text = _FakeSendNode.instances[0].sent[0][1]
    assert len(text) <= 2000
    assert text.endswith(f"(tan {post['hash']})")
    assert "…" in text


def test_forward_file_message_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": "ab" * 16,
            "name": "photo.jpg",
            "size": 42,
            "sha256": "f" * 64,
            "path": "received/photo.jpg",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "f" * 32,
            "direction": "in",
        }
    )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "bob", "ffff"])
    assert code == 1
    assert "no text to send" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_forward_group_message_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "crew",
            "from": "alice",
            "destination": "ab" * 16,
            "text": "group secret",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "6" * 32,
            "direction": "in",
        }
    )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "bob", "6666"])
    assert code == 1
    assert "group message" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


def test_forward_too_long_is_an_error(tmp_path, capsys, monkeypatch):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "ab" * 16,
            "text": "x" * 1990,
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "1" * 32,
            "direction": "in",
        }
    )
    code = _run_forward(tmp_path, monkeypatch, ["--to", "bob", "1111"])
    assert code == 1
    assert "too long to forward" in capsys.readouterr().err
    assert _FakeSendNode.instances == []


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


def _seed_group_message(tmp_path):
    data_dir, sender_hash = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "crew hello",
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "crew",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "g-1",
        }
    )
    return data_dir, sender_hash


def test_react_fans_out_to_group_members(tmp_path, capsys, monkeypatch):
    data_dir, sender_hash = _seed_group_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["react", "--data-dir", str(data_dir), "g-1", "👍"])
    assert code == 0
    out = capsys.readouterr().out
    assert "reacted 👍 to the group message" in out
    assert "delivered to alice" in out
    assert "delivered to bob" in out
    node = _FakeReactNode.instances[0]
    assert node.sent == []
    # The author and send time of the reacted-to copy travel with the
    # fan-out, so each member can resolve its own copy.
    assert node.group_sent == [
        ("ab" * 16, "g-1", "👍", sender_hash.hex(), 12.0)
    ]


def test_react_to_own_group_message_uses_local_identity(tmp_path, capsys, monkeypatch):
    # CodeRabbit Major: an outbound group message names no single
    # destination, so cmd_react refused it ("no known sender") and
    # the signed payload would have carried an empty target_author.
    # The author of your own group message is this node.
    data_dir, _ = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "crew hello",
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "crew",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "g-1",
            "leg_ids": ["g-1", "g-2"],
            "direction": "out",
        }
    )
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["react", "--data-dir", str(data_dir), "g-1", "👍"])
    assert code == 0
    node = _FakeReactNode.instances[0]
    assert node.group_sent == [
        ("ab" * 16, "g-1", "👍", _FakeReactNode.destination_hexhash, 12.0)
    ]


def test_react_group_reports_failed_leg(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_group_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    monkeypatch.setattr(
        _FakeReactNode,
        "group_results",
        [
            {
                "to": "aa" * 16,
                "to_name": "alice",
                "delivered": True,
                "id": "r-group-1",
            },
            {
                "to": "bb" * 16,
                "to_name": "bob",
                "ok": False,
                "error": "no route to peer",
                "id": "r-group-1",
            },
        ],
    )
    _FakeReactNode.instances.clear()

    code = cli.main(["react", "--data-dir", str(data_dir), "g-1", "👍"])
    assert code == cli._EXIT_ERROR
    out = capsys.readouterr().out
    assert "delivered to alice" in out
    assert "failed for bob: no route to peer" in out


def test_react_group_strips_terminal_escapes_from_leg_errors(
    tmp_path, capsys, monkeypatch
):
    # A failed leg's error can carry a peer's text; escapes in it must
    # never reach the operator's terminal.
    data_dir, _ = _seed_group_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    monkeypatch.setattr(
        _FakeReactNode,
        "group_results",
        [
            {
                "to": "aa" * 16,
                "to_name": "alice",
                "delivered": True,
                "id": "r-group-1",
            },
            {
                "to": "bb" * 16,
                "to_name": "bob",
                "ok": False,
                "error": "\x1b[31mrefused\x1b[0m",
                "id": "r-group-1",
            },
        ],
    )
    _FakeReactNode.instances.clear()

    code = cli.main(["react", "--data-dir", str(data_dir), "g-1", "👍"])
    assert code == cli._EXIT_ERROR
    out = capsys.readouterr().out
    assert "\x1b" not in out
    assert "failed for bob: refused" in out


def test_react_group_json_and_clear(tmp_path, capsys, monkeypatch):
    data_dir, sender_hash = _seed_group_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(
        ["react", "--data-dir", str(data_dir), "g-1", "--clear", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["target"] == "g-1"
    assert payload["group"] == "ab" * 16
    assert payload["cleared"] is True
    assert payload["reaction"] == ""
    assert len(payload["results"]) == 2
    assert _FakeReactNode.instances[0].group_sent == [
        ("ab" * 16, "g-1", "", sender_hash.hex(), 12.0)
    ]


def _seed_sent_message(tmp_path):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
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
    return data_dir, recipient_hash


def _seed_editable_message(tmp_path):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
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
            "text": "typo here",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-1",
            "direction": "out",
        }
    )
    return data_dir, recipient_hash


def test_parser_edit_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["edit", "abcdef12", "fixed text"])
    assert args.command == "edit"
    assert args.target == "abcdef12"
    assert args.text == "fixed text"

    from_stdin = parser.parse_args(["edit", "abcdef12"])
    assert from_stdin.text is None


def test_edit_sends_to_the_message_recipient(tmp_path, capsys, monkeypatch):
    data_dir, recipient_hash = _seed_editable_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-1", "fixed here"])
    assert code == 0
    out = capsys.readouterr().out
    assert "edited your message to alice" in out
    node = _FakeReactNode.instances[0]
    assert node.started is False
    assert node.edited == [(recipient_hash.hex(), "m-1", "fixed here")]


def test_edit_json(tmp_path, capsys, monkeypatch):
    data_dir, recipient_hash = _seed_editable_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(
        ["edit", "--data-dir", str(data_dir), "m-1", "fixed here", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["target"] == "m-1"
    assert payload["to"] == recipient_hash.hex()
    assert payload["to_name"] == "alice"
    assert payload["text"] == "fixed here"
    assert payload["edited"] is True
    assert payload["id"] == "e-1"


def test_edit_rejects_blank_and_oversized_text(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_editable_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-1", "   "])
    assert code == cli._EXIT_ERROR
    assert "must not be empty" in capsys.readouterr().err
    code = cli.main(["edit", "--data-dir", str(data_dir), "m-1", "x" * 2001])
    assert code == cli._EXIT_ERROR
    assert "longer than 2000" in capsys.readouterr().err
    assert _FakeReactNode.instances == []


def test_edit_rejects_unknown_or_ambiguous_message(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_editable_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "zzz", "fixed"])
    assert code == cli._EXIT_ERROR
    assert "unknown message" in capsys.readouterr().err
    assert _FakeReactNode.instances == []


def test_edit_refuses_a_received_group_message(tmp_path, capsys, monkeypatch):
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
            "text": "group hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "g-1",
            "kind": "group",
            "group": "c" * 32,
            "group_name": "weekend",
        }
    )
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "g-1", "fixed"])
    assert code == cli._EXIT_ERROR
    assert "not sent by this node" in capsys.readouterr().err
    assert _FakeReactNode.instances == []


def test_edit_refuses_a_received_message(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_messages(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-1", "fixed"])
    assert code == cli._EXIT_ERROR
    assert "only your own sent messages can be edited" in capsys.readouterr().err
    assert _FakeReactNode.instances == []


def test_edit_refuses_a_queued_message(tmp_path, capsys, monkeypatch):
    # An edit answered "not found" while the queued copy is still in
    # the outbox would let that copy arrive later with the old text;
    # refuse and point at outbox cancel instead.
    data_dir, recipient_hash = _seed_editable_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    Mailbox(config.mailbox_path).enqueue(
        to=recipient_hash, text="typo here", message_id="m-1"
    )
    monkeypatch.setattr(cli, "TauNode", _FakeReactNode)
    _FakeReactNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-1", "fixed"])
    assert code == cli._EXIT_ERROR
    err = capsys.readouterr().err
    assert "still queued" in err
    assert "outbox cancel" in err
    assert _FakeReactNode.instances == []


def test_messages_shows_edited_marker(tmp_path, capsys):
    data_dir, _ = _seed_messages(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.apply_edit("m-1", "fixed here", edited_at=42.0)

    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "fixed here" in out
    assert "(edited)" in out

class _FakeRetractNode:
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

    def send_retraction(
        self, target, message_id, *, timeout=20.0, discover_timeout=None
    ):
        self.sent.append((target, message_id))
        return {"ok": True, "target": message_id, "deleted": True, "id": "r-1"}


def test_retract_sends_to_the_recipient_and_deletes_the_local_copy(
    tmp_path, capsys, monkeypatch
):
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    Mailbox(config.mailbox_path).record_sent(
        "m-9", to=recipient_hash, text="sent hello"
    )
    monkeypatch.setattr(cli, "TauNode", _FakeRetractNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-9"])
    assert code == 0
    out = capsys.readouterr().out
    assert "retracted" in out
    assert "alice" in out
    node = _FakeRetractNode.instances[0]
    assert node.started is False
    assert node.sent == [(recipient_hash.hex(), "m-9")]
    # The local sent copy is gone too.
    inbox = Inbox(TauConfig.resolve(data_dir=data_dir, name="bob").inbox_path)
    assert inbox.list_messages(direction="out") == []
    # So is the sent record: `tau messages --sent` must not keep
    # showing the retracted text.
    assert Mailbox(config.mailbox_path).sent_list() == []
    # The retraction is recorded durably, so the bridge drops the
    # console's outbound entry when it loads after a serve-down retract.
    assert Mailbox(config.mailbox_path).is_sent_retracted("m-9") is True


def test_retract_reports_when_the_peer_had_no_copy(
    tmp_path, capsys, monkeypatch
):
    data_dir, _ = _seed_sent_message(tmp_path)

    class _NoCopyNode(_FakeRetractNode):
        def send_retraction(
            self, target, message_id, *, timeout=20.0, discover_timeout=None
        ):
            self.sent.append((target, message_id))
            return {"ok": True, "target": message_id, "deleted": False, "id": "r-1"}

    monkeypatch.setattr(cli, "TauNode", _NoCopyNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-9", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["target"] == "m-9"
    assert payload["deleted"] is False


def test_retract_refuses_a_received_message(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_messages(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeRetractNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-1"])
    assert code == cli._EXIT_ERROR
    assert "not sent by this node" in capsys.readouterr().err
    assert _FakeRetractNode.instances == []


def test_retract_sends_group_retract_and_deletes_the_local_copy(
    tmp_path, capsys, monkeypatch
):
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
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
            "leg_ids": ["m-10", "m-11"],
            "leg_sent_at": 100.0,
        }
    )
    mailbox = Mailbox(config.mailbox_path)
    mailbox.record_sent("m-10", to=recipient_hash, text="group hello")
    mailbox.record_sent("m-11", to=recipient_hash, text="group hello")

    class _GroupNode(_FakeRetractNode):
        def send_group_retraction(
            self,
            group,
            message_id,
            *,
            timeout=20.0,
            discover_timeout=None,
            target_sent_at=None,
            target_author=None,
        ):
            self.sent.append((group, message_id, target_sent_at))
            return {
                "ok": True,
                "group": group,
                "target": message_id,
                "id": "r-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": recipient_hash.hex(),
                        "to_name": "alice",
                        "delivered": True,
                        "deleted": True,
                        "id": "r-1",
                    }
                ],
            }

    monkeypatch.setattr(cli, "TauNode", _GroupNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-10"])
    assert code == 0
    out = capsys.readouterr().out
    assert "retracted your group message" in out
    assert "alice" in out
    node = _FakeRetractNode.instances[0]
    assert node.started is False
    assert node.sent == [("ab" * 16, "m-10", 100.0)]
    # The local sent copy is gone too (the seeded m-9 DM stays).
    outbox = Inbox(config.inbox_path).list_messages(direction="out")
    assert [m["id"] for m in outbox] == ["m-9"]
    # So are the sent records for every leg: `tau messages --sent`
    # must not keep showing the retracted text.
    assert Mailbox(config.mailbox_path).sent_list() == []
    # The group retraction is recorded durably, so the bridge drops
    # the console's outbound legs when it loads after a serve-down
    # retract; a single leg id never matches on its own.
    mailbox = Mailbox(config.mailbox_path)
    assert mailbox.is_sent_group_retracted("ab" * 16, 100.0) is True
    assert mailbox.is_sent_retracted("m-10") is False


def _seed_moderator_group(tmp_path, *, creator_is_local: bool):
    """A data dir with an identity, a group, and one received message.

    Returns (data_dir, group_id, author_hash).  When
    *creator_is_local* the local identity created the group, so it
    may moderate; otherwise another peer did, so it may not.
    """
    from tau.groups import GroupStore, derive_group_id

    data_dir = tmp_path / "node"
    assert (
        cli.main(
            [
                "init",
                "--data-dir",
                str(data_dir),
                "--name",
                "bob",
                "--no-auto-interface",
                "--json",
            ]
        )
        == 0
    )
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    identity = load_identity(config.identity_path)
    own_hash = destination_hash(identity).hex()
    author = RNS.Identity()
    author_hash = destination_hash(author)
    third = RNS.Identity()
    third_hash = destination_hash(third)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(author_hash, author.get_public_key(), name="alice", now=100.0)
    registry.upsert(third_hash, third.get_public_key(), name="carol", now=100.0)
    registry.save()
    creator_hash = own_hash if creator_is_local else author_hash.hex()
    members = sorted([own_hash, author_hash.hex(), third_hash.hex()])
    group_id = derive_group_id(creator_hash, "crew", members)
    store = GroupStore(config.groups_path)
    assert (
        store.create("crew", members, created_by=creator_hash) is not None
    )
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": author_hash.hex(),
            "text": "crew hello",
            "sent_at": 100.0,
            "received_at": 110.0,
            "id": "m-20",
            "direction": "in",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
        }
    )
    return data_dir, group_id, author_hash


def test_retract_moderator_group_message(tmp_path, capsys, monkeypatch):
    data_dir, group_id, author_hash = _seed_moderator_group(
        tmp_path, creator_is_local=True
    )
    config = TauConfig.resolve(data_dir=data_dir, name="bob")

    class _ModeratorNode(_FakeRetractNode):
        def send_group_retraction(
            self,
            group,
            message_id,
            *,
            timeout=20.0,
            discover_timeout=None,
            target_sent_at=None,
            target_author=None,
        ):
            self.sent.append(
                (group, message_id, target_sent_at, target_author)
            )
            return {
                "ok": True,
                "group": group,
                "target": message_id,
                "id": "r-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": author_hash.hex(),
                        "to_name": "alice",
                        "delivered": True,
                        "deleted": True,
                        "id": "r-1",
                    }
                ],
            }

    monkeypatch.setattr(cli, "TauNode", _ModeratorNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-20"])
    assert code == 0
    out = capsys.readouterr().out
    assert "retracted alice's group message" in out
    node = _FakeRetractNode.instances[0]
    assert node.started is False
    # The fan-out named the message's author, not this node.
    assert node.sent == [(group_id, "m-20", 100.0, author_hash.hex())]
    # The local received copy is gone.
    assert Inbox(config.inbox_path).list_messages() == []


def test_retract_refuses_group_message_from_non_creator(
    tmp_path, capsys, monkeypatch
):
    data_dir, _, _ = _seed_moderator_group(tmp_path, creator_is_local=False)
    monkeypatch.setattr(cli, "TauNode", _FakeRetractNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-20"])
    assert code == cli._EXIT_ERROR
    assert "not sent by this node" in capsys.readouterr().err
    assert _FakeRetractNode.instances == []


def test_retract_keeps_group_copy_when_a_member_is_unreachable(
    tmp_path, capsys, monkeypatch
):
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
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

    class _PartialNode(_FakeRetractNode):
        def send_group_retraction(
            self,
            group,
            message_id,
            *,
            timeout=20.0,
            discover_timeout=None,
            target_sent_at=None,
            target_author=None,
        ):
            return {
                "ok": False,
                "group": group,
                "target": message_id,
                "id": "r-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": recipient_hash.hex(),
                        "to_name": "alice",
                        "ok": False,
                        "error": "no route to peer",
                        "id": "r-1",
                    }
                ],
            }

    monkeypatch.setattr(cli, "TauNode", _PartialNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-10"])
    assert code == cli._EXIT_ERROR
    err = capsys.readouterr().err
    assert "some members" in err
    assert "no route to peer" in err
    # The failed retract leaves the local sent copy in place, so the
    # user can retry once the member is online (the seeded m-9 DM
    # stays alongside it).
    outbox = Inbox(config.inbox_path).list_messages(direction="out")
    assert {m["id"] for m in outbox} == {"m-9", "m-10"}
    assert len(Mailbox(config.mailbox_path).sent_list()) == 1
    # No marker either: a partial fan-out keeps the console legs, so
    # the retract stays retryable.
    assert (
        Mailbox(config.mailbox_path).is_sent_group_retracted("ab" * 16, 100.0)
        is False
    )


def test_retract_refuses_group_message_without_leg_send_time(
    tmp_path, capsys, monkeypatch
):
    # A group message sent before group retracts landed carries no
    # shared leg send time, so no context can name it for the members.
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
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
        }
    )
    monkeypatch.setattr(cli, "TauNode", _FakeRetractNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-10"])
    assert code == cli._EXIT_ERROR
    assert "no leg send time" in capsys.readouterr().err
    assert _FakeRetractNode.instances == []


def _seed_sent_group_message(tmp_path):
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "",
            "text": "group typo",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-10",
            "direction": "out",
            "kind": "group",
            "group": "ab" * 16,
            "group_name": "team",
            "leg_ids": ["m-10", "m-11"],
            "leg_sent_at": 100.0,
        }
    )
    return data_dir, recipient_hash


def test_edit_sends_group_edit_to_every_member(
    tmp_path, capsys, monkeypatch
):
    data_dir, recipient_hash = _seed_sent_group_message(tmp_path)

    class _GroupEditNode(_FakeRetractNode):
        def send_group_edit(
            self,
            group,
            message_id,
            text,
            *,
            timeout=20.0,
            discover_timeout=None,
            target_sent_at=None,
        ):
            self.sent.append((group, message_id, text, target_sent_at))
            return {
                "ok": True,
                "group": group,
                "target": message_id,
                "id": "e-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": recipient_hash.hex(),
                        "to_name": "alice",
                        "delivered": True,
                        "edited": True,
                        "id": "e-1",
                    }
                ],
            }

    monkeypatch.setattr(cli, "TauNode", _GroupEditNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-10", "group fixed"])
    assert code == 0
    out = capsys.readouterr().out
    assert "edited your group message" in out
    assert "alice" in out
    node = _FakeRetractNode.instances[0]
    assert node.started is False
    assert node.sent == [("ab" * 16, "m-10", "group fixed", 100.0)]


def test_edit_reports_deferred_scheduled_member(
    tmp_path, capsys, monkeypatch
):
    data_dir, recipient_hash = _seed_sent_group_message(tmp_path)

    class _DeferredEditNode(_FakeRetractNode):
        def send_group_edit(
            self,
            group,
            message_id,
            text,
            *,
            timeout=20.0,
            discover_timeout=None,
            target_sent_at=None,
        ):
            self.sent.append((group, message_id, text, target_sent_at))
            return {
                "ok": True,
                "group": group,
                "target": message_id,
                "id": "e-1",
                "cancelled_queued": ["m-10"],
                "requeued": ["m-10"],
                "results": [
                    {
                        "to": recipient_hash.hex(),
                        "to_name": "alice",
                        "delivered": True,
                        "deferred": True,
                        "edited": False,
                        "id": "e-1",
                    }
                ],
            }

    monkeypatch.setattr(cli, "TauNode", _DeferredEditNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-10", "group fixed"])
    assert code == 0
    out = capsys.readouterr().out
    assert "edit is queued for alice" in out
    assert "re-queued leg" in out


def test_edit_keeps_group_copy_when_a_member_is_unreachable(
    tmp_path, capsys, monkeypatch
):
    data_dir, recipient_hash = _seed_sent_group_message(tmp_path)

    class _PartialEditNode(_FakeRetractNode):
        def send_group_edit(
            self,
            group,
            message_id,
            text,
            *,
            timeout=20.0,
            discover_timeout=None,
            target_sent_at=None,
        ):
            self.sent.append((group, message_id, text, target_sent_at))
            return {
                "ok": False,
                "group": group,
                "target": message_id,
                "id": "e-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": recipient_hash.hex(),
                        "to_name": "alice",
                        "ok": False,
                        "error": "no path to peer",
                        "id": "e-1",
                    }
                ],
            }

    monkeypatch.setattr(cli, "TauNode", _PartialEditNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-10", "group fixed"])
    assert code == cli._EXIT_ERROR
    err = capsys.readouterr().err
    assert "reached only some members" in err
    assert "retry `tau edit`" in err
    # The local sent copy is kept, so the edit stays retryable.
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    outbox = Inbox(config.inbox_path).list_messages(direction="out")
    kept = next(m for m in outbox if m["id"] == "m-10")
    assert kept["text"] == "group typo"
    assert "edited_at" not in kept


def test_edit_reports_when_the_local_copy_could_not_update(
    tmp_path, capsys, monkeypatch
):
    # Every member received the edit, but the node's own copies did not
    # update: the CLI must report it instead of a silent success, so
    # the edit stays retryable.
    data_dir, recipient_hash = _seed_sent_group_message(tmp_path)

    class _StaleLocalNode(_FakeRetractNode):
        def send_group_edit(
            self,
            group,
            message_id,
            text,
            *,
            timeout=20.0,
            discover_timeout=None,
            target_sent_at=None,
        ):
            self.sent.append((group, message_id, text, target_sent_at))
            return {
                "ok": False,
                "local_errors": ["sent copy not updated: disk full"],
                "group": group,
                "target": message_id,
                "id": "e-1",
                "cancelled_queued": [],
                "results": [
                    {
                        "to": recipient_hash.hex(),
                        "to_name": "alice",
                        "delivered": True,
                        "edited": True,
                        "id": "e-1",
                    }
                ],
            }

    monkeypatch.setattr(cli, "TauNode", _StaleLocalNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-10", "group fixed"])
    assert code == cli._EXIT_ERROR
    err = capsys.readouterr().err
    assert "local copy could not be updated" in err
    assert "disk full" in err
    assert "retry `tau edit`" in err


def test_edit_refuses_group_message_without_leg_send_time(
    tmp_path, capsys, monkeypatch
):
    # A group message sent before group edits landed carries no
    # shared leg send time, so no context can name it for the members.
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
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
        }
    )
    monkeypatch.setattr(cli, "TauNode", _FakeRetractNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["edit", "--data-dir", str(data_dir), "m-10", "fixed"])
    assert code == cli._EXIT_ERROR
    assert "no leg send time" in capsys.readouterr().err
    assert _FakeRetractNode.instances == []


def test_retract_rejects_unknown_messages(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_sent_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeRetractNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "zzz"])
    assert code == cli._EXIT_ERROR
    assert "unknown message" in capsys.readouterr().err
    assert _FakeRetractNode.instances == []


def test_retract_refuses_a_queued_message(tmp_path, capsys, monkeypatch):
    # A retract answered "not found" while the queued copy is still in
    # the outbox would let that copy arrive later; refuse and point at
    # outbox cancel instead.
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    Mailbox(config.mailbox_path).enqueue(
        to=recipient_hash, text="sent hello", message_id="m-9"
    )
    monkeypatch.setattr(cli, "TauNode", _FakeRetractNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-9"])
    assert code == cli._EXIT_ERROR
    err = capsys.readouterr().err
    assert "still queued" in err
    assert "outbox cancel" in err
    assert _FakeRetractNode.instances == []


def test_retract_reports_an_offline_peer(tmp_path, capsys, monkeypatch):
    data_dir, recipient_hash = _seed_sent_message(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    Mailbox(config.mailbox_path).record_sent(
        "m-9", to=recipient_hash, text="sent hello"
    )

    class _OfflineNode(_FakeRetractNode):
        def send_retraction(
            self, target, message_id, *, timeout=20.0, discover_timeout=None
        ):
            raise cli.NodeError("no path to peer")

    monkeypatch.setattr(cli, "TauNode", _OfflineNode)
    _FakeRetractNode.instances.clear()

    code = cli.main(["retract", "--data-dir", str(data_dir), "m-9"])
    assert code == cli._EXIT_ERROR
    err = capsys.readouterr().err
    assert "could not deliver retract" in err
    assert "online" in err
    # The failed retract leaves the local sent copy in place.
    inbox = Inbox(TauConfig.resolve(data_dir=data_dir, name="bob").inbox_path)
    assert len(inbox.list_messages(direction="out")) == 1
    assert len(Mailbox(config.mailbox_path).sent_list()) == 1


class _FakeGroupStore:
    """Stand-in for GroupStore.get: the seeded group only."""

    def get(self, group_id):
        if group_id == "ab" * 16:
            return {
                "id": "ab" * 16,
                "name": "crew",
                "members": ["aa" * 16, "bb" * 16],
                "creator": "aa" * 16,
            }
        return None


class _FakeReactNode:
    instances = []
    # Tests may override the fan-out results (monkeypatch restores it).
    group_results = None
    # The local node's identity hash, as TauNode.destination_hexhash.
    destination_hexhash = "ef" * 16

    def __init__(self, config, name=None):
        self.config = config
        self.sent = []
        self.group_sent = []
        self.edited = []
        self.started = False
        self.groups = _FakeGroupStore()
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

    def send_group_reaction(
        self,
        group,
        message_id,
        reaction,
        *,
        timeout=20.0,
        discover_timeout=None,
        target_author=None,
        target_sent_at=None,
    ):
        # The real node takes a group record and returns its id; mirror
        # that so the fan-out report reads the id, not the record.
        group_id = group["id"] if isinstance(group, dict) else group
        self.group_sent.append(
            (group_id, message_id, reaction, target_author, target_sent_at)
        )
        results = type(self).group_results
        if results is None:
            results = [
                {
                    "to": "aa" * 16,
                    "to_name": "alice",
                    "delivered": True,
                    "id": "r-group-1",
                },
                {
                    "to": "bb" * 16,
                    "to_name": "bob",
                    "delivered": True,
                    "id": "r-group-1",
                },
            ]
        return {
            "ok": all(r.get("delivered") for r in results),
            "v": 1,
            "group": group_id,
            "name": "crew",
            "target": message_id,
            "reaction": reaction,
            "cleared": not reaction,
            "id": "r-group-1",
            "results": results,
        }

    def send_edit(
        self, target, message_id, text, *, timeout=20.0, discover_timeout=None
    ):
        self.edited.append((target, message_id, text))
        return {"ok": True, "edited": True, "target": message_id, "id": "e-1"}


def _post_image_fixture(tmp_path, capsys):
    import hashlib

    from tau.tangents.tan import init_db, media_path

    data_dir = tmp_path / "node"
    assert cli.main(["init", "--data-dir", str(data_dir), "--json"]) == 0
    capsys.readouterr()
    png = tmp_path / "photo.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
    digest = hashlib.sha256(png.read_bytes()).hexdigest()
    code = cli.main(
        [
            "tan",
            "post",
            "--data-dir",
            str(data_dir),
            "--no-publish",
            "look at this",
            "--image",
            str(png),
        ]
    )
    assert code == 0
    capsys.readouterr()
    return data_dir, png, digest


def test_tan_post_with_image_stores_attachment(tmp_path, capsys):
    import hashlib

    from tau.tangents.tan import feed, init_db, media_path

    data_dir, png, digest = _post_image_fixture(tmp_path, capsys)
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        posts = feed(conn)
    finally:
        conn.close()
    assert len(posts) == 1
    assert posts[0]["image"] == {
        "sha256": digest,
        "size": len(png.read_bytes()),
        "mime": "image/png",
        "name": "photo.png",
    }
    assert media_path(data_dir, digest) is not None
    assert cli.main(["tan", "timeline", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "photo.png" in out
    assert f"tau tan media {digest}" in out
    # The cached image downloads without touching the mesh.
    assert cli.main(["tan", "media", "--data-dir", str(data_dir), digest]) == 0
    out = capsys.readouterr().out
    assert "already cached" in out
    assert digest[:12] in out


def test_tan_post_image_rejects_bad_input(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert cli.main(["init", "--data-dir", str(data_dir), "--json"]) == 0
    capsys.readouterr()
    text_file = tmp_path / "notes.txt"
    text_file.write_text("not an image")
    base = ["tan", "post", "--data-dir", str(data_dir), "--no-publish", "hi"]
    assert cli.main([*base, "--image", str(text_file)]) == 1
    big = tmp_path / "big.png"
    big.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * (2 * 1024 * 1024))
    assert cli.main([*base, "--image", str(big)]) == 1
    png = tmp_path / "photo.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 10)
    assert cli.main([*base, "--image", str(png), "--delay", "5m"]) == 2
    # A reply cannot carry an image: post first, then reply to it.
    assert cli.main([*base, "--image", str(png)]) == 0
    capsys.readouterr()
    from tau.tangents.tan import feed, init_db

    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post_hash = feed(conn)[0]["hash"]
    finally:
        conn.close()
    assert (
        cli.main([*base, "--image", str(png), "--reply-to", post_hash]) == 2
    )


def test_tan_media_rejects_unknown_image(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert cli.main(["init", "--data-dir", str(data_dir), "--json"]) == 0
    capsys.readouterr()
    assert (
        cli.main(["tan", "media", "--data-dir", str(data_dir), "ab" * 32]) == 2
    )
    assert cli.main(["tan", "media", "--data-dir", str(data_dir), "nope"]) == 2


def test_print_tan_posts_counts_appear_once(capsys):
    from tau.cli import _print_tan_posts

    post = {
        "name": "alice",
        "author": "ab" * 16,
        "hash": "cd" * 32,
        "created_at": 1000.0,
        "text": "hi",
        "kind": "post",
        "likes": 3,
        "liked": False,
        "reposts": 1,
        "reposted": False,
        "quotes": 2,
    }
    _print_tan_posts([post], json_output=False)
    out = capsys.readouterr().out
    assert out.count("♥ 3") == 1
    assert out.count("↻ 1") == 1
    assert out.count("❝ 2") == 1


def test_fetch_running_bounds_reply_wait(tmp_path, monkeypatch):
    from tau import local_media

    seen = {}

    def fake_call_local(path, *, reply_timeout=None, **request):
        seen["reply_timeout"] = reply_timeout
        seen["path"] = path
        return {"ok": True}

    monkeypatch.setattr(local_media, "call_local", fake_call_local)
    result = local_media.fetch_running(
        tmp_path, author="ab" * 16, sha256="cd" * 32, timeout=45.0
    )
    assert result == {"ok": True}
    assert seen["reply_timeout"] == 75.0
    assert str(seen["path"]).endswith("media.sock")


# -- live location beacons (issue #468) -------------------------------------

BEACON_PEER = "ab" * 16
BEACON_PIN = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}


def _seed_beacon(tmp_path, **kwargs):
    from tau.beacons import Beacons, save_beacons
    from tau.config import TauConfig

    config = TauConfig.resolve(data_dir=tmp_path / "node", name="tester")
    beacons = Beacons()
    beacon = beacons.start(
        target=kwargs.pop("target", BEACON_PEER),
        target_kind=kwargs.pop("target_kind", "dm"),
        location=kwargs.pop("location", BEACON_PIN),
        now=kwargs.pop("now", time.time()),
        **kwargs,
    )
    save_beacons(config.beacons_path, beacons)
    return config, beacon


def test_parser_beacon_arguments():
    parser = cli.build_parser()
    start = parser.parse_args(
        [
            "beacon", "start", "alice",
            "--location", "34.05,-83.88 Rock Springs Park",
            "--coarse", "--duration-mins", "30", "--interval-mins", "10",
        ]
    )
    assert start.beacon_command == "start"
    assert start.peer == "alice"
    assert start.func is cli.cmd_beacon_start
    assert start.coarse is True
    assert start.duration_mins == 30.0
    assert start.interval_mins == 10.0
    stop = parser.parse_args(["beacon", "stop", "--group", "crew"])
    assert stop.beacon_command == "stop"
    assert stop.group == "crew"
    assert stop.func is cli.cmd_beacon_stop
    update = parser.parse_args(
        ["beacon", "update", BEACON_PEER, "--location", "35,-84"]
    )
    assert update.beacon_command == "update"
    assert update.func is cli.cmd_beacon_update
    listed = parser.parse_args(["beacon", "list"])
    assert listed.beacon_command == "list"
    assert listed.func is cli.cmd_beacon_list


def test_beacon_list_shows_running_and_ended(tmp_path, capsys):
    _seed_beacon(tmp_path)
    code = cli.main(["beacon", "list", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    out = capsys.readouterr().out
    assert "Rock Springs Park" in out
    assert "update(s) sent" in out


def test_beacon_list_json(tmp_path, capsys):
    _, beacon = _seed_beacon(tmp_path)
    code = cli.main(
        ["beacon", "list", "--json", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert len(data["beacons"]) == 1
    assert data["beacons"][0]["id"] == beacon.id
    assert data["beacons"][0]["active"] is True


def test_beacon_stop_stops_running_beacon(tmp_path, capsys):
    _seed_beacon(tmp_path)
    code = cli.main(
        ["beacon", "stop", BEACON_PEER, "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "Stopped the live beacon" in capsys.readouterr().out
    # A second stop reports that nothing is running.
    code = cli.main(
        ["beacon", "stop", BEACON_PEER, "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "No beacon is running" in capsys.readouterr().out


def test_beacon_update_refreshes_position(tmp_path, capsys):
    _seed_beacon(tmp_path)
    code = cli.main(
        [
            "beacon", "update", BEACON_PEER,
            "--location", "35.0,-84.0 New Park",
            "--data-dir", str(tmp_path / "node"),
        ]
    )
    assert code == 0
    assert "New Park" in capsys.readouterr().out
    from tau.beacons import load_beacons
    from tau.config import TauConfig

    config = TauConfig.resolve(data_dir=tmp_path / "node", name="tester")
    stored = load_beacons(config.beacons_path)
    assert stored.beacons
    beacon = next(iter(stored.beacons.values()))
    assert beacon.location == {"lat": 35.0, "lon": -84.0, "name": "New Park"}


def test_beacon_update_requires_a_position(tmp_path, capsys):
    _seed_beacon(tmp_path)
    code = cli.main(
        ["beacon", "update", BEACON_PEER, "--data-dir", str(tmp_path / "node")]
    )
    assert code != 0


class _FakeBeaconNode:
    """One-shot node stub for `tau beacon start` (no Reticulum)."""

    instances = []

    def __init__(self, config, name=None):
        from tau.mailbox import Mailbox

        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.started = False
        self.start_calls = []
        self.update_calls = []
        _FakeBeaconNode.instances.append(self)

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.started = False

    def start_beacon(self, target, location, **kwargs):
        from tau.beacons import Beacons, load_beacons, save_beacons

        self.start_calls.append((target, location, kwargs))
        beacons = load_beacons(self.config.beacons_path)
        beacon = beacons.start(
            target=target,
            target_kind="group" if kwargs.get("group") else "dm",
            target_name="alice",
            location=location,
            coarse=kwargs.get("coarse", False),
            duration=kwargs.get("duration", 3600.0),
            interval=kwargs.get("interval", 300.0),
            location_file=kwargs.get("location_file"),
        )
        save_beacons(self.config.beacons_path, beacons)
        return beacon

    def send_beacon_update(self, beacon, **kwargs):
        self.update_calls.append(beacon.id)
        beacon.updates_sent += 1
        beacon.next_update_at = time.time() + beacon.interval
        return {"ok": True, "queued": True, "id": "f" * 32}


def test_beacon_start_sends_first_update_via_one_shot_node(
    tmp_path, capsys, monkeypatch
):
    monkeypatch.setattr(cli, "TauNode", _FakeBeaconNode)
    _FakeBeaconNode.instances.clear()
    code = cli.main(
        [
            "beacon", "start", BEACON_PEER,
            "--location", "34.05,-83.88 Rock Springs Park",
            "--data-dir", str(tmp_path / "node"),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Started a live beacon" in out
    assert "first update queued" in out
    node = _FakeBeaconNode.instances[0]
    assert len(node.start_calls) == 1
    assert len(node.update_calls) == 1
    assert node.started is False


def test_beacon_start_local_op_starts_and_sends(tmp_path):
    node = _FakeBeaconNode(
        TauConfig.resolve(data_dir=tmp_path / "node", name="tester")
    )
    result = cli._beacon_start_local(
        node,
        target=BEACON_PEER,
        location=BEACON_PIN,
        duration=1800.0,
        interval=300.0,
    )
    assert result["ok"] is True
    assert result["beacon"]["target"] == BEACON_PEER
    # The summary is taken after the first update went out.
    assert result["beacon"]["updates_sent"] == 1
    assert result["update"]["queued"] is True
    assert node.update_calls == [result["beacon"]["id"]]


# -- channel scheduled messages (#547) --------------------------------------

def _run_channel(tmp_path, *argv):
    return cli.main(["channel", "--data-dir", str(tmp_path / "node"), *argv])


def test_parser_channel_scheduled_arguments():
    parser = cli.build_parser()

    send = parser.parse_args(["channel", "send", "general", "hi", "--delay", "5m"])
    assert send.func is cli.cmd_channel_send
    assert send.delay == "5m"

    listed = parser.parse_args(["channel", "scheduled"])
    assert listed.func is cli.cmd_channel_scheduled
    assert listed.channel is None

    filtered = parser.parse_args(["channel", "scheduled", "general"])
    assert filtered.channel == "general"

    cancelled = parser.parse_args(["channel", "cancel-scheduled", "abcdef"])
    assert cancelled.func is cli.cmd_channel_cancel_scheduled
    assert cancelled.target == "abcdef"


def test_channel_send_delay_schedules_lists_and_cancels(tmp_path, capsys):
    _init_node(tmp_path, "node", "alice")
    assert _run_channel(tmp_path, "join", "general") == 0
    capsys.readouterr()

    code = _run_channel(tmp_path, "send", "general", "morning hello", "--delay", "5m")
    assert code == 0
    out = capsys.readouterr().out
    assert "scheduled" in out
    assert "publishes in 5m" in out

    code = _run_channel(tmp_path, "scheduled")
    assert code == 0
    out = capsys.readouterr().out
    assert "#general" in out
    assert "morning hello" in out
    entry_id = out.split()[0]

    code = _run_channel(tmp_path, "scheduled", "general")
    assert code == 0
    assert "morning hello" in capsys.readouterr().out

    code = _run_channel(tmp_path, "cancel-scheduled", entry_id[:8])
    assert code == 0
    assert "cancelled" in capsys.readouterr().out

    code = _run_channel(tmp_path, "scheduled")
    assert code == 0
    assert "No channel messages scheduled." in capsys.readouterr().out


def test_channel_send_delay_json_round_trip(tmp_path, capsys):
    _init_node(tmp_path, "node", "alice")
    assert _run_channel(tmp_path, "join", "general") == 0
    capsys.readouterr()

    code = cli.main(
        [
            "channel",
            "--data-dir",
            str(tmp_path / "node"),
            "--json",
            "send",
            "general",
            "later",
            "--delay",
            "5m",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["scheduled"]["channel"] == "general"
    assert payload["scheduled"]["text"] == "later"
    assert payload["scheduled"]["publish_at"] > time.time()

    code = cli.main(
        ["channel", "--data-dir", str(tmp_path / "node"), "--json", "scheduled"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["scheduled"]) == 1
    assert payload["scheduled"][0]["channel"] == "general"


def test_channel_send_delay_rejects_bad_combinations(tmp_path, capsys):
    _init_node(tmp_path, "node", "alice")
    assert _run_channel(tmp_path, "join", "general") == 0
    capsys.readouterr()

    code = _run_channel(
        tmp_path, "send", "general", "hi", "--delay", "5m", "--no-publish"
    )
    assert code == 2
    assert "--delay cannot be combined with --no-publish" in capsys.readouterr().err

    code = _run_channel(tmp_path, "send", "general", "hi", "--delay", "soon")
    assert code == 2
    assert "invalid --delay duration" in capsys.readouterr().err

    code = _run_channel(tmp_path, "send", "general", "hi", "--delay", "8d")
    assert code == 2
    assert "--delay duration must be at most 1w" in capsys.readouterr().err

    code = _run_channel(tmp_path, "send", "other", "hi", "--delay", "5m")
    assert code == 2
    assert "not a member of channel 'other'" in capsys.readouterr().err

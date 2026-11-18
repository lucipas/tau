"""Away message — one automatic reply per peer per away period (issue #515).

Covers the away state file (validation, persistence, corrupt-file
behaviour), the node hook on both DM receive paths (direct RNS and
LXMF), and the ``tau away`` CLI — all without a live network.
"""

from __future__ import annotations

import json
import threading
import time
import types
from types import SimpleNamespace

import RNS
import pytest

from tau import lxmf
from tau.away import (
    DEFAULT_AWAY_MESSAGE,
    AwayError,
    AwayState,
    away_lock,
    away_state_for,
    load_away,
    save_away,
)
from tau.cli import main as cli_main
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.node import TauNode
from tau.peers import PeerRegistry
from tau.protocol import MAX_MESSAGE_CHARS, decode, encode_message


# -- state file ---------------------------------------------------------------


def test_away_default_is_off(tmp_path):
    state = load_away(tmp_path / "away.json")
    assert state.enabled is False
    assert state.message == DEFAULT_AWAY_MESSAGE
    assert state.since is None
    assert state.replied_to == frozenset()


def test_away_on_uses_custom_message_and_resets_replied_to():
    state = AwayState.on("back on Monday", now=1234.5)
    assert state.enabled is True
    assert state.message == "back on Monday"
    assert state.since == 1234.5
    assert state.replied_to == frozenset()


def test_away_on_without_message_uses_default():
    assert AwayState.on(None).message == DEFAULT_AWAY_MESSAGE


def test_away_on_rejects_empty_and_overlong_messages():
    with pytest.raises(AwayError, match="must not be empty"):
        AwayState.on("   ")
    with pytest.raises(AwayError, match="longer than"):
        AwayState.on("x" * (MAX_MESSAGE_CHARS + 1))


def test_away_off_keeps_message_and_clears_replied_to():
    peer_hash = "ab" * 16
    state = AwayState.on("back soon").with_replied(peer_hash).off()
    assert state.enabled is False
    assert state.message == "back soon"
    assert state.since is None
    assert state.replied_to == frozenset()


def test_away_with_replied_rejects_bad_hash():
    with pytest.raises(AwayError):
        AwayState.on("back soon").with_replied("not-a-hash")


def test_away_round_trips_through_disk(tmp_path):
    path = tmp_path / "away.json"
    peer_hash = "ab" * 16
    state = AwayState.on("back soon", now=1234.5).with_replied(peer_hash)
    save_away(path, state)
    assert load_away(path) == state


def test_away_load_rejects_corrupt_file_but_state_for_fails_open(tmp_path):
    path = tmp_path / "away.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(AwayError):
        load_away(path)
    assert away_state_for(path).enabled is False
    path.write_text(json.dumps({"enabled": "yes", "message": "x"}), encoding="utf-8")
    with pytest.raises(AwayError, match="boolean"):
        load_away(path)
    assert away_state_for(path).enabled is False


def test_away_from_dict_requires_enabled_and_message():
    with pytest.raises(AwayError, match="required"):
        AwayState.from_dict({"enabled": True})
    with pytest.raises(AwayError, match="required"):
        AwayState.from_dict({"message": "x"})
    with pytest.raises(AwayError, match="must be a JSON object"):
        AwayState.from_dict(["nope"])


def test_away_lock_serializes_reload_inside_it(tmp_path):
    path = tmp_path / "away.json"
    save_away(path, AwayState.on("first"))
    with away_lock(path):
        state = load_away(path)
        save_away(path, state.with_replied("ab" * 16))
    assert load_away(path).replied_to == frozenset({"ab" * 16})


# -- node hook ----------------------------------------------------------------


def make_node(tmp_path, name="holder", on_message=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
    )
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    return node


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


def register_peer(node, name="alice"):
    sender = RNS.Identity()
    node.registry.upsert(
        destination_hash(sender), sender.get_public_key(), name=name
    )
    return sender


def deliver_dm(node, sender, text, message_id):
    payload = encode_message(text, message_id=message_id)
    return call_handler(node._handle_message, payload, remote_identity=sender)


def stub_flush(node, calls):
    """Replace the outbox flush with a recorder; the node is not started."""
    event = threading.Event()

    def _flush(*, timeout=None, max_peers=None, **kwargs):
        calls.append({"timeout": timeout, "max_peers": max_peers})
        event.set()
        return {"delivered": 0, "deposited": 0, "received": 0}

    node.flush_outbox = _flush  # type: ignore[method-assign]
    return event


def set_away(node, message="back soon"):
    save_away(node.config.away_path, AwayState.on(message))


def test_away_off_by_default_sends_no_reply(tmp_path):
    node = make_node(tmp_path)
    sender = register_peer(node)
    response = deliver_dm(node, sender, "hello?", "m-1")
    assert response["ok"] is True
    assert len(node.inbox.list_messages()) == 1
    assert node.mailbox.outbox_list() == []
    assert node.mailbox.sent_list() == []


def test_away_on_replies_once_per_peer(tmp_path):
    node = make_node(tmp_path)
    calls: list = []
    flush_ran = stub_flush(node, calls)
    alice = register_peer(node, "alice")
    bob = register_peer(node, "bob")
    set_away(node, "on holiday")

    first = deliver_dm(node, alice, "are you there?", "a-1")
    assert first["ok"] is True
    assert flush_ran.wait(timeout=2.0)
    assert calls == [{"timeout": 6.0, "max_peers": 5}]
    outbox = node.mailbox.outbox_list()
    assert len(outbox) == 1
    assert outbox[0].text == "on holiday"
    assert outbox[0].kind == "chat"
    assert outbox[0].to == destination_hash(alice)
    assert outbox[0].to_name == "alice"
    sent = node.mailbox.sent_list()
    assert len(sent) == 1
    assert sent[0]["to"] == destination_hash(alice).hex()
    outgoing = node.inbox.list_messages(direction="out")
    assert len(outgoing) == 1
    assert outgoing[0]["text"] == "on holiday"
    assert outgoing[0]["direction"] == "out"
    assert load_away(node.config.away_path).replied_to == frozenset(
        {destination_hash(alice).hex()}
    )

    # A second DM from Alice gets stored but no second reply.
    deliver_dm(node, alice, "hello again", "a-2")
    assert len(node.mailbox.outbox_list()) == 1
    assert len(node.inbox.list_messages()) == 2

    # A first DM from Bob gets his own reply.
    deliver_dm(node, bob, "ping", "b-1")
    assert len(node.mailbox.outbox_list()) == 2
    assert node.mailbox.outbox_list()[1].to == destination_hash(bob)


def test_away_ignores_unregistered_peer(tmp_path):
    node = make_node(tmp_path)
    stranger = RNS.Identity()
    set_away(node)
    response = deliver_dm(node, stranger, "hello?", "s-1")
    assert response["ok"] is True
    assert len(node.inbox.list_messages()) == 1
    assert node.mailbox.outbox_list() == []


def test_away_ignores_duplicate_delivery(tmp_path):
    node = make_node(tmp_path)
    calls: list = []
    stub_flush(node, calls)
    sender = register_peer(node)
    set_away(node)
    deliver_dm(node, sender, "hello?", "d-1")
    deliver_dm(node, sender, "hello?", "d-1")
    assert len(node.mailbox.outbox_list()) == 1
    assert len(calls) == 1


def test_away_off_and_on_again_resets_replied_to(tmp_path):
    node = make_node(tmp_path)
    stub_flush(node, [])
    sender = register_peer(node)
    set_away(node)
    deliver_dm(node, sender, "one", "r-1")
    assert len(node.mailbox.outbox_list()) == 1

    state = load_away(node.config.away_path)
    save_away(node.config.away_path, state.off())
    deliver_dm(node, sender, "two", "r-2")
    assert len(node.mailbox.outbox_list()) == 1

    set_away(node)
    deliver_dm(node, sender, "three", "r-3")
    assert len(node.mailbox.outbox_list()) == 2


def test_away_corrupt_file_sends_no_reply(tmp_path):
    node = make_node(tmp_path)
    sender = register_peer(node)
    node.config.away_path.write_text("{broken", encoding="utf-8")
    response = deliver_dm(node, sender, "hello?", "c-1")
    assert response["ok"] is True
    assert len(node.inbox.list_messages()) == 1
    assert node.mailbox.outbox_list() == []


def test_away_reply_via_lxmf(tmp_path):
    node = make_node(tmp_path)
    calls: list = []
    flush_ran = stub_flush(node, calls)
    sender = register_peer(node, "alice")
    set_away(node, "away via lxmf")
    message = types.SimpleNamespace(
        source_hash=lxmf.lxmf_delivery_hash(sender),
        signature_validated=True,
        content_as_string=lambda: "hello via lxmf",
        hash=b"\x11" * 16,
        timestamp=1_700_000_000.0,
        fields=None,
    )
    node._deliver_lxmf_message(message)
    assert flush_ran.wait(timeout=2.0)
    outbox = node.mailbox.outbox_list()
    assert len(outbox) == 1
    assert outbox[0].text == "away via lxmf"
    assert outbox[0].to == destination_hash(sender)


def test_away_reply_honours_disappearing_default(tmp_path, monkeypatch):
    from tau.disappearing import save_disappearing_defaults
    from tau.disappearing import DisappearingDefaults

    node = make_node(tmp_path)
    stub_flush(node, [])
    sender = register_peer(node)
    sender_hash = destination_hash(sender).hex()
    save_disappearing_defaults(
        node.config.disappearing_path,
        DisappearingDefaults().with_peer(sender_hash, 3600),
    )
    set_away(node)
    before = time.time()
    deliver_dm(node, sender, "hello?", "e-1")
    entry = node.mailbox.outbox_list()[0]
    assert entry.message_expires_at is not None
    assert before + 3500 < entry.message_expires_at < before + 3700


# -- CLI ----------------------------------------------------------------------


def test_cli_away_on_off_status(tmp_path, capsys):
    data_dir = tmp_path / "node"
    data_dir.mkdir(parents=True)
    assert cli_main(["away", "status", "--data-dir", str(data_dir)]) == 0
    assert "off" in capsys.readouterr().out

    assert (
        cli_main(
            ["away", "on", "--message", "back soon", "--data-dir", str(data_dir)]
        )
        == 0
    )
    assert "back soon" in capsys.readouterr().out

    assert cli_main(["away", "status", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "on" in out and "back soon" in out

    assert cli_main(["away", "off", "--data-dir", str(data_dir)]) == 0
    assert "off" in capsys.readouterr().out
    assert cli_main(["away", "off", "--data-dir", str(data_dir)]) == 0
    assert "already off" in capsys.readouterr().out


def test_cli_away_on_defaults_to_default_message(tmp_path, capsys):
    data_dir = tmp_path / "node"
    data_dir.mkdir(parents=True)
    assert cli_main(["away", "on", "--data-dir", str(data_dir)]) == 0
    assert DEFAULT_AWAY_MESSAGE in capsys.readouterr().out


def test_cli_away_json_round_trip(tmp_path, capsys):
    data_dir = tmp_path / "node"
    data_dir.mkdir(parents=True)
    assert (
        cli_main(["away", "on", "--json", "--data-dir", str(data_dir)]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["enabled"] is True
    assert payload["message"] == DEFAULT_AWAY_MESSAGE
    assert cli_main(["away", "status", "--json", "--data-dir", str(data_dir)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["enabled"] is True
    assert payload["replied_to"] == []


def test_cli_away_on_rejects_empty_message(tmp_path, capsys):
    data_dir = tmp_path / "node"
    data_dir.mkdir(parents=True)
    assert (
        cli_main(
            ["away", "on", "--message", "   ", "--data-dir", str(data_dir)]
        )
        == 1
    )
    assert "error:" in capsys.readouterr().err

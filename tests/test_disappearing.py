"""Per-conversation disappearing defaults: store, node send paths, CLI."""

from __future__ import annotations

import json
import os
import stat
import time
import types

import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.disappearing import (
    DisappearingDefaults,
    DisappearingError,
    group_default_for,
    load_disappearing_defaults,
    peer_default_for,
    save_disappearing_defaults,
)
from tau.inbox import Inbox
from tau.node import TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import APP_NAME, ASPECT, parse_group_message, parse_message

ALICE = "aa" * 16
BOB = "bb" * 16
GROUP_ID = "cc" * 16


# -- module ---------------------------------------------------------------

def test_missing_file_means_no_defaults(tmp_path):
    defaults = load_disappearing_defaults(tmp_path / "disappearing.json")
    assert defaults.peers == {}
    assert defaults.groups == {}
    assert defaults.peer_default(ALICE) is None
    assert defaults.group_default(GROUP_ID) is None
    assert defaults.peer_default(None) is None
    assert defaults.group_default(None) is None


def test_unreadable_location_raises(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(DisappearingError):
        load_disappearing_defaults(blocker / "disappearing.json")


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "disappearing.json"
    saved = save_disappearing_defaults(
        path,
        DisappearingDefaults(peers={ALICE: 3600.0}, groups={GROUP_ID: 60.0}),
    )
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_disappearing_defaults(path)
    assert loaded.peers == {ALICE: 3600.0}
    assert loaded.groups == {GROUP_ID: 60.0}
    assert loaded.to_dict() == {
        "peers": {ALICE: 3600.0},
        "groups": {GROUP_ID: 60.0},
    }


def test_malformed_documents_raise(tmp_path):
    path = tmp_path / "disappearing.json"
    for document in (
        {"peers": {"alice": 60}},
        {"peers": {ALICE: 0}},
        {"peers": {ALICE: -5}},
        {"peers": {ALICE: 31 * 24 * 3600}},
        {"peers": {ALICE: "1h"}},
        {"peers": {ALICE: True}},
        {"peers": {ALICE: None}},
        {"peers": {}},
        {"groups": {GROUP_ID: 60}},
        {"groups": {"not-a-group": 60}},
        {"groups": {GROUP_ID: 0}},
        "not-an-object",
    ):
        path.write_text(json.dumps(document), encoding="utf-8")
        with pytest.raises(DisappearingError):
            load_disappearing_defaults(path)
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(DisappearingError):
        load_disappearing_defaults(path)


def test_fail_open_helpers_return_none_on_corrupt_file(tmp_path, caplog):
    # A send must never break over a corrupt defaults file: the
    # message goes out without a default, and the warning surfaces it.
    path = tmp_path / "disappearing.json"
    path.write_text("{not json", encoding="utf-8")
    assert peer_default_for(path, ALICE) is None
    assert group_default_for(path, GROUP_ID) is None
    assert "unusable" in caplog.text


def test_with_and_without_are_immutable_and_idempotent():
    defaults = DisappearingDefaults()
    added = defaults.with_peer(ALICE, 3600)
    assert added.peer_default(ALICE) == 3600.0
    assert added.peer_default(ALICE.upper()) == 3600.0
    assert added.with_peer(ALICE, 3600).peers == added.peers
    assert defaults.peer_default(ALICE) is None
    removed = added.without_peer(ALICE)
    assert removed.peer_default(ALICE) is None
    assert removed.without_peer(ALICE).peers == {}
    assert added.peer_default(ALICE) == 3600.0

    grouped = defaults.with_group(GROUP_ID, 60)
    assert grouped.group_default(GROUP_ID) == 60.0
    assert grouped.group_default(GROUP_ID.upper()) == 60.0
    assert defaults.group_default(GROUP_ID) is None
    assert grouped.without_group(GROUP_ID).groups == {}


def test_seconds_must_be_positive_and_within_mail_ttl():
    with pytest.raises(DisappearingError):
        DisappearingDefaults().with_peer(ALICE, 0)
    with pytest.raises(DisappearingError):
        DisappearingDefaults().with_group(GROUP_ID, 8 * 24 * 3600)
    # The cap is 7 days, the range the console picker offers.
    assert (
        DisappearingDefaults().with_peer(ALICE, 7 * 24 * 3600).peer_default(ALICE)
        == 7 * 24 * 3600.0
    )
    # An integer too large for a float is malformed, not a crash.
    with pytest.raises(DisappearingError):
        DisappearingDefaults().with_peer(ALICE, 10**400)


# -- node send paths ------------------------------------------------------

def _make_peer(name: str = "alice") -> Peer:
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    return Peer(
        destination_hash=destination,
        name=name,
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )


def _make_node(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    return TauNode(config, registry=PeerRegistry(config.peers_path))


def _set_peer_default(node, peer, seconds):
    save_disappearing_defaults(
        node.config.disappearing_path,
        load_disappearing_defaults(node.config.disappearing_path).with_peer(
            peer.hexhash, seconds
        ),
    )


def test_send_message_applies_peer_default(tmp_path, monkeypatch):
    node = _make_node(tmp_path)
    peer = _make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    _set_peer_default(node, peer, 3600)
    sent = {}
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: (sent.setdefault("payload", args[2]), {"ok": True})[1],
    )
    before = time.time()
    node.send_message("alice", "this disappears")
    expires_at = parse_message(sent["payload"])["expires_at"]
    assert expires_at is not None
    assert before + 3600 <= expires_at <= time.time() + 3600


def test_send_message_explicit_expire_wins_over_default(tmp_path, monkeypatch):
    node = _make_node(tmp_path)
    peer = _make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    _set_peer_default(node, peer, 3600)
    sent = {}
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: (sent.setdefault("payload", args[2]), {"ok": True})[1],
    )
    node.send_message("alice", "this disappears sooner", expires_at=time.time() + 60)
    assert parse_message(sent["payload"])["expires_at"] is not None
    assert parse_message(sent["payload"])["expires_at"] <= time.time() + 61


def test_send_message_without_default_sends_no_expiry(tmp_path, monkeypatch):
    node = _make_node(tmp_path)
    peer = _make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    sent = {}
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: (sent.setdefault("payload", args[2]), {"ok": True})[1],
    )
    node.send_message("alice", "this stays")
    assert parse_message(sent["payload"])["expires_at"] is None


def test_scheduled_send_takes_no_default(tmp_path, monkeypatch):
    node = _make_node(tmp_path)
    node.reticulum = object()  # started enough to queue
    monkeypatch.setattr(node, "flush_outbox", lambda **kwargs: None)
    peer = _make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    _set_peer_default(node, peer, 3600)
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no live send"))
    )
    # A schedule inside the default window would expire before it is
    # sent, so the default must not apply to scheduled sends at all.
    response = node.send_message(
        "alice", "later", send_after=time.time() + 60
    )
    assert response["queued"] is True
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    # The envelope keeps its own 7-day TTL; the *message* expiry is what
    # the default would have set, and it must stay unset.
    assert entry.message_expires_at is None


def _group_node(tmp_path):
    node = _make_node(tmp_path)
    identity = RNS.Identity()
    node.identity = identity
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    node.destination = types.SimpleNamespace(hexhash=own_hash)
    alice = _make_peer("alice")
    bob = _make_peer("bob")
    for peer in (alice, bob):
        node.registry.upsert(
            peer.destination_hash, peer.public_key, name=peer.name, now=1.0
        )
    group = node.groups.create(
        "crew",
        [alice.hexhash, bob.hexhash, own_hash],
        created_by=alice.hexhash,
    )
    return node, group, alice, bob


def test_send_group_message_applies_group_default(tmp_path, monkeypatch):
    node, group, alice, bob = _group_node(tmp_path)
    save_disappearing_defaults(
        node.config.disappearing_path,
        DisappearingDefaults(groups={group["id"]: 300}),
    )
    legs = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        legs.append(payload)
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)
    before = time.time()
    response = node.send_group_message("crew", "this disappears for all")
    assert response["ok"] is True
    assert len(legs) == 2
    for payload in legs:
        expires_at = parse_group_message(payload)["expires_at"]
        assert expires_at is not None
        assert before + 300 <= expires_at <= time.time() + 300


def test_send_group_message_explicit_expire_wins(tmp_path, monkeypatch):
    node, group, alice, bob = _group_node(tmp_path)
    save_disappearing_defaults(
        node.config.disappearing_path,
        DisappearingDefaults(groups={group["id"]: 300}),
    )
    legs = []

    def fake_request(peer_arg, path, payload, timeout, deadline=None):
        legs.append(payload)
        return {"ok": True, "_rtt": 0.004}

    monkeypatch.setattr(node, "_request", fake_request)
    node.send_group_message("crew", "sooner", expires_at=time.time() + 60)
    assert len(legs) == 2
    for payload in legs:
        assert parse_group_message(payload)["expires_at"] <= time.time() + 61


# -- CLI ------------------------------------------------------------------

def _seed_two_peers(tmp_path):
    from tau.identity import destination_hash

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    senders = {}
    inbox = Inbox(config.inbox_path)
    for name, text, received_at in (
        ("alice", "first hello", 20.0),
        ("carol", "hi from carol", 21.0),
    ):
        sender = RNS.Identity()
        sender_hash = destination_hash(sender)
        registry = PeerRegistry(config.peers_path)
        registry.upsert(sender_hash, sender.get_public_key(), name=name, now=100.0)
        registry.save()
        inbox.append(
            {
                "from": name,
                "destination": sender_hash.hex(),
                "text": text,
                "sent_at": 10.0,
                "received_at": received_at,
                "id": f"m-{name}",
            }
        )
        senders[name] = sender_hash.hex()
    return data_dir, senders


def test_parser_disappearing_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["disappearing", "--peer", "alice", "1h"])
    assert args.command == "disappearing"
    assert args.peer == "alice"
    assert args.duration == "1h"
    assert args.off is False
    args = parser.parse_args(["disappearing", "--group", "crew", "--off"])
    assert args.group == "crew"
    assert args.duration is None
    assert args.off is True
    args = parser.parse_args(["disappearing", "--peer", "alice"])
    assert args.duration is None
    assert args.off is False


def test_disappearing_peer_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(["disappearing", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    assert "no disappearing default" in capsys.readouterr().out

    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--peer", "alice", "1h"]
    )
    assert code == 0
    assert "disappear after 1h" in capsys.readouterr().out

    code = cli.main(["disappearing", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    assert "disappear after 1h" in capsys.readouterr().out
    defaults = load_disappearing_defaults(data_dir / "disappearing.json")
    assert defaults.peer_default(senders["alice"]) == 3600.0

    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--peer", "alice", "--json"]
    )
    assert code == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["disappearing_seconds"] == 3600.0
    assert shown["group"] is False

    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--peer", "alice", "--off"]
    )
    assert code == 0
    assert "Cleared the disappearing default" in capsys.readouterr().out
    defaults = load_disappearing_defaults(data_dir / "disappearing.json")
    assert defaults.peer_default(senders["alice"]) is None


def test_disappearing_group_roundtrip(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    from tau.groups import GroupStore

    store = GroupStore(config.groups_path)
    created = store.create("crew", ["aa" * 16, "bb" * 16], created_by="aa" * 16)

    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--group", "crew", "5m"]
    )
    assert code == 0
    assert "disappear after 5m" in capsys.readouterr().out
    defaults = load_disappearing_defaults(data_dir / "disappearing.json")
    assert defaults.group_default(created["id"]) == 300.0

    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--group", "crew", "--off"]
    )
    assert code == 0
    defaults = load_disappearing_defaults(data_dir / "disappearing.json")
    assert defaults.group_default(created["id"]) is None


def test_disappearing_rejects_bad_input(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--peer", "alice", "0s"]
    )
    assert code == 1
    assert "error" in capsys.readouterr().err
    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--peer", "alice", "31d"]
    )
    assert code == 1
    code = cli.main(
        [
            "disappearing",
            "--data-dir",
            str(data_dir),
            "--peer",
            "alice",
            "1h",
            "--off",
        ]
    )
    assert code == 1
    code = cli.main(
        ["disappearing", "--data-dir", str(data_dir), "--peer", "nobody", "1h"]
    )
    assert code != 0

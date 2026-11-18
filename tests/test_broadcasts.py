"""Broadcast lists: durable store and CLI (create/list/show/send).

Covers issue #520: named sets of peers for one-to-many DMs, where each
send fans out as an ordinary DM per member.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import RNS

from tau import cli
from tau.broadcasts import (
    MAX_BROADCAST_MEMBERS,
    BroadcastStore,
)
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.mailbox import Mailbox
from tau.node import NodeError
from tau.peers import PeerRegistry

OWN_HASH = "ab" * 16
ALICE_HASH = "cd" * 16
BOB_HASH = "ef" * 16
CAROL_HASH = "12" * 16


# -- store --------------------------------------------------------------


def test_broadcast_store_create_find_and_list(tmp_path):
    store = BroadcastStore(tmp_path / "broadcasts.json")
    created = store.create("team", [BOB_HASH, ALICE_HASH])
    assert created["name"] == "team"
    assert created["members"] == sorted([ALICE_HASH, BOB_HASH])
    assert store.find("team") == created
    assert store.find("TEAM") == created
    assert [item["name"] for item in store.list_broadcasts()] == ["team"]
    assert len(store) == 1
    # Creating the same name again is refused, not idempotent: a list
    # name commits to nothing, so a second create is a mistake.
    with pytest.raises(ValueError, match="already exists"):
        store.create("Team", [CAROL_HASH])
    assert len(store) == 1


def test_broadcast_store_create_errors(tmp_path):
    store = BroadcastStore(tmp_path / "broadcasts.json")
    with pytest.raises(ValueError, match="needs a name"):
        store.create("   ", [ALICE_HASH])
    with pytest.raises(ValueError, match="1\\.\\.50"):
        store.create("empty", [])
    with pytest.raises(ValueError, match="1\\.\\.50"):
        store.create("crowd", [f"{n:032x}" for n in range(MAX_BROADCAST_MEMBERS + 1)])
    with pytest.raises(ValueError, match="1\\.\\.50"):
        store.create("dupes", [ALICE_HASH, ALICE_HASH])
    with pytest.raises(ValueError, match="unknown broadcast list"):
        store.find("nope")
    assert len(store) == 0


def test_broadcast_store_add_and_remove_members(tmp_path):
    store = BroadcastStore(tmp_path / "broadcasts.json")
    store.create("team", [ALICE_HASH])
    updated = store.add_members("team", [BOB_HASH, ALICE_HASH])
    assert updated["members"] == sorted([ALICE_HASH, BOB_HASH])
    # Adding again is a no-op, not an error.
    assert store.add_members("team", [BOB_HASH])["members"] == updated["members"]
    updated = store.remove_members("team", [BOB_HASH])
    assert updated["members"] == [ALICE_HASH]
    # Removing a non-member is a no-op.
    assert store.remove_members("team", [CAROL_HASH])["members"] == [ALICE_HASH]
    # Removing the last member is refused: delete the list instead.
    with pytest.raises(ValueError, match="delete .* instead"):
        store.remove_members("team", [ALICE_HASH])
    with pytest.raises(ValueError, match="unknown broadcast list"):
        store.add_members("nope", [BOB_HASH])
    with pytest.raises(ValueError, match="unknown broadcast list"):
        store.remove_members("nope", [BOB_HASH])


def test_broadcast_store_delete(tmp_path):
    store = BroadcastStore(tmp_path / "broadcasts.json")
    store.create("team", [ALICE_HASH])
    assert store.delete("TEAM") is True
    assert store.delete("team") is False
    assert len(store) == 0


def test_broadcast_store_persists_and_drops_corrupt_entries(tmp_path):
    path = tmp_path / "broadcasts.json"
    store = BroadcastStore(path)
    store.create("team", [ALICE_HASH, BOB_HASH])
    reloaded = BroadcastStore(path)
    assert [item["name"] for item in reloaded.list_broadcasts()] == ["team"]
    assert reloaded.find("team")["members"] == sorted([ALICE_HASH, BOB_HASH])
    # A hand-edited file with a corrupt entry drops that entry only.
    raw = json.loads(path.read_text())
    raw["broadcasts"].append({"name": "broken", "members": ["not-a-hash"]})
    raw["broadcasts"].append({"name": "empty", "members": []})
    path.write_text(json.dumps(raw))
    reloaded = BroadcastStore(path)
    assert [item["name"] for item in reloaded.list_broadcasts()] == ["team"]


# Name -> destination hash for the peers seeded by _seed_peers.  The
# hashes come from real identities: the registry only loads a peer
# whose stored public key actually derives its destination hash, so a
# made-up key under a made-up hash is dropped on load.
_PEER_MAP: dict = {}


class _FakeBroadcastNode:
    """A node that resolves a few known peers and records DM sends."""

    instances: list = []

    def __init__(self, config, name=None):
        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.registry = PeerRegistry(config.peers_path)
        self.destination_hexhash = OWN_HASH
        self.started = False
        self.calls: list = []
        _FakeBroadcastNode.instances.append(self)

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.started = False

    def resolve_peer(self, target, timeout=10.0):
        if target == "self":
            return SimpleNamespace(hexhash=OWN_HASH, display_name="me")
        hashed = _PEER_MAP.get(str(target).lower())
        if hashed is None:
            raise NodeError(f"unknown peer: {target!r}")
        return SimpleNamespace(hexhash=hashed, display_name=str(target).lower())

    def send_message(self, target, text, **kwargs):
        self.calls.append((target, text))
        return {"ok": True, "echo": text, "_rtt": 0.05}


def _seed_peers(tmp_path):
    """Seed alice/bob/carol as real identities and remember their hashes."""
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="tester")
    registry = PeerRegistry(config.peers_path)
    for name in ("alice", "bob", "carol"):
        identity = RNS.Identity()
        hashed = destination_hash(identity).hex()
        _PEER_MAP[name] = hashed
        registry.upsert(
            bytes.fromhex(hashed), identity.get_public_key(), name=name
        )
    registry.save()
    return config


def _run_broadcast(tmp_path, monkeypatch, argv):
    monkeypatch.setattr(cli, "TauNode", _FakeBroadcastNode)
    _FakeBroadcastNode.instances.clear()
    return cli.main(["broadcast", *argv, "--data-dir", str(tmp_path / "node")])


def test_broadcast_create_list_and_show(tmp_path, capsys, monkeypatch):
    _seed_peers(tmp_path)
    code = _run_broadcast(
        tmp_path, monkeypatch, ["create", "team", "--peer", "alice", "--peer", "bob"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "created broadcast list team" in out
    assert "alice" in out and "bob" in out
    code = _run_broadcast(tmp_path, monkeypatch, ["list"])
    assert code == 0
    out = capsys.readouterr().out
    # Members print in stored (hash-sorted) order, which varies with the
    # seeded identities, so assert the set, not the order.
    assert "team (2 members)" in out
    assert "alice" in out and "bob" in out
    code = _run_broadcast(tmp_path, monkeypatch, ["show", "team"])
    assert code == 0
    out = capsys.readouterr().out
    assert "team (2 members)" in out
    assert "alice" in out and "bob" in out


def test_broadcast_create_refuses_self_and_duplicates(tmp_path, monkeypatch):
    _seed_peers(tmp_path)
    code = _run_broadcast(
        tmp_path, monkeypatch, ["create", "team", "--peer", "self"]
    )
    assert code != 0
    code = _run_broadcast(
        tmp_path, monkeypatch, ["create", "team", "--peer", "alice"]
    )
    assert code == 0
    code = _run_broadcast(
        tmp_path, monkeypatch, ["create", "TEAM", "--peer", "bob"]
    )
    assert code != 0
    code = _run_broadcast(tmp_path, monkeypatch, ["create", "nope", "--peer", "mallory"])
    assert code != 0


def test_broadcast_add_and_remove_member(tmp_path, capsys, monkeypatch):
    _seed_peers(tmp_path)
    assert (
        _run_broadcast(tmp_path, monkeypatch, ["create", "team", "--peer", "alice"])
        == 0
    )
    code = _run_broadcast(
        tmp_path, monkeypatch, ["add-member", "team", "--peer", "carol"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "team (2 members)" in out
    assert "alice" in out and "carol" in out
    code = _run_broadcast(
        tmp_path, monkeypatch, ["add-member", "team", "--peer", "alice"]
    )
    assert code != 0
    code = _run_broadcast(
        tmp_path, monkeypatch, ["remove-member", "team", "--peer", "alice"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "team (1 members) — carol" in out
    code = _run_broadcast(
        tmp_path, monkeypatch, ["remove-member", "team", "--peer", "carol"]
    )
    assert code != 0


def test_broadcast_delete(tmp_path, capsys, monkeypatch):
    _seed_peers(tmp_path)
    assert (
        _run_broadcast(tmp_path, monkeypatch, ["create", "team", "--peer", "alice"])
        == 0
    )
    code = _run_broadcast(tmp_path, monkeypatch, ["delete", "team"])
    assert code == 0
    assert "deleted broadcast list team" in capsys.readouterr().out
    code = _run_broadcast(tmp_path, monkeypatch, ["delete", "team"])
    assert code != 0
    code = _run_broadcast(tmp_path, monkeypatch, ["list"])
    assert code == 0
    assert "No broadcast lists yet" in capsys.readouterr().out


def test_broadcast_send_fans_out_as_individual_dms(tmp_path, capsys, monkeypatch):
    _seed_peers(tmp_path)
    assert (
        _run_broadcast(
            tmp_path,
            monkeypatch,
            ["create", "team", "--peer", "alice", "--peer", "bob"],
        )
        == 0
    )
    code = _run_broadcast(tmp_path, monkeypatch, ["send", "team", "meet at noon"])
    assert code == 0
    node = _FakeBroadcastNode.instances[-1]
    # Members are stored sorted by hash, so the fan-out follows that order.
    assert node.calls == [
        (hashed, "meet at noon")
        for hashed in sorted([_PEER_MAP["alice"], _PEER_MAP["bob"]])
    ]
    out = capsys.readouterr().out
    assert f"delivered to {_PEER_MAP['alice']}" in out
    assert f"delivered to {_PEER_MAP['bob']}" in out
    assert node.started is False


def test_broadcast_send_unknown_list(tmp_path, monkeypatch):
    code = _run_broadcast(tmp_path, monkeypatch, ["send", "nope", "hi"])
    assert code != 0

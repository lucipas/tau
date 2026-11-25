"""Broadcast lists: durable store and CLI (create/list/show/send).

Covers issue #520: named sets of peers for one-to-many DMs, where each
send fans out as an ordinary DM per member.
"""

from __future__ import annotations

import hashlib
import json
import time
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
from tau.inbox import Inbox
from tau.mailbox import Mailbox
from tau.node import NodeError, TauNode
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


def test_broadcast_store_rename(tmp_path):
    store = BroadcastStore(tmp_path / "broadcasts.json")
    created = store.create("team", [ALICE_HASH, BOB_HASH])
    store.create("other", [CAROL_HASH])
    renamed = store.rename("TEAM", "squad")
    assert renamed["name"] == "squad"
    assert renamed["members"] == created["members"]
    # The rename is in place: creation time (and list order) survive.
    assert renamed["created_at"] == created["created_at"]
    assert [item["name"] for item in store.list_broadcasts()] == [
        "squad",
        "other",
    ]
    assert store.find("squad") == renamed
    with pytest.raises(ValueError, match="unknown broadcast list"):
        store.find("team")
    # Renaming onto another list's name is refused, case-insensitively.
    with pytest.raises(ValueError, match="already exists"):
        store.rename("squad", "OTHER")
    with pytest.raises(ValueError, match="unknown broadcast list"):
        store.rename("nope", "squad")
    with pytest.raises(ValueError, match="needs a name"):
        store.rename("squad", "   ")
    # A case-only rename is allowed: same list, new casing.
    assert store.rename("squad", "Squad")["name"] == "Squad"
    assert [item["name"] for item in store.list_broadcasts()] == [
        "Squad",
        "other",
    ]
    # The rename persists across a reload.
    assert BroadcastStore(tmp_path / "broadcasts.json").find("Squad") == (
        store.find("Squad")
    )


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
    fail_for: set = set()

    def __init__(self, config, name=None):
        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.registry = PeerRegistry(config.peers_path)
        self.broadcasts = BroadcastStore(config.broadcasts_path)
        self.destination_hexhash = OWN_HASH
        self.started = False
        self.calls: list = []
        self.send_kwargs: list = []
        self.file_calls: list = []
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
        self.send_kwargs.append(kwargs)
        return {"ok": True, "echo": text, "_rtt": 0.05}

    def send_broadcast_file(self, broadcast, path, **kwargs):
        """Fan a file out like the real node, recording each leg."""
        if isinstance(broadcast, str):
            broadcast = self.broadcasts.find(broadcast)
        data = Path(path).read_bytes()
        name = Path(path).name
        digest = hashlib.sha256(data).hexdigest()
        results = []
        for member in broadcast["members"]:
            if member == OWN_HASH:
                continue
            peer = self.registry.get(bytes.fromhex(member))
            peer_name = peer.display_name if peer is not None else ""
            self.file_calls.append((member, name, data))
            if member in _FakeBroadcastNode.fail_for:
                results.append(
                    {"to": member, "to_name": peer_name, "ok": False,
                     "error": "unreachable"}
                )
                continue
            results.append(
                {
                    "to": member,
                    "to_name": peer_name,
                    "delivered": True,
                    "id": f"{member[:8]}-transfer",
                    "name": name,
                    "size": len(data),
                    "sha256": digest,
                }
            )
        return {
            "ok": all(r.get("delivered") for r in results),
            "broadcast": broadcast["name"],
            "results": results,
        }


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


def test_broadcast_rename(tmp_path, capsys, monkeypatch):
    _seed_peers(tmp_path)
    assert (
        _run_broadcast(tmp_path, monkeypatch, ["create", "team", "--peer", "alice"])
        == 0
    )
    assert (
        _run_broadcast(tmp_path, monkeypatch, ["create", "other", "--peer", "bob"])
        == 0
    )
    code = _run_broadcast(tmp_path, monkeypatch, ["rename", "team", "squad"])
    assert code == 0
    assert "renamed broadcast list 'team' to 'squad'" in capsys.readouterr().out
    # The old name is gone; the new name resolves with the same member.
    code = _run_broadcast(tmp_path, monkeypatch, ["show", "squad"])
    assert code == 0
    out = capsys.readouterr().out
    assert "squad (1 members)" in out
    assert "alice" in out
    code = _run_broadcast(tmp_path, monkeypatch, ["show", "team"])
    assert code != 0
    # Renaming onto another list's name, or an unknown list, fails.
    code = _run_broadcast(tmp_path, monkeypatch, ["rename", "squad", "other"])
    assert code != 0
    code = _run_broadcast(tmp_path, monkeypatch, ["rename", "nope", "squad"])
    assert code != 0
    # A case-only rename is a rename, not an error.
    code = _run_broadcast(tmp_path, monkeypatch, ["rename", "squad", "Squad"])
    assert code == 0
    assert "renamed broadcast list 'squad' to 'Squad'" in capsys.readouterr().out
    # JSON mode reports the renamed list and the previous name.
    code = _run_broadcast(
        tmp_path, monkeypatch, ["rename", "Squad", "crew", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["broadcast"]["name"] == "crew"
    assert payload["previous_name"] == "Squad"
    assert payload["broadcast"]["members"][0]["name"] == "alice"


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


# -- broadcast poll ---------------------------------------------------------
# Issue #663: `tau broadcast poll` posts a poll to every member of the
# list, one private DM poll each — the poll surface broadcast lists
# were missing while DM, group, channel, and Tan all had one.


def test_broadcast_poll_fans_out_as_individual_dm_polls(
    tmp_path, capsys, monkeypatch
):
    _create_team(tmp_path, monkeypatch)
    code = _run_broadcast(
        tmp_path, monkeypatch, ["poll", "team", "lunch?", "noon", "1pm"]
    )
    assert code == 0
    node = _FakeBroadcastNode.instances[-1]
    # Members are stored sorted by hash, so the fan-out follows that order.
    assert node.calls == [
        (hashed, "lunch?")
        for hashed in sorted([_PEER_MAP["alice"], _PEER_MAP["bob"]])
    ]
    # Every leg carries the same question as a DM poll with the options.
    assert len(node.send_kwargs) == 2
    assert all(
        kwargs.get("poll_options") == ["noon", "1pm"]
        for kwargs in node.send_kwargs
    )
    out = capsys.readouterr().out
    assert f"delivered to {_PEER_MAP['alice']}" in out
    assert f"delivered to {_PEER_MAP['bob']}" in out
    assert node.started is False


def test_broadcast_poll_unknown_list(tmp_path, monkeypatch):
    code = _run_broadcast(
        tmp_path, monkeypatch, ["poll", "nope", "lunch?", "noon", "1pm"]
    )
    assert code != 0


def test_broadcast_poll_rejects_bad_options(tmp_path, capsys, monkeypatch):
    _create_team(tmp_path, monkeypatch)
    # One option is not a poll.
    code = _run_broadcast(tmp_path, monkeypatch, ["poll", "team", "lunch?", "noon"])
    assert code != 0
    assert "poll" in capsys.readouterr().err.lower()
    # Duplicate options are not distinct.
    code = _run_broadcast(
        tmp_path, monkeypatch, ["poll", "team", "lunch?", "noon", "noon"]
    )
    assert code != 0
    # Nine options exceed the 2-8 rule.
    code = _run_broadcast(
        tmp_path,
        monkeypatch,
        ["poll", "team", "lunch?", *[f"opt{i}" for i in range(9)]],
    )
    assert code != 0
    # Nothing was sent for any of the rejected polls: validation fails
    # before a node is even started.
    assert _FakeBroadcastNode.instances == []


def test_broadcast_poll_delay_schedules(tmp_path, monkeypatch):
    _create_team(tmp_path, monkeypatch)
    before = time.time()
    code = _run_broadcast(
        tmp_path,
        monkeypatch,
        ["poll", "team", "lunch?", "noon", "1pm", "--delay", "5m"],
    )
    assert code == 0
    node = _FakeBroadcastNode.instances[-1]
    assert len(node.send_kwargs) == 2
    # Both legs carry the same scheduled time, about five minutes out.
    assert all(
        kwargs.get("send_after") is not None
        and before + 290 <= kwargs["send_after"] <= before + 310
        for kwargs in node.send_kwargs
    )


def test_broadcast_poll_delay_and_no_queue_conflict(tmp_path, monkeypatch):
    _create_team(tmp_path, monkeypatch)
    code = _run_broadcast(
        tmp_path,
        monkeypatch,
        ["poll", "team", "lunch?", "noon", "1pm", "--delay", "5m", "--no-queue"],
    )
    assert code != 0
    # The conflict is rejected before a node is started: nothing sent.
    assert _FakeBroadcastNode.instances == []


# -- forward to a broadcast list ----------------------------------------
# Issue #630: `tau forward --broadcast LIST` forwards a stored DM to
# every member of the list, one private DM each.


def _seed_forwardable_message(tmp_path):
    """One received DM from alice, forwardable by its id prefix."""
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="tester")
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
    return config


def test_forward_to_broadcast_fans_out_attributed_text(
    tmp_path, capsys, monkeypatch
):
    _seed_peers(tmp_path)
    _seed_forwardable_message(tmp_path)
    assert (
        _run_broadcast(
            tmp_path,
            monkeypatch,
            ["create", "team", "--peer", "alice", "--peer", "bob"],
        )
        == 0
    )
    monkeypatch.setattr(cli, "TauNode", _FakeBroadcastNode)
    _FakeBroadcastNode.instances.clear()
    code = cli.main(
        ["forward", "--data-dir", str(tmp_path / "node"), "--broadcast", "team", "aaaa"]
    )
    assert code == 0
    node = _FakeBroadcastNode.instances[-1]
    # Members are stored sorted by hash, so the fan-out follows that order.
    assert node.calls == [
        (hashed, "Forwarded from alice:\noriginal thought")
        for hashed in sorted([_PEER_MAP["alice"], _PEER_MAP["bob"]])
    ]
    out = capsys.readouterr().out
    assert f"delivered to {_PEER_MAP['alice']}" in out
    assert f"delivered to {_PEER_MAP['bob']}" in out
    assert node.started is False


def test_forward_to_broadcast_unknown_list(tmp_path, capsys, monkeypatch):
    _seed_peers(tmp_path)
    _seed_forwardable_message(tmp_path)
    monkeypatch.setattr(cli, "TauNode", _FakeBroadcastNode)
    _FakeBroadcastNode.instances.clear()
    code = cli.main(
        ["forward", "--data-dir", str(tmp_path / "node"), "--broadcast", "nope", "aaaa"]
    )
    assert code == 1
    assert "unknown broadcast list" in capsys.readouterr().err

# -- broadcast file fan-out (CLI) -------------------------------------------


def _create_team(tmp_path, monkeypatch):
    _seed_peers(tmp_path)
    assert (
        _run_broadcast(
            tmp_path,
            monkeypatch,
            ["create", "team", "--peer", "alice", "--peer", "bob"],
        )
        == 0
    )


def test_broadcast_send_file_fans_out_as_individual_transfers(
    tmp_path, capsys, monkeypatch
):
    _create_team(tmp_path, monkeypatch)
    shared = tmp_path / "photo.png"
    shared.write_bytes(b"fake png bytes")
    code = _run_broadcast(
        tmp_path, monkeypatch, ["send-file", "team", str(shared)]
    )
    assert code == 0
    node = _FakeBroadcastNode.instances[-1]
    # Members are stored sorted by hash, so the fan-out follows that order.
    assert [call[0] for call in node.file_calls] == sorted(
        [_PEER_MAP["alice"], _PEER_MAP["bob"]]
    )
    # Every leg carries the same bytes, read once up front.
    assert all(call[1] == "photo.png" for call in node.file_calls)
    assert all(call[2] == b"fake png bytes" for call in node.file_calls)
    out = capsys.readouterr().out
    assert "delivered photo.png (14 bytes) to alice" in out
    assert "delivered photo.png (14 bytes) to bob" in out
    assert node.started is False


def test_broadcast_send_file_unknown_list(tmp_path, monkeypatch):
    code = _run_broadcast(tmp_path, monkeypatch, ["send-file", "nope", "x.png"])
    assert code != 0


def test_broadcast_send_file_failed_leg_is_reported(
    tmp_path, capsys, monkeypatch
):
    _create_team(tmp_path, monkeypatch)
    monkeypatch.setattr(
        _FakeBroadcastNode, "fail_for", {_PEER_MAP["alice"]}
    )
    shared = tmp_path / "notes.txt"
    shared.write_bytes(b"hello")
    code = _run_broadcast(
        tmp_path, monkeypatch, ["send-file", "team", str(shared)]
    )
    assert code != 0
    out = capsys.readouterr().out
    assert "failed for alice: unreachable" in out
    assert "delivered notes.txt (5 bytes) to bob" in out


# -- broadcast file fan-out (node) ------------------------------------------


def _make_node(tmp_path, name="alice"):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
    )
    node.destination = SimpleNamespace(
        hexhash=OWN_HASH, hash=bytes.fromhex(OWN_HASH)
    )
    return node


def _stub_broadcast_send_file(node, *, fail_for=()):
    """Replace ``_send_file_bytes`` with a recorder; *fail_for* members raise."""
    calls = []

    def fake_send_file_bytes(
        target,
        data,
        name,
        digest,
        *,
        timeout=300.0,
        discover_timeout=None,
        deadline=None,
        forwarded_from=None,
        group=None,
        voice=False,
    ):
        calls.append(
            (target, name, timeout, discover_timeout, data, deadline, forwarded_from)
        )
        member = target if isinstance(target, str) else target.hexhash
        if member in fail_for:
            raise NodeError("unreachable")
        return {
            "ok": True,
            "id": f"{member[:8]}-transfer",
            "name": name,
            "size": len(data),
            "sha256": digest,
            "to": member,
        }

    node._send_file_bytes = fake_send_file_bytes  # type: ignore[method-assign]
    return calls


def test_send_broadcast_file_fans_out_to_every_member(tmp_path):
    node = _make_node(tmp_path)
    node.reticulum = object()
    broadcast = node.broadcasts.create(
        "team", [OWN_HASH, ALICE_HASH, BOB_HASH]
    )
    shared = tmp_path / "photo.png"
    shared.write_bytes(b"fake png bytes")
    calls = _stub_broadcast_send_file(node)
    result = node.send_broadcast_file(broadcast, shared)
    assert result["ok"] is True
    assert result["broadcast"] == "team"
    by_to = {entry["to"]: entry for entry in result["results"]}
    assert set(by_to) == {ALICE_HASH, BOB_HASH}
    # The sender is skipped, and each leg is its own transfer.
    assert by_to[ALICE_HASH]["delivered"] is True
    assert by_to[ALICE_HASH]["id"] == f"{ALICE_HASH[:8]}-transfer"
    assert by_to[ALICE_HASH]["name"] == "photo.png"
    assert by_to[ALICE_HASH]["size"] == len(b"fake png bytes")
    assert by_to[BOB_HASH]["delivered"] is True
    assert by_to[BOB_HASH]["id"] != by_to[ALICE_HASH]["id"]
    assert len(calls) == 2
    assert {call[0] for call in calls} == {ALICE_HASH, BOB_HASH}
    # Every leg carries the same bytes, read once up front.
    assert all(call[1] == "photo.png" for call in calls)
    assert all(call[4] == b"fake png bytes" for call in calls)
    # Each leg carries its own deadline, so one slow member cannot
    # consume another member's window.
    assert all(call[5] is not None for call in calls)
    # Nothing is queued: files are direct links only in v1.
    assert node.mailbox.outbox_list() == []


def test_send_broadcast_file_keeps_going_after_a_failed_leg(tmp_path):
    node = _make_node(tmp_path)
    node.reticulum = object()
    broadcast = node.broadcasts.create(
        "team", [OWN_HASH, ALICE_HASH, BOB_HASH]
    )
    shared = tmp_path / "notes.txt"
    shared.write_bytes(b"hello")
    calls = _stub_broadcast_send_file(node, fail_for={ALICE_HASH})
    result = node.send_broadcast_file(broadcast, shared)
    assert result["ok"] is False
    by_to = {entry["to"]: entry for entry in result["results"]}
    assert by_to[ALICE_HASH].get("delivered") is None
    assert by_to[ALICE_HASH]["error"] == "unreachable"
    assert by_to[BOB_HASH]["delivered"] is True
    assert len(calls) == 2
    assert node.mailbox.outbox_list() == []


def test_send_broadcast_file_rejects_a_bad_file_before_any_leg(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("tau.node.MAX_FILE_BYTES", 10)
    node = _make_node(tmp_path)
    node.reticulum = object()
    broadcast = node.broadcasts.create(
        "team", [OWN_HASH, ALICE_HASH, BOB_HASH]
    )
    big = tmp_path / "big.bin"
    big.write_bytes(b"x" * 11)
    calls = _stub_broadcast_send_file(node)
    with pytest.raises(NodeError, match="outside the"):
        node.send_broadcast_file(broadcast, big)
    assert calls == []


def test_send_broadcast_file_accepts_a_list_name_string(tmp_path):
    node = _make_node(tmp_path)
    node.reticulum = object()
    node.broadcasts.create("team", [OWN_HASH, ALICE_HASH])
    shared = tmp_path / "notes.txt"
    shared.write_bytes(b"hello")
    calls = _stub_broadcast_send_file(node)
    result = node.send_broadcast_file("team", shared)
    assert result["ok"] is True
    assert [entry["to"] for entry in result["results"]] == [ALICE_HASH]
    assert len(calls) == 1


def test_send_broadcast_file_unknown_list_raises(tmp_path):
    node = _make_node(tmp_path)
    node.reticulum = object()
    shared = tmp_path / "notes.txt"
    shared.write_bytes(b"hello")
    calls = _stub_broadcast_send_file(node)
    with pytest.raises(ValueError, match="unknown broadcast list"):
        node.send_broadcast_file("nope", shared)
    assert calls == []


def test_send_broadcast_file_self_only_list_sends_nowhere(tmp_path):
    node = _make_node(tmp_path)
    node.reticulum = object()
    shared = tmp_path / "notes.txt"
    shared.write_bytes(b"hello")
    calls = _stub_broadcast_send_file(node)
    # A list needs at least one member besides the sender to be
    # created through the store, so a self-only list only reaches the
    # fan-out as a bare record (a corrupt or hand-edited
    # broadcasts.json). The fan-out must still be a no-op, not an
    # error — and must never send to this node itself.
    broadcast = {"name": "solo", "members": [OWN_HASH]}
    result = node.send_broadcast_file(broadcast, shared)
    assert result["ok"] is True
    assert result["results"] == []
    assert calls == []


def test_send_broadcast_file_timeout_zero_attempts_no_leg(tmp_path):
    node = _make_node(tmp_path)
    node.reticulum = object()
    broadcast = node.broadcasts.create(
        "team", [OWN_HASH, ALICE_HASH, BOB_HASH]
    )
    shared = tmp_path / "notes.txt"
    shared.write_bytes(b"hello")
    calls = _stub_broadcast_send_file(node)
    result = node.send_broadcast_file(broadcast, shared, timeout=0.0)
    assert result["ok"] is False
    assert all(
        entry["error"] == "not attempted: broadcast send time limit reached"
        for entry in result["results"]
    )
    assert calls == []

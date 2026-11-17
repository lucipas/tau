"""Tests for message drafts: the local store and the `tau draft` CLI."""

from __future__ import annotations

import io
import json
import sys

import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.drafts import MAX_DRAFTS, DraftStore
from tau.groups import GroupStore
from tau.identity import destination_hash
from tau.mailbox import Mailbox
from tau.node import NodeError
from tau.peers import PeerRegistry
from tau.protocol import MAX_MESSAGE_CHARS

ALICE_HASH = "aa" * 16
BOB_HASH = "bb" * 16
GROUP_ID = "cc" * 16


def _seed_peer(tmp_path, name="alice"):
    """A data dir with one known peer; returns (data_dir, peer hash)."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    identity = RNS.Identity()
    peer_hash = destination_hash(identity).hex()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(identity), identity.get_public_key(), name=name, now=100.0
    )
    registry.save()
    return data_dir, peer_hash


def _seed_group(tmp_path, name="crew"):
    """A data dir with one known group; returns (data_dir, group id)."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    store = GroupStore(config.groups_path)
    group = store.create(name, [ALICE_HASH, BOB_HASH], created_by=ALICE_HASH)
    return data_dir, str(group["id"])


def _draft_main(data_dir, *argv):
    return cli.main(["draft", "--data-dir", str(data_dir), *argv])


# --- Store ---


def test_store_save_get_overwrite_and_delete(tmp_path):
    store = DraftStore(tmp_path / "drafts.json")
    assert store.get("peer", ALICE_HASH) is None

    saved = store.save("peer", ALICE_HASH, "hello alice")
    assert saved["kind"] == "peer"
    assert saved["target"] == ALICE_HASH
    assert saved["text"] == "hello alice"

    # Saving again overwrites: still one draft per conversation.
    store.save("peer", ALICE_HASH, "second try")
    assert len(store) == 1
    assert store.get("peer", ALICE_HASH)["text"] == "second try"

    # A group draft is a separate conversation, even for the same id.
    store.save("group", GROUP_ID, "hello crew")
    assert len(store) == 2
    assert [d["kind"] for d in store.list_drafts()] == ["peer", "group"]

    assert store.delete("peer", ALICE_HASH) is True
    assert store.delete("peer", ALICE_HASH) is False
    assert store.get("peer", ALICE_HASH) is None
    assert len(store) == 1


def test_store_rejects_bad_text_kind_and_target(tmp_path):
    store = DraftStore(tmp_path / "drafts.json")
    with pytest.raises(ValueError):
        store.save("peer", ALICE_HASH, "   ")
    with pytest.raises(ValueError):
        store.save("peer", ALICE_HASH, "x" * (MAX_MESSAGE_CHARS + 1))
    with pytest.raises(ValueError):
        store.save("channel", ALICE_HASH, "hello")
    with pytest.raises(ValueError):
        store.save("peer", "not-a-hash", "hello")
    assert len(store) == 0
    assert store.get("peer", "not-a-hash") is None
    assert store.delete("peer", "not-a-hash") is False


def test_store_cap_counts_conversations_not_saves(tmp_path):
    store = DraftStore(tmp_path / "drafts.json")
    for index in range(MAX_DRAFTS):
        store.save("peer", f"{index:032x}", f"draft {index}")
    assert len(store) == MAX_DRAFTS
    with pytest.raises(ValueError, match="draft limit reached"):
        store.save("peer", "ff" * 16, "one too many")
    # Overwriting an existing draft does not hit the cap.
    store.save("peer", f"{0:032x}", "still fine")
    assert len(store) == MAX_DRAFTS


def test_store_persists_and_drops_corrupt_entries(tmp_path):
    path = tmp_path / "drafts.json"
    store = DraftStore(path)
    store.save("peer", ALICE_HASH, "keep me")
    reloaded = DraftStore(path)
    assert reloaded.get("peer", ALICE_HASH)["text"] == "keep me"

    raw = json.loads(path.read_text())
    raw["drafts"].append({"kind": "peer", "target": "junk", "text": "bad"})
    raw["drafts"].append({"kind": "peer", "target": BOB_HASH, "text": "   "})
    path.write_text(json.dumps(raw))
    reloaded = DraftStore(path)
    assert len(reloaded) == 1
    assert reloaded.get("peer", ALICE_HASH)["text"] == "keep me"


def test_store_memory_only_without_path():
    store = DraftStore()
    store.save("peer", ALICE_HASH, "ephemeral")
    assert store.get("peer", ALICE_HASH)["text"] == "ephemeral"
    assert DraftStore().get("peer", ALICE_HASH) is None


# --- CLI: save / list / show / delete ---


def test_cli_save_list_show_delete(tmp_path, capsys):
    data_dir, peer_hash = _seed_peer(tmp_path)

    assert _draft_main(data_dir, "save", "--to", "alice", "meet at noon?") == 0
    out = capsys.readouterr().out
    assert "draft saved for alice" in out
    assert "meet at noon?" in out

    assert _draft_main(data_dir, "list") == 0
    out = capsys.readouterr().out
    assert "alice" in out and "meet at noon?" in out

    assert _draft_main(data_dir, "show", "--to", peer_hash) == 0
    assert capsys.readouterr().out.strip() == "meet at noon?"

    assert _draft_main(data_dir, "save", "--to", "alice", "changed my mind") == 0
    capsys.readouterr()
    assert _draft_main(data_dir, "show", "--to", "alice") == 0
    assert capsys.readouterr().out.strip() == "changed my mind"

    assert _draft_main(data_dir, "delete", "--to", "alice") == 0
    assert "deleted draft for alice" in capsys.readouterr().out
    assert _draft_main(data_dir, "list") == 0
    assert "No drafts yet" in capsys.readouterr().out


def test_cli_save_reads_stdin_and_json_round_trip(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_peer(tmp_path)
    monkeypatch.setattr(sys, "stdin", io.StringIO("piped draft\n"))
    assert _draft_main(data_dir, "save", "--to", "alice") == 0
    capsys.readouterr()

    assert _draft_main(data_dir, "list", "--json") == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["drafts"][0]["target_name"] == "alice"
    assert payload["drafts"][0]["text"] == "piped draft"


def test_cli_save_group_draft(tmp_path, capsys):
    data_dir, group_id = _seed_group(tmp_path)

    assert _draft_main(data_dir, "save", "--group", "crew", "agenda?") == 0
    assert "draft saved for crew" in capsys.readouterr().out
    assert _draft_main(data_dir, "list") == 0
    out = capsys.readouterr().out
    assert "group crew" in out and "agenda?" in out
    assert _draft_main(data_dir, "show", "--group", group_id) == 0
    assert capsys.readouterr().out.strip() == "agenda?"


def test_cli_unknown_peer_group_and_missing_draft(tmp_path, capsys):
    data_dir, _ = _seed_peer(tmp_path)

    assert _draft_main(data_dir, "save", "--to", "nobody", "hi") != 0
    assert "unknown peer" in capsys.readouterr().err
    assert _draft_main(data_dir, "save", "--group", "nope", "hi") != 0
    assert "unknown group" in capsys.readouterr().err

    assert _draft_main(data_dir, "show", "--to", "alice") != 0
    assert "no draft for alice" in capsys.readouterr().err
    assert _draft_main(data_dir, "delete", "--to", "alice") != 0
    assert "no draft for alice" in capsys.readouterr().err
    assert _draft_main(data_dir, "send", "--to", "alice") != 0
    assert "no draft for alice" in capsys.readouterr().err


# --- CLI: send ---


class _FakeDraftSendNode:
    """Stub TauNode for `tau draft send`: records the send, delivers it."""

    instances: list = []
    fail = False

    def __init__(self, config, **kwargs):
        self.config = config
        self.mailbox = Mailbox(config.mailbox_path)
        self.calls: list = []
        self.started = False
        _FakeDraftSendNode.instances.append(self)

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.started = False

    def send_message(self, target, text, **kwargs):
        self.calls.append((target, text))
        if _FakeDraftSendNode.fail:
            raise NodeError(f"no path to {target}")
        return {"ok": True, "target": target, "delivered": True, "echo": text}


def test_cli_send_peer_draft_deletes_on_success(tmp_path, capsys, monkeypatch):
    data_dir, peer_hash = _seed_peer(tmp_path)
    assert _draft_main(data_dir, "save", "--to", "alice", "sent soon") == 0
    capsys.readouterr()

    monkeypatch.setattr(cli, "TauNode", _FakeDraftSendNode)
    _FakeDraftSendNode.instances.clear()
    _FakeDraftSendNode.fail = False

    assert _draft_main(data_dir, "send", "--to", "alice") == 0
    out = capsys.readouterr().out
    # The send path reports the hash target, as `tau send --to <hash>`
    # does; the draft line below names the peer.
    assert f"delivered to {peer_hash}" in out
    assert "sent draft to alice" in out
    node = _FakeDraftSendNode.instances[0]
    assert node.calls == [(peer_hash, "sent soon")]

    assert _draft_main(data_dir, "list") == 0
    assert "No drafts yet" in capsys.readouterr().out


def test_cli_send_failure_keeps_draft(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_peer(tmp_path)
    assert _draft_main(data_dir, "save", "--to", "alice", "still here") == 0
    capsys.readouterr()

    monkeypatch.setattr(cli, "TauNode", _FakeDraftSendNode)
    _FakeDraftSendNode.instances.clear()
    _FakeDraftSendNode.fail = True

    assert _draft_main(data_dir, "send", "--to", "alice") != 0
    err = capsys.readouterr().err
    assert "no path to" in err

    assert _draft_main(data_dir, "list") == 0
    out = capsys.readouterr().out
    assert "alice" in out and "still here" in out


def test_cli_send_group_draft_uses_group_path(tmp_path, capsys, monkeypatch):
    data_dir, group_id = _seed_group(tmp_path)
    assert _draft_main(data_dir, "save", "--group", "crew", "group hello") == 0
    capsys.readouterr()

    calls: list = []

    def _fake_group_send(args):
        calls.append((args.group, args.message))
        return 0

    monkeypatch.setattr(cli, "cmd_group_send", _fake_group_send)
    assert _draft_main(data_dir, "send", "--group", "crew") == 0
    assert "sent draft to crew" in capsys.readouterr().out
    assert calls == [(group_id, "group hello")]

    assert _draft_main(data_dir, "list") == 0
    assert "No drafts yet" in capsys.readouterr().out


def test_cli_send_group_failure_keeps_draft(tmp_path, capsys, monkeypatch):
    data_dir, _ = _seed_group(tmp_path)
    assert _draft_main(data_dir, "save", "--group", "crew", "group hello") == 0
    capsys.readouterr()

    monkeypatch.setattr(cli, "cmd_group_send", lambda args: 1)
    assert _draft_main(data_dir, "send", "--group", "crew") != 0
    assert "draft for crew kept" in capsys.readouterr().err

    assert _draft_main(data_dir, "list") == 0
    assert "group hello" in capsys.readouterr().out

"""Muted conversations: store, CLI, and unread suppression."""

from __future__ import annotations

import json
import os
import stat

import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.groups import GroupStore
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.muted import (
    MutedConversations,
    MutedError,
    load_muted,
    muted_groups_for,
    muted_peers_for,
    save_muted,
)
from tau.peers import PeerRegistry

ALICE = "aa" * 16
BOB = "bb" * 16
GROUP_ID = "cc" * 16


# -- module ---------------------------------------------------------------

def test_missing_file_means_nothing_muted(tmp_path):
    muted = load_muted(tmp_path / "muted.json")
    assert muted.peers == frozenset()
    assert muted.groups == frozenset()
    assert muted.is_muted_peer(ALICE) is False
    assert muted.is_muted_group(GROUP_ID) is False
    assert muted.is_muted_peer(None) is False
    assert muted.is_muted_group(None) is False


def test_unreadable_location_raises(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(MutedError):
        load_muted(blocker / "muted.json")


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "muted.json"
    saved = save_muted(
        path,
        MutedConversations(peers=frozenset({ALICE}), groups=frozenset({GROUP_ID})),
    )
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_muted(path)
    assert loaded.peers == frozenset({ALICE})
    assert loaded.groups == frozenset({GROUP_ID})
    assert loaded.to_dict() == {"peers": [ALICE], "groups": [GROUP_ID]}


def test_malformed_documents_raise(tmp_path):
    path = tmp_path / "muted.json"
    for document in (
        {"peers": [ALICE]},
        {"groups": [GROUP_ID]},
        {"peers": "not-a-list", "groups": []},
        {"peers": [], "groups": "not-a-list"},
        {"peers": ["alice"], "groups": []},
        {"peers": [], "groups": ["not-a-group"]},
        "not-an-object",
    ):
        path.write_text(json.dumps(document), encoding="utf-8")
        with pytest.raises(MutedError):
            load_muted(path)
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(MutedError):
        load_muted(path)


def test_fail_open_helpers_return_empty_sets_on_corrupt_file(tmp_path, caplog):
    # An unread read must never break over a corrupt mutes file: the
    # conversations count as unmuted, and the warning surfaces it.
    path = tmp_path / "muted.json"
    path.write_text("{not json", encoding="utf-8")
    assert muted_peers_for(path) == frozenset()
    assert muted_groups_for(path) == frozenset()
    assert "unusable" in caplog.text


def test_with_and_without_are_immutable_and_idempotent():
    muted = MutedConversations()
    added = muted.with_peer(ALICE)
    assert added.is_muted_peer(ALICE) is True
    assert added.is_muted_peer(ALICE.upper()) is True
    assert added.with_peer(ALICE).peers == added.peers
    assert muted.is_muted_peer(ALICE) is False
    removed = added.without_peer(ALICE)
    assert removed.is_muted_peer(ALICE) is False
    assert removed.without_peer(ALICE).peers == frozenset()
    assert added.is_muted_peer(ALICE) is True

    grouped = muted.with_group(GROUP_ID)
    assert grouped.is_muted_group(GROUP_ID) is True
    assert grouped.is_muted_group(GROUP_ID.upper()) is True
    assert muted.is_muted_group(GROUP_ID) is False
    assert grouped.without_group(GROUP_ID).groups == frozenset()


def test_with_rejects_bad_values():
    with pytest.raises(MutedError):
        MutedConversations().with_peer("alice")
    with pytest.raises(MutedError):
        MutedConversations().with_group("not-a-group")


# -- CLI ------------------------------------------------------------------

def _seed_two_peers(tmp_path):
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


def test_parser_mute_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(["mute", "--peer", "alice"])
    assert args.command == "mute"
    assert args.peer == "alice"
    assert args.off is False
    args = parser.parse_args(["mute", "--group", "crew", "--off"])
    assert args.group == "crew"
    assert args.off is True
    args = parser.parse_args(["mute"])
    assert args.peer is None
    assert args.group is None


def test_mute_peer_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(["mute", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    assert "Muted conversation with alice" in capsys.readouterr().out
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_peer(senders["alice"]) is True

    code = cli.main(["mute", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    assert "already muted" in capsys.readouterr().out

    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--json"]
    )
    assert code == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["muted"] is True
    assert shown["group"] is False
    assert shown["changed"] is False

    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--off"]
    )
    assert code == 0
    assert "Unmuted conversation with alice" in capsys.readouterr().out
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_peer(senders["alice"]) is False


def test_mute_group_roundtrip(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    store = GroupStore(config.groups_path)
    created = store.create("crew", ["aa" * 16, "bb" * 16], created_by="aa" * 16)

    code = cli.main(["mute", "--data-dir", str(data_dir), "--group", "crew"])
    assert code == 0
    assert "Muted group crew" in capsys.readouterr().out
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_group(created["id"]) is True

    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--group", "crew", "--off"]
    )
    assert code == 0
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_group(created["id"]) is False


def test_mute_list_shows_muted_conversations(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)

    code = cli.main(["mute", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No muted conversations." in capsys.readouterr().out

    cli.main(["mute", "--data-dir", str(data_dir), "--peer", "alice"])
    capsys.readouterr()
    code = cli.main(["mute", "--data-dir", str(data_dir)])
    assert code == 0
    assert "alice" in capsys.readouterr().out


def test_mute_rejects_bad_input(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    code = cli.main(["mute", "--data-dir", str(data_dir), "--off"])
    assert code == 1
    assert "error" in capsys.readouterr().err
    code = cli.main(["mute", "--data-dir", str(data_dir), "--peer", "nobody"])
    assert code != 0


# -- unread suppression ---------------------------------------------------

def test_muted_conversations_never_count_unread(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    assert "Total: 2 unread" in capsys.readouterr().out

    code = cli.main(["mute", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    capsys.readouterr()

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    out = capsys.readouterr().out
    assert "Total: 1 unread" in out
    assert "carol" in out
    assert "alice" not in out

    # The listing clears the unread marker for the muted peer's
    # message, while the other peer's marker stays.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    shown = json.loads(capsys.readouterr().out)
    by_id = {entry["id"]: entry for entry in shown["messages"]}
    assert by_id["m-alice"]["unread"] is False
    assert by_id["m-carol"]["unread"] is True


def test_unmuting_restores_the_count(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    cli.main(["mute", "--data-dir", str(data_dir), "--peer", "alice"])
    capsys.readouterr()

    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--off"]
    )
    assert code == 0
    capsys.readouterr()

    # Messages that arrived while muted count again: the read cursor
    # never moved, so nothing was lost.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    assert "Total: 2 unread" in capsys.readouterr().out

"""Muted conversations: store, CLI, and unread suppression."""

from __future__ import annotations

import json
import os
import stat
import time

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


def test_timed_peer_mute_expires():
    muted = MutedConversations().with_peer(ALICE, until=1000.0)
    assert muted.is_muted_peer(ALICE, now=999.0) is True
    assert muted.muted_until_peer(ALICE, now=999.0) == 1000.0
    # The mute lifts at the expiry second itself.
    assert muted.is_muted_peer(ALICE, now=1000.0) is False
    assert muted.muted_until_peer(ALICE, now=1000.0) is None
    # Re-muting with no duration leaves the timed mute alone — a
    # plain re-mute is a no-op, not a silent promotion to indefinite.
    kept = muted.with_peer(ALICE)
    assert kept.is_muted_peer(ALICE, now=10_000.0) is False
    assert kept.muted_until_peer(ALICE, now=999.0) == 1000.0
    # A fresh mute with no duration is indefinite.
    indefinite = MutedConversations().with_peer(ALICE)
    assert indefinite.is_muted_peer(ALICE, now=10_000.0) is True
    assert indefinite.muted_until_peer(ALICE, now=10_000.0) is None
    # And a timed re-mute replaces the expiry rather than adding one.
    retimed = indefinite.with_peer(ALICE, until=2000.0)
    assert retimed.muted_until_peer(ALICE, now=1500.0) == 2000.0
    # Unmuting drops the expiry with the key.
    assert retimed.without_peer(ALICE).peer_until == {}


def test_timed_group_mute_lineage():
    muted = MutedConversations().with_group_lineage([GROUP_ID], until=500.0)
    assert muted.is_muted_group_lineage([GROUP_ID], now=499.0) is True
    assert muted.muted_until_group_lineage([GROUP_ID], now=499.0) == 500.0
    assert muted.is_muted_group_lineage([GROUP_ID], now=500.0) is False
    assert muted.muted_until_group_lineage([GROUP_ID], now=500.0) is None
    assert muted.without_group_lineage([GROUP_ID]).group_until == {}
    # An indefinite mute anywhere in the lineage means no expiry.
    mixed = (
        MutedConversations()
        .with_group_lineage([GROUP_ID], until=500.0)
        .with_group("dd" * 16)
    )
    assert mixed.muted_until_group_lineage([GROUP_ID, "dd" * 16]) is None


def test_until_roundtrip_and_expired_entries_drop_on_load(tmp_path):
    path = tmp_path / "muted.json"
    save_muted(
        path,
        MutedConversations().with_peer(ALICE, until=time.time() + 3600),
    )
    loaded = load_muted(path)
    assert loaded.is_muted_peer(ALICE) is True
    assert loaded.muted_until_peer(ALICE) is not None
    assert loaded.to_dict()["peer_until"][ALICE] == pytest.approx(
        time.time() + 3600, abs=5
    )

    # An entry already past reads as unmuted, and the load sheds it —
    # a stale expiry never resurrects a mute.
    path.write_text(
        json.dumps(
            {
                "peers": [ALICE],
                "groups": [],
                "peer_until": {ALICE: 1.0},
            }
        ),
        encoding="utf-8",
    )
    loaded = load_muted(path)
    assert loaded.is_muted_peer(ALICE) is False
    assert loaded.peer_until == {}
    assert "peer_until" not in loaded.to_dict()


def test_malformed_until_maps_raise(tmp_path):
    path = tmp_path / "muted.json"
    for document in (
        {"peers": [ALICE], "groups": [], "peer_until": "soon"},
        {"peers": [ALICE], "groups": [], "peer_until": {ALICE: "soon"}},
        {"peers": [ALICE], "groups": [], "peer_until": {ALICE: True}},
        {"peers": [ALICE], "groups": [], "peer_until": {ALICE: -5}},
        {"peers": [ALICE], "groups": [], "peer_until": {"alice": 9999999999}},
        {"peers": [], "groups": [GROUP_ID], "group_until": {GROUP_ID: None}},
    ):
        path.write_text(json.dumps(document), encoding="utf-8")
        with pytest.raises(MutedError):
            load_muted(path)


def test_dangling_expiry_is_dropped_not_an_error(tmp_path):
    # An expiry for a key that is no longer muted is stale, not
    # corrupt: the load drops it and the file still reads.
    path = tmp_path / "muted.json"
    path.write_text(
        json.dumps(
            {
                "peers": [],
                "groups": [],
                "peer_until": {ALICE: 9999999999},
            }
        ),
        encoding="utf-8",
    )
    loaded = load_muted(path)
    assert loaded.peers == frozenset()
    assert loaded.peer_until == {}


def test_with_until_rejects_bad_values():
    with pytest.raises(MutedError):
        MutedConversations().with_peer(ALICE, until="soon")
    with pytest.raises(MutedError):
        MutedConversations().with_peer(ALICE, until=float("inf"))
    with pytest.raises(MutedError):
        MutedConversations().with_group(GROUP_ID, until=-1)


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
    assert args.for_duration is None
    args = parser.parse_args(["mute", "--group", "crew", "--off"])
    assert args.group == "crew"
    assert args.off is True
    args = parser.parse_args(["mute", "--peer", "alice", "--for", "1h"])
    assert args.for_duration == "1h"
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


def test_mute_peer_timed_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--for", "1h"]
    )
    assert code == 0
    assert "for 1h:" in capsys.readouterr().out
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_peer(senders["alice"]) is True
    assert muted.muted_until_peer(senders["alice"]) == pytest.approx(
        time.time() + 3600, abs=5
    )

    # The list names the remaining time, and --json carries the epoch.
    code = cli.main(["mute", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "muted for" in out and "more" in out
    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--json"]
    )
    assert code == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["muted"] is True
    assert shown["until"] == pytest.approx(time.time() + 3600, abs=5)

    # A timed re-mute replaces the expiry; a plain re-mute lifts it
    # back to indefinite.
    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--for", "2h"]
    )
    assert code == 0
    assert "for 2h" in capsys.readouterr().out
    muted = load_muted(data_dir / "muted.json")
    assert muted.muted_until_peer(senders["alice"]) == pytest.approx(
        time.time() + 7200, abs=5
    )
    # A plain re-mute leaves the timed mute alone: still muted, same
    # expiry, and the command says so instead of changing anything.
    code = cli.main(["mute", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    assert "already muted for" in capsys.readouterr().out
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_peer(senders["alice"]) is True
    assert muted.muted_until_peer(senders["alice"]) == pytest.approx(
        time.time() + 7200, abs=5
    )

    # Indefinite again takes --off followed by a fresh mute.
    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--off"]
    )
    assert code == 0
    capsys.readouterr()
    code = cli.main(["mute", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_peer(senders["alice"]) is True
    assert muted.muted_until_peer(senders["alice"]) is None

    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--off"]
    )
    assert code == 0
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_peer(senders["alice"]) is False


def test_mute_group_timed_roundtrip(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    store = GroupStore(config.groups_path)
    created = store.create("crew", ["aa" * 16, "bb" * 16], created_by="aa" * 16)

    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--group", "crew", "--for", "30m"]
    )
    assert code == 0
    assert "for 30m:" in capsys.readouterr().out
    muted = load_muted(data_dir / "muted.json")
    assert muted.is_muted_group(created["id"]) is True
    assert muted.muted_until_group(created["id"]) == pytest.approx(
        time.time() + 1800, abs=5
    )

    code = cli.main(["mute", "--data-dir", str(data_dir)])
    assert code == 0
    assert "muted for" in capsys.readouterr().out


def test_mute_for_rejects_bad_input(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    code = cli.main(["mute", "--data-dir", str(data_dir), "--for", "1h"])
    assert code == 1
    assert "error" in capsys.readouterr().err
    code = cli.main(
        [
            "mute",
            "--data-dir",
            str(data_dir),
            "--peer",
            "alice",
            "--for",
            "1h",
            "--off",
        ]
    )
    assert code == 1
    assert "error" in capsys.readouterr().err
    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--for", "banana"]
    )
    assert code == 1
    assert "error" in capsys.readouterr().err
    code = cli.main(
        ["mute", "--data-dir", str(data_dir), "--peer", "alice", "--for", "31d"]
    )
    assert code == 1
    assert "error" in capsys.readouterr().err


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


def test_expired_timed_mute_counts_unread(tmp_path, capsys):
    # A timed mute whose hour has passed is simply unmuted: the badge
    # comes back on its own, with no second command.
    data_dir, senders = _seed_two_peers(tmp_path)
    (data_dir / "muted.json").write_text(
        json.dumps(
            {
                "peers": [senders["alice"]],
                "groups": [],
                "peer_until": {senders["alice"]: 1.0},
            }
        ),
        encoding="utf-8",
    )

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    out = capsys.readouterr().out
    assert "Total: 2 unread" in out
    assert "alice" in out

    # The list agrees: nothing is muted anymore.
    code = cli.main(["mute", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No muted conversations." in capsys.readouterr().out

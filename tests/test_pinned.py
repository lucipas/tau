"""Pinned conversations: module behaviour and CLI behaviour."""

from __future__ import annotations

import json
import os
import stat

import pytest

from tau import cli
from tau.config import TauConfig
from tau.inbox import Inbox
from tau.pinned import (
    MAX_PINNED,
    PinnedError,
    PinnedList,
    load_pinned,
    load_pinned_set,
    save_pinned,
)


# -- module ---------------------------------------------------------------

ALICE = "aa" * 16
BOB = "bb" * 16
CAROL = "cc" * 16


def test_missing_file_pins_nobody(tmp_path):
    pinned = load_pinned(tmp_path / "pinned.json")
    assert pinned.entries == ()
    assert pinned.is_pinned("peer", ALICE) is False
    assert pinned.is_pinned("channel", "general") is False
    assert pinned.is_pinned("peer", None) is False


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "pinned.json"
    pinned = PinnedList().with_pinned("peer", ALICE, now=10.0)
    pinned = pinned.with_pinned("channel", "general", now=20.0)
    saved = save_pinned(path, pinned)
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_pinned(path)
    assert [entry.kind for entry in loaded.entries] == ["channel", "peer"]
    assert loaded.entries[0].target == "general"
    assert loaded.entries[0].pinned_at == 20.0
    assert loaded.to_dict() == {
        "pinned": [
            {"kind": "channel", "target": "general", "pinned_at": 20.0},
            {"kind": "peer", "target": ALICE, "pinned_at": 10.0},
        ]
    }


def test_malformed_documents_raise(tmp_path):
    path = tmp_path / "pinned.json"
    for bad in (
        {"pinned": "alice"},
        {"other": []},
        {"pinned": [{"kind": "peer", "target": ALICE}]},
        {"pinned": [{"kind": "dm", "target": ALICE, "pinned_at": 1.0}]},
        {"pinned": [{"kind": "peer", "target": "not-a-hash", "pinned_at": 1.0}]},
        {"pinned": [{"kind": "channel", "target": "bad name!", "pinned_at": 1.0}]},
        {"pinned": [{"kind": "peer", "target": ALICE, "pinned_at": "soon"}]},
        {
            "pinned": [
                {"kind": "peer", "target": ALICE, "pinned_at": 1.0},
                {"kind": "peer", "target": ALICE, "pinned_at": 2.0},
            ]
        },
    ):
        path.write_text(json.dumps(bad), encoding="utf-8")
        with pytest.raises(PinnedError):
            load_pinned(path)
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(PinnedError):
        load_pinned(path)


def test_pinned_set_fails_open_on_corrupt_file(tmp_path, caplog):
    # Pinning is a display convenience, not a security boundary: a
    # corrupt file shows everything unpinned (with a warning) instead
    # of reordering mail the owner never saw.
    path = tmp_path / "pinned.json"
    path.write_text("{not json", encoding="utf-8")
    assert load_pinned_set(path, "peer") == frozenset()
    assert "unusable" in caplog.text


def test_with_pinned_moves_to_front_and_unpin_is_idempotent():
    pinned = PinnedList()
    pinned = pinned.with_pinned("peer", ALICE, now=10.0)
    pinned = pinned.with_pinned("peer", BOB, now=20.0)
    assert [entry.target for entry in pinned.entries] == [BOB, ALICE]
    # Re-pinning moves to the front and refreshes the stamp.
    pinned = pinned.with_pinned("peer", ALICE, now=30.0)
    assert [entry.target for entry in pinned.entries] == [ALICE, BOB]
    assert pinned.entries[0].pinned_at == 30.0
    assert pinned.is_pinned("peer", ALICE) is True
    assert pinned.is_pinned("peer", CAROL) is False
    assert pinned.pinned_targets("peer") == frozenset({ALICE, BOB})
    assert pinned.pinned_targets("group") == frozenset()
    unpinned = pinned.without("peer", ALICE)
    assert [entry.target for entry in unpinned.entries] == [BOB]
    assert unpinned.without("peer", ALICE).entries == unpinned.entries
    assert pinned.entries[0].target == ALICE  # the original is untouched


def test_pin_cap_is_enforced():
    pinned = PinnedList()
    for index in range(MAX_PINNED):
        pinned = pinned.with_pinned("peer", f"{index:032x}", now=float(index))
    assert len(pinned.entries) == MAX_PINNED
    with pytest.raises(PinnedError, match="too many pinned"):
        pinned.with_pinned("peer", CAROL)
    # Re-pinning one already pinned still fits: it moves, not grows.
    pinned.with_pinned("peer", f"{0:032x}", now=999.0)


def test_channel_names_are_cleaned():
    pinned = PinnedList().with_pinned("channel", "  general  ", now=1.0)
    assert pinned.entries[0].target == "general"
    assert pinned.is_pinned("channel", "general") is True
    # Channel names keep their case; only surrounding whitespace is cut.
    assert (
        PinnedList().with_pinned("channel", "General", now=1.0).entries[0].target
        == "General"
    )
    with pytest.raises(PinnedError):
        PinnedList().with_pinned("channel", "bad name!")
    with pytest.raises(PinnedError):
        PinnedList().with_pinned("dm", ALICE)


# -- CLI ------------------------------------------------------------------

def _seed_two_peers(tmp_path):
    from tau.identity import destination_hash
    from tau.peers import PeerRegistry
    import RNS

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


def _seed_group(data_dir, senders):
    from tau.groups import GroupStore

    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    group = GroupStore(config.groups_path).create(
        "weekend",
        [senders["alice"], senders["carol"]],
        created_by=senders["alice"],
    )
    return group["id"]


def test_parser_pin_conversation_arguments():
    parser = cli.build_parser()
    pin = parser.parse_args(["pin-conversation", "--peer", "alice"])
    assert pin.command == "pin-conversation"
    assert pin.peer == "alice"
    assert pin.group is None and pin.channel is None
    group = parser.parse_args(["pin-conversation", "--group", "weekend"])
    assert group.group == "weekend"
    channel = parser.parse_args(["pin-conversation", "--channel", "general"])
    assert channel.channel == "general"
    unpin = parser.parse_args(["unpin-conversation", "--peer", "alice"])
    assert unpin.command == "unpin-conversation"
    listed = parser.parse_args(["pinned-conversations"])
    assert listed.command == "pinned-conversations"


def test_pin_unpin_peer_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--peer", "alice"]
    )
    assert code == 0
    assert "Pinned conversation with alice" in capsys.readouterr().out

    # Idempotent: pinning again says so and exits 0.
    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--peer", "alice"]
    )
    assert code == 0
    assert "already pinned" in capsys.readouterr().out

    code = cli.main(["pinned-conversations", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "carol" not in out

    code = cli.main(
        ["unpin-conversation", "--data-dir", str(data_dir), "--peer", "alice"]
    )
    assert code == 0
    assert "Unpinned conversation with alice" in capsys.readouterr().out

    # Idempotent the other way too.
    code = cli.main(
        ["unpin-conversation", "--data-dir", str(data_dir), "--peer", "alice"]
    )
    assert code == 0
    assert "was not pinned" in capsys.readouterr().out

    code = cli.main(["pinned-conversations", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No pinned conversations" in capsys.readouterr().out


def test_pin_conversation_json_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--json", "--peer", "alice"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "kind": "peer",
        "target": senders["alice"],
        "pinned": True,
        "changed": True,
    }
    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--json", "--peer", "alice"]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["changed"] is False

    code = cli.main(["pinned-conversations", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["pinned"][0]["kind"] == "peer"
    assert payload["pinned"][0]["target"] == senders["alice"]


def test_pin_group_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    group_id = _seed_group(data_dir, senders)

    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--group", "weekend"]
    )
    assert code == 0
    assert "Pinned group weekend" in capsys.readouterr().out

    code = cli.main(["group", "list", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["groups"][0]["id"] == group_id
    assert payload["groups"][0]["pinned"] is True

    code = cli.main(["group", "list", "--data-dir", str(data_dir)])
    assert code == 0
    assert "[pinned]" in capsys.readouterr().out

    code = cli.main(
        ["unpin-conversation", "--data-dir", str(data_dir), "--group", "weekend"]
    )
    assert code == 0
    assert "Unpinned group weekend" in capsys.readouterr().out


def test_pin_channel_roundtrip(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert cli.main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        cli.main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    assert (
        cli.main(["channel", "join", "--data-dir", str(data_dir), "random"]) == 0
    )
    capsys.readouterr()

    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--channel", "general"]
    )
    assert code == 0
    assert "Pinned channel #general" in capsys.readouterr().out

    # Pinned first in the list, flagged in JSON and in text.
    code = cli.main(["channel", "list", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["channel"] for item in payload["channels"]] == [
        "general",
        "random",
    ]
    assert [item["pinned"] for item in payload["channels"]] == [True, False]

    code = cli.main(["channel", "list", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert out.index("#general") < out.index("#random")
    assert "[pinned]" in out

    code = cli.main(
        ["unpin-conversation", "--data-dir", str(data_dir), "--channel", "general"]
    )
    assert code == 0
    assert "Unpinned channel #general" in capsys.readouterr().out


def test_pin_unknown_targets_error(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)

    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--peer", "nobody"]
    )
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err

    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--group", "nope"]
    )
    assert code == 1
    assert "unknown group" in capsys.readouterr().err


def test_pin_unjoined_channel_errors(tmp_path, capsys):
    # A channel this node never joined is a typo, not a pin.
    data_dir = tmp_path / "alice"
    assert cli.main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    capsys.readouterr()

    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--channel", "general"]
    )
    assert code == 1
    assert "not a member of channel" in capsys.readouterr().err

    # Unpinning needs no membership: a pin for a channel left since
    # stays removable instead of wedging the list.
    code = cli.main(
        ["unpin-conversation", "--data-dir", str(data_dir), "--channel", "general"]
    )
    assert code == 0
    assert "was not pinned" in capsys.readouterr().out


def test_peers_list_pinned_first(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    cli.main(["pin-conversation", "--data-dir", str(data_dir), "--peer", "carol"])
    capsys.readouterr()

    code = cli.main(["peers", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [entry["name"] for entry in payload["peers"]] == ["carol", "alice"]
    assert [entry["pinned"] for entry in payload["peers"]] == [True, False]

    code = cli.main(["peers", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "📌 carol" in out
    assert out.index("carol") < out.index("alice")


def test_pin_takes_the_lock(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    code = cli.main(
        ["pin-conversation", "--data-dir", str(data_dir), "--peer", "alice"]
    )
    assert code == 0
    capsys.readouterr()
    assert (data_dir / ".pinned.json.lock").exists()


def test_pinned_list_newest_first(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    cli.main(["pin-conversation", "--data-dir", str(data_dir), "--peer", "alice"])
    cli.main(["pin-conversation", "--data-dir", str(data_dir), "--peer", "carol"])
    capsys.readouterr()

    code = cli.main(["pinned-conversations", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert out.index("carol") < out.index("alice")

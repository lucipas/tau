"""Starred messages: module behaviour and CLI behaviour."""

from __future__ import annotations

import json
import os
import stat

import pytest
import RNS

from tau import cli
from tau.channels import create_message, init_db as init_channel_db
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.peers import PeerRegistry
from tau.saved_messages import (
    MAX_SAVED_MESSAGES,
    SavedMessages,
    SavedMessagesError,
    load_saved_messages,
    save_saved_messages,
)


# -- module ---------------------------------------------------------------

ALICE = "aa" * 16
BOB = "bb" * 16
CAROL = "cc" * 16


def test_missing_file_stars_nothing(tmp_path):
    saved = load_saved_messages(tmp_path / "saved_messages.json")
    assert saved.entries == ()
    assert saved.is_saved("peer", ALICE, "m-1") is False
    assert saved.is_saved("channel", "general", "abc") is False
    assert saved.is_saved("peer", None, None) is False


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "saved_messages.json"
    saved = SavedMessages()
    saved, already = saved.with_saved("peer", ALICE, "m-1", now=10.0)
    assert already is False
    saved, already = saved.with_saved("channel", "general", "abc123", now=20.0)
    assert already is False
    saved_path = save_saved_messages(path, saved)
    assert saved_path == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_saved_messages(path)
    assert [(entry.kind, entry.scope) for entry in loaded.entries] == [
        ("channel", "general"),
        ("peer", ALICE),
    ]
    assert loaded.entries[0].message_id == "abc123"
    assert loaded.entries[0].saved_at == 20.0
    assert loaded.to_dict() == {
        "saved": [
            {
                "kind": "channel",
                "scope": "general",
                "message_id": "abc123",
                "saved_at": 20.0,
            },
            {
                "kind": "peer",
                "scope": ALICE,
                "message_id": "m-1",
                "saved_at": 10.0,
            },
        ]
    }


def test_malformed_documents_raise(tmp_path):
    path = tmp_path / "saved_messages.json"
    for bad in (
        "not json",
        "[]",
        "{}",
        {"saved": {}},
        {"saved": ["nope"]},
        {"saved": [{"kind": "dm", "scope": ALICE, "message_id": "m-1", "saved_at": 1.0}]},
        {"saved": [{"kind": "peer", "scope": "x", "message_id": "m-1", "saved_at": 1.0}]},
        {"saved": [{"kind": "peer", "scope": ALICE, "message_id": "", "saved_at": 1.0}]},
        {"saved": [{"kind": "peer", "scope": ALICE, "message_id": "m-1", "saved_at": "soon"}]},
        {
            "saved": [
                {
                    "kind": "peer",
                    "scope": ALICE,
                    "message_id": "m-1",
                    "saved_at": 1.0,
                    "direction": "sideways",
                }
            ]
        },
        {
            "saved": [
                {
                    "kind": "peer",
                    "scope": ALICE,
                    "message_id": "m-1",
                    "saved_at": 1.0,
                    "sender": 123,
                }
            ]
        },
        {
            "saved": [
                {"kind": "peer", "scope": ALICE, "message_id": "m-1", "saved_at": 1.0},
                {"kind": "peer", "scope": ALICE, "message_id": "m-1", "saved_at": 2.0},
            ]
        },
    ):
        path.write_text(json.dumps(bad), encoding="utf-8")
        with pytest.raises(SavedMessagesError):
            load_saved_messages(path)


def test_restarring_is_a_noop_and_moves_to_front(tmp_path):
    saved = SavedMessages()
    saved, _ = saved.with_saved("peer", ALICE, "m-1", now=10.0)
    saved, _ = saved.with_saved("peer", BOB, "m-2", now=20.0)
    saved, already = saved.with_saved("peer", ALICE, "m-1", now=30.0)
    assert already is True
    assert [entry.message_id for entry in saved.entries] == ["m-1", "m-2"]
    assert saved.entries[0].saved_at == 30.0


def test_unstar_roundtrip(tmp_path):
    saved = SavedMessages()
    saved, _ = saved.with_saved("peer", ALICE, "m-1", now=10.0)
    saved, changed = saved.without("peer", ALICE, "m-1")
    assert changed is True
    assert saved.entries == ()
    saved, changed = saved.without("peer", ALICE, "m-1")
    assert changed is False
    assert saved.is_saved("peer", ALICE, "m-1") is False


def test_cap_is_enforced(tmp_path):
    saved = SavedMessages()
    for n in range(MAX_SAVED_MESSAGES):
        saved, already = saved.with_saved("peer", ALICE, f"m-{n}", now=float(n))
        assert already is False
    with pytest.raises(SavedMessagesError, match="too many starred messages"):
        saved.with_saved("peer", ALICE, "m-extra", now=999.0)
    # Unstarring frees capacity.
    saved, _ = saved.without("peer", ALICE, "m-0")
    saved, already = saved.with_saved("peer", ALICE, "m-extra", now=999.0)
    assert already is False


def test_same_message_id_in_two_conversations_is_two_stars():
    saved = SavedMessages()
    saved, _ = saved.with_saved("peer", ALICE, "m-1", now=10.0)
    saved, already = saved.with_saved("peer", BOB, "m-1", now=20.0)
    assert already is False
    assert len(saved.entries) == 2


# -- CLI -------------------------------------------------------------------

def _seed_two_peers(tmp_path):
    data_dir = tmp_path / "alice"
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    senders = {}
    for name, received_at in (
        ("alice", 20.0),
        ("bob", 21.0),
        ("carol", 22.0),
    ):
        sender = RNS.Identity()
        sender_hash = destination_hash(sender)
        registry.upsert(sender_hash, sender.get_public_key(), name=name, now=100.0)
        registry.save()
        inbox.append(
            {
                "from": name,
                "destination": sender_hash.hex(),
                "text": f"hello from {name}",
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
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "carol",
            "destination": senders["carol"],
            "text": "weekend plans?",
            "sent_at": 11.0,
            "received_at": 23.0,
            "id": "m-group-1",
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
        }
    )
    return group["id"]


def _seed_channel_message(data_dir):
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    conn = init_channel_db(config.channels_db_path)
    try:
        message = create_message(
            conn, RNS.Identity(), "general", "hello channel", name="alice", now=1000
        )
    finally:
        conn.close()
    return message["hash"]


def test_parser_save_arguments():
    parser = cli.build_parser()
    save = parser.parse_args(["save", "--peer", "alice", "m-alice"])
    assert save.command == "save"
    assert save.peer == "alice"
    assert save.group is None and save.channel is None
    assert save.message == "m-alice"
    group = parser.parse_args(["save", "--group", "weekend", "m-group-1"])
    assert group.group == "weekend"
    channel = parser.parse_args(["save", "--channel", "general", "abc123"])
    assert channel.channel == "general"
    unsave = parser.parse_args(["unsave", "--peer", "alice", "m-alice"])
    assert unsave.command == "unsave"
    listed = parser.parse_args(["saved"])
    assert listed.command == "saved"
    assert listed.limit == 30


def test_save_saved_unsave_dm_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(
        ["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "saved [m-alice] from alice" in out
    assert "DM with alice" in out
    assert "local-only" in out

    # Idempotent: saving again says so and exits 0.
    code = cli.main(
        ["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"]
    )
    assert code == 0
    assert "already saved" in capsys.readouterr().out

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "★ saved" in out
    assert "DM with alice" in out
    assert "hello from alice" in out
    assert "hello from bob" not in out

    code = cli.main(
        ["unsave", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"]
    )
    assert code == 0
    assert "unsaved [m-alice] from alice" in capsys.readouterr().out

    # Idempotent the other way too.
    code = cli.main(
        ["unsave", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"]
    )
    assert code == 0
    assert "was not saved" in capsys.readouterr().out

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No starred messages" in capsys.readouterr().out


def test_save_dm_json_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(
        [
            "save",
            "--data-dir",
            str(data_dir),
            "--json",
            "--peer",
            "alice",
            "m-alice",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "kind": "peer",
        "scope": senders["alice"],
        "message_id": "m-alice",
        "saved": True,
        "changed": True,
    }
    code = cli.main(
        [
            "save",
            "--data-dir",
            str(data_dir),
            "--json",
            "--peer",
            "alice",
            "m-alice",
        ]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["changed"] is False

    code = cli.main(["saved", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["saved"][0]["kind"] == "peer"
    assert payload["saved"][0]["scope"] == senders["alice"]
    assert payload["saved"][0]["message_id"] == "m-alice"
    assert payload["saved"][0]["message"]["text"] == "hello from alice"


def test_save_by_prefix_and_ambiguous_prefix_refused(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    # A unique prefix stars the same message.
    code = cli.main(["save", "--data-dir", str(data_dir), "--peer", "bob", "m-b"])
    assert code == 0
    assert "saved [m-bob]" in capsys.readouterr().out

    # A second message from the same peer sharing the prefix: ambiguous.
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": senders["alice"],
            "text": "second hello from alice",
            "sent_at": 12.0,
            "received_at": 24.0,
            "id": "m-alice-2",
        }
    )
    code = cli.main(["save", "--data-dir", str(data_dir), "--peer", "alice", "m-a"])
    assert code != 0
    assert "match" in capsys.readouterr().err

    # The full id still resolves.
    code = cli.main(
        ["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice-2"]
    )
    assert code == 0
    assert "saved [m-alice-]" in capsys.readouterr().out

    # An unknown ref is refused.
    code = cli.main(
        ["save", "--data-dir", str(data_dir), "--peer", "alice", "m-nope"]
    )
    assert code != 0
    assert "no message" in capsys.readouterr().err


def test_save_group_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    group_id = _seed_group(data_dir, senders)

    code = cli.main(
        ["save", "--data-dir", str(data_dir), "--group", "weekend", "m-group-1"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "saved [m-group-]" in out
    assert 'group "weekend"' in out

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert 'group "weekend"' in out
    assert "weekend plans?" in out


def test_save_channel_roundtrip(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert cli.main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        cli.main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    digest = _seed_channel_message(data_dir)
    capsys.readouterr()

    code = cli.main(
        ["save", "--data-dir", str(data_dir), "--channel", "general", digest[:12]]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert f"saved [{digest[:8]}]" in out
    assert "#general" in out

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "#general" in out
    assert "hello channel" in out

    code = cli.main(["saved", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["saved"][0]["kind"] == "channel"
    assert payload["saved"][0]["message"]["text"] == "hello channel"


def test_saved_list_keeps_stub_when_message_is_gone(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    assert (
        cli.main(["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"])
        == 0
    )
    capsys.readouterr()
    # The message leaves the inbox; the star stays and renders as gone.
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    inbox.delete("m-alice")
    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "DM with alice" in out
    assert "no longer stored" in out


def test_corrupt_list_refuses_save_but_lists_empty(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    config.saved_messages_path.write_text("not json", encoding="utf-8")

    code = cli.main(
        ["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"]
    )
    assert code != 0
    assert "could not read starred-message list" in capsys.readouterr().err

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No starred messages" in capsys.readouterr().out


def test_backup_includes_saved_messages_once():
    from tau.backup import BACKUP_FILES

    assert BACKUP_FILES.count("saved_messages.json") == 1


def test_save_sent_message_by_peer_prefix(tmp_path, capsys):
    # The recipient lives only in outgoing history: no registry entry,
    # so the hash prefix must resolve from sent records (direction=None).
    data_dir, senders = _seed_two_peers(tmp_path)
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient).hex()
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": recipient_hash,
            "text": "sent to a stranger",
            "sent_at": 13.0,
            "received_at": 25.0,
            "id": "m-sent-1",
            "direction": "out",
        }
    )
    code = cli.main(
        [
            "save",
            "--data-dir",
            str(data_dir),
            "--peer",
            recipient_hash[:12],
            "m-sent-1",
        ]
    )
    assert code == 0
    assert "saved [m-sent-1]" in capsys.readouterr().out


def test_unsave_after_message_deleted(tmp_path, capsys):
    # A star outlives its message: the star itself must still be removable.
    data_dir, senders = _seed_two_peers(tmp_path)
    assert (
        cli.main(["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"])
        == 0
    )
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    Inbox(config.inbox_path).delete("m-alice")

    code = cli.main(
        ["unsave", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "unsaved [m-alice]" in out
    assert "no longer stored" in out

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No starred messages" in capsys.readouterr().out


def test_group_star_survives_rename(tmp_path, capsys):
    # The star is scoped to the group id the message was stored under,
    # so renaming the group must not turn the star into a stub.
    from tau.groups import GroupStore

    data_dir, senders = _seed_two_peers(tmp_path)
    group_id = _seed_group(data_dir, senders)
    assert (
        cli.main(
            ["save", "--data-dir", str(data_dir), "--group", "weekend", "m-group-1"]
        )
        == 0
    )
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    GroupStore(config.groups_path).rename(
        group_id, "weekend2", created_by=senders["alice"]
    )

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "weekend plans?" in out
    assert "no longer stored" not in out


def test_saved_list_hides_blocked_sender(tmp_path, capsys):
    from tau.blocklist import BlockList, save_blocklist

    data_dir, senders = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    # A sent message to bob, starred alongside alice's received one.
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": senders["bob"],
            "text": "sent to bob",
            "sent_at": 14.0,
            "received_at": 26.0,
            "id": "m-sent-bob",
            "direction": "out",
        }
    )
    assert (
        cli.main(["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"])
        == 0
    )
    assert (
        cli.main(["save", "--data-dir", str(data_dir), "--peer", "bob", "m-sent-bob"])
        == 0
    )
    capsys.readouterr()
    save_blocklist(
        config.blocklist_path, BlockList().with_added(senders["alice"])
    )

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "hello from alice" not in out
    assert "sent to bob" in out

    code = cli.main(["saved", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["saved"][0]["message_id"] == "m-sent-bob"


def test_get_visible_channel_message_applies_lifecycle(tmp_path):
    from tau.channels import (
        create_edit,
        create_message,
        get_message,
        get_visible_message,
    )

    config = TauConfig.resolve(data_dir=tmp_path / "alice", name="alice")
    identity = RNS.Identity()
    conn = init_channel_db(config.channels_db_path)
    try:
        live = create_message(
            conn, identity, "general", "live text", name="alice", now=1000
        )
        expired = create_message(
            conn,
            identity,
            "general",
            "old text",
            name="alice",
            now=1000,
            expires_at=1001.0,
        )
        # The expired row is still stored; history must not show it.
        assert get_message(conn, expired["hash"]) is not None
        assert get_visible_message(conn, expired["hash"]) is None
        assert get_visible_message(conn, live["hash"]) is not None
        # An edit shows its latest text, like channel history.
        create_edit(conn, identity, "general", live["hash"], "edited text", now=1002)
        visible = get_visible_message(conn, live["hash"])
        assert visible is not None
        assert visible["text"] == "edited text"
        assert visible["edited"] is True
    finally:
        conn.close()


# -- Qodo follow-ups on #674 ------------------------------------------------

def test_unsave_refuses_ambiguous_prefix_when_one_star_exists(tmp_path, capsys):
    # Two live messages share the prefix; only one is starred.  The
    # live resolver refuses the prefix, so unsave must refuse it too
    # instead of clearing the one star that happens to match.
    data_dir, senders = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    Inbox(config.inbox_path).append(
        {
            "from": "alice",
            "destination": senders["alice"],
            "text": "second hello from alice",
            "sent_at": 12.0,
            "received_at": 24.0,
            "id": "m-alice-2",
        }
    )
    assert (
        cli.main(["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"])
        == 0
    )
    capsys.readouterr()

    code = cli.main(["unsave", "--data-dir", str(data_dir), "--peer", "alice", "m-a"])
    assert code != 0
    assert "match" in capsys.readouterr().err

    # The star survives the refused unsave.
    assert load_saved_messages(config.saved_messages_path).is_saved(
        "peer", senders["alice"], "m-alice"
    )


def test_unsave_refuses_short_channel_digest_prefix(tmp_path, capsys):
    # A one-character digest prefix is refused by the live resolver,
    # so it must not fall through to the starred-entry search and
    # clear the star it happens to match.
    data_dir = tmp_path / "alice"
    assert cli.main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert cli.main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    digest = _seed_channel_message(data_dir)
    capsys.readouterr()
    assert (
        cli.main(
            ["save", "--data-dir", str(data_dir), "--channel", "general", digest[:12]]
        )
        == 0
    )
    capsys.readouterr()

    code = cli.main(
        ["unsave", "--data-dir", str(data_dir), "--channel", "general", digest[0]]
    )
    assert code != 0
    assert "4 hex" in capsys.readouterr().err

    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    assert load_saved_messages(config.saved_messages_path).is_saved(
        "channel", "general", digest
    )


def test_unsave_deleted_peer_resolves_via_saved_scope(tmp_path, capsys):
    # The peer was never registered and its last inbox record is
    # deleted, so the hash prefix resolves only from the star itself.
    data_dir, senders = _seed_two_peers(tmp_path)
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient).hex()
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": recipient_hash,
            "text": "sent to a stranger",
            "sent_at": 13.0,
            "received_at": 25.0,
            "id": "m-sent-1",
            "direction": "out",
        }
    )
    assert (
        cli.main(
            [
                "save",
                "--data-dir",
                str(data_dir),
                "--peer",
                recipient_hash[:12],
                "m-sent-1",
            ]
        )
        == 0
    )
    capsys.readouterr()
    Inbox(config.inbox_path).delete("m-sent-1")

    code = cli.main(
        [
            "unsave",
            "--data-dir",
            str(data_dir),
            "--peer",
            recipient_hash[:12],
            "m-sent-1",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "unsaved [m-sent-1]" in out
    assert "no longer stored" in out


def test_unsave_refuses_ambiguous_saved_peer_prefix(tmp_path, capsys):
    # Two starred peer scopes share the prefix: the saved scopes join
    # the prefix search, so the ambiguity must be refused, not resolved
    # to whichever scope happens to come first.
    data_dir, senders = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    scope_a = "aabbccdd" * 4
    scope_b = "aabbccdd" + "ee" * 12
    saved = SavedMessages()
    saved, _ = saved.with_saved("peer", scope_a, "m-1", now=10.0)
    saved, _ = saved.with_saved("peer", scope_b, "m-2", now=20.0)
    save_saved_messages(config.saved_messages_path, saved)

    code = cli.main(
        ["unsave", "--data-dir", str(data_dir), "--peer", "aabbccdd", "m-1"]
    )
    assert code != 0
    assert "2 senders match" in capsys.readouterr().err


def test_saved_list_hides_blocked_sender_stub(tmp_path, capsys):
    # The star outlives its message, but the blocklist still applies
    # to the stub: a blocked sender's star must not resurface once
    # the message is deleted.
    from tau.blocklist import BlockList, save_blocklist

    data_dir, senders = _seed_two_peers(tmp_path)
    assert (
        cli.main(["save", "--data-dir", str(data_dir), "--peer", "alice", "m-alice"])
        == 0
    )
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    save_blocklist(config.blocklist_path, BlockList().with_added(senders["alice"]))
    Inbox(config.inbox_path).delete("m-alice")

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No starred messages" in capsys.readouterr().out

    code = cli.main(["saved", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0


def test_saved_list_keeps_outbound_stub(tmp_path, capsys):
    # A message this node sent stays visible as a stub even when its
    # recipient is blocked: outbound is never hidden by the blocklist.
    from tau.blocklist import BlockList, save_blocklist

    data_dir, senders = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    Inbox(config.inbox_path).append(
        {
            "from": "alice",
            "destination": senders["bob"],
            "text": "sent to bob",
            "sent_at": 14.0,
            "received_at": 26.0,
            "id": "m-sent-bob",
            "direction": "out",
        }
    )
    assert (
        cli.main(["save", "--data-dir", str(data_dir), "--peer", "bob", "m-sent-bob"])
        == 0
    )
    capsys.readouterr()
    save_blocklist(config.blocklist_path, BlockList().with_added(senders["bob"]))
    Inbox(config.inbox_path).delete("m-sent-bob")

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "DM with bob" in out
    assert "no longer stored" in out


def test_legacy_stub_without_sender_is_hidden(tmp_path, capsys):
    # A star written before sender/direction were recorded cannot be
    # checked against the blocklist, so its stub stays hidden once
    # the message itself is gone.
    data_dir, senders = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    config.saved_messages_path.write_text(
        json.dumps(
            {
                "saved": [
                    {
                        "kind": "peer",
                        "scope": senders["alice"],
                        "message_id": "m-alice",
                        "saved_at": 1.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    Inbox(config.inbox_path).delete("m-alice")

    code = cli.main(["saved", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No starred messages" in capsys.readouterr().out


def test_sender_and_direction_roundtrip(tmp_path):
    path = tmp_path / "saved_messages.json"
    saved = SavedMessages()
    saved, _ = saved.with_saved(
        "peer", ALICE, "m-1", now=10.0, sender=ALICE, direction="in"
    )
    saved, _ = saved.with_saved(
        "peer", BOB, "m-2", now=20.0, sender=BOB, direction="out"
    )
    save_saved_messages(path, saved)
    loaded = load_saved_messages(path)
    assert [(e.sender, e.direction) for e in loaded.entries] == [
        (BOB, "out"),
        (ALICE, "in"),
    ]
    assert loaded.to_dict() == {
        "saved": [
            {
                "kind": "peer",
                "scope": BOB,
                "message_id": "m-2",
                "saved_at": 20.0,
                "sender": BOB,
                "direction": "out",
            },
            {
                "kind": "peer",
                "scope": ALICE,
                "message_id": "m-1",
                "saved_at": 10.0,
                "sender": ALICE,
                "direction": "in",
            },
        ]
    }
